# 🔧 dbt (Data Build Tool) — SQL Transformations & Modeling

## Overview

dbt (Data Build Tool) is the **transformation layer (the "T" in ETL/ELT)** of Foodingo's data engineering pipeline. 

While the Python Kafka Consumer ingests raw event data into PostgreSQL's `raw` schema, those tables contain duplicate retried events, raw timestamps, and unaggregated JSON strings. **dbt transforms this messy raw data into clean, dimensional, business-ready analytical tables in the `analytics` schema — using nothing but modular SQL.**

---

## Why dbt?

| Requirement | How dbt Solves It |
|---|---|
| **SQL-First Engineering** | Analysts and Data Engineers write SELECT queries; dbt handles DDL (`CREATE TABLE/VIEW`) |
| **Dependency Management** | `{{ ref('stg_orders') }}` syntax automatically builds a Directed Acyclic Graph (DAG) |
| **Version Control** | Data models are text `.sql` files tracked in Git alongside application code |
| **Automated Testing** | Built-in data quality tests (`unique`, `not_null`, `accepted_values`) run in CI/CD |
| **Incremental Processing** | Only transforms newly arrived rows instead of rebuilding entire historical tables |

---

## The 3-Layer Architecture in Foodingo

```
                       RAW SCHEMA (PostgreSQL)
                       ┌─────────────────────┐
                       │  raw.user_events    │
                       │  raw.cart_events    │
                       │  raw.order_events   │
                       │  raw.cdc_events     │
                       └──────────┬──────────┘
                                  │
         ═════════════════════════╪═════════════════════════
         ║               STAGING LAYER (Views)             ║
         ═════════════════════════╪═════════════════════════
                                  │
                       ┌──────────▼──────────┐
                       │ stg_orders   (VIEW) │── Deduplicates retried order events
                       │ stg_cart     (VIEW) │── Cleans cart events & parses dates
                       │ stg_users    (VIEW) │── Cleans user registrations
                       └──────────┬──────────┘
                                  │
         ═════════════════════════╪═════════════════════════
         ║               FACT LAYER (Tables)               ║
         ═════════════════════════╪═════════════════════════
                                  │
                       ┌──────────▼──────────┐
                       │ fact_orders (TABLE) │── Immutable paid order history
                       │ fact_cart   (TABLE) │── Cleaned cart interaction history
                       └──────────┬──────────┘
                                  │
         ═════════════════════════╪═════════════════════════
         ║               MART LAYER (Tables)               ║
         ═════════════════════════╪═════════════════════════
                                  │
         ┌────────────────────────┼────────────────────────┐
         ▼                        ▼                        ▼
┌─────────────────┐     ┌───────────────────┐     ┌──────────────────┐
│  daily_revenue  │     │  food_popularity  │     │ cart_abandonment │
│  (CEO Revenue)  │     │  (JSONB Explode)  │     │ (ML Churn Risk)  │
└─────────────────┘     └───────────────────┘     └──────────────────┘
         │                                                 │
         └────────────────────────┬────────────────────────┘
                                  ▼
                    ┌──────────────────────────┐
                    │   ml_user_order_matrix   │
                    │   (ML Collab Filter)     │
                    └──────────────────────────┘
```

---

## 🔬 How dbt Works Internally — Step by Step

Let's trace what happens when Airflow executes `dbt run`:

### Step 1: Compilation & DAG Construction
Before a single query touches PostgreSQL, dbt scans all `.sql` files inside `/dbt/models/` and parses Jinja macros:
- It finds `{{ ref('stg_orders') }}` inside `fact_orders.sql`.
- It builds a topological **Execution Graph (DAG)** to guarantee that Staging Views are created before Fact Tables, and Fact Tables before Marts.

### Step 2: Executing Staging Models as SQL Views (`stg_*`)
Staging models are configured as `materialized='view'`. dbt executes SQL directly against PostgreSQL:
```sql
CREATE OR REPLACE VIEW analytics.stg_orders AS (
    WITH ranked AS (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY order_id, event_type ORDER BY event_timestamp DESC) as rn
        FROM raw.order_events
    )
    SELECT * FROM ranked WHERE rn = 1
);
```
- **Why `ROW_NUMBER()`?** If an order payment fails and is retried, `raw.order_events` might contain two entries for the same `order_id`. Staging deduplicates them dynamically.

### Step 3: Executing Fact Models as Tables (`fact_*`)
Fact models are configured as `materialized='table'`. dbt executes a **CREATE TABLE AS SELECT (CTAS)** query:
```sql
CREATE TABLE analytics.fact_orders AS (
    SELECT
        order_id, user_id, amount, payment_status, order_status,
        ordered_items,                              -- Preserved JSONB array
        jsonb_array_length(ordered_items) AS item_count,
        DATE(event_timestamp)             AS order_date,
        EXTRACT(HOUR FROM event_timestamp) AS order_hour
    FROM analytics.stg_orders
    WHERE event_type = 'order.created' AND payment_status IS NOT NULL
);
```

### Step 4: JSONB Array Exploding in Marts (`food_popularity`)
In MongoDB and `raw.order_events`, items are nested arrays. To compute item-level revenue in `analytics.food_popularity`, dbt uses PostgreSQL's native `jsonb_array_elements()` function:
```sql
WITH exploded AS (
    SELECT order_id, amount,
           jsonb_array_elements(ordered_items) AS item
    FROM analytics.fact_orders WHERE payment_status = 'paid'
)
SELECT
    item->>'foodId'    AS food_id,
    item->>'name'      AS food_name,
    COUNT(*)           AS order_count,
    SUM((item->>'price')::DECIMAL * (item->>'quantity')::INT) AS total_revenue
FROM exploded
GROUP BY 1, 2;
```
- This flattens the nested JSON on-the-fly, transforming document-style data into relational rows for Metabase!

---

## 🔗 How dbt Interacts With Other Components

```
┌───────────────────────────────────────┐
│     Apache Airflow (:8089)            │
│     DAG: foodingo_daily_pipeline       │
└──────────────────┬────────────────────┘
                   │ Triggers nightly: dbt run --select staging/facts/marts
                   ▼
┌───────────────────────────────────────┐
│     dbt CLI (Inside Airflow Worker)   │
│     Configuration: dbt_project.yml    │
└──────────────────┬────────────────────┘
                   │ Executes DDL & SELECT queries via JDBC/psycopg2
                   ▼
┌───────────────────────────────────────┐
│     PostgreSQL 15 (:5432)             │
│     Reads: raw.* schema               │
│     Writes: analytics.* schema        │
└──────────────────┬────────────────────┘
                   │
         ┌─────────┴─────────┐
         │ SQL READ          │ SQL READ
         ▼                   ▼
┌─────────────────┐ ┌───────────────────┐
│ Metabase (:3000)│ │ ML Service (:5001)│
│ CEO BI Dashboards│ │ FastAPI Training │
└─────────────────┘ └───────────────────┘
```

1. **Apache Airflow:** Schedules and executes `dbt run` commands sequentially via `BashOperator` every night at 2:00 AM IST.
2. **PostgreSQL:** Serves as both the input source (`raw` schema) and the destination (`analytics` schema).
3. **Metabase & ML Service:** Consumers that read the resulting mart tables (`daily_revenue`, `ml_user_order_matrix`, `cart_abandonment`).

---

## Project Configuration

### `dbt_project.yml`
```yaml
name: 'foodingo'
profile: 'foodingo'
models:
  foodingo:
    staging:
      +materialized: view      # Always fresh, zero disk space cost
    facts:
      +materialized: table     # Pre-computed for fast BI queries
    marts:
      +materialized: table     # Pre-aggregated for instant Metabase UI loading
```

### `profiles.yml`
```yaml
foodingo:
  target: dev
  outputs:
    dev:
      type: postgres
      host: "{{ env_var('POSTGRES_HOST', 'localhost') }}"
      port: "{{ env_var('POSTGRES_PORT', '5432') | int }}"
      user: "{{ env_var('POSTGRES_USER', 'foodingo') }}"
      password: "{{ env_var('POSTGRES_PASSWORD', 'foodingo123') }}"
      dbname: "{{ env_var('POSTGRES_DB', 'foodingo_warehouse') }}"
      schema: analytics     # Target destination schema
      threads: 2            # Concurrent SQL execution threads
```

---

## 📈 Scalability & Current Configuration

### Current Setup
| Setting | Value | Why |
|---|---|---|
| Materialization | Views (Staging), Tables (Facts/Marts) | Balances storage overhead against BI dashboard query performance |
| Threads | 2 concurrent queries | Suitable for single PostgreSQL Docker container |
| Orchestration | Airflow `BashOperator` | Complete automation without manual CLI intervention |
| Execution Time | ~3 seconds | Extremely fast for current development dataset sizes |

---

## 🚀 Future Scaling Guide

When Foodingo's database grows to millions of orders, implement these enterprise dbt scaling techniques:

### Level 1: Incremental Models (`materialized='incremental'`)
Currently, `fact_orders` drops and recreates the entire table every night (`CREATE TABLE AS`). At 10 million rows, full rebuilds take too long.
- Convert `fact_orders.sql` to an **Incremental Model**:
  ```sql
  {{ config(materialized='incremental', unique_key='order_id') }}
  
  SELECT ... FROM {{ ref('stg_orders') }}
  {% if is_incremental() %}
    -- Only process records newer than the most recent timestamp in the table
    WHERE event_timestamp > (SELECT max(event_timestamp) FROM {{ this }})
  {% endif %}
  ```
- **Impact:** Rebuild time drops from 10 minutes to **10 seconds**, processing only the new orders from that day.

### Level 2: Parallel Thread Tuning (`threads: 4` or `8`)
In `profiles.yml`, increase `threads` from `2` to `4` or `8`.
- Allows dbt to build independent branches of the DAG simultaneously (e.g., building `daily_revenue` and `cart_abandonment` concurrently).

### Level 3: Data Quality Testing (`dbt test`)
In production, prevent bad data from reaching executive dashboards by adding schema tests in a `schema.yml` file:
```yaml
models:
  - name: fact_orders
    columns:
      - name: order_id
        tests:
          - unique
          - not_null
      - name: amount
        tests:
          - dbt_utils.expression_is_true:
              expression: ">= 0" # Orders cannot have negative revenue
```

---

## Useful Commands

```bash
# Execute all dbt models manually inside the Airflow Webserver container
docker exec -it foodingo-airflow-webserver bash -c \
  "POSTGRES_HOST=postgres POSTGRES_PORT=5432 POSTGRES_DB=foodingo_warehouse \
   POSTGRES_USER=foodingo POSTGRES_PASSWORD=foodingo123 \
   dbt run --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt"

# Execute only the staging models
docker exec -it foodingo-airflow-webserver bash -c \
  "dbt run --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt --select staging"

# Execute only the marts models
docker exec -it foodingo-airflow-webserver bash -c \
  "dbt run --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt --select marts"

# Run built-in dbt data tests
docker exec -it foodingo-airflow-webserver bash -c \
  "dbt test --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt"
```

---

## Learn More

- [dbt Official Documentation](https://docs.getdbt.com/)
- [dbt Incremental Models Guide](https://docs.getdbt.com/docs/build/incremental-models)
- [dbt Jinja Macros Reference](https://docs.getdbt.com/docs/build/jinja-macros)
- [PostgreSQL JSONB Functions](https://www.postgresql.org/docs/15/functions-json.html)
