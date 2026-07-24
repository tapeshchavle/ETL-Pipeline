# 🔧 dbt (Data Build Tool) — SQL Transformations

## Overview

dbt (Data Build Tool) is the **transformation layer** of Foodingo's data pipeline. It takes the messy, raw event data in PostgreSQL's `raw` schema and transforms it into clean, business-ready analytics tables in the `analytics` schema — using nothing but **SQL**.

dbt follows a **3-layer architecture**: Staging → Facts → Marts, where each layer builds on the previous one using `{{ ref() }}` references. This creates a clean **DAG (Directed Acyclic Graph)** of dependencies.

---

## Why dbt?

| Requirement | How dbt Solves It |
|---|---|
| **SQL-only transforms** | Data engineers write SQL, not Python/Spark |
| **Version control** | Models are `.sql` files tracked in Git |
| **Dependency management** | `{{ ref('stg_orders') }}` auto-resolves execution order |
| **Incremental builds** | Only rebuild what changed |
| **Testing** | Built-in `dbt test` for data quality checks |
| **Documentation** | Auto-generates docs from schema definitions |

---

## 3-Layer Architecture

```
           RAW SCHEMA (PostgreSQL)
           ┌─────────────────────┐
           │  raw.user_events    │
           │  raw.cart_events    │
           │  raw.order_events   │
           │  raw.cdc_events     │
           └─────────┬───────────┘
                     │
        ═════════════╪═══════════════
        ║   STAGING LAYER (Views)   ║
        ═════════════╪═══════════════
                     │
           ┌─────────▼───────────┐
           │  stg_orders (VIEW)  │──── Deduplicates by (order_id, event_type)
           │  stg_cart   (VIEW)  │──── Cleans cart events, extracts date/hour
           │  stg_users  (VIEW)  │──── Cleans user events
           └─────────┬───────────┘
                     │
        ═════════════╪═══════════════
        ║    FACT LAYER (Tables)    ║
        ═════════════╪═══════════════
                     │
           ┌─────────▼───────────┐
           │  fact_orders (TABLE)│──── One row per paid order
           │  fact_cart   (TABLE)│──── One row per cart interaction
           └─────────┬───────────┘
                     │
        ═════════════╪═══════════════
        ║    MART LAYER (Tables)    ║
        ═════════════╪═══════════════
                     │
           ┌─────────▼───────────────────────┐
           │  daily_revenue       (TABLE)     │──── Revenue metrics per day
           │  food_popularity     (TABLE)     │──── Order count per food item
           │  cart_abandonment    (TABLE)     │──── Users at risk of churning
           │  user_funnel         (TABLE)     │──── Registration→Order conversion
           │  ml_user_order_matrix (TABLE)    │──── Input for ML recommender
           └─────────────────────────────────┘
```

---

## Model Details

### Staging Layer (3 Views)

Views are **not materialized** — they execute on-the-fly, always reflecting the latest raw data.

#### `stg_orders.sql`
- **Source:** `raw.order_events`
- **Logic:** Deduplicates by `(order_id, event_type)` using `ROW_NUMBER()` window function — keeps only the most recent event per order
- **Why?** If a payment is retried, the same `order_id` might appear multiple times

#### `stg_cart.sql`
- **Source:** `raw.cart_events`
- **Logic:** Cleans and standardizes cart events, extracts `event_date` and `event_hour`

#### `stg_users.sql`
- **Source:** `raw.user_events`
- **Logic:** Cleans user registration and login events

### Fact Layer (2 Tables)

#### `fact_orders.sql`
```sql
SELECT
    order_id, user_id, amount, payment_status, order_status,
    ordered_items,                              -- JSONB array kept for downstream
    jsonb_array_length(ordered_items) AS item_count,
    DATE(event_timestamp)            AS order_date,
    EXTRACT(HOUR FROM event_timestamp) AS order_hour
FROM stg_orders
WHERE event_type = 'order.created' AND payment_status IS NOT NULL
```

**Key insight:** `ordered_items` (the JSONB array of food items) is preserved in the fact table so that downstream marts can explode it using `jsonb_array_elements()`.

#### `fact_cart_events.sql`
- Cleans cart interactions with date/hour extraction for time-series analysis

### Mart Layer (5 Tables)

#### `daily_revenue.sql`
Aggregates orders by date → `total_revenue`, `order_count`, `avg_order_value`, `unique_users`

#### `food_popularity.sql`
Explodes `ordered_items` JSONB array → counts orders per food item → `order_count`, `total_revenue`, `avg_price`

```sql
WITH exploded AS (
    SELECT order_id, amount,
           jsonb_array_elements(ordered_items) AS item
    FROM fact_orders WHERE payment_status = 'paid'
)
SELECT
    item->>'foodId'    AS food_id,
    item->>'name'      AS food_name,
    COUNT(*)           AS order_count,
    SUM((item->>'price')::DECIMAL * (item->>'quantity')::INT) AS total_revenue
FROM exploded GROUP BY food_id, food_name
```

#### `cart_abandonment.sql`
Identifies users who added to cart but didn't place an order → `days_since_cart`, `has_ordered_after`

#### `user_funnel.sql`
Daily conversion funnel: `registered_users` → `logged_in_users` → `cart_added_users` → `ordered_users` with `reg_to_order_rate` percentage

#### `ml_user_order_matrix.sql`
Creates a user × food matrix: `(user_id, food_id, order_count)` — this is the direct input for the ML recommender's collaborative filtering algorithm

---

## Configuration

### `dbt_project.yml`
```yaml
name: 'foodingo'
profile: 'foodingo'
models:
  foodingo:
    staging:
      +materialized: view      # Always fresh, no storage cost
    facts:
      +materialized: table     # Pre-computed for fast queries
    marts:
      +materialized: table     # Pre-computed for dashboards + ML
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
      schema: analytics     # All models go into the analytics schema
      threads: 2
```

---

## dbt DAG (Dependency Graph)

```
stg_orders ─────┐
                ├──▶ fact_orders ─────┬──▶ daily_revenue
stg_users ──────┤                    ├──▶ food_popularity
                │                    ├──▶ ml_user_order_matrix
                │                    │
stg_cart ───────┴──▶ fact_cart_events ├──▶ cart_abandonment
                                     └──▶ user_funnel
```

---

## Directory Structure

```
dbt/
├── dbt_project.yml          # Project configuration
├── profiles.yml             # PostgreSQL connection profile
├── models/
│   ├── staging/
│   │   ├── stg_orders.sql   # Deduplicated orders view
│   │   ├── stg_cart.sql     # Cleaned cart events view
│   │   └── stg_users.sql    # Cleaned user events view
│   ├── facts/
│   │   ├── fact_orders.sql  # Core order fact table
│   │   └── fact_cart_events.sql  # Core cart fact table
│   └── marts/
│       ├── daily_revenue.sql         # Revenue by date
│       ├── food_popularity.sql       # Order count per food
│       ├── cart_abandonment.sql      # Churn risk users
│       ├── user_funnel.sql           # Conversion funnel
│       └── ml_user_order_matrix.sql  # ML input matrix
├── seeds/                   # (empty — static data would go here)
├── tests/                   # (empty — custom tests would go here)
└── macros/                  # (empty — reusable SQL macros would go here)
```

---

## Running dbt Manually

```bash
# Run all models
docker exec -i foodingo-airflow-webserver bash -c \
  "POSTGRES_HOST=postgres POSTGRES_PORT=5432 POSTGRES_DB=foodingo_warehouse \
   POSTGRES_USER=foodingo POSTGRES_PASSWORD=foodingo123 \
   dbt run --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt"

# Run only staging models
dbt run --select staging

# Run only marts
dbt run --select marts

# Run tests
dbt test --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt

# Generate documentation
dbt docs generate
dbt docs serve
```

---

## Learn More

- [dbt Documentation](https://docs.getdbt.com/)
- [dbt Best Practices](https://docs.getdbt.com/guides/best-practices)
- [Jinja Templating in dbt](https://docs.getdbt.com/docs/build/jinja-macros)
- [PostgreSQL JSONB Functions](https://www.postgresql.org/docs/15/functions-json.html)
