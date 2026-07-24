# 🐘 PostgreSQL — Analytical Data Warehouse

## Overview

PostgreSQL 15 serves as the **analytical data warehouse** for Foodingo. Unlike MongoDB (which handles live application transactions), PostgreSQL is purpose-built for **analytical SQL queries** — aggregations, joins, window functions, and time-series analysis.

The database is organized into **two schemas**:
- **`raw`** — Raw event data ingested from Kafka (written by the Python consumer)
- **`analytics`** — Cleaned, transformed, business-ready tables (written by dbt)

---

## Schema Architecture

```
┌────────────────────────────────────────────────────────────────┐
│                  foodingo_warehouse Database                    │
│                                                                │
│  ┌──────────────── raw Schema ──────────────────────────────┐  │
│  │                                                          │  │
│  │  user_events    ← user.registered, user.login            │  │
│  │  cart_events    ← cart.item_added, cart.item_removed, ... │  │
│  │  order_events   ← order.created, payment.verified, ...   │  │
│  │  cdc_events     ← foodingo.foodies.* (Debezium CDC)      │  │
│  │                                                          │  │
│  │  Source: Python Kafka Consumer (real-time inserts)        │  │
│  └──────────────────────────────────────────────────────────┘  │
│                           │                                    │
│                    dbt transforms                              │
│                           │                                    │
│  ┌──────────── analytics Schema ────────────────────────────┐  │
│  │                                                          │  │
│  │  STAGING (Views — lightweight, always fresh)              │  │
│  │  ├── stg_orders     — Deduplicated orders                │  │
│  │  ├── stg_cart        — Cleaned cart events                │  │
│  │  └── stg_users       — Cleaned user events               │  │
│  │                                                          │  │
│  │  FACTS (Tables — core business facts)                    │  │
│  │  ├── fact_orders     — One row per paid order             │  │
│  │  └── fact_cart_events — Cleaned cart interactions         │  │
│  │                                                          │  │
│  │  MARTS (Tables — business-specific aggregations)          │  │
│  │  ├── daily_revenue          — Revenue by date            │  │
│  │  ├── food_popularity        — Order count per food       │  │
│  │  ├── cart_abandonment       — Users who didn't convert   │  │
│  │  ├── user_funnel            — Conversion funnel          │  │
│  │  └── ml_user_order_matrix   — ML input matrix            │  │
│  │                                                          │  │
│  │  Source: dbt (runs via Airflow DAG, daily at 2 AM IST)   │  │
│  └──────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────┘
```

---

## Raw Tables (4 tables)

### `raw.user_events`
| Column | Type | Description |
|---|---|---|
| `id` | BIGSERIAL PK | Auto-increment ID |
| `event_type` | VARCHAR(50) | `user.registered` or `user.login` |
| `user_id` | VARCHAR(100) | MongoDB user ID |
| `email` | VARCHAR(255) | User email |
| `name` | VARCHAR(255) | User display name |
| `event_timestamp` | TIMESTAMPTZ | When the event occurred |
| `ingested_at` | TIMESTAMPTZ | When the consumer wrote it |
| `kafka_topic` | VARCHAR(100) | Source Kafka topic |
| `kafka_partition` | INT | Kafka partition number |
| `kafka_offset` | BIGINT | Kafka message offset |

### `raw.order_events`
| Column | Type | Description |
|---|---|---|
| `ordered_items` | **JSONB** | Array of `{foodId, name, price, quantity, category}` |
| `amount` | DECIMAL(10,2) | Order total in INR |
| `payment_status` | VARCHAR(50) | `pending`, `paid`, `failed` |
| `razorpay_order_id` | VARCHAR(200) | Razorpay order reference |
| ... | ... | See `init.sql` for complete schema |

### `raw.cdc_events`
| Column | Type | Description |
|---|---|---|
| `collection` | VARCHAR(100) | MongoDB collection (`orders`, `users`, `food`, `carts`) |
| `operation` | VARCHAR(10) | `c` (create), `u` (update), `d` (delete) |
| `document_id` | VARCHAR(100) | MongoDB `_id` |
| `full_document` | **JSONB** | Complete MongoDB document as JSON |

---

## Analytics Tables (7 tables)

These are **created empty** by `init.sql` (so Metabase can discover them) and **populated by dbt** during each Airflow run.

| Table | Rows Represent | Key Metrics |
|---|---|---|
| `fact_orders` | One paid order | `amount`, `item_count`, `order_date`, `order_hour` |
| `fact_cart_events` | One cart interaction | `event_type`, `food_id`, `quantity` |
| `daily_revenue` | One calendar day | `total_revenue`, `order_count`, `avg_order_value` |
| `food_popularity` | One food item | `order_count`, `total_revenue`, `avg_price` |
| `cart_abandonment` | One user at risk | `days_since_cart`, `has_ordered_after` |
| `user_funnel` | One calendar day | `registered_users` → `ordered_users` conversion |
| `ml_user_order_matrix` | One (user, food) pair | `order_count` (input for ML recommender) |

---

## Performance Indexes

```sql
CREATE INDEX idx_order_events_user_id   ON raw.order_events(user_id);
CREATE INDEX idx_order_events_timestamp ON raw.order_events(event_timestamp);
CREATE INDEX idx_cart_events_user_id    ON raw.cart_events(user_id);
CREATE INDEX idx_user_events_email      ON raw.user_events(email);
CREATE INDEX idx_fact_orders_date       ON analytics.fact_orders(order_date);
CREATE INDEX idx_cart_user_food         ON analytics.fact_cart_events(user_id, food_id);
```

---

## Connection Details

| Setting | Value |
|---|---|
| Host (from Docker) | `postgres` |
| Host (from host machine) | `localhost` |
| Port | `5432` |
| Database | `foodingo_warehouse` |
| Username | `foodingo` |
| Password | `foodingo123` |

---

## Files in This Directory

| File | Purpose |
|---|---|
| `init.sql` | Initializes both schemas, all 8+ tables, and performance indexes on first boot |

---

## Useful Commands

```bash
# Connect to PostgreSQL
docker exec -it foodingo-postgres psql -U foodingo -d foodingo_warehouse

# List all schemas
\dn

# List tables in a schema
\dt raw.*
\dt analytics.*

# Count events
SELECT count(*) FROM raw.order_events;
SELECT count(*) FROM raw.cart_events;

# View analytics data
SELECT * FROM analytics.daily_revenue ORDER BY revenue_date DESC;
SELECT * FROM analytics.food_popularity ORDER BY order_count DESC;
SELECT * FROM analytics.cart_abandonment;
SELECT * FROM analytics.user_funnel;
```

---

## Learn More

- [PostgreSQL Documentation](https://www.postgresql.org/docs/15/)
- [PostgreSQL JSONB Functions](https://www.postgresql.org/docs/15/functions-json.html)
- [DBeaver (GUI Client)](https://dbeaver.io/)
