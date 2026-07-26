# 📊 Metabase — Executive Business Intelligence & Analytics Dashboards

## Overview

Metabase is an open-source **Business Intelligence (BI) and Data Visualization platform** that connects directly to Foodingo's PostgreSQL data warehouse (`foodingo_warehouse`). 

It sits at the very end of our analytical pipeline, allowing the CEO, Product Managers, and Marketing Teams to build interactive dashboards, inspect customer funnels, and monitor revenue trends **without writing a single line of code**.

---

## Why Metabase?

| Requirement | How Metabase Solves It |
|---|---|
| **Zero-Code Exploration** | Drag-and-drop query builder allows non-technical users to filter and chart data |
| **Native PostgreSQL JDBC** | Connects directly to our cleaned dbt `analytics` schema |
| **Interactive Dashboards** | Combines multiple charts (Revenue, Food Popularity, Churn Risk) on a single screen |
| **Scheduled Reports** | Can email daily or weekly PDF/image dashboard summaries to stakeholders |
| **Embedded BI** | Dashboards can be securely embedded inside customer-facing web apps |

---

## Architecture & Data Flow in Foodingo

```
┌────────────────────────────────────────────────────────────────────────┐
│                        Python Kafka Consumer                           │
│        (Ingests real-time events into PostgreSQL raw schema)           │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                                    │ Nightly dbt Transformations
                                    ▼ (Scheduled by Airflow at 2 AM IST)
┌────────────────────────────────────────────────────────────────────────┐
│                        PostgreSQL 15 (:5432)                           │
│                     Database: foodingo_warehouse                       │
│                                                                        │
│  analytics Schema (Clean Dimensional Tables):                          │
│  ├── daily_revenue         ──► Date, Total Revenue, Order Count        │
│  ├── food_popularity       ──► Food ID, Name, Total Orders, Revenue    │
│  ├── cart_abandonment      ──► User ID, Days Since Cart, Churn Risk    │
│  ├── user_funnel           ──► Reg ──► Login ──► Cart ──► Paid Funnel  │
│  └── ml_user_order_matrix  ──► Input matrix for ML collaborative filter│
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                                    │ JDBC / SQL READ
                                    ▼ (Sub-second query response)
┌────────────────────────────────────────────────────────────────────────┐
│                         Metabase Server (:3000)                        │
│                   Container: foodingo-metabase                         │
│                                                                        │
│  ┌─────────────────────── Executive Dashboards ─────────────────────┐  │
│  │  📈 Daily Revenue Trend (Line Chart)                             │  │
│  │  🍕 Food Popularity Ranking (Bar / Pie Chart)                    │  │
│  │  🛒 Cart Abandonment Recovery List (Conditional Table)           │  │
│  │  📊 User Registration-to-Order Funnel (Funnel Visualization)     │  │
│  └──────────────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 🔬 How Metabase Works Internally — Step by Step

### Step 1: Connecting via PostgreSQL JDBC Driver
When `foodingo-metabase` starts, it uses the Java JDBC driver to connect to `postgres:5432/foodingo_warehouse`. It authenticates using the `MB_DB_*` credentials defined in `docker-compose-pipeline.yml`.

### Step 2: Automatic Schema Discovery
Once connected, Metabase performs an automatic inspection of the `analytics` schema:
- It catalogs all tables (`daily_revenue`, `food_popularity`, etc.) and columns.
- It detects foreign keys, timestamps, and numeric types automatically.

### Step 3: Translating Visual Selections to SQL
When the CEO drags "Total Revenue" and groups by "Revenue Date" in the UI:
- Metabase internally compiles a PostgreSQL-optimized SQL query:
  ```sql
  SELECT revenue_date, SUM(total_revenue) AS sum_revenue
  FROM analytics.daily_revenue
  GROUP BY revenue_date
  ORDER BY revenue_date ASC;
  ```
- Because dbt already pre-aggregated `daily_revenue` overnight, the query executes in **< 10 milliseconds**!

---

## 🔗 How Spark Big Data Results Reach the CEO in Metabase

### The "Write-Back to PostgreSQL" Pattern (Lambda Architecture)
One of the most powerful patterns in Foodingo's Data Lake is how **Apache Spark** calculations appear in Metabase:

```
┌───────────────────────────┐     ┌───────────────────────────┐
│ AWS S3 / MinIO Data Lake  │     │ Apache Spark Cluster      │
│ (Billions of raw Parquet  │ ──► │ (Performs heavy Big Data  │
│  historical event files)  │     │  aggregations on S3)      │
└───────────────────────────┘     └─────────────┬─────────────┘
                                                │
                                                │ Writes summary table via JDBC
                                                ▼
                                  ┌───────────────────────────┐
                                  │ PostgreSQL (:5432)        │
                                  │ analytics.spark_summary   │
                                  └─────────────┬─────────────┘
                                                │
                                                │ Metabase queries Postgres
                                                ▼
                                  ┌───────────────────────────┐
                                  │ Metabase CEO Dashboard    │
                                  │ (Instant response times!) │
                                  └───────────────────────────┘
```

1. **Why not connect Metabase directly to S3?** Scanning terabytes of Parquet files in real-time while an executive clicks around a dashboard would cause 30–60 second loading delays.
2. **The Solution:**
   - At 2:00 AM IST, Apache Airflow triggers an **Apache Spark script**.
   - Spark crunches billions of rows directly from S3 Parquet files in distributed memory.
   - Spark writes a **compact summarized results table** (e.g., 5-year customer lifetime value cohort metrics) back into PostgreSQL's `analytics` schema via JDBC.
   - When the CEO opens Metabase at 9:00 AM, the dashboard loads instantly from PostgreSQL!

---

## Suggested Executive Dashboards

### 1. 📈 Daily Revenue & Order Growth
- **Table:** `analytics.daily_revenue`
- **Visualization:** Dual-axis Line Chart
- **X-Axis:** `revenue_date`
- **Y-Axis 1 (Primary):** `total_revenue`
- **Y-Axis 2 (Secondary):** `order_count`

### 2. 🍕 Top Selling Food Items
- **Table:** `analytics.food_popularity`
- **Visualization:** Horizontal Bar Chart
- **Metric:** `order_count` sorted descending
- **Group By:** `food_name` or `category`

### 3. 🛒 Cart Abandonment Recovery Target List
- **Table:** `analytics.cart_abandonment`
- **Visualization:** Table with Conditional Formatting
- **Filter:** `has_ordered_after = false` AND `days_since_cart >= 3`
- **Action:** Marketing team exports this list to send 20% discount coupon codes.

### 4. 📊 End-to-End Customer Conversion Funnel
- **Table:** `analytics.user_funnel`
- **Visualization:** Funnel Chart
- **Steps:** `registered_users` ──► `logged_in_users` ──► `cart_added_users` ──► `ordered_users`

---

## Accessing & Setting Up Metabase

| Setting | Value |
|---|---|
| Dashboard URL | **http://localhost:3000** |
| Database Host | `postgres` *(Docker internal hostname)* |
| Port | `5432` |
| Database Name | `foodingo_warehouse` |
| Username | `foodingo` |
| Password | `foodingo123` |

---

## 📈 Scalability & Current Configuration

### Current Setup
| Setting | Value | Why |
|---|---|---|
| Metabase Image | `metabase/metabase:latest` | Feature-complete open-source BI release |
| Internal Metadata DB | PostgreSQL (`foodingo_warehouse`) | Stores dashboards, users, and queries in Postgres rather than ephemeral H2 files |
| Query Caching | Enabled (Default) | Reduces load on PostgreSQL during frequent dashboard views |

---

## 🚀 Future Scaling Guide

When Foodingo scales to hundreds of internal employees viewing dashboards simultaneously:

### Level 1: Read-Replica Connection Routing
If heavy Metabase BI queries start slowing down real-time Kafka Consumer ingestion:
- Provision a **PostgreSQL Read Replica** (e.g., AWS RDS Read Replica).
- Update Metabase's Data Source setting to point to `postgres-read-replica:5432` instead of the primary write database.

### Level 2: Query Result Caching & TTL Tuning
In Metabase Admin Settings → Caching:
- Configure a **Minimum Query Execution Time** threshold for caching (e.g., cache results for queries taking > 3 seconds).
- Set Cache TTL to **24 hours**, aligning with Airflow's nightly 2:00 AM IST dbt transformation refresh cycle.

### Level 3: Embedding Dashboards in React Frontend (`Embedded BI`)
To let Foodingo restaurant partners view their own sales analytics inside their vendor portal:
- Use **Metabase Interactive Embedding** (JWT-based SSO).
- Safely embed customer-specific filtered dashboards directly into the web app without exposing the entire BI backend.

---

## Useful Commands

```bash
# View live Metabase server container logs
docker logs -f foodingo-metabase

# Restart the Metabase container after configuration changes
docker compose -f docker-compose-pipeline.yml restart metabase

# Check if Metabase can communicate with PostgreSQL inside Docker network
docker exec -it foodingo-metabase bash -c "nc -zv postgres 5432"
```

---

## Learn More

- [Metabase Official Documentation](https://www.metabase.com/docs/latest/)
- [Creating Metabase Dashboards Tutorial](https://www.metabase.com/learn/dashboards/creating-dashboards)
- [Writing Native SQL Queries in Metabase](https://www.metabase.com/docs/latest/questions/native-editor/writing-sql)
- [Metabase SSO & Embedding Guide](https://www.metabase.com/docs/latest/embedding/interactive-embedding)
