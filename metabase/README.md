# 📊 Metabase — Business Intelligence Dashboards

## Overview

Metabase is an **open-source Business Intelligence (BI) tool** that connects directly to Foodingo's PostgreSQL data warehouse and provides interactive dashboards, charts, and self-service analytics — **without writing a single line of code**.

It sits at the very end of the data pipeline, consuming the clean, transformed data from the `analytics` schema that dbt builds every night.

---

## Architecture

```
              Airflow (nightly)
                  │
                  ▼ dbt transforms
              PostgreSQL
              analytics schema
                  │
                  │ JDBC Connection
                  ▼
┌─────────────────────────────────────┐
│          Metabase (:3000)            │
│                                     │
│  ┌─── Dashboards ─────────────────┐ │
│  │                                │ │
│  │  📈 Daily Revenue Trend        │ │
│  │  🍕 Food Popularity Chart      │ │
│  │  🛒 Cart Abandonment Report    │ │
│  │  📊 User Conversion Funnel     │ │
│  │  🤖 ML Model Input Summary    │ │
│  │                                │ │
│  └────────────────────────────────┘ │
│                                     │
│  ┌─── Features ───────────────────┐ │
│  │  • SQL Query Builder           │ │
│  │  • Drag-and-drop charting      │ │
│  │  • Scheduled email reports     │ │
│  │  • Role-based access control   │ │
│  │  • Embeddable dashboards       │ │
│  └────────────────────────────────┘ │
└─────────────────────────────────────┘
```

---

## Initial Setup

### Step 1: Open Metabase
Navigate to **http://localhost:3000**

### Step 2: Complete the Welcome Wizard
1. Set your language
2. Create your admin account (name, email, password)
3. **Add your data source:**
   - Database type: **PostgreSQL**
   - Name: `Foodingo Warehouse`
   - Host: `postgres`
   - Port: `5432`
   - Database name: `foodingo_warehouse`
   - Username: `foodingo`
   - Password: `foodingo123`
4. Click **"Save"**

### Step 3: Explore Your Data
Click **"Browse Data"** → **"Foodingo Warehouse"** → **"Analytics"**

You'll see all 7 analytics tables ready to visualize!

---

## Suggested Dashboards

### 1. 📈 Daily Revenue Dashboard
- **Table:** `analytics.daily_revenue`
- **Chart type:** Line chart
- **X-axis:** `revenue_date`
- **Y-axis:** `total_revenue`
- **Bonus:** Add `order_count` as a secondary axis

### 2. 🍕 Food Popularity
- **Table:** `analytics.food_popularity`
- **Chart type:** Bar chart or Pie chart
- **Sort by:** `order_count` descending
- **Group by:** `category` for category-level insights

### 3. 🛒 Cart Abandonment
- **Table:** `analytics.cart_abandonment`
- **Chart type:** Table with conditional formatting
- **Highlight:** Users where `has_ordered_after = false` AND `days_since_cart > 3`

### 4. 📊 User Conversion Funnel
- **Table:** `analytics.user_funnel`
- **Chart type:** Funnel chart or Stacked bar
- **Metrics:** `registered_users` → `logged_in_users` → `cart_added_users` → `ordered_users`

---

## Connection Details

| Setting | Value |
|---|---|
| Metabase URL | http://localhost:3000 |
| Database Type | PostgreSQL |
| Host | `postgres` (Docker internal hostname) |
| Port | `5432` |
| Database | `foodingo_warehouse` |
| Username | `foodingo` |
| Password | `foodingo123` |

---

## Docker Configuration

```yaml
metabase:
  image: metabase/metabase:latest
  container_name: foodingo-metabase
  depends_on:
    postgres:
      condition: service_healthy
  ports:
    - "3000:3000"
  environment:
    MB_DB_TYPE: postgres
    MB_DB_DBNAME: foodingo_warehouse
    MB_DB_PORT: 5432
    MB_DB_USER: foodingo
    MB_DB_PASS: foodingo123
    MB_DB_HOST: postgres
```

> **Note:** `MB_DB_*` variables tell Metabase where to store its own internal metadata (dashboards, users, settings). It also uses this same database for browsing analytics data.

---

## Useful Commands

```bash
# View Metabase logs
docker logs -f foodingo-metabase

# Restart Metabase
docker compose -f docker-compose-pipeline.yml restart metabase
```

---

## Learn More

- [Metabase Documentation](https://www.metabase.com/docs/latest/)
- [Metabase Dashboard Tutorial](https://www.metabase.com/learn/dashboards/creating-dashboards)
- [Metabase SQL Guide](https://www.metabase.com/docs/latest/questions/native-editor/writing-sql)
