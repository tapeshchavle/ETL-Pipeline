# 📊 Metabase (Business Intelligence) Guide

## Overview
Metabase is a fast, open-source Business Intelligence (BI) tool. 
It allows stakeholders, data analysts, and product managers to ask questions about the data and create beautiful dashboards without necessarily needing to write raw SQL.

In our pipeline, Metabase sits at the very end of the data flow. It connects directly to the PostgreSQL `foodingo_warehouse` database, specifically querying the `analytics` schema that was cleaned and prepped by dbt.

## Setting up Metabase (`http://localhost:3000`)
The first time you open Metabase, you must complete the 1-minute setup:
1. Choose a language and create an admin account.
2. **Add your data:**
   - **Database type:** PostgreSQL
   - **Name:** Foodingo Analytics
   - **Host:** `foodingo-postgres` (This is the internal docker network name)
   - **Port:** `5432`
   - **Database name:** `foodingo_warehouse`
   - **Username:** `foodingo`
   - **Password:** `foodingo123`
3. Finish the setup and go to the Home screen.

## How to use Metabase
Metabase allows you to "Ask a Question". You can use a visual query builder or write native SQL.

### Visual Builder (No Code)
1. Click **New -> Question**.
2. Select the `Foodingo Analytics` database and pick the `analytics.fact_orders` table.
3. Click **Summarize**.
4. Group by `order_date` and Sum by `total_amount`.
5. Metabase instantly generates a time-series chart showing revenue over time!

### Native SQL
If you prefer raw SQL, click **New -> SQL query**:
```sql
SELECT food_id, COUNT(*) as order_count 
FROM analytics.fact_orders 
GROUP BY food_id 
ORDER BY order_count DESC 
LIMIT 5;
```
You can save these questions and pin them to a Dashboard to share with your team.
