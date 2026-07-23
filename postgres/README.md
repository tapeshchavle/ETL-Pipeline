# 🐘 PostgreSQL Data Warehouse Guide

## Overview
PostgreSQL acts as our central Data Warehouse (OLAP). While MongoDB handles the fast, user-facing transactions, Postgres handles the heavy analytical lifting for business intelligence and machine learning.

## Database Schemas
Inside the `foodingo_warehouse` database, we use schemas to organize data logically based on its stage in the pipeline (a Medallion Architecture approach).

1. **`raw` Schema (Bronze):**
   - Contains raw, messy, nested JSON data directly ingested from Kafka.
   - Tables: `cart_events`, `order_events`, `cdc_events`.
   - Populated by: The Python Kafka Consumer.
2. **`analytics` Schema (Gold):**
   - Contains clean, structured, heavily typed tables optimized for querying.
   - Tables: `fact_orders`, `daily_revenue`, `food_popularity`.
   - Populated by: Apache Airflow & dbt.

## Important Scripts
- `init.sql`: Runs automatically when the container boots. It creates the databases, users, schemas, and the initial `raw` tables required by the Kafka consumer.

## How to Connect
You can connect to this data warehouse using **DBeaver** or **pgAdmin**:
- **Host:** `localhost`
- **Port:** `5432`
- **Database:** `foodingo_warehouse`
- **Username:** `foodingo`
- **Password:** `foodingo123`
