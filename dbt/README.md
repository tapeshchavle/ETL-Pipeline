# 🏭 dbt (Data Build Tool) Guide

## Overview
dbt is the "T" in ELT (Extract, Load, Transform). 
Instead of writing complex Python scripts to transform data, dbt allows data analysts to write simple SQL `SELECT` statements, and it automatically handles turning those statements into fully structured tables and views in the data warehouse.

## How it fits in our pipeline
1. The **Kafka Python Consumer** extracts and loads (EL) raw JSON events into the `raw` schema in PostgreSQL.
2. **Airflow** triggers `dbt run` daily.
3. **dbt** executes the SQL files inside the `models/` directory (e.g., `fact_orders.sql`, `daily_revenue.sql`).
4. It reads from the `raw` schema, cleans the data, casts types, and writes the final tables into the `analytics` schema in PostgreSQL.

## Folder Structure
- `models/`: Contains the SQL files that define our transformations.
- `dbt_project.yml`: The main configuration file telling dbt where the models are and how to connect to the database.

## Testing locally
If you want to manually run dbt without waiting for Airflow, you can run:
```bash
# Exec into the airflow container where dbt is installed
docker exec -it foodingo-airflow-scheduler /bin/bash
cd dbt
dbt run
```
