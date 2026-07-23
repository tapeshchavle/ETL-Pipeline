# 🌬️ Apache Airflow & dbt Guide

## Overview
Airflow is the "conductor" of this data pipeline. It doesn't process data itself; instead, it schedules and orchestrates other tools to do the work at the right time. 

In our pipeline, Airflow runs on a daily schedule to trigger **dbt** (Data Build Tool), which transforms messy JSON data in our Postgres `raw` tables into beautiful, structured tables in the `analytics` schema. Once dbt finishes, Airflow tells the ML Service to retrain its models.

## Understanding the Airflow UI (`http://localhost:8089`)

### 1. The DAGs Page (Home)
A DAG (Directed Acyclic Graph) is a workflow. You will see a DAG named `foodingo_daily_pipeline`.
- **Toggle Switch (Left):** Unpause the DAG to allow it to run on its schedule.
- **Play Button (Right):** Manually trigger a run of the DAG immediately.
- **Recent Runs:** Shows green circles (Success), red circles (Failed), or light green circles (Running).

### 2. Graph View
Click on the `foodingo_daily_pipeline` name, then click **Graph**. 
This shows the visual dependency chain. You will see something like:
`start_pipeline` ➔ `run_dbt_models` ➔ `retrain_ml_models` ➔ `end_pipeline`
If a task fails, it turns red, and dependent tasks won't run.

### 3. Logs
If a task turns red (fails), click on the red square in the Grid view, and click the **Logs** button. This will show you exactly what command failed (e.g., a SQL syntax error in dbt).

## What is dbt doing?
dbt (Data Build Tool) is executed by Airflow. 
The Python consumer dumps raw JSON events into `raw.order_events`. This is hard for business users to query.
dbt runs SQL `SELECT` statements (called "models") to extract the JSON fields and create clean tables.

For example, it might convert:
`{"userId": "123", "total": 45.00, "status": "completed"}`
into an actual Postgres table row in `analytics.fact_orders`.

## How to Test
1. Click the Play button in the Airflow UI.
2. Watch the DAG run.
3. Open DBeaver or Postgres and query the `analytics` schema to see your newly transformed data!
