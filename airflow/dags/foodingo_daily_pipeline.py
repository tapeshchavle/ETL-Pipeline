"""
Airflow DAG: foodingo_daily_pipeline
=====================================
Runs every day at 2 AM IST (8:30 PM UTC previous day).

Steps:
  1. Run dbt staging models (stg_orders, stg_users, stg_cart)
  2. Run dbt fact models (fact_orders, fact_cart_events)
  3. Run dbt mart models (daily_revenue, food_popularity, cart_abandonment,
                           user_funnel, ml_user_order_matrix)
  4. Trigger ML recommender retrain (POST /train/recommender)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator

# ── DAG Config ────────────────────────────────────────────────────────────────
default_args = {
    "owner": "foodingo-data-team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

DBT_PROJECT_DIR = "/opt/airflow/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt"
POSTGRES_ENV = (
    "POSTGRES_HOST=postgres "
    "POSTGRES_PORT=5432 "
    "POSTGRES_DB=foodingo_warehouse "
    "POSTGRES_USER=foodingo "
    "POSTGRES_PASSWORD=foodingo123"
)

with DAG(
    dag_id="foodingo_daily_pipeline",
    description="Daily dbt transforms + ML model retrain for Foodingo",
    default_args=default_args,
    schedule_interval="30 20 * * *",   # 2:00 AM IST = 20:30 UTC
    start_date=datetime(2026, 7, 22),
    catchup=False,
    tags=["foodingo", "dbt", "pipeline"],
) as dag:

    # ── Step 1: dbt staging layer ─────────────────────────────────────────────
    dbt_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=(
            f"{POSTGRES_ENV} "
            f"dbt run --profiles-dir {DBT_PROFILES_DIR} "
            f"--project-dir {DBT_PROJECT_DIR} "
            "--select staging"
        ),
    )

    # ── Step 2: dbt fact layer ────────────────────────────────────────────────
    dbt_facts = BashOperator(
        task_id="dbt_run_facts",
        bash_command=(
            f"{POSTGRES_ENV} "
            f"dbt run --profiles-dir {DBT_PROFILES_DIR} "
            f"--project-dir {DBT_PROJECT_DIR} "
            "--select facts"
        ),
    )

    # ── Step 3: dbt mart layer ────────────────────────────────────────────────
    dbt_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=(
            f"{POSTGRES_ENV} "
            f"dbt run --profiles-dir {DBT_PROFILES_DIR} "
            f"--project-dir {DBT_PROJECT_DIR} "
            "--select marts"
        ),
    )

    # ── Step 4: dbt tests ─────────────────────────────────────────────────────
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"{POSTGRES_ENV} "
            f"dbt test --profiles-dir {DBT_PROFILES_DIR} "
            f"--project-dir {DBT_PROJECT_DIR}"
        ),
    )

    # ── Step 5: Retrain food recommender ────────────────────────────────────
    retrain_recommender = SimpleHttpOperator(
        task_id="retrain_recommender",
        http_conn_id="ml_service",          # configure in Airflow Connections UI
        endpoint="/train/recommender",
        method="POST",
        response_check=lambda response: response.json().get("success", False),
        log_response=True,
        extra_options={"timeout": 300},     # 5 min timeout for training
    )

    # ── DAG Dependency Chain ──────────────────────────────────────────────────
    dbt_staging >> dbt_facts >> dbt_marts >> dbt_test >> retrain_recommender
