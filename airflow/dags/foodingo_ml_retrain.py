"""
Airflow DAG: foodingo_ml_retrain
==================================
Retrains both ML models (recommender + churn predictor) every Sunday at 3 AM IST.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator

default_args = {
    "owner": "foodingo-data-team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
}

with DAG(
    dag_id="foodingo_ml_retrain",
    description="Weekly ML model retrain — recommender + churn predictor",
    default_args=default_args,
    schedule_interval="30 21 * * 0",   # Sunday 3:00 AM IST = 21:30 UTC Saturday
    start_date=datetime(2026, 7, 22),
    catchup=False,
    tags=["foodingo", "ml", "retrain"],
) as dag:

    retrain_recommender = SimpleHttpOperator(
        task_id="retrain_recommender",
        http_conn_id="ml_service",
        endpoint="/train/recommender",
        method="POST",
        response_check=lambda r: r.json().get("success", False),
        log_response=True,
        extra_options={"timeout": 300},
    )

    retrain_churn = SimpleHttpOperator(
        task_id="retrain_churn_predictor",
        http_conn_id="ml_service",
        endpoint="/train/churn",
        method="POST",
        response_check=lambda r: r.json().get("success", False),
        log_response=True,
        extra_options={"timeout": 300},
    )

    retrain_recommender >> retrain_churn
