# 🌀 Apache Airflow — Workflow & Pipeline Orchestration

## Overview

Apache Airflow 2.9.1 is the **master workflow orchestrator** for Foodingo's data engineering and machine learning pipeline. 

Instead of engineers manually executing SQL scripts, PySpark jobs, or ML training commands, Airflow schedules and monitors the entire end-to-end batch processing pipeline as a **Directed Acyclic Graph (DAG)** every night at **2:00 AM IST (20:30 UTC)**.

---

## Why Apache Airflow?

| Requirement | How Airflow Solves It |
|---|---|
| **Dependency Guarantee** | Ensures ML models *never* train until dbt transformations and Spark jobs finish successfully |
| **Automatic Retry** | Transient database or network failures auto-retry up to 2 times with a 5-minute backoff |
| **Multi-Engine Glue** | Seamlessly orchestrates PostgreSQL SQL (`dbt`), AWS S3 (`PySpark`), and Python HTTP APIs (`FastAPI`) |
| **Visual Monitoring UI** | Executive dashboard showing task durations, logs, success rates, and historical timelines |
| **Backfill Capability** | Can re-run historical DAG runs across arbitrary date ranges with a single click |

---

## Complete Orchestration DAG in Foodingo

```
┌──────────────────┐     ┌────────────────┐     ┌───────────────┐
│ dbt_run_staging  │───► │ dbt_run_facts  │───► │ dbt_run_marts │
│                  │     │                │     │               │
│ (BashOperator)   │     │ (BashOperator) │     │ (BashOperator)│
│  --select staging│     │  --select facts│     │  --select marts
└──────────────────┘     └────────────────┘     └───────┬───────┘
                                                        │
                                                        ▼
┌─────────────────────────────────┐             ┌───────────────┐
│       retrain_recommender       │ ◄───────────│   dbt_test    │
│                                 │             │               │
│      (SimpleHttpOperator)       │             │ (BashOperator)│
│  POST http://ml-service:5001/   │             │   dbt test    │
│       train/recommender         │             └───────────────┘
└────────────────┬────────────────┘
                 │
                 ▼
┌─────────────────────────────────┐
│       trigger_spark_job         │  ◄── (Optional / Lambda Architecture Cold Path)
│                                 │
│         (BashOperator)          │
│  docker exec foodingo-spark-... │
│  python data_lake_analyzer.py   │
└─────────────────────────────────┘
```

---

## 🔬 How Airflow Works Internally — Step by Step

Let's examine how Airflow executes `foodingo_daily_pipeline.py`:

### Step 1: Scheduler Parsing Loop
- Every **30 seconds**, the `foodingo-airflow-scheduler` container scans the `/opt/airflow/dags/` folder.
- It parses Python script files, evaluates topological dependencies (`>>`), and registers DAG execution schedules in Airflow's internal PostgreSQL metadata database (`airflow-db`).

### Step 2: Triggering Task 1 (`dbt_run_staging`)
- At 2:00 AM IST, the Scheduler creates a **DAG Run** and assigns the first task (`dbt_run_staging`) to the **`LocalExecutor`**.
- Because Airflow is configured with `LocalExecutor`, it spawns a local Linux subprocess inside the scheduler container.
- It injects PostgreSQL environment variables (`POSTGRES_HOST=postgres`, etc.) and executes:
  ```bash
  dbt run --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt --select staging
  ```
- dbt executes SQL views in PostgreSQL. When the subprocess exits with return code `0`, Airflow marks the task **SUCCESS (Green)**.

### Step 3: Sequential Downstream Propagation
- With `dbt_run_staging` complete, Airflow unlocks `dbt_run_facts`, followed by `dbt_run_marts`, and `dbt_test`.
- If `dbt_test` fails (for example, if a data quality check detects null `order_id` values), **execution stops immediately**. Downstream ML training tasks are aborted to prevent bad data from poisoning recommendation models!

### Step 4: Triggering the ML Service via HTTP
- Once dbt tests pass, Airflow executes `retrain_recommender` using a **`SimpleHttpOperator`**:
  ```python
  retrain_recommender = SimpleHttpOperator(
      task_id="retrain_recommender",
      http_conn_id="ml_service_api",   # Points to http://ml-service:5001
      endpoint="/train/recommender",
      method="POST",
      response_check=lambda response: response.json().get("success") == True,
  )
  ```
- Airflow sends an HTTP `POST` request to the FastAPI ML Service over Docker's internal network.
- It waits for the JSON response `{"success": true}` before marking the task complete.

### Step 5: Triggering Spark Big Data Analytics (Lambda Architecture)
- For heavy historical Big Data jobs, Airflow triggers a PySpark job via `BashOperator` or `SparkSubmitOperator`:
  ```python
  trigger_spark = BashOperator(
      task_id="trigger_spark_job",
      bash_command="docker exec foodingo-spark-jupyter python /home/jovyan/work/spark/data_lake_analyzer.py",
  )
  ```
- This bridges the **Cold Path**, instructing Apache Spark to scan S3 Parquet files and write summary metrics back into PostgreSQL for Metabase.

---

## 🔗 How Airflow Interacts With Other Components

```
┌────────────────────────────────────────────────────────┐
│             Apache Airflow (:8089)                     │
│  • Webserver (:8089)                                   │
│  • Scheduler (LocalExecutor)                           │
│  • Metadata DB (PostgreSQL internal :5432)             │
└───────────┬──────────────────────────────┬─────────────┘
            │                              │
            │ 1. Executes Bash commands    │ 2. Sends HTTP POST
            ▼                              ▼
┌───────────────────────┐      ┌─────────────────────────┐
│ dbt CLI / Spark CLI   │      │ FastAPI ML Service      │
│                       │      │ (:5001)                 │
│ • Runs SQL on Postgres│      │                         │
│ • Scans S3 Parquet    │      │ • Re-builds .pkl models │
└───────────────────────┘      └─────────────────────────┘
```

---

## Accessing the Airflow Web UI

| Setting | Value |
|---|---|
| URL | **http://localhost:8089** |
| Username | `admin` |
| Password | `admin` |

### How to Trigger a Manual Pipeline Run
1. Navigate to **http://localhost:8089**.
2. Locate **`foodingo_daily_pipeline`** in the DAG list.
3. Ensure the left toggle switch is **ON (Unpaused)**.
4. Click the **▶ (Play)** button on the right and select **"Trigger DAG"**.
5. Click on the DAG name and select **"Graph"** to watch tasks turn **Green** in real-time!

---

## 📈 Scalability & Current Configuration

### Current Setup
| Setting | Value | Why |
|---|---|---|
| Airflow Version | 2.9.1 | Stable modern release with improved UI and scheduling speed |
| Executor | `LocalExecutor` | Runs tasks on local CPU cores; zero broker setup required |
| Metadata Store | PostgreSQL 15 | Dedicated internal database (`airflow-db`) |
| Package Injection | `_PIP_ADDITIONAL_REQUIREMENTS` | Auto-installs `dbt-postgres==1.7.9` inside Airflow containers |

---

## 🚀 Future Scaling Guide

When Foodingo scales to dozens of complex daily pipelines and hundreds of tasks, upgrade Airflow's execution architecture:

### Level 1: `CeleryExecutor` (Distributed Workers)
When a single `LocalExecutor` machine runs out of CPU/RAM during concurrent dbt and Spark executions:
- Switch to **`CeleryExecutor`** with **Redis** or **RabbitMQ** as a message broker.
- Deploy multiple independent **Airflow Worker** containers across separate physical servers.
- The Scheduler pushes tasks to Redis; Workers pick up tasks asynchronously and execute them in parallel.

### Level 2: `KubernetesExecutor` (Cloud Native Auto-Scaling)
In an enterprise Kubernetes deployment (EKS/GKE):
- Switch to **`KubernetesExecutor`**.
- Instead of static worker containers, Airflow spawns a **brand-new Kubernetes Pod** for each individual task (`dbt_run_staging`, `retrain_recommender`), executes the command, and destroys the pod immediately upon completion.
- Results in zero wasted idle compute resources and infinite horizontal scaling.

### Level 3: Automated Slack / PagerDuty Alerting
In `default_args`, configure automated failure notifications:
```python
default_args = {
    "owner": "foodingo-data-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": slack_failed_alert_callback,  # Post stack trace to Slack channel
}
```

---

## Directory Structure

```
airflow/
├── README.md
└── dags/
    ├── foodingo_daily_pipeline.py   # Master DAG: dbt SQL transforms + ML retraining + Spark
    └── foodingo_ml_retrain.py       # Dedicated weekly ML retraining DAG
```

---

## Useful Commands

```bash
# Follow live Airflow Scheduler logs
docker logs -f foodingo-airflow-scheduler

# List all discovered DAGs from command line
docker exec -it foodingo-airflow-scheduler airflow dags list

# Trigger a DAG execution headlessly
docker exec -it foodingo-airflow-scheduler airflow dags trigger foodingo_daily_pipeline

# Test a specific task in isolation (without running the whole DAG)
docker exec -it foodingo-airflow-scheduler airflow tasks test foodingo_daily_pipeline dbt_run_staging 2026-07-26

# Check the execution status of a task
docker exec -it foodingo-airflow-scheduler airflow tasks state foodingo_daily_pipeline dbt_run_staging 2026-07-26
```

---

## Learn More

- [Apache Airflow Official Documentation](https://airflow.apache.org/docs/)
- [Airflow DAG Fundamentals Tutorial](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/fundamentals.html)
- [Airflow + dbt Production Deployment Guide](https://docs.getdbt.com/docs/deploy/deployment-tools#airflow)
- [Airflow Executors Explained](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/index.html)
