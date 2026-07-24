# 🌀 Apache Airflow — Workflow Orchestration

## Overview

Apache Airflow 2.9.1 is the **workflow orchestrator** for Foodingo's data pipeline. It schedules and monitors the daily execution of dbt transformations and ML model retraining. Instead of manually running dbt commands, Airflow automates the entire process as a **DAG (Directed Acyclic Graph)** that runs every night at 2:00 AM IST.

---

## Why Airflow?

| Requirement | How Airflow Solves It |
|---|---|
| **Scheduled execution** | Cron-like scheduling with timezone support |
| **Dependency management** | Tasks run in strict order (staging → facts → marts → test → ML) |
| **Retry logic** | Failed tasks auto-retry up to 2 times with 5-minute delay |
| **Monitoring UI** | Visual dashboard showing task status, logs, and history |
| **Alerting** | Can send emails/Slack on failure (configurable) |
| **Backfill** | Can re-run historical DAG runs if needed |

---

## DAGs

### 1. `foodingo_daily_pipeline` (Primary DAG)

**Schedule:** Every day at 2:00 AM IST (20:30 UTC)

```
┌─────────────────┐    ┌────────────────┐    ┌───────────────┐
│ dbt_run_staging  │───▶│ dbt_run_facts  │───▶│ dbt_run_marts │
│                  │    │                │    │               │
│ BashOperator     │    │ BashOperator   │    │ BashOperator  │
│ --select staging │    │ --select facts │    │ --select marts│
└─────────────────┘    └────────────────┘    └───────┬───────┘
                                                      │
                                                      ▼
                        ┌─────────────────┐    ┌──────────────────────┐
                        │ dbt_test        │───▶│ retrain_recommender  │
                        │                 │    │                      │
                        │ BashOperator    │    │ SimpleHttpOperator   │
                        │ dbt test        │    │ POST /train/         │
                        │                 │    │   recommender        │
                        └─────────────────┘    └──────────────────────┘
```

**Task Details:**

| Task ID | Operator | What It Does |
|---|---|---|
| `dbt_run_staging` | BashOperator | Runs `dbt run --select staging` — refreshes 3 staging views |
| `dbt_run_facts` | BashOperator | Runs `dbt run --select facts` — rebuilds fact tables |
| `dbt_run_marts` | BashOperator | Runs `dbt run --select marts` — rebuilds 5 mart tables |
| `dbt_test` | BashOperator | Runs `dbt test` — validates data quality |
| `retrain_recommender` | SimpleHttpOperator | `POST http://ml-service:5001/train/recommender` — retrains ML model |

**Configuration:**
```python
default_args = {
    "owner": "foodingo-data-team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}
```

### 2. `foodingo_ml_retrain` (ML Retraining DAG)

**Schedule:** Weekly (configurable)

Dedicated DAG for ML model retraining, separate from the daily dbt pipeline for flexibility.

---

## Airflow Architecture in Docker

```
┌─────────────────────────────────────────────────┐
│                Airflow Components                 │
│                                                   │
│  ┌──────────────────────────────────────┐        │
│  │  airflow-db (PostgreSQL 15)          │        │
│  │  Stores: DAG metadata, task history, │        │
│  │          connections, variables      │        │
│  │  Port: internal only                 │        │
│  └──────────────────────────────────────┘        │
│                    │                              │
│  ┌─────────────────▼─────────────────────┐       │
│  │  airflow-init (one-shot)              │       │
│  │  Runs: db migrate + create admin user │       │
│  └───────────────────────────────────────┘       │
│                    │                              │
│    ┌───────────────┼───────────────┐             │
│    ▼                               ▼             │
│  ┌──────────────────┐  ┌──────────────────────┐  │
│  │ airflow-webserver │  │ airflow-scheduler    │  │
│  │ Port: 8089        │  │ Executes DAG tasks   │  │
│  │ UI Dashboard      │  │ LocalExecutor        │  │
│  │                   │  │                      │  │
│  │ Volumes:          │  │ Volumes:             │  │
│  │ ./airflow/dags    │  │ ./airflow/dags       │  │
│  │ ./dbt             │  │ ./dbt                │  │
│  │ airflow-logs      │  │ airflow-logs         │  │
│  │                   │  │                      │  │
│  │ pip install:      │  │ pip install:         │  │
│  │ dbt-postgres      │  │ dbt-postgres         │  │
│  └──────────────────┘  └──────────────────────┘  │
└─────────────────────────────────────────────────┘
```

**Key Design Decision:** dbt is installed via `_PIP_ADDITIONAL_REQUIREMENTS: "dbt-postgres==1.7.9"` inside both the webserver and scheduler containers. The dbt project files are mounted at `/opt/airflow/dbt`.

---

## Accessing the Airflow UI

| Setting | Value |
|---|---|
| URL | **http://localhost:8089** |
| Username | `admin` |
| Password | `admin` |

### What You'll See

1. **DAGs Page** — List of all DAGs with status indicators (green=success, red=failure, yellow=running)
2. **Graph View** — Visual representation of task dependencies
3. **Tree View** — Historical run timeline
4. **Task Logs** — Detailed logs for each individual task execution

### How to Trigger a Manual Run

1. Go to http://localhost:8089
2. Find `foodingo_daily_pipeline` in the DAGs list
3. Toggle the switch to **ON** (unpause)
4. Click the **▶ Play** button on the right
5. Select "Trigger DAG" from the dropdown
6. Watch the tasks turn green one by one!

---

## Environment Variables Passed to dbt

The DAG passes PostgreSQL credentials as environment variables before each dbt command:

```python
POSTGRES_ENV = (
    "POSTGRES_HOST=postgres "
    "POSTGRES_PORT=5432 "
    "POSTGRES_DB=foodingo_warehouse "
    "POSTGRES_USER=foodingo "
    "POSTGRES_PASSWORD=foodingo123"
)

dbt_staging = BashOperator(
    task_id="dbt_run_staging",
    bash_command=f"{POSTGRES_ENV} dbt run --profiles-dir /opt/airflow/dbt "
                 f"--project-dir /opt/airflow/dbt --select staging",
)
```

---

## Directory Structure

```
airflow/
├── README.md
└── dags/
    ├── foodingo_daily_pipeline.py   # Primary DAG (daily dbt + ML retrain)
    └── foodingo_ml_retrain.py       # ML-specific retraining DAG
```

---

## Troubleshooting

### DAG tasks fail with "dbt not found"
Ensure `_PIP_ADDITIONAL_REQUIREMENTS: "dbt-postgres==1.7.9"` is set in both `airflow-webserver` and `airflow-scheduler` in `docker-compose-pipeline.yml`.

### DAG tasks fail with "Connection refused"
The dbt command needs `POSTGRES_HOST=postgres` (Docker hostname), not `localhost`. The DAG hardcodes this correctly.

### DAG doesn't appear in the UI
Wait 30 seconds for the scheduler to parse the DAGs directory. Check scheduler logs: `docker logs foodingo-airflow-scheduler`

---

## Useful Commands

```bash
# View Airflow scheduler logs
docker logs -f foodingo-airflow-scheduler

# Trigger DAG from CLI
docker exec -i foodingo-airflow-scheduler airflow dags trigger foodingo_daily_pipeline

# List all DAGs
docker exec -i foodingo-airflow-scheduler airflow dags list

# Check task status
docker exec -i foodingo-airflow-scheduler airflow tasks state foodingo_daily_pipeline dbt_run_staging <execution_date>
```

---

## Learn More

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Airflow DAG Tutorial](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/fundamentals.html)
- [Airflow + dbt Integration Guide](https://docs.getdbt.com/docs/deploy/deployment-tools#airflow)
