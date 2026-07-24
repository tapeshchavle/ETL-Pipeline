# ⚡ Apache Spark — Big Data Analytics Cluster

## Overview

Apache Spark is the **distributed computing engine** for Foodingo's Data Lake. While PostgreSQL and dbt handle everyday business intelligence and operational reporting (the "Hot Path"), Apache Spark is designed to process massive volumes of historical data directly from MinIO (the "Cold/Big Data Path").

By adding Spark, Foodingo implements a true **Lambda Architecture**, separating real-time analytics from heavy batch processing and machine learning.

---

## Architecture

```
┌────────────────────────────────────────────────────────┐
│             Apache Spark Cluster (Docker)              │
│                                                        │
│  ┌─────────────────┐                                   │
│  │  spark-jupyter  │ (JupyterLab UI — Port 8888)       │
│  │  (PySpark env)  │ Runs Spark in local[*] mode       │
│  └─────────────────┘                                   │
└────────────────────────────────────────────────────────┘
           │
           │ Read/Write Parquet via s3a://
           ▼
┌────────────────────────────────────────────────────────┐
│          MinIO (S3) — foodingo-data-lake               │
└────────────────────────────────────────────────────────┘
```

---

## Components

1. **Spark Jupyter (`jupyter/pyspark-notebook`)**: An interactive data science environment pre-configured with PySpark and the AWS S3/MinIO drivers. We run Spark here in "Local Cluster Mode" (`local[*]`) to bypass complex Master/Worker network configuration while still doing parallel big data aggregation across your CPU cores.

---

## Accessing the UIs

| Service | URL | Default Password / Token |
|---|---|---|
| **JupyterLab** | [http://localhost:8888](http://localhost:8888) | `foodingo123` |
| **Spark Job UI** | [http://localhost:4040](http://localhost:4040) | (Only active when a Spark job is running) |

---

## Running the Sample Script

I have provided a sample script `data_lake_analyzer.py` that demonstrates how to connect Spark to MinIO and read the Kafka Consumer's Parquet files.

### Option 1: Via Jupyter Notebook (Interactive UI)
1. Go to **http://localhost:8888** and log in with token `foodingo123`.
2. Navigate to the `spark` folder on the left sidebar.
3. Open a new Terminal or Notebook inside Jupyter and run the script!

### Option 2: Via Docker CLI (Headless)
You can submit the job directly to the Spark environment without opening the browser:
```bash
docker exec -it foodingo-spark-jupyter python /home/jovyan/work/spark/data_lake_analyzer.py
```

---

## Why read from MinIO instead of PostgreSQL?

PostgreSQL is excellent for structured, indexed queries on megabytes or gigabytes of data. 

However, if Foodingo generates **100 million orders**:
- PostgreSQL will struggle to aggregate 100 million rows quickly.
- The Python Kafka Consumer has already saved all those events as compressed **Apache Parquet** files in MinIO.
- Spark connects to MinIO and reads these Parquet files in parallel, completely bypassing PostgreSQL. This means heavy analytics jobs will **never slow down your live database**.
