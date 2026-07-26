# ⚡ Apache Spark — Big Data Analytics & ML Processing Engine

## Overview

Apache Spark is the **distributed computing engine** for Foodingo's Data Lake. While PostgreSQL and dbt handle everyday business intelligence and operational reporting (the "Hot Path"), Apache Spark is designed to process massive volumes of historical data directly from AWS S3 / MinIO (the "Cold / Big Data Path").

By incorporating Spark, Foodingo implements a true **Lambda Architecture**, cleanly separating low-latency relational SQL reporting from heavy distributed batch processing and machine learning pipeline preparation.

---

## Why Apache Spark?

| Requirement | How Apache Spark Solves It |
|---|---|
| **Big Data Scale** | Can aggregate billions of rows from S3 without slowing down live databases |
| **In-Memory Speed** | Performs intermediate DAG computations in RAM, 100x faster than Hadoop MapReduce |
| **Native Parquet Support** | Automatically parses Hive partitions (`year=2026/month=07`) and prunes unneeded columns |
| **Unified Engine** | Combines SQL queries (`Spark SQL`), DataFrames, and Machine Learning (`MLlib`) in one script |
| **Orchestration Ready** | Can be scheduled and triggered nightly by Apache Airflow |

---

## Architecture & End-to-End Flow in Foodingo

```
┌────────────────────────────────────────────────────────────────────────┐
│                        Apache Airflow (:8089)                          │
│         (Schedules & Triggers Nightly Big Data Processing Jobs)        │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                                    │ Triggers Spark Script / Job
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│                    Apache Spark Cluster (Docker)                       │
│                    Container: foodingo-spark-jupyter                   │
│                    Mode: local[*] (Multi-core Parallelism)             │
│                                                                        │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │                     Spark Driver & Executors                     │  │
│  │                                                                  │  │
│  │  1. s3a:// Parquet Scan   ──► Reads partitioned S3 files in      │  │
│  │                               parallel using CPU worker threads  │  │
│  │  2. Shuffle & Aggregation ──► Distributed GROUP BY / JOINs across│  │
│  │                               historical user and order records  │  │
│  └──────────────────┬─────────────────────────────┬─────────────────┘  │
└─────────────────────┼─────────────────────────────┼────────────────────┘
                      │                             │
   1. Read Raw Events │                             │ 2. Write Aggregates / ML Features
                      ▼                             ▼
┌───────────────────────────────────┐     ┌───────────────────────────────────┐
│     AWS S3 / MinIO Data Lake      │     │      PostgreSQL / ML Service      │
│     (s3://foodingo-data-lake/)    │     │                                   │
│                                   │     │  • Writes Big Data aggregations   │
│  • Immutable Parquet logs         │     │    back to PostgreSQL analytics   │
│  • Historical event archive       │     │    for Metabase CEO dashboards    │
│  • Bypasses PostgreSQL entirely   │     │  • Exports cleaned feature matrix │
│                                   │     │    to FastAPI ML Service          │
└───────────────────────────────────┘     └───────────────────────────────────┘
```

---

## 🔬 How Spark Works Internally — Step by Step

Let's examine what happens when you execute `python work/spark/data_lake_analyzer.py`:

### Step 1: Initializing the `SparkSession` & AWS JARs

```python
spark = SparkSession.builder \
    .appName("Foodingo Data Lake Analyzer") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", f"https://s3.{aws_region}.amazonaws.com") \
    ...
    .getOrCreate()
```

- **`master("local[*]")`:** Instructs Spark to run in local cluster mode, automatically spawning **one Executor thread per CPU core** on your machine.
- **`hadoop-aws` & `aws-java-sdk-bundle` JARs:** Apache Spark does not natively understand AWS S3. These packages are dynamically downloaded via Ivy on startup to enable the `s3a://` filesystem protocol.

### Step 2: Distributed Parquet Read & Partition Discovery

```python
orders_df = spark.read.parquet(f"s3a://{aws_bucket}/raw/events/order/created/")
```

1. **Driver Operation:** The Spark Driver connects to AWS S3 and lists the directory metadata.
2. **Hive Partition Discovery:** Spark detects folders named `year=2026/month=07/day=24/` and automatically infers `year`, `month`, and `day` as integer columns.
3. **Parallel Task Assignment:** The Driver divides the discovered `.parquet` files into **Tasks** and assigns them to the CPU Executor threads. Each thread reads a Parquet file from S3 in parallel.

### Step 3: Distributed Shuffle & Aggregation

```python
revenue_df = orders_df.groupBy("paymentStatus") \
    .agg(
        sum("amount").alias("Total Revenue"),
        count("orderId").alias("Order Count")
    )
```

1. **Map Phase:** Each executor thread computes partial sums and counts for its assigned Parquet chunks.
2. **Shuffle Phase:** Executors exchange data across CPU threads so that all records with the same `paymentStatus` are grouped together.
3. **Reduce Phase:** Spark sums the partial totals into the final `Total Revenue` and `Order Count`.

---

## 🔗 How Spark Connects to Airflow, Metabase & the ML Service

### 1. The "Write-Back to PostgreSQL" Pattern (For Metabase Dashboards)
How does the CEO see historical Big Data calculations in Metabase if Metabase only connects to PostgreSQL?
- When historical data exceeds PostgreSQL's query capacity, an **Airflow DAG** triggers a Spark script.
- Spark reads billions of rows from **AWS S3 Parquet** and computes high-level executive summaries (e.g., 5-year customer lifetime value trends).
- Spark writes the resulting **small aggregated summary table** directly into PostgreSQL's `analytics` schema via JDBC:
  ```python
  summary_df.write \
      .format("jdbc") \
      .option("url", "jdbc:postgresql://postgres:5432/foodingo_warehouse") \
      .option("dbtable", "analytics.big_data_customer_lvt") \
      .option("user", "foodingo") \
      .option("password", "foodingo123") \
      .mode("overwrite") \
      .save()
  ```
- The CEO opens Metabase and views the chart instantly — queries run in milliseconds against the summarized Postgres table while Spark did the heavy lifting on S3!

### 2. Preparing Data for the ML Service
The Python ML Service (`ml-service`) performs cosine-similarity food recommendations and logistic regression churn prediction.
- At small scale, dbt builds `analytics.ml_user_order_matrix` inside PostgreSQL.
- At Big Data scale, **Apache Spark** scans historical cart abandonment logs and full user order histories across S3, builds dense feature matrices using `PySpark MLlib`, and exports cleaned training sets for the FastAPI ML Service to load into RAM.

---

## Accessing the Spark JupyterLab UI

| Service | URL | Default Token |
|---|---|---|
| **JupyterLab UI** | **http://localhost:8888** | `foodingo123` |
| **Spark Master / Job UI** | `http://localhost:4040` | *(Active only while a Spark script is running)* |

---

## 📈 Scalability & Current Configuration

### Current Setup
| Setting | Value | Why |
|---|---|---|
| Cluster Mode | `local[*]` (Single Container) | Simplifies local development while using all CPU cores |
| S3 Protocol | `s3a://` via `hadoop-aws:3.3.4` | Enterprise-standard S3 connector for Spark |
| Memory Allocation | Default Docker Container RAM | Sufficient for local sample Parquet datasets |
| Orchestration | Ready for Airflow BashOperator | Executable headlessly via container terminal |

---

## 🚀 Future Scaling Guide

When Foodingo's Data Lake grows to terabytes or petabytes, migrate from single-node Spark to a distributed cloud cluster:

### Level 1: Multi-Node Spark Standalone Cluster (Docker Swarm / K8s)
Instead of running in `local[*]` mode inside one Jupyter container:
- Deploy a dedicated **Spark Master** container and multiple **Spark Worker** containers.
- Update the connection URL in scripts:
  ```python
  # Change from local[*] to standalone cluster master:
  spark = SparkSession.builder.master("spark://spark-master:7077").getOrCreate()
  ```

### Level 2: Managed Cloud Big Data (AWS EMR / Google Dataproc)
For zero-infrastructure production deployment:
- Submit PySpark jobs directly to **Amazon EMR (Elastic MapReduce)** or **GCP Dataproc** serverless clusters.
- S3 access credentials (`AWS_ACCESS_KEY_ID`) are replaced by IAM Roles attached to the EMR EC2 instances, eliminating hardcoded keys entirely.

### Level 3: Memory & Partition Tuning (`spark.sql.shuffle.partitions`)
When joining large datasets in production Spark:
- The default number of shuffle partitions in Spark SQL is `200`.
- For terabyte-scale workloads, tune partition count based on cluster CPU cores:
  ```python
  spark.conf.set("spark.sql.shuffle.partitions", "800")
  spark.conf.set("spark.default.parallelism", "800")
  ```

---

## Files in This Directory

| File | Purpose |
|---|---|
| `data_lake_analyzer.py` | Executable PySpark script demonstrating direct S3 Parquet reads, Hive partition discovery, and aggregations |
| `README.md` | Comprehensive architectural documentation |

---

## Useful Commands

```bash
# Execute the sample Spark Data Lake Analyzer script headlessly in Docker
docker exec -it foodingo-spark-jupyter python /home/jovyan/work/spark/data_lake_analyzer.py

# Check installed Python packages inside the Spark container
docker exec -it foodingo-spark-jupyter pip list

# View JupyterLab server logs
docker logs -f foodingo-spark-jupyter

# Open an interactive PySpark shell inside the container
docker exec -it foodingo-spark-jupyter pyspark --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262
```

---

## Learn More

- [Apache Spark Official Documentation](https://spark.apache.org/docs/latest/)
- [PySpark SQL Module Reference](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html)
- [Hadoop AWS s3a:// Best Practices](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html)
- [Jupyter Docker Stacks (pyspark-notebook)](https://jupyter-docker-stacks.readthedocs.io/en/latest/using/selecting.html#jupyter-pyspark-notebook)
