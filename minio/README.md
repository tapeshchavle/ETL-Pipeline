# 📦 MinIO & AWS S3 — Object Storage Data Lake

## Overview

In Foodingo's data engineering pipeline, object storage serves as the **"Cold Path"** (or Big Data path) of our Lambda Architecture.

- **In Production / Cloud:** We use an **AWS S3 bucket** (`foodingo-data-lake`).
- **In Local Development:** We provide **MinIO**, an open-source, high-performance S3-compatible object store running in Docker (`foodingo-minio`).

Because MinIO implements the **Amazon S3 v4 API** exactly, all upstream and downstream code — including Python `boto3`, Apache PySpark (`s3a://`), and AWS SDKs — works **identically** whether pointing to local MinIO or real AWS S3.

---

## Why Object Storage + Apache Parquet?

While PostgreSQL is great for structured SQL analytics up to ~100 GB, storing billions of historical raw event logs in a relational database becomes prohibitively expensive and slow.

By streaming raw events from Kafka into object storage as **Apache Parquet** files, Foodingo achieves:

| Feature | Benefit in Foodingo |
|---|---|
| **Columnar Storage** | Analytics queries only read the specific columns needed (e.g., `amount` and `event_timestamp`), skipping 80%+ of disk I/O |
| **High Compression** | Parquet uses Snappy/Gzip compression, shrinking JSON event data by ~10x |
| **Self-Describing Schema** | Column data types are embedded directly in the Parquet file footer |
| **Hive Partitioning** | Folders structured as `year=2026/month=07/day=24/` allow Spark to skip irrelevant dates instantly |
| **Immutable Audit Log** | Once written, historical event files are never modified, creating an immutable audit trail |
| **Infinite Scalability** | S3 scales to exabytes of data without managing disk volumes or database shards |

---

## Architecture & Data Flow in Foodingo

```
┌────────────────────────────────────────────────────────────────────────┐
│                        Python Kafka Consumer                           │
│     (consumer.py — reads from 13 topics in foodingo-data-pipeline)     │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                                    │ boto3.put_object()
                                    │ (Writes compressed Apache Parquet)
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│                   AWS S3 / MinIO Object Storage                        │
│                   Bucket: foodingo-data-lake                           │
│                                                                        │
│  Directory Structure (Hive-Style Partitioning):                        │
│  s3://foodingo-data-lake/                                              │
│  ├── raw/events/                                                       │
│  │   ├── order/created/                                                │
│  │   │   └── year=2026/month=07/day=24/hour=14/                        │
│  │   │       └── 1721829600.parquet                                    │
│  │   ├── user/registered/                                              │
│  │   │   └── year=2026/month=07/day=24/hour=14/                        │
│  │   │       └── 1721829605.parquet                                    │
│  │   ├── cart/item_added/                                              │
│  │   │   └── year=2026/month=07/...                                    │
│  │   └── foodingo/foodies/orders/  (Debezium CDC mutations)            │
│  │       └── year=2026/month=07/...                                    │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                                    │ s3a:// Read (Distributed Scan)
                                    │ (Partition Pruning via Hive Keys)
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│                         Apache Spark Cluster                           │
│                   (foodingo-spark-jupyter :8888)                       │
│                                                                        │
│  • Performs Big Data aggregations across millions of rows             │
│  • Trains recommendation algorithms on full historical datasets        │
│  • Completely bypasses PostgreSQL (zero load on live DB)               │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 🔬 How Parquet Writing Works Internally

### Step 1: DataFrame Conversion in `consumer.py`

When the Python Kafka Consumer processes an event, it converts the JSON payload into a columnar structure using Pandas and PyArrow:

```python
def upload_parquet(s3, records, topic):
    # 1. Convert records list to Pandas DataFrame
    df = pd.DataFrame(records)
    
    # 2. Convert DataFrame to PyArrow Columnar Table
    table = pa.Table.from_pandas(df)
    
    # 3. Serialize to in-memory compressed Parquet bytes
    buf = BytesIO()
    pq.write_table(table, buf)
    
    # 4. Upload to S3/MinIO
    s3.put_object(
        Bucket="foodingo-data-lake",
        Key=f"raw/events/{topic.replace('.', '/')}/year={now.year}/month={now.month:02d}/day={now.day:02d}/hour={now.hour:02d}/{timestamp}.parquet",
        Body=buf.getvalue()
    )
```

### Step 2: Why Hive Partitioning (`year=2026/month=07/day=24/`)?

Notice the file path convention:
```
raw/events/order/created/year=2026/month=07/day=24/hour=14/1721829600.parquet
```

This format is called **Hive-Style Partitioning**. 
- When Apache Spark reads `s3a://foodingo-data-lake/raw/events/order/created/`, it automatically recognizes `year`, `month`, `day`, and `hour` as **virtual columns** in the dataframe.
- **Partition Pruning:** If a Spark SQL query asks for `WHERE year = 2026 AND month = 7 AND day = 24`, Spark's query optimizer checks S3 directory prefixes and **completely ignores** folders for other days, saving massive amounts of network and disk read time.

---

## 🔗 How S3 / MinIO Interacts With Other Components

```
┌──────────────────────────────┐
│ Kafka Consumer               │
│ (Writes .parquet files)      │
└──────────────┬───────────────┘
               │ boto3 S3 API
               ▼
┌──────────────────────────────┐
│ AWS S3 / MinIO               │
│ Bucket: foodingo-data-lake   │
└──────────────┬───────────────┘
               │ s3a:// protocol (hadoop-aws)
               ▼
┌──────────────────────────────┐
│ Apache Spark                 │
│ (Reads Parquet for ML & BI)  │
└──────────────────────────────┘
```

1. **Python Kafka Consumer (`consumer.py`):** Uses the standard AWS `boto3` SDK. If configured with AWS credentials in `.env`, it writes directly to AWS S3 (`us-east-1` or `ap-south-1`). In offline Docker development, it can point to the local `foodingo-minio` endpoint.
2. **Apache Spark (`foodingo-spark-jupyter`):** Uses the `hadoop-aws:3.3.4` and `aws-java-sdk-bundle:1.12.262` libraries to connect via the `s3a://` filesystem scheme.
3. **MinIO Init Container (`foodingo-minio-init`):** A one-shot shell script (`init-buckets.sh`) that uses the MinIO Client (`mc`) to automatically create the `foodingo-data-lake` bucket on first startup when running locally.

---

## MinIO Local Web Console

When testing locally with MinIO, you can inspect the data lake visually:

| Setting | Value |
|---|---|
| Console URL | **http://localhost:9001** |
| API URL | `http://localhost:9000` |
| Username | `foodingo` |
| Password | `foodingo123` |
| Default Bucket | `foodingo-data-lake` |

---

## 📈 Scalability & Current Configuration

### Current Setup
| Setting | Value | Why |
|---|---|---|
| Production Storage | AWS S3 Bucket (`foodingo-data-lake`) | Highly available, durable cloud object storage |
| Local Storage | MinIO Container (`foodingo-minio`) | Lightweight local S3 emulator for offline development |
| File Format | Apache Parquet (Snappy compression) | Optimal balance of compression ratio and read speed |
| Partition Strategy | Hourly (`year=/month=/day=/hour=`) | Prevents folders from containing too many files while maintaining fine-grained pruning |

---

## 🚀 Future Scaling Guide

When Foodingo scales to millions of daily events, implement these enterprise storage optimizations:

### Level 1: Small File Compaction (Spark Compaction Job)
If the Kafka Consumer writes hundreds of small 50 KB Parquet files every hour:
- S3 GET request overhead can slow down Spark queries.
- Schedule a daily **Spark Compaction Job** via Airflow to combine small hourly Parquet files into large ~128 MB optimized Parquet files per day.

### Level 2: AWS S3 Lifecycle Policies (Cost Optimization)
To prevent cloud storage costs from growing infinitely:
- **0–30 Days:** Keep files in **S3 Standard** storage for frequent Spark analytics.
- **30–90 Days:** Automatically transition files to **S3 Infrequent Access (IA)** (~40% cost reduction).
- **90+ Days:** Archive historical logs to **Amazon S3 Glacier Flexible Retrieval** (~80% cost reduction) for audit compliance.

### Level 3: Amazon Athena / AWS Glue Catalog Integration
To make the S3 Data Lake directly queryable by analysts without launching a Spark cluster:
- Run an **AWS Glue Crawler** over `s3://foodingo-data-lake/`.
- Query Parquet files instantly using serverless **Amazon Athena** SQL.

---

## Files in This Directory

| File | Purpose |
|---|---|
| `init-buckets.sh` | Bash script run by `minio-init` container to provision `foodingo-data-lake` bucket |

---

## Useful Commands

```bash
# List all buckets in local MinIO
docker exec -i foodingo-minio-init mc ls minio/

# List Parquet files inside the local data lake recursively
docker exec -i foodingo-minio-init mc ls --recursive minio/foodingo-data-lake/

# Check storage stats for the bucket
docker exec -i foodingo-minio-init mc du minio/foodingo-data-lake/

# Copy a local test Parquet file into the MinIO container for inspection
docker exec -i foodingo-minio-init mc cp minio/foodingo-data-lake/raw/events/order/created/2026/07/24/14/file.parquet /tmp/

# View MinIO Server info and health
docker exec -i foodingo-minio mc admin info minio
```

---

## Learn More

- [AWS S3 Documentation](https://docs.aws.amazon.com/s3/)
- [MinIO Docker Documentation](https://min.io/docs/minio/linux/index.html)
- [Apache Parquet Official Documentation](https://parquet.apache.org/)
- [Apache Spark s3a:// Guide](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html)
