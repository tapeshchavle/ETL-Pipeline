# 📦 MinIO — S3-Compatible Data Lake

## Overview

MinIO is an **S3-compatible object storage** server that acts as Foodingo's **local data lake**. In production, this would be Amazon S3 or Google Cloud Storage. For local development, MinIO provides the exact same API — meaning your code works identically in both environments.

The Kafka Consumer writes **Apache Parquet** files to MinIO for every event it processes. These Parquet files serve as the **long-term archival layer** — immutable, columnar, and ready for big data tools like Apache Spark, AWS Athena, or Databricks.

---

## Architecture

```
Python Kafka Consumer
       │
       │  boto3 S3 client (same API as AWS S3)
       ▼
┌─────────────────────────────────────────────┐
│          MinIO Server                        │
│          Container: foodingo-minio           │
│                                             │
│  API  Port: 9000                            │
│  Console Port: 9001                          │
│                                             │
│  ┌─── Bucket: foodingo-data-lake ────────┐  │
│  │                                       │  │
│  │  raw/events/                          │  │
│  │  ├── user/registered/                 │  │
│  │  │   └── year=2026/month=07/day=24/   │  │
│  │  │       └── hour=12/                 │  │
│  │  │           └── 1721822400.parquet   │  │
│  │  ├── cart/item_added/                 │  │
│  │  │   └── year=2026/month=07/...      │  │
│  │  ├── order/created/                   │  │
│  │  │   └── year=2026/month=07/...      │  │
│  │  └── ...                              │  │
│  └───────────────────────────────────────┘  │
│                                             │
│  Credentials:                               │
│  User: foodingo                              │
│  Password: foodingo123                       │
└─────────────────────────────────────────────┘
```

---

## Why Parquet + S3?

| Feature | Benefit |
|---|---|
| **Columnar format** | Queries that read only 3 columns out of 20 skip 85% of the data |
| **Compression** | Parquet files are ~10x smaller than JSON |
| **Schema embedded** | Column types are stored in the file footer — self-describing |
| **Hive partitioning** | `year=2026/month=07/day=24/` enables partition pruning |
| **Immutable** | Once written, files are never modified — perfect audit trail |
| **S3 API compatible** | Same code works with MinIO locally and AWS S3 in production |

---

## File Path Convention

```
s3://foodingo-data-lake/raw/events/{topic}/{year}/{month}/{day}/{hour}/{timestamp}.parquet
```

Example:
```
s3://foodingo-data-lake/raw/events/order/created/year=2026/month=07/day=24/hour=14/1721829600.parquet
```

---

## MinIO Console

Access the web-based file browser at **http://localhost:9001**.

| Setting | Value |
|---|---|
| URL | http://localhost:9001 |
| Username | `foodingo` |
| Password | `foodingo123` |

You can:
- Browse the `foodingo-data-lake` bucket
- Download Parquet files
- View storage statistics
- Manage access policies

---

## Files in This Directory

| File | Purpose |
|---|---|
| `init-buckets.sh` | Creates the `foodingo-data-lake` bucket on first startup (executed by `minio-init` container) |

---

## Useful Commands

```bash
# List all buckets
docker exec -i foodingo-minio-init mc ls minio/

# List files in the data lake
docker exec -i foodingo-minio-init mc ls --recursive minio/foodingo-data-lake/

# Download a Parquet file
docker exec -i foodingo-minio-init mc cp minio/foodingo-data-lake/raw/events/order/created/... /tmp/

# View MinIO server info
docker exec -i foodingo-minio mc admin info minio
```

---

## Learn More

- [MinIO Documentation](https://min.io/docs/minio/linux/index.html)
- [Apache Parquet](https://parquet.apache.org/)
- [boto3 S3 API](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html)
