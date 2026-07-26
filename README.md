# 🍕 Foodingo — Full-Stack Food Ordering Platform with Real-Time Lambda Data Engineering Pipeline

> A production-grade, enterprise-scale food ordering application built with **Spring Boot 3 + MongoDB 7** on the backend, a **React** frontend, and a **7-Stage Lambda Data Engineering Pipeline** powered by Apache Kafka, Debezium CDC, PostgreSQL, Apache Parquet / AWS S3 Data Lake, Apache Spark, dbt, Apache Airflow, a FastAPI ML Recommendation Engine, and Metabase BI Dashboards — fully orchestrated via Docker Compose.

---

## 📑 Table of Contents

- [Enterprise Lambda Architecture](#-enterprise-lambda-architecture)
- [Master Scalability Roadmap](#-master-scalability-roadmap)
- [Tech Stack](#-tech-stack)
- [Repository Structure](#-repository-structure)
- [Data Flow — The 7 Stages (Hot Path vs. Cold Path)](#-data-flow--the-7-stages-hot-path-vs-cold-path)
- [How Services Interact (Data Types & Protocols)](#-how-services-interact-data-types--protocols)
- [Prerequisites & Quick Start](#-prerequisites--quick-start)
- [Environment Variables](#-environment-variables)
- [Service Ports & Dashboards](#-service-ports--dashboards)
- [Testing the Full Pipeline](#-testing-the-full-pipeline)
- [Troubleshooting](#-troubleshooting)
- [Component Deep-Dive READMEs](#-component-deep-dive-readmes)

---

## 🏗 Enterprise Lambda Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                             FOODINGO WEB APPLICATION                                            │
│                                                                                                                 │
│  ┌─────────────────────────┐          ┌──────────────────────────────────────────────────────────────────────┐  │
│  │     React Frontend      │─────────▶│                    Spring Boot 3.4.4 (Java 17)                       │  │
│  │     (User Interface)    │◀─────────│                    REST API on port :8080                            │  │
│  └─────────────────────────┘          └──────────────────────────────────┬───────────────────────────────────┘  │
│                                                                          │                                      │
│                                         [REST JSON / BSON]               │ [Spring Kafka JSON / TCP]            │
│                                         ▼                                ▼                                      │
│                               ┌───────────────────┐            ┌───────────────────┐                            │
│                               │  MongoDB 7 (rs0)  │            │   Apache Kafka    │                            │
│                               │  Primary Database │            │   13 Topics       │                            │
│                               └─────────┬─────────┘            │   3 Partitions    │                            │
│                                         │                      └─────────▲─────────┘                            │
│                                         │ [Change Streams oplog]         │                                      │
│                                         ▼                                │                                      │
│                               ┌──────────────────────────────────────────┴─────────┐                            │
│                               │       Debezium CDC 2.5 (Kafka Connect :8083)       │                            │
│                               │       Captures raw inserts/updates/deletes         │                            │
│                               └────────────────────────────────────────────────────┘                            │
└──────────────────────────────────────────────────────────────────┬──────────────────────────────────────────────┘
                                                                   │
                                                                   │ [Kafka Consumer poll() JSON]
                                                                   ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                           PYTHON KAFKA CONSUMER ETL SERVICE (consumer.py / Docker :9092)                        │
│                                                                                                                 │
│  • Subscribes to all 13 Kafka topics in consumer group: foodingo-data-pipeline                                  │
│  • Performs DUAL-WRITE: splits live operational data from long-term immutable Big Data storage                  │
└──────────────────────┬───────────────────────────────────────────────────────────────────┬──────────────────────┘
                       │                                                                   │
                       │ [SQL INSERT / psycopg2]                                           │ [boto3 PUT / .parquet]
                       │ (Hot Path — OLAP Warehousing)                                     │ (Cold Path — Big Data Lake)
                       ▼                                                                   ▼
┌──────────────────────────────────────────────┐                   ┌──────────────────────────────────────────────┐
│           PostgreSQL 15 (Warehouse)          │                   │          AWS S3 / MinIO (Data Lake)          │
│           Database: foodingo_warehouse       │                   │          Bucket: foodingo-data-lake          │
│                                              │                   │                                              │
│  ┌─ raw Schema (append-only events) ───────┐ │                   │  s3://foodingo-data-lake/raw/events/         │
│  │ • user_events      • cart_events        │ │                   │    ├── order/created/year=2026/month=07/...  │
│  │ • order_events     • cdc_events         │ │                   │    └── foodingo/foodies/orders/...           │
│  └───────────────────┬─────────────────────┘ │                   │  (Hive-style date/hour partitioned parquet)  │
│                      │                       │                   └───────────────────────┬──────────────────────┘
│                      │ [dbt run — 2 AM]      │                                           │
│                      ▼                       │                                           │
│  ┌─ analytics Schema (dimensional marts) ──┐ │                                           │
│  │ • stg_* (Views)    • fact_* (Tables)    │ │                                           │ [s3a:// Parquet scan]
│  │ • daily_revenue    • food_popularity    │ │                                           │ (hadoop-aws 3.3.4)
│  │ • cart_abandonment • user_funnel        │ │                                           ▼
│  │ • ml_user_order_matrix                  │ │                   ┌──────────────────────────────────────────────┐
│  └───────────────────┬─────────────────────┘ │                   │             Apache Spark Cluster             │
│                      │                       │                   │        Container: foodingo-spark-jupyter     │
│                      │                       │                   │        Mode: local[*] multi-core             │
│                      │                       │                   │                                              │
│                      │                       │                   │  • Distributed Big Data compute & aggregations │
│                      │                       │                   │  • Bypasses PostgreSQL (zero DB load)        │
│                      │                       │                   └───────────────────────┬──────────────────────┘
│                      │                       │                                           │
│                      │                       │   [JDBC write-back summary]               │ [Pivots ML user matrix]
└──────────────────────┼───────────────────────┴───────────────────────────────────────────┼──────────────────────┘
                       │                                                                   │
         ┌─────────────┴─────────────┐                                                     │
         │ [JDBC SQL READ]           │ [SQL SELECT / training]                             ▼
         ▼                           │                             ┌──────────────────────────────────────────────┐
┌─────────────────┐                  └────────────────────────────►│           FastAPI ML Service (:5001)         │
│ Metabase (:3000)│                                                │                                              │
│ BI Dashboards   │◄───────────────────────────────────────────────┤  • Item-Item Cosine Similarity Recommender   │
│ CEO Executive UI│       [REST HTTP GET /recommend/{id} < 10ms]   │  • Logistic Regression Churn Classifier      │
└─────────────────┘                                                │  • Persisted in RAM via joblib (/tmp/*.pkl)  │
                                                                   └───────────────────────▲──────────────────────┘
                                                                                           │
                                                                                           │ [HTTP POST /train/* — 2 AM]
                                                                   ┌───────────────────────┴──────────────────────┘
                                                                   │            Apache Airflow (:8089)
                                                                   │      Master DAG: foodingo_daily_pipeline
                                                                   └──────────────────────────────────────────────
```

---

## 📈 Master Scalability Roadmap

When Foodingo scales from thousands to millions of daily orders, follow this component-by-component scaling guide:

| Component | Current Configuration | Capacity / Bottleneck | Step 1: Intermediate Scaling | Step 2: Enterprise Big Data Scaling |
|---|---|---|---|---|
| **Apache Kafka** | 1 Broker, 13 Topics, 3 Partitions per topic (`replication-factor: 1`) | ~50,000 msgs/sec | Increase partitions per topic from `3` to `6` | Deploy a 3-broker cluster, set `replication-factor: 3`, migrate to KRaft mode (Zookeeper-free) |
| **Debezium CDC** | 1 Connect Worker container, 1 task monitoring 4 collections | ~5,000 mutations/sec | Set `"tasks.max": "4"` in JSON config for collection-level parallelism | Deploy a distributed Kafka Connect cluster (3+ workers) with Avro Schema Registry |
| **MongoDB** | Replica Set (`rs0`) with 1 member, WiredTiger engine | ~50,000 writes/sec | Add 2 Secondary read-replicas (`rs.initiate()`) for automatic primary failover | Shard `foodies.orders` by `userId (hashed)` across multiple shard replica sets |
| **Kafka Consumer** | 1 Python container instance reading 39 partitions | 100 events/sec per container | Scale to 3 instances (`docker compose up --scale kafka-consumer=3`) | Replace Python consumer with **Apache Flink** or **Spark Streaming** for exactly-once ETL |
| **PostgreSQL** | Single instance, `raw` + `analytics` schemas | ~100 GB relational limit | Add **PgBouncer** connection pooler; partition `raw.order_events` by month | Provision a dedicated PostgreSQL Read-Replica for Metabase BI dashboards |
| **MinIO / AWS S3** | `foodingo-data-lake` bucket, Hive-partitioned Parquet files | Infinite storage scale | Implement Airflow daily **Small File Compaction** PySpark job | Set S3 Lifecycle Rules: move >30 day logs to S3 Infrequent Access, >90 days to Glacier |
| **Apache Spark** | `local[*]` mode in single Jupyter Docker container | Multi-core host RAM limit | Migrate to Spark Standalone Cluster (Master + Worker containers) | Submit serverless PySpark jobs to **AWS EMR** or **GCP Dataproc** clusters |
| **dbt** | `threads: 2`, Table & View materializations | Full table rebuilds take time | Convert `fact_orders` to **Incremental Model** (`materialized='incremental'`) | Increase `threads: 8`; implement built-in data quality tests (`unique`, `not_null`) |
| **Apache Airflow** | `LocalExecutor`, 30s scheduler loop, Postgres metadata | Subprocess CPU/RAM limits | Switch to **`CeleryExecutor`** with Redis message broker and worker nodes | Upgrade to **`KubernetesExecutor`** on EKS/GKE (auto-spawns ephemeral task pods) |
| **ML Service** | 1 Uvicorn worker process, `/tmp/*.pkl` RAM joblib loading | 1 Host CPU core | Run `gunicorn` with 4 Uvicorn worker processes (`--workers 4`) | Migrate model inference to **Redis Shared Memory** or **AWS SageMaker Endpoints** |
| **Metabase BI** | Direct JDBC connection to primary PostgreSQL DB | Concurrent BI queries slow DB | Point Metabase JDBC to a PostgreSQL Read-Replica; set Cache TTL to 24h | Embed JWT-secured interactive dashboards in React frontend (`Embedded BI`) |

---

## 🛠 Tech Stack

### Backend Application
| Technology | Version | Purpose |
|---|---|---|
| **Java** | 17 | Core language |
| **Spring Boot** | 3.4.4 | REST API framework |
| **Spring Security + JWT** | jjwt 0.11.5 | Authentication & authorization |
| **Spring Data MongoDB** | 3.4.x | ODM for MongoDB |
| **Spring Kafka** | 3.4.x | Kafka producer integration |
| **Razorpay Java SDK** | 1.4.1 | Payment gateway |
| **AWS SDK v2** | 2.31.23 | S3 image uploads |
| **Lombok** | latest | Boilerplate reduction |
| **Springdoc OpenAPI** | 2.8.6 | Swagger UI auto-generation |

### Data Engineering Pipeline
| Technology | Version | Purpose |
|---|---|---|
| **Apache Kafka** | 7.6.1 (Confluent) | Distributed event streaming |
| **Apache Zookeeper** | 7.6.1 (Confluent) | Kafka cluster coordination |
| **Schema Registry** | 7.6.1 (Confluent) | Schema evolution (available) |
| **Debezium** | 2.5 | MongoDB Change Data Capture |
| **Kafka Connect** | Debezium 2.5 | Connector framework for CDC |
| **Python Kafka Consumer** | kafka-python | Event consumption + ETL |
| **PostgreSQL** | 15-alpine | Analytical data warehouse (Hot Path) |
| **MinIO / AWS S3** | latest / S3 API | S3-compatible data lake (Cold Path) |
| **Apache Spark** | 3.5.1 | Big Data distributed analytics on MinIO |
| **Jupyter** | PySpark 3.5 | Interactive data lake querying UI |
| **Apache Airflow** | 2.9.1 | Workflow orchestration (DAGs) |
| **dbt (Data Build Tool)** | 1.7.9 | SQL-based data transformations |
| **FastAPI** | latest | ML model serving API |
| **scikit-learn** | latest | ML: Collaborative Filtering, Logistic Regression |
| **Metabase** | latest | Business Intelligence dashboards |

---

## 📁 Repository Structure

```
foodingo-main/
│
├── src/                          # 🟢 Spring Boot Application (Java 17)
│   └── main/java/com/food/
│       ├── config/               #    Security, CORS, Kafka, AWS, OpenAPI config
│       ├── controller/           #    REST controllers (Auth, Cart, Food, Order, Recommendation)
│       ├── entity/               #    MongoDB document entities
│       ├── event/                #    Kafka event POJOs (OrderEvent, CartEvent, UserEvent)
│       ├── filters/              #    JWT authentication filter
│       ├── io/                   #    Request/Response DTOs
│       ├── repository/           #    MongoDB repositories
│       ├── service/              #    Business logic + KafkaPublishingService
│       └── service/impl/         #    Service implementations
│
├── .env                          # 🔐 Environment variables (MongoDB URI, JWT, AWS, Kafka)
├── pom.xml                       # 📦 Maven dependencies
├── Dockerfile                    # 🐳 Multi-stage Spring Boot Docker image
├── docker-compose-pipeline.yml   # 🐳 Full 14-service data pipeline orchestration
│
├── kafka/                        # 📨 Kafka topic initialization (init-topics.sh)
├── debezium/                     # 🔄 Change Data Capture configuration (mongodb-connector.json)
├── mongodb/                      # 🍃 MongoDB replica set initialization (init-replica-set.js)
├── kafka-consumer/               # 🐍 Python Kafka Consumer ETL service (consumer.py)
├── postgres/                     # 🐘 PostgreSQL Data Warehouse schemas & indexes (init.sql)
├── minio/                        # 📦 MinIO / S3 bucket initialization (init-buckets.sh)
├── spark/                        # ⚡ Apache Spark scripts (data_lake_analyzer.py)
├── dbt/                          # 🔧 dbt SQL transformations (staging/facts/marts)
├── airflow/                      # 🌀 Apache Airflow DAGs (foodingo_daily_pipeline.py)
├── ml-service/                   # 🧠 FastAPI Machine Learning Service (recommender / churn)
├── metabase/                     # 📊 Metabase BI Executive Dashboards
└── architecture.md               # 📐 Extended architectural diagrams
```

---

## 🌊 Data Flow — The 7 Stages (Hot Path vs. Cold Path)

Foodingo implements a true **Lambda Architecture**, ensuring that real-time transactional operations, interactive executive dashboards, and heavy Big Data machine learning jobs operate without interfering with one another.

### Stage 1: Event Generation (Spring Boot → Kafka)
When a user interacts with the app, `KafkaPublishingService.java` asynchronously publishes JSON events to 9 specific Kafka topics (`order.created`, `cart.item_added`, etc.) over port `9094`.

### Stage 2: Change Data Capture (MongoDB → Debezium → Kafka)
Simultaneously, Debezium tails MongoDB's Replica Set (`rs0`) oplog and streams raw database inserts, updates, and deletes to 4 Kafka topics (`foodingo.foodies.orders`, etc.).

### Stage 3: Real-Time Dual-Write Ingestion (Kafka Consumer)
A Python ETL service (`consumer.py`) polls all 13 topics and splits the stream:
- **The Hot Path (PostgreSQL):** Inserts structured rows into `raw.order_events`, `raw.cart_events`, etc., for sub-second SQL queries.
- **The Cold Path (S3 / MinIO Parquet):** Converts JSON into columnar **Apache Parquet** files and uploads to `s3://foodingo-data-lake/` using Hive date partitioning (`year=2026/month=07/day=24/`).

### Stage 4: Dimensional Data Modeling (dbt)
dbt transforms messy `raw` ingestion tables into clean, business-ready star-schema tables in the `analytics` schema:
- **Staging Views:** Deduplicates retried events (`stg_orders`, `stg_cart`).
- **Fact Tables:** Immutable historical records (`fact_orders`, `fact_cart_events`).
- **Mart Tables:** Pre-computed BI summaries (`daily_revenue`, `food_popularity`, `cart_abandonment`). Notice that `food_popularity` uses PostgreSQL's native `jsonb_array_elements()` to explode nested order items into individual rows!

### Stage 5: Master Pipeline Orchestration (Apache Airflow)
Every night at **2:00 AM IST**, Apache Airflow executes `foodingo_daily_pipeline.py`:
1. Triggers `dbt run --select staging/facts/marts`.
2. Runs built-in data quality tests (`dbt test`).
3. Triggers historical **Apache Spark** jobs for heavy Data Lake aggregations.
4. Triggers HTTP `POST` requests to the ML Service to retrain AI algorithms.

### Stage 6: Artificial Intelligence & Recommender Engine (FastAPI ML Service)
The ML microservice (`ml-service`) provides two algorithms:
- **Food Recommender (Item-Item Collaborative Filtering):** Reads `analytics.ml_user_order_matrix` (or Spark Data Lake matrices), computes Cosine Similarity across food items, and caches the sparse matrix in RAM (`/tmp/recommender_model.pkl`). When Spring Boot requests `GET /recommend/{id}`, it responds in **< 10 milliseconds**.
- **Churn Predictor (Logistic Regression):** Evaluates shopping cart abandonment risk (`0.0` to `1.0`) to help marketing teams trigger discount coupon recovery emails.

### Stage 7: Executive BI Dashboards (Metabase)
Metabase connects via JDBC to PostgreSQL's `analytics` schema. Executives view interactive charts (`Daily Revenue`, `Food Popularity`, `Customer Funnels`) in real-time. Even when analyzing 5-year historical trends, queries run in milliseconds because Apache Spark pre-computed and wrote the summary tables back to Postgres overnight!

---

## 🔗 How Services Interact (Data Types & Protocols)

```
Spring Boot (:8080)
    │
    ├──[BSON / Wire Protocol]──▶ MongoDB (:27017) ──[Change Stream oplog]──▶ Debezium ──▶ Kafka
    │
    ├──[JSON over TCP]──▶ Kafka Broker (:9094 External / :9092 Internal)
    │                         │
    │                         ▼
    │                   Python Kafka Consumer ──┬──[SQL INSERT]──▶ PostgreSQL (:5432)
    │                                           └──[boto3 Parquet]─▶ AWS S3 / MinIO (:9000)
    │                                                                   │
    │                                                                   ▼
    │                   Apache Spark (:8888) ◄──[s3a:// Parquet scan]───┘
    │                         │
    │                         └──[JDBC Summary Write]──▶ PostgreSQL (:5432)
    │
    ├──[HTTP GET /recommend/{id}]──▶ FastAPI ML Service (:5001) ──[SQL SELECT]──▶ PostgreSQL
    │
    └──[Swagger UI]──▶ http://localhost:8080/swagger-ui.html

Apache Airflow (:8089)
    │
    ├──[BashOperator / dbt CLI]──▶ dbt ──[SQL DDL/SELECT]──▶ PostgreSQL (:5432)
    ├──[BashOperator / PySpark]──▶ Apache Spark Container (:8888)
    └──[SimpleHttpOperator]──────▶ FastAPI ML Service (POST :5001/train/recommender)

Metabase (:3000) ──[JDBC SQL Read]──▶ PostgreSQL (analytics Schema)
```

---

## ✅ Prerequisites & Quick Start

### Prerequisites
- **Docker Desktop** (4.0+) with at least **6 GB RAM** allocated
- **Java 17+** (if running Spring Boot outside Docker)
- **Maven 3.8+** (included via `./mvnw` wrapper)

### Quick Start (Start the Full 14-Service Pipeline)

```bash
# 1. Start all infrastructure, database, streaming, ETL, and BI containers
docker compose -f docker-compose-pipeline.yml --env-file .env up -d

# 2. Check container health (Wait until all containers show healthy / Up)
docker compose -f docker-compose-pipeline.yml ps

# 3. Follow logs of the Python Kafka Consumer to watch real-time ingestion
docker logs -f foodingo-kafka-consumer
```

---

## 🔐 Environment Variables

The pipeline reads configuration from `.env` in the project root:

```ini
# MongoDB & Application
MONGODB_URI=mongodb://localhost:27017/foodies?replicaSet=rs0&directConnection=true
JWT_SECRET=YourSuperSecretKeyForJWTAuthentication2026!

# AWS S3 Data Lake (Or local MinIO credentials)
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
AWS_DATALAKE_BUCKET=foodingo-data-lake
AWS_DATALAKE_REGION=us-east-1

# PostgreSQL Data Warehouse
POSTGRES_USER=foodingo
POSTGRES_PASSWORD=foodingo123
POSTGRES_DB=foodingo_warehouse
```

---

## 🌐 Service Ports & Dashboards

| Service | Port | Dashboard / Endpoint | Login Credentials |
|---|---|---|---|
| **Spring Boot API** | `8080` | `http://localhost:8080/swagger-ui.html` | — |
| **Metabase BI** | `3000` | `http://localhost:3000` | Setup on first visit |
| **Apache Airflow** | `8089` | `http://localhost:8089` | `admin` / `admin` |
| **Kafka UI** | `8090` | `http://localhost:8090` | — |
| **MinIO Console** | `9001` | `http://localhost:9001` | `foodingo` / `foodingo123` |
| **Spark JupyterLab**| `8888` | `http://localhost:8888` | Token: `foodingo123` |
| **FastAPI ML Service**| `5001` | `http://localhost:5001/docs` | — |
| **PostgreSQL** | `5432` | `localhost:5432/foodingo_warehouse` | `foodingo` / `foodingo123` |
| **MongoDB Replica Set**| `27017`| `mongodb://localhost:27017/foodies` | — |

---

## 🧪 Testing the Full Pipeline

1. **Place a Test Order via Swagger UI:**
   - Open **http://localhost:8080/swagger-ui.html**.
   - Authenticate and submit an order via `POST /api/orders`.
2. **Verify Real-Time Kafka Ingestion:**
   - Open Kafka UI (**http://localhost:8090**) and inspect messages under topic **`order.created`** and **`foodingo.foodies.orders`**.
3. **Verify PostgreSQL Raw Event Ingestion:**
   ```bash
   docker exec -i foodingo-postgres psql -U foodingo -d foodingo_warehouse -c \
     "SELECT order_id, amount, event_timestamp FROM raw.order_events ORDER BY ingested_at DESC LIMIT 5;"
   ```
4. **Trigger Nightly Airflow Pipeline Manually:**
   - Open Airflow (**http://localhost:8089**) → select **`foodingo_daily_pipeline`** → click **▶ Trigger DAG**.
5. **Verify Clean dbt Analytics Tables:**
   ```bash
   docker exec -i foodingo-postgres psql -U foodingo -d foodingo_warehouse -c \
     "SELECT * FROM analytics.fact_orders LIMIT 5;"
   ```
6. **Verify Real-Time ML Recommendation Response (< 10 ms):**
   ```bash
   curl http://localhost:5001/recommend/YOUR_USER_ID
   ```
7. **Run PySpark Data Lake Analysis on S3 Parquet Logs:**
   ```bash
   docker exec -it foodingo-spark-jupyter python /home/jovyan/work/spark/data_lake_analyzer.py
   ```

---

## 🔧 Troubleshooting

### Kafka Won't Start (`NodeExistsException`)
Occurs when Docker Desktop is restarted and Zookeeper retains stale volume session state:
```bash
docker compose -f docker-compose-pipeline.yml rm -sf zookeeper kafka
docker compose -f docker-compose-pipeline.yml --env-file .env up -d
```

### Airflow dbt Tasks Fail
Ensure `dbt-postgres` is installed inside Airflow containers (configured automatically via docker-compose):
```bash
docker exec -it foodingo-airflow-webserver dbt --version
```

---

## 📚 Component Deep-Dive READMEs

Every component in the Foodingo pipeline has an exhaustive, standalone technical README detailing its internal workings, component interaction flows, configurations, and scaling guides:

| Component | README File | Core Focus Area |
|---|---|---|
| **Apache Kafka** | [kafka/README.md](kafka/README.md) | Event Streaming, Commit Logs, 13 Topics & 3 Partitions |
| **Debezium CDC** | [debezium/README.md](debezium/README.md) | MongoDB Change Streams, Oplog Tailing, Envelopes |
| **MongoDB** | [mongodb/README.md](mongodb/README.md) | Replica Set (`rs0`), WiredTiger, JSON Document Storage |
| **Kafka Consumer** | [kafka-consumer/README.md](kafka-consumer/README.md) | Real-Time ETL, psycopg2 SQL Insert, Parquet Conversion |
| **PostgreSQL** | [postgres/README.md](postgres/README.md) | Warehouse Schemas (`raw`/`analytics`), JSONB Indexing |
| **MinIO / AWS S3** | [minio/README.md](minio/README.md) | Columnar Parquet, Hive-style Date/Hour Partitioning |
| **Apache Spark** | [spark/README.md](spark/README.md) | Distributed Big Data, `s3a://` Protocol, Airflow Integration |
| **dbt** | [dbt/README.md](dbt/README.md) | Jinja SQL Compilation, DAGs, JSONB Exploding (`marts`) |
| **Apache Airflow**| [airflow/README.md](airflow/README.md) | Scheduler, `LocalExecutor`, Automated Nightly DAG Chain |
| **ML Service** | [ml-service/README.md](ml-service/README.md) | Collaborative Filtering, Logistic Regression, RAM Inference |
| **Metabase BI** | [metabase/README.md](metabase/README.md) | JDBC Warehousing, Caching, Embedded BI Dashboards |

---

## 📄 License

This repository is developed for enterprise architecture demonstration, educational, and portfolio purposes.
