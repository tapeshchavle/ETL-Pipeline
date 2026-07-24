# 🍕 Foodingo — Full-Stack Food Ordering Platform with Real-Time Data Engineering Pipeline

> A production-grade food ordering application built with **Spring Boot 3 + MongoDB** on the backend, a **React** frontend, and a **7-stage real-time data engineering pipeline** powered by Kafka, Debezium CDC, PostgreSQL, dbt, Airflow, a FastAPI ML service, and Metabase dashboards — all orchestrated via Docker Compose.

---

## 📑 Table of Contents

- [High-Level Architecture](#-high-level-architecture)
- [Tech Stack](#-tech-stack)
- [Repository Structure](#-repository-structure)
- [Data Flow — The 7 Stages](#-data-flow--the-7-stages)
- [How Services Interact](#-how-services-interact)
- [Prerequisites](#-prerequisites)
- [Quick Start](#-quick-start)
- [Environment Variables](#-environment-variables)
- [Service Ports & Dashboards](#-service-ports--dashboards)
- [Testing the Full Pipeline](#-testing-the-full-pipeline)
- [Troubleshooting](#-troubleshooting)
- [Component Deep-Dive READMEs](#-component-deep-dive-readmes)

---

## 🏗 High-Level Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                          FOODINGO PLATFORM                                  │
│                                                                              │
│  ┌─────────────────┐        ┌───────────────────────────────────────────┐    │
│  │   React Frontend │───────▶│       Spring Boot 3.4 (Java 17)          │    │
│  │   (User-facing)  │◀───────│       REST API on :8080                  │    │
│  └─────────────────┘        │                                           │    │
│                              │  ┌─────────┐ ┌─────────┐ ┌───────────┐  │    │
│                              │  │  Users   │ │  Cart   │ │  Orders   │  │    │
│                              │  └────┬─────┘ └────┬────┘ └─────┬─────┘  │    │
│                              │       │            │            │        │    │
│                              │       ▼            ▼            ▼        │    │
│                              │  ┌──────────────────────────────────┐    │    │
│                              │  │     MongoDB (foodies DB)         │    │    │
│                              │  │     Primary Data Store           │    │    │
│                              │  └──────────┬───────────────────────┘    │    │
│                              │             │                           │    │
│                              │             │ CDC (Change Data Capture) │    │
│                              └─────────────┼───────────────────────────┘    │
│                                            │                                │
│  ════════════════════════════════════════════════════════════════════════     │
│  ║                  DATA ENGINEERING PIPELINE                          ║     │
│  ════════════════════════════════════════════════════════════════════════     │
│                                            │                                │
│  ┌──────────────┐     ┌────────────────────▼────────────────────┐           │
│  │  Spring Boot  │────▶│         Apache Kafka (9092/9094)        │           │
│  │  (Producers)  │     │    13 Topics • 3 Partitions Each       │           │
│  └──────────────┘     │                                         │           │
│                        │  ┌──────────────┐  ┌────────────────┐  │           │
│  ┌──────────────┐     │  │ App Events   │  │ CDC Events     │  │           │
│  │  Debezium    │────▶│  │ user.*, cart.*│  │ foodingo.      │  │           │
│  │  (CDC)       │     │  │ order.*      │  │ foodies.*      │  │           │
│  └──────────────┘     │  └──────────────┘  └────────────────┘  │           │
│                        └──────────┬──────────────────────────────┘           │
│                                   │                                         │
│                        ┌──────────▼──────────────────────────────┐           │
│                        │     Python Kafka Consumer               │           │
│                        │     (consumer.py)                       │           │
│                        │                                         │           │
│                        │  ┌──────────────┐  ┌────────────────┐  │           │
│                        │  │ PostgreSQL   │  │ MinIO / S3     │  │           │
│                        │  │ raw schema   │  │ Parquet files  │  │           │
│                        │  └──────┬───────┘  └───────┬────────┘  │           │
│                        └─────────┼──────────────────┼───────────┘           │
│                                  │                  │                       │
│                        ┌─────────▼────────┐  ┌──────▼─────────────┐         │
│                        │  Apache Airflow  │  │  Apache Spark      │         │
│                        │  (Orchestrator)  │─▶│  Big Data Cluster  │         │
│                        │                  │  │  s3a:// analytics  │         │
│                        │  ┌────────────┐  │  └──────┬─────────────┘         │
│                        │  │    dbt     │  │         │                       │
│                        │  └──────┬─────┘  │         │ Trains AI Models      │
│                        └─────────┼────────┘         │                       │
│                                  │                  │                       │
│                        ┌─────────▼────────┐         │                       │
│                        │  PostgreSQL      │         │                       │
│                        │  analytics       │         │                       │
│                        └─────────┬────────┘         │                       │
│                                  │                  │                       │
│                        ┌─────────▼────────┐  ┌──────▼─────────────┐         │
│                        │  Metabase BI     │  │ FastAPI ML Service │         │
│                        │  Dashboards      │  │ Recommender/Churn  │         │
│                        └──────────────────┘  └────────────────────┘         │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## 🧠 The Lambda Architecture (PostgreSQL vs Apache Spark)

Foodingo implements a **Lambda Architecture** to handle both operational reporting and massive Big Data compute without ever slowing down the live application.

1. **The Application Database (MongoDB):** 
   This is the live database powering the Spring Boot backend. It is purely for real-time CRUD operations.
   
2. **The "Hot Path" (PostgreSQL + dbt):** 
   PostgreSQL acts as the analytical data warehouse. It is incredibly fast for everyday business intelligence (Metabase dashboards on gigabytes of data). However, as a single-node server, it has a hard limit on how much data it can process quickly.
   
3. **The "Cold/Big Data Path" (MinIO S3 + Apache Spark):** 
   If Foodingo grows to **billions of rows** (Terabytes of data), a single PostgreSQL server will choke. To solve this, the Kafka Consumer constantly backs up all events as compressed **Apache Parquet files** into the MinIO (S3) Data Lake. 
   When data scientists need to process these billions of rows, they use **Apache Spark**. Spark is a *distributed compute engine* (with no hard drive of its own). It reads the raw data directly from the S3 data lake and spreads the math across multiple worker nodes in memory, completely bypassing PostgreSQL. This guarantees that massive machine learning jobs never slow down your daily operations!

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
| **MinIO** | latest | S3-compatible data lake (Cold Path) |
| **Apache Spark** | 3.5.1 | Big Data distributed analytics on MinIO |
| **Jupyter** | PySpark 3.5 | Interactive data lake querying UI |
| **Apache Airflow** | 2.9.1 | Workflow orchestration (DAGs) |
| **dbt (Data Build Tool)** | 1.7.9 | SQL-based data transformations |
| **FastAPI** | latest | ML model serving API |
| **scikit-learn** | latest | ML: Collaborative Filtering, Logistic Regression |
| **Metabase** | latest | Business Intelligence dashboards |

### Infrastructure
| Technology | Purpose |
|---|---|
| **Docker & Docker Compose** | Container orchestration |
| **MongoDB** | 7.0 with Replica Set for CDC |
| **Maven** | 3.8.5 for Java builds |

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
│
├── docker-compose-pipeline.yml   # 🐳 Full 14-service data pipeline orchestration
│
├── kafka/                        # 📨 Kafka topic initialization
│   └── init-topics.sh            #    Creates 13 topics on startup
│
├── debezium/                     # 🔄 Change Data Capture configuration
│   └── mongodb-connector.json    #    Debezium MongoDB connector config
│
├── mongodb/                      # 🍃 MongoDB replica set initialization
│   └── init-replica-set.js       #    Initializes rs0 replica set for CDC
│
├── kafka-consumer/               # 🐍 Python Kafka Consumer (ETL)
│   ├── consumer.py               #    Consumes Kafka → writes PostgreSQL + MinIO
│   ├── Dockerfile                #    Consumer Docker image
│   └── requirements.txt          #    Python dependencies
│
├── postgres/                     # 🐘 PostgreSQL Data Warehouse
│   └── init.sql                  #    Creates raw + analytics schemas (8 tables)
│
├── minio/                        # 📦 MinIO (S3-compatible) Data Lake
│   └── init-buckets.sh           #    Creates foodingo-data-lake bucket
│
├── dbt/                          # 🔧 dbt Data Transformations
│   ├── dbt_project.yml           #    Project configuration
│   ├── profiles.yml              #    PostgreSQL connection profile
│   └── models/
│       ├── staging/              #    3 view models (dedup + clean)
│       ├── facts/                #    2 table models (fact_orders, fact_cart_events)
│       └── marts/                #    5 table models (revenue, popularity, churn, funnel, ML matrix)
│
├── airflow/                      # 🌀 Apache Airflow DAGs
│   └── dags/
│       ├── foodingo_daily_pipeline.py   # Daily: dbt staging→facts→marts→ML retrain
│       └── foodingo_ml_retrain.py       # ML model retraining DAG
│
├── ml-service/                   # 🧠 FastAPI Machine Learning Service
│   ├── app.py                    #    FastAPI application (5 endpoints)
│   ├── recommender.py            #    Collaborative Filtering (Cosine Similarity)
│   ├── churn_predictor.py        #    Logistic Regression (Cart Abandonment)
│   ├── Dockerfile                #    ML service Docker image
│   └── requirements.txt          #    Python ML dependencies
│
├── metabase/                     # 📊 Metabase BI Dashboard
│   └── README.md                 #    Dashboard setup guide
│
├── architecture.md               # 📐 Detailed architecture documentation
└── atlas-backup/                 # 💾 MongoDB Atlas → local migration scripts
```

---

## 🌊 Data Flow — The 7 Stages

### Stage 1: Event Generation (Spring Boot → Kafka)

When a user interacts with the Foodingo app (registers, adds to cart, places an order), the Spring Boot backend **publishes real-time events** to Apache Kafka topics.

**How it works at the code level:**
- `KafkaPublishingService.java` is a safety wrapper around Spring's `KafkaTemplate`.
- It uses **fire-and-forget async publishing** — if Kafka is down, the main app still works.
- Each service (`OrderServiceImpl`, `CartServiceImpl`, `UserServiceImpl`) calls `kafkaPublishingService.publish(topic, key, event)`.
- Events are serialized to JSON using Spring's `JsonSerializer`.
- Spring Boot connects to Kafka on port **9094** (external listener).

**Topics produced by Spring Boot (9 topics):**
| Topic | Triggered When |
|---|---|
| `user.registered` | New user signs up |
| `user.login` | User logs in |
| `cart.item_added` | Item added to cart |
| `cart.item_removed` | Item removed from cart |
| `cart.cleared` | Cart emptied |
| `cart.item_deleted` | Single item deleted |
| `order.created` | Order placed (pre-payment) |
| `payment.verified` | Razorpay payment confirmed |
| `order.status_updated` | Order status changed |

### Stage 2: Change Data Capture (MongoDB → Debezium → Kafka)

In parallel to the application events, **Debezium** watches MongoDB for any direct database changes (inserts, updates, deletes) and streams them to Kafka.

**How it works:**
- MongoDB runs as a **Replica Set** (`rs0`) — required for Change Streams.
- Debezium's MongoDB connector uses `change_streams_update_full` capture mode.
- It watches 4 collections: `foodies.orders`, `foodies.users`, `foodies.food`, `foodies.carts`.
- Changes are published to Kafka topics prefixed with `foodingo.foodies.*`.

**Topics produced by Debezium (4 topics):**
| Topic | MongoDB Collection |
|---|---|
| `foodingo.foodies.orders` | Orders collection |
| `foodingo.foodies.users` | Users collection |
| `foodingo.foodies.food` | Food items collection |
| `foodingo.foodies.carts` | Cart collection |

### Stage 3: Kafka Consumer (Kafka → PostgreSQL + MinIO)

A **Python consumer** (`consumer.py`) subscribes to all 13 Kafka topics and performs dual writes:

1. **PostgreSQL (raw schema):** Writes structured rows to 4 raw tables (`raw.user_events`, `raw.cart_events`, `raw.order_events`, `raw.cdc_events`).
2. **MinIO/S3 (Parquet):** Converts events to columnar Parquet format and uploads to `foodingo-data-lake` bucket, partitioned by `year/month/day/hour`.

**Why dual-write?**
- PostgreSQL is optimized for **fast SQL queries** (used by dbt and Metabase).
- Parquet on S3 is optimized for **long-term archival** and big data tools like Spark.

### Stage 4: dbt Data Transformations (raw → analytics)

**dbt (Data Build Tool)** transforms the raw event data into clean, business-ready analytics tables using a 3-layer architecture:

| Layer | Model Count | Materialization | Purpose |
|---|---|---|---|
| **Staging** | 3 views | `VIEW` | Deduplication, type casting, filtering |
| **Facts** | 2 tables | `TABLE` | Core business facts (orders, cart events) |
| **Marts** | 5 tables | `TABLE` | Business-specific aggregations |

**The 10 dbt Models:**
| Model | Layer | What It Does |
|---|---|---|
| `stg_orders` | Staging | Deduplicates raw orders by `(order_id, event_type)` |
| `stg_cart` | Staging | Cleans raw cart events |
| `stg_users` | Staging | Cleans raw user events |
| `fact_orders` | Facts | One row per paid order with item count, order hour |
| `fact_cart_events` | Facts | Cleaned cart interactions with date/hour extraction |
| `daily_revenue` | Marts | Daily revenue, order count, avg order value |
| `food_popularity` | Marts | Per-food-item order count and revenue |
| `cart_abandonment` | Marts | Users who added to cart but didn't order |
| `user_funnel` | Marts | Daily conversion funnel (register→login→cart→order) |
| `ml_user_order_matrix` | Marts | User×Food order count matrix for ML |

### Stage 5: Airflow Orchestration (Automating Spark & dbt)

Apache Airflow runs the `foodingo_daily_pipeline` DAG **every day at 2:00 AM IST** (20:30 UTC). This is the "brain" that runs all background tasks while the CEO is sleeping.

**DAG Task Chain:**
```
dbt_run_staging → dbt_run_facts → dbt_run_marts → trigger_spark_job → retrain_ml_models
```
*Note: The `trigger_spark_job` step spins up the Apache Spark cluster, points it to the S3 Data Lake, crunches the billions of rows of historical data, and prepares the heavy matrix calculations for the Machine Learning models.*

### Stage 6: Machine Learning Service (Trained by Spark)

The FastAPI ML service provides two AI models that rely on the heavy lifting done by Apache Spark:

1. **Food Recommender** (Collaborative Filtering with Cosine Similarity)
   - **Trained by Spark**: Spark crunches years of historical data from the S3 Data Lake to generate the `analytics.ml_user_order_matrix`.
   - Builds an item-item similarity matrix
   - For known users: recommends foods similar to what they've ordered
   - For new users: returns the top-10 most popular foods (cold-start fallback)
   - Saves model as `/tmp/recommender_model.pkl` inside the container

2. **Churn Predictor** (Logistic Regression)
   - Reads `analytics.cart_abandonment` data
   - Features: `days_since_cart`, `cart_item_count`, `has_ordered_after`
   - Output: probability (0–1) of abandonment + risk label (low/medium/high)
   - Provides actionable recommendation (e.g., "Send recovery email with discount")

### Stage 7: Business Intelligence (Metabase)

Metabase connects directly to the PostgreSQL `analytics` schema and provides:
- Interactive dashboards and charts
- SQL query builder
- Automated email reports
- Self-service analytics for non-technical stakeholders

---

## 🔗 How Services Interact

```
Spring Boot (:8080)
    │
    ├──[MongoDB]──▶ MongoDB (:27017) ──[CDC]──▶ Debezium ──▶ Kafka
    │
    ├──[Kafka Producer]──▶ Kafka (:9094 external / :9092 internal)
    │                           │
    │                           ▼
    │                     Python Consumer ──┬──▶ PostgreSQL (:5432)
    │                                      └──▶ MinIO (:9000)
    │
    ├──[HTTP GET]──▶ ML Service (:5001) ──[SQL]──▶ PostgreSQL
    │
    └──[Swagger UI]──▶ http://localhost:8080/swagger-ui.html

Airflow (:8089) ──[BashOperator]──▶ dbt ──[SQL]──▶ PostgreSQL
                 ──[HTTP POST]──▶ ML Service (:5001/train/recommender)

Metabase (:3000) ──[SQL]──▶ PostgreSQL (analytics schema)

Kafka UI (:8090) ──[Admin]──▶ Kafka (view topics, messages, consumer groups)

MinIO Console (:9001) ──[Admin]──▶ MinIO (view Parquet files in data lake)
```

---

## ✅ Prerequisites

- **Docker Desktop** (4.0+) with at least **6 GB RAM** allocated
- **Java 17** or later (for running Spring Boot locally)
- **Maven 3.8+** (included via `./mvnw` wrapper)
- **Node.js 18+** (only if running the React frontend)

---

## 🚀 Quick Start

### Step 1: Clone and Configure

```bash
git clone https://github.com/your-repo/foodingo-main.git
cd foodingo-main
```

Copy `.env.example` to `.env` and fill in your credentials (MongoDB URI, JWT secret, AWS keys, Razorpay keys).

### Step 2: Start the Data Pipeline (Docker Compose)

```bash
docker compose -f docker-compose-pipeline.yml --env-file .env up -d
```

This boots **14 containers** in dependency order. Wait ~2 minutes for everything to be healthy.

### Step 3: Verify All Services Are Running

```bash
docker compose -f docker-compose-pipeline.yml ps
```

All containers should show `Up` or `Healthy` status.

### Step 4: Start the Spring Boot Backend

```bash
# Load environment variables
source .env

# Run the application
./mvnw spring-boot:run
```

The backend starts on `http://localhost:8080`.

### Step 5: Test the API

Open Swagger UI: **http://localhost:8080/swagger-ui.html**

1. Register a user via `POST /api/user/register`
2. Login via `POST /api/user/login` (copy the JWT token)
3. Add items to cart via `POST /api/cart`
4. Place an order via `POST /api/orders`

### Step 6: Run the dbt Pipeline

Either wait for Airflow to run at 2 AM IST, or trigger manually:

```bash
docker exec -i foodingo-airflow-webserver bash -c \
  "POSTGRES_HOST=postgres POSTGRES_PORT=5432 POSTGRES_DB=foodingo_warehouse \
   POSTGRES_USER=foodingo POSTGRES_PASSWORD=foodingo123 \
   dbt run --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt"
```

### Step 7: Check Your Analytics

```bash
docker exec -i foodingo-postgres psql -U foodingo -d foodingo_warehouse -c \
  "SELECT * FROM analytics.fact_orders;"
```

---

## 🔐 Environment Variables

| Variable | Description | Example |
|---|---|---|
| `MONGODB_URI` | MongoDB connection string | `mongodb://localhost:27017/foodies?replicaSet=rs0` |
| `JWT_SECRET` | JWT signing key (min 32 chars) | `your-secret-key-here` |
| `AWS_ACCESS_KEY` | AWS S3 access key | `AKIA...` |
| `AWS_SECRET_KEY` | AWS S3 secret key | `U9ti...` |
| `AWS_S3_BUCKET` | Food images S3 bucket | `tapesh-myfood-images` |
| `AWS_REGION` | AWS region for images | `eu-north-1` |
| `RAZORPAY_KEY` | Razorpay API key | `rzp_test_...` |
| `RAZORPAY_SECRET` | Razorpay API secret | `OFLTu...` |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker address | `localhost:9094` |
| `ML_SERVICE_URL` | ML service URL | `http://localhost:5001` |
| `AWS_DATALAKE_BUCKET` | Data lake S3 bucket | `foodingo-data-lake` |

---

## 🌐 Service Ports & Dashboards

| Service | Port | URL |
|---|---|---|
| Spring Boot API | 8080 | http://localhost:8080 |
| Swagger UI | 8080 | http://localhost:8080/swagger-ui.html |
| Kafka UI | 8090 | http://localhost:8090 |
| Schema Registry | 8081 | http://localhost:8081 |
| Kafka Connect | 8083 | http://localhost:8083 |
| Airflow | 8089 | http://localhost:8089 (admin/admin) |
| ML Service (FastAPI) | 5001 | http://localhost:5001/docs |
| Metabase | 3000 | http://localhost:3000 |
| MinIO Console | 9001 | http://localhost:9001 (foodingo/foodingo123) |
| MinIO API | 9000 | http://localhost:9000 |
| PostgreSQL | 5432 | `psql -U foodingo -d foodingo_warehouse` |
| MongoDB | 27017 | `mongosh localhost:27017` |
| Kafka (internal) | 9092 | Used by Docker services |
| Kafka (external) | 9094 | Used by Spring Boot on host |

---

## 🧪 Testing the Full Pipeline

### End-to-End Test Walkthrough

1. **Register + Login** via Swagger UI → check Kafka UI for `user.registered` topic
2. **Add to Cart** → check `cart.item_added` topic in Kafka UI
3. **Place Order** → check `order.created` topic
4. **Verify PostgreSQL raw data:**
   ```bash
   docker exec -i foodingo-postgres psql -U foodingo -d foodingo_warehouse -c \
     "SELECT count(*) FROM raw.order_events;"
   ```
5. **Trigger Airflow DAG** → click Play on `foodingo_daily_pipeline` in Airflow UI
6. **Check analytics tables:**
   ```bash
   docker exec -i foodingo-postgres psql -U foodingo -d foodingo_warehouse -c \
     "SELECT * FROM analytics.fact_orders;"
   ```
7. **Train ML model:**
   ```bash
   curl -X POST http://localhost:5001/train/recommender
   ```
8. **Get recommendations:**
   ```bash
   curl http://localhost:5001/recommend/YOUR_USER_ID
   ```
9. **Open Metabase** at http://localhost:3000 and create dashboards

---

## 🔧 Troubleshooting

### Kafka won't start (`NodeExistsException`)
This happens when Docker Desktop is restarted and Zookeeper has stale session data.
```bash
docker compose -f docker-compose-pipeline.yml rm -sf zookeeper kafka
docker compose -f docker-compose-pipeline.yml --env-file .env up -d
```

### Airflow dbt tasks fail
Ensure the Airflow containers have dbt installed and the dbt directory mounted:
```yaml
# docker-compose-pipeline.yml — airflow-scheduler/webserver
_PIP_ADDITIONAL_REQUIREMENTS: "dbt-postgres==1.7.9"
volumes:
  - ./dbt:/opt/airflow/dbt
```

### Analytics tables are empty
Run dbt manually to verify:
```bash
docker exec -i foodingo-airflow-webserver bash -c \
  "POSTGRES_HOST=postgres dbt run --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt"
```

---

## 📚 Component Deep-Dive READMEs

Each component has its own detailed README with deep technical explanations:

| Component | README |
|---|---|
| Kafka (Event Streaming) | [kafka/README.md](kafka/README.md) |
| Debezium (CDC) | [debezium/README.md](debezium/README.md) |
| MongoDB (Replica Set) | [mongodb/README.md](mongodb/README.md) |
| Kafka Consumer (ETL) | [kafka-consumer/README.md](kafka-consumer/README.md) |
| PostgreSQL (Warehouse) | [postgres/README.md](postgres/README.md) |
| MinIO (Data Lake) | [minio/README.md](minio/README.md) |
| dbt (Transformations) | [dbt/README.md](dbt/README.md) |
| Airflow (Orchestration) | [airflow/README.md](airflow/README.md) |
| ML Service (FastAPI) | [ml-service/README.md](ml-service/README.md) |
| Metabase (BI) | [metabase/README.md](metabase/README.md) |

---

## 📄 License

This project is built for educational and portfolio purposes.
