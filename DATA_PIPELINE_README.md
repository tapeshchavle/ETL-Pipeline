# 🍔 Foodingo — Data Engineering Pipeline

Complete production-level data pipeline with Kafka, Debezium CDC, PostgreSQL, dbt, ML (FastAPI), Airflow, and Metabase.

---

## 📋 Prerequisites

```bash
# Install MongoDB Database Tools (for Atlas migration)
brew install mongodb-database-tools

# Docker Desktop with ≥ 6 GB RAM allocated
# Docker Desktop → Settings → Resources → Memory → 6 GB
```

---

## 🚀 Step-by-Step Local Setup

### Step 1 — Start the Pipeline (Docker)

```bash
cd data-pipeline
docker compose -f docker-compose-pipeline.yml up -d

# Takes ~2 minutes for all services to be healthy
docker compose -f docker-compose-pipeline.yml ps
```

### Step 2 — Migrate Data from Atlas to Local MongoDB

```bash
# Run the one-shot migration script (run ONCE)
chmod +x mongodb/migrate-from-atlas.sh
./mongodb/migrate-from-atlas.sh
```

Then update your `.env` file — comment Atlas URI, uncomment local:
```env
# MONGODB_URI="mongodb+srv://..."   ← comment this out
MONGODB_URI="mongodb://localhost:27017/foodies?replicaSet=rs0&directConnection=true"
```

Restart Spring Boot to pick up new MONGODB_URI.

### Step 3 — Restart Spring Boot

```bash
# Stop existing Spring Boot instance (Ctrl+C if running in terminal)
# Then restart:
cd /Users/tapeshchavle/Downloads/foodingo-main
./mvnw spring-boot:run
```

Spring Boot will now:
- Connect to local MongoDB (not Atlas)
- Publish Kafka events on every API call
- Serve ML recommendations via `/api/recommendations`

### Step 4 — Register Debezium Connector

Wait 60 seconds after startup, then:
```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @debezium/mongodb-connector.json
```

Verify connector is running:
```bash
curl http://localhost:8083/connectors/foodingo-mongodb-connector/status
```

### Step 5 — Test the Pipeline

```bash
# Hit any API endpoint and watch Kafka events flow in:
open http://localhost:8090    # Kafka UI — browse topics and messages

# Register a user
curl -X POST http://localhost:8080/api/user/register \
  -H "Content-Type: application/json" \
  -d '{"name":"Test User","email":"test@example.com","password":"test123"}'

# Check the event in Kafka UI → topic: user.registered
```

### Step 6 — Run dbt Transforms

```bash
# Install dbt
pip install dbt-postgres

# Run all models
cd dbt
POSTGRES_HOST=localhost POSTGRES_USER=foodingo POSTGRES_PASSWORD=foodingo123 \
  dbt run --profiles-dir . --project-dir .

# Run tests
dbt test --profiles-dir . --project-dir .
```

### Step 7 — Check ML Recommendations

```bash
# Login to get JWT token
TOKEN=$(curl -s -X POST http://localhost:8080/api/user/login \
  -H "Content-Type: application/json" \
  -d '{"email":"test@example.com","password":"test123"}' \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['token'])")

# Get food recommendations
curl http://localhost:8080/api/recommendations \
  -H "Authorization: Bearer $TOKEN"

# ML service Swagger UI
open http://localhost:5001/docs
```

### Step 8 — Open Dashboards

| Dashboard | URL | Login |
|-----------|-----|-------|
| **Kafka UI** | http://localhost:8090 | — |
| **MinIO Console** | http://localhost:9001 | foodingo / foodingo123 |
| **Metabase** | http://localhost:3000 | Follow setup wizard |
| **Airflow** | http://localhost:8089 | admin / admin |
| **ML Service** | http://localhost:5001/docs | — |

---

## 🗂️ Service Architecture

```
Spring Boot :8080  →  Kafka :9094  →  Consumer  →  PostgreSQL :5432
                           ↓                             ↓
                    MongoDB :27017                    dbt models
                    (Debezium CDC) ↗                      ↓
                                              Metabase + ML Service
```

---

## 🪣 AWS S3 Setup (Production)

Create the data lake bucket:
```bash
aws s3 mb s3://foodingo-data-lake --region us-east-1
```

Add to `.env`:
```env
AWS_DATALAKE_BUCKET=foodingo-data-lake
AWS_DATALAKE_REGION=us-east-1
```

---

## ⚠️ Port Reference

| Service | Port |
|---------|------|
| Spring Boot | 8080 |
| MongoDB (local) | 27017 |
| Kafka (external) | **9094** |
| Kafka (internal) | 9092 |
| Kafka UI | **8090** |
| Debezium Connect | 8083 |
| Schema Registry | 8081 |
| MinIO API | 9000 |
| MinIO Console | **9001** |
| PostgreSQL | 5432 |
| ML Service | **5001** |
| Airflow | **8089** |
| Metabase | **3000** |

> Spring Boot connects to Kafka on **9094** (external listener). All Docker services use **9092** (internal).

---

## 🛑 Stop Pipeline

```bash
cd data-pipeline
docker compose -f docker-compose-pipeline.yml down

# To also remove all data volumes (full reset):
docker compose -f docker-compose-pipeline.yml down -v
```
