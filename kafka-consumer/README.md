# 🐍 Kafka Consumer — Real-Time ETL Service

## Overview

The Kafka Consumer is a **Python-based ETL (Extract, Transform, Load) service** that subscribes to all 13 Kafka topics and performs **dual-write** operations:

1. **PostgreSQL (raw schema):** Writes structured rows into 4 normalized tables for SQL analytics
2. **MinIO / S3 (Parquet):** Converts events to columnar Apache Parquet format and uploads to the data lake for long-term archival

This is the **bridge** between the streaming world (Kafka) and the analytical world (PostgreSQL + dbt).

---

## Architecture

```
┌──────────────────────────────────────────┐
│           Apache Kafka (13 topics)        │
│                                          │
│  user.*  cart.*  order.*  foodingo.*      │
└──────────────────┬───────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────────┐
│      Python Kafka Consumer               │
│      (consumer.py)                       │
│                                          │
│      Consumer Group:                     │
│      foodingo-data-pipeline              │
│                                          │
│      auto_offset_reset: earliest         │
│      (reads ALL historical events)       │
│                                          │
│  ┌────────────────────────────────────┐  │
│  │   Message Router                   │  │
│  │                                    │  │
│  │   user.* topics                    │  │
│  │     → handle_user_event()          │  │
│  │     → INSERT INTO raw.user_events  │  │
│  │                                    │  │
│  │   cart.* topics                    │  │
│  │     → handle_cart_event()          │  │
│  │     → INSERT INTO raw.cart_events  │  │
│  │                                    │  │
│  │   order.* topics                   │  │
│  │     → handle_order_event()         │  │
│  │     → INSERT INTO raw.order_events │  │
│  │                                    │  │
│  │   foodingo.foodies.* topics (CDC)  │  │
│  │     → handle_cdc_event()           │  │
│  │     → INSERT INTO raw.cdc_events   │  │
│  └────────────────────────────────────┘  │
│                                          │
│  ┌────────────────────────────────────┐  │
│  │   Parquet Uploader                 │  │
│  │                                    │  │
│  │   Converts batch → Apache Parquet  │  │
│  │   Uploads to MinIO S3 bucket:      │  │
│  │   s3://foodingo-data-lake/         │  │
│  │     raw/events/{topic}/            │  │
│  │       year=2026/month=07/day=24/   │  │
│  │         hour=12/1721822400.parquet  │  │
│  └────────────────────────────────────┘  │
└──────────────────────────────────────────┘
         │                    │
         ▼                    ▼
   PostgreSQL            MinIO / S3
   (raw schema)          (Parquet files)
```

---

## Code Walkthrough

### 1. Kafka Connection with Retry Logic

```python
consumer = None
for attempt in range(20):
    try:
        consumer = KafkaConsumer(
            *TOPICS,
            bootstrap_servers=KAFKA_SERVERS,
            group_id="foodingo-data-pipeline",
            auto_offset_reset="earliest",  # Read all historical events
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )
        break
    except NoBrokersAvailable:
        time.sleep(5)  # Retry every 5 seconds for up to 100 seconds
```

**Why retry?** In Docker Compose, the consumer starts before Kafka is fully ready. The 20-attempt retry loop ensures it waits patiently.

### 2. Message Routing

```python
if topic in ("user.registered", "user.login"):
    handle_user_event(conn, payload, topic, partition, offset)
elif topic.startswith("cart."):
    handle_cart_event(conn, payload, topic, partition, offset)
elif topic in ("order.created", "payment.verified", "order.status_updated"):
    handle_order_event(conn, payload, topic, partition, offset)
elif topic.startswith("foodingo.foodies."):
    handle_cdc_event(conn, payload, topic)
```

### 3. PostgreSQL Inserts

Each handler performs a targeted `INSERT` into the appropriate raw table:

```python
def handle_order_event(conn, payload, topic, partition, offset):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO raw.order_events
                (event_type, order_id, user_id, ..., ordered_items, ...)
            VALUES (%s, %s, %s, ..., %s::jsonb, ...)
        """, (
            payload.get("eventType"),
            payload.get("orderId"),
            payload.get("userId"),
            json.dumps(payload.get("orderedItems", [])),  # JSON array → JSONB
            ...
        ))
    conn.commit()
```

**Key detail:** `ordered_items` is stored as PostgreSQL `JSONB` — this allows dbt to later use `jsonb_array_elements()` to explode it into individual food items.

### 4. Parquet Upload to MinIO

```python
def upload_parquet(s3, records, topic):
    df = pd.DataFrame(records)
    table = pa.Table.from_pandas(df)
    buf = BytesIO()
    pq.write_table(table, buf)

    key = f"raw/events/{topic.replace('.', '/')}/year={now.year}/month=.../..."
    s3.put_object(Bucket=MINIO_BUCKET, Key=key, Body=buf.read())
```

**Partition scheme:** `raw/events/<topic>/year=YYYY/month=MM/day=DD/hour=HH/<timestamp>.parquet`

This Hive-style partitioning allows tools like Apache Spark or Athena to efficiently query only the relevant time ranges.

---

## Dependencies

```
kafka-python    # Kafka consumer library
psycopg2-binary # PostgreSQL driver
pandas          # DataFrame construction
pyarrow         # Parquet serialization
boto3           # S3/MinIO client
```

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Kafka broker (Docker internal) |
| `POSTGRES_HOST` | `postgres` | PostgreSQL host |
| `POSTGRES_PORT` | `5432` | PostgreSQL port |
| `POSTGRES_DB` | `foodingo_warehouse` | Database name |
| `POSTGRES_USER` | `foodingo` | Database user |
| `POSTGRES_PASSWORD` | `foodingo123` | Database password |
| `MINIO_ENDPOINT` | `http://minio:9000` | MinIO/S3 endpoint |
| `MINIO_ACCESS_KEY` | `foodingo` | MinIO access key |
| `MINIO_SECRET_KEY` | `foodingo123` | MinIO secret key |
| `MINIO_BUCKET` | `foodingo-data-lake` | S3 bucket name |

---

## Files in This Directory

| File | Purpose |
|---|---|
| `consumer.py` | Main consumer application (249 lines) |
| `Dockerfile` | Docker image definition |
| `requirements.txt` | Python dependencies |

---

## Useful Commands

```bash
# View consumer logs
docker logs -f foodingo-kafka-consumer

# Check consumer group lag
docker exec -i foodingo-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group foodingo-data-pipeline

# Restart consumer after code changes
docker compose -f docker-compose-pipeline.yml up -d --build kafka-consumer
```

---

## Learn More

- [kafka-python Documentation](https://kafka-python.readthedocs.io/)
- [Apache Parquet Format](https://parquet.apache.org/)
- [boto3 S3 Documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html)
