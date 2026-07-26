# 🐍 Kafka Consumer — Real-Time ETL Service

## Overview

The Kafka Consumer is a **Python-based ETL (Extract, Transform, Load) service** that subscribes to all 13 Kafka topics and performs **dual-write** operations:

1. **PostgreSQL (raw schema):** Writes structured rows into 4 normalized tables for SQL analytics
2. **AWS S3 (Parquet):** Converts events to columnar Apache Parquet format and uploads to the data lake for long-term archival and big data processing

This is the **bridge** between the streaming world (Kafka) and the analytical world (PostgreSQL + dbt + Metabase) AND the big data world (S3 + Spark + ML).

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
│  │   Converts event → Apache Parquet  │  │
│  │   Uploads to AWS S3 bucket:        │  │
│  │   s3://foodingo-data-lake/         │  │
│  │     raw/events/{topic}/            │  │
│  │       year=2026/month=07/day=24/   │  │
│  │         hour=12/1721822400.parquet  │  │
│  └────────────────────────────────────┘  │
└──────────────────────────────────────────┘
         │                    │
         ▼                    ▼
   PostgreSQL            AWS S3 Data Lake
   (raw schema)          (Parquet files)
      │                       │
      ▼                       ▼
   dbt → Metabase         Spark → ML Service
   (CEO dashboards)       (AI recommendations)
```

---

## 🔬 How the Consumer Works Internally — Step by Step

### Step 1: Connect to Kafka with Retry Logic

When the consumer container starts, Kafka may not be ready yet. The consumer retries up to 20 times (every 5 seconds):

```python
consumer = None
for attempt in range(20):
    try:
        consumer = KafkaConsumer(
            *TOPICS,                            # All 13 topics
            bootstrap_servers=KAFKA_SERVERS,     # "kafka:9092" (Docker internal)
            group_id="foodingo-data-pipeline",   # Consumer group name
            auto_offset_reset="earliest",        # Start from very first message
            enable_auto_commit=True,             # Auto-commit offsets
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )
        break
    except NoBrokersAvailable:
        time.sleep(5)  # Wait 5 seconds and retry
```

**Why `auto_offset_reset="earliest"`?** If this is a brand new consumer group, it starts reading from offset 0 (the very first message ever produced). This ensures no historical events are missed.

### Step 2: Poll Messages in an Infinite Loop

```python
for message in consumer:  # Blocks until a message arrives
    topic = message.topic        # e.g., "order.created"
    partition = message.partition # e.g., 1
    offset = message.offset      # e.g., 42
    payload = message.value      # Deserialized JSON dict
```

`KafkaConsumer.__next__()` internally calls `poll()`, which:
1. Sends a **FetchRequest** to the Kafka broker for all 39 partitions (13 topics × 3 partitions)
2. The broker returns a batch of messages (up to `max_poll_records`, default 500)
3. The consumer processes them one at a time

### Step 3: Route Message to Correct Handler

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

### Step 4: Write to PostgreSQL (Hot Path)

Each handler performs a targeted `INSERT` using `psycopg2`:

```python
def handle_order_event(conn, payload, topic, partition, offset):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO raw.order_events
                (event_type, order_id, user_id, user_address, email,
                 phone_number, amount, payment_status, order_status,
                 razorpay_order_id, razorpay_payment_id, ordered_items,
                 event_timestamp, ingested_at, kafka_topic, kafka_partition, kafka_offset)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb,
                    to_timestamp(%s), NOW(), %s, %s, %s)
        """, (
            payload.get("eventType"),
            payload.get("orderId"),
            payload.get("userId"),
            payload.get("userAddress"),
            payload.get("email"),
            payload.get("phoneNumber"),
            payload.get("amount"),
            payload.get("paymentStatus"),
            payload.get("orderStatus"),
            payload.get("razorpayOrderId"),
            payload.get("razorpayPaymentId"),
            json.dumps(payload.get("orderedItems", [])),  # JSON → JSONB
            payload.get("timestamp"),
            topic, partition, offset,
        ))
    conn.commit()
```

**Key detail:** `ordered_items` is stored as PostgreSQL `JSONB` — this allows dbt to later use `jsonb_array_elements()` to explode the array into individual food items for the `food_popularity` mart.

### Step 5: Write to AWS S3 (Cold Path)

Simultaneously, the same event is converted to Parquet and uploaded to AWS S3:

```python
def upload_parquet(s3, records, topic):
    # 1. Convert list of event dicts to a pandas DataFrame
    df = pd.DataFrame(records)

    # 2. Convert DataFrame to PyArrow Table (columnar format)
    table = pa.Table.from_pandas(df)

    # 3. Write the Table as a compressed Parquet file into an in-memory buffer
    buf = BytesIO()
    pq.write_table(table, buf)

    # 4. Build the Hive-partitioned S3 key
    now = datetime.utcnow()
    key = f"raw/events/{topic.replace('.', '/')}/year={now.year}/month={now.month:02d}/day={now.day:02d}/hour={now.hour:02d}/{int(now.timestamp())}.parquet"

    # 5. Upload to AWS S3 using boto3
    s3.put_object(
        Bucket=DATALAKE_BUCKET,  # "foodingo-data-lake"
        Key=key,                  # "raw/events/order/created/year=2026/month=07/..."
        Body=buf.getvalue()
    )
```

**The Hive-style partition scheme:**
```
s3://foodingo-data-lake/raw/events/order/created/year=2026/month=07/day=24/hour=14/1721829600.parquet
```

This enables Apache Spark to **partition prune** — if a Spark query only needs July 2026 data, it skips ALL Parquet files from other months, making queries orders of magnitude faster.

---

## 🔗 How the Consumer Interacts With Other Components

### Upstream: Where Messages Come From

```
┌──────────────────────────────────────────────────────────────────┐
│                    PRODUCERS (2 sources)                          │
│                                                                  │
│  1. Spring Boot (9 topics)                                       │
│     KafkaPublishingService.java → kafkaTemplate.send()           │
│     Topics: user.registered, user.login, cart.item_added,        │
│             cart.item_removed, cart.cleared, cart.item_deleted,   │
│             order.created, payment.verified, order.status_updated│
│                                                                  │
│  2. Debezium CDC (4 topics)                                      │
│     mongodb-connector.json → Kafka Connect                       │
│     Topics: foodingo.foodies.orders, foodingo.foodies.users,     │
│             foodingo.foodies.food, foodingo.foodies.carts        │
└──────────────────────────────────┬───────────────────────────────┘
                                   │
                                   ▼
                            Python Consumer
```

### Downstream: Where Data Goes

```
                            Python Consumer
                                   │
                    ┌──────────────┼──────────────┐
                    │              │              │
                    ▼              ▼              ▼
           ┌──────────────┐ ┌──────────┐ ┌──────────────┐
           │ PostgreSQL   │ │ AWS S3   │ │ Console Log  │
           │ raw schema   │ │ Parquet  │ │ (debugging)  │
           │              │ │          │ │              │
           │ user_events  │ │ raw/     │ │ "Processed   │
           │ cart_events  │ │ events/  │ │  order.created│
           │ order_events │ │ order/   │ │  offset=42"  │
           │ cdc_events   │ │ created/ │ │              │
           └──────┬───────┘ └────┬─────┘ └──────────────┘
                  │              │
                  ▼              ▼
           ┌──────────────┐ ┌──────────────┐
           │ dbt          │ │ Apache Spark │
           │ (transforms) │ │ (big data)   │
           └──────┬───────┘ └──────┬───────┘
                  │              │
                  ▼              ▼
           ┌──────────────┐ ┌──────────────┐
           │ Metabase     │ │ ML Service   │
           │ (CEO sees)   │ │ (AI models)  │
           └──────────────┘ └──────────────┘
```

---

## Dependencies

```
kafka-python    # Kafka consumer library
psycopg2-binary # PostgreSQL driver
pandas          # DataFrame construction
pyarrow         # Parquet serialization
boto3           # AWS S3 client
```

---

## Environment Variables

| Variable | Current Value | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Kafka broker (Docker internal) |
| `POSTGRES_HOST` | `postgres` | PostgreSQL host |
| `POSTGRES_PORT` | `5432` | PostgreSQL port |
| `POSTGRES_DB` | `foodingo_warehouse` | Database name |
| `POSTGRES_USER` | `foodingo` | Database user |
| `POSTGRES_PASSWORD` | `foodingo123` | Database password |
| `AWS_ACCESS_KEY_ID` | *(from .env)* | AWS access key for S3 data lake |
| `AWS_SECRET_ACCESS_KEY` | *(from .env)* | AWS secret key for S3 data lake |
| `AWS_DATALAKE_BUCKET` | `foodingo-data-lake` | S3 bucket name for the data lake |
| `AWS_DATALAKE_REGION` | `us-east-1` | AWS region where the S3 bucket lives |

> **Note:** The consumer connects to the real AWS S3 bucket `foodingo-data-lake` in `us-east-1`, not to the local MinIO container. MinIO is available for local testing if needed.

---

## 📈 Scalability & Current Configuration

### Current Setup

| Setting | Value | Why |
|---|---|---|
| Consumer instances | 1 | Sufficient for current traffic |
| Partitions consumed | 39 (13 topics × 3 partitions) | Single consumer reads all |
| Consumer group | `foodingo-data-pipeline` | Enables Kafka offset tracking |
| `auto_offset_reset` | `earliest` | Don't miss any events |
| `enable_auto_commit` | `True` | Simpler offset management |
| PostgreSQL connection | 1 persistent connection | Reused for all inserts |
| S3 upload strategy | Per-event | Each event = 1 Parquet file |

### Current Throughput

| Metric | Value |
|---|---|
| Events processed/sec | ~100 (current traffic) |
| PostgreSQL insert latency | ~2ms per event |
| S3 upload latency | ~50ms per event |
| End-to-end latency (Kafka → PostgreSQL) | ~5ms |
| End-to-end latency (Kafka → S3) | ~55ms |

---

## 🚀 Future Scaling Guide

### Level 1: Batch S3 Uploads (Easy Win)

Current: Each event triggers a separate S3 upload (expensive at high volume).
Improved: Buffer 100 events, write a single Parquet file with 100 rows:

```python
# Instead of uploading per-event:
buffer = []
for message in consumer:
    buffer.append(message.value)
    if len(buffer) >= 100:
        upload_parquet(s3, buffer, message.topic)
        buffer = []
```

**Impact:** Reduces S3 API calls by 100x, reduces cost, increases throughput.

### Level 2: Scale Consumer Instances (3x Throughput)

```yaml
# docker-compose-pipeline.yml
kafka-consumer:
  image: ...
  deploy:
    replicas: 3  # Run 3 consumer instances
```

Kafka will automatically assign partitions across the 3 instances:
- Consumer-1: partition 0 of all 13 topics
- Consumer-2: partition 1 of all 13 topics  
- Consumer-3: partition 2 of all 13 topics

**No code changes needed** — Kafka's consumer group protocol handles this automatically.

### Level 3: Async S3 Uploads (Non-Blocking)

Use `asyncio` or a thread pool to upload Parquet to S3 without blocking the Kafka message processing loop:

```python
from concurrent.futures import ThreadPoolExecutor

executor = ThreadPoolExecutor(max_workers=4)

for message in consumer:
    handle_order_event(conn, payload, ...)  # Sync PostgreSQL write
    executor.submit(upload_parquet, s3, payload, topic)  # Async S3 upload
```

### Level 4: Switch to Apache Flink or Spark Streaming

If traffic exceeds 100K events/sec, consider replacing the Python consumer with:
- **Apache Flink** for exactly-once processing guarantees
- **Spark Structured Streaming** for seamless integration with the existing Spark cluster

---

## Files in This Directory

| File | Purpose |
|---|---|
| `consumer.py` | Main consumer application |
| `Dockerfile` | Docker image definition |
| `requirements.txt` | Python dependencies |

---

## Useful Commands

```bash
# View consumer logs
docker logs -f foodingo-kafka-consumer

# Check consumer group lag (how far behind the consumer is)
docker exec -i foodingo-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group foodingo-data-pipeline

# Restart consumer after code changes
docker compose -f docker-compose-pipeline.yml up -d --build kafka-consumer

# Reset consumer offsets to re-process all events from scratch
docker exec -i foodingo-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group foodingo-data-pipeline \
  --reset-offsets --to-earliest --all-topics --execute
```

---

## Learn More

- [kafka-python Documentation](https://kafka-python.readthedocs.io/)
- [Apache Parquet Format](https://parquet.apache.org/)
- [boto3 S3 Documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html)
- [Kafka Consumer Groups](https://kafka.apache.org/documentation/#consumerconfigs)
