"""
Foodingo Kafka Consumer
=======================
Consumes all Foodingo Kafka topics and:
  1. Writes raw events to PostgreSQL (raw schema)
  2. Uploads Parquet snapshots to MinIO (mirrors real AWS S3 data lake)

Topics consumed:
  - user.registered, user.login
  - cart.item_added, cart.item_removed, cart.cleared, cart.item_deleted
  - order.created, payment.verified, order.status_updated
  - foodingo.foodies.* (Debezium CDC topics)
"""

import json
import logging
import os
import time
from datetime import datetime, timezone
from io import BytesIO

import boto3
import pandas as pd
import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.config import Config
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

# ── Configuration ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
log = logging.getLogger("foodingo-consumer")

KAFKA_SERVERS   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
PG_HOST         = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT         = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB           = os.getenv("POSTGRES_DB", "foodingo_warehouse")
PG_USER         = os.getenv("POSTGRES_USER", "foodingo")
PG_PASS         = os.getenv("POSTGRES_PASSWORD", "foodingo123")
MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_KEY       = os.getenv("MINIO_ACCESS_KEY", "foodingo")
MINIO_SECRET    = os.getenv("MINIO_SECRET_KEY", "foodingo123")
MINIO_BUCKET    = os.getenv("MINIO_BUCKET", "foodingo-data-lake")

TOPICS = [
    "user.registered", "user.login",
    "cart.item_added", "cart.item_removed", "cart.cleared", "cart.item_deleted",
    "order.created", "payment.verified", "order.status_updated",
    "foodingo.foodies.orders", "foodingo.foodies.users",
    "foodingo.foodies.food", "foodingo.foodies.carts",
]

# ── Database Connection ───────────────────────────────────────────────────────
def get_pg_conn():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT,
        dbname=PG_DB, user=PG_USER, password=PG_PASS
    )

# ── MinIO / S3 Client ────────────────────────────────────────────────────────
def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_KEY,
        aws_secret_access_key=MINIO_SECRET,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

# ── Parquet Upload to MinIO ───────────────────────────────────────────────────
def upload_parquet(s3, records: list, topic: str):
    """Converts a batch of records to Parquet and uploads to MinIO."""
    if not records:
        return
    try:
        df = pd.DataFrame(records)
        table = pa.Table.from_pandas(df)
        buf = BytesIO()
        pq.write_table(table, buf)
        buf.seek(0)

        now = datetime.now(timezone.utc)
        key = (
            f"raw/events/{topic.replace('.', '/')}/"
            f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
            f"hour={now.hour:02d}/{int(now.timestamp())}.parquet"
        )
        s3.put_object(Bucket=MINIO_BUCKET, Key=key, Body=buf.read())
        log.info("Uploaded Parquet to MinIO: %s", key)
    except Exception as e:
        log.warning("Failed to upload Parquet to MinIO: %s", e)

# ── Upsert Handlers ───────────────────────────────────────────────────────────
def handle_user_event(conn, payload: dict, topic: str, partition: int, offset: int):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO raw.user_events
                (event_type, user_id, email, name, event_timestamp, kafka_topic, kafka_partition, kafka_offset)
            VALUES (%s, %s, %s, %s, to_timestamp(%s), %s, %s, %s)
        """, (
            payload.get("eventType"),
            payload.get("userId"),
            payload.get("email"),
            payload.get("name"),
            payload.get("timestamp", time.time()),
            topic, partition, offset,
        ))
    conn.commit()

def handle_cart_event(conn, payload: dict, topic: str, partition: int, offset: int):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO raw.cart_events
                (event_type, user_id, food_id, quantity, event_timestamp, kafka_topic, kafka_offset)
            VALUES (%s, %s, %s, %s, to_timestamp(%s), %s, %s)
        """, (
            payload.get("eventType"),
            payload.get("userId"),
            payload.get("foodId"),
            payload.get("quantity", 0),
            payload.get("timestamp", time.time()),
            topic, offset,
        ))
    conn.commit()

def handle_order_event(conn, payload: dict, topic: str, partition: int, offset: int):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO raw.order_events
                (event_type, order_id, user_id, user_address, email, phone_number,
                 amount, payment_status, order_status, razorpay_order_id,
                 razorpay_payment_id, ordered_items, event_timestamp, kafka_topic, kafka_offset)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, to_timestamp(%s), %s, %s)
        """, (
            payload.get("eventType"),
            payload.get("orderId"),
            payload.get("userId"),
            payload.get("userAddress"),
            payload.get("email"),
            payload.get("phoneNumber"),
            payload.get("amount", 0),
            payload.get("paymentStatus"),
            payload.get("orderStatus"),
            payload.get("razorpayOrderId"),
            payload.get("razorpayPaymentId"),
            json.dumps(payload.get("orderedItems", [])),
            payload.get("timestamp", datetime.now(timezone.utc).isoformat()),
            topic, offset,
        ))
    conn.commit()

def handle_cdc_event(conn, payload: dict, topic: str):
    collection = topic.split(".")[-1]   # foodies.orders → orders
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO raw.cdc_events
                (collection, operation, document_id, full_document, event_timestamp)
            VALUES (%s, %s, %s, %s::jsonb, %s)
        """, (
            collection,
            payload.get("__op", "u"),
            payload.get("_id", {}).get("$oid") if isinstance(payload.get("_id"), dict)
                else str(payload.get("_id", "")),
            json.dumps(payload),
            datetime.now(timezone.utc).isoformat(),
        ))
    conn.commit()

# ── Main Consumer Loop ────────────────────────────────────────────────────────
def main():
    log.info("Starting Foodingo Kafka Consumer...")

    # Retry until Kafka is available
    consumer = None
    for attempt in range(20):
        try:
            consumer = KafkaConsumer(
                *TOPICS,
                bootstrap_servers=KAFKA_SERVERS,
                group_id="foodingo-data-pipeline",
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                consumer_timeout_ms=1000,
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000,
            )
            log.info("Connected to Kafka at %s", KAFKA_SERVERS)
            break
        except NoBrokersAvailable:
            log.warning("Kafka not ready (attempt %d/20), retrying in 5s...", attempt + 1)
            time.sleep(5)

    if consumer is None:
        log.error("Could not connect to Kafka after 20 attempts. Exiting.")
        return

    conn = get_pg_conn()
    s3 = get_s3_client()
    log.info("Connected to PostgreSQL and MinIO")

    parquet_buffer: dict = {t: [] for t in TOPICS}

    log.info("Consuming from topics: %s", TOPICS)

    while True:
        try:
            for message in consumer:
                topic   = message.topic
                payload = message.value
                partition = message.partition
                offset = message.offset

                log.debug("Received [%s] offset=%d", topic, offset)

                try:
                    if topic in ("user.registered", "user.login"):
                        handle_user_event(conn, payload, topic, partition, offset)
                    elif topic.startswith("cart."):
                        handle_cart_event(conn, payload, topic, partition, offset)
                    elif topic in ("order.created", "payment.verified", "order.status_updated"):
                        handle_order_event(conn, payload, topic, partition, offset)
                    elif topic.startswith("foodingo.foodies."):
                        handle_cdc_event(conn, payload, topic)

                    # Buffer for Parquet upload
                    parquet_buffer[topic].append(payload)

                    # Upload Parquet immediately for testing
                    if len(parquet_buffer[topic]) >= 1:
                        upload_parquet(s3, parquet_buffer[topic], topic)
                        parquet_buffer[topic] = []

                except Exception as e:
                    log.error("Error processing message from [%s]: %s", topic, e)
                    conn = get_pg_conn()  # reconnect on DB error

        except Exception as e:
            log.error("Consumer loop error: %s", e)
            time.sleep(5)

if __name__ == "__main__":
    main()
