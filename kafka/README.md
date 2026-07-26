# 📨 Apache Kafka — Event Streaming Platform

## Overview

Apache Kafka is the **central nervous system** of Foodingo's data pipeline. Every user action (registration, login, cart interaction, order placement, payment) is captured as a structured JSON event and published to a specific Kafka topic. Kafka acts as a durable, distributed message queue that decouples the Spring Boot application from all downstream data processing.

---

## Why Kafka?

| Requirement | How Kafka Solves It |
|---|---|
| **Real-time data capture** | Events are published the instant they happen (sub-millisecond) |
| **Decoupling** | Spring Boot doesn't know or care who consumes the events |
| **Durability** | Events are persisted to disk — even if the consumer crashes, no data is lost |
| **Scalability** | 3 partitions per topic allow parallel consumption |
| **Replay** | Consumer can re-read events from any offset (useful for debugging/reprocessing) |

---

## Architecture in Foodingo

```
Spring Boot (Producer)          Debezium (CDC Producer)
       │                                  │
       │  KafkaPublishingService.java     │  mongodb-connector.json
       │  kafkaTemplate.send()            │  Change Streams → Kafka Connect
       ▼                                  ▼
┌──────────────────────────────────────────────┐
│              Apache Kafka Broker             │
│           (foodingo-kafka :9092/:9094)       │
│                                              │
│  ┌─── Application Events ─────────────────┐  │
│  │ user.registered    (3 partitions)       │  │
│  │ user.login         (3 partitions)       │  │
│  │ cart.item_added    (3 partitions)       │  │
│  │ cart.item_removed  (3 partitions)       │  │
│  │ cart.cleared       (3 partitions)       │  │
│  │ cart.item_deleted  (3 partitions)       │  │
│  │ order.created      (3 partitions)       │  │
│  │ payment.verified   (3 partitions)       │  │
│  │ order.status_updated (3 partitions)     │  │
│  └─────────────────────────────────────────┘  │
│                                              │
│  ┌─── CDC Events (Debezium) ──────────────┐  │
│  │ foodingo.foodies.orders  (3 partitions) │  │
│  │ foodingo.foodies.users   (3 partitions) │  │
│  │ foodingo.foodies.food    (3 partitions) │  │
│  │ foodingo.foodies.carts   (3 partitions) │  │
│  └─────────────────────────────────────────┘  │
│                                              │
│       Zookeeper (coordination :2181)         │
└──────────────────────────────────────────────┘
       │
       ▼
Python Kafka Consumer (consumer group: foodingo-data-pipeline)
       │
       ├──▶ PostgreSQL (raw schema)   ← For daily BI dashboards (Metabase)
       └──▶ AWS S3 (Parquet files)    ← For Big Data analytics (Spark)
```

---

## 🔬 How Kafka Works Internally — Step by Step

Here is the exact sequence of events when a user places an order in Foodingo:

### Step 1: Spring Boot Publishes the Event

When `OrderServiceImpl.java` creates an order, it calls:
```java
kafkaPublishingService.publish("order.created", orderId, orderEvent);
```

Inside `KafkaPublishingService.java`:
```java
kafkaTemplate.send(topic, key, event);
//           topic = "order.created"
//           key   = "ord_ABC123"     (the orderId)
//           event = { eventType, orderId, userId, amount, orderedItems, ... }
```

### Step 2: Producer Serializes the Message

Spring Kafka's `JsonSerializer` converts the Java `OrderEvent` object into a **JSON byte array**:
```
{ "eventType": "order.created", "orderId": "ord_ABC123", "amount": 599.0, ... }
→ [7B 22 65 76 65 6E 74 54 79 70 65 22 ...]  (UTF-8 bytes)
```

The `StringSerializer` converts the key (`orderId`) into bytes separately.

### Step 3: Producer Picks a Partition

The producer determines **which of the 3 partitions** receives this message using:
```
partition = hash(key) % number_of_partitions
          = hash("ord_ABC123") % 3
          = 1  (for example)
```

**Why this matters:** All events with the **same orderId** will always go to the **same partition**. This guarantees that `order.created` and `payment.verified` for the same order arrive in the exact chronological order they were sent.

### Step 4: Producer Sends to Broker

The producer batches the message with other pending messages (for efficiency) and sends a single network request to the Kafka broker. The broker is running inside the `foodingo-kafka` Docker container.

### Step 5: Broker Writes to Commit Log

The Kafka broker appends the message to a **commit log file on disk**:
```
/var/lib/kafka/data/order.created-1/00000000000000000000.log
                     ↑ topic name  ↑ partition number
```

This is an **append-only, immutable file**. Messages are never deleted until the retention period expires (default: 7 days). This is why Kafka is so fast — sequential disk writes are faster than random reads.

### Step 6: Consumer Polls the Message

The Python Kafka Consumer (`consumer.py`) runs `consumer.poll()` in a loop. It fetches a batch of messages from all 39 partitions (13 topics × 3 partitions) in a single network call.

```python
for message in consumer:
    topic = message.topic        # "order.created"
    partition = message.partition # 1
    offset = message.offset      # 42 (the 42nd message in this partition)
    payload = message.value      # { "eventType": "order.created", ... }
```

### Step 7: Consumer Commits Offset

After successfully processing the message (writing to PostgreSQL + S3), the consumer **commits the offset** back to Kafka:
```
"I have successfully processed offset 42 on partition 1 of order.created"
```

If the consumer crashes before committing, Kafka will re-deliver the message on restart. This guarantees **at-least-once delivery**.

---

## 🔗 How Kafka Interacts With Other Components

### Data Flow: Spring Boot → Kafka → Consumer → PostgreSQL + S3

```
┌─────────────────────────────────────────────────────────────────────┐
│ SPRING BOOT (Host Machine :8080)                                   │
│                                                                     │
│  OrderServiceImpl.java                                              │
│    └─ kafkaPublishingService.publish("order.created", orderId, event)│
│                                                                     │
│  KafkaPublishingService.java                                        │
│    └─ kafkaTemplate.send(topic, key, event)                         │
│       └─ Connects to: localhost:9094 (EXTERNAL listener)            │
└─────────────────────────┬───────────────────────────────────────────┘
                          │ JSON over TCP
                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│ KAFKA BROKER (Docker: foodingo-kafka)                               │
│                                                                     │
│  Receives on :9094 (EXTERNAL) → routes internally on :9092          │
│  Writes to commit log: /var/lib/kafka/data/order.created-{0,1,2}/   │
│  Stores message for 7 days (default retention)                      │
└─────────────────────────┬───────────────────────────────────────────┘
                          │ Consumer polls via :9092 (INTERNAL)
                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│ PYTHON CONSUMER (Docker: foodingo-kafka-consumer)                   │
│                                                                     │
│  consumer.py → KafkaConsumer(bootstrap_servers="kafka:9092")        │
│    └─ if topic == "order.created":                                  │
│         ├─ handle_order_event() → INSERT INTO raw.order_events      │
│         └─ upload_parquet()     → boto3 PUT to AWS S3               │
└─────────────────────────┬──────────────────┬────────────────────────┘
                          │                  │
                          ▼                  ▼
                   PostgreSQL           AWS S3 Data Lake
                   (raw schema)         (Parquet files)
                       │                     │
                    dbt + Metabase       Apache Spark
                    (CEO dashboards)    (ML model training)
```

### Data Flow: MongoDB → Debezium → Kafka → Consumer

```
┌──────────────────────────────────────────────────────────┐
│ MONGODB (Docker: foodingo-mongodb)                        │
│                                                          │
│  Spring Boot writes: db.orders.insertOne({...})          │
│  MongoDB writes to oplog (replica set operation log)     │
└──────────────────────┬───────────────────────────────────┘
                       │ Change Stream (oplog tailing)
                       ▼
┌──────────────────────────────────────────────────────────┐
│ KAFKA CONNECT + DEBEZIUM (Docker: foodingo-kafka-connect)│
│                                                          │
│  Debezium reads Change Stream → transforms document      │
│  ExtractNewDocumentState strips the envelope             │
│  Publishes to topic: foodingo.foodies.orders             │
└──────────────────────┬───────────────────────────────────┘
                       │
                       ▼
                  Kafka Broker → Python Consumer
                  (same flow as above)
```

### Exact JSON Payload at Each Step

**What Spring Boot sends to `order.created`:**
```json
{
  "eventType": "order.created",
  "orderId": "6a637898aa977450348009d3",
  "userId": "6a6375b9aa977450348009d1",
  "userAddress": "Tapesh Chavle, E 129 patel nagar, Bhopal",
  "email": "tapeshchawle@gmail.com",
  "phoneNumber": "123456788",
  "amount": 16950.00,
  "paymentStatus": "Preparing",
  "orderStatus": "Preparing",
  "razorpayOrderId": "order_THNK7UljsjZCIj",
  "orderedItems": [
    { "name": "Paneer Paratha", "price": 100.0, "quantity": 1, "category": "paratha" },
    { "name": "Batata Poha", "price": 45.0, "quantity": 1, "category": "poha" }
  ],
  "timestamp": 1721822400
}
```

**What the Python Consumer writes to PostgreSQL (`raw.order_events`):**
```sql
INSERT INTO raw.order_events
  (event_type, order_id, user_id, user_address, email, phone_number,
   amount, payment_status, order_status, razorpay_order_id,
   ordered_items, event_timestamp, kafka_topic, kafka_partition, kafka_offset)
VALUES
  ('order.created', '6a637898...', '6a6375b9...', 'Tapesh Chavle, ...',
   'tapeshchawle@gmail.com', '123456788', 16950.00, 'Preparing', 'Preparing',
   'order_THNK7UljsjZCIj', '[{"name":"Paneer Paratha",...}]'::jsonb,
   to_timestamp(1721822400), 'order.created', 1, 42);
```

**What the Python Consumer writes to AWS S3 (Parquet):**
```
s3://foodingo-data-lake/raw/events/order/created/year=2026/month=07/day=24/hour=14/1721822400.parquet
```

---

## Topic Configuration

All 13 topics are **auto-created** by the `kafka-init` container on startup using the `init-topics.sh` script.

### Topic Details

| Topic Name | Producer | Partition Key | Payload Example |
|---|---|---|---|
| `user.registered` | Spring Boot | `userId` | `{ "eventType": "user.registered", "userId": "abc123", "email": "user@example.com", "name": "John", "timestamp": 1721750400 }` |
| `user.login` | Spring Boot | `userId` | `{ "eventType": "user.login", "userId": "abc123", "timestamp": 1721750400 }` |
| `cart.item_added` | Spring Boot | `userId` | `{ "eventType": "cart.item_added", "userId": "abc123", "foodId": "food456", "quantity": 2, "timestamp": 1721750400 }` |
| `cart.item_removed` | Spring Boot | `userId` | `{ "eventType": "cart.item_removed", "userId": "abc123", "foodId": "food456" }` |
| `cart.cleared` | Spring Boot | `userId` | `{ "eventType": "cart.cleared", "userId": "abc123" }` |
| `cart.item_deleted` | Spring Boot | `userId` | `{ "eventType": "cart.item_deleted", "userId": "abc123", "foodId": "food456" }` |
| `order.created` | Spring Boot | `orderId` | `{ "eventType": "order.created", "orderId": "ord789", "userId": "abc123", "amount": 599.00, "orderedItems": [...] }` |
| `payment.verified` | Spring Boot | `orderId` | `{ "eventType": "payment.verified", "orderId": "ord789", "paymentStatus": "paid", "razorpayPaymentId": "pay_xxx" }` |
| `order.status_updated` | Spring Boot | `orderId` | `{ "eventType": "order.status_updated", "orderId": "ord789", "orderStatus": "Delivered" }` |
| `foodingo.foodies.orders` | Debezium | MongoDB `_id` | Full MongoDB document as JSON |
| `foodingo.foodies.users` | Debezium | MongoDB `_id` | Full MongoDB document as JSON |
| `foodingo.foodies.food` | Debezium | MongoDB `_id` | Full MongoDB document as JSON |
| `foodingo.foodies.carts` | Debezium | MongoDB `_id` | Full MongoDB document as JSON |

---

## How the Producer Works (Spring Boot Side)

The `KafkaPublishingService.java` is a **safety wrapper** around `KafkaTemplate`:

```java
@Service
public class KafkaPublishingService {
    @Autowired(required = false)  // If Kafka isn't running, bean is null
    private KafkaTemplate<String, Object> kafkaTemplate;

    public void publish(String topic, String key, Object event) {
        if (kafkaTemplate == null) return;  // Graceful skip
        try {
            kafkaTemplate.send(topic, key, event);  // Fire-and-forget
        } catch (Exception e) {
            log.warn("Failed to publish: {}", e.getMessage());
            // NEVER break the main app flow
        }
    }
}
```

**Key Design Decisions:**
- `required = false` — if Kafka is completely unavailable, the app still works normally
- Fire-and-forget — publishing is async, the API doesn't wait for Kafka acknowledgement
- Exception swallowing — a Kafka outage never causes an HTTP 500 to the end user

---

## Dual Listener Configuration

Kafka is configured with **two listeners** to serve both Docker-internal and host-machine clients:

| Listener | Port | Used By | Address |
|---|---|---|---|
| `INTERNAL` | 9092 | Docker containers (Consumer, Debezium, Airflow) | `kafka:9092` |
| `EXTERNAL` | 9094 | Spring Boot on host machine | `localhost:9094` |

This is configured in `docker-compose-pipeline.yml`:
```yaml
KAFKA_LISTENERS: INTERNAL://0.0.0.0:9092,EXTERNAL://0.0.0.0:9094
KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:9092,EXTERNAL://localhost:9094
```

**Why two listeners?** Spring Boot runs directly on your Mac (not inside Docker), so it connects via `localhost:9094`. But the Python consumer runs inside Docker, so it connects via `kafka:9092` (Docker's internal DNS). Without dual listeners, one of them wouldn't be able to connect.

---

## 📈 Scalability & Partition Design

### Why 13 Topics?

Each event type gets its own topic because:

1. **Independent Scaling:** If `order.created` gets 10x more traffic than `user.login`, you can add partitions to just that topic without touching the others.
2. **Independent Retention:** You might want to keep order events for 30 days but login events for only 7 days.
3. **Consumer Flexibility:** Future services can subscribe to only the topics they care about. A fraud detection system might only need `payment.verified`, not `cart.cleared`.
4. **Monitoring Clarity:** In the Kafka UI, you can see the exact throughput and lag for each event type separately.

### Why 3 Partitions Per Topic?

```
Topic: order.created
┌────────────┐  ┌────────────┐  ┌────────────┐
│ Partition 0 │  │ Partition 1 │  │ Partition 2 │
│             │  │             │  │             │
│ offset 0    │  │ offset 0    │  │ offset 0    │
│ offset 1    │  │ offset 1    │  │ offset 1    │
│ offset 2    │  │             │  │ offset 2    │
│ ...         │  │ ...         │  │ ...         │
└──────┬──────┘  └──────┬──────┘  └──────┬──────┘
       │                │                │
       └────────────────┼────────────────┘
                        │
              Consumer Instance 1
              (reads ALL 3 partitions)
```

The number 3 was chosen for these reasons:

1. **Parallelism:** Right now, we have 1 consumer instance that reads all 3 partitions sequentially. But if traffic grows, we can run **up to 3 consumer instances** — Kafka will automatically assign 1 partition to each consumer (via consumer group rebalancing). This triples the throughput.
2. **Key Distribution:** With 3 partitions, `hash(orderId) % 3` distributes orders roughly evenly across the partitions. With only 1 partition, everything would be sequential.
3. **Overhead Tradeoff:** Each partition creates a file handle and a log file on disk. Having 100 partitions per topic on a single broker would waste resources. 3 is the sweet spot for our scale.

### What Happens If You Add a 4th Partition?

```bash
# Add a 4th partition to order.created
docker exec -i foodingo-kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --alter --topic order.created --partitions 4
```

**What changes:**
1. A new partition directory is created on disk: `order.created-3/`
2. The consumer group **immediately rebalances** — the existing consumer now reads 4 partitions instead of 3
3. New messages with keys that hash to partition 3 start going there
4. **Old messages stay where they are** — they don't move to the new partition

**⚠️ Warning — Key Ordering Can Break:**
Before adding the partition, `hash("orderXYZ") % 3 = 1`, so all events for `orderXYZ` went to partition 1 (guaranteed order). After adding the partition, `hash("orderXYZ") % 4 = 2`, so NEW events for `orderXYZ` go to partition 2 while OLD events are still in partition 1. The consumer now reads them from two different partitions, possibly out of order.

**How to fix this:** If strict ordering is critical, stop all producers, let the consumer drain all existing messages, THEN add the partition and resume.

### Scaling the Consumer Group

Currently, 1 consumer instance reads all 39 partitions (13 topics × 3 partitions). To scale:

```
Scenario 1: 1 consumer (current)
  Consumer-1 reads: P0, P1, P2 of ALL 13 topics (39 partitions total)

Scenario 2: 3 consumers (scaled)
  Consumer-1 reads: P0 of all 13 topics (13 partitions)
  Consumer-2 reads: P1 of all 13 topics (13 partitions)
  Consumer-3 reads: P2 of all 13 topics (13 partitions)

Scenario 3: 4 consumers (over-provisioned)
  Consumer-1 reads: P0 of all 13 topics
  Consumer-2 reads: P1 of all 13 topics
  Consumer-3 reads: P2 of all 13 topics
  Consumer-4 reads: NOTHING (idle! — more consumers than partitions = wasted)
```

**Rule:** You can never have more consumer instances than partitions. Extra consumers sit idle.

### Replication Factor

| Setting | Dev (Current) | Production |
|---|---|---|
| Brokers | 1 | 3 |
| Replication Factor | 1 | 3 |
| `min.insync.replicas` | 1 | 2 |

Currently, `replication-factor 1` means each message exists on only 1 broker. If the broker's disk dies, all data is lost. In production with 3 brokers, each message would be replicated to all 3 brokers. Even if 1 broker dies, the other 2 still have the data.

### Retention Configuration

| Setting | Current Value | How to Change |
|---|---|---|
| `log.retention.hours` | 168 (7 days) | Set per-topic: `kafka-configs --alter --topic order.created --add-config retention.ms=2592000000` (30 days) |
| `log.retention.bytes` | unlimited | Set per-topic to cap disk usage |
| `log.segment.bytes` | 1 GB | Each partition log file rolls over at 1 GB |

---

## 🚀 Future Scaling Guide

| Current State | When to Scale | What to Do |
|---|---|---|
| 1 Kafka broker | > 50K messages/sec OR need fault tolerance | Add 2 more brokers, set `replication-factor: 3` |
| 3 partitions per topic | Consumer lag > 1000 consistently | Add partitions: `kafka-topics --alter --partitions 6` |
| 1 consumer instance | Consumer lag > 10000 | Scale to 3 instances (docker-compose replicas) |
| Zookeeper | Kafka 3.5+ | Migrate to KRaft mode (Zookeeper-free) |
| No Schema Registry usage | Teams disagree on event format | Enable Avro serialization with Schema Registry |

---

## Kafka UI

Access the Kafka management dashboard at **http://localhost:8090**.

You can:
- Browse all topics and their messages in real-time
- Inspect consumer group offsets and lag
- View partition distribution
- Monitor broker health

---

## Files in This Directory

| File | Purpose |
|---|---|
| `init-topics.sh` | Shell script executed by `kafka-init` container to create all 13 topics with 3 partitions each |

---

## Useful Commands

```bash
# List all topics
docker exec -i foodingo-kafka kafka-topics --bootstrap-server localhost:9092 --list

# Describe a specific topic (shows partitions, replicas, ISR)
docker exec -i foodingo-kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic order.created

# Read messages from a topic (from beginning)
docker exec -i foodingo-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic order.created --from-beginning

# Check consumer group lag
docker exec -i foodingo-kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group foodingo-data-pipeline

# Add a partition (careful — see warning above)
docker exec -i foodingo-kafka kafka-topics --bootstrap-server localhost:9092 --alter --topic order.created --partitions 4

# Change retention for a topic to 30 days
docker exec -i foodingo-kafka kafka-configs --bootstrap-server localhost:9092 --alter --topic order.created --add-config retention.ms=2592000000
```

---

## Learn More

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Confluent Kafka Docker Images](https://hub.docker.com/r/confluentinc/cp-kafka)
- [Kafka UI (Provectus)](https://github.com/provectuslabs/kafka-ui)
- [Kafka Partition Rebalancing](https://kafka.apache.org/documentation/#consumerconfigs_partition.assignment.strategy)
