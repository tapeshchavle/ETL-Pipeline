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
       ├──▶ PostgreSQL (raw schema)
       └──▶ MinIO / S3 (Parquet files)
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
| `order.created` | Spring Boot | `orderId` | `{ "eventType": "order.created", "orderId": "ord789", "userId": "abc123", "amount": 599.00, "orderedItems": [...], "timestamp": 1721750400 }` |
| `payment.verified` | Spring Boot | `orderId` | `{ "eventType": "payment.verified", "orderId": "ord789", "paymentStatus": "paid", "razorpayPaymentId": "pay_xxx", "timestamp": 1721750400 }` |
| `foodingo.foodies.orders` | Debezium | MongoDB `_id` | Full MongoDB document as JSON |

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

# Describe a specific topic
docker exec -i foodingo-kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic order.created

# Read messages from a topic (from beginning)
docker exec -i foodingo-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic order.created --from-beginning

# Check consumer group lag
docker exec -i foodingo-kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group foodingo-data-pipeline
```

---

## Learn More

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Confluent Kafka Docker Images](https://hub.docker.com/r/confluentinc/cp-kafka)
- [Kafka UI (Provectus)](https://github.com/provectuslabs/kafka-ui)
