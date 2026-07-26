# 🔄 Debezium — MongoDB Change Data Capture (CDC)

## Overview

Debezium is an open-source **Change Data Capture (CDC)** platform that watches your MongoDB database for any changes (inserts, updates, deletes) and streams them as structured events into Kafka topics — **in real-time, without modifying application code**.

In Foodingo, Debezium acts as the **second data source** alongside Spring Boot's explicit Kafka events. While Spring Boot publishes high-level business events (like "order created"), Debezium captures the **raw database mutations** — giving you a complete, audit-grade view of every single change that happened in MongoDB.

---

## Why Debezium + CDC?

| Problem | How Debezium Solves It |
|---|---|
| **Missed data changes** | Any direct MongoDB write (admin scripts, migrations) is captured |
| **Audit compliance** | Every insert/update/delete is recorded with before/after state |
| **Zero application changes** | CDC works at the database level — no code modifications needed |
| **Real-time streaming** | Changes appear in Kafka within milliseconds |
| **Schema evolution** | New fields added to MongoDB documents are automatically captured |

---

## Architecture in Foodingo

```
┌─────────────────────┐
│    MongoDB 7.0       │
│    Replica Set: rs0  │
│    Database: foodies │
│                      │
│  Collections:        │
│  ├── orders          │
│  ├── users           │
│  ├── food            │
│  └── carts           │
└──────────┬───────────┘
           │ Change Streams (oplog)
           ▼
┌──────────────────────────────────────┐
│   Kafka Connect + Debezium 2.5       │
│   Container: foodingo-kafka-connect  │
│   Port: 8083                         │
│                                      │
│   Connector: foodingo-mongodb-       │
│              connector               │
│                                      │
│   Capture Mode:                      │
│   change_streams_update_full         │
│                                      │
│   Transform: ExtractNewDocumentState │
└──────────────────────────────────────┘
           │
           ▼
┌──────────────────────────────────────┐
│         Apache Kafka                  │
│                                      │
│   foodingo.foodies.orders            │
│   foodingo.foodies.users             │
│   foodingo.foodies.food              │
│   foodingo.foodies.carts             │
└──────────────────────────────────────┘
           │
           ▼
┌──────────────────────────────────────┐
│      Python Kafka Consumer           │
│      (consumer.py)                   │
│                                      │
│  handle_cdc_event()                  │
│    ├──▶ PostgreSQL raw.cdc_events    │
│    └──▶ AWS S3 Parquet files         │
└──────────────────────────────────────┘
           │                  │
           ▼                  ▼
     Metabase (via dbt)    Spark (Big Data)
```

---

## 🔬 How CDC Works Internally — Step by Step

Here is the exact sequence when Spring Boot creates a new order in MongoDB:

### Step 1: Spring Boot Writes to MongoDB

```java
// OrderServiceImpl.java
OrderEntity savedOrder = orderRepo.save(orderEntity);
// MongoDB driver sends: db.orders.insertOne({...})
```

MongoDB writes the document to the `foodies.orders` collection AND simultaneously appends an entry to the **oplog** (operation log). The oplog is an internal MongoDB collection (`local.oplog.rs`) that records every write operation on the replica set.

### Step 2: Debezium Reads the Change Stream

Debezium doesn't read the oplog directly. Instead, it uses MongoDB's **Change Streams API** — a high-level, resumable cursor over the oplog:

```javascript
// Conceptually, what Debezium does internally:
const pipeline = [{ $match: { 'ns.db': 'foodies' } }];
const changeStream = db.watch(pipeline, { fullDocument: 'updateLookup' });

changeStream.on('change', (event) => {
  // event = {
  //   operationType: "insert",
  //   fullDocument: { _id: "abc123", amount: 599, orderedItems: [...] },
  //   ns: { db: "foodies", coll: "orders" },
  //   clusterTime: Timestamp(...)
  // }
});
```

### Step 3: Debezium Wraps in Envelope

Debezium wraps the Change Stream event in a **Debezium envelope** containing metadata:
```json
{
  "schema": { ... },
  "payload": {
    "before": null,
    "after": { "_id": "abc123", "amount": 599, "orderedItems": [...] },
    "source": {
      "connector": "mongodb",
      "ts_ms": 1721822400000,
      "snapshot": "false"
    },
    "op": "c",
    "ts_ms": 1721822400500
  }
}
```

### Step 4: ExtractNewDocumentState Strips the Envelope

Our connector configuration includes the `ExtractNewDocumentState` transform. This strips away the Debezium envelope and gives the downstream consumer just the **plain MongoDB document**:

**Before transform (raw Debezium envelope):**
```json
{ "schema": {...}, "payload": { "before": null, "after": { "_id": "abc123", "amount": 599 }, "op": "c" } }
```

**After transform (what the consumer receives):**
```json
{ "_id": "abc123", "amount": 599, "orderedItems": [...], "userId": "def456" }
```

This simplification is critical — without it, the Python consumer would have to manually parse the complex nested Debezium envelope.

### Step 5: Publish to Kafka Topic

The transformed document is published to the Kafka topic `foodingo.foodies.orders`. The topic name follows the pattern: `{topic.prefix}.{database}.{collection}` = `foodingo.foodies.orders`.

### Step 6: Debezium Stores Its Position

Debezium stores its current Change Stream **resume token** in a special Kafka topic called `connect-offsets`. This means:
- If Debezium restarts, it reads the resume token from `connect-offsets`
- It resumes the Change Stream from exactly where it left off
- **No events are missed, no events are duplicated**

---

## 🔗 How Debezium Interacts With Other Components

### Complete Data Flow

```
Step 1: User places order in Foodingo app
   │
   ▼
Step 2: Spring Boot saves to MongoDB
   │     OrderServiceImpl.java → orderRepo.save()
   ▼
Step 3: MongoDB writes to collection + oplog
   │     db.orders.insertOne({_id: "abc", amount: 599, ...})
   │     oplog entry: { op: "i", ns: "foodies.orders", o: {...} }
   ▼
Step 4: Debezium reads Change Stream
   │     Kafka Connect polls every 500ms
   │     Receives: { operationType: "insert", fullDocument: {...} }
   ▼
Step 5: Debezium publishes to Kafka
   │     Topic: foodingo.foodies.orders
   │     Key: { "_id": "abc" }
   │     Value: { "_id": "abc", "amount": 599, ... }
   ▼
Step 6: Python Consumer receives and dual-writes
   │     consumer.py → handle_cdc_event()
   ├──▶ PostgreSQL: INSERT INTO raw.cdc_events
   │      (collection, operation, document_id, full_document)
   └──▶ AWS S3: Parquet file at
          s3://foodingo-data-lake/raw/events/foodingo/foodies/orders/...
```

### Why Both Debezium CDC AND Spring Boot Kafka Events?

You might wonder: "If Spring Boot already publishes `order.created` to Kafka, why does Debezium ALSO capture the same order from MongoDB?"

| Spring Boot Events | Debezium CDC Events |
|---|---|
| **Business-level** events (high-level) | **Database-level** mutations (low-level) |
| Only fires when Spring Boot code runs | Captures ANY write (admin scripts, data migrations, direct MongoDB queries) |
| Custom JSON schema designed for analytics | Raw MongoDB document (full document as-is) |
| Only captures the events you explicitly code | Captures everything automatically |
| Goes to `order.created` topic | Goes to `foodingo.foodies.orders` topic |

**Use case:** If an admin directly updates an order's status in MongoDB Compass (without going through the Spring Boot API), Spring Boot would have no idea. But Debezium would catch it!

---

## Connector Configuration

The connector is defined in `mongodb-connector.json` and is **auto-registered** by the `debezium-init` container after Kafka Connect becomes healthy.

### Configuration Breakdown

```json
{
  "name": "foodingo-mongodb-connector",
  "config": {
    "connector.class": "io.debezium.connector.mongodb.MongoDbConnector",
    "mongodb.connection.string": "mongodb://mongodb:27017/?replicaSet=rs0",
    "mongodb.name": "foodingo",
    "database.include.list": "foodies",
    "collection.include.list": "foodies.orders,foodies.users,foodies.food,foodies.carts",
    "topic.prefix": "foodingo",
    "capture.mode": "change_streams_update_full",
    "snapshot.mode": "initial",
    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.connector.mongodb.transforms.ExtractNewDocumentState",
    "transforms.unwrap.drop.tombstones": "false",
    "transforms.unwrap.delete.handling.mode": "rewrite"
  }
}
```

| Setting | Value | Explanation |
|---|---|---|
| `connector.class` | `MongoDbConnector` | Debezium's MongoDB-specific connector |
| `mongodb.connection.string` | `mongodb://mongodb:27017/?replicaSet=rs0` | Connects to Docker MongoDB replica set |
| `database.include.list` | `foodies` | Only watches the `foodies` database |
| `collection.include.list` | `foodies.orders,...` | Only watches 4 specific collections |
| `topic.prefix` | `foodingo` | Topics become `foodingo.foodies.<collection>` |
| `capture.mode` | `change_streams_update_full` | Uses MongoDB Change Streams with full document on updates |
| `snapshot.mode` | `initial` | On first start, reads ALL existing data, then switches to streaming |
| `transforms.unwrap` | `ExtractNewDocumentState` | Flattens Debezium's envelope to just the document JSON |

---

## Capture Modes Explained

| Mode | Description | Used When |
|---|---|---|
| `change_streams` | Only sends the changed fields | Bandwidth optimization |
| `change_streams_update_full` ✅ | Sends the **complete document** after every change | **We use this** — simpler downstream processing |
| `change_streams_with_pre_image` | Sends both before and after states | Audit trails requiring before-state |

We chose `change_streams_update_full` because our Python consumer needs the complete document to write to PostgreSQL.

---

## How the Snapshot Works

When Debezium starts for the **first time** with `snapshot.mode: initial`:

1. It reads ALL existing documents from the 4 collections
2. Publishes them as `"create"` events to Kafka
3. Records its position in the MongoDB oplog
4. From that point forward, it only streams **new changes**

If you restart the connector, it resumes from the last recorded oplog position — **no data is re-read**.

---

## 📈 Scalability & Current Configuration

### Current Setup

| Setting | Value | Why |
|---|---|---|
| Kafka Connect workers | 1 | Single Docker container running Kafka Connect |
| Connector tasks | 1 (default) | Single task monitors all 4 collections |
| Throughput | ~5,000 events/sec | Sufficient for Foodingo's current traffic |
| Snapshot mode | `initial` | Captures existing data on first startup |

### How Debezium Handles High Throughput

Debezium uses **batching** internally. Instead of publishing one event at a time, it batches multiple Change Stream events into a single Kafka produce request. This means:
- At low traffic (10 events/sec): each event is published individually (~1ms latency)
- At high traffic (5000 events/sec): events are batched (~50ms latency, much higher throughput)

---

## 🚀 Future Scaling Guide

| Current State | When to Scale | What to Do |
|---|---|---|
| 1 task monitoring 4 collections | > 5,000 events/sec | Set `"tasks.max": "4"` — one task per collection for parallel processing |
| 1 Kafka Connect worker | > 20,000 events/sec | Run 3 Kafka Connect workers in a Connect cluster |
| `snapshot.mode: initial` | Frequent restarts causing re-reads | Switch to `snapshot.mode: never` after initial load |
| Single connector | Need to watch a 5th collection | Add it to `collection.include.list`, restart connector |
| No schema validation | Teams disagree on document format | Enable Avro serialization with Schema Registry |

### Adding a New Collection to Watch

If you add a new MongoDB collection (e.g., `foodies.reviews`):

1. Update `mongodb-connector.json`:
   ```json
   "collection.include.list": "foodies.orders,foodies.users,foodies.food,foodies.carts,foodies.reviews"
   ```
2. Create the new Kafka topic:
   ```bash
   docker exec -i foodingo-kafka kafka-topics --bootstrap-server localhost:9092 \
     --create --topic foodingo.foodies.reviews --partitions 3 --replication-factor 1
   ```
3. Restart the connector:
   ```bash
   curl -X DELETE http://localhost:8083/connectors/foodingo-mongodb-connector
   curl -X POST http://localhost:8083/connectors \
     -H "Content-Type: application/json" -d @debezium/mongodb-connector.json
   ```
4. Update the Python consumer to handle `foodingo.foodies.reviews` topic

---

## Files in This Directory

| File | Purpose |
|---|---|
| `mongodb-connector.json` | Debezium connector configuration (auto-registered by `debezium-init` container) |

---

## Useful Commands

```bash
# Check connector status
curl http://localhost:8083/connectors/foodingo-mongodb-connector/status | jq

# List all registered connectors
curl http://localhost:8083/connectors | jq

# Delete and re-register the connector
curl -X DELETE http://localhost:8083/connectors/foodingo-mongodb-connector
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @debezium/mongodb-connector.json

# View Kafka Connect plugins available
curl http://localhost:8083/connector-plugins | jq

# Check connector tasks and their status
curl http://localhost:8083/connectors/foodingo-mongodb-connector/tasks | jq
```

---

## Learn More

- [Debezium Documentation](https://debezium.io/documentation/)
- [Debezium MongoDB Connector](https://debezium.io/documentation/reference/2.5/connectors/mongodb.html)
- [MongoDB Change Streams](https://www.mongodb.com/docs/manual/changeStreams/)
- [Kafka Connect Concepts](https://docs.confluent.io/platform/current/connect/concepts.html)
