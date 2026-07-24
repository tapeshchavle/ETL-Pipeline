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
       Python Consumer → PostgreSQL raw.cdc_events
```

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
```

---

## Learn More

- [Debezium Documentation](https://debezium.io/documentation/)
- [Debezium MongoDB Connector](https://debezium.io/documentation/reference/2.5/connectors/mongodb.html)
- [MongoDB Change Streams](https://www.mongodb.com/docs/manual/changeStreams/)
