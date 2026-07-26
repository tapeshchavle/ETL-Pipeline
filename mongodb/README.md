# 🍃 MongoDB — Primary Application Database

## Overview

MongoDB 7.0 serves as the **primary OLTP (Online Transaction Processing) database** for the Foodingo application. It stores all live application data — users, food items, shopping carts, and orders — using a flexible document model that maps naturally to JSON.

In the data pipeline context, MongoDB runs as a **Replica Set** (`rs0`) which is a **hard requirement** for Debezium CDC to work. Replica sets enable MongoDB's Change Streams API, which Debezium uses to capture every insert, update, and delete in real-time.

---

## Why MongoDB?

| Requirement | How MongoDB Solves It |
|---|---|
| **Flexible schema** | Food items have varying attributes; documents handle this naturally |
| **JSON-native** | Spring Boot's Jackson serializer maps directly to BSON documents |
| **Embedded arrays** | `orderedItems` is stored as an embedded array inside the order document |
| **Change Streams** | Required for Debezium CDC — only available with Replica Sets |
| **Spring Data MongoDB** | First-class ORM support with `@Document`, `MongoRepository` |

---

## Database Structure

```
MongoDB Server (localhost:27017)
└── Database: foodies
    ├── Collection: users
    │   └── { _id, name, email, password(hashed), role, createdAt }
    │
    ├── Collection: food
    │   └── { _id, name, description, price, category, imageUrl, available }
    │
    ├── Collection: carts
    │   └── { _id, userId, foodId, quantity, foodName, foodPrice, foodImage }
    │
    └── Collection: orders
        └── { _id, userId, userAddress, email, phoneNumber, amount,
              orderedItems: [...], paymentStatus, orderStatus,
              razorpayOrderId, razorpayPaymentId, razorpaySignature }
```

---

## 🔬 How MongoDB Works Internally in Foodingo

### Step 1: Spring Boot Maps Java Objects to BSON

When a user places an order, Spring Boot creates an `OrderEntity` Java object:

```java
@Document(collection = "orders")
public class OrderEntity {
    @Id
    private String id;           // Maps to MongoDB's _id
    private String userId;
    private String userAddress;
    private double amount;
    private List<OrderItem> orderedItems;  // Embedded array
    private String paymentStatus;
    private String orderStatus;
    private String razorpayOrderId;
    // ...
}
```

When `orderRepo.save(orderEntity)` is called, Spring Data MongoDB:
1. Serializes the Java object to **BSON** (Binary JSON) using Jackson
2. Sends the BSON bytes over MongoDB's wire protocol to `localhost:27017`
3. MongoDB stores the BSON document in the `foodies.orders` collection

### Step 2: MongoDB Stores on Disk (WiredTiger)

MongoDB uses the **WiredTiger** storage engine:
- Documents are stored as compressed BSON on disk inside `/data/db/` (mapped to `mongo-data` Docker volume)
- WiredTiger uses **document-level locking** — two users placing orders simultaneously don't block each other
- Writes go to an in-memory write-ahead log (WAL) first, then flushed to disk (crash-safe)

### Step 3: MongoDB Writes to the Oplog

Because MongoDB runs as a Replica Set (`rs0`), every write also gets appended to the **oplog** (`local.oplog.rs`). The oplog is a capped collection that records:
```javascript
{
  "ts": Timestamp(1721822400, 1),      // When the write happened
  "op": "i",                           // "i" = insert, "u" = update, "d" = delete
  "ns": "foodies.orders",              // Database.Collection
  "o": { "_id": "abc123", ... }        // The actual document
}
```

This oplog is what Debezium reads via Change Streams to capture every database change in real-time.

---

## 🔗 How MongoDB Interacts With Other Components

```
┌───────────────────────────────────────────────────────────────┐
│ SPRING BOOT (Host Machine :8080)                              │
│                                                               │
│  UserServiceImpl.java                                         │
│    └─ userRepo.save(user) ──────────────────────────────┐     │
│  CartServiceImpl.java                                    │     │
│    └─ cartRepo.save(cartItem) ──────────────────────┐    │     │
│  OrderServiceImpl.java                               │    │     │
│    └─ orderRepo.save(order) ────────────────────┐    │    │     │
│  FoodServiceImpl.java                            │    │    │     │
│    └─ foodRepo.save(food) ─────────────────┐     │    │    │     │
└─────────────────────────────────────────────┼─────┼────┼────┼───┘
                                              │     │    │    │
                                              ▼     ▼    ▼    ▼
┌────────────────────────────────────────────────────────────────┐
│ MONGODB (Docker: foodingo-mongodb :27017)                      │
│                                                                │
│  Replica Set: rs0 (single member for dev)                      │
│                                                                │
│  foodies.food ◀────── FoodServiceImpl                          │
│  foodies.users ◀───── UserServiceImpl                          │
│  foodies.carts ◀───── CartServiceImpl                          │
│  foodies.orders ◀──── OrderServiceImpl                         │
│                                                                │
│  oplog (local.oplog.rs) ─── records every write ───┐           │
└────────────────────────────────────────────────────┼───────────┘
                                                     │
                                                     │ Change Streams
                                                     ▼
┌────────────────────────────────────────────────────────────────┐
│ DEBEZIUM (Docker: foodingo-kafka-connect :8083)                │
│                                                                │
│  Reads Change Stream for 4 collections                         │
│  Publishes to Kafka topics:                                    │
│    foodies.orders → foodingo.foodies.orders                    │
│    foodies.users  → foodingo.foodies.users                     │
│    foodies.food   → foodingo.foodies.food                      │
│    foodies.carts  → foodingo.foodies.carts                     │
└────────────────────────────────────────────────────────────────┘
                          │
                          ▼
                    Apache Kafka → Python Consumer → PostgreSQL + AWS S3
```

### The Dual-Path Data Flow

When a user places an order, MongoDB is involved in **two parallel data flows**:

**Path 1 (Explicit — Spring Boot Kafka Events):**
```
OrderServiceImpl.java → saves to MongoDB → publishes "order.created" to Kafka
```
Spring Boot explicitly calls `kafkaPublishingService.publish()` after saving to MongoDB. This is a high-level business event with a clean JSON schema.

**Path 2 (Implicit — Debezium CDC):**
```
MongoDB oplog → Debezium Change Stream → publishes raw document to Kafka
```
Debezium automatically captures the raw MongoDB document without any code changes. This is a low-level database event.

**Why both?** Path 1 gives you clean, structured business events. Path 2 gives you a complete, audit-grade log of every database change (including direct admin updates that bypass Spring Boot).

---

## Replica Set Configuration

MongoDB **must** run as a Replica Set for CDC to work. In our Docker setup:

```yaml
# docker-compose-pipeline.yml
mongodb:
  image: mongo:7.0
  command: mongod --replSet rs0 --bind_ip_all --port 27017
```

The `mongodb-rs-init` container automatically initializes the replica set after MongoDB is healthy:

```javascript
// Executed by mongodb-rs-init container
try { rs.status(); } catch(e) {
  rs.initiate({ _id: "rs0", members: [{ _id: 0, host: "mongodb:27017" }] })
}
```

This is an **idempotent** operation — if the replica set is already initialized, it skips.

---

## Connection Strings

| Context | URI |
|---|---|
| **Spring Boot (local dev)** | `mongodb://localhost:27017/foodies?replicaSet=rs0&directConnection=true` |
| **Spring Boot (Atlas cloud)** | `mongodb+srv://user:pass@cluster.mongodb.net/foodies` |
| **Docker containers (internal)** | `mongodb://mongodb:27017/?replicaSet=rs0` |
| **Debezium connector** | `mongodb://mongodb:27017/?replicaSet=rs0` |

---

## 📈 Scalability & Current Configuration

### Current Setup

| Setting | Value | Why |
|---|---|---|
| MongoDB version | 7.0 | Latest LTS, best Change Streams support |
| Replica Set | `rs0` with 1 member | Minimum for Change Streams (dev only) |
| Storage Engine | WiredTiger | Default, best all-around performance |
| Indexes | Default `_id` only | Sufficient for current CRUD operations |
| Max document size | 16 MB | MongoDB hard limit — not a problem for orders |

### How Many Documents Can It Handle?

| Metric | Current | Limit |
|---|---|---|
| Documents in `orders` | ~100 | No hard limit (tested to billions) |
| Total database size | ~10 MB | Practical limit: ~500 GB per server |
| Concurrent connections | ~10 | Default max: 65,536 |
| Write throughput | ~100 writes/sec | Single node: ~50,000 writes/sec |

---

## 🚀 Future Scaling Guide

### Level 1: Production Replica Set (3 Nodes)

Current setup has 1 MongoDB member (no redundancy). In production:

```yaml
# Production docker-compose (conceptual)
mongodb-primary:
  command: mongod --replSet rs0 --bind_ip_all
mongodb-secondary-1:
  command: mongod --replSet rs0 --bind_ip_all
mongodb-secondary-2:
  command: mongod --replSet rs0 --bind_ip_all
```

```javascript
rs.initiate({
  _id: "rs0",
  members: [
    { _id: 0, host: "mongodb-primary:27017" },
    { _id: 1, host: "mongodb-secondary-1:27017" },
    { _id: 2, host: "mongodb-secondary-2:27017" }
  ]
});
```

**Benefits:**
- If the primary dies, a secondary auto-promotes to primary (automatic failover)
- Read queries can be sent to secondaries (`readPreference: "secondaryPreferred"`) to reduce load on primary
- Debezium can read Change Streams from any member

### Level 2: Sharding (Billions of Orders)

If `foodies.orders` grows to billions of documents:

```javascript
// Enable sharding on the database
sh.enableSharding("foodies");

// Shard the orders collection by userId (hashed)
sh.shardCollection("foodies.orders", { userId: "hashed" });
```

**Why hash userId?** Hashed sharding distributes documents evenly across shards. Without hashing, all orders from user "Tapesh" would go to the same shard, creating a hotspot.

**⚠️ Important:** When sharding is enabled, Debezium requires `capture.mode: change_streams` (not `change_streams_update_full`) as per Debezium documentation.

### Level 3: MongoDB Atlas (Managed)

For zero-ops management in production, use MongoDB Atlas:
- Automated backups, scaling, monitoring
- Multi-region replication
- Built-in security (TLS, audit logging)
- The connection string changes from `mongodb://localhost:27017/...` to `mongodb+srv://user:pass@cluster.mongodb.net/...`

---

## Atlas Migration Script

The `migrate-from-atlas.sh` script helps migrate data from MongoDB Atlas (cloud) to local Docker MongoDB:

```bash
# Usage:
./mongodb/migrate-from-atlas.sh

# What it does:
# 1. Connects to Atlas using your MONGODB_URI from .env
# 2. Dumps all collections from the 'foodies' database
# 3. Restores them into the local Docker MongoDB
# 4. Verifies document counts match
```

This is useful when switching from Atlas to local development or vice versa.

---

## Files in This Directory

| File | Purpose |
|---|---|
| `init-replica-set.js` | Initializes the `rs0` replica set (executed by `mongodb-rs-init` container) |
| `migrate-from-atlas.sh` | Migration script: Atlas cloud → local Docker MongoDB |

---

## Useful Commands

```bash
# Connect to MongoDB shell
docker exec -it foodingo-mongodb mongosh

# Check replica set status
docker exec -i foodingo-mongodb mongosh --eval "rs.status()"

# List all databases
docker exec -i foodingo-mongodb mongosh --eval "show dbs"

# Query orders collection
docker exec -i foodingo-mongodb mongosh foodies --eval "db.orders.find().pretty()"

# Count documents in each collection
docker exec -i foodingo-mongodb mongosh foodies --eval \
  "['orders','users','food','carts'].forEach(c => print(c + ': ' + db[c].countDocuments()))"

# View oplog (what Debezium reads)
docker exec -i foodingo-mongodb mongosh local --eval "db.oplog.rs.find().sort({ts:-1}).limit(5).pretty()"

# Check current connections
docker exec -i foodingo-mongodb mongosh --eval "db.serverStatus().connections"
```

---

## Connecting with MongoDB Compass

1. Open MongoDB Compass
2. Paste connection string: `mongodb://localhost:27017/foodies?replicaSet=rs0&directConnection=true`
3. Click **Connect**
4. You can browse all 4 collections and see documents in real-time

---

## Learn More

- [MongoDB Documentation](https://www.mongodb.com/docs/)
- [MongoDB Change Streams](https://www.mongodb.com/docs/manual/changeStreams/)
- [Spring Data MongoDB](https://spring.io/projects/spring-data-mongodb)
- [MongoDB Sharding](https://www.mongodb.com/docs/manual/sharding/)
- [MongoDB Atlas](https://www.mongodb.com/atlas)
