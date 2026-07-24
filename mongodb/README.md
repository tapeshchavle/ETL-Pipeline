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

## Spring Boot Entity Mapping

```java
@Document(collection = "orders")
public class OrderEntity {
    @Id
    private String id;
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

---

## Files in This Directory

| File | Purpose |
|---|---|
| `init-replica-set.js` | Initializes the `rs0` replica set (executed by `mongodb-rs-init` container) |

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

# Count documents
docker exec -i foodingo-mongodb mongosh foodies --eval "db.orders.countDocuments()"
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
