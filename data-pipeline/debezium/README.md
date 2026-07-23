# 🔍 Debezium (Kafka Connect) Guide

## Overview
Debezium is a Change Data Capture (CDC) platform built on top of Kafka Connect. 
Instead of writing complex code in Spring Boot to "dual-write" to both MongoDB and a Data Warehouse (which leads to distributed transaction failures), we use Debezium to literally spy on the database.

## How it works
1. MongoDB maintains an **Oplog (Operations Log)** to replicate data across its own nodes.
2. Debezium disguises itself as a MongoDB secondary node.
3. Every time a `CREATE`, `UPDATE`, or `DELETE` happens in MongoDB, Debezium reads it from the Oplog and instantly publishes it to Kafka.
4. This ensures that our analytics pipeline has a **100% accurate, atomic record** of every single state change in the database.

## Configuration & Testing
When you start the pipeline, a setup script (`init.sh`) runs this command:
```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @mongodb-connector.json
```
This tells Kafka Connect to load our specific MongoDB Debezium plugin.

### To test it:
1. Open Kafka UI (`http://localhost:8090`).
2. Look for topics named `foodingo.foodies.users`, `foodingo.foodies.orders`, etc.
3. If they exist, Debezium is successfully listening.
4. Add a user or place an order in the app. Go to the topic's "Messages" tab. You will see a large JSON payload containing a `before` state (null if it's a new insert) and an `after` state (the new document).
