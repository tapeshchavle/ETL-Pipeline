#!/bin/bash
# ─── Kafka Topic Initialisation ──────────────────────────────────────────────
# Called by kafka-init container once Kafka is healthy.
# Creates all topics used by the Foodingo data pipeline.

KAFKA_BROKER="kafka:9092"

echo "==> Creating Foodingo Kafka topics..."

create_topic() {
  kafka-topics --bootstrap-server $KAFKA_BROKER --create \
    --if-not-exists \
    --topic "$1" \
    --partitions 3 \
    --replication-factor 1
  echo "    Created topic: $1"
}

# ── Spring Boot Application Events ────────────────────────────────────────────
create_topic "user.registered"
create_topic "user.login"
create_topic "cart.item_added"
create_topic "cart.item_removed"
create_topic "cart.cleared"
create_topic "cart.item_deleted"
create_topic "order.created"
create_topic "payment.verified"
create_topic "order.status_updated"

# ── Debezium CDC Topics (MongoDB Change Streams) ──────────────────────────────
create_topic "foodingo.foodies.orders"
create_topic "foodingo.foodies.users"
create_topic "foodingo.foodies.food"
create_topic "foodingo.foodies.carts"

echo "==> All Kafka topics created successfully!"
kafka-topics --bootstrap-server $KAFKA_BROKER --list
