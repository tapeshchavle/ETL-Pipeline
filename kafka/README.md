# 🌪️ Apache Kafka & Kafka UI Guide

## Overview
Apache Kafka is the central nervous system of this data pipeline. It acts as a highly scalable, distributed event streaming platform. Instead of services directly talking to each other (which causes tight coupling and failures if a service is down), everything sends messages to Kafka.

In our pipeline:
1. **Producers:** The Spring Boot backend and Debezium Connector push messages to Kafka.
2. **Consumers:** The Python Consumer script pulls messages from Kafka to save them to PostgreSQL and AWS S3.

## Topics Explained
A **Topic** is like a categorized folder where events are stored.
- **`user.registered`, `order.created`, `cart.item_added`:** These are *Application Events*. Spring Boot publishes these intentionally whenever a business action occurs.
- **`foodingo.foodies.*`:** These are *CDC (Change Data Capture) Events*. Debezium publishes these automatically by secretly watching MongoDB's logs. They represent raw database row changes.

## Understanding the Kafka UI (`http://localhost:8090`)

The Kafka UI is your window into the streaming platform. 

### 1. Dashboard Tab
Shows the overall health of the Kafka Cluster. You can see how many brokers are alive, total topics, and total partitions.

### 2. Topics Tab
This is the most important tab. It lists every topic.
- **Number of messages:** How many total events have flowed through this topic.
- **Click on a Topic (e.g., `cart.item_added`) -> Click "Messages" tab:** 
  You can actually view the raw JSON data flowing through the pipeline in real-time! If a user clicks "Add to Cart" on the frontend, it will appear here instantly.

### 3. Consumers Tab
Shows all "Consumer Groups" connected to Kafka. 
- You will see `foodingo-data-pipeline` here (this is our Python script).
- **Consumer Lag:** This is a critical metric. Lag means the consumer is falling behind the producers. If Lag = 0, the Python script has successfully processed every single event. If Lag = 1000, it means there are 1000 messages waiting to be saved to S3/Postgres.

### 4. Schema Registry Tab
Kafka often uses schemas (like Avro) to strictly enforce the shape of data. This tab shows the registered schemas for Debezium events to ensure data isn't corrupted.

## Troubleshooting
- **Consumer Lag is rising:** The Python consumer container might be down or crashing (e.g., due to AWS credentials). Restart the consumer.
- **No messages appearing:** The Spring Boot app might not be connected to port `9094` properly, or Debezium isn't registered.
