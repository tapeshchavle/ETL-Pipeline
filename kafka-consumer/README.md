# 🐍 Python Kafka Consumer Guide

## Overview
The `kafka-consumer` is a custom Python microservice that acts as the primary Data Ingestion layer for our pipeline.
It bridges the gap between our streaming architecture (Kafka) and our storage architecture (PostgreSQL and AWS S3).

## How it works
This script uses the `confluent-kafka` and `boto3` libraries to continuously listen to the Kafka broker.

When an event arrives (either an application event like `cart.item_added` or a Debezium CDC event):
1. **Database Sink:** The script parses the JSON, casts the timestamps to proper PostgreSQL ISO formats, and instantly runs an `INSERT` statement into the `raw` schema in the `foodingo_warehouse` database. This powers real-time analytics.
2. **Data Lake Sink:** The script adds the JSON event to an in-memory buffer. Once the buffer hits 100 messages (or 60 seconds have passed), it converts the raw JSON into an Apache Parquet binary file using Pandas/PyArrow, and uploads it to AWS S3 (or local MinIO). 

## Important Files
- `consumer.py`: The main loop and ingestion logic.
- `requirements.txt`: The Python dependencies.
- `Dockerfile`: Packages the script into a lightweight Docker image so it runs continuously alongside the rest of the pipeline infrastructure.

## Troubleshooting
If data is not appearing in PostgreSQL or S3, check the logs of this container:
```bash
docker logs foodingo-kafka-consumer --tail 50
```
Common issues:
- **`InvalidAccessKeyId`**: The AWS Access Key inside the `.env` file is inactive or incorrect.
- **Connection Refused**: Kafka or PostgreSQL is not fully booted up yet. The consumer will automatically retry.
