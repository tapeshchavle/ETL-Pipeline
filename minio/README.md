# 🪣 MinIO (Local Data Lake) Guide

## Overview
MinIO is a high-performance, S3-compatible object storage server. 
In a production cloud environment, we use **AWS S3** as our Data Lake. However, for local development and testing without internet or AWS credentials, we can route traffic to this local MinIO container.

## How it works in the Pipeline
The Python Kafka Consumer script buffers Kafka events in memory. Once the buffer hits 100 messages (or 60 seconds pass), it converts the raw JSON events into highly compressed **Apache Parquet** files and uploads them to the Data Lake.

This ensures we have a permanent, highly scalable backup of all raw data. If our PostgreSQL database is ever corrupted or destroyed, we can use these Parquet files to fully restore the database.

## Important Scripts
- `init-buckets.sh`: A shell script that runs on startup. It connects to the MinIO container and automatically creates the `foodingo-data-lake` bucket and necessary folder structures (`/raw`, `/curated`) so the pipeline can start writing data immediately.

## Accessing the MinIO Console
You can view the files visually using the built-in web interface:
- **URL:** `http://localhost:9001`
- **Username:** `foodingo`
- **Password:** `foodingo123`
