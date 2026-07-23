# 🍃 MongoDB Database Guide

## Overview
MongoDB is our primary operational database (OLTP) serving the Spring Boot backend. 
We use a local Dockerized MongoDB instance instead of MongoDB Atlas for local development to ensure fast, offline access and zero cloud costs during testing.

## Why a Replica Set?
Our local MongoDB is intentionally configured as a **single-node Replica Set (rs0)** rather than a standalone database.
- **Reason:** Debezium Change Data Capture (CDC) relies on the MongoDB `oplog` (Operations Log) to track database changes. Standalone MongoDB instances do not have an oplog. By forcing it into a Replica Set, we enable the oplog and allow Debezium to stream data to Kafka.

## Important Scripts
- `init-mongo.js`: Runs automatically when the MongoDB container starts. It initializes the `rs0` replica set so the database is ready to accept connections.
- `migrate-from-atlas.sh`: A helper script if you ever need to pull your cloud data from MongoDB Atlas into this local database.

## How to Connect
You can connect to this database using **MongoDB Compass**:
- **URI:** `mongodb://localhost:27017/foodies?replicaSet=rs0&directConnection=true`
