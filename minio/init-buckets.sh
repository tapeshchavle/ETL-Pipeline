#!/bin/sh
# ─── MinIO Bucket Initialisation ─────────────────────────────────────────────
# Mirrors the real AWS S3 foodingo-data-lake bucket structure locally.

MC=/usr/bin/mc

echo "==> Waiting for MinIO to be ready..."
sleep 5

# Configure MinIO alias
$MC alias set local http://minio:9000 foodingo foodingo123

echo "==> Creating data lake bucket and folder structure..."

# Root data lake bucket
$MC mb --ignore-existing local/foodingo-data-lake

# Raw zone — Kafka event dumps (JSON/Parquet)
$MC mb --ignore-existing local/foodingo-data-lake/raw/events
$MC mb --ignore-existing local/foodingo-data-lake/raw/cdc

# Curated zone — cleaned & deduplicated
$MC mb --ignore-existing local/foodingo-data-lake/curated

# ML models zone — trained model artifacts
$MC mb --ignore-existing local/foodingo-data-lake/ml-models/recommender/latest
$MC mb --ignore-existing local/foodingo-data-lake/ml-models/churn/latest

# Serving zone — aggregated tables for BI
$MC mb --ignore-existing local/foodingo-data-lake/serving

echo "==> MinIO buckets initialised successfully!"
$MC ls local/foodingo-data-lake
