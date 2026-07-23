# 🧠 ML Service (FastAPI) Guide

## Overview
The ML Service is a Python backend built with FastAPI. It provides the "intelligence" to the Foodingo application.
Instead of the Spring Boot application running heavy data-science libraries (which Java is not ideal for), we decouple the Machine Learning logic into this microservice.

## How it works
1. Airflow triggers the `retrain_ml_models` task daily.
2. The ML Service connects to the PostgreSQL `analytics` schema to pull the latest historical data.
3. It trains models using libraries like `scikit-learn` and `pandas`, and saves the model artifacts (`.pkl` files) to MinIO/S3.
4. When a user logs into the Spring Boot app, Spring Boot sends an HTTP request to `http://ml-service:5001/recommend/{user_id}`.
5. The ML Service loads the model, predicts the best food items for that user, and returns a JSON array of `foodIds`.

## Available Endpoints
You can interact with the API using its Swagger UI at `http://localhost:5001/docs`.

### 1. `/recommend/{user_id}`
- **Algorithm:** Collaborative Filtering (Cosine Similarity).
- **Behavior:** It looks at what users with similar tastes have ordered, and recommends those items. If the `user_id` has no history, it returns the top trending items (Cold Start mitigation).

### 2. `/churn-risk/{user_id}`
- **Algorithm:** Logistic Regression.
- **Behavior:** Calculates the probability (0.0 to 1.0) that a user will abandon their cart based on recency and frequency metrics.

## Troubleshooting
- **API is slow:** The models might be retraining. Retraining blocks the API for a few seconds (in a real production app, we would serve predictions from a cached Redis layer).
- **Postgres Connection Error:** Ensure the `analytics` schema has been populated by Airflow/dbt, or else the ML Service has no data to train on!
