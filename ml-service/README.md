# 🧠 ML Service — Food Recommender & Churn Predictor (FastAPI)

## Overview

The ML Service is a **Python FastAPI microservice** that provides real-time artificial intelligence capabilities to the Foodingo application:

1. **Food Recommender:** Suggests personalized food dishes using **Item-Item Collaborative Filtering (Cosine Similarity)** based on historical user order matrices.
2. **Churn Predictor:** Evaluates shopping cart abandonment risk using a **Logistic Regression** classifier to trigger retention campaigns (discount coupons, push notifications).

The service is dual-client:
- **Spring Boot Backend:** Calls `GET /recommend/{userId}` during user browsing for low-latency (< 10 ms) real-time recommendations.
- **Apache Airflow:** Calls `POST /train/recommender` and `POST /train/churn` nightly at 2:00 AM IST to retrain algorithms with fresh analytical data from PostgreSQL and Apache Spark.

---

## Architecture & Complete Flow

```
┌────────────────────────────────────────────────────────────────────────┐
│                        FastAPI ML Service (:5001)                      │
│                                                                        │
│  ┌───────────────────────── Endpoints ──────────────────────────────┐  │
│  │  GET  /health              ──► System & model status check         │  │
│  │  GET  /recommend/{userId}  ──► Returns top-N recommended food IDs  │  │
│  │  GET  /churn-risk/{userId} ──► Returns abandonment probability     │  │
│  │  POST /train/recommender   ──► Re-builds cosine similarity matrix  │  │
│  │  POST /train/churn         ──► Re-trains logistic regression       │  │
│  └──────────────────────────────────────────────────────────────────┘  │
│                                                                        │
│  ┌───────────────────────── RAM Model Store ────────────────────────┐  │
│  │  • FoodRecommender (Item-Item Cosine Similarity Matrix)            │  │
│  │  • ChurnPredictor (StandardScaler + LogisticRegression Pipeline)    │  │
│  │  • Persisted via joblib to: /tmp/*.pkl                             │  │
│  └──────────────────────────────────────────────────────────────────┘  │
└────────────────────┬───────────────────────────────▲───────────────────┘
                     │                               │
       SQL / S3 READ │ (During Training)             │ HTTP GET (Inference)
                     ▼                               │ (< 10 ms latency)
┌──────────────────────────────────────┐     ┌───────┴───────────────────┐
│     PostgreSQL / Apache Spark        │     │  Spring Boot Backend      │
│     (analytics Schema & Data Lake)   │     │  (RecommendationController│
│                                      │     │   calls via RestTemplate) │
│  • analytics.ml_user_order_matrix    │     └───────────────────────────┘
│  • analytics.cart_abandonment        │
└──────────────────────────────────────┘
```

---

## 🔬 How ML Training & Inference Work Internally

### Model 1: Food Recommender (Collaborative Filtering)

#### Algorithmic Deep Dive
The recommendation engine uses **Item-Item Collaborative Filtering** implemented with `scikit-learn` and `scipy.sparse`:

1. **Building the User × Food Matrix:**
   During training (`recommender.py`), it executes:
   ```sql
   SELECT user_id, food_id, order_count FROM analytics.ml_user_order_matrix;
   ```
   It pivots this data into a sparse matrix where rows are users, columns are food items, and values are purchase counts:
   ```
                 Burger  Pizza  Fries  Salad  Biryani
   user_abc123     2      1      3      0       1
   user_def456     1      3      2      1       0
   ```

2. **Computing Cosine Similarity:**
   It transposes the matrix so foods become rows, and computes pairwise cosine similarity between every food vector:
   ```python
   food_matrix = csr_matrix(pivot.values.T)  # (n_foods × n_users)
   similarity = cosine_similarity(food_matrix)
   ```
   - If two foods (e.g., Burger and Fries) are frequently ordered by the exact same users, their cosine angle approaches `0` (similarity score approaches `1.0`).

3. **Real-Time Prediction (< 10 ms):**
   When Spring Boot requests `GET /recommend/user_abc123`:
   - It retrieves all foods `user_abc123` has previously ordered.
   - For each ordered food, it looks up the most similar candidate foods from the pre-computed similarity matrix.
   - It filters out foods the user has already bought, ranks candidates by weighted similarity sum, and returns the **Top-N** recommended IDs as a JSON array.

#### The "Cold-Start" Fallback Strategy
What happens if a brand-new user with zero order history opens Foodingo?
- Cosine similarity cannot multiply zero vectors.
- During training, the recommender automatically calculates and caches the **Top-10 Globally Popular Foods** (`order_count.sum()`).
- If `user_id not in matrix`, it immediately falls back to returning these popular dishes!

---

### Model 2: Churn Predictor (Logistic Regression)

#### Algorithmic Deep Dive
The churn prediction model (`churn_predictor.py`) evaluates whether a customer who added food to their cart will abandon it without checking out:

1. **Features & Target:**
   - **Features:** `days_since_cart` (float), `cart_item_count` (int).
   - **Target Label:** `has_ordered_after` (boolean `0` or `1`).
2. **Machine Learning Pipeline:**
   ```python
   self.model = Pipeline([
       ("scaler", StandardScaler()),  # Zero-mean, unit-variance normalization
       ("lr", LogisticRegression(max_iter=200, C=1.0))
   ])
   self.model.fit(X, y)
   ```
3. **Inference & Decision Rules:**
   When requested via `GET /churn-risk/{userId}`, it calculates probability `P(abandonment)`:
   - **`P >= 0.7` (`high`):** Action: *"Send recovery email with 20% discount coupon"*
   - **`0.4 <= P < 0.7` (`medium`):** Action: *"Send push notification reminder"*
   - **`P < 0.4` (`low`):** Action: *"No action needed"*

---

## 🔬 Why RAM Inference + Joblib Persistence?

Notice how `app.py` loads models during FastAPI's lifecycle startup:

```python
MODEL_PATH = "/tmp/recommender_model.pkl"

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Load serialized model from disk into RAM on server boot
    app.state.recommender = joblib.load(MODEL_PATH)
    yield
```

**Why not query PostgreSQL during every recommendation request?**
- Executing SQL joins across millions of order rows would take **300–800 ms** per request — far too slow for user interfaces.
- By training offline and serializing the similarity matrix to `/tmp/recommender_model.pkl`, loading the `.pkl` file into RAM takes ~50 ms on boot.
- Once in RAM, looking up array indices in memory takes **< 5 milliseconds**! Zero database roundtrips required during user interaction.

---

## 🔗 How the ML Service Interacts With Other Components

```
┌───────────────────────────────┐     ┌──────────────────────────────┐
│  Apache Airflow               │     │  Spring Boot Backend         │
│  (foodingo_daily_pipeline)     │     │  (RecommendationController)  │
└───────────────┬───────────────┘     └──────────────┬───────────────┘
                │ Nightly POST                       │ Live HTTP GET
                │ /train/recommender                 │ /recommend/{id}
                ▼                                    ▼
┌────────────────────────────────────────────────────────────────────┐
│                    FastAPI ML Service (:5001)                      │
│                                                                    │
│  • Reads training data via psycopg2 / PySpark                      │
│  • Updates RAM model state seamlessly                              │
│  • Serves sub-10ms JSON recommendation arrays                      │
└───────────────────────────────┬────────────────────────────────────┘
                                │ SQL READ / Parquet Scan
                                ▼
┌────────────────────────────────────────────────────────────────────┐
│                  PostgreSQL / Apache Spark Data Lake               │
│  • analytics.ml_user_order_matrix (Hot Path — PostgreSQL)          │
│  • s3://foodingo-data-lake/parquet/... (Cold Path — Apache Spark)   │
└────────────────────────────────────────────────────────────────────┘
```

### The Spark Connection (Lambda Architecture Integration)
When Foodingo has 10,000 users, PostgreSQL easily builds `analytics.ml_user_order_matrix`. 
- **What happens at 10 million users and 100 million orders?**
- Relational `GROUP BY user_id, food_id` in PostgreSQL times out.
- **Apache Spark** steps in: Airflow triggers a Spark job that scans S3 Parquet files across the Data Lake, calculates the user-order interaction matrix across distributed nodes, and writes the summarized matrix back into PostgreSQL (or S3) where `recommender.py` reads it during training!

---

## API Endpoints Reference

| Method | Endpoint | Description | Example Payload / Response |
|---|---|---|---|
| **GET** | `/health` | Check if models are trained and ready | `{"status": "ok", "recommender_trained": true}` |
| **GET** | `/recommend/{userId}?top_n=5` | Get top food recommendations | `["food_id_1", "food_id_2", "food_id_3"]` |
| **GET** | `/churn-risk/{userId}` | Evaluate customer abandonment risk | `{"probability": 0.72, "risk": "high", ...}` |
| **POST** | `/train/recommender` | Trigger nightly collaborative filter retrain | `{"message": "Recommender retrained successfully"}` |
| **POST** | `/train/churn` | Trigger nightly churn classifier retrain | `{"message": "Churn model retrained successfully"}` |

---

## 📈 Scalability & Current Configuration

### Current Setup
| Setting | Value | Why |
|---|---|---|
| Web Server | Uvicorn (ASGI) | High-concurrency async Python HTTP handling |
| Worker Process | 1 Uvicorn Worker | Simple single-container Docker setup |
| Model Serialization | `joblib` (`/tmp/*.pkl`) | Fast binary dump/load of NumPy/SciPy sparse arrays |
| Database Driver | `psycopg2-binary` | Directly fetches analytical training matrices from PostgreSQL |
| Average Latency | **4–8 milliseconds** | RAM-based matrix lookup without I/O blocking |

---

## 🚀 Future Scaling Guide

When Foodingo's user base grows, scale the ML Service using these architectural patterns:

### Level 1: Multi-Worker Process Deployment (`gunicorn` + `uvicorn`)
Currently, `Dockerfile` runs a single Uvicorn process. To utilize multi-core host CPUs:
```dockerfile
# Run 4 parallel worker processes behind a Gunicorn process manager:
CMD ["gunicorn", "app:app", "--workers", "4", "--worker-class", "uvicorn.workers.UvicornWorker", "--bind", "0.0.0.0:5001"]
```

### Level 2: Shared Model Cache via Redis
With multiple container instances, saving `.pkl` files to `/tmp/` local container disks can cause consistency divergence.
- Store trained model matrices or pre-computed user recommendation lists in an in-memory **Redis Cache Cluster**.
- Any ML container can serve instant recommendations from Redis in < 2 ms.

### Level 3: Enterprise Model Serving (BentoML / AWS SageMaker / KServe)
When managing dozens of ML models with deep neural networks (e.g., PyTorch image classifiers for food pictures):
- Migrate from FastAPI to a specialized model serving platform like **BentoML** or **AWS SageMaker Endpoints**.
- Supports GPU acceleration, dynamic model batching, and automated Canary A/B testing of recommendation algorithms.

---

## Files in This Directory

| File | Purpose |
|---|---|
| `app.py` | FastAPI application defining all 5 HTTP endpoints and `lifespan` model loading |
| `recommender.py` | `FoodRecommender` class implementing Item-Item Cosine Similarity |
| `churn_predictor.py` | `ChurnPredictor` class implementing Logistic Regression pipeline |
| `Dockerfile` | Docker build instructions for the FastAPI container |
| `requirements.txt` | Python ML and web dependencies (`fastapi`, `scikit-learn`, `scipy`, etc.) |

---

## Useful Commands

```bash
# View live logs from the ML service container
docker logs -f foodingo-ml-service

# Trigger a manual retraining of the recommendation algorithm
curl -X POST http://localhost:5001/train/recommender

# Trigger a manual retraining of the churn prediction classifier
curl -X POST http://localhost:5001/train/churn

# Test real-time recommendation inference for a specific user
curl http://localhost:5001/recommend/6a6375b9aa977450348009d1

# Check churn abandonment risk score for a user
curl http://localhost:5001/churn-risk/6a6375b9aa977450348009d1

# Interactive API testing via Swagger UI in your browser:
# Open: http://localhost:5001/docs
```

---

## Learn More

- [FastAPI Official Documentation](https://fastapi.tiangolo.com/)
- [scikit-learn Cosine Similarity Reference](https://scikit-learn.org/stable/modules/generated/sklearn.metrics.pairwise.cosine_similarity.html)
- [scikit-learn Logistic Regression Guide](https://scikit-learn.org/stable/modules/generated/sklearn.linear_model.LogisticRegression.html)
- [Collaborative Filtering Explained](https://en.wikipedia.org/wiki/Collaborative_filtering)
