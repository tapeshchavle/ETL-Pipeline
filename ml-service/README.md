# 🧠 ML Service — Food Recommender & Churn Predictor (FastAPI)

## Overview

The ML Service is a **Python FastAPI microservice** that serves two machine learning models:

1. **Food Recommender** — Item-based Collaborative Filtering using Cosine Similarity
2. **Churn Predictor** — Logistic Regression for cart abandonment risk scoring

The service is called by two clients:
- **Spring Boot** calls `GET /recommend/{user_id}` to show personalized food suggestions
- **Airflow** calls `POST /train/recommender` nightly to retrain models with fresh data

---

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                   FastAPI ML Service (:5001)                  │
│                                                              │
│  ┌───── Endpoints ─────────────────────────────────────────┐ │
│  │                                                         │ │
│  │  GET  /health              → Model status check         │ │
│  │  GET  /recommend/{userId}  → Top-N food recommendations │ │
│  │  GET  /churn-risk/{userId} → Abandonment risk score     │ │
│  │  POST /train/recommender   → Retrain recommender        │ │
│  │  POST /train/churn         → Retrain churn model        │ │
│  │                                                         │ │
│  └─────────────────────────────────────────────────────────┘ │
│                                                              │
│  ┌───── Models ────────────────────────────────────────────┐ │
│  │                                                         │ │
│  │  FoodRecommender                                        │ │
│  │  ├── Algorithm: Cosine Similarity (Item-Item)           │ │
│  │  ├── Input: analytics.ml_user_order_matrix              │ │
│  │  ├── Output: Top-N food IDs                             │ │
│  │  ├── Cold-start: Returns top-10 popular foods           │ │
│  │  └── Saved: /tmp/recommender_model.pkl                  │ │
│  │                                                         │ │
│  │  ChurnPredictor                                         │ │
│  │  ├── Algorithm: Logistic Regression + StandardScaler    │ │
│  │  ├── Input: analytics.cart_abandonment                  │ │
│  │  ├── Output: probability (0-1), risk label, action      │ │
│  │  ├── Fallback: Heuristic (days/7) when not trained      │ │
│  │  └── Saved: /tmp/churn_model.pkl                        │ │
│  │                                                         │ │
│  └─────────────────────────────────────────────────────────┘ │
│                        │                                     │
│                        │ SQL READ (psycopg2)                 │
│                        ▼                                     │
│              PostgreSQL (analytics schema)                    │
└──────────────────────────────────────────────────────────────┘
```

---

## Model 1: Food Recommender (Collaborative Filtering)

### Algorithm Deep Dive

**Step 1: Build User-Food Matrix**
```
              Burger  Pizza  Fries  Salad  Biryani
user_abc123     2      1      3      0       1
user_def456     1      3      2      1       0
user_ghi789     0      0      1      2       3
```

**Step 2: Compute Item-Item Cosine Similarity**
- Transpose the matrix → each food becomes a vector of user purchase counts
- Calculate cosine similarity between every pair of foods
- Foods bought by similar users will have high similarity scores

```python
food_matrix = csr_matrix(pivot.values.T)  # (n_foods × n_users)
sim = cosine_similarity(food_matrix)
```

**Step 3: Generate Recommendations**
For a specific user:
1. Look at all foods they've ordered
2. For each ordered food, find the most similar foods they HAVEN'T ordered
3. Score candidates by weighted similarity sum
4. Return top-N

```python
def recommend(self, user_id, top_n=5):
    user_vector = self.user_food_matrix.loc[user_id]
    ordered_foods = set(user_vector[user_vector > 0].index)

    scores = {}
    for food in ordered_foods:
        similar = self.item_similarity[food].drop(index=list(ordered_foods))
        for candidate, score in similar.items():
            scores[candidate] = scores.get(candidate, 0) + score

    return sorted(scores, key=scores.get, reverse=True)[:top_n]
```

**Cold-Start Strategy:**
If `user_id` has zero order history (new user), return the top-10 most popular foods globally:
```python
pop = df.groupby("food_id")["order_count"].sum().sort_values(ascending=False)
self.popular_foods = list(pop.index[:10])
```

### Training Data Source
```sql
SELECT user_id, food_id, order_count FROM analytics.ml_user_order_matrix
```

---

## Model 2: Churn Predictor (Logistic Regression)

### Algorithm Deep Dive

**Features:**
| Feature | Type | Description |
|---|---|---|
| `days_since_cart` | float | Days since the user last added to cart |
| `cart_item_count` | int | Number of distinct items currently in cart |
| `has_ordered_after` | bool | Whether the user placed an order after carting |

**Pipeline:**
```python
Pipeline([
    ("scaler", StandardScaler()),   # Normalize features to mean=0, std=1
    ("lr", LogisticRegression(max_iter=200, C=1.0)),
])
```

**Prediction Output:**
| Probability | Risk Label | Recommendation |
|---|---|---|
| ≥ 0.7 | `high` | "Send recovery email with discount coupon" |
| 0.4 – 0.7 | `medium` | "Send push notification reminder" |
| < 0.4 | `low` | "No action needed" |

**Fallback (when not enough training data):**
```python
prob = min(days_since_cart / 7.0, 1.0)  # Simple heuristic: >7 days = high risk
```

### Training Data Source
```sql
SELECT user_id, last_cart_time, cart_item_count, days_since_cart, has_ordered_after
FROM analytics.cart_abandonment
```

---

## Model Persistence

Models are saved to disk using `joblib` and loaded into RAM on startup:

```python
MODEL_PATH = "/tmp/recommender_model.pkl"

def save(self):
    joblib.dump(self, MODEL_PATH)

def load():
    return joblib.load(MODEL_PATH)
```

**Why not save to the database?**
- Model files can be 10-50 MB (matrix + similarity scores)
- Loading from disk into RAM takes ~50ms
- Prediction from RAM takes <10ms per request
- No database roundtrip needed during inference

---

## API Endpoints

### `GET /health`
```json
{
  "status": "ok",
  "recommender_trained": true,
  "churn_model_trained": true
}
```

### `GET /recommend/{user_id}?top_n=5`
```json
["food_id_1", "food_id_2", "food_id_3", "food_id_4", "food_id_5"]
```

### `GET /churn-risk/{user_id}`
```json
{
  "user_id": "abc123",
  "probability": 0.7234,
  "risk": "high",
  "recommendation": "Send recovery email with discount coupon"
}
```

### `POST /train/recommender`
```json
{
  "message": "Recommender retrained successfully",
  "success": true
}
```

### `POST /train/churn`
```json
{
  "message": "Churn model retrained successfully",
  "success": true
}
```

---

## Dependencies

```
fastapi         # Web framework
uvicorn         # ASGI server
psycopg2-binary # PostgreSQL driver
pandas          # DataFrames
scikit-learn    # ML algorithms (Cosine Similarity, Logistic Regression)
scipy           # Sparse matrices for similarity computation
joblib          # Model serialization
numpy           # Numerical operations
```

---

## Directory Structure

```
ml-service/
├── app.py               # FastAPI application (5 endpoints, lifespan model loading)
├── recommender.py       # FoodRecommender class (Cosine Similarity)
├── churn_predictor.py   # ChurnPredictor class (Logistic Regression)
├── Dockerfile           # Docker image definition
└── requirements.txt     # Python dependencies
```

---

## Swagger UI

Access the interactive API documentation at **http://localhost:5001/docs**

You can test all endpoints directly from the browser — try it out, enter parameters, and execute!

---

## How Spring Boot Calls the ML Service

```java
// RecommendationController.java
@GetMapping("/api/recommendations")
public List<String> getRecommendations() {
    String userId = userService.findByUserId();
    // HTTP GET to http://ml-service:5001/recommend/{userId}
    return restTemplate.getForObject(mlServiceUrl + "/recommend/" + userId, List.class);
}
```

---

## Useful Commands

```bash
# View ML service logs
docker logs -f foodingo-ml-service

# Train the recommender manually
curl -X POST http://localhost:5001/train/recommender

# Train the churn model manually
curl -X POST http://localhost:5001/train/churn

# Get recommendations for a user
curl http://localhost:5001/recommend/YOUR_USER_ID

# Check churn risk
curl http://localhost:5001/churn-risk/YOUR_USER_ID

# Check health
curl http://localhost:5001/health
```

---

## Learn More

- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [scikit-learn Cosine Similarity](https://scikit-learn.org/stable/modules/generated/sklearn.metrics.pairwise.cosine_similarity.html)
- [scikit-learn Logistic Regression](https://scikit-learn.org/stable/modules/generated/sklearn.linear_model.LogisticRegression.html)
- [Collaborative Filtering (Wikipedia)](https://en.wikipedia.org/wiki/Collaborative_filtering)
