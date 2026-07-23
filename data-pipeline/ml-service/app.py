"""
Foodingo ML Service — FastAPI Application
=========================================
Serves food recommendations and cart-abandonment predictions.

Endpoints:
  GET  /health                → service health + model status
  GET  /recommend/{userId}    → list of food IDs (called by Spring Boot)
  GET  /churn-risk/{userId}   → churn probability + risk label
  POST /train/recommender     → retrain recommender (called by Airflow)
  POST /train/churn           → retrain churn predictor (called by Airflow)

Swagger UI: http://localhost:5001/docs
"""

import logging
import os
from contextlib import asynccontextmanager
from typing import List, Optional

import psycopg2
import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from recommender import FoodRecommender, load_or_train as load_recommender, train_recommender
from churn_predictor import ChurnPredictor, load_or_train as load_churn, train_churn_predictor

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
log = logging.getLogger("foodingo-ml")

# ── Global model holders ──────────────────────────────────────────────────────
recommender: FoodRecommender = None
churn_model: ChurnPredictor = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Load or train models on startup."""
    global recommender, churn_model
    log.info("Loading ML models...")
    recommender = load_recommender()
    churn_model = load_churn()
    log.info("Models ready. Recommender trained=%s, Churn trained=%s",
             recommender.is_trained, churn_model.is_trained)
    yield
    log.info("Shutting down ML service")


app = FastAPI(
    title="Foodingo ML Service",
    description="Food recommendation and cart abandonment prediction API",
    version="1.0.0",
    lifespan=lifespan,
)


def get_pg_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "foodingo_warehouse"),
        user=os.getenv("POSTGRES_USER", "foodingo"),
        password=os.getenv("POSTGRES_PASSWORD", "foodingo123"),
    )


# ── Response Models ───────────────────────────────────────────────────────────
class HealthResponse(BaseModel):
    status: str
    recommender_trained: bool
    churn_model_trained: bool


class ChurnResponse(BaseModel):
    user_id: str
    probability: float
    risk: str
    recommendation: str


class TrainResponse(BaseModel):
    message: str
    success: bool


# ── Endpoints ─────────────────────────────────────────────────────────────────
@app.get("/health", response_model=HealthResponse, tags=["Health"])
def health():
    """Health check — confirms service is running and shows model status."""
    return {
        "status": "ok",
        "recommender_trained": recommender.is_trained if recommender else False,
        "churn_model_trained": churn_model.is_trained if churn_model else False,
    }


@app.get("/recommend/{user_id}", response_model=List[str], tags=["Recommendations"])
def get_recommendations(user_id: str, top_n: int = 5):
    """
    Returns top-N food IDs recommended for the given user.
    Called by Spring Boot's GET /api/recommendations endpoint.
    Returns most popular items if user has no order history (cold start).
    """
    if recommender is None:
        raise HTTPException(status_code=503, detail="Recommender model not loaded")
    food_ids = recommender.recommend(user_id, top_n=top_n)
    log.info("Recommendations for user %s: %s", user_id, food_ids)
    return food_ids


@app.get("/churn-risk/{user_id}", response_model=ChurnResponse, tags=["Churn"])
def get_churn_risk(user_id: str):
    """
    Returns cart abandonment probability and risk level for a user.
    Used by admin dashboard and Airflow to trigger recovery campaigns.
    """
    if churn_model is None:
        raise HTTPException(status_code=503, detail="Churn model not loaded")

    # Fetch user's cart stats from PostgreSQL
    try:
        conn = get_pg_conn()
        row = pd.read_sql(
            "SELECT days_since_cart, cart_item_count, has_ordered_after "
            "FROM analytics.cart_abandonment WHERE user_id = %s",
            conn, params=[user_id]
        )
        conn.close()
    except Exception as e:
        log.warning("DB error fetching cart stats for %s: %s", user_id, e)
        row = pd.DataFrame()

    if row.empty:
        # Unknown user — default to high-risk prediction
        prediction = churn_model.predict(user_id, days_since_cart=10, cart_item_count=0, has_ordered_after=False)
    else:
        r = row.iloc[0]
        prediction = churn_model.predict(
            user_id,
            days_since_cart=float(r.get("days_since_cart", 0) or 0),
            cart_item_count=int(r.get("cart_item_count", 0) or 0),
            has_ordered_after=bool(r.get("has_ordered_after", False)),
        )

    return {
        "user_id": prediction.user_id,
        "probability": prediction.probability,
        "risk": prediction.risk,
        "recommendation": prediction.recommendation,
    }


@app.post("/train/recommender", response_model=TrainResponse, tags=["Training"])
def retrain_recommender():
    """
    Retrains the food recommendation model using latest order data.
    Called nightly by Airflow DAG.
    """
    global recommender
    try:
        recommender = train_recommender()
        return {"message": "Recommender retrained successfully", "success": True}
    except Exception as e:
        log.error("Recommender retrain failed: %s", e)
        return {"message": f"Retrain failed: {str(e)}", "success": False}


@app.post("/train/churn", response_model=TrainResponse, tags=["Training"])
def retrain_churn():
    """
    Retrains the churn prediction model using latest cart abandonment data.
    Called weekly by Airflow DAG.
    """
    global churn_model
    try:
        churn_model = train_churn_predictor()
        return {"message": "Churn model retrained successfully", "success": True}
    except Exception as e:
        log.error("Churn retrain failed: %s", e)
        return {"message": f"Retrain failed: {str(e)}", "success": False}
