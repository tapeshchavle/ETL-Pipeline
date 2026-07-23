"""
Foodingo ML Service — Cart Abandonment / Churn Predictor
=========================================================
Logistic Regression model predicting probability of cart abandonment.

Features:
  - hours_since_last_cart : float — how long ago the user last added to cart
  - cart_item_count       : int   — number of distinct items in cart
  - total_cart_events     : int   — total cart interactions
  - has_ordered_before    : bool  — did the user ever place an order
  - days_since_last_order : float — recency of last order (high = at-risk)

Output: probability (0–1) of abandonment; risk label (low / medium / high)
"""

import logging
import os
from dataclasses import dataclass
from typing import Optional

import joblib
import numpy as np
import pandas as pd
import psycopg2
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

log = logging.getLogger(__name__)
MODEL_PATH = "/tmp/churn_model.pkl"


def get_pg_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "foodingo_warehouse"),
        user=os.getenv("POSTGRES_USER", "foodingo"),
        password=os.getenv("POSTGRES_PASSWORD", "foodingo123"),
    )


@dataclass
class ChurnPrediction:
    user_id: str
    probability: float
    risk: str          # low | medium | high
    recommendation: str


class ChurnPredictor:
    """
    Logistic Regression pipeline with StandardScaler.
    Trained on cart_abandonment analytics table.
    """

    FEATURES = [
        "days_since_cart",
        "cart_item_count",
        "has_ordered_after",
    ]

    def __init__(self):
        self.model: Optional[Pipeline] = None
        self.is_trained = False

    def train(self, df: pd.DataFrame):
        if df.empty or len(df) < 5:
            log.warning("Not enough data to train churn predictor")
            return

        # Label: abandoned = did not order after adding to cart
        df = df.copy()
        df["abandoned"] = (~df["has_ordered_after"].astype(bool)).astype(int)
        df["days_since_cart"] = df["days_since_cart"].fillna(0)
        df["cart_item_count"] = df["cart_item_count"].fillna(0)
        df["has_ordered_after"] = df["has_ordered_after"].astype(int)

        X = df[self.FEATURES].values
        y = df["abandoned"].values

        if len(np.unique(y)) < 2:
            log.warning("Only one class in training data — skipping churn model training")
            return

        self.model = Pipeline([
            ("scaler", StandardScaler()),
            ("lr", LogisticRegression(max_iter=200, C=1.0)),
        ])
        self.model.fit(X, y)
        self.is_trained = True
        log.info("Churn predictor trained on %d users", len(df))

    def predict(self, user_id: str, days_since_cart: float,
                cart_item_count: int, has_ordered_after: bool) -> ChurnPrediction:

        if not self.is_trained:
            # Fallback heuristic when model not trained
            prob = min(days_since_cart / 7.0, 1.0)  # >7 days = high risk
        else:
            features = np.array([[days_since_cart, cart_item_count, int(has_ordered_after)]])
            prob = float(self.model.predict_proba(features)[0][1])

        if prob >= 0.7:
            risk = "high"
            recommendation = "Send recovery email with discount coupon"
        elif prob >= 0.4:
            risk = "medium"
            recommendation = "Send push notification reminder"
        else:
            risk = "low"
            recommendation = "No action needed"

        return ChurnPrediction(
            user_id=user_id,
            probability=round(prob, 4),
            risk=risk,
            recommendation=recommendation,
        )

    def save(self, path: str = MODEL_PATH):
        joblib.dump(self, path)
        log.info("Churn model saved to %s", path)

    @staticmethod
    def load(path: str = MODEL_PATH) -> "ChurnPredictor":
        m = joblib.load(path)
        log.info("Churn model loaded from %s", path)
        return m


def train_churn_predictor() -> ChurnPredictor:
    model = ChurnPredictor()
    try:
        conn = get_pg_conn()
        df = pd.read_sql(
            """SELECT user_id, last_cart_time, cart_item_count,
                      days_since_cart, has_ordered_after
               FROM analytics.cart_abandonment""",
            conn
        )
        conn.close()
        model.train(df)
        if model.is_trained:
            model.save()
    except Exception as e:
        log.warning("Could not train churn predictor (DB may not have data yet): %s", e)
    return model


def load_or_train() -> ChurnPredictor:
    if os.path.exists(MODEL_PATH):
        try:
            return ChurnPredictor.load()
        except Exception:
            pass
    return train_churn_predictor()
