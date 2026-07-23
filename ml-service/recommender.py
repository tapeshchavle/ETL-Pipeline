"""
Foodingo ML Service — Food Recommender
=======================================
Item-based Collaborative Filtering using cosine similarity.

Training data: analytics.ml_user_order_matrix (user_id × food_id → order_count)
Output: Top-N food IDs for a given user

Cold-start strategy:
  - New user (no order history) → returns most-popular food IDs overall
"""

import logging
import os
from typing import List

import joblib
import numpy as np
import pandas as pd
import psycopg2
from scipy.sparse import csr_matrix
from sklearn.metrics.pairwise import cosine_similarity

log = logging.getLogger(__name__)
MODEL_PATH = "/tmp/recommender_model.pkl"


def get_pg_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "foodingo_warehouse"),
        user=os.getenv("POSTGRES_USER", "foodingo"),
        password=os.getenv("POSTGRES_PASSWORD", "foodingo123"),
    )


class FoodRecommender:
    """
    Item-based collaborative filter using cosine similarity on
    the user × food order-count matrix.
    """

    def __init__(self):
        self.user_food_matrix: pd.DataFrame = None
        self.item_similarity: np.ndarray = None
        self.food_ids: List[str] = []
        self.popular_foods: List[str] = []  # cold-start fallback
        self.is_trained = False

    def train(self, df: pd.DataFrame):
        """
        df columns: user_id (str), food_id (str), order_count (int)
        """
        if df.empty:
            log.warning("No data for recommender training — using empty model")
            self.is_trained = False
            return

        # Build user × food pivot matrix
        pivot = df.pivot_table(index="user_id", columns="food_id",
                               values="order_count", fill_value=0)
        self.user_food_matrix = pivot
        self.food_ids = list(pivot.columns)

        # Compute item-item cosine similarity
        food_matrix = csr_matrix(pivot.values.T)  # (n_foods × n_users)
        sim = cosine_similarity(food_matrix)
        self.item_similarity = pd.DataFrame(sim, index=self.food_ids, columns=self.food_ids)

        # Popular foods (for cold-start)
        pop = df.groupby("food_id")["order_count"].sum().sort_values(ascending=False)
        self.popular_foods = list(pop.index[:10])

        self.is_trained = True
        log.info("Recommender trained: %d users, %d foods", len(pivot), len(self.food_ids))

    def recommend(self, user_id: str, top_n: int = 5) -> List[str]:
        """Returns top-N food IDs for the given user."""

        # Cold start — user not in training data
        if not self.is_trained or user_id not in self.user_food_matrix.index:
            log.info("Cold start for user %s — returning popular foods", user_id)
            return self.popular_foods[:top_n]

        user_vector = self.user_food_matrix.loc[user_id]
        ordered_foods = set(user_vector[user_vector > 0].index)

        # Score each food as weighted sum of item similarities
        scores: dict = {}
        for food in ordered_foods:
            if food not in self.item_similarity.index:
                continue
            similar = self.item_similarity[food].drop(index=list(ordered_foods), errors="ignore")
            for candidate, score in similar.items():
                scores[candidate] = scores.get(candidate, 0) + score

        if not scores:
            return self.popular_foods[:top_n]

        sorted_foods = sorted(scores, key=scores.get, reverse=True)
        return sorted_foods[:top_n]

    def save(self, path: str = MODEL_PATH):
        joblib.dump(self, path)
        log.info("Recommender model saved to %s", path)

    @staticmethod
    def load(path: str = MODEL_PATH) -> "FoodRecommender":
        model = joblib.load(path)
        log.info("Recommender model loaded from %s", path)
        return model


def train_recommender() -> FoodRecommender:
    """Reads from PostgreSQL and trains the recommender."""
    model = FoodRecommender()
    try:
        conn = get_pg_conn()
        df = pd.read_sql(
            "SELECT user_id, food_id, order_count FROM analytics.ml_user_order_matrix",
            conn
        )
        conn.close()
        model.train(df)
        model.save()
    except Exception as e:
        log.warning("Could not train recommender (DB may not have data yet): %s", e)
    return model


def load_or_train() -> FoodRecommender:
    """Loads persisted model if exists, otherwise trains a new one."""
    if os.path.exists(MODEL_PATH):
        try:
            return FoodRecommender.load()
        except Exception:
            pass
    return train_recommender()
