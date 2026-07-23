-- models/marts/ml_user_order_matrix.sql
-- User × Food order count matrix — input for collaborative filtering recommender

{{ config(materialized='table') }}

WITH exploded AS (
    SELECT
        user_id,
        jsonb_array_elements(ordered_items)->>'foodId' AS food_id
    FROM {{ ref('fact_orders') }}
    WHERE payment_status = 'paid'
      AND user_id IS NOT NULL
)

SELECT
    user_id,
    food_id,
    COUNT(*)   AS order_count
FROM exploded
WHERE food_id IS NOT NULL
GROUP BY user_id, food_id
