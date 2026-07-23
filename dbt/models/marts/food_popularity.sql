-- models/marts/food_popularity.sql
-- Explodes the ordered_items JSONB array to get per-food-item order counts

{{ config(materialized='table') }}

WITH exploded AS (
    SELECT
        order_id,
        amount,
        jsonb_array_elements(ordered_items) AS item
    FROM {{ ref('fact_orders') }}
    WHERE payment_status = 'paid'
)

SELECT
    item->>'foodId'       AS food_id,
    item->>'name'         AS food_name,
    item->>'category'     AS category,
    COUNT(*)              AS order_count,
    SUM(
        (item->>'price')::DECIMAL * (item->>'quantity')::INT
    )                     AS total_revenue,
    AVG((item->>'price')::DECIMAL) AS avg_price
FROM exploded
WHERE item->>'foodId' IS NOT NULL
GROUP BY food_id, food_name, category
ORDER BY order_count DESC
