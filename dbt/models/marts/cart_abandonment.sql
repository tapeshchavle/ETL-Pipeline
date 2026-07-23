-- models/marts/cart_abandonment.sql
-- Identifies users who added to cart but did NOT place an order within 24 hours

{{ config(materialized='table') }}

WITH cart_users AS (
    SELECT
        user_id,
        MAX(event_time)                 AS last_cart_time,
        COUNT(DISTINCT food_id)         AS cart_item_count
    FROM {{ ref('fact_cart_events') }}
    WHERE event_type = 'cart.item_added'
    GROUP BY user_id
),

order_users AS (
    SELECT DISTINCT user_id
    FROM {{ ref('fact_orders') }}
    WHERE payment_status = 'paid'
)

SELECT
    c.user_id,
    c.last_cart_time,
    c.cart_item_count,
    EXTRACT(DAY FROM NOW() - c.last_cart_time)::INT     AS days_since_cart,
    (o.user_id IS NOT NULL)                             AS has_ordered_after
FROM cart_users c
LEFT JOIN order_users o ON c.user_id = o.user_id
ORDER BY days_since_cart DESC
