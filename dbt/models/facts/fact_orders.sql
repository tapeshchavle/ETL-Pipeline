-- models/facts/fact_orders.sql
-- One row per paid order — the primary fact table for revenue analytics

{{ config(materialized='table') }}

SELECT
    order_id,
    user_id,
    amount,
    payment_status,
    order_status,
    jsonb_array_length(ordered_items)   AS item_count,
    DATE(event_timestamp)               AS order_date,
    EXTRACT(HOUR FROM event_timestamp)  AS order_hour,
    event_timestamp                     AS created_at
FROM {{ ref('stg_orders') }}
WHERE event_type = 'order.created'
  AND payment_status IS NOT NULL
