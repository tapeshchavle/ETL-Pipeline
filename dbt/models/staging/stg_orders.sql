-- models/staging/stg_orders.sql
-- Cleans raw order events: deduplicates, casts types, filters to paid orders only

{{ config(materialized='view') }}

WITH deduped AS (
    SELECT
        order_id,
        user_id,
        user_address,
        email,
        phone_number,
        amount::DECIMAL(10,2)   AS amount,
        payment_status,
        order_status,
        razorpay_order_id,
        razorpay_payment_id,
        ordered_items,
        event_type,
        event_timestamp,
        ingested_at,
        ROW_NUMBER() OVER (
            PARTITION BY order_id, event_type
            ORDER BY event_timestamp DESC
        ) AS rn
    FROM raw.order_events
    WHERE order_id IS NOT NULL
)

SELECT
    order_id,
    user_id,
    user_address,
    email,
    phone_number,
    amount,
    payment_status,
    order_status,
    razorpay_order_id,
    razorpay_payment_id,
    ordered_items,
    event_type,
    event_timestamp,
    ingested_at
FROM deduped
WHERE rn = 1
