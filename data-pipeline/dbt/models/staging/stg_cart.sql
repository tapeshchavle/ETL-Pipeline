-- models/staging/stg_cart.sql
{{ config(materialized='view') }}

SELECT
    user_id,
    food_id,
    quantity,
    event_type,
    event_timestamp,
    ingested_at
FROM raw.cart_events
WHERE user_id IS NOT NULL
