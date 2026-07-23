-- models/facts/fact_cart_events.sql
{{ config(materialized='table') }}

SELECT
    user_id,
    food_id,
    quantity,
    event_type,
    DATE(event_timestamp)               AS event_date,
    EXTRACT(HOUR FROM event_timestamp)  AS event_hour,
    event_timestamp                     AS event_time
FROM {{ ref('stg_cart') }}
