-- models/staging/stg_users.sql
{{ config(materialized='view') }}

SELECT
    user_id,
    email,
    name,
    event_type,
    event_timestamp,
    ingested_at
FROM raw.user_events
WHERE user_id IS NOT NULL
