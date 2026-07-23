-- models/marts/daily_revenue.sql
{{ config(materialized='table') }}

SELECT
    order_date                          AS revenue_date,
    SUM(amount)                         AS total_revenue,
    COUNT(order_id)                     AS order_count,
    AVG(amount)                         AS avg_order_value,
    COUNT(DISTINCT user_id)             AS unique_users
FROM {{ ref('fact_orders') }}
WHERE payment_status = 'paid'
GROUP BY order_date
ORDER BY order_date DESC
