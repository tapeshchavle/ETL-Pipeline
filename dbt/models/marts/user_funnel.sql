-- models/marts/user_funnel.sql
-- Daily funnel: registered → logged in → added to cart → placed order

{{ config(materialized='table') }}

WITH daily_registrations AS (
    SELECT DATE(event_timestamp) AS d, COUNT(DISTINCT user_id) AS registered_users
    FROM {{ ref('stg_users') }}
    WHERE event_type = 'user.registered'
    GROUP BY 1
),

daily_logins AS (
    SELECT DATE(event_timestamp) AS d, COUNT(DISTINCT user_id) AS logged_in_users
    FROM {{ ref('stg_users') }}
    WHERE event_type = 'user.login'
    GROUP BY 1
),

daily_cart AS (
    SELECT event_date AS d, COUNT(DISTINCT user_id) AS cart_added_users
    FROM {{ ref('fact_cart_events') }}
    WHERE event_type = 'cart.item_added'
    GROUP BY 1
),

daily_orders AS (
    SELECT order_date AS d, COUNT(DISTINCT user_id) AS ordered_users
    FROM {{ ref('fact_orders') }}
    WHERE payment_status = 'paid'
    GROUP BY 1
)

SELECT
    r.d                         AS funnel_date,
    r.registered_users,
    COALESCE(l.logged_in_users, 0) AS logged_in_users,
    COALESCE(c.cart_added_users, 0) AS cart_added_users,
    COALESCE(o.ordered_users, 0)    AS ordered_users,
    CASE
        WHEN r.registered_users = 0 THEN 0
        ELSE ROUND(
            100.0 * COALESCE(o.ordered_users, 0) / r.registered_users, 2
        )
    END                         AS reg_to_order_rate
FROM daily_registrations r
LEFT JOIN daily_logins l  ON r.d = l.d
LEFT JOIN daily_cart c    ON r.d = c.d
LEFT JOIN daily_orders o  ON r.d = o.d
ORDER BY funnel_date DESC
