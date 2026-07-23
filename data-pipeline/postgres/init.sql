-- ═══════════════════════════════════════════════════════════════════════════
--  Foodingo Data Warehouse — PostgreSQL Init Script
--  Schemas: raw (from Kafka events) | analytics (from dbt transforms)
-- ═══════════════════════════════════════════════════════════════════════════

-- ── SCHEMAS ──────────────────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS analytics;

-- ── RAW LAYER: User Events (from user.registered + user.login topics) ─────────
CREATE TABLE IF NOT EXISTS raw.user_events (
    id              BIGSERIAL PRIMARY KEY,
    event_type      VARCHAR(50)  NOT NULL,   -- user.registered | user.login
    user_id         VARCHAR(100),
    email           VARCHAR(255),
    name            VARCHAR(255),
    event_timestamp TIMESTAMPTZ  NOT NULL,
    ingested_at     TIMESTAMPTZ  DEFAULT NOW(),
    kafka_topic     VARCHAR(100),
    kafka_partition INT,
    kafka_offset    BIGINT
);

-- ── RAW LAYER: Cart Events (from cart.* topics) ───────────────────────────────
CREATE TABLE IF NOT EXISTS raw.cart_events (
    id              BIGSERIAL PRIMARY KEY,
    event_type      VARCHAR(50)  NOT NULL,   -- cart.item_added | cart.item_removed | cart.cleared | cart.item_deleted
    user_id         VARCHAR(100) NOT NULL,
    food_id         VARCHAR(100),            -- NULL when cart.cleared
    quantity        INT          DEFAULT 0,
    event_timestamp TIMESTAMPTZ  NOT NULL,
    ingested_at     TIMESTAMPTZ  DEFAULT NOW(),
    kafka_topic     VARCHAR(100),
    kafka_offset    BIGINT
);

-- ── RAW LAYER: Order Events (from order.* + payment.verified topics) ──────────
CREATE TABLE IF NOT EXISTS raw.order_events (
    id                   BIGSERIAL PRIMARY KEY,
    event_type           VARCHAR(50)    NOT NULL,
    order_id             VARCHAR(100)   NOT NULL,
    user_id              VARCHAR(100),
    user_address         TEXT,
    email                VARCHAR(255),
    phone_number         VARCHAR(50),
    amount               DECIMAL(10, 2),
    payment_status       VARCHAR(50),
    order_status         VARCHAR(50),
    razorpay_order_id    VARCHAR(200),
    razorpay_payment_id  VARCHAR(200),
    ordered_items        JSONB,               -- List<OrderItem> as JSON
    event_timestamp      TIMESTAMPTZ    NOT NULL,
    ingested_at          TIMESTAMPTZ    DEFAULT NOW(),
    kafka_topic          VARCHAR(100),
    kafka_offset         BIGINT
);

-- ── RAW LAYER: CDC Events (from Debezium MongoDB topics) ─────────────────────
CREATE TABLE IF NOT EXISTS raw.cdc_events (
    id              BIGSERIAL PRIMARY KEY,
    collection      VARCHAR(100) NOT NULL,   -- orders | users | food | carts
    operation       VARCHAR(10)  NOT NULL,   -- c (create) | u (update) | d (delete)
    document_id     VARCHAR(100),
    full_document   JSONB,
    event_timestamp TIMESTAMPTZ  NOT NULL,
    ingested_at     TIMESTAMPTZ  DEFAULT NOW()
);

-- ── ANALYTICS LAYER: Pre-created tables (populated by dbt) ──────────────────
-- dbt creates and manages these, but create them empty so Metabase can connect

CREATE TABLE IF NOT EXISTS analytics.fact_orders (
    order_id        VARCHAR(100) PRIMARY KEY,
    user_id         VARCHAR(100),
    amount          DECIMAL(10, 2),
    payment_status  VARCHAR(50),
    order_status    VARCHAR(50),
    item_count      INT,
    order_date      DATE,
    order_hour      INT,
    created_at      TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS analytics.fact_cart_events (
    id          BIGSERIAL PRIMARY KEY,
    event_type  VARCHAR(50),
    user_id     VARCHAR(100),
    food_id     VARCHAR(100),
    quantity    INT,
    event_date  DATE,
    event_hour  INT,
    event_time  TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS analytics.daily_revenue (
    revenue_date    DATE PRIMARY KEY,
    total_revenue   DECIMAL(12, 2),
    order_count     INT,
    avg_order_value DECIMAL(10, 2),
    unique_users    INT
);

CREATE TABLE IF NOT EXISTS analytics.food_popularity (
    food_id         VARCHAR(100) PRIMARY KEY,
    food_name       VARCHAR(255),
    category        VARCHAR(100),
    order_count     INT,
    total_revenue   DECIMAL(12, 2),
    avg_price       DECIMAL(10, 2)
);

CREATE TABLE IF NOT EXISTS analytics.cart_abandonment (
    user_id             VARCHAR(100) PRIMARY KEY,
    last_cart_time      TIMESTAMPTZ,
    cart_item_count     INT,
    days_since_cart     INT,
    has_ordered_after   BOOLEAN
);

CREATE TABLE IF NOT EXISTS analytics.user_funnel (
    funnel_date         DATE PRIMARY KEY,
    registered_users    INT,
    logged_in_users     INT,
    cart_added_users    INT,
    ordered_users       INT,
    reg_to_order_rate   DECIMAL(5, 2)
);

CREATE TABLE IF NOT EXISTS analytics.ml_user_order_matrix (
    user_id     VARCHAR(100),
    food_id     VARCHAR(100),
    order_count INT,
    PRIMARY KEY (user_id, food_id)
);

-- ── INDEXES for query performance ────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_order_events_user_id      ON raw.order_events(user_id);
CREATE INDEX IF NOT EXISTS idx_order_events_timestamp    ON raw.order_events(event_timestamp);
CREATE INDEX IF NOT EXISTS idx_cart_events_user_id       ON raw.cart_events(user_id);
CREATE INDEX IF NOT EXISTS idx_user_events_email         ON raw.user_events(email);
CREATE INDEX IF NOT EXISTS idx_fact_orders_date          ON analytics.fact_orders(order_date);
CREATE INDEX IF NOT EXISTS idx_cart_user_food            ON analytics.fact_cart_events(user_id, food_id);
