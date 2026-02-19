-- Revenue Integrity Anomaly Detection System
-- Metric Layer: Hourly funnels + Revenue KPIs
-- ==============================================

-- ==============================================
-- HOURLY EVENT COUNTS
-- ==============================================

-- Hourly counts for each event type
CREATE OR REPLACE VIEW v_hourly_event_counts AS
SELECT 
    date_trunc('hour', event_time) AS hour_ts,
    event_type,
    COUNT(*) AS event_count,
    COUNT(DISTINCT user_id) AS unique_users,
    COUNT(DISTINCT session_id) AS unique_sessions,
    SUM(amount) AS total_amount,
    AVG(amount) FILTER (WHERE amount > 0) AS avg_amount
FROM events
GROUP BY date_trunc('hour', event_time), event_type;


-- Pivoted hourly funnel metrics (one row per hour)
CREATE OR REPLACE VIEW v_hourly_funnel AS
SELECT 
    hour_ts,
    COALESCE(SUM(event_count) FILTER (WHERE event_type = 'page_view'), 0) AS page_views,
    COALESCE(SUM(event_count) FILTER (WHERE event_type = 'product_view'), 0) AS product_views,
    COALESCE(SUM(event_count) FILTER (WHERE event_type = 'add_to_cart'), 0) AS add_to_cart,
    COALESCE(SUM(event_count) FILTER (WHERE event_type = 'checkout_start'), 0) AS checkout_start,
    COALESCE(SUM(event_count) FILTER (WHERE event_type = 'checkout_complete'), 0) AS checkout_complete,
    COALESCE(SUM(event_count) FILTER (WHERE event_type = 'payment_initiated'), 0) AS payment_initiated,
    COALESCE(SUM(event_count) FILTER (WHERE event_type = 'payment_success'), 0) AS payment_success,
    COALESCE(SUM(event_count) FILTER (WHERE event_type = 'payment_failed'), 0) AS payment_failed,
    COALESCE(SUM(event_count) FILTER (WHERE event_type = 'purchase'), 0) AS purchases,
    -- Unique counts
    COALESCE(SUM(unique_users) FILTER (WHERE event_type = 'page_view'), 0) AS unique_users,
    COALESCE(SUM(unique_sessions) FILTER (WHERE event_type = 'page_view'), 0) AS unique_sessions
FROM v_hourly_event_counts
GROUP BY hour_ts;


-- ==============================================
-- CONVERSION RATES
-- ==============================================

-- Hourly conversion rates
CREATE OR REPLACE VIEW v_hourly_conversion_rates AS
SELECT 
    hour_ts,
    -- Funnel conversion rates
    CASE 
        WHEN page_views > 0 
        THEN ROUND((product_views::NUMERIC / page_views) * 100, 2)
        ELSE 0 
    END AS pv_to_product_rate,
    
    CASE 
        WHEN product_views > 0 
        THEN ROUND((add_to_cart::NUMERIC / product_views) * 100, 2)
        ELSE 0 
    END AS product_to_cart_rate,
    
    CASE 
        WHEN add_to_cart > 0 
        THEN ROUND((checkout_start::NUMERIC / add_to_cart) * 100, 2)
        ELSE 0 
    END AS cart_to_checkout_rate,
    
    CASE 
        WHEN checkout_start > 0 
        THEN ROUND((checkout_complete::NUMERIC / checkout_start) * 100, 2)
        ELSE 0 
    END AS checkout_completion_rate,
    
    CASE 
        WHEN checkout_complete > 0 
        THEN ROUND((purchases::NUMERIC / checkout_complete) * 100, 2)
        ELSE 0 
    END AS checkout_to_purchase_rate,
    
    -- Overall conversion rate
    CASE 
        WHEN unique_sessions > 0 
        THEN ROUND((purchases::NUMERIC / unique_sessions) * 100, 2)
        ELSE 0 
    END AS session_conversion_rate,
    
    -- Payment success rate
    CASE 
        WHEN payment_initiated > 0 
        THEN ROUND((payment_success::NUMERIC / payment_initiated) * 100, 2)
        ELSE 0 
    END AS payment_success_rate
FROM v_hourly_funnel;


-- ==============================================
-- REVENUE KPIs
-- ==============================================

-- Hourly revenue metrics
CREATE OR REPLACE VIEW v_hourly_revenue AS
SELECT 
    date_trunc('hour', event_time) AS hour_ts,
    -- Gross revenue
    SUM(amount) AS gross_revenue,
    -- Order metrics
    COUNT(DISTINCT order_id) AS order_count,
    -- Average order value
    CASE 
        WHEN COUNT(DISTINCT order_id) > 0 
        THEN ROUND(SUM(amount) / COUNT(DISTINCT order_id), 2)
        ELSE 0 
    END AS avg_order_value,
    -- Revenue per session (from purchases only)
    COUNT(DISTINCT session_id) AS purchasing_sessions
FROM events
WHERE event_type = 'purchase' AND amount > 0
GROUP BY date_trunc('hour', event_time);


-- Combined revenue per session view
CREATE OR REPLACE VIEW v_hourly_revenue_per_session AS
SELECT 
    f.hour_ts,
    COALESCE(r.gross_revenue, 0) AS gross_revenue,
    COALESCE(r.order_count, 0) AS order_count,
    COALESCE(r.avg_order_value, 0) AS avg_order_value,
    f.unique_sessions,
    CASE 
        WHEN f.unique_sessions > 0 
        THEN ROUND(COALESCE(r.gross_revenue, 0) / f.unique_sessions, 2)
        ELSE 0 
    END AS revenue_per_session
FROM v_hourly_funnel f
LEFT JOIN v_hourly_revenue r ON f.hour_ts = r.hour_ts;


-- ==============================================
-- UNIFIED METRICS VIEW
-- ==============================================

-- All metrics in a single view for easy querying
CREATE OR REPLACE VIEW v_hourly_metrics AS
SELECT 
    f.hour_ts,
    -- Funnel counts
    f.page_views,
    f.product_views,
    f.add_to_cart,
    f.checkout_start,
    f.checkout_complete,
    f.payment_initiated,
    f.payment_success,
    f.payment_failed,
    f.purchases,
    f.unique_users,
    f.unique_sessions,
    -- Conversion rates
    c.pv_to_product_rate,
    c.product_to_cart_rate,
    c.cart_to_checkout_rate,
    c.checkout_completion_rate,
    c.checkout_to_purchase_rate,
    c.session_conversion_rate,
    c.payment_success_rate,
    -- Revenue
    COALESCE(r.gross_revenue, 0) AS gross_revenue,
    COALESCE(r.order_count, 0) AS order_count,
    COALESCE(r.avg_order_value, 0) AS avg_order_value,
    COALESCE(rps.revenue_per_session, 0) AS revenue_per_session
FROM v_hourly_funnel f
LEFT JOIN v_hourly_conversion_rates c ON f.hour_ts = c.hour_ts
LEFT JOIN v_hourly_revenue r ON f.hour_ts = r.hour_ts
LEFT JOIN v_hourly_revenue_per_session rps ON f.hour_ts = rps.hour_ts;


-- ==============================================
-- METRICS UNPIVOTED (for detector input)
-- ==============================================

-- Unpivoted metrics for easy detector processing
CREATE OR REPLACE VIEW v_metrics_long AS
SELECT hour_ts, 'page_views' AS metric_name, page_views::NUMERIC AS metric_value FROM v_hourly_funnel
UNION ALL
SELECT hour_ts, 'product_views', product_views::NUMERIC FROM v_hourly_funnel
UNION ALL
SELECT hour_ts, 'add_to_cart', add_to_cart::NUMERIC FROM v_hourly_funnel
UNION ALL
SELECT hour_ts, 'checkout_start', checkout_start::NUMERIC FROM v_hourly_funnel
UNION ALL
SELECT hour_ts, 'checkout_complete', checkout_complete::NUMERIC FROM v_hourly_funnel
UNION ALL
SELECT hour_ts, 'purchases', purchases::NUMERIC FROM v_hourly_funnel
UNION ALL
SELECT hour_ts, 'unique_users', unique_users::NUMERIC FROM v_hourly_funnel
UNION ALL
SELECT hour_ts, 'unique_sessions', unique_sessions::NUMERIC FROM v_hourly_funnel
UNION ALL
SELECT hour_ts, 'payment_success', payment_success::NUMERIC FROM v_hourly_funnel
UNION ALL
SELECT hour_ts, 'payment_failed', payment_failed::NUMERIC FROM v_hourly_funnel
UNION ALL
SELECT hour_ts, 'checkout_completion_rate', checkout_completion_rate::NUMERIC FROM v_hourly_conversion_rates
UNION ALL
SELECT hour_ts, 'checkout_to_purchase_rate', checkout_to_purchase_rate::NUMERIC FROM v_hourly_conversion_rates
UNION ALL
SELECT hour_ts, 'session_conversion_rate', session_conversion_rate::NUMERIC FROM v_hourly_conversion_rates
UNION ALL
SELECT hour_ts, 'payment_success_rate', payment_success_rate::NUMERIC FROM v_hourly_conversion_rates
UNION ALL
SELECT hour_ts, 'gross_revenue', gross_revenue::NUMERIC FROM v_hourly_revenue_per_session
UNION ALL
SELECT hour_ts, 'order_count', order_count::NUMERIC FROM v_hourly_revenue_per_session
UNION ALL
SELECT hour_ts, 'avg_order_value', avg_order_value::NUMERIC FROM v_hourly_revenue_per_session
UNION ALL
SELECT hour_ts, 'revenue_per_session', revenue_per_session::NUMERIC FROM v_hourly_revenue_per_session;


-- ==============================================
-- HELPER: METRIC DEFINITIONS
-- ==============================================

-- Store metric metadata
CREATE TABLE IF NOT EXISTS metric_definitions (
    metric_name VARCHAR(100) PRIMARY KEY,
    metric_category VARCHAR(50) NOT NULL,
    description TEXT,
    unit VARCHAR(20),
    higher_is_better BOOLEAN DEFAULT TRUE,
    zscore_threshold_warning DECIMAL(4,2) DEFAULT 2.0,
    zscore_threshold_critical DECIMAL(4,2) DEFAULT 3.0,
    iqr_multiplier DECIMAL(4,2) DEFAULT 1.5
);

-- Insert metric definitions
INSERT INTO metric_definitions (metric_name, metric_category, description, unit, higher_is_better) VALUES
    ('page_views', 'funnel', 'Total page views per hour', 'count', TRUE),
    ('product_views', 'funnel', 'Product detail page views per hour', 'count', TRUE),
    ('add_to_cart', 'funnel', 'Add to cart events per hour', 'count', TRUE),
    ('checkout_start', 'funnel', 'Checkout initiations per hour', 'count', TRUE),
    ('checkout_complete', 'funnel', 'Checkout completions per hour', 'count', TRUE),
    ('purchases', 'funnel', 'Completed purchases per hour', 'count', TRUE),
    ('unique_users', 'engagement', 'Unique users per hour', 'count', TRUE),
    ('unique_sessions', 'engagement', 'Unique sessions per hour', 'count', TRUE),
    ('payment_success', 'payment', 'Successful payments per hour', 'count', TRUE),
    ('payment_failed', 'payment', 'Failed payments per hour', 'count', FALSE),
    ('checkout_completion_rate', 'conversion', 'Checkout completion rate', 'percent', TRUE),
    ('checkout_to_purchase_rate', 'conversion', 'Checkout to purchase conversion', 'percent', TRUE),
    ('session_conversion_rate', 'conversion', 'Overall session conversion rate', 'percent', TRUE),
    ('payment_success_rate', 'conversion', 'Payment success rate', 'percent', TRUE),
    ('gross_revenue', 'revenue', 'Total revenue per hour', 'usd', TRUE),
    ('order_count', 'revenue', 'Number of orders per hour', 'count', TRUE),
    ('avg_order_value', 'revenue', 'Average order value', 'usd', TRUE),
    ('revenue_per_session', 'revenue', 'Revenue per session', 'usd', TRUE)
ON CONFLICT (metric_name) DO NOTHING;

