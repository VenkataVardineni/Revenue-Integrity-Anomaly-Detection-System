-- Revenue Integrity Anomaly Detection System
-- Core Schema: Transactional logs + dimensions
-- ==============================================

-- Enable UUID extension for unique identifiers
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ==============================================
-- DIMENSION TABLES
-- ==============================================

-- Currency dimension
CREATE TABLE IF NOT EXISTS dim_currency (
    currency_code CHAR(3) PRIMARY KEY,
    currency_name VARCHAR(100) NOT NULL,
    usd_exchange_rate DECIMAL(12, 6) DEFAULT 1.0
);

-- Event type dimension (for referential integrity)
CREATE TABLE IF NOT EXISTS dim_event_type (
    event_type VARCHAR(50) PRIMARY KEY,
    event_category VARCHAR(50) NOT NULL,
    is_conversion_event BOOLEAN DEFAULT FALSE,
    funnel_order INT DEFAULT 0,
    description TEXT
);

-- ==============================================
-- FACT TABLES
-- ==============================================

-- Core events table - transactional logs
CREATE TABLE IF NOT EXISTS events (
    event_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    event_time TIMESTAMP WITH TIME ZONE NOT NULL,
    user_id VARCHAR(64),
    session_id VARCHAR(64),
    event_type VARCHAR(50) NOT NULL,
    amount DECIMAL(12, 2) DEFAULT 0,
    currency CHAR(3) DEFAULT 'USD',
    order_id VARCHAR(64),
    metadata_json JSONB DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Orders table - aggregated order information
CREATE TABLE IF NOT EXISTS orders (
    order_id VARCHAR(64) PRIMARY KEY,
    user_id VARCHAR(64),
    session_id VARCHAR(64),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    amount DECIMAL(12, 2) NOT NULL,
    currency CHAR(3) DEFAULT 'USD',
    status VARCHAR(20) DEFAULT 'pending',
    items_count INT DEFAULT 0,
    metadata_json JSONB DEFAULT '{}',
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- ==============================================
-- MONITORING TABLES
-- ==============================================

-- Monitor run tracking
CREATE TABLE IF NOT EXISTS monitor_runs (
    run_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    started_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    ended_at TIMESTAMP WITH TIME ZONE,
    status VARCHAR(20) DEFAULT 'running',
    window_start TIMESTAMP WITH TIME ZONE,
    window_end TIMESTAMP WITH TIME ZONE,
    baseline_periods INT DEFAULT 24,
    metrics_computed INT DEFAULT 0,
    anomalies_detected INT DEFAULT 0,
    error_message TEXT
);

-- Metric baselines (computed rolling statistics)
CREATE TABLE IF NOT EXISTS metric_baselines (
    baseline_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    metric_name VARCHAR(100) NOT NULL,
    computed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    period_start TIMESTAMP WITH TIME ZONE NOT NULL,
    period_end TIMESTAMP WITH TIME ZONE NOT NULL,
    baseline_periods INT NOT NULL,
    -- Rolling statistics
    rolling_mean DECIMAL(18, 6),
    rolling_std DECIMAL(18, 6),
    rolling_min DECIMAL(18, 6),
    rolling_max DECIMAL(18, 6),
    -- Percentiles for IQR
    percentile_25 DECIMAL(18, 6),
    percentile_50 DECIMAL(18, 6),
    percentile_75 DECIMAL(18, 6),
    iqr DECIMAL(18, 6),
    -- Sample info
    sample_count INT DEFAULT 0
);

-- Anomalies detected by various detectors
CREATE TABLE IF NOT EXISTS anomalies (
    anomaly_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    run_id UUID REFERENCES monitor_runs(run_id),
    detected_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    metric_name VARCHAR(100) NOT NULL,
    metric_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    -- Current value
    current_value DECIMAL(18, 6) NOT NULL,
    -- Baseline reference
    baseline_mean DECIMAL(18, 6),
    baseline_std DECIMAL(18, 6),
    baseline_q1 DECIMAL(18, 6),
    baseline_q3 DECIMAL(18, 6),
    baseline_iqr DECIMAL(18, 6),
    -- Deviation metrics
    z_score DECIMAL(10, 4),
    iqr_distance DECIMAL(10, 4),
    pct_change DECIMAL(10, 4),
    -- Classification
    detector VARCHAR(20) NOT NULL, -- 'zscore', 'iqr', 'rules'
    severity VARCHAR(20) NOT NULL, -- 'critical', 'warning', 'info'
    direction VARCHAR(10), -- 'above', 'below', 'missing'
    -- Context
    description TEXT,
    metadata_json JSONB DEFAULT '{}'
);

-- Rolled-up incidents (deduplicated, with root cause hints)
CREATE TABLE IF NOT EXISTS anomaly_incidents (
    incident_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    incident_key VARCHAR(255) NOT NULL, -- For deduplication
    first_detected_at TIMESTAMP WITH TIME ZONE NOT NULL,
    last_detected_at TIMESTAMP WITH TIME ZONE NOT NULL,
    -- Primary anomaly info
    metric_name VARCHAR(100) NOT NULL,
    severity VARCHAR(20) NOT NULL,
    detection_count INT DEFAULT 1,
    -- Values
    current_value DECIMAL(18, 6),
    baseline_value DECIMAL(18, 6),
    deviation_pct DECIMAL(10, 4),
    -- Root cause analysis
    root_cause_hint TEXT,
    related_metrics TEXT[],
    -- Alert status
    alert_sent_at TIMESTAMP WITH TIME ZONE,
    alert_channel VARCHAR(100),
    is_suppressed BOOLEAN DEFAULT FALSE,
    suppression_reason TEXT,
    -- Resolution
    resolved_at TIMESTAMP WITH TIME ZONE,
    resolution_notes TEXT,
    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- ==============================================
-- INDEXES
-- ==============================================

-- Events indexes
CREATE INDEX IF NOT EXISTS idx_events_event_time ON events(event_time);
CREATE INDEX IF NOT EXISTS idx_events_event_type ON events(event_type);
CREATE INDEX IF NOT EXISTS idx_events_order_id ON events(order_id);
CREATE INDEX IF NOT EXISTS idx_events_user_id ON events(user_id);
CREATE INDEX IF NOT EXISTS idx_events_session_id ON events(session_id);
CREATE INDEX IF NOT EXISTS idx_events_time_type ON events(event_time, event_type);

-- Orders indexes
CREATE INDEX IF NOT EXISTS idx_orders_created_at ON orders(created_at);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders(user_id);

-- Baselines indexes
CREATE INDEX IF NOT EXISTS idx_baselines_metric_period ON metric_baselines(metric_name, period_end);

-- Anomalies indexes
CREATE INDEX IF NOT EXISTS idx_anomalies_metric_ts ON anomalies(metric_name, metric_timestamp);
CREATE INDEX IF NOT EXISTS idx_anomalies_severity ON anomalies(severity);
CREATE INDEX IF NOT EXISTS idx_anomalies_detector ON anomalies(detector);
CREATE INDEX IF NOT EXISTS idx_anomalies_run_id ON anomalies(run_id);

-- Incidents indexes
CREATE INDEX IF NOT EXISTS idx_incidents_key ON anomaly_incidents(incident_key);
CREATE INDEX IF NOT EXISTS idx_incidents_severity ON anomaly_incidents(severity);
CREATE INDEX IF NOT EXISTS idx_incidents_last_detected ON anomaly_incidents(last_detected_at);

-- ==============================================
-- SEED DIMENSION DATA
-- ==============================================

-- Insert standard currencies
INSERT INTO dim_currency (currency_code, currency_name, usd_exchange_rate) VALUES
    ('USD', 'US Dollar', 1.000000),
    ('EUR', 'Euro', 1.080000),
    ('GBP', 'British Pound', 1.260000),
    ('JPY', 'Japanese Yen', 0.006700),
    ('CAD', 'Canadian Dollar', 0.740000)
ON CONFLICT (currency_code) DO NOTHING;

-- Insert standard event types
INSERT INTO dim_event_type (event_type, event_category, is_conversion_event, funnel_order, description) VALUES
    ('page_view', 'engagement', FALSE, 1, 'User viewed a page'),
    ('product_view', 'engagement', FALSE, 2, 'User viewed a product detail page'),
    ('add_to_cart', 'cart', TRUE, 3, 'User added item to cart'),
    ('remove_from_cart', 'cart', FALSE, 0, 'User removed item from cart'),
    ('checkout_start', 'checkout', TRUE, 4, 'User initiated checkout'),
    ('checkout_complete', 'checkout', TRUE, 5, 'User completed checkout form'),
    ('payment_initiated', 'payment', TRUE, 6, 'Payment processing started'),
    ('payment_success', 'payment', TRUE, 7, 'Payment completed successfully'),
    ('payment_failed', 'payment', FALSE, 0, 'Payment failed'),
    ('purchase', 'conversion', TRUE, 8, 'Order confirmed and completed'),
    ('refund', 'post_purchase', FALSE, 0, 'Order refunded')
ON CONFLICT (event_type) DO NOTHING;

