-- Revenue Integrity Anomaly Detection System
-- Baseline Window Logic: Rolling statistics and percentiles
-- ==============================================

-- ==============================================
-- CONFIGURATION
-- ==============================================

-- Baseline configuration table
CREATE TABLE IF NOT EXISTS baseline_config (
    config_key VARCHAR(50) PRIMARY KEY,
    config_value VARCHAR(255) NOT NULL,
    description TEXT
);

-- Insert default configuration
INSERT INTO baseline_config (config_key, config_value, description) VALUES
    ('baseline_periods', '168', 'Number of hourly periods for baseline (7 days = 168 hours)'),
    ('min_baseline_periods', '24', 'Minimum periods required for valid baseline'),
    ('zscore_warning_threshold', '2.0', 'Z-score threshold for warning severity'),
    ('zscore_critical_threshold', '3.0', 'Z-score threshold for critical severity'),
    ('iqr_multiplier', '1.5', 'IQR multiplier for outlier detection')
ON CONFLICT (config_key) DO NOTHING;


-- ==============================================
-- ROLLING STATISTICS FUNCTION
-- ==============================================

-- Function to compute rolling statistics for a metric
CREATE OR REPLACE FUNCTION compute_rolling_stats(
    p_metric_name VARCHAR(100),
    p_current_hour TIMESTAMP WITH TIME ZONE,
    p_baseline_periods INT DEFAULT 168
)
RETURNS TABLE (
    rolling_mean NUMERIC,
    rolling_std NUMERIC,
    rolling_min NUMERIC,
    rolling_max NUMERIC,
    percentile_25 NUMERIC,
    percentile_50 NUMERIC,
    percentile_75 NUMERIC,
    iqr NUMERIC,
    sample_count BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        AVG(m.metric_value)::NUMERIC AS rolling_mean,
        STDDEV(m.metric_value)::NUMERIC AS rolling_std,
        MIN(m.metric_value)::NUMERIC AS rolling_min,
        MAX(m.metric_value)::NUMERIC AS rolling_max,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY m.metric_value)::NUMERIC AS percentile_25,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY m.metric_value)::NUMERIC AS percentile_50,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY m.metric_value)::NUMERIC AS percentile_75,
        (PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY m.metric_value) - 
         PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY m.metric_value))::NUMERIC AS iqr,
        COUNT(*)::BIGINT AS sample_count
    FROM v_metrics_long m
    WHERE m.metric_name = p_metric_name
      AND m.hour_ts >= p_current_hour - (p_baseline_periods || ' hours')::INTERVAL
      AND m.hour_ts < p_current_hour;
END;
$$ LANGUAGE plpgsql;


-- ==============================================
-- BASELINE COMPUTATION PROCEDURE
-- ==============================================

-- Procedure to compute and store baselines for all metrics
CREATE OR REPLACE PROCEDURE compute_all_baselines(
    p_current_hour TIMESTAMP WITH TIME ZONE DEFAULT NULL,
    p_baseline_periods INT DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_current_hour TIMESTAMP WITH TIME ZONE;
    v_baseline_periods INT;
    v_metric RECORD;
    v_stats RECORD;
BEGIN
    -- Set current hour (default to current time truncated to hour)
    v_current_hour := COALESCE(p_current_hour, date_trunc('hour', NOW()));
    
    -- Get baseline periods from config if not specified
    IF p_baseline_periods IS NULL THEN
        SELECT config_value::INT INTO v_baseline_periods 
        FROM baseline_config 
        WHERE config_key = 'baseline_periods';
    ELSE
        v_baseline_periods := p_baseline_periods;
    END IF;
    
    -- Default if config not found
    v_baseline_periods := COALESCE(v_baseline_periods, 168);
    
    -- Compute baselines for each metric
    FOR v_metric IN 
        SELECT DISTINCT metric_name FROM metric_definitions
    LOOP
        -- Get rolling stats for this metric
        SELECT * INTO v_stats 
        FROM compute_rolling_stats(v_metric.metric_name, v_current_hour, v_baseline_periods);
        
        -- Insert baseline if we have enough data
        IF v_stats.sample_count >= 24 THEN
            INSERT INTO metric_baselines (
                metric_name,
                computed_at,
                period_start,
                period_end,
                baseline_periods,
                rolling_mean,
                rolling_std,
                rolling_min,
                rolling_max,
                percentile_25,
                percentile_50,
                percentile_75,
                iqr,
                sample_count
            ) VALUES (
                v_metric.metric_name,
                NOW(),
                v_current_hour - (v_baseline_periods || ' hours')::INTERVAL,
                v_current_hour,
                v_baseline_periods,
                v_stats.rolling_mean,
                v_stats.rolling_std,
                v_stats.rolling_min,
                v_stats.rolling_max,
                v_stats.percentile_25,
                v_stats.percentile_50,
                v_stats.percentile_75,
                v_stats.iqr,
                v_stats.sample_count
            );
        END IF;
    END LOOP;
END;
$$;


-- ==============================================
-- VIEW: CURRENT BASELINES
-- ==============================================

-- View to get the most recent baseline for each metric
CREATE OR REPLACE VIEW v_current_baselines AS
SELECT DISTINCT ON (metric_name)
    baseline_id,
    metric_name,
    computed_at,
    period_start,
    period_end,
    baseline_periods,
    rolling_mean,
    rolling_std,
    rolling_min,
    rolling_max,
    percentile_25,
    percentile_50,
    percentile_75,
    iqr,
    sample_count
FROM metric_baselines
ORDER BY metric_name, computed_at DESC;


-- ==============================================
-- VIEW: METRICS WITH BASELINES
-- ==============================================

-- Join current metric values with their baselines
CREATE OR REPLACE VIEW v_metrics_with_baseline AS
SELECT 
    m.hour_ts,
    m.metric_name,
    m.metric_value,
    b.rolling_mean AS baseline_mean,
    b.rolling_std AS baseline_std,
    b.percentile_25 AS baseline_q1,
    b.percentile_50 AS baseline_median,
    b.percentile_75 AS baseline_q3,
    b.iqr AS baseline_iqr,
    b.rolling_min AS baseline_min,
    b.rolling_max AS baseline_max,
    b.sample_count AS baseline_sample_count,
    -- Z-score calculation
    CASE 
        WHEN b.rolling_std > 0 
        THEN (m.metric_value - b.rolling_mean) / b.rolling_std
        ELSE 0 
    END AS z_score,
    -- IQR-based bounds
    b.percentile_25 - (1.5 * b.iqr) AS iqr_lower_bound,
    b.percentile_75 + (1.5 * b.iqr) AS iqr_upper_bound,
    -- Percent change from mean
    CASE 
        WHEN b.rolling_mean > 0 
        THEN ((m.metric_value - b.rolling_mean) / b.rolling_mean) * 100
        ELSE 0 
    END AS pct_change_from_mean
FROM v_metrics_long m
LEFT JOIN v_current_baselines b ON m.metric_name = b.metric_name;


-- ==============================================
-- FUNCTION: GET BASELINE FOR METRIC AT TIME
-- ==============================================

-- Function to get baseline for a specific metric at a specific time
CREATE OR REPLACE FUNCTION get_baseline_for_metric(
    p_metric_name VARCHAR(100),
    p_timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW()
)
RETURNS TABLE (
    baseline_mean NUMERIC,
    baseline_std NUMERIC,
    baseline_q1 NUMERIC,
    baseline_q3 NUMERIC,
    baseline_iqr NUMERIC,
    sample_count BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        b.rolling_mean,
        b.rolling_std,
        b.percentile_25,
        b.percentile_75,
        b.iqr,
        b.sample_count
    FROM metric_baselines b
    WHERE b.metric_name = p_metric_name
      AND b.computed_at <= p_timestamp
    ORDER BY b.computed_at DESC
    LIMIT 1;
END;
$$ LANGUAGE plpgsql;


-- ==============================================
-- SAME-HOUR-OF-WEEK BASELINE (for seasonal patterns)
-- ==============================================

-- View for same-hour-of-week baselines (comparing to same hour on previous weeks)
CREATE OR REPLACE VIEW v_same_hour_baseline AS
WITH weekly_metrics AS (
    SELECT 
        metric_name,
        hour_ts,
        metric_value,
        EXTRACT(DOW FROM hour_ts) AS day_of_week,
        EXTRACT(HOUR FROM hour_ts) AS hour_of_day
    FROM v_metrics_long
),
baseline_windows AS (
    SELECT 
        w1.metric_name,
        w1.hour_ts AS current_hour,
        w1.metric_value AS current_value,
        w1.day_of_week,
        w1.hour_of_day,
        -- Get values from same hour on previous 4 weeks
        AVG(w2.metric_value) AS same_hour_mean,
        STDDEV(w2.metric_value) AS same_hour_std,
        COUNT(w2.metric_value) AS sample_count
    FROM weekly_metrics w1
    LEFT JOIN weekly_metrics w2 ON 
        w1.metric_name = w2.metric_name
        AND w1.day_of_week = w2.day_of_week
        AND w1.hour_of_day = w2.hour_of_day
        AND w2.hour_ts >= w1.hour_ts - INTERVAL '28 days'
        AND w2.hour_ts < w1.hour_ts
    GROUP BY w1.metric_name, w1.hour_ts, w1.metric_value, w1.day_of_week, w1.hour_of_day
)
SELECT 
    metric_name,
    current_hour,
    current_value,
    same_hour_mean,
    same_hour_std,
    sample_count,
    CASE 
        WHEN same_hour_std > 0 
        THEN (current_value - same_hour_mean) / same_hour_std
        ELSE 0 
    END AS same_hour_zscore
FROM baseline_windows
WHERE sample_count >= 2;

