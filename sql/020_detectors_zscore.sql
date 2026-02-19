-- Revenue Integrity Anomaly Detection System
-- Z-Score Detector: Statistical anomaly detection
-- ==============================================
-- 
-- Z-Score Formula: z = (value - mean) / std
-- 
-- Thresholds:
--   |z| >= 3.0 → CRITICAL
--   |z| >= 2.0 → WARNING
--   |z| >= 1.5 → INFO (optional)
--
-- ==============================================

-- ==============================================
-- Z-SCORE ANOMALY DETECTION FUNCTION
-- ==============================================

-- Function to run Z-score detection on recent data
CREATE OR REPLACE FUNCTION detect_zscore_anomalies(
    p_run_id UUID,
    p_window_start TIMESTAMP WITH TIME ZONE DEFAULT NULL,
    p_window_end TIMESTAMP WITH TIME ZONE DEFAULT NULL,
    p_warning_threshold NUMERIC DEFAULT 2.0,
    p_critical_threshold NUMERIC DEFAULT 3.0
)
RETURNS INT AS $$
DECLARE
    v_window_start TIMESTAMP WITH TIME ZONE;
    v_window_end TIMESTAMP WITH TIME ZONE;
    v_anomaly_count INT := 0;
BEGIN
    -- Set window defaults
    v_window_end := COALESCE(p_window_end, date_trunc('hour', NOW()));
    v_window_start := COALESCE(p_window_start, v_window_end - INTERVAL '1 hour');
    
    -- Insert anomalies detected by Z-score
    INSERT INTO anomalies (
        run_id,
        detected_at,
        metric_name,
        metric_timestamp,
        current_value,
        baseline_mean,
        baseline_std,
        z_score,
        pct_change,
        detector,
        severity,
        direction,
        description
    )
    SELECT 
        p_run_id,
        NOW(),
        mb.metric_name,
        mb.hour_ts,
        mb.metric_value,
        mb.baseline_mean,
        mb.baseline_std,
        mb.z_score,
        mb.pct_change_from_mean,
        'zscore',
        CASE 
            WHEN ABS(mb.z_score) >= p_critical_threshold THEN 'critical'
            WHEN ABS(mb.z_score) >= p_warning_threshold THEN 'warning'
            ELSE 'info'
        END,
        CASE 
            WHEN mb.z_score > 0 THEN 'above'
            WHEN mb.z_score < 0 THEN 'below'
            ELSE 'none'
        END,
        FORMAT(
            '%s %s by %.1f standard deviations (z=%.2f). Current: %.2f, Baseline: %.2f ± %.2f',
            mb.metric_name,
            CASE WHEN mb.z_score > 0 THEN 'increased' ELSE 'decreased' END,
            ABS(mb.z_score),
            mb.z_score,
            mb.metric_value,
            mb.baseline_mean,
            mb.baseline_std
        )
    FROM v_metrics_with_baseline mb
    WHERE mb.hour_ts >= v_window_start
      AND mb.hour_ts < v_window_end
      AND mb.baseline_std > 0  -- Must have valid std deviation
      AND mb.baseline_sample_count >= 24  -- Minimum baseline periods
      AND ABS(mb.z_score) >= p_warning_threshold;  -- At least warning level
    
    GET DIAGNOSTICS v_anomaly_count = ROW_COUNT;
    
    RETURN v_anomaly_count;
END;
$$ LANGUAGE plpgsql;


-- ==============================================
-- VIEW: Z-SCORE ANOMALIES (Real-time)
-- ==============================================

-- View to see current Z-score anomalies (without storing them)
CREATE OR REPLACE VIEW v_zscore_anomalies AS
SELECT 
    mb.hour_ts,
    mb.metric_name,
    mb.metric_value,
    mb.baseline_mean,
    mb.baseline_std,
    mb.z_score,
    mb.pct_change_from_mean,
    CASE 
        WHEN ABS(mb.z_score) >= 3.0 THEN 'critical'
        WHEN ABS(mb.z_score) >= 2.0 THEN 'warning'
        WHEN ABS(mb.z_score) >= 1.5 THEN 'info'
        ELSE 'normal'
    END AS severity,
    CASE 
        WHEN mb.z_score > 0 THEN 'above'
        WHEN mb.z_score < 0 THEN 'below'
        ELSE 'none'
    END AS direction,
    mb.baseline_sample_count
FROM v_metrics_with_baseline mb
WHERE mb.baseline_std > 0
  AND mb.baseline_sample_count >= 24
  AND ABS(mb.z_score) >= 1.5
ORDER BY ABS(mb.z_score) DESC;


-- ==============================================
-- METRIC-SPECIFIC Z-SCORE DETECTORS
-- ==============================================

-- Revenue-focused Z-score detection (higher sensitivity for revenue drops)
CREATE OR REPLACE FUNCTION detect_revenue_zscore_anomalies(
    p_run_id UUID,
    p_window_start TIMESTAMP WITH TIME ZONE DEFAULT NULL,
    p_window_end TIMESTAMP WITH TIME ZONE DEFAULT NULL
)
RETURNS INT AS $$
DECLARE
    v_window_start TIMESTAMP WITH TIME ZONE;
    v_window_end TIMESTAMP WITH TIME ZONE;
    v_anomaly_count INT := 0;
BEGIN
    v_window_end := COALESCE(p_window_end, date_trunc('hour', NOW()));
    v_window_start := COALESCE(p_window_start, v_window_end - INTERVAL '1 hour');
    
    -- Revenue metrics get lower thresholds (more sensitive)
    INSERT INTO anomalies (
        run_id,
        detected_at,
        metric_name,
        metric_timestamp,
        current_value,
        baseline_mean,
        baseline_std,
        z_score,
        pct_change,
        detector,
        severity,
        direction,
        description,
        metadata_json
    )
    SELECT 
        p_run_id,
        NOW(),
        mb.metric_name,
        mb.hour_ts,
        mb.metric_value,
        mb.baseline_mean,
        mb.baseline_std,
        mb.z_score,
        mb.pct_change_from_mean,
        'zscore',
        CASE 
            -- Revenue drops are more critical
            WHEN mb.metric_name IN ('gross_revenue', 'order_count', 'avg_order_value') 
                 AND mb.z_score <= -2.5 THEN 'critical'
            WHEN mb.metric_name IN ('gross_revenue', 'order_count', 'avg_order_value') 
                 AND mb.z_score <= -1.5 THEN 'warning'
            -- Standard thresholds for spikes
            WHEN ABS(mb.z_score) >= 3.0 THEN 'critical'
            WHEN ABS(mb.z_score) >= 2.0 THEN 'warning'
            ELSE 'info'
        END,
        CASE 
            WHEN mb.z_score > 0 THEN 'above'
            WHEN mb.z_score < 0 THEN 'below'
            ELSE 'none'
        END,
        FORMAT(
            'REVENUE ALERT: %s %s by %.1f%% (z=%.2f). Current: $%.2f, Expected: $%.2f',
            mb.metric_name,
            CASE WHEN mb.z_score > 0 THEN 'spiked' ELSE 'dropped' END,
            ABS(mb.pct_change_from_mean),
            mb.z_score,
            mb.metric_value,
            mb.baseline_mean
        ),
        jsonb_build_object(
            'metric_category', 'revenue',
            'impact_severity', 'high',
            'requires_immediate_action', mb.z_score <= -2.5
        )
    FROM v_metrics_with_baseline mb
    WHERE mb.hour_ts >= v_window_start
      AND mb.hour_ts < v_window_end
      AND mb.metric_name IN ('gross_revenue', 'order_count', 'avg_order_value', 'revenue_per_session')
      AND mb.baseline_std > 0
      AND mb.baseline_sample_count >= 24
      AND (
          -- Lower threshold for revenue drops
          (mb.z_score <= -1.5)
          OR
          -- Standard threshold for spikes
          (mb.z_score >= 2.0)
      );
    
    GET DIAGNOSTICS v_anomaly_count = ROW_COUNT;
    
    RETURN v_anomaly_count;
END;
$$ LANGUAGE plpgsql;


-- Conversion rate focused Z-score detection
CREATE OR REPLACE FUNCTION detect_conversion_zscore_anomalies(
    p_run_id UUID,
    p_window_start TIMESTAMP WITH TIME ZONE DEFAULT NULL,
    p_window_end TIMESTAMP WITH TIME ZONE DEFAULT NULL
)
RETURNS INT AS $$
DECLARE
    v_window_start TIMESTAMP WITH TIME ZONE;
    v_window_end TIMESTAMP WITH TIME ZONE;
    v_anomaly_count INT := 0;
BEGIN
    v_window_end := COALESCE(p_window_end, date_trunc('hour', NOW()));
    v_window_start := COALESCE(p_window_start, v_window_end - INTERVAL '1 hour');
    
    INSERT INTO anomalies (
        run_id,
        detected_at,
        metric_name,
        metric_timestamp,
        current_value,
        baseline_mean,
        baseline_std,
        z_score,
        pct_change,
        detector,
        severity,
        direction,
        description,
        metadata_json
    )
    SELECT 
        p_run_id,
        NOW(),
        mb.metric_name,
        mb.hour_ts,
        mb.metric_value,
        mb.baseline_mean,
        mb.baseline_std,
        mb.z_score,
        mb.pct_change_from_mean,
        'zscore',
        CASE 
            WHEN ABS(mb.z_score) >= 3.0 THEN 'critical'
            WHEN ABS(mb.z_score) >= 2.0 THEN 'warning'
            ELSE 'info'
        END,
        CASE 
            WHEN mb.z_score > 0 THEN 'above'
            WHEN mb.z_score < 0 THEN 'below'
            ELSE 'none'
        END,
        FORMAT(
            'CONVERSION ALERT: %s at %.1f%% vs baseline %.1f%% (z=%.2f)',
            mb.metric_name,
            mb.metric_value,
            mb.baseline_mean,
            mb.z_score
        ),
        jsonb_build_object(
            'metric_category', 'conversion',
            'baseline_rate', mb.baseline_mean,
            'current_rate', mb.metric_value
        )
    FROM v_metrics_with_baseline mb
    WHERE mb.hour_ts >= v_window_start
      AND mb.hour_ts < v_window_end
      AND mb.metric_name IN (
          'checkout_completion_rate', 
          'checkout_to_purchase_rate', 
          'session_conversion_rate',
          'payment_success_rate'
      )
      AND mb.baseline_std > 0
      AND mb.baseline_sample_count >= 24
      AND ABS(mb.z_score) >= 2.0;
    
    GET DIAGNOSTICS v_anomaly_count = ROW_COUNT;
    
    RETURN v_anomaly_count;
END;
$$ LANGUAGE plpgsql;


-- ==============================================
-- COMBINED Z-SCORE DETECTOR
-- ==============================================

-- Run all Z-score based detectors
CREATE OR REPLACE FUNCTION run_all_zscore_detectors(
    p_run_id UUID,
    p_window_start TIMESTAMP WITH TIME ZONE DEFAULT NULL,
    p_window_end TIMESTAMP WITH TIME ZONE DEFAULT NULL
)
RETURNS INT AS $$
DECLARE
    v_total_anomalies INT := 0;
    v_count INT;
BEGIN
    -- General Z-score detection
    SELECT detect_zscore_anomalies(p_run_id, p_window_start, p_window_end) INTO v_count;
    v_total_anomalies := v_total_anomalies + v_count;
    
    -- Revenue-specific detection (may create duplicates, handled in rollup)
    SELECT detect_revenue_zscore_anomalies(p_run_id, p_window_start, p_window_end) INTO v_count;
    v_total_anomalies := v_total_anomalies + v_count;
    
    -- Conversion-specific detection
    SELECT detect_conversion_zscore_anomalies(p_run_id, p_window_start, p_window_end) INTO v_count;
    v_total_anomalies := v_total_anomalies + v_count;
    
    RETURN v_total_anomalies;
END;
$$ LANGUAGE plpgsql;

