-- Revenue Integrity Anomaly Detection System
-- IQR Detector: Robust outlier detection for skewed metrics
-- ==============================================
-- 
-- IQR (Interquartile Range) Method:
--   Q1 = 25th percentile
--   Q3 = 75th percentile  
--   IQR = Q3 - Q1
--
-- Outlier if:
--   value < Q1 - 1.5 * IQR  (lower outlier)
--   value > Q3 + 1.5 * IQR  (upper outlier)
--
-- For severe outliers, use 3.0 * IQR multiplier
--
-- Why IQR over Z-score for some metrics:
--   - More robust to existing outliers in baseline
--   - Better for skewed distributions (revenue, order values)
--   - Doesn't assume normal distribution
--
-- ==============================================

-- ==============================================
-- IQR ANOMALY DETECTION FUNCTION
-- ==============================================

-- Function to run IQR-based detection
CREATE OR REPLACE FUNCTION detect_iqr_anomalies(
    p_run_id UUID,
    p_window_start TIMESTAMP WITH TIME ZONE DEFAULT NULL,
    p_window_end TIMESTAMP WITH TIME ZONE DEFAULT NULL,
    p_iqr_multiplier NUMERIC DEFAULT 1.5,
    p_severe_multiplier NUMERIC DEFAULT 3.0
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
    
    -- Insert anomalies detected by IQR method
    INSERT INTO anomalies (
        run_id,
        detected_at,
        metric_name,
        metric_timestamp,
        current_value,
        baseline_mean,
        baseline_q1,
        baseline_q3,
        baseline_iqr,
        iqr_distance,
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
        mb.baseline_q1,
        mb.baseline_q3,
        mb.baseline_iqr,
        -- Calculate distance in IQR units
        CASE 
            WHEN mb.metric_value < mb.iqr_lower_bound AND mb.baseline_iqr > 0 THEN
                (mb.iqr_lower_bound - mb.metric_value) / mb.baseline_iqr
            WHEN mb.metric_value > mb.iqr_upper_bound AND mb.baseline_iqr > 0 THEN
                (mb.metric_value - mb.iqr_upper_bound) / mb.baseline_iqr
            ELSE 0
        END,
        mb.pct_change_from_mean,
        'iqr',
        CASE 
            -- Severe outliers (beyond 3x IQR)
            WHEN mb.metric_value < mb.baseline_q1 - (p_severe_multiplier * mb.baseline_iqr) THEN 'critical'
            WHEN mb.metric_value > mb.baseline_q3 + (p_severe_multiplier * mb.baseline_iqr) THEN 'critical'
            -- Standard outliers (beyond 1.5x IQR)
            WHEN mb.metric_value < mb.iqr_lower_bound THEN 'warning'
            WHEN mb.metric_value > mb.iqr_upper_bound THEN 'warning'
            ELSE 'info'
        END,
        CASE 
            WHEN mb.metric_value < mb.iqr_lower_bound THEN 'below'
            WHEN mb.metric_value > mb.iqr_upper_bound THEN 'above'
            ELSE 'none'
        END,
        FORMAT(
            '%s is %s IQR bounds. Current: %.2f, Expected range: [%.2f, %.2f] (Q1=%.2f, Q3=%.2f, IQR=%.2f)',
            mb.metric_name,
            CASE 
                WHEN mb.metric_value < mb.iqr_lower_bound THEN 'below'
                ELSE 'above'
            END,
            mb.metric_value,
            mb.iqr_lower_bound,
            mb.iqr_upper_bound,
            mb.baseline_q1,
            mb.baseline_q3,
            mb.baseline_iqr
        )
    FROM v_metrics_with_baseline mb
    WHERE mb.hour_ts >= v_window_start
      AND mb.hour_ts < v_window_end
      AND mb.baseline_iqr > 0  -- Must have valid IQR
      AND mb.baseline_sample_count >= 24  -- Minimum baseline periods
      AND (
          mb.metric_value < mb.iqr_lower_bound
          OR mb.metric_value > mb.iqr_upper_bound
      );
    
    GET DIAGNOSTICS v_anomaly_count = ROW_COUNT;
    
    RETURN v_anomaly_count;
END;
$$ LANGUAGE plpgsql;


-- ==============================================
-- VIEW: IQR ANOMALIES (Real-time)
-- ==============================================

-- View to see current IQR-based anomalies without storing
CREATE OR REPLACE VIEW v_iqr_anomalies AS
SELECT 
    mb.hour_ts,
    mb.metric_name,
    mb.metric_value,
    mb.baseline_q1,
    mb.baseline_median,
    mb.baseline_q3,
    mb.baseline_iqr,
    mb.iqr_lower_bound,
    mb.iqr_upper_bound,
    CASE 
        WHEN mb.metric_value < mb.iqr_lower_bound AND mb.baseline_iqr > 0 THEN
            (mb.iqr_lower_bound - mb.metric_value) / mb.baseline_iqr
        WHEN mb.metric_value > mb.iqr_upper_bound AND mb.baseline_iqr > 0 THEN
            (mb.metric_value - mb.iqr_upper_bound) / mb.baseline_iqr
        ELSE 0
    END AS iqr_distance,
    CASE 
        WHEN mb.metric_value < mb.baseline_q1 - (3.0 * mb.baseline_iqr) THEN 'critical'
        WHEN mb.metric_value > mb.baseline_q3 + (3.0 * mb.baseline_iqr) THEN 'critical'
        WHEN mb.metric_value < mb.iqr_lower_bound THEN 'warning'
        WHEN mb.metric_value > mb.iqr_upper_bound THEN 'warning'
        ELSE 'normal'
    END AS severity,
    CASE 
        WHEN mb.metric_value < mb.iqr_lower_bound THEN 'below'
        WHEN mb.metric_value > mb.iqr_upper_bound THEN 'above'
        ELSE 'within'
    END AS direction,
    mb.baseline_sample_count
FROM v_metrics_with_baseline mb
WHERE mb.baseline_iqr > 0
  AND mb.baseline_sample_count >= 24
  AND (
      mb.metric_value < mb.iqr_lower_bound
      OR mb.metric_value > mb.iqr_upper_bound
  )
ORDER BY 
    CASE 
        WHEN mb.metric_value < mb.iqr_lower_bound THEN mb.iqr_lower_bound - mb.metric_value
        ELSE mb.metric_value - mb.iqr_upper_bound
    END DESC;


-- ==============================================
-- REVENUE-SPECIFIC IQR DETECTOR
-- ==============================================

-- More sensitive IQR detection for revenue metrics
-- Uses tighter bounds for revenue drops
CREATE OR REPLACE FUNCTION detect_revenue_iqr_anomalies(
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
    
    -- Revenue metrics use 1.0x IQR for lower bound (more sensitive to drops)
    INSERT INTO anomalies (
        run_id,
        detected_at,
        metric_name,
        metric_timestamp,
        current_value,
        baseline_mean,
        baseline_q1,
        baseline_q3,
        baseline_iqr,
        iqr_distance,
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
        mb.baseline_q1,
        mb.baseline_q3,
        mb.baseline_iqr,
        CASE 
            WHEN mb.baseline_iqr > 0 THEN
                ABS(mb.metric_value - mb.baseline_median) / mb.baseline_iqr
            ELSE 0
        END,
        mb.pct_change_from_mean,
        'iqr',
        CASE 
            -- Severe drop (below Q1 - 1.0 * IQR for revenue)
            WHEN mb.metric_value < mb.baseline_q1 - (1.0 * mb.baseline_iqr) THEN 'critical'
            -- Severe spike (above Q3 + 2.0 * IQR)
            WHEN mb.metric_value > mb.baseline_q3 + (2.0 * mb.baseline_iqr) THEN 'critical'
            -- Warning level
            WHEN mb.metric_value < mb.baseline_q1 THEN 'warning'
            WHEN mb.metric_value > mb.baseline_q3 + (1.5 * mb.baseline_iqr) THEN 'warning'
            ELSE 'info'
        END,
        CASE 
            WHEN mb.metric_value < mb.baseline_median THEN 'below'
            WHEN mb.metric_value > mb.baseline_median THEN 'above'
            ELSE 'none'
        END,
        FORMAT(
            'REVENUE IQR: %s at $%.2f is %s typical range. Median: $%.2f, Range: [$%.2f, $%.2f]',
            mb.metric_name,
            mb.metric_value,
            CASE 
                WHEN mb.metric_value < mb.baseline_q1 THEN 'below'
                ELSE 'above'
            END,
            mb.baseline_median,
            mb.baseline_q1,
            mb.baseline_q3
        ),
        jsonb_build_object(
            'metric_category', 'revenue',
            'detection_method', 'iqr',
            'iqr_multiplier_used', 
                CASE WHEN mb.metric_value < mb.baseline_median THEN 1.0 ELSE 1.5 END
        )
    FROM v_metrics_with_baseline mb
    WHERE mb.hour_ts >= v_window_start
      AND mb.hour_ts < v_window_end
      AND mb.metric_name IN ('gross_revenue', 'order_count', 'avg_order_value', 'revenue_per_session')
      AND mb.baseline_iqr > 0
      AND mb.baseline_sample_count >= 24
      AND (
          -- Tighter lower bound for revenue drops
          mb.metric_value < mb.baseline_q1 - (1.0 * mb.baseline_iqr)
          OR
          -- Standard upper bound for spikes
          mb.metric_value > mb.baseline_q3 + (1.5 * mb.baseline_iqr)
      );
    
    GET DIAGNOSTICS v_anomaly_count = ROW_COUNT;
    
    RETURN v_anomaly_count;
END;
$$ LANGUAGE plpgsql;


-- ==============================================
-- AOV (Average Order Value) SPECIFIC DETECTOR
-- ==============================================

-- AOV often has skewed distribution, IQR works better
CREATE OR REPLACE FUNCTION detect_aov_anomalies(
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
        baseline_q1,
        baseline_q3,
        baseline_iqr,
        iqr_distance,
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
        'avg_order_value',
        mb.hour_ts,
        mb.metric_value,
        mb.baseline_mean,
        mb.baseline_q1,
        mb.baseline_q3,
        mb.baseline_iqr,
        CASE 
            WHEN mb.baseline_iqr > 0 THEN
                GREATEST(
                    CASE WHEN mb.metric_value < mb.iqr_lower_bound 
                         THEN (mb.iqr_lower_bound - mb.metric_value) / mb.baseline_iqr
                         ELSE 0 END,
                    CASE WHEN mb.metric_value > mb.iqr_upper_bound 
                         THEN (mb.metric_value - mb.iqr_upper_bound) / mb.baseline_iqr
                         ELSE 0 END
                )
            ELSE 0
        END,
        mb.pct_change_from_mean,
        'iqr',
        CASE 
            -- Zero or near-zero AOV is always critical
            WHEN mb.metric_value < 1.0 AND mb.baseline_median > 10.0 THEN 'critical'
            -- Extreme spike (potential fraud or pricing bug)
            WHEN mb.metric_value > mb.baseline_q3 * 3.0 THEN 'critical'
            -- Standard IQR bounds
            WHEN mb.metric_value < mb.iqr_lower_bound OR mb.metric_value > mb.iqr_upper_bound THEN 'warning'
            ELSE 'info'
        END,
        CASE 
            WHEN mb.metric_value < mb.baseline_median THEN 'below'
            WHEN mb.metric_value > mb.baseline_median THEN 'above'
            ELSE 'none'
        END,
        FORMAT(
            'AOV ALERT: $%.2f vs typical $%.2f (%.1f%% change). %s',
            mb.metric_value,
            mb.baseline_median,
            mb.pct_change_from_mean,
            CASE 
                WHEN mb.metric_value < 1.0 AND mb.baseline_median > 10.0 
                    THEN 'Zero/near-zero AOV detected - possible payment issue!'
                WHEN mb.metric_value > mb.baseline_q3 * 3.0 
                    THEN 'Extreme spike - investigate for fraud or pricing bug!'
                ELSE 'Value outside expected range.'
            END
        ),
        jsonb_build_object(
            'aov_baseline_median', mb.baseline_median,
            'aov_current', mb.metric_value,
            'is_zero_aov', mb.metric_value < 1.0,
            'is_extreme_spike', mb.metric_value > mb.baseline_q3 * 3.0
        )
    FROM v_metrics_with_baseline mb
    WHERE mb.hour_ts >= v_window_start
      AND mb.hour_ts < v_window_end
      AND mb.metric_name = 'avg_order_value'
      AND mb.baseline_iqr > 0
      AND mb.baseline_sample_count >= 24
      AND (
          -- Zero AOV
          (mb.metric_value < 1.0 AND mb.baseline_median > 10.0)
          OR
          -- Extreme spike
          mb.metric_value > mb.baseline_q3 * 3.0
          OR
          -- Standard IQR bounds
          mb.metric_value < mb.iqr_lower_bound
          OR
          mb.metric_value > mb.iqr_upper_bound
      );
    
    GET DIAGNOSTICS v_anomaly_count = ROW_COUNT;
    
    RETURN v_anomaly_count;
END;
$$ LANGUAGE plpgsql;


-- ==============================================
-- COMBINED IQR DETECTOR
-- ==============================================

-- Run all IQR-based detectors
CREATE OR REPLACE FUNCTION run_all_iqr_detectors(
    p_run_id UUID,
    p_window_start TIMESTAMP WITH TIME ZONE DEFAULT NULL,
    p_window_end TIMESTAMP WITH TIME ZONE DEFAULT NULL
)
RETURNS INT AS $$
DECLARE
    v_total_anomalies INT := 0;
    v_count INT;
BEGIN
    -- General IQR detection
    SELECT detect_iqr_anomalies(p_run_id, p_window_start, p_window_end) INTO v_count;
    v_total_anomalies := v_total_anomalies + v_count;
    
    -- Revenue-specific IQR detection
    SELECT detect_revenue_iqr_anomalies(p_run_id, p_window_start, p_window_end) INTO v_count;
    v_total_anomalies := v_total_anomalies + v_count;
    
    -- AOV-specific detection
    SELECT detect_aov_anomalies(p_run_id, p_window_start, p_window_end) INTO v_count;
    v_total_anomalies := v_total_anomalies + v_count;
    
    RETURN v_total_anomalies;
END;
$$ LANGUAGE plpgsql;

