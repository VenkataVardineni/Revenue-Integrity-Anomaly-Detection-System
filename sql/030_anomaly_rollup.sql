-- Revenue Integrity Anomaly Detection System
-- Anomaly Rollup: Deduplication + Root-Cause Hints
-- ==============================================
--
-- This module:
--   1. Merges anomalies from multiple detectors
--   2. Deduplicates by metric/time bucket
--   3. Chooses final severity (most severe wins)
--   4. Adds root-cause hints based on patterns
--   5. Creates actionable incidents
--
-- ==============================================

-- ==============================================
-- ROOT CAUSE HINT PATTERNS
-- ==============================================

-- Table to store root cause patterns
CREATE TABLE IF NOT EXISTS root_cause_patterns (
    pattern_id SERIAL PRIMARY KEY,
    pattern_name VARCHAR(100) NOT NULL,
    description TEXT,
    -- Conditions (JSON array of metric patterns)
    conditions JSONB NOT NULL,
    -- Hint to display when pattern matches
    root_cause_hint TEXT NOT NULL,
    -- Priority (higher = check first)
    priority INT DEFAULT 0,
    is_active BOOLEAN DEFAULT TRUE
);

-- Insert common root cause patterns
INSERT INTO root_cause_patterns (pattern_name, conditions, root_cause_hint, priority) VALUES
    (
        'payment_gateway_down',
        '[{"metric": "payment_success_rate", "direction": "below", "severity": ["critical", "warning"]},
          {"metric": "payment_gateway_failure", "direction": "below", "severity": ["critical"]}]'::JSONB,
        'Payment gateway appears to be failing. Check payment provider status page and API logs.',
        100
    ),
    (
        'purchase_pipeline_broken',
        '[{"metric": "checkout_to_purchase_rate", "direction": "below"},
          {"metric": "funnel_break_purchase", "direction": "missing"}]'::JSONB,
        'Purchase events missing despite successful payments. Check order creation service and event publishing.',
        95
    ),
    (
        'cart_tracking_broken',
        '[{"metric": "add_to_cart", "direction": "below"},
          {"metric": "add_to_cart_count", "direction": "missing"}]'::JSONB,
        'Add-to-cart events missing. Check frontend tracking code and analytics pipeline.',
        90
    ),
    (
        'checkout_conversion_drop',
        '[{"metric": "checkout_completion_rate", "direction": "below"},
          {"metric": "checkout_complete", "direction": "below"}]'::JSONB,
        'Checkout completion dropped. Possible causes: checkout form errors, payment form issues, or slow page load.',
        85
    ),
    (
        'pricing_bug_spike',
        '[{"metric": "avg_order_value", "direction": "above", "severity": ["critical"]},
          {"metric": "gross_revenue", "direction": "above"}]'::JSONB,
        'AOV spike detected. Check for pricing bugs, discount code issues, or potential fraud.',
        80
    ),
    (
        'pricing_bug_zero',
        '[{"metric": "avg_order_value", "direction": "below"},
          {"metric": "zero_revenue_bug", "direction": "below"}]'::JSONB,
        'Orders with zero amount detected. Check product pricing, discount logic, and checkout amount calculation.',
        80
    ),
    (
        'traffic_source_issue',
        '[{"metric": "unique_users_drop", "direction": "below"},
          {"metric": "unique_sessions_drop", "direction": "below"}]'::JSONB,
        'Site-wide traffic drop. Check: CDN issues, DNS problems, ad campaigns paused, or SEO penalty.',
        75
    ),
    (
        'revenue_drop_with_stable_orders',
        '[{"metric": "gross_revenue", "direction": "below"},
          {"metric": "order_count", "direction": "none"}]'::JSONB,
        'Revenue dropped but order count stable. Check AOV and product mix - possibly lower-value items selling.',
        70
    ),
    (
        'revenue_spike_fraud_check',
        '[{"metric": "gross_revenue", "direction": "above", "severity": ["critical"]},
          {"metric": "avg_order_value", "direction": "above"}]'::JSONB,
        'Revenue and AOV spike. Could be legitimate high-value orders or potential fraud. Review recent orders.',
        65
    )
ON CONFLICT DO NOTHING;


-- ==============================================
-- INCIDENT CREATION FUNCTION
-- ==============================================

-- Function to roll up anomalies into incidents
CREATE OR REPLACE FUNCTION create_anomaly_incidents(
    p_run_id UUID
)
RETURNS INT AS $$
DECLARE
    v_incident_count INT := 0;
    v_anomaly RECORD;
    v_existing_incident RECORD;
    v_incident_key VARCHAR(255);
    v_root_cause_hint TEXT;
    v_related_metrics TEXT[];
BEGIN
    -- Process each unique metric/timestamp combination
    FOR v_anomaly IN (
        SELECT 
            metric_name,
            metric_timestamp,
            -- Aggregate across detectors
            MAX(severity) AS severity,  -- Take most severe
            ARRAY_AGG(DISTINCT detector) AS detectors,
            MAX(current_value) AS current_value,
            MAX(baseline_mean) AS baseline_value,
            MAX(pct_change) AS deviation_pct,
            STRING_AGG(DISTINCT description, ' | ') AS combined_description,
            COUNT(*) AS detection_count
        FROM anomalies
        WHERE run_id = p_run_id
        GROUP BY metric_name, metric_timestamp
        ORDER BY 
            CASE MAX(severity) 
                WHEN 'critical' THEN 1 
                WHEN 'warning' THEN 2 
                ELSE 3 
            END,
            MAX(ABS(pct_change)) DESC NULLS LAST
    )
    LOOP
        -- Generate incident key for deduplication
        v_incident_key := v_anomaly.metric_name || '_' || 
                          date_trunc('hour', v_anomaly.metric_timestamp)::TEXT;
        
        -- Find related metrics (other anomalies in same time window)
        SELECT ARRAY_AGG(DISTINCT metric_name)
        INTO v_related_metrics
        FROM anomalies
        WHERE run_id = p_run_id
          AND metric_timestamp = v_anomaly.metric_timestamp
          AND metric_name != v_anomaly.metric_name;
        
        -- Get root cause hint
        SELECT get_root_cause_hint(p_run_id, v_anomaly.metric_timestamp)
        INTO v_root_cause_hint;
        
        -- Check for existing incident
        SELECT * INTO v_existing_incident
        FROM anomaly_incidents
        WHERE incident_key = v_incident_key
          AND resolved_at IS NULL
          AND last_detected_at >= NOW() - INTERVAL '24 hours';
        
        IF v_existing_incident IS NOT NULL THEN
            -- Update existing incident
            UPDATE anomaly_incidents
            SET 
                last_detected_at = NOW(),
                detection_count = detection_count + 1,
                current_value = v_anomaly.current_value,
                deviation_pct = v_anomaly.deviation_pct,
                severity = CASE 
                    WHEN v_anomaly.severity = 'critical' THEN 'critical'
                    WHEN severity = 'critical' THEN 'critical'
                    ELSE v_anomaly.severity
                END,
                root_cause_hint = COALESCE(v_root_cause_hint, root_cause_hint),
                related_metrics = COALESCE(v_related_metrics, related_metrics),
                updated_at = NOW()
            WHERE incident_id = v_existing_incident.incident_id;
        ELSE
            -- Create new incident
            INSERT INTO anomaly_incidents (
                incident_key,
                first_detected_at,
                last_detected_at,
                metric_name,
                severity,
                detection_count,
                current_value,
                baseline_value,
                deviation_pct,
                root_cause_hint,
                related_metrics
            ) VALUES (
                v_incident_key,
                NOW(),
                NOW(),
                v_anomaly.metric_name,
                v_anomaly.severity,
                v_anomaly.detection_count,
                v_anomaly.current_value,
                v_anomaly.baseline_value,
                v_anomaly.deviation_pct,
                v_root_cause_hint,
                v_related_metrics
            );
            
            v_incident_count := v_incident_count + 1;
        END IF;
    END LOOP;
    
    RETURN v_incident_count;
END;
$$ LANGUAGE plpgsql;


-- ==============================================
-- ROOT CAUSE HINT MATCHING
-- ==============================================

-- Function to find matching root cause pattern
CREATE OR REPLACE FUNCTION get_root_cause_hint(
    p_run_id UUID,
    p_timestamp TIMESTAMP WITH TIME ZONE
)
RETURNS TEXT AS $$
DECLARE
    v_pattern RECORD;
    v_anomalies JSONB;
    v_matches BOOLEAN;
    v_condition RECORD;
BEGIN
    -- Get all anomalies for this timestamp as JSON
    SELECT jsonb_agg(jsonb_build_object(
        'metric', metric_name,
        'direction', direction,
        'severity', severity
    ))
    INTO v_anomalies
    FROM anomalies
    WHERE run_id = p_run_id
      AND metric_timestamp = p_timestamp;
    
    IF v_anomalies IS NULL THEN
        RETURN NULL;
    END IF;
    
    -- Check each pattern in priority order
    FOR v_pattern IN (
        SELECT * FROM root_cause_patterns
        WHERE is_active = TRUE
        ORDER BY priority DESC
    )
    LOOP
        v_matches := TRUE;
        
        -- Check if all conditions in pattern are met
        FOR v_condition IN (
            SELECT * FROM jsonb_array_elements(v_pattern.conditions)
        )
        LOOP
            -- Check if any anomaly matches this condition
            IF NOT EXISTS (
                SELECT 1 FROM jsonb_array_elements(v_anomalies) AS a
                WHERE (a->>'metric') LIKE (v_condition.value->>'metric') || '%'
                  AND (
                      (v_condition.value->>'direction') IS NULL 
                      OR (a->>'direction') = (v_condition.value->>'direction')
                  )
            ) THEN
                v_matches := FALSE;
                EXIT;
            END IF;
        END LOOP;
        
        IF v_matches THEN
            RETURN v_pattern.root_cause_hint;
        END IF;
    END LOOP;
    
    -- Default hint based on most severe anomaly
    RETURN 'Review the affected metrics and check recent deployments or configuration changes.';
END;
$$ LANGUAGE plpgsql;


-- ==============================================
-- VIEW: ACTIVE INCIDENTS
-- ==============================================

-- View to see current active incidents
CREATE OR REPLACE VIEW v_active_incidents AS
SELECT 
    incident_id,
    incident_key,
    first_detected_at,
    last_detected_at,
    metric_name,
    severity,
    detection_count,
    current_value,
    baseline_value,
    deviation_pct,
    root_cause_hint,
    related_metrics,
    alert_sent_at,
    is_suppressed,
    -- Duration
    EXTRACT(EPOCH FROM (NOW() - first_detected_at)) / 3600 AS hours_active,
    -- Time since last detection
    EXTRACT(EPOCH FROM (NOW() - last_detected_at)) / 60 AS minutes_since_last
FROM anomaly_incidents
WHERE resolved_at IS NULL
  AND is_suppressed = FALSE
ORDER BY 
    CASE severity 
        WHEN 'critical' THEN 1 
        WHEN 'warning' THEN 2 
        ELSE 3 
    END,
    last_detected_at DESC;


-- ==============================================
-- VIEW: INCIDENT SUMMARY
-- ==============================================

-- Summary view for reporting
CREATE OR REPLACE VIEW v_incident_summary AS
SELECT 
    severity,
    COUNT(*) AS incident_count,
    COUNT(*) FILTER (WHERE alert_sent_at IS NULL) AS pending_alerts,
    COUNT(*) FILTER (WHERE resolved_at IS NOT NULL) AS resolved_count,
    MIN(first_detected_at) AS earliest_detection,
    MAX(last_detected_at) AS latest_detection
FROM anomaly_incidents
WHERE last_detected_at >= NOW() - INTERVAL '24 hours'
GROUP BY severity
ORDER BY 
    CASE severity 
        WHEN 'critical' THEN 1 
        WHEN 'warning' THEN 2 
        ELSE 3 
    END;


-- ==============================================
-- FULL DETECTION PIPELINE
-- ==============================================

-- Run full detection pipeline
CREATE OR REPLACE FUNCTION run_full_detection_pipeline(
    p_window_start TIMESTAMP WITH TIME ZONE DEFAULT NULL,
    p_window_end TIMESTAMP WITH TIME ZONE DEFAULT NULL,
    p_baseline_periods INT DEFAULT 168
)
RETURNS TABLE (
    run_id UUID,
    zscore_anomalies INT,
    iqr_anomalies INT,
    rules_anomalies INT,
    total_anomalies INT,
    new_incidents INT
) AS $$
DECLARE
    v_run_id UUID;
    v_window_start TIMESTAMP WITH TIME ZONE;
    v_window_end TIMESTAMP WITH TIME ZONE;
    v_zscore_count INT := 0;
    v_iqr_count INT := 0;
    v_rules_count INT := 0;
    v_incident_count INT := 0;
BEGIN
    -- Set window
    v_window_end := COALESCE(p_window_end, date_trunc('hour', NOW()));
    v_window_start := COALESCE(p_window_start, v_window_end - INTERVAL '1 hour');
    
    -- Create run record
    INSERT INTO monitor_runs (window_start, window_end, baseline_periods, status)
    VALUES (v_window_start, v_window_end, p_baseline_periods, 'running')
    RETURNING monitor_runs.run_id INTO v_run_id;
    
    -- Compute baselines
    CALL compute_all_baselines(v_window_end, p_baseline_periods);
    
    -- Run Z-score detectors
    SELECT run_all_zscore_detectors(v_run_id, v_window_start, v_window_end) INTO v_zscore_count;
    
    -- Run IQR detectors
    SELECT run_all_iqr_detectors(v_run_id, v_window_start, v_window_end) INTO v_iqr_count;
    
    -- Run rules-based detectors
    SELECT run_all_rules_detectors(v_run_id, v_window_start, v_window_end) INTO v_rules_count;
    
    -- Create/update incidents
    SELECT create_anomaly_incidents(v_run_id) INTO v_incident_count;
    
    -- Update run record
    UPDATE monitor_runs
    SET 
        ended_at = NOW(),
        status = 'completed',
        anomalies_detected = v_zscore_count + v_iqr_count + v_rules_count
    WHERE monitor_runs.run_id = v_run_id;
    
    -- Return results
    run_id := v_run_id;
    zscore_anomalies := v_zscore_count;
    iqr_anomalies := v_iqr_count;
    rules_anomalies := v_rules_count;
    total_anomalies := v_zscore_count + v_iqr_count + v_rules_count;
    new_incidents := v_incident_count;
    
    RETURN NEXT;
END;
$$ LANGUAGE plpgsql;


-- ==============================================
-- CLEANUP FUNCTIONS
-- ==============================================

-- Resolve old incidents
CREATE OR REPLACE FUNCTION resolve_stale_incidents(
    p_stale_hours INT DEFAULT 24
)
RETURNS INT AS $$
DECLARE
    v_resolved_count INT;
BEGIN
    UPDATE anomaly_incidents
    SET 
        resolved_at = NOW(),
        resolution_notes = 'Auto-resolved: no new detections in ' || p_stale_hours || ' hours'
    WHERE resolved_at IS NULL
      AND last_detected_at < NOW() - (p_stale_hours || ' hours')::INTERVAL;
    
    GET DIAGNOSTICS v_resolved_count = ROW_COUNT;
    
    RETURN v_resolved_count;
END;
$$ LANGUAGE plpgsql;


-- Clean up old anomalies
CREATE OR REPLACE FUNCTION cleanup_old_anomalies(
    p_retention_days INT DEFAULT 30
)
RETURNS INT AS $$
DECLARE
    v_deleted_count INT;
BEGIN
    DELETE FROM anomalies
    WHERE detected_at < NOW() - (p_retention_days || ' days')::INTERVAL;
    
    GET DIAGNOSTICS v_deleted_count = ROW_COUNT;
    
    RETURN v_deleted_count;
END;
$$ LANGUAGE plpgsql;

