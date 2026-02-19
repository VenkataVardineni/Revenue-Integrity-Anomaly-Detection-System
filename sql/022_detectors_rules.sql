-- Revenue Integrity Anomaly Detection System
-- Missingness & Rules-Based Detectors
-- ==============================================
--
-- These detectors catch "silent" bugs that Z-score and IQR might miss:
--   1. Missing event types entirely
--   2. Zero purchases but non-zero checkouts (pipeline break)
--   3. Sudden drop in unique users/sessions
--   4. Funnel inconsistencies
--   5. Payment gateway issues (payments initiated but none succeed)
--
-- These are rule-based checks, not statistical.
-- ==============================================

-- ==============================================
-- MISSING EVENT TYPES DETECTOR
-- ==============================================

-- Detect when expected event types are completely missing
CREATE OR REPLACE FUNCTION detect_missing_event_types(
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
        detector,
        severity,
        direction,
        description,
        metadata_json
    )
    SELECT 
        p_run_id,
        NOW(),
        expected.event_type || '_count',
        v_window_start,
        0,
        baseline.avg_count,
        'rules',
        CASE 
            WHEN expected.is_conversion_event THEN 'critical'
            ELSE 'warning'
        END,
        'missing',
        FORMAT(
            'MISSING EVENT TYPE: No %s events in window [%s to %s]. Expected ~%.0f based on baseline.',
            expected.event_type,
            v_window_start,
            v_window_end,
            baseline.avg_count
        ),
        jsonb_build_object(
            'event_type', expected.event_type,
            'is_conversion_event', expected.is_conversion_event,
            'baseline_avg_count', baseline.avg_count,
            'window_start', v_window_start,
            'window_end', v_window_end
        )
    FROM dim_event_type expected
    -- Get baseline average for comparison
    CROSS JOIN LATERAL (
        SELECT AVG(cnt)::NUMERIC AS avg_count
        FROM (
            SELECT COUNT(*) AS cnt
            FROM events e
            WHERE e.event_type = expected.event_type
              AND e.event_time >= v_window_start - INTERVAL '7 days'
              AND e.event_time < v_window_start
            GROUP BY date_trunc('hour', e.event_time)
        ) hourly_counts
    ) baseline
    -- Check if event type is missing in current window
    WHERE NOT EXISTS (
        SELECT 1 FROM events e
        WHERE e.event_type = expected.event_type
          AND e.event_time >= v_window_start
          AND e.event_time < v_window_end
    )
    -- Only alert if we expect events (baseline > 5)
    AND baseline.avg_count > 5;
    
    GET DIAGNOSTICS v_anomaly_count = ROW_COUNT;
    
    RETURN v_anomaly_count;
END;
$$ LANGUAGE plpgsql;


-- ==============================================
-- FUNNEL BREAK DETECTOR
-- ==============================================

-- Detect when downstream funnel events exist but upstream are missing
-- (e.g., checkouts but no add_to_cart, or purchases but no checkouts)
CREATE OR REPLACE FUNCTION detect_funnel_breaks(
    p_run_id UUID,
    p_window_start TIMESTAMP WITH TIME ZONE DEFAULT NULL,
    p_window_end TIMESTAMP WITH TIME ZONE DEFAULT NULL
)
RETURNS INT AS $$
DECLARE
    v_window_start TIMESTAMP WITH TIME ZONE;
    v_window_end TIMESTAMP WITH TIME ZONE;
    v_anomaly_count INT := 0;
    v_funnel RECORD;
BEGIN
    v_window_end := COALESCE(p_window_end, date_trunc('hour', NOW()));
    v_window_start := COALESCE(p_window_start, v_window_end - INTERVAL '1 hour');
    
    -- Get funnel counts for the window
    SELECT 
        COALESCE(SUM(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END), 0) AS page_views,
        COALESCE(SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END), 0) AS add_to_cart,
        COALESCE(SUM(CASE WHEN event_type = 'checkout_start' THEN 1 ELSE 0 END), 0) AS checkout_start,
        COALESCE(SUM(CASE WHEN event_type = 'checkout_complete' THEN 1 ELSE 0 END), 0) AS checkout_complete,
        COALESCE(SUM(CASE WHEN event_type = 'payment_success' THEN 1 ELSE 0 END), 0) AS payment_success,
        COALESCE(SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END), 0) AS purchases
    INTO v_funnel
    FROM events
    WHERE event_time >= v_window_start
      AND event_time < v_window_end;
    
    -- Check: Checkouts but no add_to_cart
    IF v_funnel.checkout_start > 0 AND v_funnel.add_to_cart = 0 THEN
        INSERT INTO anomalies (
            run_id, detected_at, metric_name, metric_timestamp, current_value,
            detector, severity, direction, description, metadata_json
        ) VALUES (
            p_run_id, NOW(), 'funnel_break_cart', v_window_start, 0,
            'rules', 'critical', 'missing',
            FORMAT('FUNNEL BREAK: %s checkout_start events but 0 add_to_cart events', v_funnel.checkout_start),
            jsonb_build_object('checkout_start', v_funnel.checkout_start, 'add_to_cart', 0)
        );
        v_anomaly_count := v_anomaly_count + 1;
    END IF;
    
    -- Check: Checkout complete but no checkout_start
    IF v_funnel.checkout_complete > 0 AND v_funnel.checkout_start = 0 THEN
        INSERT INTO anomalies (
            run_id, detected_at, metric_name, metric_timestamp, current_value,
            detector, severity, direction, description, metadata_json
        ) VALUES (
            p_run_id, NOW(), 'funnel_break_checkout', v_window_start, 0,
            'rules', 'critical', 'missing',
            FORMAT('FUNNEL BREAK: %s checkout_complete but 0 checkout_start events', v_funnel.checkout_complete),
            jsonb_build_object('checkout_complete', v_funnel.checkout_complete, 'checkout_start', 0)
        );
        v_anomaly_count := v_anomaly_count + 1;
    END IF;
    
    -- Check: Payment success but no purchases (pipeline break after payment)
    IF v_funnel.payment_success > 10 AND v_funnel.purchases = 0 THEN
        INSERT INTO anomalies (
            run_id, detected_at, metric_name, metric_timestamp, current_value,
            detector, severity, direction, description, metadata_json
        ) VALUES (
            p_run_id, NOW(), 'funnel_break_purchase', v_window_start, 0,
            'rules', 'critical', 'missing',
            FORMAT('CRITICAL: %s payment_success events but 0 purchase events - ORDER PIPELINE BROKEN!', v_funnel.payment_success),
            jsonb_build_object('payment_success', v_funnel.payment_success, 'purchases', 0)
        );
        v_anomaly_count := v_anomaly_count + 1;
    END IF;
    
    -- Check: Checkout complete but very few purchases (>50% drop)
    IF v_funnel.checkout_complete > 20 AND 
       v_funnel.purchases < v_funnel.checkout_complete * 0.5 THEN
        INSERT INTO anomalies (
            run_id, detected_at, metric_name, metric_timestamp, current_value,
            detector, severity, direction, description, metadata_json
        ) VALUES (
            p_run_id, NOW(), 'funnel_break_conversion', v_window_start, 
            (v_funnel.purchases::NUMERIC / v_funnel.checkout_complete * 100)::NUMERIC,
            'rules', 'warning', 'below',
            FORMAT('LOW CONVERSION: Only %s purchases from %s checkout_complete (%.1f%% vs expected ~95%%)',
                v_funnel.purchases, v_funnel.checkout_complete,
                v_funnel.purchases::NUMERIC / v_funnel.checkout_complete * 100),
            jsonb_build_object(
                'checkout_complete', v_funnel.checkout_complete, 
                'purchases', v_funnel.purchases,
                'conversion_rate', v_funnel.purchases::NUMERIC / v_funnel.checkout_complete * 100
            )
        );
        v_anomaly_count := v_anomaly_count + 1;
    END IF;
    
    RETURN v_anomaly_count;
END;
$$ LANGUAGE plpgsql;


-- ==============================================
-- PAYMENT GATEWAY FAILURE DETECTOR
-- ==============================================

-- Detect when payment gateway might be failing
CREATE OR REPLACE FUNCTION detect_payment_gateway_issues(
    p_run_id UUID,
    p_window_start TIMESTAMP WITH TIME ZONE DEFAULT NULL,
    p_window_end TIMESTAMP WITH TIME ZONE DEFAULT NULL
)
RETURNS INT AS $$
DECLARE
    v_window_start TIMESTAMP WITH TIME ZONE;
    v_window_end TIMESTAMP WITH TIME ZONE;
    v_anomaly_count INT := 0;
    v_payments RECORD;
    v_baseline RECORD;
BEGIN
    v_window_end := COALESCE(p_window_end, date_trunc('hour', NOW()));
    v_window_start := COALESCE(p_window_start, v_window_end - INTERVAL '1 hour');
    
    -- Get current window payment stats
    SELECT 
        COALESCE(SUM(CASE WHEN event_type = 'payment_initiated' THEN 1 ELSE 0 END), 0) AS initiated,
        COALESCE(SUM(CASE WHEN event_type = 'payment_success' THEN 1 ELSE 0 END), 0) AS success,
        COALESCE(SUM(CASE WHEN event_type = 'payment_failed' THEN 1 ELSE 0 END), 0) AS failed
    INTO v_payments
    FROM events
    WHERE event_time >= v_window_start
      AND event_time < v_window_end;
    
    -- Get baseline success rate
    SELECT 
        AVG(success_rate) AS avg_success_rate
    INTO v_baseline
    FROM (
        SELECT 
            date_trunc('hour', event_time) AS hour,
            SUM(CASE WHEN event_type = 'payment_success' THEN 1 ELSE 0 END)::NUMERIC /
            NULLIF(SUM(CASE WHEN event_type = 'payment_initiated' THEN 1 ELSE 0 END), 0) * 100 AS success_rate
        FROM events
        WHERE event_time >= v_window_start - INTERVAL '7 days'
          AND event_time < v_window_start
          AND event_type IN ('payment_initiated', 'payment_success')
        GROUP BY date_trunc('hour', event_time)
        HAVING SUM(CASE WHEN event_type = 'payment_initiated' THEN 1 ELSE 0 END) > 5
    ) hourly_rates;
    
    -- Check: Zero success rate when we have initiations
    IF v_payments.initiated > 10 AND v_payments.success = 0 THEN
        INSERT INTO anomalies (
            run_id, detected_at, metric_name, metric_timestamp, current_value,
            baseline_mean, detector, severity, direction, description, metadata_json
        ) VALUES (
            p_run_id, NOW(), 'payment_gateway_failure', v_window_start, 0,
            v_baseline.avg_success_rate, 'rules', 'critical', 'below',
            FORMAT('PAYMENT GATEWAY DOWN: %s payments initiated, 0 succeeded, %s failed!',
                v_payments.initiated, v_payments.failed),
            jsonb_build_object(
                'initiated', v_payments.initiated,
                'success', v_payments.success,
                'failed', v_payments.failed,
                'baseline_success_rate', v_baseline.avg_success_rate
            )
        );
        v_anomaly_count := v_anomaly_count + 1;
    
    -- Check: Success rate significantly below baseline
    ELSIF v_payments.initiated > 10 AND v_baseline.avg_success_rate IS NOT NULL THEN
        DECLARE
            v_current_rate NUMERIC;
        BEGIN
            v_current_rate := v_payments.success::NUMERIC / v_payments.initiated * 100;
            
            IF v_current_rate < v_baseline.avg_success_rate * 0.7 THEN
                INSERT INTO anomalies (
                    run_id, detected_at, metric_name, metric_timestamp, current_value,
                    baseline_mean, detector, severity, direction, description, metadata_json
                ) VALUES (
                    p_run_id, NOW(), 'payment_success_rate_drop', v_window_start, v_current_rate,
                    v_baseline.avg_success_rate, 'rules', 
                    CASE WHEN v_current_rate < v_baseline.avg_success_rate * 0.5 THEN 'critical' ELSE 'warning' END,
                    'below',
                    FORMAT('Payment success rate dropped to %.1f%% (baseline: %.1f%%)',
                        v_current_rate, v_baseline.avg_success_rate),
                    jsonb_build_object(
                        'initiated', v_payments.initiated,
                        'success', v_payments.success,
                        'failed', v_payments.failed,
                        'current_rate', v_current_rate,
                        'baseline_rate', v_baseline.avg_success_rate
                    )
                );
                v_anomaly_count := v_anomaly_count + 1;
            END IF;
        END;
    END IF;
    
    RETURN v_anomaly_count;
END;
$$ LANGUAGE plpgsql;


-- ==============================================
-- TRAFFIC DROP DETECTOR
-- ==============================================

-- Detect sudden drops in users/sessions
CREATE OR REPLACE FUNCTION detect_traffic_drops(
    p_run_id UUID,
    p_window_start TIMESTAMP WITH TIME ZONE DEFAULT NULL,
    p_window_end TIMESTAMP WITH TIME ZONE DEFAULT NULL,
    p_drop_threshold NUMERIC DEFAULT 0.5  -- 50% drop threshold
)
RETURNS INT AS $$
DECLARE
    v_window_start TIMESTAMP WITH TIME ZONE;
    v_window_end TIMESTAMP WITH TIME ZONE;
    v_anomaly_count INT := 0;
    v_current RECORD;
    v_baseline RECORD;
BEGIN
    v_window_end := COALESCE(p_window_end, date_trunc('hour', NOW()));
    v_window_start := COALESCE(p_window_start, v_window_end - INTERVAL '1 hour');
    
    -- Get current window traffic
    SELECT 
        COUNT(DISTINCT user_id) AS unique_users,
        COUNT(DISTINCT session_id) AS unique_sessions
    INTO v_current
    FROM events
    WHERE event_time >= v_window_start
      AND event_time < v_window_end;
    
    -- Get baseline (same hour over past 4 weeks, accounting for day-of-week patterns)
    SELECT 
        AVG(unique_users) AS avg_users,
        AVG(unique_sessions) AS avg_sessions,
        STDDEV(unique_users) AS std_users,
        STDDEV(unique_sessions) AS std_sessions
    INTO v_baseline
    FROM (
        SELECT 
            date_trunc('hour', event_time) AS hour,
            COUNT(DISTINCT user_id) AS unique_users,
            COUNT(DISTINCT session_id) AS unique_sessions
        FROM events
        WHERE event_time >= v_window_start - INTERVAL '28 days'
          AND event_time < v_window_start
          AND EXTRACT(DOW FROM event_time) = EXTRACT(DOW FROM v_window_start)
          AND EXTRACT(HOUR FROM event_time) = EXTRACT(HOUR FROM v_window_start)
        GROUP BY date_trunc('hour', event_time)
    ) same_hour_traffic;
    
    -- Check for significant drops
    IF v_baseline.avg_users IS NOT NULL AND v_baseline.avg_users > 10 THEN
        -- User drop
        IF v_current.unique_users < v_baseline.avg_users * (1 - p_drop_threshold) THEN
            INSERT INTO anomalies (
                run_id, detected_at, metric_name, metric_timestamp, current_value,
                baseline_mean, baseline_std, pct_change, detector, severity, direction,
                description, metadata_json
            ) VALUES (
                p_run_id, NOW(), 'unique_users_drop', v_window_start, v_current.unique_users,
                v_baseline.avg_users, v_baseline.std_users,
                ((v_current.unique_users - v_baseline.avg_users) / v_baseline.avg_users * 100),
                'rules',
                CASE WHEN v_current.unique_users < v_baseline.avg_users * 0.3 THEN 'critical' ELSE 'warning' END,
                'below',
                FORMAT('TRAFFIC DROP: Only %s unique users (expected ~%.0f, %.1f%% drop)',
                    v_current.unique_users, v_baseline.avg_users,
                    (1 - v_current.unique_users::NUMERIC / v_baseline.avg_users) * 100),
                jsonb_build_object(
                    'current_users', v_current.unique_users,
                    'baseline_avg', v_baseline.avg_users,
                    'drop_pct', (1 - v_current.unique_users::NUMERIC / v_baseline.avg_users) * 100
                )
            );
            v_anomaly_count := v_anomaly_count + 1;
        END IF;
        
        -- Session drop
        IF v_current.unique_sessions < v_baseline.avg_sessions * (1 - p_drop_threshold) THEN
            INSERT INTO anomalies (
                run_id, detected_at, metric_name, metric_timestamp, current_value,
                baseline_mean, baseline_std, pct_change, detector, severity, direction,
                description, metadata_json
            ) VALUES (
                p_run_id, NOW(), 'unique_sessions_drop', v_window_start, v_current.unique_sessions,
                v_baseline.avg_sessions, v_baseline.std_sessions,
                ((v_current.unique_sessions - v_baseline.avg_sessions) / v_baseline.avg_sessions * 100),
                'rules',
                CASE WHEN v_current.unique_sessions < v_baseline.avg_sessions * 0.3 THEN 'critical' ELSE 'warning' END,
                'below',
                FORMAT('SESSION DROP: Only %s sessions (expected ~%.0f, %.1f%% drop)',
                    v_current.unique_sessions, v_baseline.avg_sessions,
                    (1 - v_current.unique_sessions::NUMERIC / v_baseline.avg_sessions) * 100),
                jsonb_build_object(
                    'current_sessions', v_current.unique_sessions,
                    'baseline_avg', v_baseline.avg_sessions,
                    'drop_pct', (1 - v_current.unique_sessions::NUMERIC / v_baseline.avg_sessions) * 100
                )
            );
            v_anomaly_count := v_anomaly_count + 1;
        END IF;
    END IF;
    
    RETURN v_anomaly_count;
END;
$$ LANGUAGE plpgsql;


-- ==============================================
-- ZERO REVENUE DETECTOR
-- ==============================================

-- Detect when revenue drops to zero or near-zero
CREATE OR REPLACE FUNCTION detect_zero_revenue(
    p_run_id UUID,
    p_window_start TIMESTAMP WITH TIME ZONE DEFAULT NULL,
    p_window_end TIMESTAMP WITH TIME ZONE DEFAULT NULL
)
RETURNS INT AS $$
DECLARE
    v_window_start TIMESTAMP WITH TIME ZONE;
    v_window_end TIMESTAMP WITH TIME ZONE;
    v_anomaly_count INT := 0;
    v_current RECORD;
    v_baseline RECORD;
BEGIN
    v_window_end := COALESCE(p_window_end, date_trunc('hour', NOW()));
    v_window_start := COALESCE(p_window_start, v_window_end - INTERVAL '1 hour');
    
    -- Get current revenue
    SELECT 
        COALESCE(SUM(amount), 0) AS total_revenue,
        COUNT(*) AS purchase_count
    INTO v_current
    FROM events
    WHERE event_time >= v_window_start
      AND event_time < v_window_end
      AND event_type = 'purchase';
    
    -- Get baseline
    SELECT AVG(hourly_revenue) AS avg_revenue
    INTO v_baseline
    FROM (
        SELECT SUM(amount) AS hourly_revenue
        FROM events
        WHERE event_time >= v_window_start - INTERVAL '7 days'
          AND event_time < v_window_start
          AND event_type = 'purchase'
        GROUP BY date_trunc('hour', event_time)
    ) hourly;
    
    -- Check: Zero revenue with purchases (amount = 0 bug)
    IF v_current.purchase_count > 0 AND v_current.total_revenue = 0 THEN
        INSERT INTO anomalies (
            run_id, detected_at, metric_name, metric_timestamp, current_value,
            baseline_mean, detector, severity, direction, description, metadata_json
        ) VALUES (
            p_run_id, NOW(), 'zero_revenue_bug', v_window_start, 0,
            v_baseline.avg_revenue, 'rules', 'critical', 'below',
            FORMAT('ZERO REVENUE BUG: %s purchases recorded but total revenue = $0!', v_current.purchase_count),
            jsonb_build_object(
                'purchase_count', v_current.purchase_count,
                'total_revenue', v_current.total_revenue,
                'baseline_avg_revenue', v_baseline.avg_revenue
            )
        );
        v_anomaly_count := v_anomaly_count + 1;
    
    -- Check: Revenue dropped to near-zero when we expect significant revenue
    ELSIF v_baseline.avg_revenue > 100 AND v_current.total_revenue < v_baseline.avg_revenue * 0.1 THEN
        INSERT INTO anomalies (
            run_id, detected_at, metric_name, metric_timestamp, current_value,
            baseline_mean, detector, severity, direction, description, metadata_json
        ) VALUES (
            p_run_id, NOW(), 'near_zero_revenue', v_window_start, v_current.total_revenue,
            v_baseline.avg_revenue, 'rules', 'critical', 'below',
            FORMAT('REVENUE COLLAPSE: $%.2f revenue vs expected $%.2f (%.1f%% drop)',
                v_current.total_revenue, v_baseline.avg_revenue,
                (1 - v_current.total_revenue / v_baseline.avg_revenue) * 100),
            jsonb_build_object(
                'current_revenue', v_current.total_revenue,
                'baseline_avg_revenue', v_baseline.avg_revenue,
                'drop_pct', (1 - v_current.total_revenue / v_baseline.avg_revenue) * 100
            )
        );
        v_anomaly_count := v_anomaly_count + 1;
    END IF;
    
    RETURN v_anomaly_count;
END;
$$ LANGUAGE plpgsql;


-- ==============================================
-- COMBINED RULES DETECTOR
-- ==============================================

-- Run all rule-based detectors
CREATE OR REPLACE FUNCTION run_all_rules_detectors(
    p_run_id UUID,
    p_window_start TIMESTAMP WITH TIME ZONE DEFAULT NULL,
    p_window_end TIMESTAMP WITH TIME ZONE DEFAULT NULL
)
RETURNS INT AS $$
DECLARE
    v_total_anomalies INT := 0;
    v_count INT;
BEGIN
    -- Missing event types
    SELECT detect_missing_event_types(p_run_id, p_window_start, p_window_end) INTO v_count;
    v_total_anomalies := v_total_anomalies + v_count;
    
    -- Funnel breaks
    SELECT detect_funnel_breaks(p_run_id, p_window_start, p_window_end) INTO v_count;
    v_total_anomalies := v_total_anomalies + v_count;
    
    -- Payment gateway issues
    SELECT detect_payment_gateway_issues(p_run_id, p_window_start, p_window_end) INTO v_count;
    v_total_anomalies := v_total_anomalies + v_count;
    
    -- Traffic drops
    SELECT detect_traffic_drops(p_run_id, p_window_start, p_window_end) INTO v_count;
    v_total_anomalies := v_total_anomalies + v_count;
    
    -- Zero revenue
    SELECT detect_zero_revenue(p_run_id, p_window_start, p_window_end) INTO v_count;
    v_total_anomalies := v_total_anomalies + v_count;
    
    RETURN v_total_anomalies;
END;
$$ LANGUAGE plpgsql;

