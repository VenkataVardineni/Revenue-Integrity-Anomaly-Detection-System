-- Data retention: prune old monitoring rows (configurable ages in days)
-- ================================================================

CREATE OR REPLACE FUNCTION apply_data_retention(
    p_anomalies_days INT DEFAULT 30,
    p_baselines_days INT DEFAULT 14,
    p_runs_days INT DEFAULT 90
) RETURNS JSONB AS $$
DECLARE
    n_anom INT := 0;
    n_base INT := 0;
    n_runs INT := 0;
    n_batch INT;
BEGIN
    DELETE FROM anomalies a
    USING monitor_runs m
    WHERE a.run_id = m.run_id
      AND m.started_at < NOW() - (p_runs_days || ' days')::INTERVAL;
    GET DIAGNOSTICS n_batch = ROW_COUNT;
    n_anom := n_anom + n_batch;

    DELETE FROM monitor_runs
    WHERE started_at < NOW() - (p_runs_days || ' days')::INTERVAL;
    GET DIAGNOSTICS n_runs = ROW_COUNT;

    DELETE FROM anomalies
    WHERE detected_at < NOW() - (p_anomalies_days || ' days')::INTERVAL;
    GET DIAGNOSTICS n_batch = ROW_COUNT;
    n_anom := n_anom + n_batch;

    DELETE FROM metric_baselines
    WHERE period_end < NOW() - (p_baselines_days || ' days')::INTERVAL;
    GET DIAGNOSTICS n_base = ROW_COUNT;

    RETURN jsonb_build_object(
        'deleted_anomalies', n_anom,
        'deleted_baselines', n_base,
        'deleted_runs', n_runs
    );
END;
$$ LANGUAGE plpgsql;
