# Operations Runbook

This document provides step-by-step procedures for responding to Revenue Integrity alerts and common operational tasks.

---

## Table of Contents

1. [Alert Response Procedures](#alert-response-procedures)
2. [Common Issues and Resolutions](#common-issues-and-resolutions)
3. [Manual Operations](#manual-operations)
4. [Escalation Matrix](#escalation-matrix)

---

## Alert Response Procedures

### Critical Alert: Revenue Drop

**Symptoms:**
- `gross_revenue` or `avg_order_value` significantly below baseline
- Alert mentions "REVENUE ALERT" or "REVENUE COLLAPSE"

**Immediate Actions:**

1. **Verify the alert is real**
   ```bash
   # Check current metrics
   psql -c "SELECT * FROM v_hourly_metrics ORDER BY hour_ts DESC LIMIT 5;"
   ```

2. **Check payment gateway status**
   - Review payment provider status page
   - Check `payment_success_rate` in metrics

3. **Review recent deployments**
   - Check deployment logs for changes in last 2 hours
   - Look for pricing or checkout changes

4. **Investigate root cause hints**
   - The alert includes a suggested root cause
   - Cross-reference with related metrics mentioned

**Resolution Steps:**

| Root Cause | Action |
|------------|--------|
| Payment gateway down | Contact payment provider, enable backup gateway |
| Pricing bug | Rollback deployment, verify product prices |
| Checkout broken | Check frontend errors, verify checkout flow |

---

### Critical Alert: Zero Revenue Bug

**Symptoms:**
- Orders exist but revenue = $0
- Alert mentions "zero_revenue_bug"

**Immediate Actions:**

1. **Verify orders with zero amount**
   ```sql
   SELECT * FROM orders 
   WHERE created_at >= NOW() - INTERVAL '2 hours'
     AND amount = 0;
   ```

2. **Check pricing service**
   - Verify product prices are loading
   - Check discount/coupon logic

3. **Review purchase events**
   ```sql
   SELECT * FROM events 
   WHERE event_type = 'purchase'
     AND event_time >= NOW() - INTERVAL '2 hours'
   ORDER BY event_time DESC LIMIT 20;
   ```

**Resolution:**
- Fix the underlying pricing/amount bug
- Consider whether affected orders need manual correction

---

### Critical Alert: Funnel Break (Missing Purchases)

**Symptoms:**
- `payment_success` events exist but no `purchase` events
- Alert mentions "funnel_break_purchase"

**Immediate Actions:**

1. **Verify the gap**
   ```sql
   SELECT 
       SUM(CASE WHEN event_type = 'payment_success' THEN 1 ELSE 0 END) AS payments,
       SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchases
   FROM events
   WHERE event_time >= NOW() - INTERVAL '1 hour';
   ```

2. **Check order creation service**
   - Review order service logs
   - Check for queue backlog

3. **Check event publishing**
   - Verify purchase events are being emitted
   - Check message queue health

**Resolution:**
- Fix the order creation or event publishing pipeline
- Consider replaying missed events if orders were created

---

### Warning Alert: Conversion Rate Drop

**Symptoms:**
- `checkout_completion_rate` or `session_conversion_rate` dropped
- Z-score indicates significant deviation

**Investigation Steps:**

1. **Compare to same hour last week**
   ```sql
   SELECT * FROM v_same_hour_baseline
   WHERE metric_name = 'checkout_completion_rate'
   ORDER BY current_hour DESC LIMIT 10;
   ```

2. **Check for external factors**
   - Marketing campaign changes
   - Competitor promotions
   - Site performance issues

3. **Review error rates**
   - Check application error logs
   - Review frontend JavaScript errors

**Resolution:**
- If technical: Fix the underlying issue
- If business: Document and continue monitoring

---

### Warning Alert: Traffic Drop

**Symptoms:**
- `unique_users` or `unique_sessions` significantly down
- Alert mentions "TRAFFIC DROP"

**Investigation Steps:**

1. **Check traffic sources**
   - Review analytics for traffic source breakdown
   - Check ad campaign status

2. **Verify infrastructure**
   - CDN status
   - DNS resolution
   - Load balancer health

3. **Check for external issues**
   - Google Search Console for SEO issues
   - Social media for brand issues

---

## Common Issues and Resolutions

### Issue: False Positive Alerts

**Symptoms:**
- Alerts triggered but no actual problem
- Often happens during known traffic fluctuations

**Resolution:**

1. **Suppress the incident**
   ```bash
   python src/suppression.py --suppress INCIDENT_ID "Known event - Black Friday"
   ```

2. **Adjust thresholds** (if recurring)
   - Edit `config.yaml`
   - Increase Z-score threshold for affected metric
   - Consider same-hour-of-week baselines

3. **Add to known patterns**
   - Document in this runbook
   - Consider adding exception rules

---

### Issue: Alert Fatigue (Too Many Alerts)

**Resolution:**

1. **Review suppression settings**
   ```yaml
   # config.yaml
   suppression:
     cooldown_hours: 4  # Increase if needed
   ```

2. **Adjust severity thresholds**
   - Critical: Only for immediate revenue impact
   - Warning: For investigation, not pages

3. **Enable incident grouping**
   - Groups related metrics together
   - Reduces alert volume

---

### Issue: Monitor Not Running

**Symptoms:**
- No recent runs in `monitor_runs` table
- Health check shows "unhealthy"

**Resolution:**

1. **Check GitHub Actions**
   - Review workflow run history
   - Check for failed runs

2. **Verify database connectivity**
   ```bash
   python src/health.py --json
   ```

3. **Manual run**
   - Trigger workflow_dispatch from GitHub Actions
   - Or run locally:
   ```bash
   python src/extract.py --init-schema --run
   ```

---

## Manual Operations

### Run Detection Manually

```bash
# Initialize and run
python src/extract.py --init-schema --run

# Just run detection
python src/detect.py --window-hours 1 --baseline-periods 168

# Show current incidents
python src/extract.py --show-incidents
```

### Generate Report

```bash
# Get run ID
psql -c "SELECT run_id FROM monitor_runs ORDER BY ended_at DESC LIMIT 1;"

# Generate report
python src/report.py --run-id <RUN_ID>
```

### Backtest Detectors

```bash
# Run 30-day backtest
python src/backtest.py --days 30 --output backtest_results.json
```

### Health Check

```bash
# Check system health
python src/health.py --stats

# Send alert if unhealthy
python src/health.py --alert
```

### Manage Incidents

```bash
# View suppression stats
python src/suppression.py --stats

# Auto-resolve stale incidents
python src/suppression.py --auto-resolve

# Show suppressed incidents
python src/suppression.py --show-suppressed
```

### Test Slack Integration

```bash
# Send test message
python src/slack.py --test

# Test with custom message
python src/slack.py --message "Test alert from runbook"
```

---

## Escalation Matrix

| Severity | Response Time | First Responder | Escalation |
|----------|---------------|-----------------|------------|
| Critical | 15 minutes | On-call engineer | → Engineering Lead → CTO |
| Warning | 1 hour | Data team | → Engineering |
| Info | Next business day | Data team | None |

### Contact Information

| Role | Contact Method | Hours |
|------|----------------|-------|
| On-call engineer | PagerDuty | 24/7 |
| Data team | #data-quality Slack | Business hours |
| Engineering lead | Direct message | Business hours |

---

## Post-Incident Review

After resolving a critical incident:

1. **Document the incident**
   - What was detected
   - Root cause
   - Resolution steps
   - Time to resolution

2. **Review detector performance**
   - Did we detect it quickly enough?
   - Any false negatives?
   - Threshold adjustments needed?

3. **Update this runbook**
   - Add new patterns
   - Update procedures

4. **Schedule post-mortem** (if major incident)
   - Root cause analysis
   - Preventive measures
   - Action items

---

## Appendix: SQL Quick Reference

### Recent Anomalies
```sql
SELECT metric_name, severity, current_value, baseline_mean, z_score, detected_at
FROM anomalies
ORDER BY detected_at DESC
LIMIT 20;
```

### Active Incidents
```sql
SELECT * FROM v_active_incidents;
```

### Hourly Metrics
```sql
SELECT * FROM v_hourly_metrics
ORDER BY hour_ts DESC
LIMIT 24;
```

### Baseline Values
```sql
SELECT * FROM v_current_baselines;
```

### Recent Runs
```sql
SELECT run_id, started_at, status, anomalies_detected
FROM monitor_runs
ORDER BY started_at DESC
LIMIT 10;
```

