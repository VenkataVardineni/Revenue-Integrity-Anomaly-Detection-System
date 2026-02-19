# Detector Reference - Mathematical Foundations

This document explains the statistical methods used for anomaly detection in the Revenue Integrity system.

## Overview

The system employs three complementary detection methods:

| Method | Type | Best For | Assumption |
|--------|------|----------|------------|
| Z-score | Statistical | Normally distributed metrics | Gaussian distribution |
| IQR | Statistical | Skewed metrics (revenue, AOV) | None (non-parametric) |
| Rules | Deterministic | Pipeline breaks, missing events | Domain knowledge |

---

## Z-Score Detection

### Theory

The Z-score measures how many standard deviations a value is from the mean:

$$z = \frac{x - \mu}{\sigma}$$

Where:
- $x$ = current observed value
- $\mu$ = baseline mean
- $\sigma$ = baseline standard deviation

### Interpretation

| Z-Score | Percentile | Interpretation |
|---------|------------|----------------|
| 0 | 50% | At the mean |
| ±1 | 16% / 84% | Normal variation |
| ±2 | 2.3% / 97.7% | Unusual |
| ±3 | 0.13% / 99.87% | Extremely rare |

### Thresholds

| Severity | Threshold | Probability |
|----------|-----------|-------------|
| **Critical** | \|z\| ≥ 3.0 | < 0.3% chance if normal |
| **Warning** | \|z\| ≥ 2.0 | < 5% chance if normal |
| **Info** | \|z\| ≥ 1.5 | < 13% chance if normal |

### When to Use

✅ Good for:
- Metrics with symmetric distribution
- Stable baseline (low variance)
- Session counts, conversion rates

⚠️ Caution for:
- Heavily skewed distributions (use IQR instead)
- Metrics with many zeros
- Small sample sizes

### Implementation

```sql
z_score = (current_value - rolling_mean) / rolling_std

-- Classification
CASE 
    WHEN ABS(z_score) >= 3.0 THEN 'critical'
    WHEN ABS(z_score) >= 2.0 THEN 'warning'
    ELSE 'info'
END
```

---

## IQR Detection (Interquartile Range)

### Theory

The IQR method identifies outliers based on quartiles:

$$IQR = Q_3 - Q_1$$

Where:
- $Q_1$ = 25th percentile (first quartile)
- $Q_3$ = 75th percentile (third quartile)

A value is an **outlier** if:

$$x < Q_1 - k \cdot IQR \quad \text{or} \quad x > Q_3 + k \cdot IQR$$

Where $k$ is typically 1.5 (moderate) or 3.0 (severe).

### Visual Representation

```
      Lower         Q1         Median        Q3        Upper
      Fence                      │                      Fence
        │                        │                        │
        ├────────────────────────┼────────────────────────┤
        │      │      IQR       │        IQR       │     │
        │      └────────────────┴────────────────────┘    │
        │                                                  │
   Q1 - 1.5×IQR                                    Q3 + 1.5×IQR
        
   Points outside fences = OUTLIERS
```

### Thresholds

| Severity | Multiplier | Description |
|----------|------------|-------------|
| **Critical** | k = 3.0 | Severe outlier |
| **Warning** | k = 1.5 | Moderate outlier |

### When to Use

✅ Good for:
- Skewed distributions (revenue, order values)
- Metrics with occasional large values
- When you don't want to assume normality

⚠️ Caution for:
- Very small sample sizes
- Bimodal distributions

### Implementation

```sql
iqr_lower_bound = percentile_25 - (1.5 * iqr)
iqr_upper_bound = percentile_75 + (1.5 * iqr)

-- Classification
CASE 
    WHEN value < Q1 - (3.0 * IQR) THEN 'critical'
    WHEN value > Q3 + (3.0 * IQR) THEN 'critical'
    WHEN value < iqr_lower_bound THEN 'warning'
    WHEN value > iqr_upper_bound THEN 'warning'
    ELSE 'normal'
END
```

### Revenue-Specific Tuning

For revenue metrics, we use asymmetric thresholds:
- **Drops**: More sensitive (k = 1.0 for lower bound)
- **Spikes**: Standard sensitivity (k = 1.5 for upper bound)

This catches revenue drops faster than spikes.

---

## Rules-Based Detection

### Theory

Rules-based detectors use domain knowledge to catch anomalies that statistical methods might miss, particularly "silent" failures where data looks statistically normal but is logically inconsistent.

### Detector Types

#### 1. Missing Event Types

**Logic**: Alert when an expected event type has zero occurrences but had consistent historical presence.

```sql
-- Missing if:
COUNT(event_type) = 0 
AND baseline_avg > 5  -- Expect events normally
```

#### 2. Funnel Breaks

**Logic**: Downstream events exist but upstream events are missing.

| Pattern | Severity | Description |
|---------|----------|-------------|
| checkout_start > 0, add_to_cart = 0 | Critical | Cart tracking broken |
| checkout_complete > 0, checkout_start = 0 | Critical | Checkout tracking broken |
| payment_success > 10, purchases = 0 | Critical | Order pipeline broken |

#### 3. Payment Gateway Failure

**Logic**: Payments initiated but none succeed.

```sql
-- Gateway down if:
payment_initiated > 10 AND payment_success = 0
```

#### 4. Traffic Drops

**Logic**: Significant drops in unique users or sessions.

```sql
-- Alert if:
current_sessions < baseline_avg * 0.5  -- 50%+ drop
```

#### 5. Zero Revenue

**Logic**: Purchases recorded but total revenue is zero.

```sql
-- Bug detected if:
purchase_count > 0 AND total_revenue = 0
```

---

## Baseline Computation

### Rolling Window

Baselines are computed over a rolling 168-hour (7-day) window:

```sql
SELECT 
    AVG(metric_value) AS rolling_mean,
    STDDEV(metric_value) AS rolling_std,
    PERCENTILE_CONT(0.25) AS percentile_25,
    PERCENTILE_CONT(0.75) AS percentile_75
FROM v_metrics_long
WHERE hour_ts >= current_hour - INTERVAL '168 hours'
  AND hour_ts < current_hour
```

### Minimum Sample Size

Baselines require at least 24 data points to be valid:

```sql
WHERE baseline_sample_count >= 24
```

This prevents false alerts during cold-start or after data gaps.

### Same-Hour-of-Week Baseline

For metrics with weekly seasonality, we also compute baselines from the same hour on previous weeks:

```sql
-- Compare Tuesday 3pm to previous 4 Tuesday 3pms
WHERE EXTRACT(DOW FROM event_time) = EXTRACT(DOW FROM current_time)
  AND EXTRACT(HOUR FROM event_time) = EXTRACT(HOUR FROM current_time)
  AND event_time >= current_time - INTERVAL '28 days'
```

---

## Threshold Tuning

### General Guidelines

| Scenario | Z-score | IQR Multiplier |
|----------|---------|----------------|
| High sensitivity (revenue) | ±2.0 / ±2.5 | 1.0 / 1.5 |
| Standard sensitivity | ±2.0 / ±3.0 | 1.5 / 3.0 |
| Low sensitivity (engagement) | ±2.5 / ±3.5 | 2.0 / 3.0 |

### Backtesting

Use `src/backtest.py` to evaluate threshold choices:

```bash
python src/backtest.py --days 30 --output results.json
```

Metrics to optimize:
- **Precision**: % of alerts that were real issues
- **Recall**: % of real issues that triggered alerts
- **F1 Score**: Harmonic mean of precision and recall

---

## Detector Selection Guide

| Metric Category | Primary Detector | Secondary |
|-----------------|------------------|-----------|
| Revenue (gross, AOV) | IQR | Z-score |
| Conversion rates | Z-score | IQR |
| Session/user counts | Z-score | Rules |
| Payment metrics | Rules | Z-score |
| Funnel counts | Rules | Z-score |

---

## References

1. Grubbs, F. E. (1969). "Procedures for Detecting Outlying Observations in Samples"
2. Tukey, J. W. (1977). "Exploratory Data Analysis"
3. Hochenbaum, J., Vallis, O. S., & Kejariwal, A. (2017). "Automatic Anomaly Detection in the Cloud Via Statistical Learning"

