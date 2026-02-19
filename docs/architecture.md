# Revenue Integrity Anomaly Detection System - Architecture

## Overview

This system provides real-time monitoring of revenue and conversion metrics to detect "silent" failures in e-commerce pipelines. It uses statistical methods (Z-score and IQR) combined with rule-based detectors to identify anomalies and alert operations teams via Slack.

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        GitHub Actions (Cron)                            │
│                     Every 15 minutes trigger                            │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         Python Orchestrator                              │
│                                                                          │
│  ┌─────────────┐  ┌──────────────┐  ┌──────────────┐  ┌─────────────┐  │
│  │ extract.py  │──│  detect.py   │──│  report.py   │──│  slack.py   │  │
│  │ (SQL runner)│  │ (detectors)  │  │ (artifacts)  │  │ (alerts)    │  │
│  └─────────────┘  └──────────────┘  └──────────────┘  └─────────────┘  │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                            PostgreSQL                                    │
│                                                                          │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                         SQL Layer                                │   │
│  │                                                                   │   │
│  │  ┌─────────────┐  ┌──────────────┐  ┌──────────────────────┐   │   │
│  │  │ Schema      │──│ Metrics      │──│ Detectors            │   │   │
│  │  │ (tables)    │  │ (views)      │  │ (functions)          │   │   │
│  │  └─────────────┘  └──────────────┘  └──────────────────────┘   │   │
│  │                                             │                     │   │
│  │                                             ▼                     │   │
│  │                                    ┌──────────────────────┐      │   │
│  │                                    │ Anomaly Rollup       │      │   │
│  │                                    │ (incidents)          │      │   │
│  │                                    └──────────────────────┘      │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         Slack Channels                                   │
│                                                                          │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐         │
│  │ #revenue-alerts │  │ #data-quality   │  │ #monitoring     │         │
│  │ (critical)      │  │ (warning)       │  │ (health)        │         │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘         │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

## Data Flow

### 1. Event Ingestion

Events flow into the `events` table with the following key fields:
- `event_time`: When the event occurred
- `event_type`: Type of event (page_view, add_to_cart, purchase, etc.)
- `user_id` / `session_id`: User identification
- `amount`: Transaction amount (for purchase events)
- `order_id`: Order identifier

### 2. Metric Computation

SQL views aggregate events into hourly metrics:

```
events table → v_hourly_event_counts → v_hourly_funnel
                                             │
                                             ▼
                                    v_hourly_conversion_rates
                                             │
                                             ▼
                                      v_hourly_metrics
                                             │
                                             ▼
                                      v_metrics_long (unpivoted)
```

### 3. Baseline Computation

Rolling statistics are computed over a 7-day (168-hour) baseline window:
- Mean and standard deviation (for Z-score)
- Q1, Q3, and IQR (for IQR method)
- Same-hour-of-week baselines (for seasonal patterns)

### 4. Anomaly Detection

Three detector types run in parallel:

| Detector | Method | Best For |
|----------|--------|----------|
| Z-score | (value - mean) / std | Normal distributions |
| IQR | Outliers beyond Q1-1.5×IQR or Q3+1.5×IQR | Skewed distributions |
| Rules | Conditional logic | Pipeline breaks, missing events |

### 5. Incident Rollup

Anomalies are deduplicated and rolled up into incidents:
- Same metric/hour combinations are merged
- Severity is escalated (most severe wins)
- Root cause hints are attached based on patterns
- Related metrics are linked

### 6. Alerting

Alerts are routed based on severity:
- **Critical**: `#revenue-alerts` channel with `@here` mention
- **Warning**: `#data-quality` channel
- **Info**: Logged but not alerted

## Component Details

### SQL Layer (sql/)

| File | Purpose |
|------|---------|
| `001_schema.sql` | Core tables: events, orders, anomalies, incidents |
| `010_metrics_views.sql` | Metric views and definitions |
| `011_baseline_logic.sql` | Rolling statistics and baselines |
| `020_detectors_zscore.sql` | Z-score anomaly detection |
| `021_detectors_iqr.sql` | IQR anomaly detection |
| `022_detectors_rules.sql` | Rule-based detectors |
| `030_anomaly_rollup.sql` | Incident creation and root cause hints |

### Python Layer (src/)

| File | Purpose |
|------|---------|
| `extract.py` | SQL execution and pipeline orchestration |
| `detect.py` | Detector wrapper and run management |
| `report.py` | Markdown/CSV report generation |
| `slack.py` | Slack webhook integration |
| `routing.py` | Severity-based channel routing |
| `suppression.py` | Alert cooldown and grouping |
| `backtest.py` | Historical evaluation of detectors |
| `health.py` | Monitor self-health checks |
| `seed.py` | Test data generation |

### GitHub Actions (.github/workflows/)

| File | Purpose |
|------|---------|
| `monitor.yml` | Scheduled monitoring workflow |

## Database Schema

### Core Tables

```sql
events          -- Raw event stream
orders          -- Order records
monitor_runs    -- Pipeline run tracking
metric_baselines -- Computed baselines
anomalies       -- Detected anomalies
anomaly_incidents -- Rolled-up incidents
```

### Key Relationships

```
events ──┬── v_hourly_funnel ── v_metrics_long ── metric_baselines
         │                                              │
         │                                              ▼
         └── orders                              v_metrics_with_baseline
                                                        │
                                                        ▼
                                                   anomalies
                                                        │
                                                        ▼
                                               anomaly_incidents
```

## Configuration

Configuration is managed via `config.yaml`:

- **detection**: Thresholds and window sizes
- **slack**: Channel routing and rate limiting
- **suppression**: Cooldown and grouping settings
- **observability**: Health check parameters

## Scalability Considerations

1. **Event Volume**: Indexes on `event_time`, `event_type` support efficient aggregation
2. **Baseline Window**: 7-day rolling window balances accuracy vs. computation
3. **Run Frequency**: 15-minute cron provides near-real-time detection
4. **Retention**: 30-day anomaly retention, 90-day run history

## Security

- **Secrets**: Webhook URLs stored in GitHub Secrets, never committed
- **Database**: Credentials via environment variables
- **Access**: GitHub Actions-only execution (no external triggers)

