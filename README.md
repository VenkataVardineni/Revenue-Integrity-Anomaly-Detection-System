# Revenue Integrity Anomaly Detection System

A production-ready monitoring suite that detects "silent" revenue leaks, conversion drops, and data pipeline anomalies using statistical methods (Z-score + IQR).

## Features

- **SQL-first detectors**: Z-score and IQR-based anomaly detection directly in SQL
- **Automated monitoring**: GitHub Actions cron-based scheduled runs
- **Slack alerts**: Real-time notifications with severity routing
- **Missingness detection**: Catches silent pipeline breaks and missing event types
- **Incident rollup**: Deduplication and root-cause hints
- **Configurable logging**: Set `LOG_LEVEL` for all Python entrypoints
- **Schema validation**: `python src/validate_schema.py` checks required tables, views, and functions
- **Data retention**: `apply_data_retention` (SQL) plus `python src/extract.py --retention-cleanup`
- **Metrics export**: Optional Prometheus textfile via `METRICS_TEXTFILE_PATH` after each `extract --run`
- **JSON outputs**: `extract --run --json` and `report --export-json` for automation

## Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  Transactional  │────▶│   SQL Metrics    │────▶│   Detectors     │
│     Logs        │     │     Views        │     │ (Z-score, IQR)  │
└─────────────────┘     └──────────────────┘     └────────┬────────┘
                                                          │
                                                          ▼
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  Slack Alerts   │◀────│  Anomaly Rollup  │◀────│   Anomalies     │
│                 │     │  + Root Cause    │     │     Table       │
└─────────────────┘     └──────────────────┘     └─────────────────┘
```

## Local Development

### Prerequisites

- Docker & Docker Compose
- Python 3.9+
- PostgreSQL client (optional, for direct queries)

### Quick Start

1. **Start Postgres**
   ```bash
   cd infra
   docker-compose up -d
   ```

2. **Verify database is running**
   ```bash
   docker-compose ps
   docker-compose logs postgres
   ```

3. **Connect to database**
   ```bash
   # Using psql
   psql -h localhost -U analyst -d revenue_integrity
   # Password: analyst_secure_pw
   
   # Or via Docker
   docker exec -it revenue_integrity_db psql -U analyst -d revenue_integrity
   ```

4. **Install Python dependencies**
   ```bash
   pip install -r requirements.txt
   ```

5. **Run the seed generator** (creates test data with injected anomalies)
   ```bash
   python src/seed.py
   ```

6. **Execute the monitoring pipeline**
   ```bash
   python src/extract.py --run
   ```

7. **Optional checks**
   ```bash
   cp .env.example .env   # then edit DATABASE_URL / webhooks
   python src/validate_schema.py
   python src/extract.py --retention-cleanup
   python -m pytest tests/ -q
   ```

### Stopping the Database

```bash
cd infra
docker-compose down

# To remove all data:
docker-compose down -v
```

## Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `DATABASE_URL` | PostgreSQL connection string | Yes |
| `SLACK_WEBHOOK_URL` | Slack incoming webhook URL | Yes (for alerts) |
| `ALERT_COOLDOWN_HOURS` | Hours between repeat alerts | No (default: 4) |
| `LOG_LEVEL` | Python log level (`INFO`, `DEBUG`, …) | No (default: INFO) |
| `METRICS_TEXTFILE_PATH` | Path to write Prometheus textfile after `extract --run` | No |

## Project Structure

```
├── sql/
│   ├── 001_schema.sql          # Core tables
│   ├── 010_metrics_views.sql   # Metric layer
│   ├── 020_detectors_zscore.sql
│   ├── 021_detectors_iqr.sql
│   ├── 030_anomaly_rollup.sql
│   └── 040_retention.sql       # Retention helper function
├── src/
│   ├── extract.py              # SQL runner + pipeline
│   ├── detect.py               # Detector execution
│   ├── report.py               # Report generation
│   ├── validate_schema.py      # DB object checks
│   ├── metrics_export.py       # Optional Prometheus textfile
│   ├── slack.py                # Webhook sender
│   └── seed.py                 # Test data generator
├── tests/                      # Pytest (version, logutil)
├── infra/
│   └── docker-compose.yml
├── .github/workflows/
│   └── monitor.yml             # Scheduled workflow
└── docs/
    ├── architecture.md
    ├── detectors.md
    └── runbook.md
```

## License

MIT

