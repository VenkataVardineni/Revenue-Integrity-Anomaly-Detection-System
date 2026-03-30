#!/usr/bin/env python3
"""Verify required database objects exist (tables, views, functions)."""

import os
import sys
from typing import List, Tuple

import psycopg2
from dotenv import load_dotenv

from logutil import setup_logging
from _version import __version__

load_dotenv()
setup_logging()

import logging

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://analyst:analyst_secure_pw@localhost:5432/revenue_integrity",
)

REQUIRED_TABLES = (
    "events",
    "orders",
    "monitor_runs",
    "anomalies",
    "anomaly_incidents",
    "metric_baselines",
)

REQUIRED_VIEWS = (
    "v_active_incidents",
    "v_incident_summary",
    "v_metrics_with_baseline",
)

REQUIRED_FUNCTIONS = (
    "run_full_detection_pipeline",
    "compute_all_baselines",
    "create_anomaly_incidents",
)


def _check(cur, schema: str, names: Tuple[str, ...]) -> List[str]:
    missing: List[str] = []
    for name in names:
        cur.execute(
            """
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
            """,
            (schema, name),
        )
        if cur.fetchone() is None:
            missing.append(name)
    return missing


def _check_routine(cur, names: Tuple[str, ...]) -> List[str]:
    missing: List[str] = []
    for name in names:
        cur.execute(
            """
            SELECT 1 FROM information_schema.routines
            WHERE routine_schema = 'public' AND routine_name = %s
            """,
            (name,),
        )
        if cur.fetchone() is None:
            missing.append(name)
    return missing


def validate_schema(database_url: str) -> int:
    """Return 0 if all checks pass, 1 otherwise."""
    try:
        conn = psycopg2.connect(database_url)
    except Exception as e:
        logger.error("Connection failed: %s", e)
        return 1

    try:
        with conn.cursor() as cur:
            t_miss = _check(cur, "public", REQUIRED_TABLES)
            v_miss = _check(cur, "public", REQUIRED_VIEWS)
            f_miss = _check_routine(cur, REQUIRED_FUNCTIONS)
    finally:
        conn.close()

    ok = not (t_miss or v_miss or f_miss)
    if t_miss:
        logger.error("Missing tables: %s", ", ".join(t_miss))
    if v_miss:
        logger.error("Missing views: %s", ", ".join(v_miss))
    if f_miss:
        logger.error("Missing functions: %s", ", ".join(f_miss))
    if ok:
        logger.info("Schema validation passed")
    return 0 if ok else 1


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="Validate Revenue Integrity DB schema objects",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"revenue-integrity-validate-schema {__version__}",
    )
    args = parser.parse_args()
    code = validate_schema(DATABASE_URL)
    sys.exit(code)


if __name__ == "__main__":
    main()
