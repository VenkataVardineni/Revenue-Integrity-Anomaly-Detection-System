#!/usr/bin/env python3
"""
Extract & Execute Module for Revenue Integrity Anomaly Detection System

This module:
1. Connects to PostgreSQL
2. Executes SQL files in order to set up schema/views
3. Runs the detection pipeline
4. Returns run results and anomalies
"""

import os
import sys
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple
import yaml
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

from logutil import setup_logging
from _version import __version__
from metrics_export import write_run_metrics_textfile

setup_logging()
logger = logging.getLogger(__name__)

# Configuration
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://analyst:analyst_secure_pw@localhost:5432/revenue_integrity')
SQL_DIR = Path(__file__).parent.parent / 'sql'
CONFIG_PATH = Path(__file__).parent.parent / 'config.yaml'

# SQL files in execution order
SQL_FILES = [
    '001_schema.sql',
    '010_metrics_views.sql',
    '011_baseline_logic.sql',
    '020_detectors_zscore.sql',
    '021_detectors_iqr.sql',
    '022_detectors_rules.sql',
    '030_anomaly_rollup.sql',
    '040_retention.sql',
]


class DatabaseConnection:
    """Context manager for database connections."""
    
    def __init__(self, database_url: str = DATABASE_URL):
        self.database_url = database_url
        self.conn = None
    
    def __enter__(self):
        self.conn = psycopg2.connect(self.database_url)
        return self.conn
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            if exc_type is None:
                self.conn.commit()
            else:
                self.conn.rollback()
            self.conn.close()


def get_connection():
    """Create a database connection."""
    return psycopg2.connect(DATABASE_URL)


def execute_sql_file(conn, filepath: Path) -> bool:
    """Execute a SQL file."""
    logger.info(f"Executing SQL file: {filepath.name}")
    
    try:
        with open(filepath, 'r') as f:
            sql = f.read()
        
        with conn.cursor() as cur:
            cur.execute(sql)
        
        conn.commit()
        logger.info(f"Successfully executed: {filepath.name}")
        return True
        
    except Exception as e:
        logger.error(f"Error executing {filepath.name}: {e}")
        conn.rollback()
        return False


def initialize_schema(conn) -> bool:
    """Execute all SQL files to initialize schema and functions."""
    logger.info("Initializing database schema and functions...")
    
    success = True
    for sql_file in SQL_FILES:
        filepath = SQL_DIR / sql_file
        if filepath.exists():
            if not execute_sql_file(conn, filepath):
                success = False
                break
        else:
            logger.warning(f"SQL file not found: {filepath}")
    
    return success


def run_detection_pipeline(
    conn,
    window_start: Optional[datetime] = None,
    window_end: Optional[datetime] = None,
    baseline_periods: int = 168
) -> Dict[str, Any]:
    """
    Run the full detection pipeline.
    
    Args:
        conn: Database connection
        window_start: Start of detection window (default: 1 hour ago)
        window_end: End of detection window (default: now, truncated to hour)
        baseline_periods: Number of hourly periods for baseline (default: 168 = 7 days)
    
    Returns:
        Dictionary with run results
    """
    logger.info("Running detection pipeline...")
    
    # Set defaults
    if window_end is None:
        window_end = datetime.now().replace(minute=0, second=0, microsecond=0)
    if window_start is None:
        window_start = window_end - timedelta(hours=1)
    
    logger.info(f"Detection window: {window_start} to {window_end}")
    logger.info(f"Baseline periods: {baseline_periods} hours")
    
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Run the full detection pipeline
            cur.execute("""
                SELECT * FROM run_full_detection_pipeline(
                    %s::TIMESTAMP WITH TIME ZONE,
                    %s::TIMESTAMP WITH TIME ZONE,
                    %s
                )
            """, (window_start, window_end, baseline_periods))
            
            result = cur.fetchone()
            
            conn.commit()
            
            if result:
                logger.info(f"Pipeline completed. Run ID: {result['run_id']}")
                logger.info(f"  Z-score anomalies: {result['zscore_anomalies']}")
                logger.info(f"  IQR anomalies: {result['iqr_anomalies']}")
                logger.info(f"  Rules anomalies: {result['rules_anomalies']}")
                logger.info(f"  Total anomalies: {result['total_anomalies']}")
                logger.info(f"  New incidents: {result['new_incidents']}")
                
                return dict(result)
            else:
                logger.error("No result returned from pipeline")
                return {}
                
    except Exception as e:
        logger.error(f"Error running detection pipeline: {e}")
        conn.rollback()
        raise


def get_active_incidents(conn) -> List[Dict[str, Any]]:
    """Get all active (unresolved) incidents."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT * FROM v_active_incidents
            ORDER BY 
                CASE severity 
                    WHEN 'critical' THEN 1 
                    WHEN 'warning' THEN 2 
                    ELSE 3 
                END,
                last_detected_at DESC
        """)
        return [dict(row) for row in cur.fetchall()]


def get_anomalies_for_run(conn, run_id: str) -> List[Dict[str, Any]]:
    """Get all anomalies for a specific run."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT 
                anomaly_id,
                metric_name,
                metric_timestamp,
                current_value,
                baseline_mean,
                baseline_std,
                z_score,
                iqr_distance,
                pct_change,
                detector,
                severity,
                direction,
                description
            FROM anomalies
            WHERE run_id = %s
            ORDER BY 
                CASE severity 
                    WHEN 'critical' THEN 1 
                    WHEN 'warning' THEN 2 
                    ELSE 3 
                END,
                ABS(COALESCE(z_score, 0)) DESC
        """, (run_id,))
        return [dict(row) for row in cur.fetchall()]


def get_metrics_snapshot(conn, window_start: datetime, window_end: datetime) -> List[Dict[str, Any]]:
    """Get current metrics snapshot for the window."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT 
                hour_ts,
                metric_name,
                metric_value,
                baseline_mean,
                baseline_std,
                z_score,
                pct_change_from_mean
            FROM v_metrics_with_baseline
            WHERE hour_ts >= %s AND hour_ts < %s
            ORDER BY hour_ts DESC, metric_name
        """, (window_start, window_end))
        return [dict(row) for row in cur.fetchall()]


def run_retention_cleanup(conn) -> Dict[str, Any]:
    """Prune old rows using config.yaml database.retention (or defaults)."""
    defaults = (30, 14, 90)
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
            cfg = yaml.safe_load(f) or {}
        ret = cfg.get('database', {}).get('retention', {})
        a = int(ret.get('anomalies_days', defaults[0]))
        b = int(ret.get('baselines_days', defaults[1]))
        r = int(ret.get('runs_days', defaults[2]))
    else:
        a, b, r = defaults

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            'SELECT apply_data_retention(%s, %s, %s) AS result',
            (a, b, r),
        )
        row = cur.fetchone()
    conn.commit()
    result = dict(row['result']) if row and row.get('result') else {}
    logger.info('Retention cleanup: %s', result)
    return result


def get_run_summary(conn, run_id: str) -> Dict[str, Any]:
    """Get summary of a monitoring run."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT 
                run_id,
                started_at,
                ended_at,
                status,
                window_start,
                window_end,
                baseline_periods,
                anomalies_detected,
                EXTRACT(EPOCH FROM (ended_at - started_at)) AS duration_seconds
            FROM monitor_runs
            WHERE run_id = %s
        """, (run_id,))
        result = cur.fetchone()
        return dict(result) if result else {}


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Revenue Integrity Detection Pipeline')
    parser.add_argument(
        '--version',
        action='version',
        version=f'revenue-integrity-extract {__version__}',
    )
    parser.add_argument('--init-schema', action='store_true', help='Initialize database schema')
    parser.add_argument('--run', action='store_true', help='Run detection pipeline')
    parser.add_argument('--window-hours', type=int, default=1, help='Detection window in hours')
    parser.add_argument('--baseline-periods', type=int, default=168, help='Baseline periods (hours)')
    parser.add_argument('--show-incidents', action='store_true', help='Show active incidents')
    parser.add_argument(
        '--retention-cleanup',
        action='store_true',
        help='Run apply_data_retention using config.yaml retention settings',
    )
    
    args = parser.parse_args()
    
    try:
        with DatabaseConnection() as conn:
            if args.init_schema:
                if not initialize_schema(conn):
                    logger.error("Schema initialization failed")
                    sys.exit(1)
                logger.info("Schema initialization completed")

            if args.retention_cleanup:
                summary = run_retention_cleanup(conn)
                print(json.dumps(summary, indent=2, default=str))
            
            if args.run:
                window_end = datetime.now().replace(minute=0, second=0, microsecond=0)
                window_start = window_end - timedelta(hours=args.window_hours)
                
                result = run_detection_pipeline(
                    conn,
                    window_start=window_start,
                    window_end=window_end,
                    baseline_periods=args.baseline_periods
                )
                
                if result:
                    write_run_metrics_textfile(result)
                    print(f"\nRun ID: {result['run_id']}")
                    print(f"Total Anomalies: {result['total_anomalies']}")
                    print(f"New Incidents: {result['new_incidents']}")
            
            if args.show_incidents:
                incidents = get_active_incidents(conn)
                if incidents:
                    print(f"\n{'='*60}")
                    print(f"Active Incidents: {len(incidents)}")
                    print(f"{'='*60}")
                    for inc in incidents:
                        print(f"\n[{inc['severity'].upper()}] {inc['metric_name']}")
                        print(f"  Value: {inc['current_value']:.2f} (baseline: {inc['baseline_value']:.2f})")
                        print(f"  Deviation: {inc['deviation_pct']:.1f}%")
                        print(f"  First detected: {inc['first_detected_at']}")
                        if inc['root_cause_hint']:
                            print(f"  Hint: {inc['root_cause_hint']}")
                else:
                    print("\nNo active incidents")
                    
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()

