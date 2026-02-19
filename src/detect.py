#!/usr/bin/env python3
"""
Detection Module for Revenue Integrity Anomaly Detection System

This module provides Python wrappers around the SQL detectors
and handles the orchestration of detection runs.
"""

import os
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://analyst:analyst_secure_pw@localhost:5432/revenue_integrity')


class AnomalyDetector:
    """Main detector class for running anomaly detection."""
    
    def __init__(self, database_url: str = DATABASE_URL):
        self.database_url = database_url
        self.conn = None
    
    def connect(self):
        """Establish database connection."""
        if self.conn is None or self.conn.closed:
            self.conn = psycopg2.connect(self.database_url)
        return self.conn
    
    def close(self):
        """Close database connection."""
        if self.conn and not self.conn.closed:
            self.conn.close()
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    def compute_baselines(
        self,
        current_hour: Optional[datetime] = None,
        baseline_periods: int = 168
    ) -> bool:
        """Compute baselines for all metrics."""
        conn = self.connect()
        
        if current_hour is None:
            current_hour = datetime.now().replace(minute=0, second=0, microsecond=0)
        
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "CALL compute_all_baselines(%s, %s)",
                    (current_hour, baseline_periods)
                )
            conn.commit()
            logger.info(f"Baselines computed for {current_hour}")
            return True
        except Exception as e:
            logger.error(f"Error computing baselines: {e}")
            conn.rollback()
            return False
    
    def run_zscore_detectors(
        self,
        run_id: str,
        window_start: datetime,
        window_end: datetime
    ) -> int:
        """Run all Z-score based detectors."""
        conn = self.connect()
        
        with conn.cursor() as cur:
            cur.execute(
                "SELECT run_all_zscore_detectors(%s, %s, %s)",
                (run_id, window_start, window_end)
            )
            count = cur.fetchone()[0]
        
        conn.commit()
        logger.info(f"Z-score detectors found {count} anomalies")
        return count
    
    def run_iqr_detectors(
        self,
        run_id: str,
        window_start: datetime,
        window_end: datetime
    ) -> int:
        """Run all IQR based detectors."""
        conn = self.connect()
        
        with conn.cursor() as cur:
            cur.execute(
                "SELECT run_all_iqr_detectors(%s, %s, %s)",
                (run_id, window_start, window_end)
            )
            count = cur.fetchone()[0]
        
        conn.commit()
        logger.info(f"IQR detectors found {count} anomalies")
        return count
    
    def run_rules_detectors(
        self,
        run_id: str,
        window_start: datetime,
        window_end: datetime
    ) -> int:
        """Run all rules-based detectors."""
        conn = self.connect()
        
        with conn.cursor() as cur:
            cur.execute(
                "SELECT run_all_rules_detectors(%s, %s, %s)",
                (run_id, window_start, window_end)
            )
            count = cur.fetchone()[0]
        
        conn.commit()
        logger.info(f"Rules detectors found {count} anomalies")
        return count
    
    def create_run(
        self,
        window_start: datetime,
        window_end: datetime,
        baseline_periods: int = 168
    ) -> str:
        """Create a new monitoring run record."""
        conn = self.connect()
        
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO monitor_runs (window_start, window_end, baseline_periods, status)
                VALUES (%s, %s, %s, 'running')
                RETURNING run_id
            """, (window_start, window_end, baseline_periods))
            run_id = cur.fetchone()[0]
        
        conn.commit()
        logger.info(f"Created run: {run_id}")
        return str(run_id)
    
    def complete_run(
        self,
        run_id: str,
        anomalies_detected: int,
        status: str = 'completed',
        error_message: Optional[str] = None
    ):
        """Mark a run as completed."""
        conn = self.connect()
        
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE monitor_runs
                SET ended_at = NOW(),
                    status = %s,
                    anomalies_detected = %s,
                    error_message = %s
                WHERE run_id = %s
            """, (status, anomalies_detected, error_message, run_id))
        
        conn.commit()
    
    def create_incidents(self, run_id: str) -> int:
        """Create/update incidents from anomalies."""
        conn = self.connect()
        
        with conn.cursor() as cur:
            cur.execute("SELECT create_anomaly_incidents(%s)", (run_id,))
            count = cur.fetchone()[0]
        
        conn.commit()
        logger.info(f"Created/updated {count} incidents")
        return count
    
    def run_full_pipeline(
        self,
        window_start: Optional[datetime] = None,
        window_end: Optional[datetime] = None,
        baseline_periods: int = 168
    ) -> Dict[str, Any]:
        """Run the complete detection pipeline."""
        
        # Set defaults
        if window_end is None:
            window_end = datetime.now().replace(minute=0, second=0, microsecond=0)
        if window_start is None:
            window_start = window_end - timedelta(hours=1)
        
        logger.info(f"Starting pipeline: {window_start} to {window_end}")
        
        try:
            # Create run
            run_id = self.create_run(window_start, window_end, baseline_periods)
            
            # Compute baselines
            self.compute_baselines(window_end, baseline_periods)
            
            # Run detectors
            zscore_count = self.run_zscore_detectors(run_id, window_start, window_end)
            iqr_count = self.run_iqr_detectors(run_id, window_start, window_end)
            rules_count = self.run_rules_detectors(run_id, window_start, window_end)
            
            total_anomalies = zscore_count + iqr_count + rules_count
            
            # Create incidents
            incident_count = self.create_incidents(run_id)
            
            # Complete run
            self.complete_run(run_id, total_anomalies)
            
            return {
                'run_id': run_id,
                'window_start': window_start,
                'window_end': window_end,
                'zscore_anomalies': zscore_count,
                'iqr_anomalies': iqr_count,
                'rules_anomalies': rules_count,
                'total_anomalies': total_anomalies,
                'new_incidents': incident_count,
                'status': 'completed'
            }
            
        except Exception as e:
            logger.error(f"Pipeline error: {e}")
            if 'run_id' in locals():
                self.complete_run(run_id, 0, 'failed', str(e))
            raise
    
    def get_active_incidents(self) -> List[Dict[str, Any]]:
        """Get all active incidents."""
        conn = self.connect()
        
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM v_active_incidents")
            return [dict(row) for row in cur.fetchall()]
    
    def get_incident_summary(self) -> Dict[str, Any]:
        """Get summary of incidents."""
        conn = self.connect()
        
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM v_incident_summary")
            rows = cur.fetchall()
        
        summary = {
            'critical': 0,
            'warning': 0,
            'info': 0,
            'total': 0
        }
        
        for row in rows:
            summary[row['severity']] = row['incident_count']
            summary['total'] += row['incident_count']
        
        return summary
    
    def resolve_stale_incidents(self, stale_hours: int = 24) -> int:
        """Resolve incidents with no recent detections."""
        conn = self.connect()
        
        with conn.cursor() as cur:
            cur.execute("SELECT resolve_stale_incidents(%s)", (stale_hours,))
            count = cur.fetchone()[0]
        
        conn.commit()
        logger.info(f"Resolved {count} stale incidents")
        return count


def main():
    """Run detection from command line."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Run anomaly detection')
    parser.add_argument('--window-hours', type=int, default=1)
    parser.add_argument('--baseline-periods', type=int, default=168)
    
    args = parser.parse_args()
    
    with AnomalyDetector() as detector:
        result = detector.run_full_pipeline(
            baseline_periods=args.baseline_periods
        )
        
        print(f"\nDetection Complete")
        print(f"==================")
        print(f"Run ID: {result['run_id']}")
        print(f"Window: {result['window_start']} to {result['window_end']}")
        print(f"Anomalies: {result['total_anomalies']}")
        print(f"  - Z-score: {result['zscore_anomalies']}")
        print(f"  - IQR: {result['iqr_anomalies']}")
        print(f"  - Rules: {result['rules_anomalies']}")
        print(f"New Incidents: {result['new_incidents']}")
        
        # Show summary
        summary = detector.get_incident_summary()
        print(f"\nIncident Summary")
        print(f"================")
        print(f"Critical: {summary['critical']}")
        print(f"Warning: {summary['warning']}")
        print(f"Info: {summary['info']}")


if __name__ == '__main__':
    main()

