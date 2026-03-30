#!/usr/bin/env python3
"""
Health & Observability Module for Revenue Integrity Anomaly Detection System

This module provides meta-monitoring:
1. Checks if the monitor ran successfully in the expected timeframe
2. Alerts if the monitor itself is failing or delayed
3. Provides health check endpoints/outputs
4. Tracks monitor SLA metrics
"""

import os
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import yaml
from pathlib import Path

load_dotenv()

from logutil import setup_logging
from _version import __version__

setup_logging()
logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://analyst:analyst_secure_pw@localhost:5432/revenue_integrity')
CONFIG_PATH = Path(__file__).parent.parent / 'config.yaml'


class HealthChecker:
    """Monitor the health of the monitoring system itself."""
    
    def __init__(
        self,
        database_url: str = DATABASE_URL,
        config_path: Path = CONFIG_PATH
    ):
        self.database_url = database_url
        self.config = self._load_config(config_path)
        self.conn = None
    
    def _load_config(self, config_path: Path) -> Dict[str, Any]:
        """Load configuration."""
        if not config_path.exists():
            return self._get_default_config()
        
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        return config or self._get_default_config()
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Default configuration."""
        return {
            'observability': {
                'health_check': {
                    'max_run_gap_minutes': 30,
                    'webhook_env': 'SLACK_WEBHOOK_HEALTH'
                }
            }
        }
    
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
    
    def check_database_connection(self) -> Dict[str, Any]:
        """Check if database is accessible."""
        try:
            conn = self.connect()
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
            return {
                'status': 'healthy',
                'message': 'Database connection successful'
            }
        except Exception as e:
            return {
                'status': 'unhealthy',
                'message': f'Database connection failed: {str(e)}'
            }
    
    def check_recent_runs(self) -> Dict[str, Any]:
        """Check if monitoring runs are happening on schedule."""
        conn = self.connect()
        max_gap_minutes = self.config.get('observability', {}).get(
            'health_check', {}
        ).get('max_run_gap_minutes', 30)
        
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Get most recent successful run
            cur.execute("""
                SELECT 
                    run_id,
                    started_at,
                    ended_at,
                    status,
                    anomalies_detected,
                    EXTRACT(EPOCH FROM (NOW() - ended_at)) / 60 AS minutes_ago
                FROM monitor_runs
                WHERE status = 'completed'
                ORDER BY ended_at DESC
                LIMIT 1
            """)
            last_run = cur.fetchone()
            
            # Get recent failed runs
            cur.execute("""
                SELECT COUNT(*) AS failed_count
                FROM monitor_runs
                WHERE status = 'failed'
                  AND started_at >= NOW() - INTERVAL '1 hour'
            """)
            failed = cur.fetchone()
        
        if not last_run:
            return {
                'status': 'warning',
                'message': 'No completed monitoring runs found',
                'last_run': None,
                'failed_count': failed['failed_count'] if failed else 0
            }
        
        minutes_ago = last_run['minutes_ago']
        
        if minutes_ago > max_gap_minutes:
            return {
                'status': 'unhealthy',
                'message': f'No successful run in {minutes_ago:.0f} minutes (threshold: {max_gap_minutes})',
                'last_run': {
                    'run_id': str(last_run['run_id']),
                    'ended_at': last_run['ended_at'].isoformat(),
                    'minutes_ago': minutes_ago
                },
                'failed_count': failed['failed_count'] if failed else 0
            }
        
        return {
            'status': 'healthy',
            'message': f'Last successful run {minutes_ago:.0f} minutes ago',
            'last_run': {
                'run_id': str(last_run['run_id']),
                'ended_at': last_run['ended_at'].isoformat(),
                'minutes_ago': minutes_ago,
                'anomalies_detected': last_run['anomalies_detected']
            },
            'failed_count': failed['failed_count'] if failed else 0
        }
    
    def check_data_freshness(self) -> Dict[str, Any]:
        """Check if event data is being ingested."""
        conn = self.connect()
        
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT 
                    MAX(event_time) AS latest_event,
                    EXTRACT(EPOCH FROM (NOW() - MAX(event_time))) / 60 AS minutes_ago,
                    COUNT(*) AS events_last_hour
                FROM events
                WHERE event_time >= NOW() - INTERVAL '1 hour'
            """)
            result = cur.fetchone()
        
        if not result or not result['latest_event']:
            return {
                'status': 'warning',
                'message': 'No events found in the last hour',
                'latest_event': None,
                'events_last_hour': 0
            }
        
        minutes_ago = result['minutes_ago']
        
        # If no events in last 30 minutes, warn
        if minutes_ago > 30:
            return {
                'status': 'warning',
                'message': f'No new events in {minutes_ago:.0f} minutes',
                'latest_event': result['latest_event'].isoformat(),
                'events_last_hour': result['events_last_hour']
            }
        
        return {
            'status': 'healthy',
            'message': f'Data is fresh (last event {minutes_ago:.0f} minutes ago)',
            'latest_event': result['latest_event'].isoformat(),
            'events_last_hour': result['events_last_hour']
        }
    
    def check_incident_queue(self) -> Dict[str, Any]:
        """Check the current incident queue."""
        conn = self.connect()
        
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT 
                    COUNT(*) FILTER (WHERE severity = 'critical') AS critical_count,
                    COUNT(*) FILTER (WHERE severity = 'warning') AS warning_count,
                    COUNT(*) FILTER (WHERE alert_sent_at IS NULL) AS pending_alerts,
                    COUNT(*) AS total_active
                FROM anomaly_incidents
                WHERE resolved_at IS NULL
            """)
            result = cur.fetchone()
        
        status = 'healthy'
        if result['critical_count'] > 0:
            status = 'critical'
        elif result['warning_count'] > 0:
            status = 'warning'
        
        return {
            'status': status,
            'message': f"{result['critical_count']} critical, {result['warning_count']} warning incidents",
            'critical_count': result['critical_count'],
            'warning_count': result['warning_count'],
            'pending_alerts': result['pending_alerts'],
            'total_active': result['total_active']
        }
    
    def get_run_statistics(self, hours: int = 24) -> Dict[str, Any]:
        """Get monitoring run statistics."""
        conn = self.connect()
        
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT 
                    COUNT(*) AS total_runs,
                    COUNT(*) FILTER (WHERE status = 'completed') AS completed,
                    COUNT(*) FILTER (WHERE status = 'failed') AS failed,
                    AVG(EXTRACT(EPOCH FROM (ended_at - started_at))) AS avg_duration_seconds,
                    SUM(anomalies_detected) AS total_anomalies
                FROM monitor_runs
                WHERE started_at >= NOW() - INTERVAL '%s hours'
            """, (hours,))
            result = cur.fetchone()
        
        success_rate = 0
        if result['total_runs'] > 0:
            success_rate = result['completed'] / result['total_runs'] * 100
        
        return {
            'period_hours': hours,
            'total_runs': result['total_runs'],
            'completed': result['completed'],
            'failed': result['failed'],
            'success_rate': success_rate,
            'avg_duration_seconds': result['avg_duration_seconds'] or 0,
            'total_anomalies': result['total_anomalies'] or 0
        }
    
    def get_full_health_status(self) -> Dict[str, Any]:
        """Get comprehensive health status."""
        checks = {
            'database': self.check_database_connection(),
            'recent_runs': self.check_recent_runs(),
            'data_freshness': self.check_data_freshness(),
            'incidents': self.check_incident_queue()
        }
        
        # Determine overall status
        statuses = [check['status'] for check in checks.values()]
        
        if 'unhealthy' in statuses:
            overall_status = 'unhealthy'
        elif 'critical' in statuses:
            overall_status = 'critical'
        elif 'warning' in statuses:
            overall_status = 'warning'
        else:
            overall_status = 'healthy'
        
        return {
            'timestamp': datetime.now().isoformat(),
            'overall_status': overall_status,
            'checks': checks,
            'statistics': self.get_run_statistics()
        }
    
    def should_alert_unhealthy(self) -> bool:
        """Determine if we should send a health alert."""
        status = self.get_full_health_status()
        return status['overall_status'] in ['unhealthy', 'critical']
    
    def send_health_alert(self) -> bool:
        """Send alert if monitor is unhealthy."""
        from slack import SlackClient
        
        health_status = self.get_full_health_status()
        
        if health_status['overall_status'] == 'healthy':
            logger.info("Monitor is healthy, no alert needed")
            return True
        
        # Build alert message
        unhealthy_checks = [
            f"- {name}: {check['message']}"
            for name, check in health_status['checks'].items()
            if check['status'] in ['unhealthy', 'critical', 'warning']
        ]
        
        message = f"""❌ Revenue Integrity Monitor Health Alert

Overall Status: {health_status['overall_status'].upper()}

Issues:
{chr(10).join(unhealthy_checks)}

Statistics (last 24h):
- Total runs: {health_status['statistics']['total_runs']}
- Success rate: {health_status['statistics']['success_rate']:.1f}%
- Failed runs: {health_status['statistics']['failed']}
"""
        
        # Get health webhook or fall back to default
        webhook_env = self.config.get('observability', {}).get(
            'health_check', {}
        ).get('webhook_env', 'SLACK_WEBHOOK_URL')
        
        webhook_url = os.getenv(webhook_env) or os.getenv('SLACK_WEBHOOK_URL')
        
        client = SlackClient(webhook_url=webhook_url)
        return client.send_simple_message(message)


def check_health() -> Dict[str, Any]:
    """Convenience function to check health."""
    with HealthChecker() as checker:
        return checker.get_full_health_status()


def main():
    """Run health check from command line."""
    import argparse
    import json
    
    parser = argparse.ArgumentParser(description='Monitor health check')
    parser.add_argument(
        '--version',
        action='version',
        version=f'revenue-integrity-health {__version__}',
    )
    parser.add_argument('--json', action='store_true', help='Output as JSON')
    parser.add_argument('--alert', action='store_true', help='Send alert if unhealthy')
    parser.add_argument('--stats', action='store_true', help='Show run statistics')
    
    args = parser.parse_args()
    
    with HealthChecker() as checker:
        health = checker.get_full_health_status()
        
        if args.json:
            print(json.dumps(health, indent=2, default=str))
        else:
            # Pretty print
            status_emoji = {
                'healthy': '✅',
                'warning': '⚠️',
                'critical': '🔴',
                'unhealthy': '❌'
            }
            
            print("\n" + "=" * 50)
            print("REVENUE INTEGRITY MONITOR HEALTH")
            print("=" * 50)
            
            overall = health['overall_status']
            print(f"\n{status_emoji.get(overall, '?')} Overall Status: {overall.upper()}")
            
            print("\nComponent Checks:")
            for name, check in health['checks'].items():
                emoji = status_emoji.get(check['status'], '?')
                print(f"  {emoji} {name}: {check['message']}")
            
            if args.stats:
                stats = health['statistics']
                print("\nRun Statistics (last 24h):")
                print(f"  Total runs: {stats['total_runs']}")
                print(f"  Completed: {stats['completed']}")
                print(f"  Failed: {stats['failed']}")
                print(f"  Success rate: {stats['success_rate']:.1f}%")
                print(f"  Avg duration: {stats['avg_duration_seconds']:.1f}s")
                print(f"  Total anomalies: {stats['total_anomalies']}")
            
            print()
        
        if args.alert:
            if checker.should_alert_unhealthy():
                print("Sending health alert...")
                checker.send_health_alert()
            else:
                print("Monitor is healthy, no alert sent")


if __name__ == '__main__':
    main()

