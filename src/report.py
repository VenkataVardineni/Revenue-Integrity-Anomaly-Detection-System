#!/usr/bin/env python3
"""
Report Generation Module for Revenue Integrity Anomaly Detection System

This module:
1. Generates markdown reports for monitoring runs
2. Creates CSV exports of incidents and metrics
3. Saves artifacts to the artifacts directory
4. Builds Slack message payloads
"""

import os
import csv
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

from logutil import setup_logging
from _version import __version__

setup_logging()
logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://analyst:analyst_secure_pw@localhost:5432/revenue_integrity')
ARTIFACTS_DIR = Path(__file__).parent.parent / 'artifacts'


class ReportGenerator:
    """Generate reports and artifacts for monitoring runs."""
    
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
    
    def get_run_details(self, run_id: str) -> Dict[str, Any]:
        """Get details of a monitoring run."""
        conn = self.connect()
        
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM monitor_runs WHERE run_id = %s
            """, (run_id,))
            result = cur.fetchone()
        
        return dict(result) if result else {}
    
    def get_anomalies(self, run_id: str) -> List[Dict[str, Any]]:
        """Get all anomalies for a run."""
        conn = self.connect()
        
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT 
                    anomaly_id,
                    metric_name,
                    metric_timestamp,
                    current_value,
                    baseline_mean,
                    baseline_std,
                    baseline_q1,
                    baseline_q3,
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
                    CASE severity WHEN 'critical' THEN 1 WHEN 'warning' THEN 2 ELSE 3 END,
                    ABS(COALESCE(z_score, 0)) DESC
            """, (run_id,))
            return [dict(row) for row in cur.fetchall()]
    
    def get_incidents(self, run_id: str) -> List[Dict[str, Any]]:
        """Get active incidents related to a run's timeframe."""
        conn = self.connect()
        
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM v_active_incidents
                ORDER BY 
                    CASE severity WHEN 'critical' THEN 1 WHEN 'warning' THEN 2 ELSE 3 END,
                    last_detected_at DESC
            """)
            return [dict(row) for row in cur.fetchall()]
    
    def get_metrics_snapshot(self, window_start: datetime, window_end: datetime) -> List[Dict[str, Any]]:
        """Get metrics snapshot for a time window."""
        conn = self.connect()
        
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
    
    def create_run_directory(self, run_id: str) -> Path:
        """Create artifacts directory for a run."""
        run_dir = ARTIFACTS_DIR / f"run_{run_id}"
        run_dir.mkdir(parents=True, exist_ok=True)
        return run_dir
    
    def save_incidents_csv(self, run_id: str, incidents: List[Dict]) -> Path:
        """Save incidents to CSV."""
        run_dir = self.create_run_directory(run_id)
        filepath = run_dir / "incidents.csv"
        
        if not incidents:
            # Create empty file
            filepath.touch()
            return filepath
        
        fieldnames = [
            'incident_id', 'incident_key', 'metric_name', 'severity',
            'current_value', 'baseline_value', 'deviation_pct',
            'first_detected_at', 'last_detected_at', 'detection_count',
            'root_cause_hint', 'related_metrics'
        ]
        
        with open(filepath, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            for incident in incidents:
                # Convert arrays to strings
                row = dict(incident)
                if row.get('related_metrics'):
                    row['related_metrics'] = ', '.join(row['related_metrics'])
                writer.writerow(row)
        
        logger.info(f"Saved incidents to {filepath}")
        return filepath
    
    def save_metrics_csv(self, run_id: str, metrics: List[Dict]) -> Path:
        """Save metrics snapshot to CSV."""
        run_dir = self.create_run_directory(run_id)
        filepath = run_dir / "metrics_snapshot.csv"
        
        if not metrics:
            filepath.touch()
            return filepath
        
        fieldnames = [
            'hour_ts', 'metric_name', 'metric_value',
            'baseline_mean', 'baseline_std', 'z_score', 'pct_change_from_mean'
        ]
        
        with open(filepath, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(metrics)
        
        logger.info(f"Saved metrics to {filepath}")
        return filepath
    
    def generate_markdown_report(
        self,
        run_id: str,
        run_details: Dict,
        anomalies: List[Dict],
        incidents: List[Dict]
    ) -> str:
        """Generate markdown report for a run."""
        
        # Count by severity
        critical_count = sum(1 for i in incidents if i['severity'] == 'critical')
        warning_count = sum(1 for i in incidents if i['severity'] == 'warning')
        
        # Build report
        report = []
        report.append("# Revenue Integrity Monitoring Report")
        report.append("")
        report.append(f"**Run ID:** `{run_id}`")
        report.append(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}")
        report.append("")
        
        # Summary
        report.append("## Summary")
        report.append("")
        report.append(f"| Metric | Value |")
        report.append(f"|--------|-------|")
        report.append(f"| Window Start | {run_details.get('window_start', 'N/A')} |")
        report.append(f"| Window End | {run_details.get('window_end', 'N/A')} |")
        report.append(f"| Status | {run_details.get('status', 'N/A')} |")
        report.append(f"| Anomalies Detected | {run_details.get('anomalies_detected', 0)} |")
        report.append(f"| Critical Incidents | {critical_count} |")
        report.append(f"| Warning Incidents | {warning_count} |")
        report.append("")
        
        # Alert status
        if critical_count > 0:
            report.append("## ⛔ CRITICAL ALERTS")
            report.append("")
            for inc in incidents:
                if inc['severity'] == 'critical':
                    report.append(f"### {inc['metric_name']}")
                    report.append("")
                    report.append(f"- **Current Value:** {inc['current_value']:.2f}")
                    report.append(f"- **Baseline Value:** {inc['baseline_value']:.2f}")
                    report.append(f"- **Deviation:** {inc['deviation_pct']:.1f}%")
                    report.append(f"- **First Detected:** {inc['first_detected_at']}")
                    report.append(f"- **Detection Count:** {inc['detection_count']}")
                    if inc.get('root_cause_hint'):
                        report.append(f"- **Root Cause Hint:** {inc['root_cause_hint']}")
                    if inc.get('related_metrics'):
                        report.append(f"- **Related Metrics:** {', '.join(inc['related_metrics'])}")
                    report.append("")
        
        if warning_count > 0:
            report.append("## ⚠️ WARNING ALERTS")
            report.append("")
            for inc in incidents:
                if inc['severity'] == 'warning':
                    report.append(f"### {inc['metric_name']}")
                    report.append("")
                    report.append(f"- **Current Value:** {inc['current_value']:.2f}")
                    report.append(f"- **Baseline Value:** {inc['baseline_value']:.2f}")
                    report.append(f"- **Deviation:** {inc['deviation_pct']:.1f}%")
                    report.append("")
        
        # Anomaly details
        if anomalies:
            report.append("## Anomaly Details")
            report.append("")
            report.append("| Metric | Value | Baseline | Z-Score | Detector | Severity |")
            report.append("|--------|-------|----------|---------|----------|----------|")
            
            for a in anomalies[:20]:  # Top 20
                z = f"{a['z_score']:.2f}" if a['z_score'] else "N/A"
                baseline = f"{a['baseline_mean']:.2f}" if a['baseline_mean'] else "N/A"
                report.append(
                    f"| {a['metric_name']} | {a['current_value']:.2f} | "
                    f"{baseline} | {z} | {a['detector']} | {a['severity']} |"
                )
            report.append("")
        
        # Footer
        report.append("---")
        report.append("")
        report.append("*Generated by Revenue Integrity Anomaly Detection System*")
        
        return "\n".join(report)
    
    def save_markdown_report(self, run_id: str, report: str) -> Path:
        """Save markdown report to file."""
        run_dir = self.create_run_directory(run_id)
        filepath = run_dir / "report.md"
        
        with open(filepath, 'w') as f:
            f.write(report)
        
        logger.info(f"Saved report to {filepath}")
        return filepath
    
    def generate_full_report(self, run_id: str) -> Dict[str, Path]:
        """Generate all artifacts for a run."""
        
        # Get data
        run_details = self.get_run_details(run_id)
        if not run_details:
            raise ValueError(f"Run not found: {run_id}")
        
        anomalies = self.get_anomalies(run_id)
        incidents = self.get_incidents(run_id)
        
        # Get metrics if we have window info
        metrics = []
        if run_details.get('window_start') and run_details.get('window_end'):
            metrics = self.get_metrics_snapshot(
                run_details['window_start'],
                run_details['window_end']
            )
        
        # Generate and save artifacts
        incidents_csv = self.save_incidents_csv(run_id, incidents)
        metrics_csv = self.save_metrics_csv(run_id, metrics)
        
        markdown_report = self.generate_markdown_report(
            run_id, run_details, anomalies, incidents
        )
        report_md = self.save_markdown_report(run_id, markdown_report)
        
        return {
            'incidents_csv': incidents_csv,
            'metrics_csv': metrics_csv,
            'report_md': report_md
        }
    
    def build_slack_payload(
        self,
        run_id: str,
        incidents: List[Dict],
        github_run_url: Optional[str] = None
    ) -> Dict[str, Any]:
        """Build Slack message payload."""
        
        critical_incidents = [i for i in incidents if i['severity'] == 'critical']
        warning_incidents = [i for i in incidents if i['severity'] == 'warning']
        
        # Determine overall severity
        if critical_incidents:
            severity = 'critical'
            color = '#dc3545'  # Red
            emoji = '🚨'
        elif warning_incidents:
            severity = 'warning'
            color = '#ffc107'  # Yellow
            emoji = '⚠️'
        else:
            severity = 'info'
            color = '#17a2b8'  # Blue
            emoji = 'ℹ️'
        
        # Build text
        headline = f"{emoji} Revenue Integrity Alert ({severity.upper()})"
        
        # Build blocks
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": headline,
                    "emoji": True
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Run ID:* `{run_id}`\n*Time:* {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}"
                }
            },
            {"type": "divider"}
        ]
        
        # Add top incidents
        top_incidents = (critical_incidents + warning_incidents)[:5]
        
        if top_incidents:
            incident_text = "*Top Incidents:*\n"
            for inc in top_incidents:
                severity_emoji = "🔴" if inc['severity'] == 'critical' else "🟡"
                incident_text += (
                    f"{severity_emoji} *{inc['metric_name']}*: "
                    f"{inc['current_value']:.2f} (baseline: {inc['baseline_value']:.2f}, "
                    f"{inc['deviation_pct']:+.1f}%)\n"
                )
            
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": incident_text
                }
            })
        
        # Add root cause hints
        hints = [i.get('root_cause_hint') for i in top_incidents if i.get('root_cause_hint')]
        if hints:
            blocks.append({"type": "divider"})
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Suggested Action:*\n{hints[0]}"
                }
            })
        
        # Add link to GitHub Actions
        if github_run_url:
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"<{github_run_url}|View Full Report in GitHub Actions>"
                }
            })
        
        return {
            "text": headline,
            "attachments": [
                {
                    "color": color,
                    "blocks": blocks
                }
            ]
        }


def main():
    """Generate report from command line."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Generate monitoring report')
    parser.add_argument(
        '--version',
        action='version',
        version=f'revenue-integrity-report {__version__}',
    )
    parser.add_argument('--run-id', required=True, help='Run ID to generate report for')
    parser.add_argument('--slack-preview', action='store_true', help='Preview Slack message')
    
    args = parser.parse_args()
    
    with ReportGenerator() as generator:
        # Generate artifacts
        artifacts = generator.generate_full_report(args.run_id)
        
        print(f"\nArtifacts generated:")
        for name, path in artifacts.items():
            print(f"  {name}: {path}")
        
        # Preview Slack message
        if args.slack_preview:
            incidents = generator.get_incidents(args.run_id)
            payload = generator.build_slack_payload(args.run_id, incidents)
            print(f"\nSlack Payload Preview:")
            print(json.dumps(payload, indent=2, default=str))


if __name__ == '__main__':
    main()

