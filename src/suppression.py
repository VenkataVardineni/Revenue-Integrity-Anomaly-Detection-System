#!/usr/bin/env python3
"""
Alert Suppression Module for Revenue Integrity Anomaly Detection System

This module handles:
1. Preventing alert spam for recurring incidents
2. Grouping related incidents
3. Cooldown logic to avoid duplicate alerts
4. Incident lifecycle management
"""

import os
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
import yaml
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
CONFIG_PATH = Path(__file__).parent.parent / 'config.yaml'


class AlertSuppressor:
    """Manages alert suppression and incident grouping."""
    
    def __init__(
        self,
        database_url: str = DATABASE_URL,
        config_path: Path = CONFIG_PATH
    ):
        self.database_url = database_url
        self.config = self._load_config(config_path)
        self.conn = None
    
    def _load_config(self, config_path: Path) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        if not config_path.exists():
            return self._get_default_config()
        
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        return config or self._get_default_config()
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Return default suppression configuration."""
        return {
            'suppression': {
                'cooldown_hours': 4,
                'auto_resolve_hours': 24,
                'grouping': {
                    'enabled': True,
                    'window_minutes': 60
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
    
    def get_cooldown_hours(self) -> int:
        """Get cooldown period from config."""
        return self.config.get('suppression', {}).get('cooldown_hours', 4)
    
    def should_suppress(self, incident: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """
        Check if an incident should be suppressed.
        
        Returns:
            Tuple of (should_suppress, reason)
        """
        conn = self.connect()
        cooldown_hours = self.get_cooldown_hours()
        
        incident_key = incident.get('incident_key')
        if not incident_key:
            return False, None
        
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Check if we've already alerted for this incident recently
            cur.execute("""
                SELECT 
                    incident_id,
                    alert_sent_at,
                    is_suppressed,
                    suppression_reason,
                    detection_count
                FROM anomaly_incidents
                WHERE incident_key = %s
                  AND resolved_at IS NULL
                ORDER BY last_detected_at DESC
                LIMIT 1
            """, (incident_key,))
            
            existing = cur.fetchone()
        
        if not existing:
            return False, None
        
        # If already marked suppressed
        if existing['is_suppressed']:
            return True, existing['suppression_reason']
        
        # If alert was sent recently (within cooldown period)
        if existing['alert_sent_at']:
            time_since_alert = datetime.now() - existing['alert_sent_at']
            if time_since_alert < timedelta(hours=cooldown_hours):
                reason = f"Alert sent {time_since_alert.total_seconds() / 60:.0f} minutes ago"
                return True, reason
        
        return False, None
    
    def mark_alert_sent(self, incident_id: str, channel: str):
        """Record that an alert was sent for an incident."""
        conn = self.connect()
        
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE anomaly_incidents
                SET alert_sent_at = NOW(),
                    alert_channel = %s,
                    updated_at = NOW()
                WHERE incident_id = %s
            """, (channel, incident_id))
        
        conn.commit()
        logger.info(f"Marked alert sent for incident {incident_id} to {channel}")
    
    def suppress_incident(self, incident_id: str, reason: str):
        """Mark an incident as suppressed."""
        conn = self.connect()
        
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE anomaly_incidents
                SET is_suppressed = TRUE,
                    suppression_reason = %s,
                    updated_at = NOW()
                WHERE incident_id = %s
            """, (reason, incident_id))
        
        conn.commit()
        logger.info(f"Suppressed incident {incident_id}: {reason}")
    
    def unsuppress_incident(self, incident_id: str):
        """Remove suppression from an incident."""
        conn = self.connect()
        
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE anomaly_incidents
                SET is_suppressed = FALSE,
                    suppression_reason = NULL,
                    updated_at = NOW()
                WHERE incident_id = %s
            """, (incident_id,))
        
        conn.commit()
        logger.info(f"Unsuppressed incident {incident_id}")
    
    def filter_alertable_incidents(
        self,
        incidents: List[Dict[str, Any]]
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Filter incidents to only those that should trigger alerts.
        
        Returns:
            Tuple of (alertable_incidents, suppressed_incidents)
        """
        alertable = []
        suppressed = []
        
        for incident in incidents:
            should_suppress, reason = self.should_suppress(incident)
            
            if should_suppress:
                incident['suppression_reason'] = reason
                suppressed.append(incident)
                logger.debug(f"Suppressing {incident.get('metric_name')}: {reason}")
            else:
                alertable.append(incident)
        
        logger.info(f"Filtered incidents: {len(alertable)} alertable, {len(suppressed)} suppressed")
        return alertable, suppressed
    
    def group_incidents(
        self,
        incidents: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Group related incidents together.
        
        Groups by:
        - Metric category (revenue, conversion, etc.)
        - Time proximity
        - Root cause similarity
        """
        if not self.config.get('suppression', {}).get('grouping', {}).get('enabled', True):
            return incidents
        
        window_minutes = self.config.get('suppression', {}).get('grouping', {}).get('window_minutes', 60)
        
        # Group by metric category
        grouped = {}
        for incident in incidents:
            metric_name = incident.get('metric_name', '')
            
            # Determine category
            if any(m in metric_name for m in ['revenue', 'order', 'aov']):
                category = 'revenue'
            elif any(m in metric_name for m in ['rate', 'conversion']):
                category = 'conversion'
            elif any(m in metric_name for m in ['funnel', 'break', 'missing']):
                category = 'pipeline'
            else:
                category = 'other'
            
            if category not in grouped:
                grouped[category] = {
                    'category': category,
                    'primary_incident': incident,
                    'related_incidents': [],
                    'severity': incident.get('severity', 'info'),
                    'metric_names': [metric_name]
                }
            else:
                grouped[category]['related_incidents'].append(incident)
                grouped[category]['metric_names'].append(metric_name)
                
                # Escalate severity if any related incident is more severe
                if incident.get('severity') == 'critical':
                    grouped[category]['severity'] = 'critical'
                elif incident.get('severity') == 'warning' and grouped[category]['severity'] != 'critical':
                    grouped[category]['severity'] = 'warning'
        
        # Flatten back to list, keeping primary incidents
        result = []
        for category, group in grouped.items():
            primary = group['primary_incident']
            primary['related_count'] = len(group['related_incidents'])
            primary['category'] = category
            primary['grouped_metrics'] = group['metric_names']
            result.append(primary)
        
        logger.info(f"Grouped {len(incidents)} incidents into {len(result)} groups")
        return result
    
    def auto_resolve_stale_incidents(self) -> int:
        """Resolve incidents that haven't been detected recently."""
        conn = self.connect()
        auto_resolve_hours = self.config.get('suppression', {}).get('auto_resolve_hours', 24)
        
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE anomaly_incidents
                SET resolved_at = NOW(),
                    resolution_notes = %s,
                    updated_at = NOW()
                WHERE resolved_at IS NULL
                  AND last_detected_at < NOW() - INTERVAL '%s hours'
            """, (
                f'Auto-resolved: no detections in {auto_resolve_hours} hours',
                auto_resolve_hours
            ))
            
            count = cur.rowcount
        
        conn.commit()
        logger.info(f"Auto-resolved {count} stale incidents")
        return count
    
    def get_suppression_stats(self) -> Dict[str, Any]:
        """Get statistics about suppression."""
        conn = self.connect()
        
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT 
                    COUNT(*) FILTER (WHERE is_suppressed) AS suppressed_count,
                    COUNT(*) FILTER (WHERE NOT is_suppressed AND resolved_at IS NULL) AS active_count,
                    COUNT(*) FILTER (WHERE resolved_at IS NOT NULL) AS resolved_count,
                    COUNT(*) FILTER (WHERE alert_sent_at IS NOT NULL) AS alerted_count
                FROM anomaly_incidents
                WHERE last_detected_at >= NOW() - INTERVAL '24 hours'
            """)
            result = cur.fetchone()
        
        return dict(result) if result else {}


def filter_and_group_incidents(
    incidents: List[Dict[str, Any]],
    database_url: str = DATABASE_URL
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Convenience function to filter and group incidents.
    
    Returns:
        Tuple of (alertable incidents, suppressed incidents)
    """
    with AlertSuppressor(database_url) as suppressor:
        alertable, suppressed = suppressor.filter_alertable_incidents(incidents)
        grouped = suppressor.group_incidents(alertable)
        return grouped, suppressed


def main():
    """Test suppression from command line."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Alert suppression management')
    parser.add_argument('--stats', action='store_true', help='Show suppression stats')
    parser.add_argument('--auto-resolve', action='store_true', help='Auto-resolve stale incidents')
    parser.add_argument('--show-suppressed', action='store_true', help='Show suppressed incidents')
    
    args = parser.parse_args()
    
    with AlertSuppressor() as suppressor:
        if args.stats:
            stats = suppressor.get_suppression_stats()
            print("\nSuppression Statistics (last 24 hours):")
            print(f"  Active incidents: {stats.get('active_count', 0)}")
            print(f"  Suppressed incidents: {stats.get('suppressed_count', 0)}")
            print(f"  Resolved incidents: {stats.get('resolved_count', 0)}")
            print(f"  Alerted incidents: {stats.get('alerted_count', 0)}")
        
        if args.auto_resolve:
            count = suppressor.auto_resolve_stale_incidents()
            print(f"\nAuto-resolved {count} stale incidents")
        
        if args.show_suppressed:
            conn = suppressor.connect()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT metric_name, severity, suppression_reason, last_detected_at
                    FROM anomaly_incidents
                    WHERE is_suppressed = TRUE AND resolved_at IS NULL
                    ORDER BY last_detected_at DESC
                    LIMIT 20
                """)
                rows = cur.fetchall()
            
            if rows:
                print("\nSuppressed Incidents:")
                for row in rows:
                    print(f"  [{row['severity']}] {row['metric_name']}: {row['suppression_reason']}")
            else:
                print("\nNo suppressed incidents")


if __name__ == '__main__':
    main()

