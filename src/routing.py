#!/usr/bin/env python3
"""
Alert Routing Module for Revenue Integrity Anomaly Detection System

This module handles:
1. Loading routing configuration from config.yaml
2. Routing alerts to different Slack channels based on severity
3. Applying mentions (@here) for critical alerts
4. Managing multiple webhook destinations
"""

import os
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
import yaml
from dotenv import load_dotenv

from slack import SlackClient

load_dotenv()

from logutil import setup_logging
from _version import __version__

setup_logging()
logger = logging.getLogger(__name__)

CONFIG_PATH = Path(__file__).parent.parent / 'config.yaml'


class AlertRouter:
    """Routes alerts to appropriate Slack channels based on severity."""
    
    def __init__(self, config_path: Path = CONFIG_PATH):
        self.config = self._load_config(config_path)
        self.slack_client = SlackClient()
        self._last_alert_times: Dict[str, datetime] = {}
    
    def _load_config(self, config_path: Path) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        if not config_path.exists():
            logger.warning(f"Config file not found: {config_path}. Using defaults.")
            return self._get_default_config()
        
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        return config or self._get_default_config()
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Return default configuration."""
        return {
            'slack': {
                'default_channel': True,
                'routing': {
                    'critical': {
                        'webhook_env': 'SLACK_WEBHOOK_CRITICAL',
                        'channel': '#revenue-alerts',
                        'mention': '@here',
                        'also_default': True
                    },
                    'warning': {
                        'webhook_env': 'SLACK_WEBHOOK_WARNING',
                        'channel': '#data-quality',
                        'mention': '',
                        'also_default': False
                    },
                    'info': {
                        'webhook_env': '',
                        'channel': '',
                        'mention': '',
                        'also_default': True
                    }
                },
                'rate_limit': {
                    'min_interval_seconds': 300
                },
                'formatting': {
                    'max_incidents': 5,
                    'include_hints': True,
                    'include_github_link': True
                }
            },
            'suppression': {
                'cooldown_hours': 4
            }
        }
    
    def get_webhook_url(self, severity: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Get webhook URL for a severity level.
        
        Returns:
            Tuple of (severity-specific webhook, default webhook)
        """
        routing_config = self.config.get('slack', {}).get('routing', {})
        severity_config = routing_config.get(severity, {})
        
        # Get severity-specific webhook
        webhook_env = severity_config.get('webhook_env', '')
        specific_webhook = os.getenv(webhook_env) if webhook_env else None
        
        # Get default webhook
        default_webhook = os.getenv('SLACK_WEBHOOK_URL')
        
        return specific_webhook, default_webhook
    
    def should_alert(self, severity: str, channel: str) -> bool:
        """Check if we should send an alert based on rate limiting."""
        rate_limit = self.config.get('slack', {}).get('rate_limit', {})
        min_interval = rate_limit.get('min_interval_seconds', 300)
        
        key = f"{severity}:{channel}"
        last_time = self._last_alert_times.get(key)
        
        if last_time is None:
            return True
        
        elapsed = (datetime.now() - last_time).total_seconds()
        return elapsed >= min_interval
    
    def record_alert(self, severity: str, channel: str):
        """Record that an alert was sent."""
        key = f"{severity}:{channel}"
        self._last_alert_times[key] = datetime.now()
    
    def get_mention(self, severity: str) -> str:
        """Get mention string for severity level."""
        routing_config = self.config.get('slack', {}).get('routing', {})
        severity_config = routing_config.get(severity, {})
        return severity_config.get('mention', '')
    
    def route_alert(
        self,
        run_id: str,
        incidents: List[Dict[str, Any]],
        github_run_url: Optional[str] = None
    ) -> Dict[str, bool]:
        """
        Route alerts to appropriate channels based on severity.
        
        Returns:
            Dictionary of {channel: success} for each alert sent
        """
        results = {}
        
        if not incidents:
            logger.info("No incidents to alert on")
            return results
        
        # Group incidents by severity
        critical_incidents = [i for i in incidents if i.get('severity') == 'critical']
        warning_incidents = [i for i in incidents if i.get('severity') == 'warning']
        info_incidents = [i for i in incidents if i.get('severity') == 'info']
        
        # Determine overall severity
        if critical_incidents:
            overall_severity = 'critical'
        elif warning_incidents:
            overall_severity = 'warning'
        else:
            overall_severity = 'info'
        
        # Get routing configuration
        routing_config = self.config.get('slack', {}).get('routing', {})
        formatting_config = self.config.get('slack', {}).get('formatting', {})
        
        # Build the alert payload
        max_incidents = formatting_config.get('max_incidents', 5)
        include_hints = formatting_config.get('include_hints', True)
        
        # Route to severity-specific channel
        severity_config = routing_config.get(overall_severity, {})
        specific_webhook, default_webhook = self.get_webhook_url(overall_severity)
        
        # Prepare incidents with mention
        mention = self.get_mention(overall_severity)
        
        # Build payload
        payload = self.slack_client.build_alert_payload(
            run_id=run_id,
            severity=overall_severity,
            incidents=incidents[:max_incidents],
            github_run_url=github_run_url
        )
        
        # Add mention to payload if configured
        if mention:
            # Insert mention block after header
            mention_block = {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"{mention} Immediate attention required!"
                }
            }
            if payload.get('attachments') and payload['attachments'][0].get('blocks'):
                blocks = payload['attachments'][0]['blocks']
                # Insert after header
                blocks.insert(1, mention_block)
        
        # Send to severity-specific channel
        if specific_webhook:
            channel_name = severity_config.get('channel', 'specific')
            if self.should_alert(overall_severity, channel_name):
                success = self.slack_client.send_message(payload, specific_webhook)
                results[channel_name] = success
                if success:
                    self.record_alert(overall_severity, channel_name)
                    logger.info(f"Alert sent to {channel_name} ({overall_severity})")
            else:
                logger.info(f"Rate limited: skipping alert to {channel_name}")
                results[channel_name] = False
        
        # Also send to default channel if configured
        also_default = severity_config.get('also_default', True)
        if also_default and default_webhook:
            if self.should_alert(overall_severity, 'default'):
                success = self.slack_client.send_message(payload, default_webhook)
                results['default'] = success
                if success:
                    self.record_alert(overall_severity, 'default')
                    logger.info(f"Alert sent to default channel ({overall_severity})")
            else:
                logger.info("Rate limited: skipping alert to default channel")
                results['default'] = False
        
        return results
    
    def send_test_alert(self, severity: str = 'warning') -> Dict[str, bool]:
        """Send a test alert to verify routing."""
        test_incidents = [
            {
                'metric_name': 'test_metric',
                'severity': severity,
                'current_value': 50.0,
                'baseline_value': 100.0,
                'deviation_pct': -50.0,
                'root_cause_hint': 'This is a test alert. No action required.'
            }
        ]
        
        return self.route_alert(
            run_id='test-routing-12345',
            incidents=test_incidents,
            github_run_url='https://github.com/test/repo/actions/runs/123'
        )


def route_alert(
    run_id: str,
    incidents: List[Dict[str, Any]],
    github_run_url: Optional[str] = None
) -> Dict[str, bool]:
    """
    Convenience function to route an alert.
    
    This is the main entry point for routing alerts from the monitoring pipeline.
    """
    router = AlertRouter()
    return router.route_alert(run_id, incidents, github_run_url)


def main():
    """Test routing from command line."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Test alert routing')
    parser.add_argument(
        '--version',
        action='version',
        version=f'revenue-integrity-routing {__version__}',
    )
    parser.add_argument('--severity', choices=['critical', 'warning', 'info'], 
                       default='warning', help='Severity level to test')
    parser.add_argument('--show-config', action='store_true', help='Show routing config')
    
    args = parser.parse_args()
    
    router = AlertRouter()
    
    if args.show_config:
        import json
        print("Routing Configuration:")
        print(json.dumps(router.config.get('slack', {}), indent=2))
        print("\nEnvironment Variables:")
        print(f"  SLACK_WEBHOOK_URL: {'set' if os.getenv('SLACK_WEBHOOK_URL') else 'not set'}")
        print(f"  SLACK_WEBHOOK_CRITICAL: {'set' if os.getenv('SLACK_WEBHOOK_CRITICAL') else 'not set'}")
        print(f"  SLACK_WEBHOOK_WARNING: {'set' if os.getenv('SLACK_WEBHOOK_WARNING') else 'not set'}")
        return
    
    print(f"Sending test {args.severity} alert...")
    results = router.send_test_alert(args.severity)
    
    print("\nResults:")
    for channel, success in results.items():
        status = "✓ sent" if success else "✗ failed"
        print(f"  {channel}: {status}")


if __name__ == '__main__':
    main()

