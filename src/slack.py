#!/usr/bin/env python3
"""
Slack Integration Module for Revenue Integrity Anomaly Detection System

This module handles:
1. Sending alerts to Slack via incoming webhooks
2. Message formatting with proper blocks and attachments
3. Rate limiting and error handling
"""

import os
import json
import logging
import time
from datetime import datetime
from typing import Dict, Any, List, Optional
import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
SLACK_WEBHOOK_URL = os.getenv('SLACK_WEBHOOK_URL')
SLACK_TIMEOUT = int(os.getenv('SLACK_TIMEOUT', '10'))
SLACK_RETRY_COUNT = int(os.getenv('SLACK_RETRY_COUNT', '3'))
SLACK_RETRY_DELAY = int(os.getenv('SLACK_RETRY_DELAY', '2'))


class SlackWebhookError(Exception):
    """Custom exception for Slack webhook errors."""
    pass


class SlackClient:
    """Client for sending messages to Slack via incoming webhooks."""
    
    def __init__(
        self,
        webhook_url: Optional[str] = None,
        timeout: int = SLACK_TIMEOUT,
        retry_count: int = SLACK_RETRY_COUNT,
        retry_delay: int = SLACK_RETRY_DELAY
    ):
        self.webhook_url = webhook_url or SLACK_WEBHOOK_URL
        self.timeout = timeout
        self.retry_count = retry_count
        self.retry_delay = retry_delay
        
        if not self.webhook_url:
            logger.warning("SLACK_WEBHOOK_URL not configured. Slack alerts will be disabled.")
    
    def is_configured(self) -> bool:
        """Check if Slack webhook is configured."""
        return bool(self.webhook_url)
    
    def send_message(
        self,
        payload: Dict[str, Any],
        webhook_url: Optional[str] = None
    ) -> bool:
        """
        Send a message to Slack.
        
        Args:
            payload: Slack message payload (text, blocks, attachments)
            webhook_url: Override webhook URL (for routing to different channels)
        
        Returns:
            True if message was sent successfully
        """
        url = webhook_url or self.webhook_url
        
        if not url:
            logger.warning("No Slack webhook URL configured. Skipping message.")
            return False
        
        for attempt in range(self.retry_count):
            try:
                response = requests.post(
                    url,
                    json=payload,
                    headers={'Content-Type': 'application/json'},
                    timeout=self.timeout
                )
                
                if response.status_code == 200:
                    logger.info("Slack message sent successfully")
                    return True
                
                # Handle rate limiting
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', self.retry_delay))
                    logger.warning(f"Slack rate limited. Retrying after {retry_after}s")
                    time.sleep(retry_after)
                    continue
                
                # Log error but continue retrying for server errors
                if response.status_code >= 500:
                    logger.warning(f"Slack server error: {response.status_code}")
                    time.sleep(self.retry_delay)
                    continue
                
                # Client error - don't retry
                logger.error(f"Slack error: {response.status_code} - {response.text}")
                return False
                
            except requests.exceptions.Timeout:
                logger.warning(f"Slack request timeout (attempt {attempt + 1})")
                time.sleep(self.retry_delay)
            except requests.exceptions.RequestException as e:
                logger.error(f"Slack request error: {e}")
                time.sleep(self.retry_delay)
        
        logger.error("Failed to send Slack message after all retries")
        return False
    
    def send_simple_message(self, text: str, webhook_url: Optional[str] = None) -> bool:
        """Send a simple text message."""
        return self.send_message({'text': text}, webhook_url)
    
    def send_alert(
        self,
        run_id: str,
        severity: str,
        incidents: List[Dict[str, Any]],
        github_run_url: Optional[str] = None,
        webhook_url: Optional[str] = None
    ) -> bool:
        """
        Send an alert for detected anomalies.
        
        Args:
            run_id: Monitoring run ID
            severity: Overall severity (critical, warning, info)
            incidents: List of incident dictionaries
            github_run_url: URL to GitHub Actions run
            webhook_url: Override webhook URL
        """
        payload = self.build_alert_payload(run_id, severity, incidents, github_run_url)
        return self.send_message(payload, webhook_url)
    
    def build_alert_payload(
        self,
        run_id: str,
        severity: str,
        incidents: List[Dict[str, Any]],
        github_run_url: Optional[str] = None
    ) -> Dict[str, Any]:
        """Build a rich Slack alert payload."""
        
        # Severity styling
        severity_config = {
            'critical': {
                'emoji': '🚨',
                'color': '#dc3545',
                'text': 'CRITICAL',
                'mention': '<!here>'
            },
            'warning': {
                'emoji': '⚠️',
                'color': '#ffc107',
                'text': 'WARNING',
                'mention': ''
            },
            'info': {
                'emoji': 'ℹ️',
                'color': '#17a2b8',
                'text': 'INFO',
                'mention': ''
            }
        }
        
        config = severity_config.get(severity, severity_config['info'])
        
        # Build headline
        headline = f"{config['emoji']} Revenue Integrity Alert ({config['text']})"
        
        # Build blocks
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": headline,
                    "emoji": True
                }
            }
        ]
        
        # Add mention for critical alerts
        if config['mention']:
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"{config['mention']} Immediate attention required!"
                }
            })
        
        # Run info
        blocks.append({
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": f"*Run ID:*\n`{run_id[:8]}...`"
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Time:*\n{datetime.now().strftime('%Y-%m-%d %H:%M UTC')}"
                }
            ]
        })
        
        blocks.append({"type": "divider"})
        
        # Top incidents
        if incidents:
            critical_incidents = [i for i in incidents if i.get('severity') == 'critical']
            warning_incidents = [i for i in incidents if i.get('severity') == 'warning']
            
            top_incidents = (critical_incidents + warning_incidents)[:5]
            
            for inc in top_incidents:
                inc_severity = inc.get('severity', 'info')
                inc_emoji = "🔴" if inc_severity == 'critical' else "🟡" if inc_severity == 'warning' else "🔵"
                
                current_value = inc.get('current_value', 0)
                baseline_value = inc.get('baseline_value', 0)
                deviation_pct = inc.get('deviation_pct', 0)
                
                blocks.append({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": (
                            f"{inc_emoji} *{inc.get('metric_name', 'Unknown Metric')}*\n"
                            f"Current: `{current_value:.2f}` | "
                            f"Baseline: `{baseline_value:.2f}` | "
                            f"Change: `{deviation_pct:+.1f}%`"
                        )
                    }
                })
            
            # Root cause hint (from first incident with hint)
            hints = [i.get('root_cause_hint') for i in top_incidents if i.get('root_cause_hint')]
            if hints:
                blocks.append({"type": "divider"})
                blocks.append({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"💡 *Suggested Action:*\n{hints[0]}"
                    }
                })
        
        # Summary counts
        total_critical = len([i for i in incidents if i.get('severity') == 'critical'])
        total_warning = len([i for i in incidents if i.get('severity') == 'warning'])
        
        blocks.append({"type": "divider"})
        blocks.append({
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"🔴 Critical: {total_critical} | 🟡 Warning: {total_warning}"
                }
            ]
        })
        
        # GitHub Actions link
        if github_run_url:
            blocks.append({
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "📊 View Full Report",
                            "emoji": True
                        },
                        "url": github_run_url,
                        "action_id": "view_report"
                    }
                ]
            })
        
        return {
            "text": headline,
            "attachments": [
                {
                    "color": config['color'],
                    "blocks": blocks
                }
            ]
        }
    
    def send_health_check(self, status: str, details: Optional[str] = None) -> bool:
        """Send a health check notification."""
        emoji = "✅" if status == "healthy" else "❌"
        text = f"{emoji} Revenue Integrity Monitor: {status.upper()}"
        
        if details:
            text += f"\n{details}"
        
        return self.send_simple_message(text)
    
    def send_recovery_notification(
        self,
        metric_name: str,
        previous_value: float,
        current_value: float
    ) -> bool:
        """Send notification when a metric recovers to normal."""
        payload = {
            "text": f"✅ Recovery: {metric_name}",
            "attachments": [
                {
                    "color": "#28a745",
                    "blocks": [
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": (
                                    f"✅ *{metric_name} has recovered*\n"
                                    f"Previous: `{previous_value:.2f}` → Current: `{current_value:.2f}`"
                                )
                            }
                        }
                    ]
                }
            ]
        }
        
        return self.send_message(payload)


def send_alert(
    run_id: str,
    incidents: List[Dict[str, Any]],
    github_run_url: Optional[str] = None
) -> bool:
    """
    Convenience function to send an alert.
    
    This is the main entry point for sending alerts from the monitoring pipeline.
    """
    client = SlackClient()
    
    if not client.is_configured():
        logger.info("Slack not configured. Printing alert to console instead.")
        print(f"\n{'='*60}")
        print("ALERT: Revenue Integrity Anomaly Detected")
        print(f"{'='*60}")
        print(f"Run ID: {run_id}")
        print(f"Incidents: {len(incidents)}")
        for inc in incidents[:5]:
            print(f"  - [{inc.get('severity', 'info').upper()}] {inc.get('metric_name')}: "
                  f"{inc.get('current_value', 0):.2f}")
        print(f"{'='*60}\n")
        return True
    
    # Determine overall severity
    if any(i.get('severity') == 'critical' for i in incidents):
        severity = 'critical'
    elif any(i.get('severity') == 'warning' for i in incidents):
        severity = 'warning'
    else:
        severity = 'info'
    
    return client.send_alert(run_id, severity, incidents, github_run_url)


def main():
    """Test Slack integration from command line."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Test Slack integration')
    parser.add_argument('--test', action='store_true', help='Send test message')
    parser.add_argument('--message', type=str, help='Custom message to send')
    
    args = parser.parse_args()
    
    client = SlackClient()
    
    if not client.is_configured():
        print("ERROR: SLACK_WEBHOOK_URL not set")
        print("Set it with: export SLACK_WEBHOOK_URL='https://hooks.slack.com/services/...'")
        return
    
    if args.message:
        success = client.send_simple_message(args.message)
    elif args.test:
        # Send test alert
        test_incidents = [
            {
                'metric_name': 'checkout_completion_rate',
                'severity': 'critical',
                'current_value': 45.2,
                'baseline_value': 82.5,
                'deviation_pct': -45.2,
                'root_cause_hint': 'This is a test alert. No action required.'
            },
            {
                'metric_name': 'gross_revenue',
                'severity': 'warning',
                'current_value': 1250.00,
                'baseline_value': 2100.00,
                'deviation_pct': -40.5
            }
        ]
        
        success = client.send_alert(
            run_id='test-run-12345678',
            severity='critical',
            incidents=test_incidents,
            github_run_url='https://github.com/example/repo/actions/runs/123'
        )
    else:
        success = client.send_simple_message("🔔 Revenue Integrity Monitor: Test message")
    
    if success:
        print("Message sent successfully!")
    else:
        print("Failed to send message")


if __name__ == '__main__':
    main()

