#!/usr/bin/env python3
"""
Backtesting Module for Revenue Integrity Anomaly Detection System

This module:
1. Replays historical data through the detectors
2. Compares detected anomalies against known injected anomalies
3. Calculates precision/recall metrics for detector evaluation
4. Helps tune detection thresholds
"""

import os
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import json

load_dotenv()

from logutil import setup_logging
from _version import __version__

setup_logging()
logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://analyst:analyst_secure_pw@localhost:5432/revenue_integrity')


# Known injected anomalies from seed.py (for evaluation)
KNOWN_ANOMALIES = {
    'missing_purchase_events': {
        'offset_hours': -48,
        'duration_hours': 3,
        'expected_metrics': ['purchases', 'funnel_break_purchase', 'checkout_to_purchase_rate']
    },
    'checkout_complete_drop': {
        'offset_hours': -24,
        'duration_hours': 2,
        'expected_metrics': ['checkout_complete', 'checkout_completion_rate']
    },
    'revenue_spike': {
        'offset_hours': -12,
        'duration_hours': 1,
        'expected_metrics': ['gross_revenue', 'avg_order_value', 'revenue_per_session']
    },
    'revenue_zero': {
        'offset_hours': -6,
        'duration_hours': 1,
        'expected_metrics': ['gross_revenue', 'avg_order_value', 'zero_revenue_bug']
    },
    'missing_event_type': {
        'offset_hours': -4,
        'duration_hours': 2,
        'expected_metrics': ['add_to_cart', 'add_to_cart_count']
    }
}


class Backtester:
    """Run detectors over historical data and evaluate performance."""
    
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
    
    def get_data_range(self) -> Tuple[datetime, datetime]:
        """Get the time range of available data."""
        conn = self.connect()
        
        with conn.cursor() as cur:
            cur.execute("""
                SELECT MIN(event_time), MAX(event_time)
                FROM events
            """)
            result = cur.fetchone()
        
        return result[0], result[1]
    
    def run_detection_for_window(
        self,
        window_start: datetime,
        window_end: datetime,
        baseline_periods: int = 168
    ) -> Dict[str, Any]:
        """Run detection for a specific time window."""
        conn = self.connect()
        
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
        
        return dict(result) if result else {}
    
    def get_detected_anomalies(
        self,
        start_time: datetime,
        end_time: datetime
    ) -> List[Dict[str, Any]]:
        """Get all anomalies detected in a time range."""
        conn = self.connect()
        
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT 
                    metric_name,
                    metric_timestamp,
                    current_value,
                    baseline_mean,
                    z_score,
                    detector,
                    severity,
                    direction
                FROM anomalies
                WHERE metric_timestamp >= %s
                  AND metric_timestamp < %s
                ORDER BY metric_timestamp, metric_name
            """, (start_time, end_time))
            
            return [dict(row) for row in cur.fetchall()]
    
    def get_known_anomaly_windows(
        self,
        reference_time: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """Get time windows where we know anomalies were injected."""
        if reference_time is None:
            _, reference_time = self.get_data_range()
        
        windows = []
        for name, config in KNOWN_ANOMALIES.items():
            start = reference_time + timedelta(hours=config['offset_hours'])
            end = start + timedelta(hours=config['duration_hours'])
            
            windows.append({
                'anomaly_name': name,
                'start_time': start,
                'end_time': end,
                'expected_metrics': config['expected_metrics']
            })
        
        return windows
    
    def evaluate_detection(
        self,
        detected: List[Dict[str, Any]],
        known_windows: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Evaluate detection performance against known anomalies.
        
        Returns precision, recall, and per-anomaly breakdown.
        """
        results = {
            'total_detected': len(detected),
            'per_anomaly': {},
            'summary': {
                'true_positives': 0,
                'false_positives': 0,
                'false_negatives': 0
            }
        }
        
        # Track which detected anomalies matched known ones
        matched_detections = set()
        
        for window in known_windows:
            anomaly_name = window['anomaly_name']
            start = window['start_time']
            end = window['end_time']
            expected_metrics = window['expected_metrics']
            
            # Find detections in this window
            window_detections = [
                d for d in detected
                if start <= d['metric_timestamp'] < end
            ]
            
            # Check if expected metrics were detected
            detected_expected = []
            detected_other = []
            
            for d in window_detections:
                metric = d['metric_name']
                if any(exp in metric for exp in expected_metrics):
                    detected_expected.append(d)
                    matched_detections.add(id(d))
                else:
                    detected_other.append(d)
            
            # Calculate metrics for this anomaly
            hit = len(detected_expected) > 0
            
            results['per_anomaly'][anomaly_name] = {
                'start_time': start.isoformat(),
                'end_time': end.isoformat(),
                'expected_metrics': expected_metrics,
                'detected_expected': len(detected_expected),
                'detected_other': len(detected_other),
                'hit': hit,
                'detections': [
                    {
                        'metric': d['metric_name'],
                        'severity': d['severity'],
                        'detector': d['detector']
                    }
                    for d in detected_expected
                ]
            }
            
            if hit:
                results['summary']['true_positives'] += 1
            else:
                results['summary']['false_negatives'] += 1
        
        # Count false positives (detections outside known anomaly windows)
        false_positives = [
            d for d in detected
            if id(d) not in matched_detections
        ]
        results['summary']['false_positives'] = len(false_positives)
        
        # Calculate precision and recall
        tp = results['summary']['true_positives']
        fp = results['summary']['false_positives']
        fn = results['summary']['false_negatives']
        
        results['summary']['precision'] = tp / (tp + fp) if (tp + fp) > 0 else 0
        results['summary']['recall'] = tp / (tp + fn) if (tp + fn) > 0 else 0
        
        f1_denom = results['summary']['precision'] + results['summary']['recall']
        results['summary']['f1_score'] = (
            2 * results['summary']['precision'] * results['summary']['recall'] / f1_denom
            if f1_denom > 0 else 0
        )
        
        return results
    
    def run_backtest(
        self,
        days: int = 30,
        baseline_periods: int = 168,
        step_hours: int = 1
    ) -> Dict[str, Any]:
        """
        Run backtest over historical data.
        
        Args:
            days: Number of days to backtest
            baseline_periods: Baseline periods for each detection run
            step_hours: Hours between each detection window
        """
        logger.info(f"Starting backtest: {days} days, step={step_hours}h, baseline={baseline_periods}h")
        
        # Get data range
        data_start, data_end = self.get_data_range()
        logger.info(f"Data range: {data_start} to {data_end}")
        
        # Calculate backtest range
        # Need at least baseline_periods hours before we can detect
        backtest_start = data_start + timedelta(hours=baseline_periods)
        backtest_end = data_end
        
        logger.info(f"Backtest range: {backtest_start} to {backtest_end}")
        
        # Run detection for each window
        all_anomalies = []
        runs = []
        
        current = backtest_start
        while current < backtest_end:
            window_start = current - timedelta(hours=step_hours)
            window_end = current
            
            try:
                result = self.run_detection_for_window(
                    window_start=window_start,
                    window_end=window_end,
                    baseline_periods=baseline_periods
                )
                
                runs.append({
                    'window_start': window_start,
                    'window_end': window_end,
                    'anomalies': result.get('total_anomalies', 0)
                })
                
            except Exception as e:
                logger.warning(f"Detection failed for {window_start}: {e}")
            
            current += timedelta(hours=step_hours)
        
        # Get all detected anomalies
        all_anomalies = self.get_detected_anomalies(backtest_start, backtest_end)
        
        # Get known anomaly windows
        known_windows = self.get_known_anomaly_windows(data_end)
        
        # Evaluate
        evaluation = self.evaluate_detection(all_anomalies, known_windows)
        
        return {
            'backtest_start': backtest_start.isoformat(),
            'backtest_end': backtest_end.isoformat(),
            'total_runs': len(runs),
            'total_anomalies_detected': len(all_anomalies),
            'known_anomalies': len(known_windows),
            'evaluation': evaluation
        }
    
    def print_results(self, results: Dict[str, Any]):
        """Print backtest results in a readable format."""
        print("\n" + "=" * 60)
        print("BACKTEST RESULTS")
        print("=" * 60)
        
        print(f"\nTime Range: {results['backtest_start']} to {results['backtest_end']}")
        print(f"Total Detection Runs: {results['total_runs']}")
        print(f"Total Anomalies Detected: {results['total_anomalies_detected']}")
        print(f"Known Injected Anomalies: {results['known_anomalies']}")
        
        eval_results = results['evaluation']
        summary = eval_results['summary']
        
        print("\n" + "-" * 40)
        print("EVALUATION METRICS")
        print("-" * 40)
        print(f"True Positives:  {summary['true_positives']}")
        print(f"False Positives: {summary['false_positives']}")
        print(f"False Negatives: {summary['false_negatives']}")
        print(f"\nPrecision: {summary['precision']:.2%}")
        print(f"Recall:    {summary['recall']:.2%}")
        print(f"F1 Score:  {summary['f1_score']:.2%}")
        
        print("\n" + "-" * 40)
        print("PER-ANOMALY BREAKDOWN")
        print("-" * 40)
        
        for name, details in eval_results['per_anomaly'].items():
            status = "✓" if details['hit'] else "✗"
            print(f"\n{status} {name}")
            print(f"  Time: {details['start_time']} to {details['end_time']}")
            print(f"  Expected metrics: {', '.join(details['expected_metrics'])}")
            print(f"  Detected: {details['detected_expected']} expected, {details['detected_other']} other")
            
            if details['detections']:
                print("  Detections:")
                for d in details['detections'][:3]:  # Show first 3
                    print(f"    - {d['metric']} ({d['severity']}, {d['detector']})")
        
        print("\n" + "=" * 60)


def main():
    """Run backtest from command line."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Run detector backtest')
    parser.add_argument(
        '--version',
        action='version',
        version=f'revenue-integrity-backtest {__version__}',
    )
    parser.add_argument('--days', type=int, default=30, help='Days to backtest')
    parser.add_argument('--baseline', type=int, default=168, help='Baseline periods')
    parser.add_argument('--step', type=int, default=1, help='Step size in hours')
    parser.add_argument('--output', type=str, help='Output JSON file')
    
    args = parser.parse_args()
    
    with Backtester() as backtester:
        results = backtester.run_backtest(
            days=args.days,
            baseline_periods=args.baseline,
            step_hours=args.step
        )
        
        backtester.print_results(results)
        
        if args.output:
            with open(args.output, 'w') as f:
                json.dump(results, f, indent=2, default=str)
            print(f"\nResults saved to {args.output}")


if __name__ == '__main__':
    main()

