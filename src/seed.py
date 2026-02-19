#!/usr/bin/env python3
"""
Seed Generator for Revenue Integrity Anomaly Detection System

Generates realistic transactional data with controlled anomaly injection
for testing and demonstration purposes.

Anomaly scenarios injected:
1. Missing 'purchase' events (silent pipeline break)
2. Sudden drop in 'checkout_complete' events
3. Revenue spike (potential fraud or pricing bug)
4. Revenue zeroing (payment gateway failure)
5. Missing event types entirely
6. Abnormal conversion rates
"""

import os
import random
import uuid
import json
from datetime import datetime, timedelta
from decimal import Decimal
import psycopg2
from psycopg2.extras import execute_values

# Configuration
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://analyst:analyst_secure_pw@localhost:5432/revenue_integrity')

# Time configuration
DAYS_OF_DATA = 30
HOURS_PER_DAY = 24

# Traffic patterns (hourly multipliers, 0-23 hours)
HOURLY_TRAFFIC_PATTERN = [
    0.3, 0.2, 0.15, 0.1, 0.1, 0.15,   # 0-5 AM (low traffic)
    0.3, 0.5, 0.7, 0.9, 1.0, 1.1,      # 6-11 AM (ramping up)
    1.0, 0.9, 0.85, 0.9, 1.0, 1.2,     # 12-5 PM (steady)
    1.3, 1.4, 1.3, 1.1, 0.8, 0.5       # 6-11 PM (evening peak, then decline)
]

# Base metrics per hour (will be multiplied by traffic pattern)
BASE_SESSIONS_PER_HOUR = 100
BASE_PAGE_VIEWS_PER_SESSION = 4
BASE_ADD_TO_CART_RATE = 0.25
BASE_CHECKOUT_START_RATE = 0.60  # Of those who add to cart
BASE_CHECKOUT_COMPLETE_RATE = 0.80  # Of those who start checkout
BASE_PURCHASE_RATE = 0.95  # Of those who complete checkout

# Price distribution
MIN_ORDER_VALUE = 10.00
MAX_ORDER_VALUE = 500.00
MEAN_ORDER_VALUE = 75.00

# Anomaly injection configuration
ANOMALY_CONFIG = {
    'missing_purchase_events': {
        'enabled': True,
        'start_hour_offset': -48,  # 48 hours ago
        'duration_hours': 3,
        'drop_rate': 0.95  # 95% of purchase events missing
    },
    'checkout_complete_drop': {
        'enabled': True,
        'start_hour_offset': -24,  # 24 hours ago
        'duration_hours': 2,
        'drop_rate': 0.70  # 70% drop in checkout_complete
    },
    'revenue_spike': {
        'enabled': True,
        'start_hour_offset': -12,  # 12 hours ago
        'duration_hours': 1,
        'multiplier': 5.0  # 5x normal revenue
    },
    'revenue_zero': {
        'enabled': True,
        'start_hour_offset': -6,  # 6 hours ago
        'duration_hours': 1,
        'zero_rate': 1.0  # All purchases have $0 amount
    },
    'missing_event_type': {
        'enabled': True,
        'start_hour_offset': -4,  # 4 hours ago
        'duration_hours': 2,
        'missing_type': 'add_to_cart'  # No add_to_cart events
    }
}


def get_connection():
    """Create database connection."""
    return psycopg2.connect(DATABASE_URL)


def generate_order_amount():
    """Generate realistic order amount with log-normal distribution."""
    # Log-normal distribution for realistic price spread
    amount = random.lognormvariate(4.0, 0.8)
    amount = max(MIN_ORDER_VALUE, min(MAX_ORDER_VALUE, amount))
    return round(amount, 2)


def generate_session_events(session_id: str, user_id: str, base_time: datetime,
                            anomaly_state: dict) -> tuple:
    """
    Generate events for a single user session following the funnel.
    Returns (events_list, order_dict or None)
    """
    events = []
    order = None
    
    # Generate page views (1-8 per session)
    num_page_views = random.randint(1, 8)
    current_time = base_time
    
    for i in range(num_page_views):
        events.append({
            'event_time': current_time,
            'user_id': user_id,
            'session_id': session_id,
            'event_type': 'page_view',
            'amount': 0,
            'order_id': None,
            'metadata_json': json.dumps({'page_number': i + 1})
        })
        current_time += timedelta(seconds=random.randint(10, 120))
    
    # Product view (70% chance after page views)
    if random.random() < 0.70:
        events.append({
            'event_time': current_time,
            'user_id': user_id,
            'session_id': session_id,
            'event_type': 'product_view',
            'amount': 0,
            'order_id': None,
            'metadata_json': json.dumps({'product_id': f'PROD-{random.randint(1000, 9999)}'})
        })
        current_time += timedelta(seconds=random.randint(30, 180))
    
    # Add to cart
    add_to_cart = random.random() < BASE_ADD_TO_CART_RATE
    
    # Check for missing_event_type anomaly
    if anomaly_state.get('missing_add_to_cart'):
        add_to_cart = False
    
    if add_to_cart:
        events.append({
            'event_time': current_time,
            'user_id': user_id,
            'session_id': session_id,
            'event_type': 'add_to_cart',
            'amount': 0,
            'order_id': None,
            'metadata_json': json.dumps({'items_count': random.randint(1, 5)})
        })
        current_time += timedelta(seconds=random.randint(60, 300))
        
        # Checkout start
        if random.random() < BASE_CHECKOUT_START_RATE:
            order_id = f'ORD-{uuid.uuid4().hex[:12].upper()}'
            
            events.append({
                'event_time': current_time,
                'user_id': user_id,
                'session_id': session_id,
                'event_type': 'checkout_start',
                'amount': 0,
                'order_id': order_id,
                'metadata_json': json.dumps({})
            })
            current_time += timedelta(seconds=random.randint(60, 180))
            
            # Checkout complete
            checkout_complete_rate = BASE_CHECKOUT_COMPLETE_RATE
            if anomaly_state.get('checkout_complete_drop'):
                checkout_complete_rate *= (1 - ANOMALY_CONFIG['checkout_complete_drop']['drop_rate'])
            
            if random.random() < checkout_complete_rate:
                events.append({
                    'event_time': current_time,
                    'user_id': user_id,
                    'session_id': session_id,
                    'event_type': 'checkout_complete',
                    'amount': 0,
                    'order_id': order_id,
                    'metadata_json': json.dumps({})
                })
                current_time += timedelta(seconds=random.randint(5, 30))
                
                # Payment initiated
                events.append({
                    'event_time': current_time,
                    'user_id': user_id,
                    'session_id': session_id,
                    'event_type': 'payment_initiated',
                    'amount': 0,
                    'order_id': order_id,
                    'metadata_json': json.dumps({})
                })
                current_time += timedelta(seconds=random.randint(2, 10))
                
                # Payment success (95% success rate)
                if random.random() < 0.95:
                    # Generate order amount
                    amount = generate_order_amount()
                    
                    # Apply revenue anomalies
                    if anomaly_state.get('revenue_spike'):
                        amount *= ANOMALY_CONFIG['revenue_spike']['multiplier']
                    if anomaly_state.get('revenue_zero'):
                        amount = 0.0
                    
                    events.append({
                        'event_time': current_time,
                        'user_id': user_id,
                        'session_id': session_id,
                        'event_type': 'payment_success',
                        'amount': amount,
                        'order_id': order_id,
                        'metadata_json': json.dumps({})
                    })
                    current_time += timedelta(seconds=random.randint(1, 5))
                    
                    # Purchase event
                    should_create_purchase = random.random() < BASE_PURCHASE_RATE
                    if anomaly_state.get('missing_purchase'):
                        should_create_purchase = random.random() > ANOMALY_CONFIG['missing_purchase_events']['drop_rate']
                    
                    if should_create_purchase:
                        events.append({
                            'event_time': current_time,
                            'user_id': user_id,
                            'session_id': session_id,
                            'event_type': 'purchase',
                            'amount': amount,
                            'order_id': order_id,
                            'metadata_json': json.dumps({})
                        })
                        
                        # Create order record
                        order = {
                            'order_id': order_id,
                            'user_id': user_id,
                            'session_id': session_id,
                            'created_at': current_time,
                            'amount': amount,
                            'currency': 'USD',
                            'status': 'completed',
                            'items_count': random.randint(1, 5),
                            'metadata_json': json.dumps({})
                        }
                else:
                    # Payment failed
                    events.append({
                        'event_time': current_time,
                        'user_id': user_id,
                        'session_id': session_id,
                        'event_type': 'payment_failed',
                        'amount': 0,
                        'order_id': order_id,
                        'metadata_json': json.dumps({'reason': 'card_declined'})
                    })
    
    return events, order


def get_anomaly_state(current_hour: datetime) -> dict:
    """Determine which anomalies are active for the given hour."""
    now = datetime.now()
    state = {}
    
    for anomaly_name, config in ANOMALY_CONFIG.items():
        if not config['enabled']:
            continue
            
        anomaly_start = now + timedelta(hours=config['start_hour_offset'])
        anomaly_end = anomaly_start + timedelta(hours=config['duration_hours'])
        
        if anomaly_start <= current_hour < anomaly_end:
            if anomaly_name == 'missing_purchase_events':
                state['missing_purchase'] = True
            elif anomaly_name == 'checkout_complete_drop':
                state['checkout_complete_drop'] = True
            elif anomaly_name == 'revenue_spike':
                state['revenue_spike'] = True
            elif anomaly_name == 'revenue_zero':
                state['revenue_zero'] = True
            elif anomaly_name == 'missing_event_type':
                state['missing_add_to_cart'] = True
    
    return state


def generate_hour_data(hour_start: datetime) -> tuple:
    """Generate all data for a single hour."""
    all_events = []
    all_orders = []
    
    # Get traffic multiplier for this hour
    hour_of_day = hour_start.hour
    traffic_multiplier = HOURLY_TRAFFIC_PATTERN[hour_of_day]
    
    # Add some daily variation (weekends have different patterns)
    day_of_week = hour_start.weekday()
    if day_of_week >= 5:  # Weekend
        traffic_multiplier *= 0.7
    
    # Add random noise
    traffic_multiplier *= random.uniform(0.85, 1.15)
    
    # Calculate sessions for this hour
    num_sessions = int(BASE_SESSIONS_PER_HOUR * traffic_multiplier)
    
    # Get anomaly state for this hour
    anomaly_state = get_anomaly_state(hour_start)
    
    # Generate sessions
    for _ in range(num_sessions):
        session_id = f'SES-{uuid.uuid4().hex[:16]}'
        user_id = f'USR-{uuid.uuid4().hex[:12]}'
        
        # Random start time within the hour
        session_start = hour_start + timedelta(
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59)
        )
        
        events, order = generate_session_events(
            session_id, user_id, session_start, anomaly_state
        )
        
        all_events.extend(events)
        if order:
            all_orders.append(order)
    
    return all_events, all_orders


def insert_events(conn, events: list):
    """Bulk insert events into database."""
    if not events:
        return 0
    
    with conn.cursor() as cur:
        values = [
            (
                e['event_time'], e['user_id'], e['session_id'],
                e['event_type'], e['amount'], 'USD',
                e['order_id'], e['metadata_json']
            )
            for e in events
        ]
        
        execute_values(
            cur,
            """
            INSERT INTO events (event_time, user_id, session_id, event_type, 
                               amount, currency, order_id, metadata_json)
            VALUES %s
            """,
            values,
            page_size=1000
        )
    
    return len(events)


def insert_orders(conn, orders: list):
    """Bulk insert orders into database."""
    if not orders:
        return 0
    
    with conn.cursor() as cur:
        values = [
            (
                o['order_id'], o['user_id'], o['session_id'],
                o['created_at'], o['amount'], o['currency'],
                o['status'], o['items_count'], o['metadata_json']
            )
            for o in orders
        ]
        
        execute_values(
            cur,
            """
            INSERT INTO orders (order_id, user_id, session_id, created_at, 
                               amount, currency, status, items_count, metadata_json)
            VALUES %s
            ON CONFLICT (order_id) DO NOTHING
            """,
            values,
            page_size=1000
        )
    
    return len(orders)


def clear_existing_data(conn):
    """Clear existing data from tables."""
    with conn.cursor() as cur:
        cur.execute("TRUNCATE events, orders, anomalies, anomaly_incidents, metric_baselines, monitor_runs CASCADE")
    conn.commit()
    print("Cleared existing data")


def main():
    """Main seed generation function."""
    print("=" * 60)
    print("Revenue Integrity Anomaly Detection - Seed Generator")
    print("=" * 60)
    
    conn = get_connection()
    
    # Clear existing data
    clear_existing_data(conn)
    
    # Generate data for each hour
    now = datetime.now().replace(minute=0, second=0, microsecond=0)
    start_time = now - timedelta(days=DAYS_OF_DATA)
    
    total_events = 0
    total_orders = 0
    
    print(f"\nGenerating {DAYS_OF_DATA} days of data ({DAYS_OF_DATA * 24} hours)")
    print(f"Start: {start_time}")
    print(f"End: {now}")
    print()
    
    # Print anomaly schedule
    print("Injected Anomalies:")
    print("-" * 40)
    for name, config in ANOMALY_CONFIG.items():
        if config['enabled']:
            anomaly_start = now + timedelta(hours=config['start_hour_offset'])
            anomaly_end = anomaly_start + timedelta(hours=config['duration_hours'])
            print(f"  {name}:")
            print(f"    Start: {anomaly_start}")
            print(f"    End: {anomaly_end}")
    print()
    
    current_hour = start_time
    hour_count = 0
    
    while current_hour < now:
        events, orders = generate_hour_data(current_hour)
        
        total_events += insert_events(conn, events)
        total_orders += insert_orders(conn, orders)
        
        hour_count += 1
        if hour_count % 24 == 0:
            conn.commit()
            days_done = hour_count // 24
            print(f"  Day {days_done}/{DAYS_OF_DATA} complete - Events: {total_events:,}, Orders: {total_orders:,}")
        
        current_hour += timedelta(hours=1)
    
    conn.commit()
    
    # Print summary
    print()
    print("=" * 60)
    print("Seed Generation Complete")
    print("=" * 60)
    print(f"Total Events: {total_events:,}")
    print(f"Total Orders: {total_orders:,}")
    print(f"Hours of Data: {hour_count}")
    
    # Print event distribution
    with conn.cursor() as cur:
        cur.execute("""
            SELECT event_type, COUNT(*) as cnt 
            FROM events 
            GROUP BY event_type 
            ORDER BY cnt DESC
        """)
        print("\nEvent Distribution:")
        print("-" * 40)
        for row in cur.fetchall():
            print(f"  {row[0]}: {row[1]:,}")
    
    conn.close()
    print("\nDone!")


if __name__ == '__main__':
    main()

