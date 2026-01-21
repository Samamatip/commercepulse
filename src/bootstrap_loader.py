from dotenv import load_dotenv
import json
import hashlib
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any
from pymongo import UpdateOne
import os
from src.DB_connection import get_mongo_client
from src.utility import load_json_file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Event type mapping for historical files
EVENT_TYPE_MAPPING = {
    'orders_2023.json': 'historical_order',
    'payments_2023.json': 'historical_payment',
    'refunds_2023.json': 'historical_refund',
    'shipments_2023.json': 'historical_shipment'
}

# Function to generate deterministic event_id
def generate_event_id(event_type: str, payload: Dict[str, Any]) -> str:
    
    # Extract key identifier from payload based on event type
    if 'historical_order' in event_type:
        # Try different vendor field names for order ID
        order_id = payload.get('orderRef') or payload.get('order_id') or payload.get('order')['id'] or ''
        key = f"order_{order_id}"
    elif 'historical_payment' in event_type:
        # Combine order reference and transaction reference
        order_ref = payload.get('orderRef') or payload.get('order_id') or payload.get('order')
        tx_ref = payload.get('txRef') or payload.get('transaction_id') or payload.get('txn') or ''
        key = f"payment_{order_ref}_{tx_ref}"
    elif 'historical_refund' in event_type:
        order_ref = payload.get('orderRef') or payload.get('order_id') or payload.get('order')
        refund_id = payload.get('refundId') or payload.get('refund_id') or ''
        # Include amount to differentiate partial refunds on same order
        amount = payload.get('amount') or payload.get('refundAmount') or payload.get('amt') or 0
        key = f"refund_{order_ref}_{refund_id}_{amount}"
    elif 'historical_shipment' in event_type:
        order_ref = payload.get('orderRef') or payload.get('order_id') or payload.get('order')['id'] or ''
        tracking = payload.get('tracking') or payload.get('tracking_code') or ''
        key = f"shipment_{order_ref}_{tracking}"
    else:
        # Fallback: hash entire payload
        key = json.dumps(payload, sort_keys=True)
    
    # Create deterministic hash
    hash_input = f"{event_type}_{key}"
    return hashlib.sha1(hash_input.encode('utf-8')).hexdigest()

# Function to extract event timestamp
def extract_event_time(payload: Dict[str, Any], event_type: str) -> datetime:
    timestamp_str = None
    
    if 'historical_order' in event_type:
        timestamp_str = payload.get('created_at') or payload.get('created')
    
    elif 'historical_payment' in event_type:
        timestamp_str = payload.get('paidAt') or payload.get('paid_at')
        # Handle Unix timestamp for vendor_c
        if isinstance(payload.get('ts'), (int, float)):
            return datetime.fromtimestamp(payload['ts'])
    
    elif 'historical_refund' in event_type:
        timestamp_str = payload.get('refundedAt') or payload.get('refunded_at')
        if isinstance(payload.get('ts'), (int, float)):
            return datetime.fromtimestamp(payload['ts'])
    
    elif 'historical_shipment' in event_type:
        # Get latest update time from shipment history
        if 'updates' in payload and payload['updates']:
            timestamp_str = payload['updates'][-1].get('time')
        elif 'status_history' in payload and payload['status_history']:
            timestamp_str = payload['status_history'][-1].get('time')
        elif 'timeline' in payload and payload['timeline']:
            timestamp_str = payload['timeline'][-1].get('time')
    
    # Parse timestamp string
    if timestamp_str:
        # Try multiple datetime formats
        formats = [
            '%Y-%m-%dT%H:%M:%S',
            '%Y-%m-%dT%H:%M:%SZ',
            '%Y-%m-%dT%H:%M:%S.%f',
            '%Y/%m/%d %H:%M:%S',
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d %H:%M',
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(timestamp_str, fmt)
            except (ValueError, TypeError):
                continue
    
    # Fallback: use a default date in 2023 if parsing fails
    return datetime(2023, 1, 1, 0, 0, 0)

def detect_vendor(payload: Dict[str, Any], event_type: str) -> str:
    # Vendor detection logic based on field naming patterns
    
    if 'historical_order' in event_type:
        if 'orderRef' in payload:
            return 'vendor_a'
        elif 'order_id' in payload:
            return 'vendor_b'
        elif 'order' in payload and 'id' in payload['order']:
            return 'vendor_c'
        else:
            return 'unknown'
    
    elif 'historical_payment' in event_type:
        if 'orderRef' in payload and 'paidAt' in payload:
            return 'vendor_a'
        elif 'order_id' in payload and 'paid_at' in payload:
            return 'vendor_b'
        elif 'order' in payload and 'timestamp' in payload:
            return 'vendor_c'
    
    elif 'historical_refund' in event_type:
        if 'orderRef' in payload and 'refundedAt' in payload:
            return 'vendor_a'
        elif 'order_id' in payload and 'refunded_at' in payload:
            return 'vendor_b'
        elif 'order' in payload and 'ts' in payload:
            return 'vendor_c'
    
    elif 'historical_shipment' in event_type:
        if 'orderRef' in payload and 'updates' in payload:
            return 'vendor_a'
        elif 'order_id' in payload and 'status_history' in payload:
            return 'vendor_b'
        elif 'order' in payload and 'timeline' in payload:
            return 'vendor_c'
    return 'unknown'

# Function to wrap payload as event
def wrap_as_event(payload: Dict[str, Any], event_type: str) -> Dict[str, Any]:
    
    vendor = detect_vendor(payload, event_type)
    event_time = extract_event_time(payload, event_type)
    event_id = generate_event_id(event_type, payload)
    
    return {
        'event_id': event_id,
        'event_type': event_type,
        'event_time': event_time,
        'vendor': vendor,
        'payload': payload,
        'ingested_at': datetime.now(datetime.now().astimezone().tzinfo),
        '_bootstrapped': True  # Flag to identify historical data
    }
    

def bootstrap_load(bootstrap_dir, batch_size=500):
    
    bootstrap_path = Path(bootstrap_dir)
    
    if not bootstrap_path.exists():
        raise FileNotFoundError(f"Bootstrap directory not found: {bootstrap_dir}")
    
    # Get MongoDB connection
    client = get_mongo_client()
    db_name = os.getenv('MONGO_DB')
    db = client[db_name]
    collection = db['events_raw']
    
    # Create multiple indexes for performance on upserts and queries
    collection.create_index('event_id', unique=True)
    collection.create_index('event_type')
    collection.create_index('event_time')
    collection.create_index('ingested_at')
    
    print(f"Loading historical data from {bootstrap_dir}...")
    print(f"Target: MongoDB collection '{db_name}.events_raw'\n")
    
    total_processed = 0
    total_inserted = 0
    total_collisions = 0
    collision_details = []
    
    # Process each historical file by looping through the mapping
    for file_name, event_type in EVENT_TYPE_MAPPING.items():
        file_path = f"{bootstrap_path}/{file_name}"
        
        if not Path(file_path).exists():
            print(f"Skipping {file_name} (not found)")
            continue
        
        print(f"Processing {file_name}...")
        
        # Load records from file
        records = load_json_file(file_path)
        print(f"  Loaded {len(records)} records")
        
        # Wrap records as events and prepare bulk operations
        bulk_operations = []
        seen_event_ids = set()  # Track event_ids in current file
        file_collisions = 0
        
        for record in records:
            event_doc = wrap_as_event(record, event_type)
            event_id = event_doc['event_id']
            
            # Check for duplicate within current file
            if event_id in seen_event_ids:
                file_collisions += 1
                logging.warning(f"Duplicate event_id within {file_name}: {event_id}")
                collision_details.append({
                    'file': file_name,
                    'event_id': event_id,
                    'event_type': event_type
                })
            
            seen_event_ids.add(event_id)
            
            # Use UpdateOne with upsert to handle duplicates
            bulk_operations.append(
                UpdateOne(
                    {'event_id': event_doc['event_id']},
                    {'$set': event_doc},
                    upsert=True
                )
            )
            
            # Execute batch when batch_size reached
            if len(bulk_operations) >= batch_size:
                result = collection.bulk_write(bulk_operations, ordered=False)
                total_inserted += result.upserted_count + result.modified_count
                # Track collisions (modified_count means event_id already existed)
                if result.modified_count > 0:
                    total_collisions += result.modified_count
                    logging.info(f"Batch: {result.modified_count} event_id collisions (overwrites)")
                bulk_operations = []
        
        # Insert remaining records
        if bulk_operations:
            result = collection.bulk_write(bulk_operations, ordered=False)
            total_inserted += result.upserted_count + result.modified_count
            if result.modified_count > 0:
                total_collisions += result.modified_count
                logging.info(f"Final batch: {result.modified_count} event_id collisions (overwrites)")
        
        total_processed += len(records)
        if file_collisions > 0:
            print(f" --- Found {file_collisions} duplicate event_ids within {file_name} ---")
        print(f"  ✓ Completed {file_name}\n")
    
    print(f"\n{'='*60}")
    print("Bootstrap Load Summary:")
    print(f"  Total records processed: {total_processed:,}")
    print(f"  Total events in MongoDB: {collection.count_documents({}):,}")
    print(f"  Total duplicate event_ids: {total_collisions:,}")
    if collision_details:
        print(f"  Collision details: {len(collision_details)} duplicate(s) within files")
        logging.info(f"Full collision list: {collision_details[:10]}...")  # Log first 10
    print(f"{'='*60}\n")
    
    client.close()
    
    
    return {
        'total_processed': total_processed,
        'total_inserted': total_inserted,
        'total_collisions': total_collisions,
        'collision_details': collision_details
    }
    

def check_bootstrap_loaded() -> bool:
    
    # Check if bootstrap data has been loaded by verifying any document with _bootstrapped = True
    client = get_mongo_client()
    db_name = os.getenv('MONGO_DB')
    db = client[db_name]
    collection = db['events_raw']
    
    count = collection.count_documents({'_bootstrapped': True})
    client.close()
    
    return count > 0