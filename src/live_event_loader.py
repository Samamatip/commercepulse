from src.utility import load_json_file
from pathlib import Path
from typing import Dict, Any, List
from datetime import datetime
from src.DB_connection import get_mongo_client
from pymongo import UpdateOne
import logging
import os
from dotenv import load_dotenv
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def extract_live_events(file_path: Path) -> List[Dict[str, Any]]:
    """Extract live events from JSONL file."""
    if not file_path.exists():
        raise FileNotFoundError(f"Live event file not found: {file_path}")
    
    events = load_json_file(file_path)
    return events if events else []


def validate_event_structure(event: Dict[str, Any]) -> bool:
    
    required_fields = ['event_id', 'event_type', 'event_time', 'vendor', 'payload']
    
    for field in required_fields:
        if field not in event:
            logging.warning(f"Invalid event structure - missing field: {field}")
            return False
    
    return True


def load_events_to_mongo(events: List[Dict[str, Any]], batch_size: int = 1000) -> Dict[str, int]:
    
    if not events:
        logging.warning("No events to load")
        return {'inserted': 0, 'updated': 0, 'skipped': 0, 'duplicates': 0}
    
    # Get MongoDB connection
    client = get_mongo_client()
    db_name = os.getenv('MONGO_DB')
    db = client[db_name]
    collection = db['events_raw']
    
    # Ensure indexes exist
    collection.create_index('event_id', unique=True)
    collection.create_index('event_type')
    collection.create_index('event_time')
    collection.create_index('ingested_at')
    
    stats = {
        'inserted': 0,
        'updated': 0,
        'skipped': 0,
        'duplicates': 0
    }
    
    bulk_operations = []
    seen_event_ids = set()
    
    for event in events:
        # Validate event structure
        if not validate_event_structure(event):
            stats['skipped'] += 1
            continue
        
        event_id = event['event_id']
        
        # Check for duplicates within the batch
        if event_id in seen_event_ids:
            stats['duplicates'] += 1
            logging.warning(f"Duplicate event_id in batch: {event_id}")
            continue
        
        seen_event_ids.add(event_id)
        
        # Add ingested_at timestamp
        if 'ingested_at' not in event:
            event['ingested_at'] = datetime.now()
        
        # Add _bootstrapped flag (false for live events)
        if '_bootstrapped' not in event:
            event['_bootstrapped'] = False
        
        # Prepare upsert operation
        bulk_operations.append(
            UpdateOne(
                {'event_id': event_id},
                {'$set': event},
                upsert=True
            )
        )
        
        # Execute batch when batch_size reached
        if len(bulk_operations) >= batch_size:
            try:
                result = collection.bulk_write(bulk_operations, ordered=False)
                stats['inserted'] += result.upserted_count
                stats['updated'] += result.modified_count
                
                if result.modified_count > 0:
                    logging.info(f"Batch: {result.upserted_count} new, {result.modified_count} updated")
                
                bulk_operations = []
            except Exception as e:
                logging.error(f"Batch write error: {e}")
                bulk_operations = []
    
    # Insert remaining records
    if bulk_operations:
        try:
            result = collection.bulk_write(bulk_operations, ordered=False)
            stats['inserted'] += result.upserted_count
            stats['updated'] += result.modified_count
            
            if result.modified_count > 0:
                logging.info(f"Final batch: {result.upserted_count} new, {result.modified_count} updated")
        except Exception as e:
            logging.error(f"Final batch write error: {e}")
    
    client.close()
    return stats


def live_event_loader(file_path: Path, batch_size: int = 1000) -> Dict[str, int]:
    
    if not file_path.exists():
        logging.error(f"File not found: {file_path}")
        return {'inserted': 0, 'updated': 0, 'skipped': 0, 'duplicates': 0}
    
    print(f"Loading live events from {file_path}...")
    
    # Extract events from file
    events = extract_live_events(file_path)
    print(f"Extracted {len(events)} events from file")
    
    # Load to MongoDB
    stats = load_events_to_mongo(events, batch_size)
    
    # Print summary
    print(f"\n{'='*60}")
    print("Live Event Load Summary:")
    print(f"  Total events processed: {len(events):,}")
    print(f"  New events inserted: {stats['inserted']:,}")
    print(f"  Existing events updated: {stats['updated']:,}")
    print(f"  Invalid/skipped events: {stats['skipped']:,}")
    print(f"  Duplicate event_ids in batch: {stats['duplicates']:,}")
    print(f"{'='*60}\n")
    
    return stats
    