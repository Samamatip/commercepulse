from pathlib import Path
from datetime import datetime
from .bootstrap_loader import bootstrap_load, check_bootstrap_loaded
from .live_event_loader import live_event_loader
from config import configs
from src.analytics.run_analytics import run_analytics

BOOTSTRAP_DIR = configs['BOOTSTRAP_DIR']

def run_pipeline(args):
    # Determine live events file path
    if args.date:
        live_event_path = Path(f'data/live_events/{args.date}/events.jsonl')
    else:
        today = datetime.now().strftime('%Y-%m-%d')
        live_event_path = Path(f'data/live_events/{today}/events.jsonl')
    
    try:
        # Handle bootstrap loading
        if args.bootstrap_only or not args.skip_bootstrap:
            bootstrap_loaded = check_bootstrap_loaded()
            
            if (not bootstrap_loaded and not args.skip_bootstrap) or args.force_rerun_bootstrap:
                print("\n" + "="*60)
                print("Starting bootstrap load...")
                print("="*60)
                stats_bootstrap = bootstrap_load(BOOTSTRAP_DIR, args.batch_size)
                print(f"Bootstrap Load Stats: {stats_bootstrap}\n")
            else:
                print("\n✓ Bootstrap data already loaded. Skipping bootstrap load...\n")
        
        # Handle live events loading
        if not args.bootstrap_only:
            print("="*60)
            print("Starting live event load...")
            print("="*60)
            stats_live = live_event_loader(live_event_path, args.batch_size)
            print(f"Live Event Load Stats: {stats_live}\n")
        
        print("="*60)
        
        # Run analytics
        print("Running transformations and analytics...")
        run_analytics()
        
        print("Pipeline execution completed successfully!")
        print("="*60)
    
    except Exception as e:
        print(f"\n Error during pipeline execution: {e}")
        raise

