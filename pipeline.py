import argparse
from src.bootstrap_loader import bootstrap_load
from src.live_event_loader import live_event_loader
from src.bootstrap_loader import check_bootstrap_loaded
from pathlib import Path
from datetime import datetime

"""
    # Load both bootstrap and live events (default)
    python src/main.py

    # Skip bootstrap, only load live events
    python src/main.py --skip-bootstrap

    # Force reload bootstrap even if already loaded
    python src/main.py --force-rerun-bootstrap

    # Load only bootstrap data, skip live events
    python src/main.py --bootstrap-only

    # Specify a different date for live events
    python src/main.py --date 2026-01-18

    # Change batch size
    python src/main.py --batch-size 2000

    # Combine multiple arguments
    python src/main.py --force-rerun-bootstrap --batch-size 1000 --date 2026-01-19

    # View help
    python src/main.py --help
"""

BOOTSTRAP_DIR = 'data/bootstrap'


def main():
    """Main orchestration function with command-line argument support."""
    parser = argparse.ArgumentParser(
        description='CommercePulse Data Pipeline: Load bootstrap and live event data into MongoDB and process, then store into PostgreSQL.'
    )
    
    parser.add_argument(
        '--skip-bootstrap',
        action='store_true',
        help='Skip bootstrap data loading (only load live events)'
    )
    
    parser.add_argument(
        '--force-rerun-bootstrap',
        action='store_true',
        help='Force re-run bootstrap loading even if already loaded'
    )
    
    parser.add_argument(
        '--date',
        type=str,
        help='Date for live events in YYYY-MM-DD format (defaults to today)'
    )
    
    parser.add_argument(
        '--batch-size',
        type=int,
        default=500,
        help='Batch size for MongoDB inserts (default: 500)'
    )
    
    parser.add_argument(
        '--bootstrap-only',
        action='store_true',
        help='Load only bootstrap data, skip live events'
    )
    
    args = parser.parse_args()
    
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
        print("Pipeline execution completed successfully!")
        print("="*60)
    
    except Exception as e:
        print(f"\n Error during pipeline execution: {e}")
        raise


if __name__ == "__pipeline__":
    main()