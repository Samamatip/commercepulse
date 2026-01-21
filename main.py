import argparse
from src.pipeline import run_pipeline

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
    
    run_pipeline(args)
    
if __name__ == "__main__":
    main()