from src.analytics.create_tables import create_tables_if_not_exists

def run_analytics():
    try:
        # Create necessary tables if they do not exist
        create_tables_if_not_exists()
    
    except Exception as e:
        print(f"An error occurred while running analytics: {e}")