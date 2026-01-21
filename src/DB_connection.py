from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from pymongo import MongoClient
import pandas as pd
from typing import Dict, Any
from config import configs

# Try DATABASE_URL first, then construct from components if not available
MONGO_URI = configs["MONGO_URI"]
PostgreSQL_URI = configs["PostgreSQL_URI"]

def construct_postgresql_uri():
    """Construct PostgreSQL URI from individual components if not provided."""
    print("Configs data: ", configs)
    host = configs["host"]
    database = configs["database"]
    user = configs["user"]
    password = configs["password"]
    port = configs["port"]

    # Validate that all required env vars are loaded
    if not all([host, database, user, password, port]):
        raise ValueError("Missing required environment variables in .env file: host, database, user, password, port")

    # Encode the password to handle special characters like @
    encoded_password = quote_plus(password)
    PostgreSQL_URI = f'postgresql+psycopg2://{user}:{encoded_password}@{host}:{port}/{database}'
    return PostgreSQL_URI

# If PostgreSQL_URI is not set, construct it
URI = construct_postgresql_uri()
if not URI:
    URI = PostgreSQL_URI

### connection using SQLAlchemy for Postgres
def make_sqlalchemy_db_connection():
    """Create a SQLAlchemy engine for the PostgreSQL database."""
    engine = None
    try:
        engine = create_engine(URI)
        # Fallback to PostgreSQL_URI if engine creation fails with URI construction
        if not engine:
            engine = create_engine(PostgreSQL_URI)
        print("SQLAlchemy engine created successfully")
    except Exception as e:
        print(f"Error: {e}")
    return engine


def execute_postgre_query(query: str) -> pd.DataFrame | None:
    """Execute a SQL query and return the results as a DataFrame or None if the connection is not initialized or the query is not a fetch query."""
    connection_engine = make_sqlalchemy_db_connection()
    if not connection_engine:
        raise ValueError("Database connection engine is not initialized.")
    
    with connection_engine.connect() as connection:
        result = connection.execute(text(query))
        if query.strip().lower().startswith("select"):
            executed = pd.DataFrame(result.fetchall(), columns=result.keys())
            print("Query executed successfully.")
            return executed
        else:
            connection.commit()
            print("Query executed successfully.")
            return None
        

def get_mongo_client() -> MongoClient:
    print("Connecting to MongoDB...")
    print(f"MONGO_URI: {MONGO_URI}")
    return MongoClient(MONGO_URI)


# load data from MongoDB, for transformation and storing into tables for analytics
def load_from_mongoDB(query: Dict[str, Any] = {}, batch_size: int = 1000) -> pd.DataFrame:
    """
    Load events from MongoDB based on a query.

    Args:
        query: MongoDB query dictionary
        batch_size: Number of documents to fetch per batch
    Returns:
        DataFrame containing the events
    """
    client = get_mongo_client()
    db = client[configs["MONGO_DB"]]
    collection = db['events_raw']

    cursor = collection.find(query).batch_size(batch_size)
    return pd.DataFrame(list(cursor))