from sqlalchemy import create_engine
from urllib.parse import quote_plus
from dotenv import load_dotenv
import os
from pymongo import MongoClient

load_dotenv()  # Load environment variables from .env file

# Try DATABASE_URL first (for cloud deployment like Render)
MONGO_URI = os.getenv("MONGO_URI")
PostgreSQL_URI = os.getenv("PostgreSQL_URI")

def construct_postgresql_uri():
    """Construct PostgreSQL URI from individual components if not provided."""
    
    host = os.getenv("host")
    database = os.getenv("database")
    user = os.getenv("user")
    password = os.getenv("password")
    port = os.getenv("port")

    # Validate that all required env vars are loaded
    if not all([host, database, user, password, port]):
        raise ValueError("Missing required environment variables in .env file: host, database, user, password, port")

    # Encode the password to handle special characters like @
    encoded_password = quote_plus(password)
    PostgreSQL_URI = f'postgresql+psycopg2://{user}:{encoded_password}@{host}:{port}/{database}'
    return PostgreSQL_URI

if not PostgreSQL_URI:
    PostgreSQL_URI = construct_postgresql_uri()

### connection using SQLAlchemy (if needed)
def make_sqlalchemy_db_connection():
    """Create a SQLAlchemy engine for the PostgreSQL database."""
    engine = None
    try:
        engine = create_engine(PostgreSQL_URI)
        print("SQLAlchemy engine created successfully")
    except Exception as e:
        print(f"Error: {e}")
    return engine

def get_mongo_client() -> MongoClient:
    print("Connecting to MongoDB...")
    print(f"MONGO_URI: {MONGO_URI}")
    return MongoClient(MONGO_URI)
