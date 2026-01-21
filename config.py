import os
from dotenv import load_dotenv
load_dotenv()  # Load environment variables from .env file

configs = {
    # directories
    'BOOTSTRAP_DIR': 'data/bootstrap', 
    
    # Database configurations
    "MONGO_URI": os.getenv("MONGO_URI"),
    "MONGO_DB": os.getenv("MONGO_DB"),
    "PostgreSQL_URI": os.getenv("PostgreSQL_URI"),
    "host": os.getenv("host"),
    "database": os.getenv("database"),
    "user": os.getenv("user"),
    "password": os.getenv("password"),
    "port": os.getenv("port")
}