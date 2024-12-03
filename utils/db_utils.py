import psycopg2

DB_CONFIG = {
    "dbname": "FP_DB",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": "5432",
}

def get_db_connection():
    """Create and return a database connection."""
    return psycopg2.connect(**DB_CONFIG)
