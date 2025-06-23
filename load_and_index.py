import duckdb
import os
import sys
import logging
from datetime import datetime

# Configuration
DB_PATH = 'data/engine.duckdb'
PARQUET_FILE = 'data/sample.parquet'
TABLE_NAME = 'data'
RECREATE_TABLE = True  # Change to False to avoid reloading if already exists

# Setup logger
os.makedirs("logs", exist_ok=True)
log_filename = datetime.now().strftime("logs/load_duckdb_%Y-%m-%d_%H-%M-%S.log")

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

def table_exists(con, table_name):
    try:
        result = con.execute(f"""
            SELECT COUNT(*) > 0
            FROM information_schema.tables
            WHERE table_name = '{table_name}'
        """).fetchone()[0]
        return bool(result)
    except Exception as e:
        logger.warning(f"Error checking table existence: {e}")
        return False

def load_parquet_into_duckdb():
    # Validate paths
    if not os.path.exists(PARQUET_FILE):
        logger.error(f"Parquet file not found at: {PARQUET_FILE}")
        sys.exit(1)

    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    try:
        con = duckdb.connect(DB_PATH)
        logger.info(f"Connected to DuckDB database at: {DB_PATH}")

        if table_exists(con, TABLE_NAME):
            if RECREATE_TABLE:
                logger.info(f"Dropping and recreating table '{TABLE_NAME}'...")
                con.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
            else:
                logger.info(f"Table '{TABLE_NAME}' already exists. Skipping reload.")
                row_count = con.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
                logger.info(f"Row count: {row_count:,}")
                return

        logger.info(f"Loading data from: {PARQUET_FILE}")
        con.execute(f"""
            CREATE TABLE {TABLE_NAME} AS 
            SELECT * FROM '{PARQUET_FILE}';
        """)

        row_count = con.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
        logger.info(f"Table '{TABLE_NAME}' created successfully with {row_count:,} rows.")

    except Exception as e:
        logger.error(f" Failed to load table: {e}")
        sys.exit(1)

if __name__ == "__main__":
    load_parquet_into_duckdb()
