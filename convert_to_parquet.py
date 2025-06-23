import pandas as pd
import os
import sys
import logging
from datetime import datetime
import argparse
import subprocess

# ─────────────────────────────
# Logging Setup
os.makedirs("logs", exist_ok=True)
log_filename = datetime.now().strftime("logs/convert_parquet_%Y-%m-%d_%H-%M-%S.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

# ─────────────────────────────
# CSV → Parquet Conversion
def convert_csv_to_parquet(csv_file, parquet_file, chunk_size):
    if not os.path.exists(csv_file):
        logger.error(f"CSV file not found: {csv_file}")
        sys.exit(1)

    logger.info(f"Reading CSV from: {csv_file}")
    logger.info(f"Converting to Parquet → {parquet_file} in chunks of {chunk_size:,} rows")

    try:
        first_chunk = True
        for chunk in pd.read_csv(csv_file, chunksize=chunk_size):
            chunk.to_parquet(parquet_file, index=False, engine='pyarrow', compression='snappy')
            logger.info(f"✅ Processed chunk with {len(chunk):,} rows")
            first_chunk = False

        logger.info(f"Conversion complete: {parquet_file}")
        return True

    except Exception as e:
        logger.error(f"Failed to convert: {e}")
        sys.exit(1)

# ─────────────────────────────
# Optional: Load to DuckDB
def auto_load_data():
    logger.info("Launching load_and_index.py...")
    try:
        subprocess.run([sys.executable, "load_and_index.py"], check=True)
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to load/index data: {e}")
        sys.exit(1)

# ─────────────────────────────
# CLI Entry
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert large CSV to Parquet and optionally load into DuckDB.")
    parser.add_argument('--csv', default='data/sample.csv', help="Path to the input CSV file")
    parser.add_argument('--parquet', default='data/sample.parquet', help="Path to the output Parquet file")
    parser.add_argument('--chunk', type=int, default=1_000_000, help="Chunk size for streaming conversion")
    parser.add_argument('--load', choices=['yes', 'no'], default='no', help="Whether to run load_and_index.py after conversion")

    args = parser.parse_args()

    success = convert_csv_to_parquet(args.csv, args.parquet, args.chunk)

    if success and args.load == 'yes':
        auto_load_data()
