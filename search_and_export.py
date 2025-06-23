import duckdb
import argparse
import pandas as pd
import sys
from tabulate import tabulate

# Constants
DB_PATH = 'data/engine.duckdb'
TABLE_NAME = 'data'
INDEXED_COLS = ['col_0', 'col_1', 'col_2']
ALL_COLS = [f'col_{i}' for i in range(50)]

def validate_args(args):
    if args.field not in INDEXED_COLS:
        print(f"'{args.field}' is not a searchable field. Choose from: {INDEXED_COLS}")
        sys.exit(1)

    if not args.columns:
        print("You must specify at least one column to extract.")
        sys.exit(1)

    for col in args.columns:
        if col not in ALL_COLS:
            print(f"Invalid column: '{col}' — available columns: col_0 to col_49")
            sys.exit(1)

    if args.format not in ['csv', 'json', 'parquet']:
        print("Export format must be one of: csv, json, or parquet")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="🔍 Search and export data from DuckDB")
    parser.add_argument("--field", required=True, help="Field to search on (must be indexed)")
    parser.add_argument("--value", required=True, help="Value to search for in the field")
    parser.add_argument("--columns", required=True, nargs="+", help="Columns to extract")
    parser.add_argument("--format", default="csv", help="Export format: csv, json, or parquet")
    parser.add_argument("--output", default="data/search_output", help="Output file path without extension")

    args = parser.parse_args()
    validate_args(args)

    try:
        con = duckdb.connect(DB_PATH)
    except Exception as e:
        print(f"Failed to connect to DuckDB: {e}")
        sys.exit(1)

    query = f"SELECT {', '.join(args.columns)} FROM {TABLE_NAME} WHERE {args.field} = ?"

    print(f"🔎 Running query on '{args.field}' = '{args.value}'...")
    try:
        df = con.execute(query, (args.value,)).fetchdf()
    except Exception as e:
        print(f"Query failed: {e}")
        sys.exit(1)

    if df.empty:
        print("No matching records found.")
        return

    print(f"Found {len(df):,} matching rows.")
    print("Preview of results:")
    print(tabulate(df.head(10), headers="keys", tablefmt="psql"))

    output_file = f"{args.output}.{args.format}"
    try:
        if args.format == "csv":
            df.to_csv(output_file, index=False)
        elif args.format == "json":
            df.to_json(output_file, orient="records", lines=True)
        elif args.format == "parquet":
            df.to_parquet(output_file, index=False)

        print(f"Exported {len(df):,} rows to {output_file}")
    except Exception as e:
        print(f"Failed to export: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
