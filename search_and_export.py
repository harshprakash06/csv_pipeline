import duckdb
import argparse
import pandas as pd
import sys
from tabulate import tabulate

DB_PATH = 'data/engine.duckdb'

def validate_args(args, all_columns):
    if args.field not in args.indexed_cols:
        print(f"'{args.field}' is not a searchable field. Choose from: {args.indexed_cols}")
        sys.exit(1)

    if not args.columns:
        print("You must specify at least one column to extract.")
        sys.exit(1)

    for col in args.columns:
        if col not in all_columns:
            print(f"Invalid column: '{col}' — not found in either table.")
            sys.exit(1)

    if args.format not in ['csv', 'json', 'parquet']:
        print("Export format must be one of: csv, json, or parquet")
        sys.exit(1)

def get_columns(con, table):
    try:
        return [row[0] for row in con.execute(f"PRAGMA table_info('{table}')").fetchall()]
    except Exception:
        print(f"Failed to fetch columns for table '{table}'")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="Search and export from DuckDB (with join support)")
    parser.add_argument("--tables", required=True, nargs="+", help="One or two table names")
    parser.add_argument("--join-on", help="Common column to join on if using two tables")
    parser.add_argument("--field", required=True, help="Field to search on (must be indexed)")
    parser.add_argument("--value", required=True, help="Value to search for")
    parser.add_argument("--columns", required=True, nargs="+", help="Columns to extract")
    parser.add_argument("--format", default="csv", help="Export format: csv, json, or parquet")
    parser.add_argument("--output", default="data/search_output", help="Output file path without extension")

    args = parser.parse_args()

    if len(args.tables) > 2:
        print("Maximum of two tables supported.")
        sys.exit(1)

    try:
        con = duckdb.connect(DB_PATH)
    except Exception as e:
        print(f"Failed to connect to DuckDB: {e}")
        sys.exit(1)

    # Fetch columns for validation
    table_cols = {}
    for t in args.tables:
        table_cols[t] = get_columns(con, t)

    # Merge all columns and indexable ones
    all_columns = [f"{alias}.{col}" for alias, cols in zip(['t1', 't2'], table_cols.values()) for col in cols]
    indexed_cols = [col for col in all_columns if col.endswith('col_0') or col.endswith('col_1') or col.endswith('col_2')]
    args.indexed_cols = indexed_cols

    validate_args(args, all_columns)

    # Build SQL
    if len(args.tables) == 1:
        t1 = args.tables[0]
        query = f"""
        SELECT {', '.join(args.columns)}
        FROM {t1} AS t1
        WHERE t1.{args.field} = ?
        """
    elif len(args.tables) == 2:
        t1, t2 = args.tables
        if not args.join_on:
            print("Must provide --join-on when using two tables.")
            sys.exit(1)

        if args.join_on not in table_cols[t1] or args.join_on not in table_cols[t2]:
            print(f"Join column '{args.join_on}' must exist in both tables.")
            sys.exit(1)

        query = f"""
        SELECT {', '.join(args.columns)}
        FROM {t1} AS t1
        JOIN {t2} AS t2 ON t1.{args.join_on} = t2.{args.join_on}
        WHERE t1.{args.field} = ?
        """

    print(f"Running query on field '{args.field}' = '{args.value}'...")
    try:
        df = con.execute(query, (args.value,)).fetchdf()
    except Exception as e:
        print(f"Query failed: {e}")
        sys.exit(1)

    if df.empty:
        print("No matching records found.")
        return

    print(f"Found {len(df):,} rows.")
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

        print(f"Exported to {output_file}")
    except Exception as e:
        print(f"Failed to export: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
