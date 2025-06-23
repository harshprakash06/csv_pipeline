# DuckDB-based CSV Search Pipeline

A scalable and efficient data processing pipeline built in Python for high-performance CSV data manipulation and searching.

## 🚀 Overview

This project provides a complete data processing pipeline designed to:

- Convert large CSV files to optimized Parquet format
- Load and index the data into DuckDB
- Perform high-speed searches using indexed fields
- Export filtered results in multiple formats

## 📋 Dependencies

- Python 3.8+
- `duckdb`
- `pandas`
- `pyarrow`
- `tabulate`

## 🛠️ Installation

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 🔧 Pipeline Steps

### 1. CSV to Parquet Conversion

**Script:** `convert_to_parquet.py`

Converts large CSV files into Parquet format using chunked streaming and automatically creates directories and logs the process.

**Usage:**

```bash
python convert_to_parquet.py --csv data/sample.csv \
                              --parquet data/sample.parquet \
                              --chunk 1000000 \
                              --load yes
```

### 2. Data Loading and Indexing

**Script:** `load_and_index.py`

Loads Parquet file into DuckDB and creates a persistent table with optional recreation.

_Note: No CLI arguments are required for this script._

### 3. Search and Export

**Script:** `search_and_export.py`

Searches indexed or filterable fields using range or equality conditions and exports results in CSV, JSON, or Parquet.

**Usage Examples:**

Equality match:

```bash
python search_and_export.py --equals col_2=foo col_3=bar \
                            --columns col_2 col_3 col_4 \
                            --format csv
```

Range filter:

```bash
python search_and_export.py --range col_0 10 50 \
                            --columns col_0 col_1 \
                            --format json
```

### 4. Logging

Each script writes a timestamped log file under the `logs/` directory and also streams logs to the terminal.

## ⚠️ Error Handling

The pipeline includes comprehensive validations and exception handling for:

- Missing or invalid files
- Invalid search fields or columns
- DuckDB connection issues
- Empty query results

All errors are logged in the `logs/` directory for audit and debugging purposes.

## 📊 Logging Format

Each run produces a detailed log file containing:

- Timestamp of execution
- CLI arguments passed
- Number of rows processed
- Any warnings or errors encountered

**Sample Log Output:**

```
2025-06-23 14:30:12 [INFO] Running query: SELECT col_1 FROM data WHERE col_0 BETWEEN 10 AND 50
2025-06-23 14:30:12 [INFO] Found 3,920 rows.
2025-06-23 14:30:12 [INFO] Exported to data/search_output.csv
```

## 📁 Folder Structure

```
.
├── data/
│   ├── sample.csv
│   ├── sample.parquet
│   └── engine.duckdb
├── logs/
│   └── *.log
├── convert_to_parquet.py
├── load_and_index.py
├── search_and_export.py
└── requirements.txt
```

## 📝 Sample Schema

The test dataset contains 50 columns named `col_0` to `col_49`, with a mix of types:

- `col_0, col_1, col_2` are indexed (INTEGER)
- `col_3` to `col_20` are FLOAT
- `col_21` to `col_45` are VARCHAR
- `col_46` to `col_49` may include TEXT/BLOB data

## 👤 Author

**Harsh Prakash**  
Email: `harshprakash06@gmail.com`
