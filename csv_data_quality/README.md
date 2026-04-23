# CSV Data Quality Module

This module provides specialized logic for auditing CSV files, optimized for handling messy ingestion scenarios.

## Components

### 1. `csv_quality_audit.py`
Inherits from `BaseQualityAudit`. It combines the toolkit's shared deep-profiling engine with CSV-specific features:
- **Encoding & Delimiter Detection**: Automatic detection using `chardet` and `csv.Sniffer`.
- **Structural Integrity**: Checks for row/column mismatches before and after healing.
- **Trend Alerts**: Maintains a `quality_history.json` to alert you of sudden shifts in row counts or null spikes.

### 2. `robust_reader.py` (`RobustCSVReader`)
A high-performance ingestion utility that "heals" broken CSVs:
- **Multiline Stitching**: Merges unquoted cells that are split across multiple lines.
- **Streaming Support**: Supports a `chunksize` parameter to return a generator of DataFrames, allowing the processing of multi-gigabyte files with low memory usage.

### 3. `verify_csv_audit.py`
Verification script for testing the CSV-specific features.

## Usage

While you can use this module directly, it is recommended to use the [Unified CLI](../main.py) for the best reporting and visualization experience.
