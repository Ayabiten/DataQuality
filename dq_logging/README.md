# Data Quality Logging Module

A standardized logging and error-tracking system designed specifically for data processing workflows and API integrations.

## Components

### 1. `models.py`
Contains the core logging infrastructure:
- **`BaseLog`**: The fundamental log structure with timestamps and levels.
- **`FileErrorLog`**: Specialized log for tracking data quality issues (structural, type, or schema errors) during file processing.
- **`RequestErrorLog`**: Specialized log for tracking HTTP request failures, capturing URLs, status codes, and payloads.
- **`DataQualityLogger`**: A central manager that collects logs, prints them to the console with formatting, saves them to `.log` files, and can even sync errors to a database table.

### 2. `verify_logging.py`
A verification script that demonstrates and tests the various logging scenarios (File errors vs Request errors).

## Key Features
- **Context Management**: Use `scenario_file` or `scenario_request` context managers to automatically capture and log unhandled exceptions.
- **Database Integration**: Sync your collected errors directly to a SQLite table for long-term tracking.
- **Pandas Compatibility**: Export your logs directly to a DataFrame for analysis.

## Usage

```python
from dq_logging import DataQualityLogger

with DataQualityLogger(log_name="AuditSession") as logger:
    try:
        # data processing logic
        pass
    except Exception as e:
        logger.log_exception(file_path="data.csv", error_type="Structural")
```
