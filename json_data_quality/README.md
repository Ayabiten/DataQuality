# JSON Data Quality Module

A specialized auditing engine for semi-structured JSON data, upgraded with full deep-profiling support.

## Components

### 1. `json_quality_audit.py`
Inherits from `BaseQualityAudit`. It transforms complex JSON structures into flat analytical models:
- **Structural Depth Analysis**: Measures the maximum nesting depth of the JSON.
- **Auto-Flattening**: Uses `pd.json_normalize` to turn nested objects/lists into a flat table for profiling.
- **Deep Content Audit**: Applies the shared pattern detection engine to the flattened data.

### 2. `verify_json_audit.py`
A test script for validating the JSON audit functionality.

## Usage

Recommended usage via the [Unified CLI](../main.py):
```bash
python main.py my_data.json --visualize
```
