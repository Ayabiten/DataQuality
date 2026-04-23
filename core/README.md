# Core Module

The `core/` directory contains the foundational components of the Data Quality Toolkit. It provides the shared logic, models, and utilities used by all format-specific audit modules.

## Components

### 1. `base_audit.py` (`BaseQualityAudit`)
The heart of the toolkit's logic. This abstract base class provides:
- **Pattern Detection**: Shared regex-based detection for Email, Phone, URL, IPv4, and Credit Cards.
- **Deep Column Profiling**: Standardized logic for calculating nulls, unique values, statistical outliers, and naming consistency.
- **Fuzzy Header Matching**: Suggests corrections for column names using the Levenshtein distance algorithm.
- **Quality Scoring**: A unified formula for calculating the overall "Data Health Score" (0-100).

### 2. `config.py`
The central configuration file. It stores:
- Standardized regex patterns.
- Global placeholder/dummy value lists (e.g., 'n/a', '999').
- Quality score thresholds and visualization settings.

### 3. `models.py`
Standardized Data Models (`AuditSummary` and `ColumnProfile`) to ensure that all audits return a consistent data structure, regardless of the source file format.

### 4. `visualizer.py` (`DataQualityVisualizer`)
A premium visualization utility that generates:
- **Null Heatmaps**: Visual density of missing data.
- **Type Distributions**: Pie charts of detected data types.
- **Quality Summaries**: High-level bar charts of overall health metrics.

## Why a Core Layer?
By centralizing this logic, we ensure that an audit run on a JSON file provides the exact same depth of insight as an audit run on a CSV or Excel file, ensuring cross-format consistency.
