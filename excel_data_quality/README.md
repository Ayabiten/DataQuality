# Excel Data Quality Module

A specialized auditing engine for Microsoft Excel files (.xlsx, .xls), now upgraded with the toolkit's "Deep Profiling" capabilities.

## Components

### 1. `excel_quality_audit.py`
Inherits from `BaseQualityAudit`. It performs a full quality scan of every sheet in a workbook:
- **Sheet-Level Profiling**: Every sheet is audited for data patterns, outliers, and quality scores.
- **Deep Format Detection**: Detects Emails, Phones, URLs, etc., within each sheet's columns.
- **Multi-Sheet Reporting**: Generates a comprehensive Excel report where each audited sheet gets its own summary and deep-analysis tab.

### 2. `verify_excel_audit.py`
A test script for validating the Excel audit functionality.

## Usage

Recommended usage via the [Unified CLI](../main.py):
```bash
python main.py my_workbook.xlsx --visualize
```
