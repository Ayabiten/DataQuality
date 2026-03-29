# Data Quality Audit Engine

A Python-based suite of high-precision diagnostic tools for auditing data integrity and compliance across common file formats.

## 🔐 Data Security

As a localized auditing tool, Data Quality Audit ensures data privacy by design:
- **Local-Only Processing**: No data is transmitted externally. All audits are conducted in strict isolation on the host machine.
- **Audit Logging**: Comprehensive execution logs provide traceability for every diagnostic run.

## 🛡️ Role-Based Utilization (RBAC)

The suite is designed for specific organizational functions:
- **Data Auditor**: Executes the full diagnostic suite to identify anomalies and missing values.
- **Data Scientist**: Leverages the distribution analysis modules to validate dataset readiness for modeling.
- **Compliance Officer**: Reviews generated PDF/HTML reports for regulatory verification.

---

## 🔍 Technical Deep Dive

### Multi-Format Parsing Engine
The engine utilizes specialized handlers for different structural complexities:
- **CSV Auditor**: High-speed schema validation using `pandas` and `numpy` for flat file integrity.
- **Excel Auditor**: Deep-scan of multi-sheet workbooks, including cell-level metadata and formula validation.
- **JSON Auditor**: Recursive validation of nested structures, ensuring schema consistency across semi-structured data.

### Diagnostic Capabilities
- **Uniqueness & Completeness**: Automated identification of duplicate records and sparse columns.
- **Distribution Analysis**: Statistical profiling to detect outliers and skewness.
- **Referential Integrity**: Checks for broken relations across multiple files (if applicable).

## 🚀 How to Run

1. **Prerequisites**: Ensure you have Python and Jupyter installed.
2. **Launch Notebooks**:
   ```bash
   jupyter notebook
   ```
3. **Select Module**: Open the relevant `.ipynb` for CSV, Excel, or JSON auditing and follow the cell-by-cell instructions.

---
*Data Quality: Clean data, clear insights.*
