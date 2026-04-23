# 🛡️ DataQuality Audit Toolkit

A professional, modular, and robust toolkit designed to **analyze, heal, and monitor** the quality of various data formats (CSV, Excel, JSON). It bridges the gap between messy raw data and structured analytical storage, ensuring your data pipelines remain reliable and your insights accurate.

---

## 🏗️ Project Architecture

This toolkit is built on a layered architecture that handles everything from raw file ingestion to premium visual reporting.

### 1. Unified Audit Layer
*   **[`main.py`](./main.py)**: The central **Command Center**. Auto-detects file types and orchestrates the entire audit, reporting, and visualization workflow.
*   **[`core`](./core/)**: The **Brain** of the toolkit. Contains the `BaseQualityAudit` engine which ensures consistent "Deep Profiling" (Format detection, Placeholder identification, Scoring) across all formats.

### 2. Format-Specific Modules
*   **[`csv_data_quality`](./csv_data_quality/)**: Specialized logic for CSVs, featuring the `RobustCSVReader` for healing unquoted multiline cells and inconsistent columns. Supports **chunked streaming**.
*   **[`excel_data_quality`](./excel_data_quality/)**: Sheet-aware auditing for Microsoft Excel workbooks with deep profiling for every sheet.
*   **[`json_data_quality`](./json_data_quality/)**: Schema validation and deep-audit for semi-structured JSON data via flattening.

### 3. Data Storage & Schema Layer
*   **[`database`](./database/)**: The `DataModel` class automates the ingestion of complex data into SQLite. Features **recursive flattening**, **list exploding**, and **explicit transaction management**.

### 4. Observability & Logging
*   **[`dq_logging`](./dq_logging/)**: A centralized logging system that tracks every error, HTTP request failure, and structural anomaly.

---

## 🚀 Key Features

-   ✅ **Unified CLI**: Audit any file with a single command.
-   📊 **Premium Visualizations**: Automated generation of Null Heatmaps, Type Distributions, and Quality Bar Charts.
-   ⚡ **Scalable Ingestion**: Refactored for streaming to handle multi-gigabyte files efficiently.
-   🧠 **Deep Data Profiling**: Shared detection engine for Emails, URLs, Phones, IPv4, and Credit Cards across all formats.
-   🛡️ **Data Integrity**: Explicit database transactions ensure that ingestion into SQLite is atomic and safe.

---

## 📂 Directory Structure

```text
DataQuality/
├── main.py                 # UNIFIED CLI ENTRY POINT
├── 📁 core/                # SHARED ENGINE (Scoring, Visuals, Patterns)
├── 📁 csv_data_quality/    # CSV Specialized logic & Robust Reader
├── 📁 database/             # Relational storage (DataModel)
├── 📁 dq_logging/           # Centralized logging & error tracking
├── 📁 excel_data_quality/   # Excel Specialized logic
├── 📁 json_data_quality/    # JSON Specialized logic
└── 📁 test_files/           # Edge-case datasets
```

---

## 🛠️ Technology Stack

-   **Language**: Python 3.10+
-   **Core**: `pandas`, `numpy`, `sqlite3`
-   **Visualization**: `matplotlib`, `seaborn`
-   **Utilities**: `chardet` (encoding), `openpyxl` (excel), `thefuzz` (fuzzy matching)

---

## 📖 Getting Started

1.  **Install Dependencies**:
    ```bash
    pip install -r requirements.txt
    ```
2.  **Run a Unified Audit**:
    ```bash
    python main.py path/to/your/data.csv --visualize
    ```
3.  **Check Results**: All reports and charts will be generated in the `audit_reports/` directory.

---
*Created by Antigravity - Advanced Agentic Coding Assistant*
