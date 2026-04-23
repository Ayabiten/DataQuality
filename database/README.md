# Database Module

A dynamic, schema-agnostic interface for SQLite, optimized for ingesting complex data quality findings.

## Components

### 1. `Model.py` (`DataModel`)
A high-performance database manager featuring:
- **Recursive Flattening & Exploding**: Automatically turns nested API results into relational rows.
- **Explicit Transactions**: All `create` and `upsert` operations are wrapped in atomic transactions to ensure data integrity.
- **Chunked Processing**: Optimized for memory efficiency during large imports.
- **Schema Evolution**: Automatically adapts table structures as incoming data changes.

### 2. `verify_database_model.py`
Validation script for database operations.

## Usage

```python
from database.Model import DataModel

db = DataModel("data_vault.db")
db.upsert("my_table", dataframe, pk="id")
```
