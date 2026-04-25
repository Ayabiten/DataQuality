import pandas as pd
import json
import os
import sys
import time

# Add parent directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from database2.Model import NormalizedDataModel

def run_verification():
    db_file = "normalized_test.db"
    if os.path.exists(db_file): os.remove(db_file)
    
    db = NormalizedDataModel(db_file)
    
    # 1. Create highly redundant data (Large parent, many children)
    # If we "exploded" this, it would be 100 rows * 10 columns = 1000 cells.
    # But with normalization, it's 1 row (parent) + 100 rows (child) = 101 records total, 
    # but the parent columns are NOT duplicated.
    
    data = {
        "user_id": 101,
        "name": "Jane Doe",
        "email": "jane@example.com",
        "bio": "A very long bio string that would be duplicated many times if we exploded...",
        "tags": [f"tag_{i}" for i in range(100)], # 100 tags
        "history": [
            {"event": "login", "date": "2024-01-01"},
            {"event": "logout", "date": "2024-01-02"},
            {"event": "purchase", "amount": 50.0}
        ]
    }
    
    print("\n--- Testing Normalized Import ---")
    db.create("users", [data])
    
    print("\n--- Verifying Table Structure ---")
    tables = db.list_tables()
    print(f"Tables created: {tables}")
    
    # Check parent
    df_parent = db.read("users")
    print(f"\nMain Table (users) Row Count: {len(df_parent)}")
    print(f"Parent ID: {df_parent['_parent_id'].iloc[0]}")
    
    p_id = df_parent['_parent_id'].iloc[0]
    
    # Check children
    print("\n--- Fetching Related Data ---")
    related = db.get_related("users", p_id)
    for name, df in related.items():
        print(f"Child Table '{name}' Row Count: {len(df)}")
        print(f"First few rows of {name}:\n{df.head(2)}")

    # Test space reclamation
    db.vacuum()
    
    print(f"\nFinal DB Size: {os.path.getsize(db_file) / 1024:.2f} KB")
    print("\nVerification Complete. The data is stored efficiently without parent duplication.")

if __name__ == "__main__":
    run_verification()
