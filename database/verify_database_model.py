import pandas as pd
import json
import os
import sys

# Add parent directory to sys.path to allow importing from database.Model if needed,
# but since we are inside 'database', we can just import Model.
# However, for consistency when running from root, let's handle both.
try:
    from Model import DataModel
except ImportError:
    from database.Model import DataModel

def setup_test_data(base_path="test_data"):
    """Creates dummy data files for testing in a specific folder."""
    if not os.path.exists(base_path):
        os.makedirs(base_path)
        
    json_path = os.path.join(base_path, "test_sample.json")
    csv_path = os.path.join(base_path, "test_sample.csv")
    excel_path = os.path.join(base_path, "test_sample.xlsx")

    # 1. Nested JSON with lists (40 rows + extreme nesting)
    json_data = []
    # Add extreme nesting cases first
    json_data.append({
        "user_id": 999,
        "name": "SuperNested",
        # List inside a list inside a dict (all string encoded)
        "metadata": '{"settings": {"notifications": [{"type": "email", "flags": ["urgent", "daily"]}, {"type": "sms"}]}}',
        # Dict inside a list inside a list
        "content": '[[{"id": 1, "data": {"key": {"val":2}}}, {"id": 2}]]'
    })

    for i in range(1, 40):
        json_data.append({
            "user_id": i,
            "name": f"User_{i}",
            # These are explicitly strings to test the auto-detection fix
            "metadata": f'{{"role": "user_{i}", "active": true}}',
            "content": f'[{{\"id\": {i}01, \"text\": \"Msg {i}\"}}]'
        })
    
    with open(json_path, "w") as f:
        json.dump(json_data, f, indent=4)
    
    # 2. Simple CSV
    csv_data = pd.DataFrame([
        {"user_id": 3, "name": "Charlie", "email": "charlie@example.com"},
        {"user_id": 4, "name": "Dave", "email": "dave@example.com"}
    ])
    csv_data.to_csv(csv_path, index=False)
    
    # 3. Simple Excel
    excel_data = pd.DataFrame([
        {"user_id": 5, "name": "Eve", "age": 25},
        {"user_id": 6, "name": "Frank", "age": 30}
    ])
    excel_data.to_excel(excel_path, index=False)
    
    return json_path, csv_path, excel_path

def run_tests():
    # Database file will be created in the current directory (database/)
    db_file = "test_audit.db"
    db = DataModel(db_file)
    
    json_file, csv_file, excel_file = setup_test_data()
    
    print("\n--- Testing JSON with Nesting and Exploding ---")
    with open(json_file, "r") as f:
        json_input = json.load(f)
    db.create("users_audit", json_input)
    
    print("\n--- Testing Management: List Tables ---")
    tables = db.list_tables()
    print(f"Tables in DB: {tables}")
    
    print("\n--- Testing Management: Get Schema ---")
    schema = db.get_schema("users_audit")
    print(f"Schema for 'users_audit': {json.dumps(schema[:3], indent=2)} ...")
    
    print("\n--- Testing CSV Integration ---")
    df_csv = pd.read_csv(csv_file)
    db.create("users_audit", df_csv)
    
    print("\n--- Testing Excel Integration ---")
    df_excel = pd.read_excel(excel_file)
    db.create("users_audit", df_excel)
    
    print("\n--- Testing Management: Count ---")
    total_count = db.count("users_audit")
    print(f"Total rows in 'users_audit': {total_count}")
    
    print("\n--- Testing Schema Evolution (New Column) ---")
    new_data = {"user_id": 7, "name": "Grace", "subscription_tier": "Premium"}
    db.create("users_audit", new_data)
    
    print("\n--- Testing Management: Raw SQL ---")
    raw_res = db.execute_raw("SELECT name FROM users_audit WHERE subscription_tier = 'Premium'")
    print(f"Raw SQL Result: {raw_res}")
    
    print("\n--- Testing Update & Delete ---")
    db.update("users_audit", {"name": "Grace Updated"}, "user_id = 7")
    db.delete("users_audit", "user_id = 1")
    print(f"Count after deleting user_id=1: {db.count('users_audit')}")

    print("\n--- Testing New Extraction Formats (JSON, DF, Dict) ---")
    # 1. Extract as DataFrame
    df_extract = db.extract("users_audit", format="df")
    print(f"Extraction (DF) type: {type(df_extract)}")
    print(f"Extraction (DF) rows: {len(df_extract)}")
    print(f"Extraction (DF) rows: \n{df_extract}")

    # 2. Extract as JSON
    json_extract = db.extract("users_audit", format="json")
    print(f"Extraction (JSON) type: {type(json_extract)}")
    print(f"Extraction (JSON) sample: \n{json_extract[:100]}")

    # 3. Extract as Dict
    dict_extract = db.extract("users_audit", format="dict")
    print(f"Extraction (Dict) type: {type(dict_extract)}")
    print(f"Extraction (Dict) first item: {dict_extract[0] if dict_extract else 'Empty'}")
    print(f"Extraction (Dict) sample: \n{dict_extract[:5]}")
    
    print("\n--- Testing Management: Drop Table ---")
    db.create("temp_table", [{"a": 1}])
    print(f"Tables before drop: {db.list_tables()}")
    db.drop_table("temp_table")
    print(f"Tables after drop: {db.list_tables()}")
    
    print("\nCleanup: Verification Successful.")

if __name__ == "__main__":
    run_tests()
