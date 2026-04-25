import sqlite3
import pandas as pd
import json
import uuid
import os
from datetime import datetime
from typing import Union, List, Dict, Any, Optional, Tuple

# Import from core models if needed for consistency
try:
    from core.models import AuditSummary, ColumnProfile
except ImportError:
    # Handle local imports if path not in sys
    import sys
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
    from core.models import AuditSummary, ColumnProfile

class NormalizedDataModel:
    """
    A normalized database model that handles complex nested data structures.
    Instead of 'exploding' lists into the same table (duplicating parent data),
    it moves them into separate child tables with a foreign key relationship.
    """

    def __init__(self, db_path: str = "data_vault_v2.db"):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        """Ensures the database directory exists."""
        db_dir = os.path.dirname(self.db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir)

    def _get_connection(self):
        return sqlite3.connect(self.db_path)

    def _generate_parent_id(self) -> str:
        """Generates a unique ID for row linking."""
        return str(uuid.uuid4())[:13] # Short UUID for readability

    def _sync_schema(self, table_name: str, df: pd.DataFrame):
        """Ensures the table exists and has all the columns present in the DataFrame."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
            exists = cursor.fetchone()
            
            if not exists:
                df.head(0).to_sql(table_name, conn, index=False)
                # Ensure _parent_id and updated_date exist
                try:
                    if '_parent_id' not in df.columns:
                        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN [_parent_id] TEXT")
                    cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN [updated_date] TEXT")
                except: pass
            else:
                cursor.execute(f"PRAGMA table_info({table_name})")
                existing_cols = [row[1] for row in cursor.fetchall()]
                for col in df.columns:
                    if col not in existing_cols:
                        cursor.execute(f"ALTER TABLE [{table_name}] ADD COLUMN [{col}] TEXT")
                if 'updated_date' not in existing_cols:
                    cursor.execute(f"ALTER TABLE [{table_name}] ADD COLUMN [updated_date] TEXT")
            conn.commit()

    def process_data(self, df: pd.DataFrame, table_name: str) -> List[Tuple[str, pd.DataFrame]]:
        """
        Processes a DataFrame, flattening dictionaries and extracting lists into separate tables.
        Returns a list of (table_name, dataframe) tuples.
        """
        df = df.copy()
        # Add internal ID if not present
        if '_parent_id' not in df.columns:
            df['_parent_id'] = [self._generate_parent_id() for _ in range(len(df))]
        
        all_tables = [] # List of (name, df)
        
        # 1. Handle JSON strings in columns
        for col in df.columns:
            sample = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
            if isinstance(sample, str):
                s = sample.strip()
                if (s.startswith('{') and s.endswith('}')) or (s.startswith('[') and s.endswith(']')):
                    try:
                        df[col] = df[col].apply(lambda x: json.loads(x) if isinstance(x, str) and x.strip() else x)
                    except: pass

        # 2. Extract Lists and Flatten Dicts
        while True:
            complex_cols = []
            for col in df.columns:
                if col == '_parent_id': continue
                sample = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
                if isinstance(sample, (dict, list)):
                    complex_cols.append((col, type(sample)))
            
            if not complex_cols:
                break
                
            for col, col_type in complex_cols:
                if col_type is list:
                    # NORMALIZATION: Move to a separate table
                    child_table_name = f"{table_name}_{col}"
                    print(f"  [Normalize] Moving '{col}' (List) to child table: '{child_table_name}'")
                    
                    # Create child records
                    child_rows = []
                    for idx, row in df.iterrows():
                        p_id = row['_parent_id']
                        items = row[col]
                        if isinstance(items, list):
                            for item in items:
                                if isinstance(item, dict):
                                    # Flatten dict items in the list
                                    flat_item = {'_parent_id': p_id}
                                    flat_item.update(item)
                                    child_rows.append(flat_item)
                                else:
                                    child_rows.append({'_parent_id': p_id, 'value': item})
                    
                    if child_rows:
                        child_df = pd.DataFrame(child_rows)
                        # Recursively process the child table in case IT has nested data
                        processed_children = self.process_data(child_df, child_table_name)
                        all_tables.extend(processed_children)
                    
                    # Drop the column from main table as it's now normalized
                    df = df.drop(columns=[col])
                    
                elif col_type is dict:
                    # FLATTENING: Keep in same table (columns only)
                    print(f"  [Flatten] Expanding '{col}' (Dict) into columns")
                    normalized = pd.json_normalize(df[col].tolist())
                    normalized.index = df.index
                    normalized.columns = [f"{col}.{c}" if not c.startswith(f"{col}.") else c for c in normalized.columns]
                    df = df.drop(columns=[col]).join(normalized)
            
            df = df.reset_index(drop=True)

        all_tables.append((table_name, df))
        return all_tables

    def create(self, table_name: str, data: Union[pd.DataFrame, List[Dict], Dict]):
        """Inserts data using normalization."""
        if isinstance(data, dict): data = [data]
        df = pd.DataFrame(data) if not isinstance(data, pd.DataFrame) else data.copy()
        
        print(f"--- Processing Normalized Import for '{table_name}' ---")
        tables_to_save = self.process_data(df, table_name)
        
        with self._get_connection() as conn:
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            for name, target_df in tables_to_save:
                target_df['updated_date'] = timestamp
                self._sync_schema(name, target_df)
                print(f"  [Database] Saving {len(target_df)} rows to '{name}'")
                target_df.to_sql(name, conn, if_exists='append', index=False)
        
        print(f"Successfully imported {len(df)} records into normalized structure.")

    def read(self, table_name: str, query: str = None) -> pd.DataFrame:
        """Reads the main table."""
        with self._get_connection() as conn:
            sql = f"SELECT * FROM [{table_name}]"
            if query: sql += f" WHERE {query}"
            return pd.read_sql(sql, conn)

    def get_related(self, table_name: str, parent_id: str) -> Dict[str, pd.DataFrame]:
        """Retrieves all child records for a specific parent ID."""
        tables = self.list_tables()
        prefix = f"{table_name}_"
        children = {}
        
        with self._get_connection() as conn:
            for t in tables:
                if t.startswith(prefix):
                    sql = f"SELECT * FROM [{t}] WHERE _parent_id = ?"
                    child_df = pd.read_sql(sql, conn, params=(parent_id,))
                    if not child_df.empty:
                        children[t[len(prefix):]] = child_df
        return children

    def list_tables(self) -> List[str]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            return [row[0] for row in cursor.fetchall()]

    def vacuum(self):
        """Reclaims database space."""
        print("Reclaiming database space (VACUUM)...")
        with self._get_connection() as conn:
            conn.execute("VACUUM")
        print("Done.")

    def drop_table_family(self, table_name: str):
        """Drops a table and all its normalized child tables."""
        tables = self.list_tables()
        prefix = f"{table_name}_"
        to_drop = [t for t in tables if t == table_name or t.startswith(prefix)]
        
        with self._get_connection() as conn:
            for t in to_drop:
                conn.execute(f"DROP TABLE IF EXISTS [{t}]")
            conn.commit()
        print(f"Dropped table family: {to_drop}")
