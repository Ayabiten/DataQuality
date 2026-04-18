import sqlite3
import pandas as pd
from datetime import datetime
import os
from typing import Union, List, Dict, Any, Optional

class DataModel:
    """
    A dynamic database model for handling complex nested data structures.
    Supports recursive flattening, list exploding, and schema evolution.
    """

    def __init__(self, db_path: str = "data_vault.db"):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        """Ensures the database directory exists."""
        db_dir = os.path.dirname(self.db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir)

    def _get_connection(self):
        return sqlite3.connect(self.db_path)

    def flatten_and_explode(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Recursively flattens dictionaries and explodes lists until only scalar values remain.
        """
        df = df.copy()
        
        while True:
            # Identify columns that need processing
            complex_cols = []
            for col in df.columns:
                # Check first non-null value for type
                sample = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
                if isinstance(sample, (dict, list)):
                    complex_cols.append((col, type(sample)))
            
            if not complex_cols:
                break
            
            for col, col_type in complex_cols:
                if col_type is list:
                    df = df.explode(col)
                elif col_type is dict:
                    # Normalize the dict column
                    # We use json_normalize and then join it back
                    # To keep it simple and robust, we convert the column to a list of dicts
                    normalized = pd.json_normalize(df[col].tolist())
                    normalized.index = df.index
                    # Prefix columns if needed to avoid collisions, but user asked for content.id
                    # json_normalize already uses dot notation by default
                    normalized.columns = [f"{col}.{c}" if not c.startswith(f"{col}.") else c for c in normalized.columns]
                    
                    df = df.drop(columns=[col]).join(normalized)
            
            # Reset index to avoid issues with multiple explosions/joins
            df = df.reset_index(drop=True)
            
        return df

    def _sync_schema(self, table_name: str, df: pd.DataFrame):
        """
        Ensures the table exists and has all the columns present in the DataFrame.
        """
        with self._get_connection() as conn:
            # Check if table exists
            cursor = conn.cursor()
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
            exists = cursor.fetchone()
            
            if not exists:
                # Create table with current schema
                df.head(0).to_sql(table_name, conn, index=False)
                # Add updated_date column if not present
                try:
                    cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN updated_date TEXT")
                except:
                    pass
            else:
                # Table exists, check for missing columns
                cursor.execute(f"PRAGMA table_info({table_name})")
                existing_cols = [row[1] for row in cursor.fetchall()]
                
                for col in df.columns:
                    if col not in existing_cols:
                        # SQLite doesn't support complex types, so we use TEXT/NUMERIC/REAL based on DF
                        # But typically adding as TEXT is safest for dynamic data
                        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN [{col}] TEXT")
                
                if 'updated_date' not in existing_cols:
                    cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN updated_date TEXT")
            
            conn.commit()

    def create(self, table_name: str, data: Union[pd.DataFrame, List[Dict], Dict]):
        """
        Creates/Inserts new records into the table.
        """
        if isinstance(data, dict):
            data = [data]
        
        df = pd.DataFrame(data) if not isinstance(data, pd.DataFrame) else data.copy()
        
        # Flatten and explode
        df = self.flatten_and_explode(df)
        
        # Add audit column
        df['updated_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Sync schema
        self._sync_schema(table_name, df)
        
        # Insert
        with self._get_connection() as conn:
            df.to_sql(table_name, conn, if_exists='append', index=False)
            
        print(f"Successfully inserted {len(df)} rows into {table_name}")

    def read(self, table_name: str, query: str = None) -> pd.DataFrame:
        """
        Reads records from the table. Alias for get_df.
        """
        return self.get_df(table_name, query)

    def get_df(self, table_name: str, query: str = None) -> pd.DataFrame:
        """
        Reads records from the table and returns a DataFrame.
        """
        with self._get_connection() as conn:
            sql = f"SELECT * FROM {table_name}"
            if query:
                sql += f" WHERE {query}"
            return pd.read_sql(sql, conn)

    def list_tables(self) -> List[str]:
        """
        Returns a list of all table names in the database.
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            return [row[0] for row in cursor.fetchall()]

    def get_schema(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Returns the schema (column info) of a table.
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"PRAGMA table_info({table_name})")
            rows = cursor.fetchall()
            return [{"id": r[0], "name": r[1], "type": r[2], "notnull": r[3], "default": r[4], "pk": r[5]} for r in rows]

    def drop_table(self, table_name: str):
        """
        Completely removes a table.
        """
        with self._get_connection() as conn:
            conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            conn.commit()
        print(f"Table {table_name} dropped.")

    def count(self, table_name: str, condition: str = None) -> int:
        """
        Returns the row count of a table.
        """
        sql = f"SELECT COUNT(*) FROM {table_name}"
        if condition:
            sql += f" WHERE {condition}"
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            return cursor.fetchone()[0]

    def execute_raw(self, sql: str, params: tuple = None):
        """
        Executes a raw SQL statement.
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            conn.commit()
            return cursor.fetchall()

    def update(self, table_name: str, data: Dict, condition: str):
        """
        Updates records in the table based on a condition.
        """
        set_clause = ", ".join([f"[{k}] = ?" for k in data.keys()])
        set_clause += ", updated_date = ?"
        values = list(data.values()) + [datetime.now().strftime('%Y-%m-%d %H:%M:%S')]
        
        sql = f"UPDATE {table_name} SET {set_clause} WHERE {condition}"
        
        with self._get_connection() as conn:
            conn.execute(sql, values)
            conn.commit()
        print(f"Updated records in {table_name} where {condition}")

    def delete(self, table_name: str, condition: str):
        """
        Deletes records from the table based on a condition.
        """
        sql = f"DELETE FROM {table_name} WHERE {condition}"
        with self._get_connection() as conn:
            conn.execute(sql)
            conn.commit()
        print(f"Deleted records from {table_name} where {condition}")

    def upsert(self, table_name: str, data: Union[pd.DataFrame, List[Dict], Dict], pk: str = "id"):
        """
        Inserts new records or updates existing ones based on a primary key.
        Note: This is a simplified implementation for demonstration.
        """
        # For simplicity in this generic model, we'll implement upsert by:
        # 1. Flattening the data
        # 2. Deleting existing records with matching PK
        # 3. Inserting the new records
        
        if isinstance(data, dict):
            data = [data]
        
        df = pd.DataFrame(data) if not isinstance(data, pd.DataFrame) else data.copy()
        df = self.flatten_and_explode(df)
        
        # Sync schema first to ensure PK column exists
        self._sync_schema(table_name, df)
        
        if pk in df.columns:
            pk_values = df[pk].dropna().unique().tolist()
            if pk_values:
                # Create a condition string
                if all(isinstance(v, (int, float)) for v in pk_values):
                    cond = f"[{pk}] IN ({', '.join(map(str, pk_values))})"
                else:
                    cond = f"[{pk}] IN ({', '.join([f'\'{v}\'' for v in pk_values])})"
                
                try:
                    self.delete(table_name, cond)
                except sqlite3.OperationalError:
                    # Table might be empty or PK col might not exist in actual DB yet
                    pass
        
        self.create(table_name, df)
