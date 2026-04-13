import json
import os
import pandas as pd
from typing import Any, Dict, List, Optional
from dq_logging import DataQualityLogger

class JSONQualityProfile:
    """
    A Model class representing the quality profile of a JSON structure.
    """
    def __init__(self, file_name, max_depth, structure_type, item_count=0, 
                 key_inconsistencies=None, null_density=None):
        self.file_name = file_name
        self.max_depth = max_depth
        self.structure_type = structure_type # 'List' or 'Object'
        self.item_count = item_count
        self.key_inconsistencies = key_inconsistencies # DataFrame or Dict
        self.null_density = null_density # DataFrame or Dict

class JSONQualityAudit:
    """
    Main Model for JSON Data Quality Auditing.
    Validates nesting, schema consistency, and content.
    """
    
    def __init__(self, file_path: str, logger: Optional[DataQualityLogger] = None):
        """
        Initialize the JSON Audit Model.
        """
        self.file_path = file_path
        self.logger = logger or DataQualityLogger(log_name="JSONAudit", log_to_file=False)

    def execute_full_json_audit(self) -> JSONQualityProfile:
        """
        Performs a comprehensive audit of the JSON file structure and content.
        """
        try:
            with open(self.file_path, 'r') as f:
                data = json.load(f)
        except Exception as e:
            self.logger.log_exception(self.file_path, "JSONParseError")
            raise ValueError(f"Failed to parse JSON file: {e}")

        # Step 1: Structural Metadata
        max_depth = self.measure_nesting_depth(data)
        
        structure_type = "Object"
        item_count = 1
        key_inconsistencies = {}
        null_density = {}

        # Step 2: Collection Audit (if root is a list)
        if isinstance(data, list):
            structure_type = "List"
            item_count = len(data)
            
            # Check for key consistency across objects in the list
            key_inconsistencies_df = self.check_schema_consistency(data)
            if key_inconsistencies_df is not None:
                key_inconsistencies = key_inconsistencies_df.to_dict()
            
            # Audit content logic
            null_density_df = self.perform_json_content_audit(data)
            if null_density_df is not None:
                null_density = null_density_df.to_dict()
        
        return JSONQualityProfile(
            file_name=os.path.basename(self.file_path),
            max_depth=max_depth,
            structure_type=structure_type,
            item_count=item_count,
            key_inconsistencies=key_inconsistencies,
            null_density=null_density
        )

    def measure_nesting_depth(self, data, current_level=1) -> int:
        """
        Recursively calculates the maximum nesting depth of the JSON structure.
        """
        if not isinstance(data, (dict, list)) or not data:
            return current_level
            
        if isinstance(data, dict):
            return max(self.measure_nesting_depth(v, current_level + 1) for v in data.values())
            
        # For lists, check depth of each item
        return max(self.measure_nesting_depth(item, current_level + 1) for item in data)

    def check_schema_consistency(self, data_list: List[Dict]) -> Optional[pd.DataFrame]:
        """
        For a list of objects, identifies keys that are missing in some objects.
        """
        if not data_list or not isinstance(data_list[0], dict):
            return None
            
        all_detected_keys = set()
        for obj in data_list:
            if isinstance(obj, dict):
                all_detected_keys.update(obj.keys())
        
        report = []
        for key in all_detected_keys:
            missing_count = sum(1 for obj in data_list if isinstance(obj, dict) and key not in obj)
            if missing_count > 0:
                report.append({
                    'key': key, 
                    'missing_count': missing_count, 
                    'presence_percentage': f'{(1 - missing_count/len(data_list))*100:.1f}%'
                })
                
        return pd.DataFrame(report) if report else None

    def perform_json_content_audit(self, data: List[Dict]) -> Optional[pd.DataFrame]:
        """
        Normalizes the JSON data into a flat table and checks for null values.
        """
        if isinstance(data, list):
            try:
                # Use pandas to flatten the JSON structure
                flat_df = pd.json_normalize(data)
                null_report = flat_df.isnull().sum().to_frame('null_count')
                return null_report
            except:
                return None
        return None
