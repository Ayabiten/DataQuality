import json
import os
import pandas as pd
import sys
import datetime
from typing import Any, Dict, List, Optional

# Add parent directory to sys.path to ensure core is found
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from dq_logging import DataQualityLogger
from core.base_audit import BaseQualityAudit
from core.models import AuditSummary, ColumnProfile

class JSONQualityAudit(BaseQualityAudit):
    """
    Main Model for JSON Data Quality Auditing.
    Inherits from BaseQualityAudit to provide flat-table profiling of JSON data.
    """
    
    def __init__(self, file_path: str, logger: Optional[DataQualityLogger] = None):
        super().__init__(logger=logger)
        self.file_path = file_path
        self.logger = logger or DataQualityLogger(log_name="JSONAudit", log_to_file=False)

    def execute_full_audit(self):
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
        
        # Step 2: Content Audit (Flattening for Deep Profile)
        # We use pd.json_normalize to turn JSON into a flat table for profiling
        try:
            if isinstance(data, dict):
                df = pd.json_normalize(data)
                structure_type = "Object"
            else:
                df = pd.json_normalize(data)
                structure_type = "List"
        except Exception as e:
            self.logger.log_generic(f"Flattening failed: {e}", level="WARNING")
            df = pd.DataFrame()
            structure_type = "Complex/Malformed"

        column_profiles = []
        summary = None

        if not df.empty:
            column_profiles = self.perform_column_profiling(df)
            
            duplicate_count = int(df.duplicated().sum())
            total_null_count = int(df.isnull().sum().sum())
            
            quality_score = self.calculate_overall_quality_score(
                total_null_count, df.shape[0], df.shape[1], duplicate_count
            )

            stats = os.stat(self.file_path)
            last_mod = datetime.datetime.fromtimestamp(stats.st_mtime).strftime('%Y-%m-%d %H:%M:%S')

            summary = AuditSummary(
                file_name=os.path.basename(self.file_path),
                abs_path=os.path.abspath(self.file_path),
                size_mb=round(stats.st_size / (1024 * 1024), 4),
                last_modified=last_mod,
                total_rows=df.shape[0],
                total_cols=df.shape[1],
                duplicate_rows=duplicate_count,
                total_nulls=total_null_count,
                quality_score=quality_score
            )
            # Add JSON specific metadata
            summary.max_depth = max_depth
            summary.structure_type = structure_type

        return {
            "summary": summary,
            "columns": column_profiles,
            "raw_df": df
        }

    def measure_nesting_depth(self, data, current_level=1) -> int:
        if not isinstance(data, (dict, list)) or not data:
            return current_level
        if isinstance(data, dict):
            return max(self.measure_nesting_depth(v, current_level + 1) for v in data.values())
        return max(self.measure_nesting_depth(item, current_level + 1) for item in data)

    def export_report_to_excel(self, output_path: str):
        report = self.execute_full_audit()
        if not report['summary']:
            return None
            
        try:
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                # Summary
                summary_dict = report['summary'].to_dict()
                summary_dict['max_depth'] = report['summary'].max_depth
                summary_dict['structure_type'] = report['summary'].structure_type
                
                summary_df = pd.DataFrame(list(summary_dict.items()), columns=['Metric', 'Value'])
                summary_df.to_excel(writer, sheet_name="Audit Summary", index=False)
                
                # Column Analysis
                flattened = []
                for col in report['columns']:
                    row = col.to_dict()
                    stats = row.pop('stats', {})
                    for k, v in stats.items(): row[f'stat_{k}'] = v
                    row['placeholders_found'] = str(row['placeholders_found'])
                    row['unique_values_sample'] = ", ".join([str(v) for v in row['unique_values_sample']])
                    row['naming_issues'] = ", ".join(row['naming_issues'])
                    row['top_values'] = " | ".join(row['top_values'])
                    flattened.append(row)
                pd.DataFrame(flattened).to_excel(writer, sheet_name="Column Analysis", index=False)
                
            return output_path
        except Exception as e:
            self.logger.log_exception(error_type="ExcelExportError")
            raise e
