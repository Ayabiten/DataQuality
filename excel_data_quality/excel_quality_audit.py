import pandas as pd
import numpy as np
import os
import datetime
import sys
from typing import Dict, List, Any, Optional

# Add parent directory to sys.path to ensure core is found
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from dq_logging import DataQualityLogger
from core.base_audit import BaseQualityAudit
from core.models import AuditSummary, ColumnProfile

class ExcelQualityAudit(BaseQualityAudit):
    """
    Main Model for Excel Workbook Data Quality Auditing.
    Inherits from BaseQualityAudit to provide deep profiling for each sheet.
    """
    
    def __init__(self, file_path: str, logger: Optional[DataQualityLogger] = None):
        super().__init__(logger=logger)
        self.file_path = file_path
        self.logger = logger or DataQualityLogger(log_name="ExcelAudit", log_to_file=False)
        self.workbook_metadata: Dict[str, Any] = {}

    def execute_full_audit(self):
        """
        Performs a full audit of all sheets in the Excel workbook.
        """
        self.check_workbook_metadata()
        
        workbook_report = {
            "metadata": self.workbook_metadata,
            "sheets": {}
        }
        
        try:
            excel_file = pd.ExcelFile(self.file_path)
            for sheet_name in excel_file.sheet_names:
                df = pd.read_excel(self.file_path, sheet_name=sheet_name)
                
                if df.empty:
                    workbook_report["sheets"][sheet_name] = {"summary": "Empty Sheet"}
                    continue

                # Use shared deep profiling logic for each sheet
                column_profiles = self.perform_column_profiling(df)
                
                duplicate_count = int(df.duplicated().sum())
                total_null_count = int(df.isnull().sum().sum())
                
                quality_score = self.calculate_overall_quality_score(
                    total_null_count, df.shape[0], df.shape[1], duplicate_count
                )

                summary = AuditSummary(
                    file_name=f"{os.path.basename(self.file_path)} [{sheet_name}]",
                    abs_path=os.path.abspath(self.file_path),
                    size_mb=self.workbook_metadata['size_mb'],
                    last_modified=self.workbook_metadata['last_modified'],
                    total_rows=df.shape[0],
                    total_cols=df.shape[1],
                    duplicate_rows=duplicate_count,
                    total_nulls=total_null_count,
                    quality_score=quality_score
                )
                
                workbook_report["sheets"][sheet_name] = {
                    "summary": summary,
                    "columns": column_profiles
                }
                
        except Exception as e:
            self.logger.log_exception(self.file_path, "ExcelLoadError")
            raise ValueError(f"Failed to process Excel workbook: {e}")
            
        return workbook_report

    def check_workbook_metadata(self):
        stats = os.stat(self.file_path)
        last_mod = datetime.datetime.fromtimestamp(stats.st_mtime).strftime('%Y-%m-%d %H:%M:%S')
        
        try:
            excel_wrapper = pd.ExcelFile(self.file_path)
            sheet_names = excel_wrapper.sheet_names
        except:
            sheet_names = []

        self.workbook_metadata = {
            'file_name': os.path.basename(self.file_path),
            'size_mb': round(stats.st_size / (1024 * 1024), 4),
            'last_modified': last_mod,
            'sheet_names': sheet_names,
            'abs_path': os.path.abspath(self.file_path)
        }

    def export_report_to_excel(self, output_path: str):
        report = self.execute_full_audit()
        try:
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                for sheet_name, data in report['sheets'].items():
                    if isinstance(data, dict) and 'summary' in data and data['summary'] != "Empty Sheet":
                        # Summary for this sheet
                        summary_df = pd.DataFrame(list(data['summary'].to_dict().items()), columns=['Metric', 'Value'])
                        summary_df.to_excel(writer, sheet_name=f"{sheet_name}_Summary", index=False)
                        
                        # Column Analysis for this sheet
                        flattened = []
                        for col in data['columns']:
                            row = col.to_dict()
                            stats = row.pop('stats', {})
                            for k, v in stats.items(): row[f'stat_{k}'] = v
                            row['placeholders_found'] = str(row['placeholders_found'])
                            row['unique_values_sample'] = ", ".join([str(v) for v in row['unique_values_sample']])
                            row['naming_issues'] = ", ".join(row['naming_issues'])
                            row['top_values'] = " | ".join(row['top_values'])
                            flattened.append(row)
                        pd.DataFrame(flattened).to_excel(writer, sheet_name=f"{sheet_name}_Analysis", index=False)
            return output_path
        except Exception as e:
            self.logger.log_exception(error_type="ExcelExportError")
            raise e
