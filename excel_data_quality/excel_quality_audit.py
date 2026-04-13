import pandas as pd
import numpy as np
import os
import datetime
from typing import Dict, List, Any, Optional
from dq_logging import DataQualityLogger

class ExcelSheetProfile:
    """
    A Model class representing the data quality profile of a specific Excel sheet.
    """
    def __init__(self, sheet_name, total_rows, total_cols, quality_metrics, statistical_summary=None, placeholders=None):
        self.sheet_name = sheet_name
        self.total_rows = total_rows
        self.total_cols = total_cols
        self.quality_metrics = quality_metrics # Pandas DataFrame or List of Dicts
        self.statistical_summary = statistical_summary
        self.placeholders = placeholders

class ExcelQualityAudit:
    """
    Main Model for Excel Workbook Data Quality Auditing.
    Analyzes multiple sheets and tracks metadata.
    """
    
    def __init__(self, file_path: str, logger: Optional[DataQualityLogger] = None):
        """
        Initialize the Excel Audit Model.
        """
        self.file_path = file_path
        self.logger = logger or DataQualityLogger(log_name="ExcelAudit", log_to_file=False)
        self.workbook_metadata: Dict[str, Any] = {}

    def execute_workbook_audit(self):
        """
        Performs a full audit of all sheets in the Excel workbook.
        """
        # Step 1: Collect Metadata
        self.check_workspace_metadata()
        
        workbook_report = {
            "metadata": self.workbook_metadata,
            "sheets": []
        }
        
        # Step 2: Iterate through sheets
        try:
            excel_file = pd.ExcelFile(self.file_path)
            for sheet_name in excel_file.sheet_names:
                sheet_data = self.detect_sheet_level_quality(sheet_name)
                workbook_report["sheets"].append(sheet_data)
        except Exception as e:
            self.logger.log_exception(self.file_path, "ExcelLoadError")
            raise ValueError(f"Failed to process Excel workbook: {e}")
            
        return workbook_report

    def check_workspace_metadata(self):
        """
        Extracts basic file-level information for the workbook.
        """
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

    def detect_sheet_level_quality(self, sheet_name: str) -> ExcelSheetProfile:
        """
        Performs a deep audit on a single sheet.
        Returns an ExcelSheetProfile model.
        """
        # Load the sheet
        df = pd.read_excel(self.file_path, sheet_name=sheet_name)
        
        if df.empty:
            return ExcelSheetProfile(sheet_name, 0, 0, [])

        # Step 1: Quality Metrics (Nulls & Types)
        quality_metrics = []
        for col in df.columns:
            null_count = int(df[col].isnull().sum())
            quality_metrics.append({
                'column': col,
                'null_count': null_count,
                'null_percentage': f"{(null_count/len(df))*100:.2f}%",
                'type': str(df[col].dtype)
            })

        # Step 2: Statistical Summary for numeric columns
        stats = df.describe().transpose().to_dict()

        # Step 3: Placeholder Detection
        placeholders = self.identify_placeholder_values(df)

        return ExcelSheetProfile(
            sheet_name=sheet_name,
            total_rows=len(df),
            total_cols=len(df.columns),
            quality_metrics=quality_metrics,
            statistical_summary=stats,
            placeholders=placeholders
        )

    def identify_placeholder_values(self, df: pd.DataFrame) -> Dict[str, Dict[str, int]]:
        """
        Searches for dummy placeholders across all columns in a sheet.
        """
        placeholders = ['n/a', 'unknown', 'none', 'null', 'nan', '?', '999']
        results = {}
        
        for col in df.columns:
            # Normalize for comparison
            sample = df[col].astype(str).str.lower().str.strip()
            found_placeholders = {
                p: int((sample == p).sum()) 
                for p in placeholders 
                if (sample == p).any()
            }
            if found_placeholders:
                results[col] = found_placeholders
                
        return results
