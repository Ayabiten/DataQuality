import pandas as pd
import numpy as np
import chardet
import csv
import os
import json
import datetime
import sys
from typing import Dict, List, Any, Optional, Tuple, Union

# Add parent directory to sys.path to ensure dq_logging and core are found
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from dq_logging import DataQualityLogger
from core.base_audit import BaseQualityAudit
from core.models import AuditSummary, ColumnProfile

try:
    from .robust_reader import RobustCSVReader
except (ImportError, ValueError):
    from robust_reader import RobustCSVReader

class CSVQualityAudit(BaseQualityAudit):
    """
    Enhanced Deep CSV Data Quality Audit Engine.
    Inherits from BaseQualityAudit for shared profiling logic.
    """
    
    def __init__(self, file_path: str, logger: Optional[DataQualityLogger] = None):
        super().__init__(logger=logger)
        self.file_path = file_path
        self.logger = logger or DataQualityLogger(log_name="DeepCSVAudit", log_to_file=False)
        self.file_metadata: Dict[str, Any] = {}
        self.data_frame: Optional[pd.DataFrame] = None
        self.structural_errors: List[Dict[str, Any]] = []
        self.structural_stats: Dict[str, Any] = {}
        self.correlation_findings: List[Dict[str, Any]] = []

    def execute_full_audit(self):
        """Implementation of the abstract method from BaseQualityAudit."""
        self.detect_file_encoding_and_separator()
        self.check_for_structural_mismatches()
        
        try:
            reader = RobustCSVReader(
                self.file_path, 
                encoding=self.file_metadata['encoding'], 
                delimiter=self.file_metadata['delimiter']
            )
            self.data_frame = reader.read_robustly()
            self.structural_errors.extend(reader.errors)
            self.structural_stats['healed_indices'] = reader.healed_indices
        except Exception as e:
            self.logger.log_exception(self.file_path, "CSVLoadError")
            raise ValueError(f"Could not load the CSV: {e}")

        # Use shared profiling logic from BaseQualityAudit
        column_profiles = self.perform_column_profiling(self.data_frame)
        
        # Cross-Column Analysis
        self.find_highly_correlated_columns()
        
        # Summary Metrics
        duplicate_count = int(self.data_frame.duplicated().sum())
        total_null_count = int(self.data_frame.isnull().sum().sum())
        completeness = self.calculate_completeness_score()
        
        quality_alerts = self.check_quality_trends_and_history(
            self.data_frame.shape[0], total_null_count, self.file_metadata['size_mb']
        )
        
        quality_score = self.calculate_overall_quality_score(
            total_null_count, self.data_frame.shape[0], self.data_frame.shape[1], duplicate_count
        )

        final_summary = AuditSummary(
            file_name=os.path.basename(self.file_path),
            abs_path=os.path.abspath(self.file_path),
            size_mb=self.file_metadata['size_mb'],
            last_modified=self.file_metadata['last_modified'],
            encoding=self.file_metadata['encoding'],
            confidence=self.file_metadata['confidence'],
            delimiter=self.file_metadata['delimiter'],
            has_header=self.file_metadata['has_header'],
            total_rows=self.data_frame.shape[0],
            total_cols=self.data_frame.shape[1],
            mismatched_rows=self.structural_stats.get('mismatched_rows', 0),
            multi_line_rows=self.structural_stats.get('multi_line_rows_count', 0),
            duplicate_rows=duplicate_count,
            total_nulls=total_null_count,
            quality_score=quality_score,
            completeness_score=completeness,
            healed_rows=len(self.structural_stats.get('healed_indices', []))
        )
        
        return {
            "summary": final_summary,
            "columns": column_profiles,
            "alerts": quality_alerts,
            "correlations": self.correlation_findings
        }

    def detect_file_encoding_and_separator(self):
        stats = os.stat(self.file_path)
        last_mod = datetime.datetime.fromtimestamp(stats.st_mtime).strftime('%Y-%m-%d %H:%M:%S')
        with open(self.file_path, 'rb') as f:
            raw = f.read(50000)
            res = chardet.detect(raw)
            encoding = res['encoding'] or 'utf-8'
        try:
            with open(self.file_path, 'r', encoding=encoding) as f:
                content = f.read(50000)
                dialect = csv.Sniffer().sniff(content)
                delimiter = dialect.delimiter
                has_header = csv.Sniffer().has_header(content)
        except:
            delimiter = ','; has_header = True

        self.file_metadata = {
            'encoding': encoding, 'confidence': res['confidence'] or 0.0,
            'delimiter': delimiter, 'has_header': has_header,
            'size_mb': round(stats.st_size / (1024 * 1024), 4),
            'last_modified': last_mod
        }

    def check_for_structural_mismatches(self):
        errors = []; multi_lines = []; count = 0; expected = 0
        with open(self.file_path, 'r', encoding=self.file_metadata['encoding']) as f:
            reader = csv.reader(f, delimiter=self.file_metadata['delimiter'])
            try:
                header = next(reader); expected = len(header)
            except StopIteration: return
            for idx, row in enumerate(reader, 2):
                count += 1
                if len(row) != expected:
                    errors.append({'row': idx, 'expected': expected, 'found': len(row)})
                for cell in row:
                    if '\n' in str(cell) or '\r' in str(cell):
                        multi_lines.append(idx); break
        self.structural_errors = errors
        self.structural_stats = {
            'mismatched_rows': len(errors),
            'multi_line_rows_count': len(set(multi_lines))
        }

    def find_highly_correlated_columns(self):
        nums = self.data_frame.select_dtypes(include=[np.number])
        if nums.shape[1] < 2: return
        corr = nums.corr().abs()
        findings = []
        for i in range(len(corr.columns)):
            for j in range(i+1, len(corr.columns)):
                if corr.iloc[i, j] > 0.95:
                    findings.append({'cols': (corr.columns[i], corr.columns[j]), 'score': round(corr.iloc[i, j], 4)})
        self.correlation_findings = findings

    def calculate_completeness_score(self) -> float:
        total = self.data_frame.size
        nulls = self.data_frame.isnull().sum().sum()
        if total == 0: return 100.0
        return round(((total - nulls) / total) * 100, 2)

    def check_quality_trends_and_history(self, rows, nulls, size) -> List[str]:
        hfile = 'quality_history.json'; alerts = []; hist = {}
        if os.path.exists(hfile):
            try:
                with open(hfile, 'r') as f: hist = json.load(f)
            except: pass
        fid = os.path.basename(self.file_path)
        prev = hist.get(fid)
        if prev:
            if rows > prev['rows'] * 1.5 or rows < prev['rows'] * 0.5: alerts.append("Row Count Shift")
            if nulls > prev['nulls'] * 1.2: alerts.append("Null Spike")
        hist[fid] = {'rows': rows, 'nulls': nulls, 'size': size, 'timestamp': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        try:
            with open(hfile, 'w') as f: json.dump(hist, f, indent=4)
        except: pass
        return alerts

    def export_report_to_excel(self, output_path: str):
        report = self.execute_full_audit()
        try:
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                # Summary
                summary_df = pd.DataFrame(list(report['summary'].to_dict().items()), columns=['Metric', 'Value'])
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
                
                # Structural
                if self.structural_errors: pd.DataFrame(self.structural_errors).to_excel(writer, sheet_name="Structural Issues", index=False)
                
                # Correlations
                if self.correlation_findings:
                    pd.DataFrame(self.correlation_findings).to_excel(writer, sheet_name="Correlations", index=False)
                
                # Alerts
                if report['alerts']: pd.DataFrame([{'Alert': a} for a in report['alerts']]).to_excel(writer, sheet_name="Trend Alerts", index=False)

            return output_path
        except Exception as e:
            self.logger.log_exception(error_type="ExcelExportError")
            raise e
