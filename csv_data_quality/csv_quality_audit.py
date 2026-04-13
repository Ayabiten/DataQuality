import pandas as pd
import numpy as np
import chardet
import csv
import os
import re
import json
import datetime
from typing import Dict, List, Any, Optional, Tuple, Union
from dq_logging import DataQualityLogger, FileErrorLog

class AuditSummary:
    """
    A Model class to represent the high-level summary of a CSV quality audit.
    Enhanced to include completeness and correlation insights.
    """
    def __init__(self, file_name, abs_path, size_mb, last_modified, encoding, 
                 confidence, delimiter, has_header, total_rows, total_cols, 
                 mismatched_rows, multi_line_rows, duplicate_rows, total_nulls, 
                 quality_score, completeness_score):
        self.file_name = file_name
        self.abs_path = abs_path
        self.size_mb = size_mb
        self.last_modified = last_modified
        self.encoding = encoding
        self.encoding_confidence = confidence
        self.delimiter = delimiter
        self.has_header = has_header
        self.total_rows = total_rows
        self.total_cols = total_cols
        self.mismatched_rows = mismatched_rows
        self.multi_line_rows = multi_line_rows
        self.duplicate_rows = duplicate_rows
        self.total_nulls = total_nulls
        self.quality_score = quality_score
        self.completeness_score = completeness_score

    def to_dict(self):
        return vars(self)

class ColumnProfile:
    """
    A Model class representing a deep data quality profile of a specific column.
    """
    def __init__(self, name, pandas_type, detected_type, null_count, null_percentage, 
                 empty_strings, unique_count, is_pk_candidate, whitespace_issues, 
                 outlier_count, inconsistency_count, placeholders_found=None, 
                 naming_issues=None, top_values=None, stats=None, 
                 unique_values_sample=None, detected_format=None, 
                 is_redundant=False, is_boolean_candidate=False, 
                 date_format_consistency=None):
        self.name = name
        self.pandas_type = pandas_type
        self.detected_type = detected_type
        self.null_count = null_count
        self.null_percentage = null_percentage
        self.empty_strings = empty_strings
        self.unique_count = unique_count
        self.is_pk_candidate = is_pk_candidate
        self.whitespace_issues = whitespace_issues
        self.outlier_count = outlier_count
        self.inconsistency_count = inconsistency_count
        self.placeholders_found = placeholders_found or {}
        self.naming_issues = naming_issues or []
        self.top_values = top_values or []
        self.stats = stats or {}
        # New Deep Fields
        self.unique_values_sample = unique_values_sample or []
        self.detected_format = detected_format
        self.is_redundant = is_redundant
        self.is_boolean_candidate = is_boolean_candidate
        self.date_format_consistency = date_format_consistency

    def to_dict(self):
        return vars(self)

class CSVQualityAudit:
    """
    Enhanced Deep CSV Data Quality Audit Engine.
    Equipped with format detection, uniqueness exploration, and trend alerts.
    """
    
    def __init__(self, file_path: str, logger: Optional[DataQualityLogger] = None):
        self.file_path = file_path
        self.logger = logger or DataQualityLogger(log_name="DeepCSVAudit", log_to_file=False)
        self.file_metadata: Dict[str, Any] = {}
        self.data_frame: Optional[pd.DataFrame] = None
        self.structural_errors: List[Dict[str, Any]] = []
        self.structural_stats: Dict[str, Any] = {}
        self.correlation_findings: List[Dict[str, Any]] = []

    def execute_full_quality_audit(self):
        """Orchestrates the entire Deep Audit process."""
        self.detect_file_encoding_and_separator()
        self.check_for_structural_mismatches()
        
        try:
            self.data_frame = pd.read_csv(
                self.file_path, 
                encoding=self.file_metadata['encoding'], 
                sep=self.file_metadata['delimiter'],
                on_bad_lines='warn',
                engine='python'
            )
        except Exception as e:
            self.logger.log_exception(self.file_path, "CSVLoadError")
            raise ValueError(f"Could not load the CSV: {e}")

        # Deep Column Audit
        column_profiles = self.perform_deep_column_audit()
        
        # Cross-Column Analysis
        self.find_highly_correlated_columns()
        
        # Summary Metrics
        duplicate_count = int(self.data_frame.duplicated().sum())
        total_null_count = int(self.data_frame.isnull().sum().sum())
        completeness = self.calculate_completeness_score()
        
        quality_alerts = self.check_quality_trends_and_history(
            self.data_frame.shape[0], total_null_count, self.file_metadata['size_mb']
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
            quality_score=self.calculate_overall_quality_score(total_null_count, duplicate_count),
            completeness_score=completeness
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

    def perform_deep_column_audit(self) -> List[ColumnProfile]:
        profiles = []
        for col in self.data_frame.columns:
            series = self.data_frame[col]
            inferred_type, inconst_count = self.detect_real_type(series)
            
            # Deep Metrics
            null_count = int(series.isnull().sum())
            unique_count = int(series.nunique())
            
            # 1. Format Detection
            detected_format = self.identify_data_format_patterns(series)
            
            # 2. Unique Sample (Max 20)
            unique_sample = self.collect_unique_values_sample(series)
            
            # 3. Top 5 Frequencies
            top_5 = self.get_most_frequent_values(series, limit=5)
            
            # 4. Redundancy & Booleans
            is_redundant = (unique_count <= 1)
            is_boolean = (unique_count == 2)
            
            # 5. Date Consistency
            date_consistency = None
            if inferred_type == 'datetime':
                date_consistency = self.check_date_format_consistency(series)

            # 6. Numerical Stats
            stats = {}
            if inferred_type == 'numeric':
                num_s = pd.to_numeric(series, errors='coerce').dropna()
                if not num_s.empty:
                    stats = {'min': float(num_s.min()), 'max': float(num_s.max()), 'mean': float(num_s.mean()), 'median': float(num_s.median())}

            profiles.append(ColumnProfile(
                name=col, pandas_type=str(series.dtype), detected_type=inferred_type,
                null_count=null_count, null_percentage=round((null_count/len(self.data_frame))*100, 2) if len(self.data_frame) > 0 else 0.0,
                empty_strings=self.count_empty_strings(series), unique_count=unique_count,
                is_pk_candidate=(unique_count == len(self.data_frame)) and (null_count == 0),
                whitespace_issues=self.count_whitespace_issues(series),
                outlier_count=self.detect_outliers(series),
                inconsistency_count=inconst_count,
                placeholders_found=self.find_placeholders(series),
                naming_issues=self.validate_header_name(col),
                top_values=top_5, stats=stats,
                unique_values_sample=unique_sample,
                detected_format=detected_format,
                is_redundant=is_redundant,
                is_boolean_candidate=is_boolean,
                date_format_consistency=date_consistency
            ))
        return profiles

    def detect_real_type(self, series):
        non_null = series.dropna()
        if non_null.empty: return "empty", 0
        num_v = pd.to_numeric(non_null, errors='coerce').notnull().sum()
        date_v = pd.to_datetime(non_null, errors='coerce').notnull().sum()
        if num_v / len(non_null) > 0.8: return "numeric", len(non_null) - num_v
        if date_v / len(non_null) > 0.8: return "datetime", len(non_null) - date_v
        return "string", 0

    def identify_data_format_patterns(self, series) -> Optional[str]:
        patterns = {
            'Email': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
            'URL': r'^https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+',
            'Phone': r'^\+?1?\d{9,15}$'
        }
        sample = series.dropna().astype(str).head(100)
        if sample.empty: return None
        for name, regex in patterns.items():
            if sample.str.match(regex).sum() / len(sample) > 0.5:
                return name
        return None

    def collect_unique_values_sample(self, series, limit=20) -> List[Any]:
        return series.dropna().unique()[:limit].tolist()

    def get_most_frequent_values(self, series, limit=5) -> List[str]:
        if series.empty: return []
        dist = series.value_counts(normalize=True).head(limit)
        return [f"{val} ({perc:.1%})" for val, perc in dist.items()]

    def count_empty_strings(self, series) -> int:
        if series.dtype == 'object':
            return int(series.apply(lambda x: str(x).strip() == '' or str(x).lower() == 'nan').sum() - series.isnull().sum())
        return 0

    def count_whitespace_issues(self, series) -> int:
        if series.dtype == 'object':
            return int(series.apply(lambda x: len(str(x)) != len(str(x).strip()) if pd.notnull(x) else False).sum())
        return 0

    def detect_outliers(self, series) -> int:
        nums = pd.to_numeric(series, errors='coerce').dropna()
        if nums.empty: return 0
        q1, q3 = nums.quantile(0.25), nums.quantile(0.75)
        iqr = q3 - q1
        return int(((nums < q1 - 1.5*iqr) | (nums > q3 + 1.5*iqr)).sum())

    def find_placeholders(self, series) -> Dict[str, int]:
        plist = ['n/a', 'unknown', 'none', 'null', 'nan', '?', '-', '999']
        clean = series.astype(str).str.lower().str.strip()
        return {p: int((clean == p).sum()) for p in plist if (clean == p).any()}

    def validate_header_name(self, name) -> List[str]:
        errs = []
        if ' ' in name: errs.append("Spaces")
        if any(not c.isalnum() and c not in ['_', '-'] for c in name): errs.append("Special Chars")
        return errs

    def check_date_format_consistency(self, series) -> str:
        # Very basic consistency check: check lengths of date strings
        non_null = series.dropna().astype(str).head(100)
        if non_null.empty: return "consistent"
        lengths = non_null.apply(len).unique()
        return "consistent" if len(lengths) == 1 else "mixed formats likely"

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

    def calculate_overall_quality_score(self, nulls, dups) -> float:
        total = self.data_frame.size
        if total == 0: return 100.0
        return round(max(0, 100 - ((nulls + dups) / total * 100)), 2)

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
        report = self.execute_full_quality_audit()
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
