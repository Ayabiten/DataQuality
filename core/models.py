import os
from typing import Dict, List, Any, Optional

class AuditSummary:
    """
    A Model class to represent the high-level summary of a quality audit.
    """
    def __init__(self, file_name, abs_path, size_mb, last_modified, total_rows, total_cols, 
                 mismatched_rows=0, multi_line_rows=0, duplicate_rows=0, total_nulls=0, 
                 quality_score=0.0, completeness_score=0.0, healed_rows=0, 
                 encoding=None, confidence=0.0, delimiter=None, has_header=True):
        self.file_name = file_name
        self.abs_path = abs_path
        self.size_mb = size_mb
        self.last_modified = last_modified
        self.total_rows = total_rows
        self.total_cols = total_cols
        self.mismatched_rows = mismatched_rows
        self.multi_line_rows = multi_line_rows
        self.duplicate_rows = duplicate_rows
        self.total_nulls = total_nulls
        self.quality_score = quality_score
        self.completeness_score = completeness_score
        self.healed_rows = healed_rows
        # Optional fields for CSV
        self.encoding = encoding
        self.encoding_confidence = confidence
        self.delimiter = delimiter
        self.has_header = has_header

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
        self.unique_values_sample = unique_values_sample or []
        self.detected_format = detected_format
        self.is_redundant = is_redundant
        self.is_boolean_candidate = is_boolean_candidate
        self.date_format_consistency = date_format_consistency

    def to_dict(self):
        return vars(self)
