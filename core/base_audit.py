import pandas as pd
import numpy as np
import re
import os
from typing import Dict, List, Any, Optional, Tuple
from abc import ABC, abstractmethod
from thefuzz import process
from core.config import DATA_PATTERNS, PLACEHOLDERS, QUALITY_THRESHOLDS
from core.models import AuditSummary, ColumnProfile

class BaseQualityAudit(ABC):
    """
    Abstract Base Class for all Data Quality Audits.
    Provides shared logic for pattern detection, scoring, and hygiene checks.
    """

    def __init__(self, logger=None):
        self.logger = logger
        self.quality_stats = {}

    def identify_data_format_patterns(self, series: pd.Series) -> Optional[str]:
        """Detects if a column matches a known data pattern."""
        sample = series.dropna().astype(str).head(200)
        if sample.empty: return None
        
        for name, regex in DATA_PATTERNS.items():
            if sample.str.match(regex).sum() / len(sample) > 0.4:
                return name
        return None

    def detect_placeholders(self, series: pd.Series) -> Dict[str, int]:
        """Identifies dummy/placeholder values in a column."""
        normalized = series.astype(str).str.lower().str.strip()
        found = {}
        for p in PLACEHOLDERS:
            count = (normalized == p).sum()
            if count > 0:
                found[p] = int(count)
        return found

    def check_header_hygiene(self, columns: List[str]) -> List[Dict[str, Any]]:
        """Audits column names for naming convention issues."""
        issues = []
        for col in columns:
            col_issues = []
            if ' ' in col: col_issues.append("Contains Spaces")
            if any(not c.isalnum() and c not in ['_', '-'] for c in col): col_issues.append("Special Characters")
            if col != col.strip(): col_issues.append("Leading/Trailing Whitespace")
            
            if col_issues:
                issues.append({'column': col, 'issues': col_issues})
        return issues

    def suggest_header_corrections(self, columns: List[str], target_headers: List[str]) -> Dict[str, str]:
        """Suggests corrections for headers based on a target list using fuzzy matching."""
        suggestions = {}
        for col in columns:
            match, score = process.extractOne(col, target_headers)
            if score > 80 and match != col:
                suggestions[col] = match
        return suggestions

    def calculate_overall_quality_score(self, total_nulls: int, total_rows: int, total_cols: int, duplicates: int) -> float:
        """Calculates a normalized quality score (0-100)."""
        if total_rows == 0 or total_cols == 0: return 0.0
        total_cells = total_rows * total_cols
        
        null_penalty = (total_nulls / total_cells) * 100
        dup_penalty = (duplicates / total_rows) * 100 if total_rows > 0 else 0
        
        score = max(0, 100 - (null_penalty + dup_penalty))
        return round(score, 2)

    def perform_column_profiling(self, df: pd.DataFrame) -> List[ColumnProfile]:
        """Shared logic for profiling columns across any DataFrame."""
        profiles = []
        for col in df.columns:
            series = df[col]
            null_count = int(series.isnull().sum())
            unique_count = int(series.nunique())
            
            # Use shared detection logic
            detected_format = self.identify_data_format_patterns(series)
            placeholders = self.detect_placeholders(series)
            hygiene = self.check_header_hygiene([col])
            
            # Type detection (can be overridden but shared here)
            inferred_type, inconst_count = self._detect_real_type(series)
            
            stats = {}
            if inferred_type == 'numeric':
                num_s = pd.to_numeric(series, errors='coerce').dropna()
                if not num_s.empty:
                    stats = {'min': float(num_s.min()), 'max': float(num_s.max()), 'mean': float(num_s.mean())}

            profiles.append(ColumnProfile(
                name=col, pandas_type=str(series.dtype), detected_type=inferred_type,
                null_count=null_count, null_percentage=round((null_count/len(df))*100, 2) if len(df) > 0 else 0.0,
                empty_strings=int(series.apply(lambda x: str(x).strip() == '' or str(x).lower() == 'nan').sum() - null_count) if series.dtype == 'object' else 0,
                unique_count=unique_count,
                is_pk_candidate=(unique_count == len(df)) and (null_count == 0),
                whitespace_issues=int(series.apply(lambda x: len(str(x)) != len(str(x).strip()) if pd.notnull(x) else False).sum()) if series.dtype == 'object' else 0,
                outlier_count=self._detect_outliers(series),
                inconsistency_count=inconst_count,
                placeholders_found=placeholders,
                naming_issues=[i['issues'] for i in hygiene if i['column'] == col][0] if hygiene else [],
                top_values=[f"{v} ({p:.1%})" for v, p in series.value_counts(normalize=True).head(5).items()],
                stats=stats,
                unique_values_sample=series.dropna().unique()[:20].tolist(),
                detected_format=detected_format,
                is_redundant=(unique_count <= 1),
                is_boolean_candidate=(unique_count == 2)
            ))
        return profiles

    def _detect_real_type(self, series: pd.Series) -> Tuple[str, int]:
        non_null = series.dropna()
        if non_null.empty: return "empty", 0
        num_v = pd.to_numeric(non_null, errors='coerce').notnull().sum()
        try:
            date_v = pd.to_datetime(non_null, errors='coerce', format='mixed').notnull().sum()
        except:
            date_v = pd.to_datetime(non_null, errors='coerce').notnull().sum()
        
        if num_v / len(non_null) > 0.8: return "numeric", len(non_null) - num_v
        if date_v / len(non_null) > 0.8: return "datetime", len(non_null) - date_v
        return "string", 0

    def _detect_outliers(self, series: pd.Series) -> int:
        nums = pd.to_numeric(series, errors='coerce').dropna()
        if nums.empty: return 0
        q1, q3 = nums.quantile(0.25), nums.quantile(0.75)
        iqr = q3 - q1
        return int(((nums < q1 - 1.5*iqr) | (nums > q3 + 1.5*iqr)).sum())

    def get_quality_label(self, score: float) -> str:
        """Returns a human-readable quality label."""
        for label, threshold in sorted(QUALITY_THRESHOLDS.items(), key=lambda x: x[1], reverse=True):
            if score >= threshold:
                return label
        return "Poor"

    @abstractmethod
    def execute_full_audit(self):
        """Must be implemented by subclasses."""
        pass
