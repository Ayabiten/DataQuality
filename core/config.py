import re

# Standardized Data Patterns for Profiling
DATA_PATTERNS = {
    'Email': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
    'URL': r'^https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+',
    'Phone': r'^\+?1?[\s.-]?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}$',
    'IPv4': r'^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$',
    'CreditCard': r'^(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14}|3[47][0-9]{13}|3(?:0[0-5]|[68][0-9])[0-9]{11}|6(?:011|5[0-9]{2})[0-9]{12}|(?:2131|1800|35\d{3})\d{11})$',
    'Date (ISO)': r'^\d{4}-\d{2}-\d{2}$'
}

# Common Placeholders/Dummy Values
PLACEHOLDERS = [
    'n/a', 'unknown', 'none', 'null', 'nan', '?', '-', '.', 
    '999', '000', '0000-00-00', 'tbd', 'undefined'
]

# Audit Score Thresholds
QUALITY_THRESHOLDS = {
    'Excellent': 95,
    'Good': 85,
    'Fair': 70,
    'Poor': 0
}

# Visualization Settings
VIZ_SETTINGS = {
    'palette': 'viridis',
    'figure_size': (10, 6),
    'dpi': 100
}
