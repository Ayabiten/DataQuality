from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Optional, Any, Dict, List
import json
import os
import csv
import traceback
import logging
from contextlib import contextmanager

@dataclass
class BaseLog:
    """Base class for all audit logs."""
    timestamp: str = field(default_factory=lambda: datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    level: str = "ERROR"

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def __str__(self) -> str:
        """Pretty-print for notebook logs."""
        return f"[{self.timestamp}] {self.level}: {self.__class__.__name__.replace('Log', '')}"

@dataclass
class RequestErrorLog(BaseLog):
    """Logs errors encountered during HTTP requests."""
    url: str = ""
    method: str = "GET"
    status_code: Optional[int] = None
    error_message: str = ""
    request_payload: Any = None
    response_content: Any = None
    headers: Optional[Dict[str, str]] = None

    def __str__(self) -> str:
        base = super().__str__()
        return f"{base} | {self.method} {self.url} | Status: {self.status_code} | Msg: {self.error_message}"

@dataclass
class FileErrorLog(BaseLog):
    """Logs errors encountered during file processing (CSV, Excel, JSON)."""
    file_path: str = ""
    file_type: str = ""  # CSV, Excel, JSON
    error_type: str = "" # Schema, Type, Structural
    error_message: str = ""
    row: Optional[int] = None
    column: Optional[str] = None
    invalid_value: Any = None
    context: Optional[Dict[str, Any]] = None

    def __str__(self) -> str:
        base = super().__str__()
        file_name = os.path.basename(self.file_path) if self.file_path else "Unknown"
        location = f"Row: {self.row}" if self.row is not None else ""
        if self.column: location += f", Col: {self.column}"
        return f"{base} | File: {file_name} ({self.file_type}) | Error: {self.error_type} | {location} | {self.error_message}"

@dataclass
class GenericLog(BaseLog):
    """Generic status or informational log."""
    message: str = ""
    category: str = "GENERAL"

    def __str__(self) -> str:
        base = super().__str__()
        return f"{base} | {self.message}"

class DataQualityLogger:
    """Central Logger for Data Quality Audits and Requests."""
    
    def __init__(self, log_name: str = "AuditLog", log_dir: str = "logs", log_to_file: bool = True):
        self.log_name = log_name
        self.log_dir = log_dir
        self.request_logs: List[RequestErrorLog] = []
        self.file_logs: List[FileErrorLog] = []
        self.general_logs: List[GenericLog] = []
        
        # Setup standard logging
        self.std_logger = logging.getLogger(log_name)
        self.std_logger.setLevel(logging.INFO)
        
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        
        # Console handler
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        if not self.std_logger.handlers:
            self.std_logger.addHandler(ch)
            
        # File handler
        if log_to_file:
            if not os.path.exists(log_dir):
                os.makedirs(log_dir)
            self.log_path = os.path.abspath(os.path.join(log_dir, f"{log_name}.log"))
            fh = logging.FileHandler(self.log_path)
            fh.setFormatter(formatter)
            self.std_logger.addHandler(fh)
        else:
            self.log_path = None

    def log_request(self, **kwargs):
        """Record a request-related event (Error, Success, etc.)."""
        log = RequestErrorLog(**kwargs)
        self.request_logs.append(log)
        self._dispatch_log(log)
        return log

    def log_file_error(self, **kwargs):
        """Record a file processing event."""
        log = FileErrorLog(**kwargs)
        self.file_logs.append(log)
        self._dispatch_log(log)
        return log

    def log_generic(self, message: str, level: str = "INFO", category: str = "GENERAL"):
        """Record a general status message."""
        log = GenericLog(message=message, level=level, category=category)
        self.general_logs.append(log)
        self._dispatch_log(log)
        return log

    # Success/Info/Status Helpers
    def info(self, message: str): return self.log_generic(message, "INFO", "INFO")
    def success(self, message: str): return self.log_generic(message, "INFO", "SUCCESS")
    def warning(self, message: str): return self.log_generic(message, "WARNING", "WARNING")
    def error(self, message: str): return self.log_generic(message, "ERROR", "ERROR")

    def _dispatch_log(self, log):
        """Dispatches the log to the standard internal logger with correct level."""
        msg = str(log)
        if log.level == "ERROR":
            self.std_logger.error(msg)
        elif log.level == "WARNING":
            self.std_logger.warning(msg)
        else:
            self.std_logger.info(msg)

    def log_exception(self, file_path: Optional[str] = None, error_type: str = "UnhandledException"):
        """Convenience method to log a caught exception."""
        msg = traceback.format_exc()
        if file_path:
            ext = os.path.splitext(file_path)[1].upper().replace('.', '')
            return self.log_file_error(file_path=file_path, file_type=ext, error_type=error_type, error_message=msg)
        else:
            return self.log_request(error_message=msg)

    @contextmanager
    def scenario_file(self, file_path: str, error_type: str = "ProcessError"):
        """Scenario: Handling File Processing."""
        self.std_logger.info(f"Starting Scenario: File Audit for {file_path}")
        try:
            yield
        except Exception as e:
            self.log_exception(file_path=file_path, error_type=error_type)
            raise # Re-raise if necessary or handle it here

    @contextmanager
    def scenario_request(self, url: str, method: str = "GET"):
        """Scenario: Handling API Requests."""
        self.std_logger.info(f"Starting Scenario: Request to {url}")
        try:
            yield
        except Exception as e:
            self.log_request(url=url, method=method, error_message=str(e))
            raise

    def finalize_log(self):
        """Finalizes the .log file and provides the path."""
        self.std_logger.info(f"Audit session finalized. Full log available at: {self.log_path}")
        return self.log_path

    def get_df(self, log_type: str = 'file') -> 'pd.DataFrame':
        """Returns logs as a Pandas DataFrame."""
        import pandas as pd
        if log_type == 'file': data = self.file_logs
        elif log_type == 'request': data = self.request_logs
        else: data = self.general_logs
        
        if not data:
            return pd.DataFrame()
        return pd.DataFrame([asdict(l) for l in data])

    def export_json(self, file_path: str):
        """Exports all logs to a JSON file."""
        all_logs = {
            "request_logs": [asdict(l) for l in self.request_logs],
            "file_logs": [asdict(l) for l in self.file_logs]
        }
        with open(file_path, 'w') as f:
            json.dump(all_logs, f, indent=4)
        self.std_logger.info(f"Logs exported to JSON: {file_path}")

    def get_summary(self) -> Dict[str, Any]:
        """Returns a summary of the logging session."""
        summary = {
            "session_name": self.log_name,
            "total_request_logs": len(self.request_logs),
            "total_file_logs": len(self.file_logs),
            "total_general_logs": len(self.general_logs),
            "file_error_types": list(set(log.error_type for log in self.file_logs)),
            "log_file": self.log_path,
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        return summary

    def log_to_db(self, db, table_name_prefix: str = "audit"):
        """Logs all collected errors to a database table using DataModel."""
        df_file = self.get_df('file')
        df_req = self.get_df('request')
        if not df_file.empty:
            db.create(f"{table_name_prefix}_file_errors", df_file)
        if not df_req.empty:
            db.create(f"{table_name_prefix}_request_errors", df_req)
        self.std_logger.info(f"Logs synced to database with prefix: {table_name_prefix}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.log_exception(error_type="SessionFatalError")
        self.finalize_log()

    def __repr__(self) -> str:
        return f"<DataQualityLogger: Session '{self.log_name}' active. File output: logs/{self.log_name}.log>"
