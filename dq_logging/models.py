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

class DataQualityLogger:
    """Central Logger for Data Quality Audits and Requests."""
    
    def __init__(self, log_name: str = "AuditLog", log_dir: str = "logs", log_to_file: bool = True):
        self.log_name = log_name
        self.log_dir = log_dir
        self.request_logs: List[RequestErrorLog] = []
        self.file_logs: List[FileErrorLog] = []
        
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
        """Record a request-related error."""
        log = RequestErrorLog(**kwargs)
        self.request_logs.append(log)
        self.std_logger.error(str(log))
        return log

    def log_file_error(self, **kwargs):
        """Record a file processing error."""
        log = FileErrorLog(**kwargs)
        self.file_logs.append(log)
        self.std_logger.error(str(log))
        return log

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

    def __repr__(self) -> str:
        return f"<DataQualityLogger: Session '{self.log_name}' active. File output: logs/{self.log_name}.log>"
