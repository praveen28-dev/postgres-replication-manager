"""
Centralized Logging Module
Provides structured logging with timestamps and log levels.
"""

import json
import logging
import sys
from datetime import datetime
from typing import Any, Dict, Optional


class StructuredFormatter(logging.Formatter):
    """Custom formatter for structured JSON logging."""
    
    def __init__(self, include_timestamp: bool = True, include_level: bool = True):
        super().__init__()
        self.include_timestamp = include_timestamp
        self.include_level = include_level
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record as structured JSON."""
        log_data: Dict[str, Any] = {}
        
        if self.include_timestamp:
            log_data["timestamp"] = datetime.utcnow().isoformat() + "Z"
        
        if self.include_level:
            log_data["level"] = record.levelname
        
        log_data["message"] = record.getMessage()
        log_data["logger"] = record.name
        
        # Add extra fields if present
        if hasattr(record, "extra_data") and record.extra_data:
            log_data["data"] = record.extra_data
        
        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        
        return json.dumps(log_data, default=str)


class ConsoleFormatter(logging.Formatter):
    """Human-readable console formatter with colors."""
    
    # ANSI color codes
    COLORS = {
        "DEBUG": "\033[36m",     # Cyan
        "INFO": "\033[32m",      # Green
        "WARNING": "\033[33m",   # Yellow
        "ERROR": "\033[31m",     # Red
        "CRITICAL": "\033[35m",  # Magenta
    }
    RESET = "\033[0m"
    BOLD = "\033[1m"
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record with colors and structure."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        color = self.COLORS.get(record.levelname, "")
        
        # Build the formatted message
        formatted = f"{timestamp} | {color}{self.BOLD}{record.levelname:8}{self.RESET} | {record.getMessage()}"
        
        # Add extra data if present
        if hasattr(record, "extra_data") and record.extra_data:
            formatted += f" | {json.dumps(record.extra_data, default=str)}"
        
        # Add exception info if present
        if record.exc_info:
            formatted += f"\n{self.formatException(record.exc_info)}"
        
        return formatted


class ReplicationLogger:
    """
    Centralized logger for the replication manager.
    Supports both structured JSON and human-readable console output.
    """
    
    _instance: Optional["ReplicationLogger"] = None
    _logger: Optional[logging.Logger] = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if self._logger is None:
            self._logger = logging.getLogger("replication-manager")
    
    def configure(
        self,
        level: str = "INFO",
        use_json: bool = False,
        log_file: Optional[str] = None,
    ) -> None:
        """
        Configure the logger with specified settings.
        
        Args:
            level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            use_json: Whether to use JSON structured logging
            log_file: Optional file path to write logs to
        """
        self._logger.setLevel(getattr(logging, level.upper(), logging.INFO))
        self._logger.handlers.clear()
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        if use_json:
            console_handler.setFormatter(StructuredFormatter())
        else:
            console_handler.setFormatter(ConsoleFormatter())
        self._logger.addHandler(console_handler)
        
        # File handler if specified — pre-create with restricted permissions
        if log_file:
            import os
            from pathlib import Path
            log_path = Path(log_file)
            log_path.parent.mkdir(parents=True, exist_ok=True)
            # Create file with owner-only permissions (0o600) before handler opens it.
            # This prevents other users from reading potentially sensitive log content.
            fd = os.open(str(log_path), os.O_CREAT | os.O_WRONLY | os.O_APPEND, 0o600)
            os.close(fd)

            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(StructuredFormatter())
            self._logger.addHandler(file_handler)
    
    def _log(
        self,
        level: int,
        message: str,
        extra_data: Optional[Dict[str, Any]] = None,
        exc_info: bool = False,
    ) -> None:
        """Internal logging method with extra data support."""
        record = self._logger.makeRecord(
            self._logger.name,
            level,
            "(unknown)",
            0,
            message,
            (),
            None if not exc_info else sys.exc_info(),
        )
        record.extra_data = extra_data
        self._logger.handle(record)
    
    def debug(self, message: str, **kwargs) -> None:
        """Log debug message."""
        self._log(logging.DEBUG, message, kwargs if kwargs else None)
    
    def info(self, message: str, **kwargs) -> None:
        """Log info message."""
        self._log(logging.INFO, message, kwargs if kwargs else None)
    
    def warning(self, message: str, **kwargs) -> None:
        """Log warning message."""
        self._log(logging.WARNING, message, kwargs if kwargs else None)
    
    def error(self, message: str, exc_info: bool = False, **kwargs) -> None:
        """Log error message."""
        self._log(logging.ERROR, message, kwargs if kwargs else None, exc_info=exc_info)
    
    def critical(self, message: str, exc_info: bool = False, **kwargs) -> None:
        """Log critical message."""
        self._log(logging.CRITICAL, message, kwargs if kwargs else None, exc_info=exc_info)
    
    # Step-specific logging methods
    def log_step_start(self, step_name: str, step_number: int) -> None:
        """Log the start of a workflow step."""
        self.info(f"[Step {step_number}] Starting: {step_name}", step=step_number, action="start")
    
    def log_step_complete(self, step_name: str, step_number: int) -> None:
        """Log the completion of a workflow step."""
        self.info(f"[Step {step_number}] Completed: {step_name}", step=step_number, action="complete")
    
    def log_step_failed(self, step_name: str, step_number: int, error: str) -> None:
        """Log a failed workflow step."""
        self.error(f"[Step {step_number}] Failed: {step_name} - {error}", step=step_number, action="failed")


# Global logger instance
def get_logger() -> ReplicationLogger:
    """Get the global logger instance."""
    return ReplicationLogger()
