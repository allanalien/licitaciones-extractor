"""
Logging configuration for licitaciones extractor.
"""

import os
import sys
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional

class StructuredFormatter(logging.Formatter):
    """Custom formatter for structured JSON logs."""

    def format(self, record: logging.LogRecord) -> str:
        """
        Format log record as structured JSON.

        Args:
            record: Log record to format

        Returns:
            JSON-formatted log string
        """
        log_data = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "component": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno
        }

        # Add extra data if available
        if hasattr(record, 'extra_data') and record.extra_data:
            log_data["metadata"] = record.extra_data

        # Add exception information if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_data, ensure_ascii=False, separators=(',', ':'))

class ExtractionLogger:
    """Specialized logger for extraction operations."""

    def __init__(self, name: str):
        """
        Initialize extraction logger.

        Args:
            name: Logger name (typically the component name)
        """
        self.logger = logging.getLogger(name)
        self.component = name

    def info(self, message: str, extra_data: Optional[Dict[str, Any]] = None):
        """Log info message with optional metadata."""
        if extra_data:
            self.logger.info(message, extra={"extra_data": extra_data})
        else:
            self.logger.info(message)

    def warning(self, message: str, extra_data: Optional[Dict[str, Any]] = None):
        """Log warning message with optional metadata."""
        if extra_data:
            self.logger.warning(message, extra={"extra_data": extra_data})
        else:
            self.logger.warning(message)

    def error(self, message: str, extra_data: Optional[Dict[str, Any]] = None):
        """Log error message with optional metadata."""
        if extra_data:
            self.logger.error(message, extra={"extra_data": extra_data})
        else:
            self.logger.error(message)

    def debug(self, message: str, extra_data: Optional[Dict[str, Any]] = None):
        """Log debug message with optional metadata."""
        if extra_data:
            self.logger.debug(message, extra={"extra_data": extra_data})
        else:
            self.logger.debug(message)

    def log_extraction_start(self, source: str, parameters: Dict[str, Any]):
        """
        Log the start of an extraction operation.

        Args:
            source: Data source name
            parameters: Extraction parameters
        """
        extra_data = {
            "operation": "extraction_start",
            "source": source,
            "parameters": parameters
        }
        self.logger.info(f"Starting extraction from {source}", extra={"extra_data": extra_data})

    def log_extraction_end(self, source: str, records_count: int, execution_time: float, success: bool = True):
        """
        Log the end of an extraction operation.

        Args:
            source: Data source name
            records_count: Number of records extracted
            execution_time: Execution time in seconds
            success: Whether the operation was successful
        """
        extra_data = {
            "operation": "extraction_end",
            "source": source,
            "records_processed": records_count,
            "execution_time": execution_time,
            "success": success
        }

        level = logging.INFO if success else logging.ERROR
        message = f"Completed extraction from {source}: {records_count} records in {execution_time:.2f}s"
        self.logger.log(level, message, extra={"extra_data": extra_data})

    def log_data_quality(self, source: str, quality_metrics: Dict[str, float]):
        """
        Log data quality metrics.

        Args:
            source: Data source name
            quality_metrics: Quality metrics dictionary
        """
        extra_data = {
            "operation": "data_quality",
            "source": source,
            "quality_metrics": quality_metrics
        }
        self.logger.info(f"Data quality metrics for {source}", extra={"extra_data": extra_data})

    def log_error(self, source: str, error: Exception, operation: str = "extraction"):
        """
        Log an error with context.

        Args:
            source: Data source name
            error: Exception that occurred
            operation: Operation being performed
        """
        extra_data = {
            "operation": f"{operation}_error",
            "source": source,
            "error_type": type(error).__name__,
            "error_message": str(error)
        }
        self.logger.error(f"Error in {operation} for {source}: {error}", extra={"extra_data": extra_data})

def setup_logging(
    log_level: str = "INFO",
    log_file: Optional[str] = None,
    console_output: bool = True,
    structured_format: bool = True
) -> None:
    """
    Set up logging configuration for the application.

    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Path to log file (if None, uses default)
        console_output: Whether to output logs to console
        structured_format: Whether to use structured JSON format
    """
    # Convert string level to logging constant
    level = getattr(logging, log_level.upper(), logging.INFO)

    # Create logs directory if it doesn't exist
    logs_dir = Path(__file__).parent.parent.parent / "logs"
    logs_dir.mkdir(exist_ok=True)

    # Default log file path
    if log_file is None:
        timestamp = datetime.now().strftime("%Y%m%d")
        log_file = logs_dir / f"licitaciones_extractor_{timestamp}.log"

    # Clear existing handlers
    root_logger = logging.getLogger()
    root_logger.handlers.clear()

    # Set root logger level
    root_logger.setLevel(level)

    # Create formatter
    if structured_format:
        formatter = StructuredFormatter()
    else:
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

    # File handler
    if log_file:
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

    # Console handler
    if console_output:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)

        # Use simple format for console if structured format is used for file
        if structured_format and log_file:
            console_formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            console_handler.setFormatter(console_formatter)
        else:
            console_handler.setFormatter(formatter)

        root_logger.addHandler(console_handler)

    # Set third-party loggers to WARNING to reduce noise
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)

    # Log configuration
    logger = logging.getLogger("licitaciones_extractor.logger")
    logger.info(f"Logging initialized - Level: {log_level}, File: {log_file}, Console: {console_output}")

def get_logger(name: str) -> ExtractionLogger:
    """
    Get a configured logger for extraction operations.

    Args:
        name: Logger name

    Returns:
        Configured ExtractionLogger instance
    """
    return ExtractionLogger(f"licitaciones_extractor.{name}")

def log_system_info():
    """Log system information at startup."""
    import platform
    import psutil

    logger = logging.getLogger("licitaciones_extractor.system")

    system_info = {
        "platform": platform.platform(),
        "python_version": platform.python_version(),
        "cpu_count": psutil.cpu_count(),
        "memory_total": psutil.virtual_memory().total,
        "memory_available": psutil.virtual_memory().available
    }

    logger.info("System information", extra={"extra_data": system_info})