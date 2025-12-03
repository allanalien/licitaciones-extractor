"""
Configuration settings for licitaciones extractor.
"""

import os
from pathlib import Path
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

@dataclass
class DatabaseConfig:
    """Database configuration."""
    url: str
    pool_size: int = 10
    max_overflow: int = 20
    pool_recycle: int = 300
    echo: bool = False

@dataclass
class APIConfig:
    """API configuration for external services."""
    licita_ya_api_key: str
    licita_ya_base_url: str = "https://www.licitaya.com.mx/api/v1"
    cdmx_base_url: str = "https://datosabiertostianguisdigital.cdmx.gob.mx/api/v1"
    request_timeout: int = 30
    retry_attempts: int = 3
    retry_delay: float = 1.0

@dataclass
class ScrapingConfig:
    """Web scraping configuration."""
    selenium_timeout: int = 30
    headless_browser: bool = True
    user_agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    page_load_timeout: int = 30
    implicit_wait: int = 10

@dataclass
class EmbeddingsConfig:
    """Embeddings generation configuration."""
    openai_api_key: Optional[str]
    model: str = "text-embedding-ada-002"
    dimensions: int = 1536
    expected_dimension: int = 1536
    batch_size: int = 100
    max_tokens: int = 8191
    max_retries: int = 3
    retry_delay_seconds: float = 2.0

@dataclass
class LoggingConfig:
    """Logging configuration."""
    level: str = "INFO"
    file_path: Optional[str] = None
    console_output: bool = True
    structured_format: bool = True
    max_file_size: int = 10_000_000  # 10MB
    backup_count: int = 5

@dataclass
class SchedulerConfig:
    """Scheduler configuration."""
    extraction_time: str = "06:00"
    timezone: str = "America/Mexico_City"
    max_execution_time: int = 3600  # 1 hour
    batch_size: int = 100
    retry_attempts: int = 3
    retry_delay_seconds: float = 5.0

class Settings:
    """Main settings class."""

    def __init__(self):
        """Initialize settings from environment variables."""
        self.database = self._get_database_config()
        self.api = self._get_api_config()
        self.scraping = self._get_scraping_config()
        self.embeddings = self._get_embeddings_config()
        self.logging = self._get_logging_config()
        self.scheduler = self._get_scheduler_config()

        # Project paths
        self.project_root = Path(__file__).parent.parent.parent
        self.logs_dir = self.project_root / "logs"
        self.data_dir = self.project_root / "data"

        # Ensure directories exist
        self.logs_dir.mkdir(exist_ok=True)
        self.data_dir.mkdir(exist_ok=True)

    def _get_database_config(self) -> DatabaseConfig:
        """Get database configuration from environment."""
        db_url = os.getenv('POSTGRES_URL')
        if not db_url:
            # Use placeholder for testing/import - will fail on actual database operations
            db_url = "postgresql://placeholder:placeholder@localhost:5432/placeholder"

        return DatabaseConfig(
            url=db_url,
            pool_size=int(os.getenv('DB_POOL_SIZE', '10')),
            max_overflow=int(os.getenv('DB_MAX_OVERFLOW', '20')),
            pool_recycle=int(os.getenv('DB_POOL_RECYCLE', '300')),
            echo=os.getenv('DB_ECHO', 'false').lower() == 'true'
        )

    def _get_api_config(self) -> APIConfig:
        """Get API configuration from environment."""
        api_key = os.getenv('LICITA_YA_API_KEY')
        if not api_key:
            # Use placeholder for testing/import
            api_key = "placeholder_api_key"

        return APIConfig(
            licita_ya_api_key=api_key,
            licita_ya_base_url=os.getenv('LICITA_YA_BASE_URL', "https://www.licitaya.com.mx/api/v1"),
            cdmx_base_url=os.getenv('CDMX_BASE_URL', "https://datosabiertostianguisdigital.cdmx.gob.mx/api/v1"),
            request_timeout=int(os.getenv('REQUEST_TIMEOUT', '30')),
            retry_attempts=int(os.getenv('RETRY_ATTEMPTS', '3')),
            retry_delay=float(os.getenv('RETRY_DELAY', '1.0'))
        )

    def _get_scraping_config(self) -> ScrapingConfig:
        """Get scraping configuration from environment."""
        return ScrapingConfig(
            selenium_timeout=int(os.getenv('SELENIUM_TIMEOUT', '30')),
            headless_browser=os.getenv('HEADLESS_BROWSER', 'true').lower() == 'true',
            user_agent=os.getenv('USER_AGENT',
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"),
            page_load_timeout=int(os.getenv('PAGE_LOAD_TIMEOUT', '30')),
            implicit_wait=int(os.getenv('IMPLICIT_WAIT', '10'))
        )

    def _get_embeddings_config(self) -> EmbeddingsConfig:
        """Get embeddings configuration from environment."""
        return EmbeddingsConfig(
            openai_api_key=os.getenv('OPENAI_API_KEY'),
            model=os.getenv('EMBEDDING_MODEL', 'text-embedding-ada-002'),
            dimensions=int(os.getenv('EMBEDDING_DIMENSIONS', '1536')),
            expected_dimension=int(os.getenv('EMBEDDING_EXPECTED_DIMENSION', '1536')),
            batch_size=int(os.getenv('EMBEDDING_BATCH_SIZE', '100')),
            max_tokens=int(os.getenv('EMBEDDING_MAX_TOKENS', '8191')),
            max_retries=int(os.getenv('EMBEDDING_MAX_RETRIES', '3')),
            retry_delay_seconds=float(os.getenv('EMBEDDING_RETRY_DELAY', '2.0'))
        )

    def _get_logging_config(self) -> LoggingConfig:
        """Get logging configuration from environment."""
        return LoggingConfig(
            level=os.getenv('LOG_LEVEL', 'INFO').upper(),
            file_path=os.getenv('LOG_FILE'),
            console_output=os.getenv('LOG_CONSOLE', 'true').lower() == 'true',
            structured_format=os.getenv('LOG_STRUCTURED', 'true').lower() == 'true',
            max_file_size=int(os.getenv('LOG_MAX_SIZE', '10000000')),
            backup_count=int(os.getenv('LOG_BACKUP_COUNT', '5'))
        )

    def _get_scheduler_config(self) -> SchedulerConfig:
        """Get scheduler configuration from environment."""
        return SchedulerConfig(
            extraction_time=os.getenv('EXTRACTION_TIME', '06:00'),
            timezone=os.getenv('TIMEZONE', 'America/Mexico_City'),
            max_execution_time=int(os.getenv('MAX_EXECUTION_TIME', '3600')),
            batch_size=int(os.getenv('BATCH_SIZE', '100')),
            retry_attempts=int(os.getenv('SCHEDULER_RETRY_ATTEMPTS', '3')),
            retry_delay_seconds=float(os.getenv('SCHEDULER_RETRY_DELAY', '5.0'))
        )

    def validate(self) -> List[str]:
        """
        Validate configuration settings.

        Returns:
            List of validation errors (empty if valid)
        """
        errors = []

        # Database validation
        if not self.database.url:
            errors.append("Database URL is required")

        # API validation
        if not self.api.licita_ya_api_key:
            errors.append("Licita Ya API key is required")

        # Embeddings validation (if enabled)
        if self.embeddings.openai_api_key and not self.embeddings.model:
            errors.append("Embeddings model is required when OpenAI API key is provided")

        # Scheduler validation
        try:
            hour, minute = self.scheduler.extraction_time.split(':')
            if not (0 <= int(hour) <= 23) or not (0 <= int(minute) <= 59):
                errors.append("Invalid extraction time format (must be HH:MM)")
        except ValueError:
            errors.append("Invalid extraction time format (must be HH:MM)")

        return errors

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert settings to dictionary (excluding sensitive data).

        Returns:
            Dictionary representation of settings
        """
        return {
            "database": {
                "pool_size": self.database.pool_size,
                "max_overflow": self.database.max_overflow,
                "pool_recycle": self.database.pool_recycle,
                "echo": self.database.echo
            },
            "api": {
                "licita_ya_base_url": self.api.licita_ya_base_url,
                "cdmx_base_url": self.api.cdmx_base_url,
                "request_timeout": self.api.request_timeout,
                "retry_attempts": self.api.retry_attempts,
                "retry_delay": self.api.retry_delay
            },
            "scraping": {
                "selenium_timeout": self.scraping.selenium_timeout,
                "headless_browser": self.scraping.headless_browser,
                "page_load_timeout": self.scraping.page_load_timeout,
                "implicit_wait": self.scraping.implicit_wait
            },
            "embeddings": {
                "model": self.embeddings.model,
                "dimensions": self.embeddings.dimensions,
                "batch_size": self.embeddings.batch_size,
                "max_tokens": self.embeddings.max_tokens
            },
            "logging": {
                "level": self.logging.level,
                "console_output": self.logging.console_output,
                "structured_format": self.logging.structured_format,
                "max_file_size": self.logging.max_file_size,
                "backup_count": self.logging.backup_count
            },
            "scheduler": {
                "extraction_time": self.scheduler.extraction_time,
                "timezone": self.scheduler.timezone,
                "max_execution_time": self.scheduler.max_execution_time,
                "batch_size": self.scheduler.batch_size
            }
        }

# Global settings instance
settings = Settings()