"""
Database connection management for licitaciones extractor.
"""

import os
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from contextlib import contextmanager

logger = logging.getLogger(__name__)

# Base class for all database models
Base = declarative_base()

class DatabaseConnection:
    """Manages PostgreSQL database connections."""

    def __init__(self, db_url: str = None):
        """
        Initialize database connection.

        Args:
            db_url: PostgreSQL connection URL. If None, uses POSTGRES_URL env variable.
        """
        self.db_url = db_url or os.getenv('POSTGRES_URL')
        if not self.db_url:
            raise ValueError("Database URL not provided. Set POSTGRES_URL environment variable.")

        self.engine = None
        self.SessionLocal = None
        self._initialize_connection()

    def _initialize_connection(self):
        """Initialize SQLAlchemy engine and session factory."""
        try:
            # Create engine with connection pooling
            self.engine = create_engine(
                self.db_url,
                pool_size=10,
                max_overflow=20,
                pool_recycle=300,
                pool_pre_ping=True,
                echo=False  # Set to True for SQL debugging
            )

            # Create session factory
            self.SessionLocal = sessionmaker(
                autocommit=False,
                autoflush=False,
                bind=self.engine
            )

            logger.info("Database connection initialized successfully")

        except SQLAlchemyError as e:
            logger.error(f"Failed to initialize database connection: {e}")
            raise

    @contextmanager
    def get_session(self):
        """
        Context manager for database sessions.

        Yields:
            sqlalchemy.orm.Session: Database session
        """
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Database session error: {e}")
            raise
        finally:
            session.close()

    def test_connection(self) -> bool:
        """
        Test database connectivity.

        Returns:
            bool: True if connection is successful, False otherwise
        """
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Database connection test successful")
            return True
        except SQLAlchemyError as e:
            logger.error(f"Database connection test failed: {e}")
            return False

    def create_tables(self):
        """Create all tables defined in models."""
        try:
            Base.metadata.create_all(bind=self.engine)
            logger.info("Database tables created successfully")
        except SQLAlchemyError as e:
            logger.error(f"Failed to create database tables: {e}")
            raise

    def close(self):
        """Close database connection."""
        if self.engine:
            self.engine.dispose()
            logger.info("Database connection closed")

# Global database instance
db_connection = None

def get_db_connection() -> DatabaseConnection:
    """
    Get global database connection instance.

    Returns:
        DatabaseConnection: Database connection instance
    """
    global db_connection
    if db_connection is None:
        db_connection = DatabaseConnection()
    return db_connection

def initialize_database():
    """Initialize database connection and create tables."""
    db = get_db_connection()

    # Test connection
    if not db.test_connection():
        raise RuntimeError("Failed to establish database connection")

    # Create tables
    db.create_tables()

    return db