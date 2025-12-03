"""
Optimized batch processing utility for large-scale licitaciones data.
"""

from typing import List, Dict, Any, Optional, Callable, Iterator
from datetime import datetime
import time
import hashlib
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from queue import Queue
import logging

from sqlalchemy.orm import Session
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

try:
    from src.database.models import Update
    from src.database.connection import get_db_connection
    from src.utils.logger import get_logger
except ImportError:
    import sys
    from pathlib import Path
    src_path = Path(__file__).parent.parent
    sys.path.insert(0, str(src_path))
    from database.models import Update
    from database.connection import get_db_connection
    from utils.logger import get_logger


@dataclass
class BatchResult:
    """Result of a batch processing operation."""
    success: bool
    processed_count: int
    failed_count: int
    errors: List[str]
    processing_time: float
    batch_id: str


class LargeBatchProcessor:
    """
    Optimized batch processor for handling large datasets efficiently.

    Features:
    - Memory-efficient chunked processing
    - Deduplication using content hashes
    - Bulk database operations
    - Error isolation and recovery
    - Progress tracking
    """

    def __init__(self,
                 batch_size: int = 1000,
                 max_workers: int = 4,
                 memory_limit_mb: int = 512):
        """
        Initialize batch processor.

        Args:
            batch_size: Number of records to process per batch
            max_workers: Maximum number of worker threads
            memory_limit_mb: Memory limit in MB for batch operations
        """
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.memory_limit_bytes = memory_limit_mb * 1024 * 1024
        self.logger = get_logger("batch_processor")
        self._seen_hashes = set()
        self._lock = threading.Lock()

    def process_large_dataset(self,
                             records: List[Dict[str, Any]],
                             processor_func: Callable[[List[Dict[str, Any]]], List[Dict[str, Any]]],
                             validator_func: Optional[Callable[[Dict[str, Any]], bool]] = None) -> BatchResult:
        """
        Process large dataset in optimized batches.

        Args:
            records: List of records to process
            processor_func: Function to process each batch
            validator_func: Optional validation function

        Returns:
            BatchResult with processing statistics
        """
        start_time = time.time()
        batch_id = self._generate_batch_id()

        self.logger.info(f"Starting batch processing {batch_id} for {len(records)} records")

        processed_count = 0
        failed_count = 0
        errors = []

        try:
            # Process in chunks to manage memory
            for batch_records in self._chunk_records(records, self.batch_size):
                batch_result = self._process_batch(
                    batch_records,
                    processor_func,
                    validator_func,
                    batch_id
                )

                processed_count += batch_result.processed_count
                failed_count += batch_result.failed_count
                errors.extend(batch_result.errors)

                # Log progress
                total_processed = processed_count + failed_count
                progress = (total_processed / len(records)) * 100
                self.logger.info(f"Batch {batch_id} progress: {progress:.1f}% ({total_processed}/{len(records)})")

        except Exception as e:
            self.logger.error(f"Critical error in batch processing {batch_id}: {e}")
            errors.append(f"Critical error: {str(e)}")

        processing_time = time.time() - start_time

        result = BatchResult(
            success=failed_count < len(records) * 0.5,  # Success if less than 50% failed
            processed_count=processed_count,
            failed_count=failed_count,
            errors=errors,
            processing_time=processing_time,
            batch_id=batch_id
        )

        self.logger.info(f"Batch processing {batch_id} completed: "
                        f"{processed_count} processed, {failed_count} failed, "
                        f"{processing_time:.2f}s")

        return result

    def _process_batch(self,
                      batch_records: List[Dict[str, Any]],
                      processor_func: Callable,
                      validator_func: Optional[Callable],
                      batch_id: str) -> BatchResult:
        """Process a single batch of records."""
        start_time = time.time()
        processed_count = 0
        failed_count = 0
        errors = []

        try:
            # Remove duplicates within batch
            unique_records = self._deduplicate_batch(batch_records)

            # Process the batch
            processed_records = processor_func(unique_records)

            # Validate if validator provided
            if validator_func:
                valid_records = []
                for record in processed_records:
                    try:
                        if validator_func(record):
                            valid_records.append(record)
                            processed_count += 1
                        else:
                            failed_count += 1
                            errors.append(f"Validation failed for record {record.get('tender_id', 'unknown')}")
                    except Exception as e:
                        failed_count += 1
                        errors.append(f"Validation error for record {record.get('tender_id', 'unknown')}: {e}")

                processed_records = valid_records
            else:
                processed_count = len(processed_records)

            # Store to database in bulk
            if processed_records:
                self._bulk_insert_records(processed_records, batch_id)

        except Exception as e:
            failed_count = len(batch_records)
            errors.append(f"Batch processing error: {str(e)}")
            self.logger.error(f"Error processing batch in {batch_id}: {e}")

        processing_time = time.time() - start_time

        return BatchResult(
            success=failed_count == 0,
            processed_count=processed_count,
            failed_count=failed_count,
            errors=errors,
            processing_time=processing_time,
            batch_id=batch_id
        )

    def _deduplicate_batch(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Remove duplicates from a batch using content hashing.

        Args:
            records: Records to deduplicate

        Returns:
            List of unique records
        """
        unique_records = []

        for record in records:
            content_hash = self._create_content_hash(record)

            with self._lock:
                if content_hash not in self._seen_hashes:
                    self._seen_hashes.add(content_hash)
                    unique_records.append(record)

        return unique_records

    def _create_content_hash(self, record: Dict[str, Any]) -> str:
        """
        Create a content hash for deduplication.

        Args:
            record: Record to hash

        Returns:
            Content hash string
        """
        # Use key fields for hashing
        hash_components = [
            str(record.get('fuente', '')),
            str(record.get('titulo', '')),
            str(record.get('entidad', '')),
            str(record.get('fecha_catalogacion', '')),
            str(record.get('valor_estimado', ''))
        ]

        content_string = '|'.join(hash_components)
        return hashlib.sha256(content_string.encode()).hexdigest()

    def _bulk_insert_records(self, records: List[Dict[str, Any]], batch_id: str):
        """
        Bulk insert records into database efficiently.

        Args:
            records: Records to insert
            batch_id: Batch identifier
        """
        db_connection = get_db_connection()

        try:
            with db_connection.get_session() as session:
                # Use bulk insert for better performance
                update_objects = []

                for record in records:
                    try:
                        # Create Update object
                        update_obj = Update(
                            tender_id=record['tender_id'],
                            fuente=record['fuente'],
                            titulo=record.get('titulo'),
                            descripcion=record.get('descripcion'),
                            texto_semantico=record.get('texto_semantico', ''),
                            metadata=record.get('metadata', {}),
                            entidad=record.get('entidad'),
                            estado=record.get('estado'),
                            ciudad=record.get('ciudad'),
                            fecha_catalogacion=record.get('fecha_catalogacion'),
                            fecha_apertura=record.get('fecha_apertura'),
                            valor_estimado=record.get('valor_estimado'),
                            tipo_licitacion=record.get('tipo_licitacion'),
                            url_original=record.get('url_original'),
                            embeddings=record.get('embeddings')
                        )
                        update_objects.append(update_obj)

                    except Exception as e:
                        self.logger.error(f"Error creating Update object in batch {batch_id}: {e}")
                        continue

                if update_objects:
                    # Use bulk_insert_mappings for better performance
                    session.bulk_save_objects(update_objects)
                    self.logger.info(f"Bulk inserted {len(update_objects)} records in batch {batch_id}")

        except SQLAlchemyError as e:
            self.logger.error(f"Database error in bulk insert for batch {batch_id}: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error in bulk insert for batch {batch_id}: {e}")
            raise

    def _chunk_records(self, records: List[Dict[str, Any]], chunk_size: int) -> Iterator[List[Dict[str, Any]]]:
        """
        Split records into chunks for memory-efficient processing.

        Args:
            records: Records to chunk
            chunk_size: Size of each chunk

        Yields:
            Chunks of records
        """
        for i in range(0, len(records), chunk_size):
            yield records[i:i + chunk_size]

    def _generate_batch_id(self) -> str:
        """Generate unique batch ID."""
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        hash_part = hashlib.md5(str(time.time()).encode()).hexdigest()[:8]
        return f"batch_{timestamp}_{hash_part}"

    def get_processing_statistics(self, batch_id: str) -> Optional[Dict[str, Any]]:
        """
        Get processing statistics for a batch.

        Args:
            batch_id: Batch identifier

        Returns:
            Statistics dictionary or None if not found
        """
        # This could be enhanced to store and retrieve batch statistics
        # from database for monitoring purposes
        return {
            "batch_id": batch_id,
            "duplicates_removed": len(self._seen_hashes),
            "memory_usage_mb": self._estimate_memory_usage(),
            "timestamp": datetime.utcnow().isoformat()
        }

    def _estimate_memory_usage(self) -> float:
        """Estimate current memory usage in MB."""
        import sys
        return sys.getsizeof(self._seen_hashes) / (1024 * 1024)

    def clear_cache(self):
        """Clear deduplication cache to free memory."""
        with self._lock:
            self._seen_hashes.clear()
        self.logger.info("Deduplication cache cleared")


class StreamProcessor:
    """
    Stream processor for handling data that doesn't fit in memory.
    """

    def __init__(self, batch_size: int = 500):
        """Initialize stream processor."""
        self.batch_size = batch_size
        self.logger = get_logger("stream_processor")

    def process_stream(self,
                      data_iterator: Iterator[Dict[str, Any]],
                      processor_func: Callable[[List[Dict[str, Any]]], List[Dict[str, Any]]]) -> BatchResult:
        """
        Process data stream in batches.

        Args:
            data_iterator: Iterator yielding records
            processor_func: Function to process each batch

        Returns:
            BatchResult with processing statistics
        """
        start_time = time.time()
        batch_id = f"stream_{int(time.time())}"

        processed_count = 0
        failed_count = 0
        errors = []
        batch_buffer = []

        try:
            for record in data_iterator:
                batch_buffer.append(record)

                if len(batch_buffer) >= self.batch_size:
                    batch_result = self._process_stream_batch(batch_buffer, processor_func, batch_id)
                    processed_count += batch_result.processed_count
                    failed_count += batch_result.failed_count
                    errors.extend(batch_result.errors)

                    batch_buffer.clear()

            # Process remaining records
            if batch_buffer:
                batch_result = self._process_stream_batch(batch_buffer, processor_func, batch_id)
                processed_count += batch_result.processed_count
                failed_count += batch_result.failed_count
                errors.extend(batch_result.errors)

        except Exception as e:
            self.logger.error(f"Stream processing error in {batch_id}: {e}")
            errors.append(f"Stream processing error: {str(e)}")

        processing_time = time.time() - start_time

        return BatchResult(
            success=failed_count < processed_count,
            processed_count=processed_count,
            failed_count=failed_count,
            errors=errors,
            processing_time=processing_time,
            batch_id=batch_id
        )

    def _process_stream_batch(self,
                             batch: List[Dict[str, Any]],
                             processor_func: Callable,
                             batch_id: str) -> BatchResult:
        """Process a stream batch."""
        try:
            processed_records = processor_func(batch)

            # Store to database
            if processed_records:
                batch_processor = LargeBatchProcessor(batch_size=len(processed_records))
                batch_processor._bulk_insert_records(processed_records, batch_id)

            return BatchResult(
                success=True,
                processed_count=len(processed_records),
                failed_count=0,
                errors=[],
                processing_time=0.0,
                batch_id=batch_id
            )

        except Exception as e:
            self.logger.error(f"Error processing stream batch: {e}")
            return BatchResult(
                success=False,
                processed_count=0,
                failed_count=len(batch),
                errors=[str(e)],
                processing_time=0.0,
                batch_id=batch_id
            )