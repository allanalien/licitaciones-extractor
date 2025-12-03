"""
Production-ready processing pipeline for licitaciones extraction system.
"""

from typing import List, Dict, Any, Optional, Callable
from datetime import date, datetime
import time
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

try:
    from src.extractors.base_extractor import BaseExtractor, ExtractionResult
    from src.utils.batch_processor import LargeBatchProcessor, BatchResult
    from src.utils.error_handler import ProductionValidator, ErrorRecoveryManager, ProcessingResult
    from src.utils.embeddings_generator import EmbeddingsGenerator
    from src.utils.data_normalizer import DataNormalizer
    from src.utils.logger import get_logger
    from src.database.connection import get_db_connection
    from src.database.models import Update
except ImportError:
    import sys
    from pathlib import Path
    src_path = Path(__file__).parent.parent
    sys.path.insert(0, str(src_path))
    from extractors.base_extractor import BaseExtractor, ExtractionResult
    from utils.batch_processor import LargeBatchProcessor, BatchResult
    from utils.error_handler import ProductionValidator, ErrorRecoveryManager, ProcessingResult
    from utils.embeddings_generator import EmbeddingsGenerator
    from utils.data_normalizer import DataNormalizer
    from utils.logger import get_logger
    from database.connection import get_db_connection
    from database.models import Update


@dataclass
class PipelineConfig:
    """Configuration for the production pipeline."""
    batch_size: int = 1000
    max_workers: int = 4
    enable_embeddings: bool = True
    strict_validation: bool = False
    enable_deduplication: bool = True
    enable_error_recovery: bool = True
    memory_limit_mb: int = 512
    max_retry_attempts: int = 3


@dataclass
class PipelineResult:
    """Result of pipeline execution."""
    success: bool
    total_records: int
    processed_records: int
    failed_records: int
    skipped_records: int
    processing_time: float
    extraction_results: List[ExtractionResult]
    batch_results: List[BatchResult]
    errors: List[str]
    metadata: Dict[str, Any]


class ProductionPipeline:
    """
    Production-ready pipeline for licitaciones data processing.

    Features:
    - Parallel extraction from multiple sources
    - Batch processing with memory management
    - Error handling and recovery
    - Data validation and normalization
    - Embeddings generation
    - Comprehensive logging and monitoring
    """

    def __init__(self, config: PipelineConfig = None):
        """Initialize production pipeline."""
        self.config = config or PipelineConfig()
        self.logger = get_logger("production_pipeline")

        # Initialize components
        self.batch_processor = LargeBatchProcessor(
            batch_size=self.config.batch_size,
            max_workers=self.config.max_workers,
            memory_limit_mb=self.config.memory_limit_mb
        )
        self.validator = ProductionValidator()
        self.error_recovery = ErrorRecoveryManager()
        self.data_normalizer = DataNormalizer()

        if self.config.enable_embeddings:
            try:
                self.embeddings_generator = EmbeddingsGenerator()
            except Exception as e:
                self.logger.warning(f"Could not initialize embeddings generator: {e}")
                self.embeddings_generator = None
        else:
            self.embeddings_generator = None

        self._lock = threading.Lock()

    def run_pipeline(self,
                    extractors: List[BaseExtractor],
                    target_date: date,
                    **kwargs) -> PipelineResult:
        """
        Run the complete production pipeline.

        Args:
            extractors: List of extractors to use
            target_date: Date to extract data for
            **kwargs: Additional parameters for extractors

        Returns:
            PipelineResult with comprehensive results
        """
        start_time = time.time()
        pipeline_id = self._generate_pipeline_id()

        self.logger.info(f"Starting production pipeline {pipeline_id} for {target_date}")
        self.logger.info(f"Using {len(extractors)} extractors with {self.config.max_workers} workers")

        # Initialize tracking variables
        extraction_results = []
        batch_results = []
        errors = []
        all_records = []

        try:
            # Phase 1: Parallel data extraction
            self.logger.info("Phase 1: Data extraction")
            extraction_results = self._extract_data_parallel(extractors, target_date, **kwargs)

            # Collect all records from extractions
            for result in extraction_results:
                if result.success:
                    all_records.extend(result.records)
                else:
                    errors.extend(result.errors)

            self.logger.info(f"Extracted {len(all_records)} total records from all sources")

            if not all_records:
                self.logger.warning("No records extracted from any source")
                return self._create_empty_result(extraction_results, errors, time.time() - start_time)

            # Phase 2: Data normalization and validation
            self.logger.info("Phase 2: Data normalization and validation")
            normalized_records = self._process_records_batch(all_records, pipeline_id)

            # Phase 3: Embeddings generation (if enabled)
            if self.config.enable_embeddings and self.embeddings_generator and normalized_records:
                self.logger.info("Phase 3: Embeddings generation")
                normalized_records = self._generate_embeddings_batch(normalized_records, pipeline_id)

            # Phase 4: Database storage
            self.logger.info("Phase 4: Database storage")
            if normalized_records:
                batch_result = self.batch_processor.process_large_dataset(
                    normalized_records,
                    processor_func=lambda x: x,  # Records are already processed
                    validator_func=self._final_validation if self.config.strict_validation else None
                )
                batch_results.append(batch_result)

            # Calculate final statistics
            total_records = len(all_records)
            processed_records = sum(br.processed_count for br in batch_results)
            failed_records = sum(br.failed_count for br in batch_results)
            skipped_records = total_records - processed_records - failed_records

            processing_time = time.time() - start_time

            # Create result
            result = PipelineResult(
                success=failed_records < total_records * 0.5,  # Success if less than 50% failed
                total_records=total_records,
                processed_records=processed_records,
                failed_records=failed_records,
                skipped_records=skipped_records,
                processing_time=processing_time,
                extraction_results=extraction_results,
                batch_results=batch_results,
                errors=errors,
                metadata={
                    "pipeline_id": pipeline_id,
                    "target_date": target_date.isoformat(),
                    "extractors_used": [ext.source_name for ext in extractors],
                    "config": {
                        "batch_size": self.config.batch_size,
                        "max_workers": self.config.max_workers,
                        "embeddings_enabled": self.config.enable_embeddings,
                        "strict_validation": self.config.strict_validation
                    },
                    "performance_metrics": {
                        "records_per_second": total_records / processing_time if processing_time > 0 else 0,
                        "success_rate": processed_records / total_records if total_records > 0 else 0
                    }
                }
            )

            self.logger.info(f"Pipeline {pipeline_id} completed: "
                           f"{processed_records}/{total_records} records processed "
                           f"in {processing_time:.2f}s")

            return result

        except Exception as e:
            self.logger.error(f"Critical error in pipeline {pipeline_id}: {e}")
            errors.append(f"Pipeline critical error: {str(e)}")

            return PipelineResult(
                success=False,
                total_records=len(all_records),
                processed_records=0,
                failed_records=len(all_records),
                skipped_records=0,
                processing_time=time.time() - start_time,
                extraction_results=extraction_results,
                batch_results=batch_results,
                errors=errors,
                metadata={"pipeline_id": pipeline_id, "failed": True}
            )

    def _extract_data_parallel(self,
                              extractors: List[BaseExtractor],
                              target_date: date,
                              **kwargs) -> List[ExtractionResult]:
        """Extract data from multiple sources in parallel."""
        extraction_results = []

        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            # Submit extraction tasks
            future_to_extractor = {
                executor.submit(self._safe_extract, extractor, target_date, **kwargs): extractor
                for extractor in extractors
            }

            # Collect results
            for future in as_completed(future_to_extractor):
                extractor = future_to_extractor[future]
                try:
                    result = future.result()
                    extraction_results.append(result)
                    self.logger.info(f"Extraction from {extractor.source_name}: "
                                   f"{'Success' if result.success else 'Failed'} "
                                   f"({len(result.records)} records)")
                except Exception as e:
                    self.logger.error(f"Error in extraction from {extractor.source_name}: {e}")
                    extraction_results.append(ExtractionResult(
                        success=False,
                        records=[],
                        errors=[str(e)],
                        source=extractor.source_name,
                        extraction_date=datetime.utcnow(),
                        metadata={}
                    ))

        return extraction_results

    def _safe_extract(self, extractor: BaseExtractor, target_date: date, **kwargs) -> ExtractionResult:
        """Safely extract data with retry logic."""
        for attempt in range(self.config.max_retry_attempts):
            try:
                return extractor.extract_and_process(target_date, **kwargs)
            except Exception as e:
                if attempt < self.config.max_retry_attempts - 1:
                    self.logger.warning(f"Extraction attempt {attempt + 1} failed for {extractor.source_name}: {e}")
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    self.logger.error(f"All extraction attempts failed for {extractor.source_name}: {e}")
                    raise

    def _process_records_batch(self, records: List[Dict[str, Any]], pipeline_id: str) -> List[Dict[str, Any]]:
        """Process records with validation, normalization, and error recovery."""
        processed_records = []
        failed_count = 0

        for record in records:
            try:
                # Validate record
                validation_result = self.validator.validate_record(
                    record,
                    strict=self.config.strict_validation
                )

                if validation_result.success:
                    processed_records.append(validation_result.data)
                elif self.config.enable_error_recovery:
                    # Attempt error recovery
                    recovered_record = self._attempt_error_recovery(record, validation_result)
                    if recovered_record:
                        processed_records.append(recovered_record)
                    else:
                        failed_count += 1
                        self.logger.warning(f"Failed to recover record {record.get('tender_id', 'unknown')}")
                else:
                    failed_count += 1

            except Exception as e:
                failed_count += 1
                self.logger.error(f"Error processing record: {e}")

        self.logger.info(f"Record processing: {len(processed_records)} processed, {failed_count} failed")
        return processed_records

    def _attempt_error_recovery(self,
                               record: Dict[str, Any],
                               validation_result: ProcessingResult) -> Optional[Dict[str, Any]]:
        """Attempt to recover from validation errors."""
        recovered_record = record.copy()

        # Try to recover from each error
        for error in validation_result.errors:
            recovery_result = self.error_recovery.attempt_recovery(recovered_record, error)
            if recovery_result:
                recovered_record = recovery_result

        # Re-validate recovered record
        final_validation = self.validator.validate_record(recovered_record, strict=False)
        return final_validation.data if final_validation.success else None

    def _generate_embeddings_batch(self,
                                  records: List[Dict[str, Any]],
                                  pipeline_id: str) -> List[Dict[str, Any]]:
        """Generate embeddings for records in batches."""
        if not self.embeddings_generator:
            return records

        batch_size = 100  # Smaller batches for embeddings
        updated_records = []

        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]

            try:
                # Extract texts for embedding
                texts = [record.get('texto_semantico', '') for record in batch]

                # Generate embeddings
                embeddings = self.embeddings_generator.generate_embeddings_batch(texts)

                # Add embeddings to records
                for j, record in enumerate(batch):
                    record_copy = record.copy()
                    if j < len(embeddings):
                        record_copy['embeddings'] = embeddings[j]
                    updated_records.append(record_copy)

                self.logger.info(f"Generated embeddings for batch {i//batch_size + 1} "
                               f"({len(batch)} records)")

            except Exception as e:
                self.logger.error(f"Error generating embeddings for batch: {e}")
                # Add records without embeddings
                updated_records.extend(batch)

        return updated_records

    def _final_validation(self, record: Dict[str, Any]) -> bool:
        """Final validation for database insertion."""
        try:
            # Check all required fields are present and valid
            required_fields = ['tender_id', 'fuente', 'titulo', 'texto_semantico']

            for field in required_fields:
                if not record.get(field):
                    return False

            # Check data types
            if not isinstance(record['tender_id'], str):
                return False

            if 'metadata' in record and not isinstance(record['metadata'], dict):
                return False

            return True

        except Exception:
            return False

    def _create_empty_result(self,
                           extraction_results: List[ExtractionResult],
                           errors: List[str],
                           processing_time: float) -> PipelineResult:
        """Create result for when no records are processed."""
        return PipelineResult(
            success=False,
            total_records=0,
            processed_records=0,
            failed_records=0,
            skipped_records=0,
            processing_time=processing_time,
            extraction_results=extraction_results,
            batch_results=[],
            errors=errors,
            metadata={"no_records": True}
        )

    def _generate_pipeline_id(self) -> str:
        """Generate unique pipeline ID."""
        import hashlib
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        hash_part = hashlib.md5(str(time.time()).encode()).hexdigest()[:8]
        return f"pipeline_{timestamp}_{hash_part}"

    def get_pipeline_stats(self) -> Dict[str, Any]:
        """Get current pipeline statistics."""
        return {
            "config": {
                "batch_size": self.config.batch_size,
                "max_workers": self.config.max_workers,
                "embeddings_enabled": self.config.enable_embeddings,
                "strict_validation": self.config.strict_validation
            },
            "components": {
                "batch_processor_memory_mb": self.batch_processor._estimate_memory_usage(),
                "embeddings_available": self.embeddings_generator is not None
            }
        }


class PipelineMonitor:
    """
    Monitor for tracking pipeline performance and health.
    """

    def __init__(self):
        """Initialize pipeline monitor."""
        self.logger = get_logger("pipeline_monitor")
        self.execution_history = []

    def log_pipeline_execution(self, result: PipelineResult):
        """Log pipeline execution for monitoring."""
        execution_record = {
            "timestamp": datetime.utcnow().isoformat(),
            "pipeline_id": result.metadata.get("pipeline_id"),
            "success": result.success,
            "total_records": result.total_records,
            "processed_records": result.processed_records,
            "failed_records": result.failed_records,
            "processing_time": result.processing_time,
            "success_rate": result.processed_records / result.total_records if result.total_records > 0 else 0,
            "records_per_second": result.total_records / result.processing_time if result.processing_time > 0 else 0
        }

        self.execution_history.append(execution_record)

        # Keep only last 100 executions
        if len(self.execution_history) > 100:
            self.execution_history = self.execution_history[-100:]

        self.logger.info(f"Pipeline execution logged: {execution_record}")

    def get_performance_summary(self) -> Dict[str, Any]:
        """Get performance summary from recent executions."""
        if not self.execution_history:
            return {"message": "No execution history available"}

        recent_executions = self.execution_history[-10:]  # Last 10 executions

        total_executions = len(recent_executions)
        successful_executions = sum(1 for ex in recent_executions if ex["success"])
        avg_processing_time = sum(ex["processing_time"] for ex in recent_executions) / total_executions
        avg_records_per_second = sum(ex["records_per_second"] for ex in recent_executions) / total_executions
        avg_success_rate = sum(ex["success_rate"] for ex in recent_executions) / total_executions

        return {
            "total_executions": total_executions,
            "successful_executions": successful_executions,
            "success_rate": successful_executions / total_executions,
            "avg_processing_time": avg_processing_time,
            "avg_records_per_second": avg_records_per_second,
            "avg_success_rate": avg_success_rate,
            "last_execution": recent_executions[-1] if recent_executions else None
        }