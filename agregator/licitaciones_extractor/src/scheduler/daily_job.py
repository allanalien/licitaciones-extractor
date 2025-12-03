"""
Daily job scheduler for licitaciones extractor.
"""

import schedule
import time
import logging
from datetime import datetime, timedelta
import pytz
from typing import Callable, List, Dict, Any
import traceback

from src.config.settings import settings
from src.utils.logger import get_logger
from src.extractors import get_extractor, list_available_extractors
from src.database.connection import DatabaseConnection
from src.database.models import Update
from src.utils.embeddings_generator import EmbeddingsGenerator
from src.utils.data_normalizer import DataNormalizer
from src.config.keywords import CORPORATE_KEYWORDS


class ExtractionOrchestrator:
    """Main orchestrator for coordinating data extraction from all sources."""

    def __init__(self):
        """Initialize the orchestrator."""
        self.logger = get_logger("orchestrator")
        self.db_connection = DatabaseConnection()
        self.embeddings_generator = EmbeddingsGenerator()
        self.data_normalizer = DataNormalizer()
        self.extractors = {}
        self._initialize_extractors()

    def _initialize_extractors(self):
        """Initialize all available extractors."""
        try:
            available_extractors = list_available_extractors()
            self.logger.logger.info(f"Initializing extractors: {', '.join(available_extractors)}")

            for extractor_name in available_extractors:
                try:
                    config = None
                    if extractor_name == 'licita_ya':
                        config = {
                            'api_key': settings.api.licita_ya_api_key,
                            'keywords': CORPORATE_KEYWORDS
                        }
                    
                    extractor = get_extractor(extractor_name, config)
                    self.extractors[extractor_name] = extractor
                    self.logger.logger.info(f"Successfully initialized {extractor_name} extractor")
                except Exception as e:
                    self.logger.logger.error(f"Failed to initialize {extractor_name} extractor: {e}")

        except Exception as e:
            self.logger.logger.error(f"Error during extractor initialization: {e}")

    def run_daily_extraction(self, target_date: datetime = None) -> Dict[str, Any]:
        """
        Run the daily extraction process for all sources.

        Args:
            target_date: Date to extract data for (defaults to yesterday)

        Returns:
            Dictionary with extraction results and statistics
        """
        if target_date is None:
            target_date = datetime.now() - timedelta(days=1)

        extraction_start = datetime.now()
        self.logger.logger.info(f"Starting daily extraction for date: {target_date.strftime('%Y-%m-%d')}")

        results = {
            'date': target_date.strftime('%Y-%m-%d'),
            'start_time': extraction_start.isoformat(),
            'extractors': {},
            'total_records': 0,
            'errors': [],
            'success': False
        }

        # Track overall success
        overall_success = True
        total_records = 0

        # Process each extractor
        for extractor_name, extractor in self.extractors.items():
            extractor_result = self._run_extractor(extractor_name, extractor, target_date)
            results['extractors'][extractor_name] = extractor_result

            total_records += extractor_result.get('records_processed', 0)

            if not extractor_result.get('success', False):
                overall_success = False

        # Final statistics
        extraction_end = datetime.now()
        execution_time = (extraction_end - extraction_start).total_seconds()

        results.update({
            'end_time': extraction_end.isoformat(),
            'execution_time_seconds': execution_time,
            'total_records': total_records,
            'success': overall_success
        })

        # Log summary
        if overall_success:
            self.logger.logger.info(f"Daily extraction completed successfully in {execution_time:.2f}s. Total records: {total_records}")
        else:
            self.logger.logger.warning(f"Daily extraction completed with errors in {execution_time:.2f}s. Total records: {total_records}")

        return results

    def _run_extractor(self, extractor_name: str, extractor: Any, target_date: datetime) -> Dict[str, Any]:
        """
        Run a single extractor with error handling and retry logic.

        Args:
            extractor_name: Name of the extractor
            extractor: Extractor instance
            target_date: Date to extract data for

        Returns:
            Dictionary with extraction results
        """
        extractor_start = datetime.now()
        result = {
            'start_time': extractor_start.isoformat(),
            'records_extracted': 0,
            'records_processed': 0,
            'errors': [],
            'success': False
        }

        self.logger.logger.info(f"Starting extraction for {extractor_name}")

        # Retry logic
        max_retries = settings.scheduler.retry_attempts
        for attempt in range(max_retries):
            try:
                # Extract raw data
                self.logger.logger.info(f"Attempt {attempt + 1}/{max_retries} for {extractor_name}")
                extraction_result = extractor.extract_data(target_date)

                if not extraction_result.success:
                    raise Exception(f"Extraction failed: {extraction_result.error_message}")

                raw_records = extraction_result.data
                result['records_extracted'] = len(raw_records)

                self.logger.logger.info(f"{extractor_name}: Extracted {len(raw_records)} raw records")

                # Process and store records
                processed_count = self._process_and_store_records(
                    raw_records,
                    extractor_name,
                    target_date
                )

                result['records_processed'] = processed_count
                result['success'] = True

                extractor_end = datetime.now()
                execution_time = (extractor_end - extractor_start).total_seconds()
                result['end_time'] = extractor_end.isoformat()
                result['execution_time_seconds'] = execution_time

                self.logger.logger.info(f"{extractor_name}: Successfully processed {processed_count} records in {execution_time:.2f}s")
                break

            except Exception as e:
                error_msg = f"Attempt {attempt + 1} failed for {extractor_name}: {str(e)}"
                self.logger.logger.warning(error_msg)
                result['errors'].append(error_msg)

                if attempt == max_retries - 1:
                    # Final attempt failed
                    result['success'] = False
                    extractor_end = datetime.now()
                    execution_time = (extractor_end - extractor_start).total_seconds()
                    result['end_time'] = extractor_end.isoformat()
                    result['execution_time_seconds'] = execution_time

                    self.logger.logger.error(f"{extractor_name}: All {max_retries} attempts failed. Final error: {str(e)}")
                else:
                    # Wait before retry
                    retry_delay = settings.scheduler.retry_delay_seconds
                    self.logger.logger.info(f"Waiting {retry_delay}s before retry...")
                    time.sleep(retry_delay)

        return result

    def _process_and_store_records(self, raw_records: List[Dict], source: str, extraction_date: datetime) -> int:
        """
        Process raw records and store them in the database.

        Args:
            raw_records: List of raw record dictionaries
            source: Source name
            extraction_date: Date of extraction

        Returns:
            Number of successfully processed records
        """
        processed_count = 0
        batch_size = settings.scheduler.batch_size

        self.logger.logger.info(f"Processing {len(raw_records)} records from {source} in batches of {batch_size}")

        # Process in batches
        for i in range(0, len(raw_records), batch_size):
            batch = raw_records[i:i + batch_size]
            batch_number = (i // batch_size) + 1
            total_batches = ((len(raw_records) - 1) // batch_size) + 1

            self.logger.logger.info(f"Processing batch {batch_number}/{total_batches} ({len(batch)} records)")

            try:
                batch_processed = self._process_batch(batch, source, extraction_date)
                processed_count += batch_processed

            except Exception as e:
                self.logger.logger.error(f"Error processing batch {batch_number}: {e}")
                # Continue with next batch rather than failing completely

        return processed_count

    def _process_batch(self, batch: List[Dict], source: str, extraction_date: datetime) -> int:
        """
        Process a single batch of records.

        Args:
            batch: List of raw records to process
            source: Source name
            extraction_date: Date of extraction

        Returns:
            Number of successfully processed records
        """
        processed_count = 0

        with self.db_connection.get_session() as session:
            for record in batch:
                try:
                    # Normalize data
                    normalized_data = self.data_normalizer.normalize_record(record, source)

                    # Generate semantic text
                    semantic_text = self._generate_semantic_text(normalized_data)

                    # Generate embeddings
                    embeddings = self.embeddings_generator.generate_embeddings(semantic_text)

                    # Create database record
                    db_record = Update(
                        tender_id=normalized_data['tender_id'],
                        fuente=source,
                        fecha_extraccion=extraction_date,
                        fecha_catalogacion=normalized_data.get('fecha_catalogacion'),
                        fecha_apertura=normalized_data.get('fecha_apertura'),
                        titulo=normalized_data.get('titulo'),
                        descripcion=normalized_data.get('descripcion'),
                        texto_semantico=semantic_text,
                        metadata_json=normalized_data.get('metadata', {}),
                        embeddings=embeddings,
                        entidad=normalized_data.get('entidad'),
                        estado=normalized_data.get('estado'),
                        ciudad=normalized_data.get('ciudad'),
                        valor_estimado=normalized_data.get('valor_estimado'),
                        tipo_licitacion=normalized_data.get('tipo_licitacion'),
                        url_original=normalized_data.get('url_original')
                    )

                    # Save to database (handles duplicates via tender_id constraint)
                    session.merge(db_record)
                    processed_count += 1

                except Exception as e:
                    self.logger.logger.warning(f"Failed to process record {record.get('id', 'unknown')}: {e}")
                    continue

            # Commit the batch
            session.commit()

        return processed_count

    def _generate_semantic_text(self, normalized_data: Dict) -> str:
        """
        Generate semantic text for embeddings from normalized data.

        Args:
            normalized_data: Normalized record data

        Returns:
            Combined semantic text
        """
        components = []

        # Add title
        if normalized_data.get('titulo'):
            components.append(f"Título: {normalized_data['titulo']}")

        # Add description
        if normalized_data.get('descripcion'):
            components.append(f"Descripción: {normalized_data['descripcion']}")

        # Add entity
        if normalized_data.get('entidad'):
            components.append(f"Entidad: {normalized_data['entidad']}")

        # Add type
        if normalized_data.get('tipo_licitacion'):
            components.append(f"Tipo: {normalized_data['tipo_licitacion']}")

        # Add location context
        location_parts = []
        if normalized_data.get('ciudad'):
            location_parts.append(normalized_data['ciudad'])
        if normalized_data.get('estado'):
            location_parts.append(normalized_data['estado'])
        if location_parts:
            components.append(f"Ubicación: {', '.join(location_parts)}")

        return ". ".join(components)


class DailyScheduler:
    """Manages daily extraction scheduling."""

    def __init__(self):
        """Initialize scheduler."""
        self.logger = get_logger("scheduler")
        self.timezone = pytz.timezone(settings.scheduler.timezone)
        self.is_running = False
        self.job_function = None

    def set_job_function(self, job_function: Callable):
        """
        Set the function to be executed daily.

        Args:
            job_function: Function to execute on schedule
        """
        self.job_function = job_function

    def schedule_daily_extraction(self):
        """Schedule the daily extraction job."""
        if not self.job_function:
            raise ValueError("No job function set. Use set_job_function() first.")

        extraction_time = settings.scheduler.extraction_time
        self.logger.logger.info(f"Scheduling daily extraction at {extraction_time} ({settings.scheduler.timezone})")

        # Schedule the job
        schedule.every().day.at(extraction_time).do(self._run_job_with_monitoring)

        self.logger.logger.info("Daily extraction scheduled successfully")

    def _run_job_with_monitoring(self):
        """Run job with error handling and monitoring."""
        job_start = datetime.now(self.timezone)
        self.logger.logger.info(f"Starting scheduled job at {job_start}")

        try:
            self.job_function()
            job_end = datetime.now(self.timezone)
            execution_time = (job_end - job_start).total_seconds()

            self.logger.logger.info(f"Scheduled job completed successfully in {execution_time:.2f} seconds")

        except Exception as e:
            job_end = datetime.now(self.timezone)
            execution_time = (job_end - job_start).total_seconds()

            self.logger.logger.error(f"Scheduled job failed after {execution_time:.2f} seconds: {e}")
            # TODO: Add notification system for critical failures

    def start_scheduler(self):
        """Start the scheduler loop."""
        self.logger.logger.info("Starting scheduler")
        self.is_running = True

        while self.is_running:
            try:
                schedule.run_pending()
                time.sleep(60)  # Check every minute

            except KeyboardInterrupt:
                self.logger.logger.info("Scheduler interrupted by user")
                break
            except Exception as e:
                self.logger.logger.error(f"Scheduler error: {e}")
                time.sleep(60)  # Wait before retrying

    def stop_scheduler(self):
        """Stop the scheduler."""
        self.logger.logger.info("Stopping scheduler")
        self.is_running = False

    def get_next_run_time(self) -> str:
        """
        Get next scheduled run time.

        Returns:
            Formatted string of next run time
        """
        next_run = schedule.next_run()
        if next_run:
            return next_run.strftime("%Y-%m-%d %H:%M:%S")
        return "No jobs scheduled"

    def clear_schedule(self):
        """Clear all scheduled jobs."""
        schedule.clear()
        self.logger.logger.info("All scheduled jobs cleared")

def create_cron_entry():
    """
    Generate cron entry for system crontab.

    Returns:
        String representation of cron entry
    """
    extraction_time = settings.scheduler.extraction_time
    hour, minute = extraction_time.split(':')

    # Path to main.py (will need to be updated based on deployment)
    script_path = "/path/to/licitaciones_extractor/src/main.py"

    cron_entry = f"{minute} {hour} * * * /usr/bin/python3 {script_path} --mode=daily"

    return cron_entry