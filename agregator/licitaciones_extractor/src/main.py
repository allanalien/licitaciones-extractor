"""
Main entry point for licitaciones extractor.
"""

import sys
import argparse
import logging
from datetime import datetime, date, timedelta
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config.settings import settings
from src.utils.logger import setup_logging, get_logger, log_system_info
from src.database.connection import initialize_database
from src.scheduler.daily_job import ExtractionOrchestrator, DailyScheduler
from src.monitoring.metrics_collector import MetricsCollector
from src.monitoring.data_quality import DataQualityAnalyzer
from src.monitoring.performance_monitor import PerformanceMonitor, ParallelExtractor
from src.monitoring.alerting import AlertingSystem, Alert, AlertLevel
from src.monitoring.dashboard import Dashboard
from src.utils.production_pipeline import ProductionPipeline, PipelineConfig, PipelineMonitor

def setup_application():
    """Setup application configuration and dependencies."""
    # Setup logging
    setup_logging(
        log_level=settings.logging.level,
        log_file=settings.logging.file_path,
        console_output=settings.logging.console_output,
        structured_format=settings.logging.structured_format
    )

    logger = get_logger("main")
    logger.logger.info("Starting licitaciones extractor application")

    # Log system information
    log_system_info()

    # Validate configuration
    config_errors = settings.validate()
    if config_errors:
        logger.logger.error(f"Configuration validation failed: {config_errors}")
        sys.exit(1)

    # Initialize database
    try:
        initialize_database()
        logger.logger.info("Database initialized successfully")
    except Exception as e:
        logger.logger.error(f"Failed to initialize database: {e}")
        sys.exit(1)

    return logger

def run_extraction(target_date: date = None, source: str = None, dry_run: bool = False):
    """
    Run data extraction for specified date and source.

    Args:
        target_date: Date to extract data for (default: yesterday)
        source: Specific source to extract from (default: all)
        dry_run: If True, don't save to database
    """
    logger = get_logger("main.extraction")

    if target_date is None:
        target_date = date.today() - timedelta(days=1)

    logger.logger.info(f"Starting extraction for {target_date}")

    # Initialize orchestrator
    orchestrator = ExtractionOrchestrator()

    # Convert date to datetime
    target_datetime = datetime.combine(target_date, datetime.min.time())

    if source:
        logger.logger.info(f"Extracting from specific source: {source}")
        # TODO: Implement single source extraction if needed
        # For now, run all extractors
        results = orchestrator.run_daily_extraction(target_datetime)
    else:
        # Run all extractors
        results = orchestrator.run_daily_extraction(target_datetime)

    # Log results
    if results['success']:
        logger.logger.info(f"Extraction completed successfully. Total records: {results['total_records']}")
        logger.logger.info(f"Execution time: {results['execution_time_seconds']:.2f} seconds")
    else:
        logger.logger.error("Extraction completed with errors")
        for extractor_name, extractor_result in results['extractors'].items():
            if not extractor_result['success']:
                logger.logger.error(f"{extractor_name}: {extractor_result.get('errors', [])}")

    return results

def run_production_pipeline(target_date: date = None, source: str = None, dry_run: bool = False):
    """
    Run production-ready pipeline with enhanced error handling and monitoring.

    Args:
        target_date: Date to extract data for (default: yesterday)
        source: Specific source to extract from (default: all)
        dry_run: If True, don't save to database
    """
    logger = get_logger("main.production_pipeline")

    if target_date is None:
        target_date = date.today() - timedelta(days=1)

    logger.logger.info(f"Starting production pipeline for {target_date}")

    try:
        # Initialize extractors
        from src.extractors.licita_ya_extractor import LicitaYaExtractor
        from src.extractors.cdmx_extractor import CDMXExtractor
        from src.config.keywords import keyword_manager

        extractors = []

        # Configure extractors based on settings
        if not source or source == "licita_ya":
            try:
                licita_ya_config = {
                    'api_key': settings.licita_ya.api_key,
                    'base_url': settings.licita_ya.api_base_url,
                    'keywords': keyword_manager.get_all_keywords(),
                    'timeout': 30,
                    'retry_attempts': 3
                }
                extractors.append(LicitaYaExtractor(licita_ya_config))
                logger.logger.info("Licita Ya extractor initialized")
            except Exception as e:
                logger.logger.warning(f"Failed to initialize Licita Ya extractor: {e}")

        if not source or source == "cdmx":
            try:
                cdmx_config = {
                    'timeout': 30,
                    'retry_attempts': 3
                }
                extractors.append(CDMXExtractor(cdmx_config))
                logger.logger.info("CDMX extractor initialized")
            except Exception as e:
                logger.logger.warning(f"Failed to initialize CDMX extractor: {e}")

        if not extractors:
            logger.logger.error("No extractors could be initialized")
            return {"success": False, "error": "No extractors available"}

        # Configure pipeline
        pipeline_config = PipelineConfig(
            batch_size=1000,
            max_workers=4,
            enable_embeddings=True,
            strict_validation=False,
            enable_deduplication=True,
            enable_error_recovery=True,
            memory_limit_mb=512
        )

        # Initialize pipeline and monitor
        pipeline = ProductionPipeline(pipeline_config)
        monitor = PipelineMonitor()

        # Run pipeline
        logger.logger.info(f"Running pipeline with {len(extractors)} extractors")
        result = pipeline.run_pipeline(extractors, target_date)

        # Log pipeline execution
        monitor.log_pipeline_execution(result)

        # Generate summary
        if result.success:
            logger.logger.info(f"Production pipeline completed successfully")
            logger.logger.info(f"Processed: {result.processed_records}/{result.total_records} records")
            logger.logger.info(f"Processing time: {result.processing_time:.2f} seconds")
            logger.logger.info(f"Success rate: {result.processed_records/result.total_records*100:.1f}%")
        else:
            logger.logger.error(f"Production pipeline completed with errors")
            logger.logger.error(f"Failed: {result.failed_records}/{result.total_records} records")
            for error in result.errors[:5]:  # Show first 5 errors
                logger.logger.error(f"  • {error}")

        # Get performance summary
        performance_summary = monitor.get_performance_summary()
        logger.logger.info(f"Performance summary: {performance_summary}")

        return {
            "success": result.success,
            "total_records": result.total_records,
            "processed_records": result.processed_records,
            "failed_records": result.failed_records,
            "processing_time": result.processing_time,
            "pipeline_id": result.metadata.get("pipeline_id"),
            "performance_summary": performance_summary
        }

    except Exception as e:
        logger.logger.error(f"Production pipeline failed: {e}", exc_info=True)
        return {"success": False, "error": str(e)}

def run_daily_job():
    """Run the daily extraction job."""
    logger = get_logger("main.daily_job")
    logger.logger.info("Starting daily extraction job")

    try:
        # Extract data from yesterday
        yesterday = date.today() - timedelta(days=1)
        results = run_extraction(target_date=yesterday)

        # Report results
        if results['success']:
            logger.logger.info("Daily job completed successfully")
        else:
            logger.logger.warning("Daily job completed with some errors")

        return results
    except Exception as e:
        logger.logger.error(f"Daily job failed: {e}")
        raise

def run_scheduler():
    """Run the continuous scheduler with health monitoring."""
    logger = get_logger("main.scheduler")
    logger.logger.info("Starting continuous scheduler")

    # Start health check server in a separate thread (for Railway)
    import threading
    health_thread = None

    if os.getenv("PORT"):  # Railway provides PORT environment variable
        try:
            from src.health import start_health_server
            port = int(os.getenv("PORT", 8080))
            health_thread = threading.Thread(target=start_health_server, args=(port,))
            health_thread.daemon = True
            health_thread.start()
            logger.logger.info(f"Health check server started on port {port}")
        except Exception as e:
            logger.logger.warning(f"Failed to start health server: {e}")

    # Initialize scheduler
    scheduler = DailyScheduler()

    # Set the daily job function
    scheduler.set_job_function(run_daily_job)

    # Schedule daily extraction
    scheduler.schedule_daily_extraction()

    logger.logger.info(f"Next scheduled run: {scheduler.get_next_run_time()}")

    try:
        # Start the scheduler loop
        scheduler.start_scheduler()
    except KeyboardInterrupt:
        logger.logger.info("Scheduler stopped by user")
        scheduler.stop_scheduler()
    except Exception as e:
        logger.logger.error(f"Scheduler error: {e}")
        scheduler.stop_scheduler()
        raise

def test_connections():
    """Test all external connections."""
    logger = get_logger("main.test")

    # Test database connection
    try:
        from database.connection import get_db_connection
        db = get_db_connection()
        if db.test_connection():
            logger.logger.info("Database connection: OK")
        else:
            logger.logger.error("Database connection: FAILED")
            return False
    except Exception as e:
        logger.logger.error(f"Database connection error: {e}")
        return False

    # TODO: Test API endpoints when extractors are implemented

    return True

def run_monitoring():
    """Run monitoring and collect metrics."""
    logger = get_logger("main.monitoring")
    logger.logger.info("Starting monitoring system")

    try:
        from src.database.connection import DatabaseConnection
        db_conn = DatabaseConnection()

        # Initialize monitoring components
        metrics_collector = MetricsCollector(db_conn)
        quality_analyzer = DataQualityAnalyzer(db_conn)
        alerting = AlertingSystem(settings)

        # Collect all metrics
        logger.logger.info("Collecting system metrics...")
        metrics = metrics_collector.collect_all_metrics()

        # Generate quality report
        logger.logger.info("Generating data quality report...")
        quality_report = quality_analyzer.generate_quality_report()

        # Check thresholds and send alerts if needed
        logger.logger.info("Checking alert thresholds...")
        alerts = alerting.check_metrics_thresholds(metrics)

        # Print summary
        print("\n" + "="*60)
        print("MONITORING SUMMARY")
        print("="*60)
        print(metrics_collector.get_summary_report())

        if alerts:
            print(f"\n⚠️ {len(alerts)} alerts were raised")
            for alert in alerts[:5]:  # Show first 5 alerts
                print(f"  • [{alert.level.value}] {alert.title}: {alert.message}")

        print("\nFor detailed quality report, use --mode=quality-report")
        print("To start the dashboard, use --mode=dashboard")

        return True

    except Exception as e:
        logger.logger.error(f"Monitoring error: {e}")
        return False

def run_quality_report():
    """Generate and display data quality report."""
    logger = get_logger("main.quality")
    logger.logger.info("Generating data quality report")

    try:
        from src.database.connection import DatabaseConnection
        db_conn = DatabaseConnection()

        quality_analyzer = DataQualityAnalyzer(db_conn)
        report = quality_analyzer.generate_quality_report()

        print(report)

        # Optionally save to file
        report_file = Path("logs") / f"quality_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        report_file.parent.mkdir(exist_ok=True)
        with open(report_file, 'w') as f:
            f.write(report)

        logger.logger.info(f"Quality report saved to {report_file}")
        return True

    except Exception as e:
        logger.logger.error(f"Quality report error: {e}")
        return False

def run_dashboard(host="127.0.0.1", port=5000):
    """Start the monitoring dashboard."""
    logger = get_logger("main.dashboard")
    logger.logger.info(f"Starting dashboard on http://{host}:{port}")

    try:
        dashboard = Dashboard(host=host, port=port)
        dashboard.run()
    except KeyboardInterrupt:
        logger.logger.info("Dashboard stopped by user")
    except Exception as e:
        logger.logger.error(f"Dashboard error: {e}")
        raise

def fix_database_ids(check_only=False):
    """Fix database ID sequence to start from 1."""
    logger = get_logger("main.fix_ids")

    try:
        from src.database.connection import DatabaseConnection

        logger.logger.info("Checking database ID sequence...")
        db_conn = DatabaseConnection()

        with db_conn.get_session() as session:
            # Get current stats
            count_result = session.execute("SELECT COUNT(*) FROM updates").fetchone()
            total_records = count_result[0] if count_result else 0

            if total_records == 0:
                logger.logger.info("Database is empty - resetting sequence to start from 1")
                session.execute("ALTER SEQUENCE updates_id_seq RESTART WITH 1")
                session.commit()
                logger.logger.info("✅ Sequence reset to start from 1")
                return True

            # Get ID range
            min_result = session.execute("SELECT MIN(id) FROM updates").fetchone()
            max_result = session.execute("SELECT MAX(id) FROM updates").fetchone()
            min_id = min_result[0] if min_result else 0
            max_id = max_result[0] if max_result else 0

            # Get sequence value
            try:
                seq_result = session.execute("SELECT last_value FROM updates_id_seq").fetchone()
                seq_value = seq_result[0] if seq_result else 0
            except:
                seq_value = max_id

            logger.logger.info(f"Database stats: {total_records} records, ID range {min_id}-{max_id}, sequence at {seq_value}")

            if check_only:
                if min_id == 1 and total_records == max_id:
                    logger.logger.info("✅ ID sequence is correct")
                    return True
                else:
                    logger.logger.warning(f"⚠️ ID sequence needs fixing - starts at {min_id}, should start at 1")
                    return False

            # Fix the sequence
            if min_id != 1:
                logger.logger.info("Fixing ID sequence to start from 1...")

                # Create new sequential IDs
                session.execute("ALTER TABLE updates ADD COLUMN new_id SERIAL")
                session.execute("ALTER TABLE updates DROP CONSTRAINT updates_pkey")
                session.execute("ALTER TABLE updates DROP COLUMN id")
                session.execute("ALTER TABLE updates RENAME COLUMN new_id TO id")
                session.execute("ALTER TABLE updates ADD PRIMARY KEY (id)")
                session.commit()

                logger.logger.info("✅ ID sequence fixed - now starts from 1")
                return True
            else:
                # Just fix the sequence value
                next_val = max_id + 1
                session.execute(f"SELECT setval('updates_id_seq', {max_id})")
                session.commit()
                logger.logger.info(f"✅ Sequence synchronized - next ID will be {next_val}")
                return True

    except Exception as e:
        logger.logger.error(f"Error fixing database IDs: {e}")
        return False

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Licitaciones Extractor")

    parser.add_argument(
        "--mode",
        choices=["daily", "extract", "test", "setup", "scheduler", "monitor", "quality-report", "dashboard", "production", "fix-ids"],
        default="extract",
        help="Operation mode"
    )

    parser.add_argument(
        "--date",
        type=str,
        help="Target date for extraction (YYYY-MM-DD)"
    )

    parser.add_argument(
        "--source",
        type=str,
        choices=["licita_ya", "cdmx", "comprasmx"],
        help="Specific source to extract from"
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run without saving to database"
    )

    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Verbose output"
    )

    parser.add_argument(
        "--host",
        type=str,
        default="127.0.0.1",
        help="Dashboard host (default: 127.0.0.1)"
    )

    parser.add_argument(
        "--port",
        type=int,
        default=5000,
        help="Dashboard port (default: 5000)"
    )

    args = parser.parse_args()

    # Override log level if verbose
    if args.verbose:
        settings.logging.level = "DEBUG"

    # Setup application
    logger = setup_application()

    try:
        if args.mode == "setup":
            logger.logger.info("Setup mode - checking configuration and connections")
            if test_connections():
                logger.logger.info("Setup completed successfully")
                sys.exit(0)
            else:
                logger.logger.error("Setup failed")
                sys.exit(1)

        elif args.mode == "test":
            logger.logger.info("Test mode - checking all connections")
            if test_connections():
                logger.logger.info("All tests passed")
                sys.exit(0)
            else:
                logger.logger.error("Some tests failed")
                sys.exit(1)

        elif args.mode == "daily":
            run_daily_job()

        elif args.mode == "scheduler":
            logger.logger.info("Starting continuous scheduler mode")
            run_scheduler()

        elif args.mode == "monitor":
            logger.logger.info("Running monitoring mode")
            if run_monitoring():
                logger.logger.info("Monitoring completed successfully")
                sys.exit(0)
            else:
                logger.logger.error("Monitoring failed")
                sys.exit(1)

        elif args.mode == "quality-report":
            logger.logger.info("Generating quality report")
            if run_quality_report():
                logger.logger.info("Quality report generated successfully")
                sys.exit(0)
            else:
                logger.logger.error("Quality report generation failed")
                sys.exit(1)

        elif args.mode == "dashboard":
            logger.logger.info("Starting monitoring dashboard")
            run_dashboard(args.host, args.port)

        elif args.mode == "fix-ids":
            logger.logger.info("Fixing database ID sequence")
            if fix_database_ids(check_only=False):
                logger.logger.info("Database ID sequence fixed successfully")
                sys.exit(0)
            else:
                logger.logger.error("Failed to fix database ID sequence")
                sys.exit(1)

        elif args.mode == "production":
            target_date = None
            if args.date:
                try:
                    target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
                except ValueError:
                    logger.logger.error(f"Invalid date format: {args.date}. Use YYYY-MM-DD")
                    sys.exit(1)

            run_production_pipeline(
                target_date=target_date,
                source=args.source,
                dry_run=args.dry_run
            )

        elif args.mode == "extract":
            target_date = None
            if args.date:
                try:
                    target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
                except ValueError:
                    logger.logger.error(f"Invalid date format: {args.date}. Use YYYY-MM-DD")
                    sys.exit(1)

            run_extraction(
                target_date=target_date,
                source=args.source,
                dry_run=args.dry_run
            )

    except KeyboardInterrupt:
        logger.logger.info("Interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.logger.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()