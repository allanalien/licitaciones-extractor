"""
Metrics Collector for monitoring system performance and data quality
"""

import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from pathlib import Path
import psutil
from sqlalchemy import text
from sqlalchemy.orm import Session

from ..database.connection import DatabaseConnection
from ..utils.logger import get_logger


class MetricsCollector:
    """Collects and stores system metrics for monitoring and analysis"""

    def __init__(self, db_connection: Optional[DatabaseConnection] = None):
        self.db_connection = db_connection or DatabaseConnection()
        self.logger = get_logger(self.__class__.__name__)
        self.metrics_file = Path("logs/metrics.json")
        self.metrics_file.parent.mkdir(exist_ok=True)

        # Initialize metrics structure
        self.current_metrics = {
            "extraction_metrics": {},
            "performance_metrics": {},
            "error_metrics": {},
            "data_quality_metrics": {},
            "system_metrics": {}
        }

    def collect_extraction_metrics(self, source: str = None) -> Dict[str, Any]:
        """Collect metrics about extraction operations"""
        try:
            with self.db_connection.get_session() as session:
                # Base query
                base_query = """
                    SELECT
                        fuente,
                        COUNT(*) as total_records,
                        COUNT(DISTINCT DATE(fecha_extraccion)) as days_active,
                        MIN(fecha_extraccion) as first_extraction,
                        MAX(fecha_extraccion) as last_extraction,
                        COUNT(CASE WHEN procesado = true THEN 1 END) as processed_count,
                        COUNT(CASE WHEN embeddings IS NOT NULL THEN 1 END) as with_embeddings
                    FROM updates
                """

                # Add source filter if specified
                if source:
                    query = base_query + f" WHERE fuente = '{source}' GROUP BY fuente"
                else:
                    query = base_query + " GROUP BY fuente"

                result = session.execute(text(query))

                metrics = {}
                for row in result:
                    source_metrics = {
                        "total_records": row[1],
                        "days_active": row[2],
                        "first_extraction": str(row[3]) if row[3] else None,
                        "last_extraction": str(row[4]) if row[4] else None,
                        "processed_count": row[5],
                        "with_embeddings": row[6],
                        "processing_rate": round(row[5] / row[1] * 100, 2) if row[1] > 0 else 0
                    }
                    metrics[row[0]] = source_metrics

                # Collect today's metrics
                today_query = """
                    SELECT
                        fuente,
                        COUNT(*) as today_count
                    FROM updates
                    WHERE DATE(fecha_extraccion) = CURRENT_DATE
                    GROUP BY fuente
                """

                today_result = session.execute(text(today_query))
                for row in today_result:
                    if row[0] in metrics:
                        metrics[row[0]]["today_count"] = row[1]

                # Collect yesterday's metrics for comparison
                yesterday_query = """
                    SELECT
                        fuente,
                        COUNT(*) as yesterday_count
                    FROM updates
                    WHERE DATE(fecha_extraccion) = CURRENT_DATE - INTERVAL '1 day'
                    GROUP BY fuente
                """

                yesterday_result = session.execute(text(yesterday_query))
                for row in yesterday_result:
                    if row[0] in metrics:
                        metrics[row[0]]["yesterday_count"] = row[1]

                self.current_metrics["extraction_metrics"] = metrics
                return metrics

        except Exception as e:
            self.logger.error(f"Error collecting extraction metrics: {e}")
            return {}

    def collect_performance_metrics(self) -> Dict[str, Any]:
        """Collect system performance metrics"""
        try:
            metrics = {
                "cpu_usage": psutil.cpu_percent(interval=1),
                "memory_usage": psutil.virtual_memory().percent,
                "disk_usage": psutil.disk_usage('/').percent,
                "network_io": {
                    "bytes_sent": psutil.net_io_counters().bytes_sent,
                    "bytes_recv": psutil.net_io_counters().bytes_recv
                },
                "timestamp": datetime.now().isoformat()
            }

            # Check database connection performance
            start_time = time.time()
            try:
                with self.db_connection.get_session() as session:
                    session.execute(text("SELECT 1"))
                db_response_time = round((time.time() - start_time) * 1000, 2)
                metrics["db_response_time_ms"] = db_response_time
                metrics["db_status"] = "healthy"
            except Exception as e:
                metrics["db_status"] = "unhealthy"
                metrics["db_error"] = str(e)

            self.current_metrics["performance_metrics"] = metrics
            return metrics

        except Exception as e:
            self.logger.error(f"Error collecting performance metrics: {e}")
            return {}

    def collect_error_metrics(self, hours: int = 24) -> Dict[str, Any]:
        """Collect error metrics from logs"""
        try:
            metrics = {
                "errors_by_source": {},
                "total_errors": 0,
                "error_rate": 0
            }

            # Parse error logs
            log_file = Path("logs/licitaciones_extractor.log")
            if log_file.exists():
                cutoff_time = datetime.now() - timedelta(hours=hours)

                with open(log_file, 'r') as f:
                    for line in f:
                        try:
                            if line.strip():
                                log_entry = json.loads(line)
                                log_time = datetime.fromisoformat(log_entry.get('timestamp', ''))

                                if log_time >= cutoff_time and log_entry.get('level') == 'ERROR':
                                    source = log_entry.get('component', 'unknown')
                                    if source not in metrics["errors_by_source"]:
                                        metrics["errors_by_source"][source] = 0
                                    metrics["errors_by_source"][source] += 1
                                    metrics["total_errors"] += 1
                        except:
                            continue

            # Calculate error rate
            with self.db_connection.get_session() as session:
                query = f"""
                    SELECT COUNT(*)
                    FROM updates
                    WHERE fecha_extraccion >= NOW() - INTERVAL '{hours} hours'
                """
                result = session.execute(text(query)).scalar()
                if result and result > 0:
                    metrics["error_rate"] = round(metrics["total_errors"] / result * 100, 2)

            self.current_metrics["error_metrics"] = metrics
            return metrics

        except Exception as e:
            self.logger.error(f"Error collecting error metrics: {e}")
            return {}

    def collect_data_quality_metrics(self) -> Dict[str, Any]:
        """Collect data quality metrics"""
        try:
            with self.db_connection.get_session() as session:
                metrics = {}

                # Check for duplicates
                duplicate_query = """
                    SELECT COUNT(*) - COUNT(DISTINCT tender_id) as duplicates
                    FROM updates
                """
                duplicates = session.execute(text(duplicate_query)).scalar()
                metrics["duplicate_count"] = duplicates or 0

                # Check completeness
                completeness_query = """
                    SELECT
                        COUNT(*) as total,
                        COUNT(titulo) as with_title,
                        COUNT(descripcion) as with_description,
                        COUNT(fecha_apertura) as with_opening_date,
                        COUNT(entidad) as with_entity,
                        COUNT(embeddings) as with_embeddings
                    FROM updates
                    WHERE fecha_extraccion >= CURRENT_DATE - INTERVAL '7 days'
                """

                result = session.execute(text(completeness_query)).first()
                if result and result[0] > 0:
                    metrics["completeness"] = {
                        "title_completeness": round(result[1] / result[0] * 100, 2),
                        "description_completeness": round(result[2] / result[0] * 100, 2),
                        "opening_date_completeness": round(result[3] / result[0] * 100, 2),
                        "entity_completeness": round(result[4] / result[0] * 100, 2),
                        "embeddings_completeness": round(result[5] / result[0] * 100, 2),
                        "overall_completeness": round(
                            (result[1] + result[2] + result[3] + result[4] + result[5]) /
                            (5 * result[0]) * 100, 2
                        )
                    }

                # Check data freshness
                freshness_query = """
                    SELECT
                        MAX(fecha_extraccion) as last_update,
                        COUNT(CASE WHEN fecha_extraccion >= CURRENT_DATE THEN 1 END) as today_count,
                        COUNT(CASE WHEN fecha_extraccion >= CURRENT_DATE - INTERVAL '1 day' THEN 1 END) as last_24h_count
                    FROM updates
                """

                freshness = session.execute(text(freshness_query)).first()
                if freshness:
                    metrics["freshness"] = {
                        "last_update": str(freshness[0]) if freshness[0] else None,
                        "today_count": freshness[1] or 0,
                        "last_24h_count": freshness[2] or 0
                    }

                self.current_metrics["data_quality_metrics"] = metrics
                return metrics

        except Exception as e:
            self.logger.error(f"Error collecting data quality metrics: {e}")
            return {}

    def collect_all_metrics(self) -> Dict[str, Any]:
        """Collect all metrics"""
        self.logger.info("Collecting all system metrics...")

        # Collect all metric types
        self.collect_extraction_metrics()
        self.collect_performance_metrics()
        self.collect_error_metrics()
        self.collect_data_quality_metrics()

        # Add metadata
        self.current_metrics["metadata"] = {
            "collection_timestamp": datetime.now().isoformat(),
            "version": "1.0.0"
        }

        # Save to file
        self.save_metrics()

        return self.current_metrics

    def save_metrics(self):
        """Save metrics to JSON file"""
        try:
            # Read existing metrics
            existing_metrics = []
            if self.metrics_file.exists():
                try:
                    with open(self.metrics_file, 'r') as f:
                        existing_metrics = json.load(f)
                        if not isinstance(existing_metrics, list):
                            existing_metrics = [existing_metrics]
                except:
                    existing_metrics = []

            # Add current metrics
            existing_metrics.append(self.current_metrics)

            # Keep only last 7 days of metrics
            cutoff = datetime.now() - timedelta(days=7)
            filtered_metrics = []
            for metric in existing_metrics:
                try:
                    timestamp = metric.get("metadata", {}).get("collection_timestamp")
                    if timestamp and datetime.fromisoformat(timestamp) >= cutoff:
                        filtered_metrics.append(metric)
                except:
                    continue

            # Save filtered metrics
            with open(self.metrics_file, 'w') as f:
                json.dump(filtered_metrics, f, indent=2)

            self.logger.info(f"Metrics saved to {self.metrics_file}")

        except Exception as e:
            self.logger.error(f"Error saving metrics: {e}")

    def get_summary_report(self) -> str:
        """Generate a summary report of current metrics"""
        report = []
        report.append("=" * 60)
        report.append("SYSTEM METRICS SUMMARY REPORT")
        report.append("=" * 60)
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("")

        # Extraction metrics
        if self.current_metrics.get("extraction_metrics"):
            report.append("📊 EXTRACTION METRICS")
            report.append("-" * 40)
            for source, metrics in self.current_metrics["extraction_metrics"].items():
                report.append(f"\n{source.upper()}:")
                report.append(f"  • Total Records: {metrics.get('total_records', 0):,}")
                report.append(f"  • Today's Count: {metrics.get('today_count', 0)}")
                report.append(f"  • Processing Rate: {metrics.get('processing_rate', 0)}%")
                report.append(f"  • With Embeddings: {metrics.get('with_embeddings', 0):,}")

        # Performance metrics
        if self.current_metrics.get("performance_metrics"):
            report.append("\n⚡ PERFORMANCE METRICS")
            report.append("-" * 40)
            perf = self.current_metrics["performance_metrics"]
            report.append(f"  • CPU Usage: {perf.get('cpu_usage', 0)}%")
            report.append(f"  • Memory Usage: {perf.get('memory_usage', 0)}%")
            report.append(f"  • DB Response Time: {perf.get('db_response_time_ms', 'N/A')} ms")
            report.append(f"  • DB Status: {perf.get('db_status', 'unknown')}")

        # Error metrics
        if self.current_metrics.get("error_metrics"):
            report.append("\n⚠️ ERROR METRICS (Last 24h)")
            report.append("-" * 40)
            err = self.current_metrics["error_metrics"]
            report.append(f"  • Total Errors: {err.get('total_errors', 0)}")
            report.append(f"  • Error Rate: {err.get('error_rate', 0)}%")
            if err.get("errors_by_source"):
                report.append("  • Errors by Source:")
                for source, count in err["errors_by_source"].items():
                    report.append(f"    - {source}: {count}")

        # Data quality metrics
        if self.current_metrics.get("data_quality_metrics"):
            report.append("\n✅ DATA QUALITY METRICS")
            report.append("-" * 40)
            quality = self.current_metrics["data_quality_metrics"]
            report.append(f"  • Duplicate Records: {quality.get('duplicate_count', 0)}")
            if quality.get("completeness"):
                comp = quality["completeness"]
                report.append(f"  • Overall Completeness: {comp.get('overall_completeness', 0)}%")
                report.append(f"    - Titles: {comp.get('title_completeness', 0)}%")
                report.append(f"    - Descriptions: {comp.get('description_completeness', 0)}%")
                report.append(f"    - Embeddings: {comp.get('embeddings_completeness', 0)}%")
            if quality.get("freshness"):
                fresh = quality["freshness"]
                report.append(f"  • Last Update: {fresh.get('last_update', 'N/A')}")
                report.append(f"  • Records Today: {fresh.get('today_count', 0)}")

        report.append("")
        report.append("=" * 60)

        return "\n".join(report)