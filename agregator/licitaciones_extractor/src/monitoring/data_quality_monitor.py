"""
Data Quality Monitoring System for Licitaciones Extractor.

This module provides comprehensive data quality monitoring, validation,
and alerting for all extraction sources.
"""

from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Optional, Tuple
import statistics
import json
from dataclasses import dataclass, asdict
from enum import Enum

try:
    from src.utils.logger import get_logger
except ImportError:
    import logging
    def get_logger(name):
        return logging.getLogger(name)


class QualityIssueLevel(Enum):
    """Quality issue severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class QualityMetric:
    """Data quality metric."""
    name: str
    value: float
    threshold: float
    status: str
    description: str
    source: str
    timestamp: datetime


@dataclass
class QualityIssue:
    """Data quality issue."""
    level: QualityIssueLevel
    source: str
    issue_type: str
    description: str
    count: int
    sample_records: List[str]
    timestamp: datetime


@dataclass
class QualityReport:
    """Comprehensive data quality report."""
    extraction_date: datetime
    sources: List[str]
    total_records: int
    total_valid_records: int
    overall_quality_score: float
    metrics: List[QualityMetric]
    issues: List[QualityIssue]
    source_stats: Dict[str, Any]


class DataQualityMonitor:
    """
    Comprehensive data quality monitoring system.

    Monitors data quality across all extraction sources with configurable
    thresholds, alerts, and detailed reporting.
    """

    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the data quality monitor.

        Args:
            config: Configuration dictionary with thresholds and settings
        """
        self.logger = get_logger(__name__)
        self.config = config or {}

        # Default quality thresholds
        self.thresholds = {
            'min_completeness': self.config.get('min_completeness', 0.7),
            'min_reliability': self.config.get('min_reliability', 0.8),
            'min_title_length': self.config.get('min_title_length', 10),
            'max_duplicate_rate': self.config.get('max_duplicate_rate', 0.1),
            'min_records_per_source': self.config.get('min_records_per_source', 5),
            'max_extraction_time_hours': self.config.get('max_extraction_time_hours', 2)
        }

        # Track quality metrics over time
        self.quality_history = []

    def evaluate_extraction_quality(self, extraction_results: Dict[str, List[Dict[str, Any]]]) -> QualityReport:
        """
        Evaluate overall quality of extraction results from all sources.

        Args:
            extraction_results: Dictionary with source names as keys and records as values

        Returns:
            Comprehensive quality report
        """
        self.logger.info("Starting comprehensive quality evaluation")

        # Initialize report components
        extraction_date = datetime.utcnow()
        all_records = []
        source_stats = {}
        metrics = []
        issues = []

        # Process each source
        for source, records in extraction_results.items():
            self.logger.info(f"Evaluating quality for {source}: {len(records)} records")

            # Collect all records
            all_records.extend(records)

            # Evaluate source-specific quality
            source_quality = self._evaluate_source_quality(source, records)
            source_stats[source] = source_quality

            # Generate metrics for this source
            source_metrics = self._generate_source_metrics(source, records, source_quality)
            metrics.extend(source_metrics)

            # Identify issues for this source
            source_issues = self._identify_source_issues(source, records, source_quality)
            issues.extend(source_issues)

        # Calculate overall statistics
        total_records = len(all_records)
        total_valid_records = sum(1 for record in all_records if self._is_valid_record(record))
        overall_quality_score = self._calculate_overall_quality_score(source_stats)

        # Identify cross-source issues
        cross_source_issues = self._identify_cross_source_issues(extraction_results)
        issues.extend(cross_source_issues)

        # Generate final report
        report = QualityReport(
            extraction_date=extraction_date,
            sources=list(extraction_results.keys()),
            total_records=total_records,
            total_valid_records=total_valid_records,
            overall_quality_score=overall_quality_score,
            metrics=metrics,
            issues=issues,
            source_stats=source_stats
        )

        # Store in history
        self.quality_history.append({
            'timestamp': extraction_date,
            'overall_score': overall_quality_score,
            'total_records': total_records,
            'sources': list(extraction_results.keys())
        })

        # Keep only last 30 days of history
        cutoff_date = datetime.utcnow() - timedelta(days=30)
        self.quality_history = [
            entry for entry in self.quality_history
            if entry['timestamp'] > cutoff_date
        ]

        self.logger.info(f"Quality evaluation complete. Overall score: {overall_quality_score:.2f}")
        return report

    def _evaluate_source_quality(self, source: str, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Evaluate quality metrics for a specific source.

        Args:
            source: Source name
            records: List of records from the source

        Returns:
            Dictionary with quality metrics
        """
        if not records:
            return {
                'record_count': 0,
                'avg_completeness': 0.0,
                'avg_reliability': 0.0,
                'duplicate_rate': 0.0,
                'field_coverage': {},
                'data_freshness_hours': None,
                'error_rate': 1.0
            }

        # Calculate completeness scores
        completeness_scores = []
        reliability_scores = []
        field_counts = {}

        for record in records:
            # Extract metadata if available
            metadata = record.get('metadata', {})
            quality_data = metadata.get('calidad_datos', {})

            completeness = quality_data.get('completitud', 0.0)
            reliability = quality_data.get('confiabilidad', 0.0)

            completeness_scores.append(completeness)
            reliability_scores.append(reliability)

            # Count field coverage
            for field in ['titulo', 'descripcion', 'entidad', 'fecha_catalogacion', 'valor_estimado']:
                if record.get(field):
                    field_counts[field] = field_counts.get(field, 0) + 1

        # Calculate averages
        avg_completeness = statistics.mean(completeness_scores) if completeness_scores else 0.0
        avg_reliability = statistics.mean(reliability_scores) if reliability_scores else 0.0

        # Calculate field coverage percentages
        total_records = len(records)
        field_coverage = {
            field: count / total_records for field, count in field_counts.items()
        }

        # Calculate duplicate rate
        duplicate_rate = self._calculate_duplicate_rate(records)

        # Calculate data freshness
        data_freshness_hours = self._calculate_data_freshness(records)

        # Calculate error rate (records with issues)
        error_count = sum(1 for record in records if not self._is_valid_record(record))
        error_rate = error_count / total_records if total_records > 0 else 0.0

        return {
            'record_count': total_records,
            'avg_completeness': avg_completeness,
            'avg_reliability': avg_reliability,
            'duplicate_rate': duplicate_rate,
            'field_coverage': field_coverage,
            'data_freshness_hours': data_freshness_hours,
            'error_rate': error_rate
        }

    def _generate_source_metrics(self, source: str, records: List[Dict[str, Any]], quality_stats: Dict[str, Any]) -> List[QualityMetric]:
        """
        Generate quality metrics for a source.

        Args:
            source: Source name
            records: List of records
            quality_stats: Quality statistics

        Returns:
            List of quality metrics
        """
        metrics = []
        timestamp = datetime.utcnow()

        # Record count metric
        metrics.append(QualityMetric(
            name="record_count",
            value=quality_stats['record_count'],
            threshold=self.thresholds['min_records_per_source'],
            status="pass" if quality_stats['record_count'] >= self.thresholds['min_records_per_source'] else "fail",
            description=f"Number of records extracted from {source}",
            source=source,
            timestamp=timestamp
        ))

        # Completeness metric
        metrics.append(QualityMetric(
            name="completeness",
            value=quality_stats['avg_completeness'],
            threshold=self.thresholds['min_completeness'],
            status="pass" if quality_stats['avg_completeness'] >= self.thresholds['min_completeness'] else "fail",
            description=f"Average data completeness for {source}",
            source=source,
            timestamp=timestamp
        ))

        # Reliability metric
        metrics.append(QualityMetric(
            name="reliability",
            value=quality_stats['avg_reliability'],
            threshold=self.thresholds['min_reliability'],
            status="pass" if quality_stats['avg_reliability'] >= self.thresholds['min_reliability'] else "fail",
            description=f"Average data reliability for {source}",
            source=source,
            timestamp=timestamp
        ))

        # Duplicate rate metric
        metrics.append(QualityMetric(
            name="duplicate_rate",
            value=quality_stats['duplicate_rate'],
            threshold=self.thresholds['max_duplicate_rate'],
            status="pass" if quality_stats['duplicate_rate'] <= self.thresholds['max_duplicate_rate'] else "fail",
            description=f"Duplicate record rate for {source}",
            source=source,
            timestamp=timestamp
        ))

        # Error rate metric
        metrics.append(QualityMetric(
            name="error_rate",
            value=quality_stats['error_rate'],
            threshold=0.1,  # Max 10% error rate
            status="pass" if quality_stats['error_rate'] <= 0.1 else "fail",
            description=f"Error rate for {source}",
            source=source,
            timestamp=timestamp
        ))

        return metrics

    def _identify_source_issues(self, source: str, records: List[Dict[str, Any]], quality_stats: Dict[str, Any]) -> List[QualityIssue]:
        """
        Identify quality issues for a specific source.

        Args:
            source: Source name
            records: List of records
            quality_stats: Quality statistics

        Returns:
            List of quality issues
        """
        issues = []
        timestamp = datetime.utcnow()

        # Low record count
        if quality_stats['record_count'] < self.thresholds['min_records_per_source']:
            issues.append(QualityIssue(
                level=QualityIssueLevel.WARNING,
                source=source,
                issue_type="low_record_count",
                description=f"Only {quality_stats['record_count']} records extracted (minimum: {self.thresholds['min_records_per_source']})",
                count=1,
                sample_records=[],
                timestamp=timestamp
            ))

        # Low completeness
        if quality_stats['avg_completeness'] < self.thresholds['min_completeness']:
            issues.append(QualityIssue(
                level=QualityIssueLevel.WARNING,
                source=source,
                issue_type="low_completeness",
                description=f"Average completeness {quality_stats['avg_completeness']:.2f} below threshold {self.thresholds['min_completeness']}",
                count=1,
                sample_records=[],
                timestamp=timestamp
            ))

        # High duplicate rate
        if quality_stats['duplicate_rate'] > self.thresholds['max_duplicate_rate']:
            issues.append(QualityIssue(
                level=QualityIssueLevel.WARNING,
                source=source,
                issue_type="high_duplicate_rate",
                description=f"Duplicate rate {quality_stats['duplicate_rate']:.2f} exceeds threshold {self.thresholds['max_duplicate_rate']}",
                count=1,
                sample_records=[],
                timestamp=timestamp
            ))

        # Missing critical fields
        field_coverage = quality_stats['field_coverage']
        critical_fields = ['titulo', 'entidad']

        for field in critical_fields:
            coverage = field_coverage.get(field, 0)
            if coverage < 0.9:  # 90% minimum for critical fields
                missing_count = int((1 - coverage) * quality_stats['record_count'])
                issues.append(QualityIssue(
                    level=QualityIssueLevel.ERROR,
                    source=source,
                    issue_type="missing_critical_field",
                    description=f"Critical field '{field}' missing in {missing_count} records ({coverage:.1%} coverage)",
                    count=missing_count,
                    sample_records=[],
                    timestamp=timestamp
                ))

        # Validation errors
        validation_errors = []
        for i, record in enumerate(records[:10]):  # Check first 10 for samples
            if not self._is_valid_record(record):
                validation_errors.append(f"Record {i}: {record.get('tender_id', 'no_id')}")

        if validation_errors:
            issues.append(QualityIssue(
                level=QualityIssueLevel.ERROR,
                source=source,
                issue_type="validation_errors",
                description=f"Found {len(validation_errors)} validation errors",
                count=len(validation_errors),
                sample_records=validation_errors,
                timestamp=timestamp
            ))

        return issues

    def _identify_cross_source_issues(self, extraction_results: Dict[str, List[Dict[str, Any]]]) -> List[QualityIssue]:
        """
        Identify issues that span multiple sources.

        Args:
            extraction_results: Dictionary with all extraction results

        Returns:
            List of cross-source issues
        """
        issues = []
        timestamp = datetime.utcnow()

        # Check for sources with no data
        empty_sources = [source for source, records in extraction_results.items() if not records]
        if empty_sources:
            issues.append(QualityIssue(
                level=QualityIssueLevel.CRITICAL,
                source="cross_source",
                issue_type="empty_sources",
                description=f"Sources with no data: {', '.join(empty_sources)}",
                count=len(empty_sources),
                sample_records=empty_sources,
                timestamp=timestamp
            ))

        # Check for potential cross-source duplicates
        all_records = []
        for source, records in extraction_results.items():
            for record in records:
                all_records.append((source, record))

        cross_duplicates = self._find_cross_source_duplicates(all_records)
        if cross_duplicates:
            issues.append(QualityIssue(
                level=QualityIssueLevel.WARNING,
                source="cross_source",
                issue_type="cross_source_duplicates",
                description=f"Found {len(cross_duplicates)} potential cross-source duplicates",
                count=len(cross_duplicates),
                sample_records=[dup[:100] for dup in cross_duplicates[:5]],
                timestamp=timestamp
            ))

        return issues

    def _calculate_overall_quality_score(self, source_stats: Dict[str, Any]) -> float:
        """
        Calculate overall quality score across all sources.

        Args:
            source_stats: Statistics for all sources

        Returns:
            Overall quality score (0-1)
        """
        if not source_stats:
            return 0.0

        # Weight different aspects
        weights = {
            'completeness': 0.3,
            'reliability': 0.3,
            'record_count': 0.2,
            'error_rate': 0.2
        }

        total_score = 0.0
        total_weight = 0.0

        for source, stats in source_stats.items():
            if stats['record_count'] == 0:
                continue

            # Calculate weighted score for this source
            source_score = 0.0
            source_score += weights['completeness'] * stats['avg_completeness']
            source_score += weights['reliability'] * stats['avg_reliability']
            source_score += weights['record_count'] * min(1.0, stats['record_count'] / 10)  # Cap at 10 records
            source_score += weights['error_rate'] * (1 - stats['error_rate'])  # Invert error rate

            # Weight by number of records from this source
            record_weight = stats['record_count']
            total_score += source_score * record_weight
            total_weight += record_weight

        return total_score / total_weight if total_weight > 0 else 0.0

    def _calculate_duplicate_rate(self, records: List[Dict[str, Any]]) -> float:
        """
        Calculate duplicate rate within records.

        Args:
            records: List of records

        Returns:
            Duplicate rate (0-1)
        """
        if len(records) < 2:
            return 0.0

        # Use content hash for duplicate detection
        seen_hashes = set()
        duplicates = 0

        for record in records:
            content_hash = record.get('content_hash')
            if not content_hash:
                # Generate hash from key fields
                key_parts = [
                    record.get('titulo', ''),
                    record.get('entidad', ''),
                    str(record.get('fecha_catalogacion', ''))
                ]
                content_hash = str(hash('|'.join(key_parts)))

            if content_hash in seen_hashes:
                duplicates += 1
            else:
                seen_hashes.add(content_hash)

        return duplicates / len(records)

    def _calculate_data_freshness(self, records: List[Dict[str, Any]]) -> Optional[float]:
        """
        Calculate average data freshness in hours.

        Args:
            records: List of records

        Returns:
            Average freshness in hours or None
        """
        if not records:
            return None

        freshness_values = []
        now = datetime.utcnow()

        for record in records:
            # Try to get extraction timestamp
            metadata = record.get('metadata', {})
            extraction_time_str = metadata.get('fecha_extraccion')

            if extraction_time_str:
                try:
                    extraction_time = datetime.fromisoformat(extraction_time_str.replace('Z', '+00:00'))
                    if extraction_time.tzinfo:
                        extraction_time = extraction_time.replace(tzinfo=None)

                    freshness_hours = (now - extraction_time).total_seconds() / 3600
                    freshness_values.append(freshness_hours)
                except ValueError:
                    continue

        return statistics.mean(freshness_values) if freshness_values else None

    def _find_cross_source_duplicates(self, all_records: List[Tuple[str, Dict[str, Any]]]) -> List[str]:
        """
        Find potential duplicates across different sources.

        Args:
            all_records: List of (source, record) tuples

        Returns:
            List of duplicate descriptions
        """
        duplicates = []
        seen_titles = {}

        for source, record in all_records:
            titulo = record.get('titulo', '').strip().lower()
            if not titulo or len(titulo) < 10:
                continue

            # Normalize title for comparison
            normalized_title = ' '.join(titulo.split())

            if normalized_title in seen_titles:
                other_source = seen_titles[normalized_title]
                if other_source != source:
                    duplicates.append(f"{titulo[:50]}... (found in {source} and {other_source})")
            else:
                seen_titles[normalized_title] = source

        return duplicates

    def _is_valid_record(self, record: Dict[str, Any]) -> bool:
        """
        Check if a record meets basic validation criteria.

        Args:
            record: Record to validate

        Returns:
            True if valid, False otherwise
        """
        # Required fields
        if not record.get('tender_id'):
            return False

        # Title must exist and be meaningful
        titulo = record.get('titulo', '')
        if not titulo or len(titulo.strip()) < self.thresholds['min_title_length']:
            return False

        # Must have a source
        if not record.get('fuente'):
            return False

        return True

    def generate_quality_summary(self, report: QualityReport) -> str:
        """
        Generate a human-readable quality summary.

        Args:
            report: Quality report

        Returns:
            Summary string
        """
        summary_lines = []

        # Header
        summary_lines.append("=" * 60)
        summary_lines.append("DATA QUALITY REPORT")
        summary_lines.append("=" * 60)
        summary_lines.append(f"Extraction Date: {report.extraction_date.strftime('%Y-%m-%d %H:%M:%S')}")
        summary_lines.append(f"Sources: {', '.join(report.sources)}")
        summary_lines.append(f"Total Records: {report.total_records}")
        summary_lines.append(f"Valid Records: {report.total_valid_records}")
        summary_lines.append(f"Overall Quality Score: {report.overall_quality_score:.2f}/1.00")

        # Quality status
        if report.overall_quality_score >= 0.9:
            status = "🟢 EXCELLENT"
        elif report.overall_quality_score >= 0.8:
            status = "🟡 GOOD"
        elif report.overall_quality_score >= 0.7:
            status = "🟠 FAIR"
        else:
            status = "🔴 POOR"

        summary_lines.append(f"Quality Status: {status}")
        summary_lines.append("")

        # Source breakdown
        summary_lines.append("SOURCE BREAKDOWN:")
        summary_lines.append("-" * 40)
        for source, stats in report.source_stats.items():
            summary_lines.append(f"{source.upper()}:")
            summary_lines.append(f"  Records: {stats['record_count']}")
            summary_lines.append(f"  Completeness: {stats['avg_completeness']:.2f}")
            summary_lines.append(f"  Reliability: {stats['avg_reliability']:.2f}")
            summary_lines.append(f"  Error Rate: {stats['error_rate']:.2f}")
            summary_lines.append("")

        # Critical issues
        critical_issues = [issue for issue in report.issues if issue.level == QualityIssueLevel.CRITICAL]
        if critical_issues:
            summary_lines.append("🚨 CRITICAL ISSUES:")
            summary_lines.append("-" * 40)
            for issue in critical_issues:
                summary_lines.append(f"• {issue.description}")
            summary_lines.append("")

        # Warnings
        warnings = [issue for issue in report.issues if issue.level == QualityIssueLevel.WARNING]
        if warnings:
            summary_lines.append("⚠️  WARNINGS:")
            summary_lines.append("-" * 40)
            for warning in warnings[:5]:  # Show top 5
                summary_lines.append(f"• {warning.description}")
            if len(warnings) > 5:
                summary_lines.append(f"... and {len(warnings) - 5} more warnings")
            summary_lines.append("")

        # Recommendations
        summary_lines.append("💡 RECOMMENDATIONS:")
        summary_lines.append("-" * 40)

        if report.overall_quality_score < 0.8:
            summary_lines.append("• Review and improve data extraction processes")

        if any(stats['record_count'] < 10 for stats in report.source_stats.values()):
            summary_lines.append("• Investigate sources with low record counts")

        if any(stats['error_rate'] > 0.1 for stats in report.source_stats.values()):
            summary_lines.append("• Address validation errors in data processing")

        summary_lines.append("")
        summary_lines.append("=" * 60)

        return "\n".join(summary_lines)

    def save_report(self, report: QualityReport, file_path: str) -> None:
        """
        Save quality report to file.

        Args:
            report: Quality report to save
            file_path: Path to save file
        """
        try:
            # Convert to serializable format
            report_dict = asdict(report)

            # Handle datetime serialization
            def datetime_handler(obj):
                if isinstance(obj, datetime):
                    return obj.isoformat()
                elif isinstance(obj, date):
                    return obj.isoformat()
                raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(report_dict, f, indent=2, ensure_ascii=False, default=datetime_handler)

            self.logger.info(f"Quality report saved to {file_path}")

        except Exception as e:
            self.logger.error(f"Error saving quality report: {str(e)}")


# Convenience function
def evaluate_extraction_quality(extraction_results: Dict[str, List[Dict[str, Any]]],
                               config: Dict[str, Any] = None) -> QualityReport:
    """
    Convenience function to evaluate extraction quality.

    Args:
        extraction_results: Dictionary with source names as keys and records as values
        config: Optional configuration for quality thresholds

    Returns:
        Quality report
    """
    monitor = DataQualityMonitor(config)
    return monitor.evaluate_extraction_quality(extraction_results)