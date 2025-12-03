"""
Data Quality Analyzer for monitoring and improving data completeness
"""

import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from collections import Counter
from sqlalchemy import text
from sqlalchemy.orm import Session

from ..database.connection import DatabaseConnection
from ..utils.logger import get_logger


class DataQualityAnalyzer:
    """Analyzes and reports on data quality metrics"""

    def __init__(self, db_connection: Optional[DatabaseConnection] = None):
        self.db_connection = db_connection or DatabaseConnection()
        self.logger = get_logger(self.__class__.__name__)

        # Quality thresholds
        self.thresholds = {
            "completeness": {
                "critical": 50,   # Below this is critical
                "warning": 80,    # Below this is warning
                "good": 95        # Above this is good
            },
            "duplicates": {
                "critical": 10,   # Above 10% duplicates is critical
                "warning": 5,     # Above 5% is warning
                "good": 2         # Below 2% is good
            },
            "freshness": {
                "critical": 48,   # No data for 48+ hours is critical
                "warning": 24,    # No data for 24+ hours is warning
                "good": 12        # Data within 12 hours is good
            }
        }

    def analyze_completeness(self, source: str = None, days: int = 7) -> Dict[str, Any]:
        """Analyze data completeness for required fields"""
        try:
            with self.db_connection.get_session() as session:
                # Base query with source filter if provided
                where_clause = f"WHERE fecha_extraccion >= CURRENT_DATE - INTERVAL '{days} days'"
                if source:
                    where_clause += f" AND fuente = '{source}'"

                query = f"""
                    SELECT
                        fuente,
                        COUNT(*) as total_records,
                        COUNT(tender_id) as with_tender_id,
                        COUNT(titulo) as with_title,
                        COUNT(descripcion) as with_description,
                        COUNT(fecha_apertura) as with_opening_date,
                        COUNT(fecha_catalogacion) as with_catalog_date,
                        COUNT(entidad) as with_entity,
                        COUNT(estado) as with_state,
                        COUNT(ciudad) as with_city,
                        COUNT(valor_estimado) as with_value,
                        COUNT(tipo_licitacion) as with_type,
                        COUNT(url_original) as with_url,
                        COUNT(texto_semantico) as with_semantic_text,
                        COUNT(embeddings) as with_embeddings,
                        AVG(LENGTH(COALESCE(descripcion, ''))) as avg_description_length,
                        AVG(LENGTH(COALESCE(texto_semantico, ''))) as avg_semantic_length
                    FROM updates
                    {where_clause}
                    GROUP BY fuente
                    ORDER BY fuente
                """

                result = session.execute(text(query))

                analysis = {}
                for row in result:
                    source_name = row[0]
                    total = row[1]

                    if total == 0:
                        continue

                    # Calculate completeness percentages
                    field_completeness = {
                        "tender_id": self._calculate_percentage(row[2], total),
                        "title": self._calculate_percentage(row[3], total),
                        "description": self._calculate_percentage(row[4], total),
                        "opening_date": self._calculate_percentage(row[5], total),
                        "catalog_date": self._calculate_percentage(row[6], total),
                        "entity": self._calculate_percentage(row[7], total),
                        "state": self._calculate_percentage(row[8], total),
                        "city": self._calculate_percentage(row[9], total),
                        "estimated_value": self._calculate_percentage(row[10], total),
                        "tender_type": self._calculate_percentage(row[11], total),
                        "original_url": self._calculate_percentage(row[12], total),
                        "semantic_text": self._calculate_percentage(row[13], total),
                        "embeddings": self._calculate_percentage(row[14], total)
                    }

                    # Calculate overall completeness
                    critical_fields = ["tender_id", "title", "description", "semantic_text"]
                    important_fields = ["opening_date", "entity", "tender_type"]
                    optional_fields = ["state", "city", "estimated_value"]

                    critical_avg = sum(field_completeness[f] for f in critical_fields) / len(critical_fields)
                    important_avg = sum(field_completeness[f] for f in important_fields) / len(important_fields)
                    optional_avg = sum(field_completeness[f] for f in optional_fields) / len(optional_fields)

                    # Weighted average (critical: 50%, important: 30%, optional: 20%)
                    overall_completeness = (critical_avg * 0.5) + (important_avg * 0.3) + (optional_avg * 0.2)

                    # Determine quality level
                    quality_level = self._get_quality_level(overall_completeness, "completeness")

                    # Identify missing critical fields
                    missing_critical = [f for f in critical_fields if field_completeness[f] < 100]

                    analysis[source_name] = {
                        "total_records": total,
                        "field_completeness": field_completeness,
                        "overall_completeness": round(overall_completeness, 2),
                        "quality_level": quality_level,
                        "missing_critical_fields": missing_critical,
                        "avg_description_length": round(row[15] or 0, 0),
                        "avg_semantic_length": round(row[16] or 0, 0),
                        "recommendations": self._generate_completeness_recommendations(
                            field_completeness, overall_completeness
                        )
                    }

                return analysis

        except Exception as e:
            self.logger.error(f"Error analyzing completeness: {e}")
            return {}

    def analyze_duplicates(self, days: int = 30) -> Dict[str, Any]:
        """Analyze duplicate records in the system"""
        try:
            with self.db_connection.get_session() as session:
                # Find exact duplicates by tender_id
                duplicate_query = f"""
                    SELECT
                        tender_id,
                        COUNT(*) as duplicate_count,
                        array_agg(DISTINCT fuente) as sources,
                        MIN(fecha_extraccion) as first_seen,
                        MAX(fecha_extraccion) as last_seen
                    FROM updates
                    WHERE fecha_extraccion >= CURRENT_DATE - INTERVAL '{days} days'
                    GROUP BY tender_id
                    HAVING COUNT(*) > 1
                    ORDER BY duplicate_count DESC
                    LIMIT 100
                """

                duplicates = session.execute(text(duplicate_query)).fetchall()

                # Find potential duplicates by similar titles
                similarity_query = f"""
                    WITH recent_tenders AS (
                        SELECT DISTINCT tender_id, titulo, fuente
                        FROM updates
                        WHERE fecha_extraccion >= CURRENT_DATE - INTERVAL '{days} days'
                            AND titulo IS NOT NULL
                    )
                    SELECT
                        t1.tender_id as tender1,
                        t2.tender_id as tender2,
                        t1.titulo as title1,
                        t2.titulo as title2,
                        similarity(t1.titulo, t2.titulo) as similarity_score
                    FROM recent_tenders t1
                    JOIN recent_tenders t2 ON t1.tender_id < t2.tender_id
                    WHERE similarity(t1.titulo, t2.titulo) > 0.8
                    ORDER BY similarity_score DESC
                    LIMIT 50
                """

                try:
                    potential_duplicates = session.execute(text(similarity_query)).fetchall()
                except:
                    # If similarity function not available, skip this analysis
                    potential_duplicates = []

                # Calculate statistics
                total_records_query = f"""
                    SELECT COUNT(DISTINCT tender_id) as unique_count, COUNT(*) as total_count
                    FROM updates
                    WHERE fecha_extraccion >= CURRENT_DATE - INTERVAL '{days} days'
                """

                stats = session.execute(text(total_records_query)).first()
                unique_count = stats[0] or 0
                total_count = stats[1] or 0

                duplicate_rate = 0
                if total_count > 0:
                    duplicate_rate = ((total_count - unique_count) / total_count) * 100

                quality_level = self._get_quality_level(duplicate_rate, "duplicates", inverse=True)

                return {
                    "summary": {
                        "total_records": total_count,
                        "unique_records": unique_count,
                        "duplicate_records": total_count - unique_count,
                        "duplicate_rate": round(duplicate_rate, 2),
                        "quality_level": quality_level
                    },
                    "exact_duplicates": [
                        {
                            "tender_id": dup[0],
                            "count": dup[1],
                            "sources": dup[2],
                            "first_seen": str(dup[3]),
                            "last_seen": str(dup[4])
                        }
                        for dup in duplicates[:20]  # Top 20 duplicates
                    ],
                    "potential_duplicates": [
                        {
                            "tender1": pot[0],
                            "tender2": pot[1],
                            "similarity": round(pot[4] * 100, 2)
                        }
                        for pot in potential_duplicates[:10]  # Top 10 potential duplicates
                    ],
                    "recommendations": self._generate_duplicate_recommendations(duplicate_rate)
                }

        except Exception as e:
            self.logger.error(f"Error analyzing duplicates: {e}")
            return {}

    def analyze_data_freshness(self) -> Dict[str, Any]:
        """Analyze how fresh/current the data is"""
        try:
            with self.db_connection.get_session() as session:
                # Get freshness by source
                freshness_query = """
                    SELECT
                        fuente,
                        MAX(fecha_extraccion) as last_extraction,
                        COUNT(CASE WHEN fecha_extraccion >= CURRENT_DATE THEN 1 END) as today_count,
                        COUNT(CASE WHEN fecha_extraccion >= CURRENT_DATE - INTERVAL '1 day' THEN 1 END) as yesterday_count,
                        COUNT(CASE WHEN fecha_extraccion >= CURRENT_DATE - INTERVAL '7 days' THEN 1 END) as week_count
                    FROM updates
                    GROUP BY fuente
                    ORDER BY MAX(fecha_extraccion) DESC
                """

                result = session.execute(text(freshness_query))

                analysis = {}
                now = datetime.now()

                for row in result:
                    source = row[0]
                    last_extraction = row[1]

                    if last_extraction:
                        hours_since = (now - last_extraction).total_seconds() / 3600
                        quality_level = self._get_quality_level(hours_since, "freshness", inverse=True)

                        analysis[source] = {
                            "last_extraction": str(last_extraction),
                            "hours_since_last": round(hours_since, 2),
                            "quality_level": quality_level,
                            "today_count": row[2] or 0,
                            "yesterday_count": row[3] or 0,
                            "week_count": row[4] or 0,
                            "status": self._get_freshness_status(hours_since)
                        }

                # Overall system freshness
                overall_query = """
                    SELECT
                        MAX(fecha_extraccion) as last_system_update,
                        COUNT(DISTINCT fuente) as active_sources_today
                    FROM updates
                    WHERE fecha_extraccion >= CURRENT_DATE
                """

                overall = session.execute(text(overall_query)).first()
                last_system_update = overall[0]
                active_sources = overall[1] or 0

                return {
                    "by_source": analysis,
                    "overall": {
                        "last_system_update": str(last_system_update) if last_system_update else None,
                        "active_sources_today": active_sources,
                        "total_sources": len(analysis),
                        "recommendations": self._generate_freshness_recommendations(analysis)
                    }
                }

        except Exception as e:
            self.logger.error(f"Error analyzing data freshness: {e}")
            return {}

    def analyze_data_consistency(self) -> Dict[str, Any]:
        """Analyze data consistency and format issues"""
        try:
            with self.db_connection.get_session() as session:
                # Check for format inconsistencies
                consistency_checks = {}

                # Check date format consistency
                date_check = """
                    SELECT
                        fuente,
                        COUNT(CASE WHEN fecha_apertura > CURRENT_DATE + INTERVAL '1 year' THEN 1 END) as future_dates,
                        COUNT(CASE WHEN fecha_apertura < CURRENT_DATE - INTERVAL '1 year' THEN 1 END) as past_dates,
                        COUNT(CASE WHEN fecha_catalogacion > fecha_apertura THEN 1 END) as catalog_after_opening
                    FROM updates
                    WHERE fecha_extraccion >= CURRENT_DATE - INTERVAL '30 days'
                    GROUP BY fuente
                """

                date_issues = session.execute(text(date_check)).fetchall()
                consistency_checks["date_consistency"] = [
                    {
                        "source": row[0],
                        "future_dates": row[1] or 0,
                        "past_dates": row[2] or 0,
                        "catalog_after_opening": row[3] or 0
                    }
                    for row in date_issues
                ]

                # Check for invalid values
                value_check = """
                    SELECT
                        fuente,
                        COUNT(CASE WHEN valor_estimado < 0 THEN 1 END) as negative_values,
                        COUNT(CASE WHEN valor_estimado > 1000000000 THEN 1 END) as suspicious_high_values,
                        COUNT(CASE WHEN LENGTH(titulo) < 10 THEN 1 END) as short_titles,
                        COUNT(CASE WHEN LENGTH(descripcion) < 20 THEN 1 END) as short_descriptions
                    FROM updates
                    WHERE fecha_extraccion >= CURRENT_DATE - INTERVAL '30 days'
                    GROUP BY fuente
                """

                value_issues = session.execute(text(value_check)).fetchall()
                consistency_checks["value_consistency"] = [
                    {
                        "source": row[0],
                        "negative_values": row[1] or 0,
                        "suspicious_high_values": row[2] or 0,
                        "short_titles": row[3] or 0,
                        "short_descriptions": row[4] or 0
                    }
                    for row in value_issues
                ]

                return {
                    "consistency_checks": consistency_checks,
                    "recommendations": self._generate_consistency_recommendations(consistency_checks)
                }

        except Exception as e:
            self.logger.error(f"Error analyzing data consistency: {e}")
            return {}

    def generate_quality_report(self) -> str:
        """Generate comprehensive data quality report"""
        self.logger.info("Generating comprehensive data quality report...")

        report = []
        report.append("=" * 70)
        report.append("DATA QUALITY ANALYSIS REPORT")
        report.append("=" * 70)
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("")

        # Completeness Analysis
        report.append("📊 DATA COMPLETENESS ANALYSIS")
        report.append("-" * 50)
        completeness = self.analyze_completeness()
        for source, metrics in completeness.items():
            report.append(f"\n{source}:")
            report.append(f"  Overall Completeness: {metrics['overall_completeness']}%")
            report.append(f"  Quality Level: {metrics['quality_level']}")
            if metrics['missing_critical_fields']:
                report.append(f"  Missing Critical Fields: {', '.join(metrics['missing_critical_fields'])}")
            if metrics['recommendations']:
                report.append("  Recommendations:")
                for rec in metrics['recommendations']:
                    report.append(f"    • {rec}")

        # Duplicate Analysis
        report.append("\n🔄 DUPLICATE ANALYSIS")
        report.append("-" * 50)
        duplicates = self.analyze_duplicates()
        if duplicates.get('summary'):
            summary = duplicates['summary']
            report.append(f"  Total Records: {summary['total_records']:,}")
            report.append(f"  Unique Records: {summary['unique_records']:,}")
            report.append(f"  Duplicate Rate: {summary['duplicate_rate']}%")
            report.append(f"  Quality Level: {summary['quality_level']}")
            if duplicates['recommendations']:
                report.append("  Recommendations:")
                for rec in duplicates['recommendations']:
                    report.append(f"    • {rec}")

        # Freshness Analysis
        report.append("\n⏱️ DATA FRESHNESS ANALYSIS")
        report.append("-" * 50)
        freshness = self.analyze_data_freshness()
        if freshness.get('by_source'):
            for source, metrics in freshness['by_source'].items():
                report.append(f"\n{source}:")
                report.append(f"  Last Update: {metrics['last_extraction']}")
                report.append(f"  Hours Since: {metrics['hours_since_last']}")
                report.append(f"  Status: {metrics['status']}")
                report.append(f"  Today's Records: {metrics['today_count']}")

        # Consistency Analysis
        report.append("\n✅ DATA CONSISTENCY ANALYSIS")
        report.append("-" * 50)
        consistency = self.analyze_data_consistency()
        if consistency.get('consistency_checks'):
            for check_type, issues in consistency['consistency_checks'].items():
                if issues:
                    report.append(f"\n{check_type.replace('_', ' ').title()}:")
                    for issue in issues:
                        if any(v > 0 for k, v in issue.items() if k != 'source'):
                            report.append(f"  {issue['source']}:")
                            for key, value in issue.items():
                                if key != 'source' and value > 0:
                                    report.append(f"    • {key.replace('_', ' ').title()}: {value}")

        # Overall Recommendations
        report.append("\n📋 OVERALL RECOMMENDATIONS")
        report.append("-" * 50)
        overall_recommendations = self._generate_overall_recommendations(
            completeness, duplicates, freshness, consistency
        )
        for i, rec in enumerate(overall_recommendations, 1):
            report.append(f"  {i}. {rec}")

        report.append("")
        report.append("=" * 70)
        report.append("END OF REPORT")
        report.append("=" * 70)

        return "\n".join(report)

    def _calculate_percentage(self, count: int, total: int) -> float:
        """Calculate percentage safely"""
        if total == 0:
            return 0
        return round((count / total) * 100, 2)

    def _get_quality_level(self, value: float, metric_type: str, inverse: bool = False) -> str:
        """Determine quality level based on thresholds"""
        thresholds = self.thresholds.get(metric_type, {})

        if inverse:  # For metrics where lower is better
            if value <= thresholds.get("good", 0):
                return "🟢 GOOD"
            elif value <= thresholds.get("warning", 0):
                return "🟡 WARNING"
            else:
                return "🔴 CRITICAL"
        else:  # For metrics where higher is better
            if value >= thresholds.get("good", 100):
                return "🟢 GOOD"
            elif value >= thresholds.get("warning", 100):
                return "🟡 WARNING"
            else:
                return "🔴 CRITICAL"

    def _get_freshness_status(self, hours_since: float) -> str:
        """Get freshness status based on hours since last update"""
        if hours_since < 12:
            return "✅ Fresh"
        elif hours_since < 24:
            return "⚠️ Aging"
        elif hours_since < 48:
            return "⚠️ Stale"
        else:
            return "🚨 Critical"

    def _generate_completeness_recommendations(self, field_completeness: Dict, overall: float) -> List[str]:
        """Generate recommendations for improving completeness"""
        recommendations = []

        if overall < 50:
            recommendations.append("Critical: Overall completeness is very low. Review extraction logic.")

        critical_fields = ["tender_id", "title", "description", "semantic_text"]
        for field in critical_fields:
            if field_completeness.get(field, 0) < 90:
                recommendations.append(f"Improve extraction for {field} field (currently {field_completeness.get(field, 0)}%)")

        if field_completeness.get("embeddings", 0) < 80:
            recommendations.append("Generate embeddings for more records to improve search quality")

        return recommendations[:3]  # Return top 3 recommendations

    def _generate_duplicate_recommendations(self, duplicate_rate: float) -> List[str]:
        """Generate recommendations for handling duplicates"""
        recommendations = []

        if duplicate_rate > 10:
            recommendations.append("High duplicate rate detected. Implement deduplication logic.")
        elif duplicate_rate > 5:
            recommendations.append("Consider implementing duplicate detection before insertion.")

        recommendations.append("Review tender_id generation to ensure uniqueness across sources.")

        return recommendations

    def _generate_freshness_recommendations(self, source_freshness: Dict) -> List[str]:
        """Generate recommendations for data freshness"""
        recommendations = []

        for source, metrics in source_freshness.items():
            if metrics['hours_since_last'] > 48:
                recommendations.append(f"Check {source} extractor - no data for {metrics['hours_since_last']:.1f} hours")
            elif metrics['hours_since_last'] > 24:
                recommendations.append(f"Review {source} extractor scheduling")

        if not recommendations:
            recommendations.append("All sources are updating regularly")

        return recommendations[:3]

    def _generate_consistency_recommendations(self, consistency_checks: Dict) -> List[str]:
        """Generate recommendations for data consistency"""
        recommendations = []

        for check_type, issues in consistency_checks.items():
            for issue in issues:
                if issue.get('negative_values', 0) > 0:
                    recommendations.append(f"Fix negative values in {issue['source']}")
                if issue.get('future_dates', 0) > 10:
                    recommendations.append(f"Review date validation in {issue['source']}")
                if issue.get('short_titles', 0) > 20:
                    recommendations.append(f"Improve title extraction in {issue['source']}")

        return recommendations[:3]

    def _generate_overall_recommendations(self, completeness: Dict, duplicates: Dict,
                                         freshness: Dict, consistency: Dict) -> List[str]:
        """Generate overall system recommendations"""
        recommendations = []

        # Check overall system health
        critical_issues = 0
        warning_issues = 0

        for source, metrics in completeness.items():
            if "CRITICAL" in metrics.get('quality_level', ''):
                critical_issues += 1
            elif "WARNING" in metrics.get('quality_level', ''):
                warning_issues += 1

        if critical_issues > 0:
            recommendations.append(f"Address {critical_issues} critical completeness issues immediately")

        if duplicates.get('summary', {}).get('duplicate_rate', 0) > 5:
            recommendations.append("Implement automated deduplication process")

        stale_sources = sum(1 for s in freshness.get('by_source', {}).values()
                          if s.get('hours_since_last', 0) > 24)
        if stale_sources > 0:
            recommendations.append(f"Review scheduling for {stale_sources} stale sources")

        recommendations.append("Consider implementing automated quality monitoring alerts")
        recommendations.append("Schedule regular data quality reviews")

        return recommendations[:5]