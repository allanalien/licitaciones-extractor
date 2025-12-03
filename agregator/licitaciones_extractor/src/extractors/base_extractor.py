"""
Base extractor class for licitaciones extraction system.
"""

from abc import ABC, abstractmethod
from datetime import datetime, date
from typing import List, Dict, Any, Optional, Union
import time
import hashlib
import html
import re
from dataclasses import dataclass

try:
    from src.utils.logger import get_logger
    from src.config.keywords import keyword_manager
except ImportError:
    # For direct imports during testing
    import sys
    from pathlib import Path
    src_path = Path(__file__).parent.parent
    sys.path.insert(0, str(src_path))
    from utils.logger import get_logger
    from config.keywords import keyword_manager

@dataclass
class ExtractionResult:
    """Result of an extraction operation."""
    success: bool
    records: List[Dict[str, Any]]
    errors: List[str]
    source: str
    extraction_date: datetime
    metadata: Dict[str, Any]

    def __post_init__(self):
        """Post-initialization processing."""
        if self.metadata is None:
            self.metadata = {}

class BaseExtractor(ABC):
    """
    Base class for all data extractors.

    This class provides common functionality for extracting tender data
    from different sources and normalizing it to a standard format.
    """

    def __init__(self, source_name: str, config: Dict[str, Any] = None):
        """
        Initialize base extractor.

        Args:
            source_name: Name of the data source
            config: Configuration dictionary specific to the extractor
        """
        self.source_name = source_name
        self.config = config or {}
        self.logger = get_logger(f"extractor.{source_name}")
        self._setup_extractor()

    def _setup_extractor(self):
        """Setup extractor-specific configuration."""
        pass

    @abstractmethod
    def extract_data(self, target_date: date, **kwargs) -> ExtractionResult:
        """
        Extract data from the source for a specific date.

        Args:
            target_date: Date to extract data for
            **kwargs: Additional parameters specific to the extractor

        Returns:
            ExtractionResult with extracted data
        """
        pass

    @abstractmethod
    def normalize_data(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Normalize raw data to standard format.

        Args:
            raw_data: Raw data from the source

        Returns:
            List of normalized data dictionaries
        """
        pass

    @abstractmethod
    def validate_data(self, normalized_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Validate normalized data.

        Args:
            normalized_data: Normalized data to validate

        Returns:
            List of valid data dictionaries
        """
        pass

    def extract_and_process(self, target_date: date, **kwargs) -> ExtractionResult:
        """
        Complete extraction and processing pipeline.

        Args:
            target_date: Date to extract data for
            **kwargs: Additional parameters

        Returns:
            ExtractionResult with processed data
        """
        start_time = time.time()

        self.logger.log_extraction_start(
            source=self.source_name,
            parameters={"target_date": target_date.isoformat(), **kwargs}
        )

        try:
            # Extract raw data
            extraction_result = self.extract_data(target_date, **kwargs)

            if not extraction_result.success:
                self.logger.log_extraction_end(
                    source=self.source_name,
                    records_count=0,
                    execution_time=time.time() - start_time,
                    success=False
                )
                return extraction_result

            # Normalize data
            normalized_data = self.normalize_data(extraction_result.records)

            # Validate data
            valid_data = self.validate_data(normalized_data)

            # Calculate quality metrics
            quality_metrics = self._calculate_quality_metrics(valid_data)
            self.logger.log_data_quality(self.source_name, quality_metrics)

            # Update result
            extraction_result.records = valid_data
            extraction_result.metadata.update({
                "processing_time": time.time() - start_time,
                "quality_metrics": quality_metrics
            })

            self.logger.log_extraction_end(
                source=self.source_name,
                records_count=len(valid_data),
                execution_time=time.time() - start_time,
                success=True
            )

            return extraction_result

        except Exception as e:
            self.logger.log_error(self.source_name, e, "extraction_and_processing")
            return ExtractionResult(
                success=False,
                records=[],
                errors=[str(e)],
                source=self.source_name,
                extraction_date=datetime.utcnow(),
                metadata={"execution_time": time.time() - start_time}
            )

    def _calculate_quality_metrics(self, data: List[Dict[str, Any]]) -> Dict[str, float]:
        """
        Calculate data quality metrics.

        Args:
            data: Data to analyze

        Returns:
            Dictionary with quality metrics
        """
        if not data:
            return {"completeness": 0.0, "relevance": 0.0, "uniqueness": 0.0}

        total_records = len(data)

        # Calculate completeness
        required_fields = ["tender_id", "titulo", "fuente"]
        complete_records = 0
        for record in data:
            if all(record.get(field) for field in required_fields):
                complete_records += 1

        completeness = complete_records / total_records

        # Calculate relevance (based on keyword matching)
        relevant_records = 0
        for record in data:
            text = f"{record.get('titulo', '')} {record.get('descripcion', '')}"
            relevant_keywords = keyword_manager.get_relevant_keywords(text)
            if relevant_keywords:
                relevant_records += 1

        relevance = relevant_records / total_records

        # Calculate uniqueness (based on tender_id)
        unique_ids = set(record.get('tender_id') for record in data if record.get('tender_id'))
        uniqueness = len(unique_ids) / total_records if total_records > 0 else 0

        return {
            "completeness": completeness,
            "relevance": relevance,
            "uniqueness": uniqueness
        }

    def _generate_tender_id(self, data: Dict[str, Any]) -> str:
        """
        Generate a unique tender ID for data that doesn't have one.

        Args:
            data: Data dictionary

        Returns:
            Generated tender ID
        """
        # Create a hash based on source, title, and other identifying information
        identifier_string = (
            f"{self.source_name}|"
            f"{data.get('titulo', '')}|"
            f"{data.get('entidad', '')}|"
            f"{data.get('fecha_catalogacion', '')}|"
            f"{data.get('url_original', '')}"
        )

        hash_object = hashlib.md5(identifier_string.encode())
        return f"{self.source_name}_{hash_object.hexdigest()[:16]}"

    def _create_semantic_text(self, data: Dict[str, Any]) -> str:
        """
        Create semantic text for embeddings generation following the standard format.

        Example format:
        Licitación E-2025-00104673: SERVICIOS PROFESIONALES PARA LA ASESORÍA FISCAL...
        Institución: ADMINISTRACION DEL SISTEMA PORTUARIO NACIONAL PUERTO VALLARTA...
        Tipo de procedimiento: ADJUDICACIÓN DIRECTA POR MONTOS MÁXIMOS POR EXCEPCIÓN...

        Args:
            data: Normalized data dictionary

        Returns:
            Semantic text string in standardized format
        """
        parts = []

        # Primary identifier and title
        tender_id = data.get('tender_id', '')
        titulo = data.get('titulo', '')
        if tender_id and titulo:
            parts.append(f"Licitación {tender_id}: {titulo}")
        elif titulo:
            parts.append(f"Licitación: {titulo}")

        # Institution/Entity
        entidad = data.get('entidad', '')
        if entidad:
            parts.append(f"Institución: {entidad}")

        # Procedure type
        tipo_licitacion = data.get('tipo_licitacion', '')
        if tipo_licitacion:
            parts.append(f"Tipo de procedimiento: {tipo_licitacion}")

        # Description
        descripcion = data.get('descripcion', '')
        if descripcion:
            parts.append(f"Descripción: {descripcion}")

        # Provider (if available)
        meta_data = data.get('meta_data', {}) or data.get('metadata', {})
        if isinstance(meta_data, dict):
            datos_especificos = meta_data.get('datos_especificos', {})
            if isinstance(datos_especificos, dict):
                proveedor = datos_especificos.get('proveedor', '')
                rfc = datos_especificos.get('rfc', '')
                if proveedor:
                    parts.append(f"Proveedor: {proveedor}")
                if rfc:
                    parts.append(f"RFC: {rfc}")

        # Status and amount
        valor_estimado = data.get('valor_estimado')
        if valor_estimado and valor_estimado > 0:
            parts.append(f"Importe: ${valor_estimado:,.2f} MXN")

        # Status from metadata
        if isinstance(meta_data, dict) and isinstance(datos_especificos, dict):
            estatus = datos_especificos.get('estatus_drc', '')
            if estatus:
                parts.append(f"Estatus: {estatus}")

        return ". ".join(parts) + "." if parts else ""

    def _validate_required_fields(self, data: Dict[str, Any]) -> List[str]:
        """
        Validate that required fields are present.

        Args:
            data: Data dictionary to validate

        Returns:
            List of missing required fields
        """
        required_fields = ["tender_id", "fuente", "titulo", "texto_semantico"]
        missing_fields = []

        for field in required_fields:
            if not data.get(field):
                missing_fields.append(field)

        return missing_fields

    def _clean_text(self, text: str) -> str:
        """
        Clean and normalize text data.

        Args:
            text: Text to clean

        Returns:
            Cleaned text
        """
        if not text or not isinstance(text, str):
            return ""

        try:
            # Remove extra whitespace and normalize
            cleaned = " ".join(str(text).strip().split())

            # Remove common HTML entities and tags
            cleaned = html.unescape(cleaned)

            # Remove HTML tags (basic cleanup)
            cleaned = re.sub(r'<[^>]+>', '', cleaned)

            # Remove control characters
            cleaned = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', cleaned)

            # Limit length to prevent memory issues
            if len(cleaned) > 10000:
                cleaned = cleaned[:10000] + "..."

            return cleaned.strip()
        except Exception as e:
            self.logger.warning(f"Error cleaning text: {e}")
            return str(text)[:1000] if text else ""

    def _normalize_date(self, date_value: Union[str, date, datetime, None]) -> Optional[date]:
        """
        Normalize date value to date object.

        Args:
            date_value: Date value in various formats

        Returns:
            Normalized date object or None
        """
        if not date_value:
            return None

        if isinstance(date_value, date):
            return date_value

        if isinstance(date_value, datetime):
            return date_value.date()

        if isinstance(date_value, str):
            # Try common date formats
            date_formats = [
                "%Y-%m-%d",
                "%d/%m/%Y",
                "%m/%d/%Y",
                "%Y-%m-%d %H:%M:%S",
                "%d-%m-%Y",
                "%Y/%m/%d"
            ]

            for date_format in date_formats:
                try:
                    return datetime.strptime(date_value.strip(), date_format).date()
                except ValueError:
                    continue

        return None

    def _normalize_amount(self, amount_value: Union[str, int, float, None]) -> Optional[float]:
        """
        Normalize amount value to float.

        Args:
            amount_value: Amount value in various formats

        Returns:
            Normalized float value or None
        """
        if not amount_value:
            return None

        try:
            if isinstance(amount_value, (int, float)):
                value = float(amount_value)
                # Sanity check for reasonable values
                if 0 <= value <= 1e15:
                    return value
                return None

            if isinstance(amount_value, str):
                # Remove common currency symbols and formatting
                cleaned = re.sub(r'[,$€£¥₹₽\s]', '', str(amount_value).strip())
                # Handle both comma and period as decimal separators
                if ',' in cleaned and '.' in cleaned:
                    # Determine which is decimal separator
                    last_comma = cleaned.rfind(',')
                    last_period = cleaned.rfind('.')
                    if last_period > last_comma:
                        cleaned = cleaned.replace(',', '')
                    else:
                        cleaned = cleaned.replace('.', '').replace(',', '.')
                elif ',' in cleaned and not '.' in cleaned:
                    # Check if comma is decimal separator
                    if re.match(r'^\d+(,\d{1,2})$', cleaned):
                        cleaned = cleaned.replace(',', '.')
                    else:
                        cleaned = cleaned.replace(',', '')

                value = float(cleaned)
                # Sanity check
                if 0 <= value <= 1e15:
                    return value
                return None
        except (ValueError, TypeError) as e:
            self.logger.warning(f"Could not normalize amount '{amount_value}': {e}")
            return None

        return None

    def get_source_info(self) -> Dict[str, Any]:
        """
        Get information about this extractor source.

        Returns:
            Dictionary with source information
        """
        return {
            "source_name": self.source_name,
            "extractor_type": self.__class__.__name__,
            "config": self.config
        }