"""
Unified data normalization pipeline for all extractors.

This module provides a centralized normalization system that ensures
consistent data format across all extraction sources.
"""

from datetime import datetime, date
from typing import List, Dict, Any, Optional, Union
import re
import hashlib
import logging

try:
    from src.utils.logger import get_logger
    from src.utils.text_processor import clean_text, normalize_text
except ImportError:
    # For testing or standalone use
    logging.basicConfig(level=logging.INFO)
    def get_logger(name):
        return logging.getLogger(name)

    def clean_text(text):
        return str(text).strip() if text else ""

    def normalize_text(text):
        return str(text).strip() if text else ""


class UnifiedNormalizer:
    """
    Unified normalizer for all extraction sources.

    Provides consistent data normalization across CDMX API, ComprasMX scraping,
    and LicitaYa API while preserving source-specific metadata.
    """

    def __init__(self):
        """Initialize the unified normalizer."""
        self.logger = get_logger(__name__)

        # Standard field mappings for different sources
        self.field_mappings = {
            'cdmx': {
                'id_fields': ['planning_id', 'id', 'reference_number'],
                'title_fields': ['name', 'title', 'planning_name'],
                'description_fields': ['description', 'details', 'planning_description'],
                'entity_fields': ['entity', 'dependencia', 'organization', 'institution'],
                'amount_fields': ['estimated_amount', 'budget', 'amount', 'estimated_value'],
                'date_fields': ['planning_date', 'publication_date', 'created_at'],
                'deadline_fields': ['opening_date', 'submission_deadline', 'deadline'],
                'type_fields': ['hiring_method_name', 'procurement_type', 'method', 'type']
            },
            'comprasmx': {
                'id_fields': ['tender_id', 'numero_referencia'],
                'title_fields': ['titulo', 'descripcion'],
                'description_fields': ['descripcion', 'all_text'],
                'entity_fields': ['entidad'],
                'amount_fields': ['valor_estimado'],
                'date_fields': ['fecha_apertura', 'fecha_catalogacion'],
                'deadline_fields': ['fecha_apertura'],
                'type_fields': ['tipo_proceso']
            },
            'licita_ya': {
                'id_fields': ['id', 'tender_id', 'reference'],
                'title_fields': ['title', 'name', 'subject'],
                'description_fields': ['description', 'details', 'summary'],
                'entity_fields': ['entity', 'institution', 'buyer'],
                'amount_fields': ['amount', 'value', 'budget'],
                'date_fields': ['date', 'publication_date', 'created_date'],
                'deadline_fields': ['deadline', 'closing_date', 'due_date'],
                'type_fields': ['type', 'procurement_type', 'category']
            }
        }

    def normalize_records(self, records: List[Dict[str, Any]], source: str) -> List[Dict[str, Any]]:
        """
        Normalize a list of records from any source.

        Args:
            records: List of raw records
            source: Source name ('cdmx', 'comprasmx', 'licita_ya')

        Returns:
            List of normalized records
        """
        normalized_records = []

        self.logger.info(f"Normalizing {len(records)} records from {source}")

        for i, record in enumerate(records):
            try:
                normalized_record = self.normalize_single_record(record, source)
                if normalized_record:
                    normalized_records.append(normalized_record)
                else:
                    self.logger.warning(f"Record {i} from {source} failed normalization")
            except Exception as e:
                self.logger.error(f"Error normalizing record {i} from {source}: {str(e)}")
                continue

        self.logger.info(f"Successfully normalized {len(normalized_records)}/{len(records)} records from {source}")
        return normalized_records

    def normalize_single_record(self, record: Dict[str, Any], source: str) -> Optional[Dict[str, Any]]:
        """
        Normalize a single record to the unified format.

        Args:
            record: Raw record dictionary
            source: Source name

        Returns:
            Normalized record or None if normalization fails
        """
        try:
            # Get field mappings for this source
            mappings = self.field_mappings.get(source, {})

            # Extract core fields using source-specific mappings
            tender_id = self._extract_field(record, mappings.get('id_fields', []), source)
            titulo = self._extract_field(record, mappings.get('title_fields', []))
            descripcion = self._extract_field(record, mappings.get('description_fields', []))
            entidad = self._extract_field(record, mappings.get('entity_fields', []))

            # Handle dates
            fecha_catalogacion = self._extract_date_field(record, mappings.get('date_fields', []))
            fecha_apertura = self._extract_date_field(record, mappings.get('deadline_fields', []))

            # Handle amounts
            valor_estimado = self._extract_amount_field(record, mappings.get('amount_fields', []))

            # Handle type
            tipo_licitacion = self._extract_field(record, mappings.get('type_fields', []))

            # Handle location based on source
            estado, ciudad = self._extract_location(record, source)

            # Generate URL
            url_original = self._generate_url(record, source)

            # Apply universal cleaning and validation
            tender_id = tender_id or self._generate_fallback_id(record, source)
            titulo = clean_text(titulo) or self._generate_fallback_title(record, source)
            descripcion = clean_text(descripcion)
            entidad = clean_text(entidad) or self._get_default_entity(source)
            tipo_licitacion = clean_text(tipo_licitacion) or "Licitación"

            # Enhanced title validation with better fallbacks
            if not titulo or len(titulo) < 5 or titulo.lower().startswith('sin título'):
                # Try to create meaningful title from available data
                fallback_title = self._create_meaningful_title(record, source, entidad, descripcion, tipo_licitacion)
                if fallback_title:
                    titulo = fallback_title
                    self.logger.info(f"Enhanced title for {tender_id}: {titulo}")
                else:
                    # Still accept the record but mark title as incomplete
                    titulo = f"Licitación {tender_id}"
                    self.logger.warning(f"Using minimal title for record {tender_id}: {titulo}")

            # Only reject if we have no useful data at all
            if not entidad and not descripcion and len(titulo) < 10:
                self.logger.warning(f"Record has insufficient data - rejected: {tender_id}")
                return None

            # Create the normalized record
            normalized_record = {
                "tender_id": str(tender_id),
                "fuente": source,
                "titulo": titulo,
                "descripcion": descripcion,
                "entidad": entidad,
                "estado": estado,
                "ciudad": ciudad,
                "fecha_catalogacion": fecha_catalogacion,
                "fecha_apertura": fecha_apertura,
                "valor_estimado": valor_estimado,
                "tipo_licitacion": tipo_licitacion,
                "url_original": url_original,
                "metadata": {
                    "fuente_original": source,
                    "fecha_extraccion": datetime.utcnow().isoformat(),
                    "parametros_busqueda": record.get('search_params', {}),
                    "datos_especificos": {
                        source: self._extract_source_specific_metadata(record, source)
                    },
                    "calidad_datos": {
                        "completitud": self._calculate_completeness({
                            'titulo': titulo,
                            'descripcion': descripcion,
                            'entidad': entidad,
                            'fecha': fecha_catalogacion,
                            'valor': valor_estimado
                        }),
                        "confiabilidad": self._calculate_reliability(record, source)
                    }
                }
            }

            # Generate semantic text for embeddings
            normalized_record["texto_semantico"] = self._create_semantic_text(normalized_record)

            # Calculate content hash for deduplication
            normalized_record["content_hash"] = self._calculate_content_hash(normalized_record)

            return normalized_record

        except Exception as e:
            self.logger.error(f"Error in normalize_single_record: {str(e)}")
            return None

    def _extract_field(self, record: Dict[str, Any], field_list: List[str], source: str = None) -> str:
        """
        Extract field value using prioritized field list.

        Args:
            record: Record dictionary
            field_list: List of field names to try in order
            source: Source name for special handling

        Returns:
            Extracted field value or empty string
        """
        for field in field_list:
            value = record.get(field)
            if value and str(value).strip():
                return str(value).strip()

        # Special case for ComprasMX with optimized extraction
        if source == 'comprasmx' and 'all_text' in record:
            # Try to extract from all_text for ComprasMX
            return record.get('all_text', '')[:100] if len(field_list) > 0 else ""

        return ""

    def _extract_date_field(self, record: Dict[str, Any], field_list: List[str]) -> Optional[date]:
        """
        Extract and normalize date field.

        Args:
            record: Record dictionary
            field_list: List of date field names to try

        Returns:
            Normalized date or None
        """
        for field in field_list:
            value = record.get(field)
            if value:
                normalized_date = self._normalize_date(value)
                if normalized_date:
                    return normalized_date
        return None

    def _extract_amount_field(self, record: Dict[str, Any], field_list: List[str]) -> Optional[float]:
        """
        Extract and normalize amount field.

        Args:
            record: Record dictionary
            field_list: List of amount field names to try

        Returns:
            Normalized amount or None
        """
        for field in field_list:
            value = record.get(field)
            if value:
                normalized_amount = self._normalize_amount(value)
                if normalized_amount:
                    return normalized_amount
        return None

    def _extract_location(self, record: Dict[str, Any], source: str) -> tuple:
        """
        Extract location information based on source.

        Args:
            record: Record dictionary
            source: Source name

        Returns:
            Tuple of (estado, ciudad)
        """
        if source == 'cdmx':
            return "Ciudad de México", record.get('municipality', 'Ciudad de México')
        elif source == 'comprasmx':
            # Try to extract from text
            estado = self._extract_location_from_text(record.get('all_text', ''))
            return estado or "México", ""
        elif source == 'licita_ya':
            # LicitaYa covers national territory
            return record.get('state', 'México'), record.get('city', '')
        else:
            return "México", ""

    def _extract_location_from_text(self, text: str) -> str:
        """
        Extract Mexican state from text.

        Args:
            text: Text to search

        Returns:
            State name or empty string
        """
        mexican_states = [
            'aguascalientes', 'baja california', 'bcs', 'campeche', 'chiapas',
            'chihuahua', 'cdmx', 'coahuila', 'colima', 'durango', 'guanajuato',
            'guerrero', 'hidalgo', 'jalisco', 'méxico', 'michoacán', 'morelos',
            'nayarit', 'nuevo león', 'oaxaca', 'puebla', 'querétaro',
            'quintana roo', 'san luis potosí', 'sinaloa', 'sonora', 'tabasco',
            'tamaulipas', 'tlaxcala', 'veracruz', 'yucatán', 'zacatecas'
        ]

        text_lower = text.lower()
        for state in mexican_states:
            if state in text_lower:
                return state.title()
        return ""

    def _generate_url(self, record: Dict[str, Any], source: str) -> Optional[str]:
        """
        Generate or extract URL based on source.

        Args:
            record: Record dictionary
            source: Source name

        Returns:
            URL string or None
        """
        # Check if URL is already provided
        url_fields = ['url', 'link', 'url_original']
        for field in url_fields:
            if record.get(field):
                return record[field]

        # Generate source-specific URLs
        if source == 'cdmx':
            planning_id = record.get('planning_id') or record.get('id')
            if planning_id:
                return f"https://tianguisdigital.cdmx.gob.mx/planeaciones/{planning_id}"
        elif source == 'comprasmx':
            return "https://comprasmx.buengobierno.gob.mx/sitiopublico/"
        elif source == 'licita_ya':
            tender_id = record.get('id') or record.get('tender_id')
            if tender_id:
                return f"https://www.licitaya.com.mx/tender/{tender_id}"

        return None

    def _generate_fallback_id(self, record: Dict[str, Any], source: str) -> str:
        """
        Generate fallback ID when no ID is available.

        Args:
            record: Record dictionary
            source: Source name

        Returns:
            Generated ID
        """
        # Use content hash for ID generation
        content_parts = [
            str(record.get('titulo', '')),
            str(record.get('entidad', '')),
            str(record.get('fecha_catalogacion', '')),
            source
        ]
        content_str = '|'.join(content_parts)
        hash_val = hashlib.md5(content_str.encode()).hexdigest()[:12]
        return f"{source}_{hash_val}"

    def _generate_fallback_title(self, record: Dict[str, Any], source: str) -> str:
        """
        Generate fallback title when no title is available.

        Args:
            record: Record dictionary
            source: Source name

        Returns:
            Generated title
        """
        entity = record.get('entidad', source.upper())
        return f"Licitación {entity}"

    def _create_meaningful_title(self, record: Dict[str, Any], source: str,
                               entidad: str, descripcion: str, tipo_licitacion: str) -> Optional[str]:
        """
        Create a meaningful title from available record data.

        Args:
            record: Original record
            source: Source name
            entidad: Entity name
            descripcion: Description text
            tipo_licitacion: Procurement type

        Returns:
            Enhanced title or None if not possible
        """
        # Try to extract meaningful information from different sources
        meaningful_parts = []

        # Source-specific title extraction
        if source == 'cdmx':
            # For CDMX/Tianguis Digital, look for specific fields
            planning_name = record.get('planning_name') or record.get('name')
            if planning_name and not planning_name.lower().startswith('sin'):
                meaningful_parts.append(planning_name)

            # Check for procedure type or category
            method = record.get('hiring_method_name') or record.get('procurement_type')
            if method and len(method) > 5:
                meaningful_parts.append(method)

        elif source == 'comprasmx':
            # For ComprasMX, extract meaningful title from combined text
            raw_text = record.get('all_text', '') or record.get('descripcion', '')

            # Look for patterns like "C.123 ADQUISICION DE..." in the text
            title_patterns = [
                r'([A-Z]\.\d+\s+[A-Z][^|]*)',  # Pattern: C.123 DESCRIPTION
                r'(\d+-\d+-\d+\s+[A-Z][^|]*)',  # Pattern: 01-24-121 DESCRIPTION
                r'([A-Z]{3,}\s+[A-Z][^|]*)'     # Pattern: ADQUISICION DESCRIPTION
            ]

            for pattern in title_patterns:
                match = re.search(pattern, raw_text)
                if match:
                    potential_title = match.group(1).strip()
                    # Clean up the title (remove numbers at start if too long)
                    if len(potential_title) > 10 and 'ADQUISICION' in potential_title or 'SERVICIOS' in potential_title:
                        meaningful_parts.append(potential_title[:100])
                        break

            # Also check titulo_procedimiento as fallback
            titulo_proc = record.get('titulo_procedimiento')
            if not meaningful_parts and titulo_proc and not titulo_proc.lower().startswith('sin'):
                meaningful_parts.append(titulo_proc)

        elif source == 'licita_ya':
            # For LicitaYa, check subject or summary
            subject = record.get('subject') or record.get('summary')
            if subject and len(subject) > 10:
                meaningful_parts.append(subject)

        # If no source-specific title, try description
        if not meaningful_parts and descripcion and len(descripcion) > 20:
            # Take first meaningful sentence from description
            sentences = descripcion.split('.')
            for sentence in sentences[:2]:  # Check first 2 sentences
                clean_sentence = sentence.strip()
                if len(clean_sentence) > 15 and not clean_sentence.lower().startswith('sin'):
                    meaningful_parts.append(clean_sentence[:80])
                    break

        # If still nothing, try combining available info
        if not meaningful_parts:
            parts = []
            if tipo_licitacion and tipo_licitacion != "Licitación":
                parts.append(tipo_licitacion)
            if entidad and entidad != source.upper():
                parts.append(f"de {entidad}")

            if parts:
                meaningful_parts.extend(parts)

        # Create final title
        if meaningful_parts:
            title = " - ".join(meaningful_parts[:2])  # Max 2 parts
            # Clean and truncate
            title = clean_text(title)
            if len(title) > 150:
                title = title[:147] + "..."
            return title

        return None

    def _get_default_entity(self, source: str) -> str:
        """
        Get default entity name for source.

        Args:
            source: Source name

        Returns:
            Default entity name
        """
        defaults = {
            'cdmx': 'Ciudad de México',
            'comprasmx': 'ComprasMX',
            'licita_ya': 'LicitaYa'
        }
        return defaults.get(source, source.upper())

    def _extract_source_specific_metadata(self, record: Dict[str, Any], source: str) -> Dict[str, Any]:
        """
        Extract source-specific metadata.

        Args:
            record: Record dictionary
            source: Source name

        Returns:
            Source-specific metadata
        """
        if source == 'cdmx':
            return {
                'hiring_method': record.get('hiring_method'),
                'hiring_method_name': record.get('hiring_method_name'),
                'consolidated': record.get('consolidated'),
                'status': record.get('status')
            }
        elif source == 'comprasmx':
            return {
                'row_index': record.get('row_index'),
                'cell_count': record.get('cell_count'),
                'extraction_timestamp': record.get('extraction_timestamp'),
                'numero_referencia': record.get('numero_referencia'),
                'metodo_extraccion': 'optimized_scraping'
            }
        elif source == 'licita_ya':
            return {
                'keyword_search': record.get('keyword'),
                'api_date': record.get('api_date'),
                'page': record.get('page'),
                'category': record.get('category')
            }
        else:
            return {}

    def _calculate_completeness(self, fields: Dict[str, Any]) -> float:
        """
        Calculate data completeness score.

        Args:
            fields: Dictionary of fields to check

        Returns:
            Completeness score between 0 and 1
        """
        total_fields = len(fields)
        complete_fields = sum(1 for value in fields.values() if value)
        return complete_fields / total_fields if total_fields > 0 else 0.0

    def _calculate_reliability(self, record: Dict[str, Any], source: str) -> float:
        """
        Calculate data reliability score based on source and content.

        Args:
            record: Record dictionary
            source: Source name

        Returns:
            Reliability score between 0 and 1
        """
        base_reliability = {
            'cdmx': 0.95,      # Official API, very reliable
            'comprasmx': 0.8,  # Web scraping, good but can break
            'licita_ya': 0.9   # Commercial API, reliable
        }

        reliability = base_reliability.get(source, 0.7)

        # Adjust based on content quality
        if source == 'comprasmx':
            cell_count = record.get('cell_count', 0)
            if cell_count > 8:
                reliability += 0.1
            elif cell_count < 5:
                reliability -= 0.1

        # Ensure reliability is between 0 and 1
        return max(0.0, min(1.0, reliability))

    def _create_semantic_text(self, record: Dict[str, Any]) -> str:
        """
        Create semantic text for embeddings and search.

        Args:
            record: Normalized record

        Returns:
            Semantic text string
        """
        parts = []

        # Core content
        if record.get('titulo'):
            parts.append(record['titulo'])

        if record.get('descripcion'):
            parts.append(record['descripcion'])

        # Context
        if record.get('entidad'):
            parts.append(f"Entidad: {record['entidad']}")

        if record.get('estado'):
            parts.append(f"Estado: {record['estado']}")

        if record.get('tipo_licitacion'):
            parts.append(f"Tipo: {record['tipo_licitacion']}")

        if record.get('valor_estimado'):
            parts.append(f"Valor: ${record['valor_estimado']:,.2f}")

        return ' | '.join(parts)

    def _calculate_content_hash(self, record: Dict[str, Any]) -> str:
        """
        Calculate content hash for deduplication.

        Args:
            record: Normalized record

        Returns:
            Content hash
        """
        content_parts = [
            record.get('titulo', ''),
            record.get('entidad', ''),
            record.get('estado', ''),
            str(record.get('fecha_catalogacion', '')),
            str(record.get('valor_estimado', ''))
        ]

        content_str = '|'.join(str(part).lower() for part in content_parts)
        return hashlib.sha256(content_str.encode()).hexdigest()[:16]

    def _normalize_date(self, date_value: Union[str, date, datetime]) -> Optional[date]:
        """
        Normalize date value to date object.

        Args:
            date_value: Date in various formats

        Returns:
            Normalized date or None
        """
        if isinstance(date_value, date):
            return date_value
        elif isinstance(date_value, datetime):
            return date_value.date()
        elif isinstance(date_value, str):
            # Try different date patterns
            patterns = [
                '%Y-%m-%d',
                '%d/%m/%Y',
                '%m/%d/%Y',
                '%d-%m-%Y',
                '%Y%m%d'
            ]

            for pattern in patterns:
                try:
                    return datetime.strptime(date_value.strip(), pattern).date()
                except ValueError:
                    continue

        return None

    def _normalize_amount(self, amount_value: Union[str, int, float]) -> Optional[float]:
        """
        Normalize amount value to float.

        Args:
            amount_value: Amount in various formats

        Returns:
            Normalized amount or None
        """
        if isinstance(amount_value, (int, float)):
            return float(amount_value)
        elif isinstance(amount_value, str):
            # Remove currency symbols and formatting
            clean_amount = re.sub(r'[^\d.,]', '', amount_value)
            clean_amount = clean_amount.replace(',', '')

            try:
                return float(clean_amount)
            except ValueError:
                return None

        return None


# Convenience function for quick normalization
def normalize_extraction_results(records: List[Dict[str, Any]], source: str) -> List[Dict[str, Any]]:
    """
    Quick normalization function for extraction results.

    Args:
        records: List of raw records
        source: Source name

    Returns:
        List of normalized records
    """
    normalizer = UnifiedNormalizer()
    return normalizer.normalize_records(records, source)