"""
CDMX API extractor for licitaciones extraction system.
"""

import requests
from datetime import date, datetime
from typing import List, Dict, Any, Optional
import time

from .base_extractor import BaseExtractor, ExtractionResult


class CDMXExtractor(BaseExtractor):
    """
    Extractor for CDMX public API.

    This extractor uses the public API from Ciudad de México to fetch
    procurement planning data.
    """

    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize CDMX extractor.

        Args:
            config: Configuration dictionary
        """
        super().__init__("cdmx", config)

    def _setup_extractor(self):
        """Setup CDMX specific configuration."""
        self.base_url = self.config.get(
            'base_url',
            'https://datosabiertostianguisdigital.cdmx.gob.mx/api/v1'
        )
        self.timeout = self.config.get('timeout', 30)
        self.retry_attempts = self.config.get('retry_attempts', 3)
        self.retry_delay = self.config.get('retry_delay', 1)

        # Setup session with headers
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'LicitacionesExtractor/1.0',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        })

    def extract_data(self, target_date: date, **kwargs) -> ExtractionResult:
        """
        Extract data from CDMX API for a specific date.

        Args:
            target_date: Date to extract data for
            **kwargs: Additional parameters

        Returns:
            ExtractionResult with extracted data
        """
        self.logger.info(f"Starting CDMX extraction for {target_date}")

        try:
            # Fetch all planning data for the target date
            raw_records = self._fetch_planning_data(target_date)

            # Filter for relevant records (if any additional filtering is needed)
            filtered_records = self._filter_relevant_records(raw_records)

            self.logger.info(f"Extracted {len(filtered_records)} records from CDMX API")

            return ExtractionResult(
                success=True,
                records=filtered_records,
                errors=[],
                source=self.source_name,
                extraction_date=datetime.utcnow(),
                metadata={
                    "target_date": target_date.isoformat(),
                    "total_records": len(raw_records),
                    "filtered_records": len(filtered_records),
                    "api_endpoint": "plannings"
                }
            )

        except Exception as e:
            error_msg = f"Error extracting CDMX data: {str(e)}"
            self.logger.error(error_msg)
            return ExtractionResult(
                success=False,
                records=[],
                errors=[error_msg],
                source=self.source_name,
                extraction_date=datetime.utcnow(),
                metadata={"target_date": target_date.isoformat()}
            )

    def _fetch_planning_data(self, target_date: date) -> List[Dict[str, Any]]:
        """
        Fetch planning data from CDMX API.

        Args:
            target_date: Date to fetch data for

        Returns:
            List of raw records from the API
        """
        url = f"{self.base_url}/plannings"

        # Format date as dd/MM/yyyy as required by CDMX API
        date_str = target_date.strftime('%d/%m/%Y')

        params = {
            'hiring_method': '1,2,3',  # All hiring methods
            'consolidated': 'FALSE',
            'start_date': date_str,
            'end_date': date_str
        }

        for attempt in range(self.retry_attempts):
            try:
                self.logger.info(f"Fetching CDMX data: {url} with params: {params}")

                response = self.session.get(
                    url,
                    params=params,
                    timeout=self.timeout
                )
                response.raise_for_status()

                data = response.json()

                # Handle different response structures
                if isinstance(data, dict):
                    if 'data' in data:
                        return data['data']
                    elif 'results' in data:
                        return data['results']
                    elif 'plannings' in data:
                        return data['plannings']
                    else:
                        # If it's a dict with other structure, try to extract records
                        self.logger.warning(f"Unexpected CDMX response structure: {list(data.keys())}")
                        return []
                elif isinstance(data, list):
                    return data
                else:
                    self.logger.warning(f"Unexpected CDMX response type: {type(data)}")
                    return []

            except requests.exceptions.RequestException as e:
                if attempt < self.retry_attempts - 1:
                    self.logger.warning(f"CDMX API attempt {attempt + 1} failed, retrying: {str(e)}")
                    time.sleep(self.retry_delay * (attempt + 1))
                else:
                    raise
            except Exception as e:
                self.logger.error(f"Unexpected error fetching CDMX data: {str(e)}")
                raise

        return []

    def _filter_relevant_records(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Filter records for relevance based on status and content.

        Args:
            records: Raw records from API

        Returns:
            Filtered list of relevant records
        """
        filtered_records = []

        for record in records:
            # Skip records without essential information
            if not record.get('name') and not record.get('description'):
                continue

            # Skip cancelled or invalid records
            status = record.get('status', '').lower()
            if status in ['cancelado', 'cancelled', 'invalid', 'desierto']:
                continue

            # Include record if it passes basic filters
            filtered_records.append(record)

        return filtered_records

    def normalize_data(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Normalize CDMX data to standard format.

        Args:
            raw_data: Raw data from CDMX API

        Returns:
            List of normalized data dictionaries
        """
        normalized_data = []

        for record in raw_data:
            try:
                normalized_record = self._normalize_single_record(record)
                if normalized_record:
                    normalized_data.append(normalized_record)
            except Exception as e:
                self.logger.error(f"Error normalizing CDMX record: {str(e)}")
                continue

        return normalized_data

    def _normalize_single_record(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Normalize a single record from CDMX.

        Args:
            record: Single raw record

        Returns:
            Normalized record or None if invalid
        """
        # Extract basic information
        tender_id = (
            record.get('planning_id') or
            record.get('id') or
            record.get('reference_number') or
            self._generate_tender_id(record)
        )

        titulo = self._clean_text(
            record.get('name') or
            record.get('title') or
            record.get('planning_name')
        )

        descripcion = self._clean_text(
            record.get('description') or
            record.get('details') or
            record.get('planning_description')
        )

        entidad = self._clean_text(
            record.get('entity') or
            record.get('dependencia') or
            record.get('organization') or
            record.get('institution') or
            "Ciudad de México"
        )

        # Dates - CDMX API might use different date field names
        fecha_catalogacion = self._normalize_date(
            record.get('planning_date') or
            record.get('publication_date') or
            record.get('created_at')
        )

        fecha_apertura = self._normalize_date(
            record.get('opening_date') or
            record.get('submission_deadline') or
            record.get('deadline')
        )

        # Amount
        valor_estimado = self._normalize_amount(
            record.get('estimated_amount') or
            record.get('budget') or
            record.get('amount') or
            record.get('estimated_value')
        )

        # Location - CDMX by default
        estado = "Ciudad de México"
        ciudad = self._clean_text(record.get('municipality') or "Ciudad de México")

        # Type and category
        tipo_licitacion = self._clean_text(
            record.get('hiring_method_name') or
            record.get('procurement_type') or
            record.get('method') or
            record.get('type')
        )

        # URL construction - CDMX might provide links or we construct them
        url_original = (
            record.get('url') or
            record.get('link') or
            self._construct_cdmx_url(record)
        )

        # Create normalized record
        normalized_record = {
            "tender_id": str(tender_id),
            "fuente": self.source_name,
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
                "fuente_original": self.source_name,
                "fecha_extraccion": datetime.utcnow().isoformat(),
                "parametros_busqueda": {},
                "datos_especificos": {
                    "cdmx": {
                        "hiring_method": record.get('hiring_method'),
                        "hiring_method_name": record.get('hiring_method_name'),
                        "consolidated": record.get('consolidated'),
                        "status": record.get('status')
                    }
                },
                "calidad_datos": {
                    "completitud": 0.0,
                    "confiabilidad": 0.0
                }
            }
        }

        # Create semantic text
        normalized_record["texto_semantico"] = self._create_semantic_text(normalized_record)

        return normalized_record

    def _construct_cdmx_url(self, record: Dict[str, Any]) -> Optional[str]:
        """
        Construct URL for CDMX tender if not provided.

        Args:
            record: Record data

        Returns:
            Constructed URL or None
        """
        planning_id = record.get('planning_id') or record.get('id')
        if planning_id:
            return f"https://tianguisdigital.cdmx.gob.mx/planeaciones/{planning_id}"
        return None

    def validate_data(self, normalized_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Validate normalized data from CDMX.

        Args:
            normalized_data: Normalized data to validate

        Returns:
            List of valid data dictionaries
        """
        valid_data = []

        for record in normalized_data:
            validation_errors = self._validate_single_record(record)

            if not validation_errors:
                valid_data.append(record)
            else:
                self.logger.warning(f"CDMX record {record.get('tender_id')} failed validation: {validation_errors}")

        return valid_data

    def _validate_single_record(self, record: Dict[str, Any]) -> List[str]:
        """
        Validate a single normalized record.

        Args:
            record: Record to validate

        Returns:
            List of validation errors (empty if valid)
        """
        errors = []

        # Check required fields
        missing_fields = self._validate_required_fields(record)
        errors.extend(missing_fields)

        # CDMX specific validations
        # Ensure it's actually from CDMX
        if record.get('fuente') != 'cdmx':
            errors.append("Record must be from cdmx source")

        # Validate estado is Ciudad de México
        if record.get('estado') != "Ciudad de México":
            errors.append("Estado must be 'Ciudad de México' for CDMX records")

        # Validate dates
        if record.get('fecha_catalogacion') and not isinstance(record.get('fecha_catalogacion'), date):
            errors.append("fecha_catalogacion must be a date object")

        if record.get('fecha_apertura') and not isinstance(record.get('fecha_apertura'), date):
            errors.append("fecha_apertura must be a date object")

        # Validate amount
        if record.get('valor_estimado') and not isinstance(record.get('valor_estimado'), (int, float)):
            errors.append("valor_estimado must be a number")

        # Check for minimum content length
        titulo = record.get('titulo', '')
        if len(titulo) < 10:
            errors.append("titulo must have at least 10 characters")

        return errors

    def get_source_info(self) -> Dict[str, Any]:
        """
        Get information about the CDMX extractor.

        Returns:
            Dictionary with source information
        """
        base_info = super().get_source_info()
        base_info.update({
            "api_base_url": self.base_url,
            "timeout": self.timeout,
            "retry_attempts": self.retry_attempts,
            "geographic_scope": "Ciudad de México",
            "api_type": "public"
        })
        return base_info