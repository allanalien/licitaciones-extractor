"""
Licita Ya API extractor for licitaciones extraction system.
"""

import requests
from datetime import date, datetime
from typing import List, Dict, Any, Optional
import time
from urllib.parse import urljoin

from .base_extractor import BaseExtractor, ExtractionResult


class LicitaYaExtractor(BaseExtractor):
    """
    Extractor for Licita Ya private API.

    This extractor uses the private API to fetch tenders based on keywords
    and date filters.
    """

    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize Licita Ya extractor.

        Args:
            config: Configuration dictionary containing API credentials
        """
        super().__init__("licita_ya", config)

    def _setup_extractor(self):
        """Setup Licita Ya specific configuration."""
        self.api_key = self.config.get('api_key')
        self.base_url = self.config.get('base_url', 'https://www.licitaya.com.mx/api/v1')
        self.timeout = self.config.get('timeout', 30)
        self.retry_attempts = self.config.get('retry_attempts', 3)
        self.retry_delay = self.config.get('retry_delay', 1)
        self.keywords = self.config.get('keywords', [])

        if not self.api_key:
            raise ValueError("API key is required for Licita Ya extractor")

        if not self.keywords:
            raise ValueError("Keywords list is required for Licita Ya extractor")

        # Setup session with headers
        self.session = requests.Session()
        self.session.headers.update({
            'X-API-KEY': self.api_key,
            'User-Agent': 'LicitacionesExtractor/1.0',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        })

    def extract_data(self, target_date: date, **kwargs) -> ExtractionResult:
        """
        Extract data from Licita Ya API for a specific date.

        Args:
            target_date: Date to extract data for
            **kwargs: Additional parameters (e.g., specific_keywords)

        Returns:
            ExtractionResult with extracted data
        """
        all_records = []
        all_errors = []
        keywords_to_use = kwargs.get('specific_keywords', self.keywords)

        self.logger.info(f"Starting Licita Ya extraction for {target_date} with {len(keywords_to_use)} keywords")

        for keyword in keywords_to_use:
            try:
                keyword_records = self._extract_by_keyword(target_date, keyword)
                all_records.extend(keyword_records)

                # Rate limiting - small delay between keyword requests
                time.sleep(0.5)

            except Exception as e:
                error_msg = f"Error extracting data for keyword '{keyword}': {str(e)}"
                self.logger.error(error_msg)
                all_errors.append(error_msg)

        # Remove duplicates based on tender_id or unique content
        unique_records = self._remove_duplicates(all_records)

        self.logger.info(f"Extracted {len(unique_records)} unique records from {len(all_records)} total")

        return ExtractionResult(
            success=len(all_errors) < len(keywords_to_use),  # Success if at least one keyword worked
            records=unique_records,
            errors=all_errors,
            source=self.source_name,
            extraction_date=datetime.utcnow(),
            metadata={
                "keywords_processed": len(keywords_to_use),
                "keywords_failed": len(all_errors),
                "total_records_before_dedup": len(all_records),
                "unique_records": len(unique_records),
                "target_date": target_date.isoformat()
            }
        )

    def _extract_by_keyword(self, target_date: date, keyword: str) -> List[Dict[str, Any]]:
        """
        Extract data for a specific keyword.

        Args:
            target_date: Date to extract data for
            keyword: Keyword to search for

        Returns:
            List of records for the keyword
        """
        all_records = []
        page = 1
        max_pages = 10  # Safety limit

        while page <= max_pages:
            try:
                records = self._fetch_page(target_date, keyword, page)

                if not records:
                    # No more records, break the loop
                    break

                all_records.extend(records)

                # If we got less than the expected page size, we're likely at the end
                if len(records) < 25:  # Default page size
                    break

                page += 1

                # Small delay between pages
                time.sleep(0.3)

            except Exception as e:
                self.logger.error(f"Error fetching page {page} for keyword '{keyword}': {str(e)}")
                break

        self.logger.info(f"Extracted {len(all_records)} records for keyword '{keyword}' from {page-1} pages")
        return all_records

    def _fetch_page(self, target_date: date, keyword: str, page: int) -> List[Dict[str, Any]]:
        """
        Fetch a single page of results.

        Args:
            target_date: Date to search for
            keyword: Keyword to search for
            page: Page number to fetch

        Returns:
            List of records from the page
        """
        url = urljoin(self.base_url, 'tender/search')

        params = {
            'date': target_date.strftime('%Y%m%d'),
            'keyword': keyword,
            'page': page,
            'items': 25,
            'smartsearch': 1,
            'listing': 0
        }

        for attempt in range(self.retry_attempts):
            try:
                response = self.session.get(
                    url,
                    params=params,
                    timeout=self.timeout
                )
                response.raise_for_status()

                data = response.json()

                # Extract records from response
                # The exact structure may vary, adjust based on actual API response
                if 'data' in data:
                    return data['data']
                elif 'results' in data:
                    return data['results']
                elif isinstance(data, list):
                    return data
                else:
                    self.logger.warning(f"Unexpected response structure: {data.keys() if isinstance(data, dict) else type(data)}")
                    return []

            except requests.exceptions.RequestException as e:
                if attempt < self.retry_attempts - 1:
                    self.logger.warning(f"Attempt {attempt + 1} failed for page {page}, retrying: {str(e)}")
                    time.sleep(self.retry_delay * (attempt + 1))
                else:
                    raise
            except Exception as e:
                self.logger.error(f"Unexpected error fetching page {page}: {str(e)}")
                raise

        return []

    def _remove_duplicates(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Remove duplicate records based on tender ID or content similarity.

        Args:
            records: List of records to deduplicate

        Returns:
            List of unique records
        """
        seen_ids = set()
        unique_records = []

        for record in records:
            # Try to find a unique identifier
            tender_id = (
                record.get('tender_id') or
                record.get('id') or
                record.get('numero_licitacion') or
                record.get('reference')
            )

            if tender_id:
                if tender_id not in seen_ids:
                    seen_ids.add(tender_id)
                    unique_records.append(record)
            else:
                # If no ID available, create a content-based hash
                content_hash = self._create_content_hash(record)
                if content_hash not in seen_ids:
                    seen_ids.add(content_hash)
                    unique_records.append(record)

        return unique_records

    def _create_content_hash(self, record: Dict[str, Any]) -> str:
        """
        Create a content-based hash for deduplication.

        Args:
            record: Record to hash

        Returns:
            Content hash string
        """
        import hashlib

        # Use title, entity, and date to create a unique identifier
        content_parts = [
            str(record.get('titulo', '')),
            str(record.get('entidad', '')),
            str(record.get('fecha_apertura', '')),
            str(record.get('valor_estimado', ''))
        ]

        content_string = '|'.join(content_parts)
        return hashlib.md5(content_string.encode()).hexdigest()

    def normalize_data(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Normalize Licita Ya data to standard format.

        Args:
            raw_data: Raw data from Licita Ya API

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
                self.logger.error(f"Error normalizing record: {str(e)}")
                continue

        return normalized_data

    def _normalize_single_record(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Normalize a single record from Licita Ya.

        Args:
            record: Single raw record

        Returns:
            Normalized record or None if invalid
        """
        # Extract basic information - adjust field names based on actual API response
        tender_id = (
            record.get('tender_id') or
            record.get('id') or
            record.get('numero_licitacion') or
            self._generate_tender_id(record)
        )

        titulo = self._clean_text(record.get('titulo') or record.get('title') or record.get('nombre'))
        descripcion = self._clean_text(record.get('descripcion') or record.get('description') or record.get('detalle'))
        entidad = self._clean_text(record.get('entidad') or record.get('agency') or record.get('dependencia'))

        # Dates
        fecha_catalogacion = self._normalize_date(
            record.get('fecha_catalogacion') or
            record.get('fecha_publicacion') or
            record.get('publication_date')
        )

        fecha_apertura = self._normalize_date(
            record.get('fecha_apertura') or
            record.get('apertura_date') or
            record.get('opening_date')
        )

        # Amount
        valor_estimado = self._normalize_amount(
            record.get('valor_estimado') or
            record.get('monto') or
            record.get('amount') or
            record.get('budget')
        )

        # Location information
        estado = self._clean_text(record.get('estado') or record.get('state'))
        ciudad = self._clean_text(record.get('ciudad') or record.get('city') or record.get('municipio'))

        # Type and category
        tipo_licitacion = self._clean_text(
            record.get('tipo_licitacion') or
            record.get('tipo') or
            record.get('category') or
            record.get('procurement_type')
        )

        # URL
        url_original = record.get('url') or record.get('link') or record.get('tender_url')

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
                    "licita_ya": {
                        "smart_search": record.get('smart_search'),
                        "lots": record.get('lots', []),
                        "agency": record.get('agency')
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

    def validate_data(self, normalized_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Validate normalized data from Licita Ya.

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
                self.logger.warning(f"Record {record.get('tender_id')} failed validation: {validation_errors}")

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

        # Validate tender_id format
        tender_id = record.get('tender_id')
        if tender_id and not isinstance(tender_id, str):
            errors.append("tender_id must be a string")

        # Validate dates
        if record.get('fecha_catalogacion') and not isinstance(record.get('fecha_catalogacion'), date):
            errors.append("fecha_catalogacion must be a date object")

        if record.get('fecha_apertura') and not isinstance(record.get('fecha_apertura'), date):
            errors.append("fecha_apertura must be a date object")

        # Validate amount
        if record.get('valor_estimado') and not isinstance(record.get('valor_estimado'), (int, float)):
            errors.append("valor_estimado must be a number")

        # Validate URL format if present
        if record.get('url_original'):
            url = record.get('url_original')
            if not isinstance(url, str) or not (url.startswith('http://') or url.startswith('https://')):
                errors.append("url_original must be a valid URL")

        return errors

    def get_source_info(self) -> Dict[str, Any]:
        """
        Get information about the Licita Ya extractor.

        Returns:
            Dictionary with source information
        """
        base_info = super().get_source_info()
        base_info.update({
            "api_base_url": self.base_url,
            "keywords_count": len(self.keywords),
            "has_api_key": bool(self.api_key),
            "timeout": self.timeout,
            "retry_attempts": self.retry_attempts
        })
        return base_info