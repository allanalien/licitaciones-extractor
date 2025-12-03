"""
Data normalizer utility for licitaciones extraction system.
"""

from datetime import date, datetime
from typing import List, Dict, Any, Optional, Union
import re
import html
from decimal import Decimal, InvalidOperation

try:
    from src.utils.logger import get_logger
except ImportError:
    import sys
    from pathlib import Path
    src_path = Path(__file__).parent.parent
    sys.path.insert(0, str(src_path))
    from utils.logger import get_logger


class DataNormalizer:
    """
    Utility class for normalizing data across different sources.

    Provides consistent data cleaning, validation, and normalization
    methods used by all extractors.
    """

    def __init__(self):
        """Initialize the data normalizer."""
        self.logger = get_logger("data_normalizer")

    def normalize_text(self, text: Union[str, None]) -> str:
        """
        Normalize and clean text data.

        Args:
            text: Text to normalize

        Returns:
            Cleaned and normalized text
        """
        if not text or not isinstance(text, str):
            return ""

        # Decode HTML entities
        normalized = html.unescape(text)

        # Remove HTML tags
        normalized = re.sub(r'<[^>]+>', '', normalized)

        # Normalize whitespace
        normalized = re.sub(r'\s+', ' ', normalized.strip())

        # Remove control characters
        normalized = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', normalized)

        # Handle common encoding issues
        normalized = self._fix_encoding_issues(normalized)

        return normalized.strip()

    def _fix_encoding_issues(self, text: str) -> str:
        """
        Fix common encoding issues in text.

        Args:
            text: Text to fix

        Returns:
            Text with encoding issues fixed
        """
        # Common replacements for encoding issues
        replacements = {
            'Ã¡': 'á',
            'Ã©': 'é',
            'Ã­': 'í',
            'Ã³': 'ó',
            'Ãº': 'ú',
            'Ã±': 'ñ',
            'Ã¼': 'ü',
            'Ã"': 'Ñ',
            'â€œ': '"',
            'â€': '"',
            'â€™': "'",
            'â€"': '-',
            'â€"': '—',
            'Â': '',
            'Â ': ' '
        }

        for old, new in replacements.items():
            text = text.replace(old, new)

        return text

    def normalize_date(self, date_value: Union[str, date, datetime, None]) -> Optional[date]:
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
            return self._parse_date_string(date_value.strip())

        return None

    def _parse_date_string(self, date_str: str) -> Optional[date]:
        """
        Parse date string using multiple formats.

        Args:
            date_str: Date string to parse

        Returns:
            Parsed date or None
        """
        if not date_str:
            return None

        # Remove common prefixes/suffixes
        date_str = re.sub(r'^(fecha|date|del|from|al|to):\s*', '', date_str, flags=re.I)
        date_str = date_str.strip()

        # Date formats to try
        date_formats = [
            "%Y-%m-%d",           # 2023-12-01
            "%d/%m/%Y",           # 01/12/2023
            "%m/%d/%Y",           # 12/01/2023
            "%d-%m-%Y",           # 01-12-2023
            "%Y/%m/%d",           # 2023/12/01
            "%d.%m.%Y",           # 01.12.2023
            "%Y-%m-%d %H:%M:%S",  # 2023-12-01 10:30:00
            "%d/%m/%Y %H:%M",     # 01/12/2023 10:30
            "%Y%m%d",             # 20231201
            "%d de %B de %Y",     # 01 de diciembre de 2023
            "%d de %b de %Y",     # 01 de dic de 2023
            "%B %d, %Y",          # December 01, 2023
            "%b %d, %Y",          # Dec 01, 2023
            "%d %B %Y",           # 01 December 2023
            "%d %b %Y",           # 01 Dec 2023
        ]

        for date_format in date_formats:
            try:
                return datetime.strptime(date_str, date_format).date()
            except ValueError:
                continue

        # Try parsing with Spanish month names
        try:
            return self._parse_spanish_date(date_str)
        except:
            pass

        # Try extracting date components with regex
        try:
            return self._extract_date_with_regex(date_str)
        except:
            pass

        self.logger.warning(f"Could not parse date string: {date_str}")
        return None

    def _parse_spanish_date(self, date_str: str) -> Optional[date]:
        """
        Parse Spanish date strings.

        Args:
            date_str: Spanish date string

        Returns:
            Parsed date or None
        """
        spanish_months = {
            'enero': 1, 'febrero': 2, 'marzo': 3, 'abril': 4,
            'mayo': 5, 'junio': 6, 'julio': 7, 'agosto': 8,
            'septiembre': 9, 'octubre': 10, 'noviembre': 11, 'diciembre': 12,
            'ene': 1, 'feb': 2, 'mar': 3, 'abr': 4,
            'may': 5, 'jun': 6, 'jul': 7, 'ago': 8,
            'sep': 9, 'oct': 10, 'nov': 11, 'dic': 12
        }

        # Pattern: "01 de diciembre de 2023" or "1 diciembre 2023"
        pattern = r'(\d{1,2})\s+(?:de\s+)?(\w+)\s+(?:de\s+)?(\d{4})'
        match = re.search(pattern, date_str.lower())

        if match:
            day = int(match.group(1))
            month_name = match.group(2)
            year = int(match.group(3))

            if month_name in spanish_months:
                month = spanish_months[month_name]
                return date(year, month, day)

        return None

    def _extract_date_with_regex(self, date_str: str) -> Optional[date]:
        """
        Extract date using regex patterns.

        Args:
            date_str: Date string

        Returns:
            Extracted date or None
        """
        # Pattern: YYYY-MM-DD or similar
        patterns = [
            r'(\d{4})[/-](\d{1,2})[/-](\d{1,2})',  # YYYY-MM-DD or YYYY/MM/DD
            r'(\d{1,2})[/-](\d{1,2})[/-](\d{4})',  # DD-MM-YYYY or MM-DD-YYYY
            r'(\d{4})(\d{2})(\d{2})',              # YYYYMMDD
        ]

        for pattern in patterns:
            match = re.search(pattern, date_str)
            if match:
                groups = [int(g) for g in match.groups()]

                # Try different interpretations
                interpretations = [
                    (groups[0], groups[1], groups[2]),  # Year, Month, Day
                    (groups[2], groups[1], groups[0]),  # Day, Month, Year
                    (groups[2], groups[0], groups[1]),  # Month, Day, Year
                ]

                for year, month, day in interpretations:
                    try:
                        if 1 <= month <= 12 and 1 <= day <= 31 and year > 1900:
                            return date(year, month, day)
                    except ValueError:
                        continue

        return None

    def normalize_amount(self, amount_value: Union[str, int, float, None]) -> Optional[float]:
        """
        Normalize amount value to float.

        Args:
            amount_value: Amount value in various formats

        Returns:
            Normalized float value or None
        """
        if not amount_value:
            return None

        if isinstance(amount_value, (int, float)):
            return float(amount_value) if amount_value >= 0 else None

        if isinstance(amount_value, str):
            return self._parse_amount_string(amount_value.strip())

        return None

    def _parse_amount_string(self, amount_str: str) -> Optional[float]:
        """
        Parse amount string to float.

        Args:
            amount_str: Amount string to parse

        Returns:
            Parsed amount or None
        """
        if not amount_str:
            return None

        # Remove currency symbols and common prefixes
        cleaned = re.sub(r'^(monto|amount|valor|$|USD|MXN|peso|pesos):\s*', '', amount_str, flags=re.I)

        # Remove currency symbols
        cleaned = re.sub(r'[\$€£¥₹₽]', '', cleaned)

        # Remove common words
        cleaned = re.sub(r'\b(pesos?|dollars?|usd|mxn|euros?)\b', '', cleaned, flags=re.I)

        # Handle thousands/millions indicators
        multiplier = 1
        if re.search(r'\b(mil|thousand)\b', cleaned, re.I):
            multiplier = 1000
            cleaned = re.sub(r'\b(mil|thousand)\b', '', cleaned, re.I)
        elif re.search(r'\b(mill[oó]n|million)\b', cleaned, re.I):
            multiplier = 1000000
            cleaned = re.sub(r'\b(mill[oó]n|million)\b', '', cleaned, re.I)

        # Clean up punctuation - handle both comma and period as decimal separators
        # First, remove spaces and non-numeric characters except commas and periods
        cleaned = re.sub(r'[^\d.,]', '', cleaned)

        if not cleaned:
            return None

        # Handle different decimal/thousands separators
        try:
            # If there are both commas and periods, determine which is decimal
            if ',' in cleaned and '.' in cleaned:
                # Check which comes last - that's likely the decimal separator
                last_comma = cleaned.rfind(',')
                last_period = cleaned.rfind('.')

                if last_period > last_comma:
                    # Period is decimal separator
                    cleaned = cleaned.replace(',', '')
                else:
                    # Comma is decimal separator
                    cleaned = cleaned.replace('.', '').replace(',', '.')

            elif ',' in cleaned:
                # Only comma - could be thousands or decimal
                if re.match(r'^\d{1,3}(,\d{3})+$', cleaned):
                    # Thousands separator pattern (e.g., 1,000,000)
                    cleaned = cleaned.replace(',', '')
                elif re.match(r'^\d+(,\d{1,2})$', cleaned):
                    # Decimal separator pattern (e.g., 123,45)
                    cleaned = cleaned.replace(',', '.')
                else:
                    # Assume thousands separator
                    cleaned = cleaned.replace(',', '')

            # Convert to float
            value = float(cleaned) * multiplier

            # Sanity check
            if value < 0 or value > 1e15:  # Reasonable limits
                return None

            return value

        except (ValueError, InvalidOperation):
            return None

    def normalize_entity_name(self, entity: str) -> str:
        """
        Normalize entity/organization name.

        Args:
            entity: Entity name to normalize

        Returns:
            Normalized entity name
        """
        if not entity:
            return ""

        # Clean basic formatting
        normalized = self.normalize_text(entity)

        # Remove common prefixes/suffixes
        prefixes = [
            r'^(secretar[íi]a de |secretaria de |ministerio de |ministry of )',
            r'^(dep(to|artamento)\.?\s+)',
            r'^(direccion general de |dirección general de )',
            r'^(instituto nacional de |instituto nacional del )'
        ]

        for prefix_pattern in prefixes:
            normalized = re.sub(prefix_pattern, '', normalized, flags=re.I)

        # Normalize common abbreviations
        abbreviations = {
            'sedesol': 'Secretaría de Desarrollo Social',
            'sep': 'Secretaría de Educación Pública',
            'ssa': 'Secretaría de Salud',
            'sct': 'Secretaría de Comunicaciones y Transportes',
            'sedena': 'Secretaría de la Defensa Nacional',
            'semar': 'Secretaría de Marina',
        }

        lower_normalized = normalized.lower()
        for abbr, full_name in abbreviations.items():
            if abbr in lower_normalized:
                normalized = full_name

        return normalized.strip()

    def normalize_location(self, location: str) -> Dict[str, str]:
        """
        Normalize location information into estado and ciudad.

        Args:
            location: Location string

        Returns:
            Dictionary with 'estado' and 'ciudad' keys
        """
        if not location:
            return {'estado': '', 'ciudad': ''}

        normalized = self.normalize_text(location)

        # Mexican states mapping
        states_mapping = {
            'aguascalientes': 'Aguascalientes',
            'baja california': 'Baja California',
            'baja california sur': 'Baja California Sur',
            'campeche': 'Campeche',
            'chiapas': 'Chiapas',
            'chihuahua': 'Chihuahua',
            'ciudad de mexico': 'Ciudad de México',
            'cdmx': 'Ciudad de México',
            'df': 'Ciudad de México',
            'distrito federal': 'Ciudad de México',
            'coahuila': 'Coahuila',
            'colima': 'Colima',
            'durango': 'Durango',
            'guanajuato': 'Guanajuato',
            'guerrero': 'Guerrero',
            'hidalgo': 'Hidalgo',
            'jalisco': 'Jalisco',
            'mexico': 'México',
            'estado de mexico': 'México',
            'michoacan': 'Michoacán',
            'morelos': 'Morelos',
            'nayarit': 'Nayarit',
            'nuevo leon': 'Nuevo León',
            'oaxaca': 'Oaxaca',
            'puebla': 'Puebla',
            'queretaro': 'Querétaro',
            'quintana roo': 'Quintana Roo',
            'san luis potosi': 'San Luis Potosí',
            'sinaloa': 'Sinaloa',
            'sonora': 'Sonora',
            'tabasco': 'Tabasco',
            'tamaulipas': 'Tamaulipas',
            'tlaxcala': 'Tlaxcala',
            'veracruz': 'Veracruz',
            'yucatan': 'Yucatán',
            'zacatecas': 'Zacatecas'
        }

        lower_location = normalized.lower()

        # Try to find state
        estado = ''
        for key, value in states_mapping.items():
            if key in lower_location:
                estado = value
                break

        # Try to extract ciudad (city)
        ciudad = ''
        # Look for patterns like "Ciudad, Estado" or "Ciudad - Estado"
        city_patterns = [
            r'^([^,]+),\s*(.+)$',  # Ciudad, Estado
            r'^([^-]+)\s*-\s*(.+)$',  # Ciudad - Estado
            r'^(.+),\s*(.+)$'  # Generic comma separator
        ]

        for pattern in city_patterns:
            match = re.match(pattern, normalized)
            if match:
                potential_city = match.group(1).strip()
                potential_state = match.group(2).strip()

                # Check if second part is a known state
                if potential_state.lower() in states_mapping:
                    ciudad = potential_city
                    if not estado:
                        estado = states_mapping[potential_state.lower()]
                    break

        # If no pattern matched and no state found, assume entire string is ciudad
        if not ciudad and not estado:
            ciudad = normalized

        return {'estado': estado, 'ciudad': ciudad}

    def normalize_tender_type(self, tender_type: str, title: str = '', description: str = '') -> str:
        """
        Normalize tender type using type string and context from title/description.

        Args:
            tender_type: Original tender type
            title: Tender title for context
            description: Tender description for context

        Returns:
            Normalized tender type
        """
        if not tender_type:
            # Infer from title and description
            return self._infer_tender_type(title, description)

        # Clean the type
        normalized = self.normalize_text(tender_type).lower()

        # Type mapping
        type_mapping = {
            # Construction/Infrastructure
            'obra publica': 'Obra Pública',
            'obra': 'Obra Pública',
            'construccion': 'Obra Pública',
            'infraestructura': 'Obra Pública',
            'public works': 'Obra Pública',
            'construction': 'Obra Pública',

            # Services
            'servicios': 'Servicios',
            'servicio': 'Servicios',
            'consultoria': 'Servicios',
            'asesoria': 'Servicios',
            'services': 'Servicios',
            'consulting': 'Servicios',

            # Supplies/Goods
            'suministros': 'Suministros',
            'suministro': 'Suministros',
            'adquisicion': 'Suministros',
            'bienes': 'Suministros',
            'compra': 'Suministros',
            'goods': 'Suministros',
            'supplies': 'Suministros',
            'procurement': 'Suministros',

            # Technology
            'tecnologia': 'Tecnología',
            'software': 'Tecnología',
            'hardware': 'Tecnología',
            'equipo tecnologico': 'Tecnología',
            'technology': 'Tecnología',

            # Lease/Rent
            'arrendamiento': 'Arrendamiento',
            'renta': 'Arrendamiento',
            'lease': 'Arrendamiento',
            'rental': 'Arrendamiento',

            # Maintenance
            'mantenimiento': 'Mantenimiento',
            'maintenance': 'Mantenimiento'
        }

        # Check exact matches first
        for key, value in type_mapping.items():
            if key in normalized:
                return value

        # If no match, try to infer from context
        return self._infer_tender_type(title, description)

    def _infer_tender_type(self, title: str, description: str) -> str:
        """
        Infer tender type from title and description.

        Args:
            title: Tender title
            description: Tender description

        Returns:
            Inferred tender type
        """
        text = f"{title} {description}".lower()

        if any(word in text for word in ['construcción', 'obra', 'infraestructura', 'edificio', 'carretera']):
            return "Obra Pública"
        elif any(word in text for word in ['servicios', 'consultoría', 'asesoría', 'capacitación']):
            return "Servicios"
        elif any(word in text for word in ['suministro', 'adquisición', 'compra', 'medicamentos', 'alimentos']):
            return "Suministros"
        elif any(word in text for word in ['tecnología', 'software', 'equipo', 'computadora', 'sistema']):
            return "Tecnología"
        elif any(word in text for word in ['arrendamiento', 'renta', 'alquiler']):
            return "Arrendamiento"
        elif any(word in text for word in ['mantenimiento', 'reparación']):
            return "Mantenimiento"
        else:
            return "General"

    def create_metadata_template(self, source: str) -> Dict[str, Any]:
        """
        Create metadata template for a specific source.

        Args:
            source: Source name

        Returns:
            Metadata template dictionary
        """
        base_metadata = {
            "fuente_original": source,
            "fecha_extraccion": datetime.utcnow().isoformat(),
            "parametros_busqueda": {},
            "datos_especificos": {},
            "calidad_datos": {
                "completitud": 0.0,
                "confiabilidad": 0.0
            }
        }

        if source == "licita_ya":
            base_metadata["datos_especificos"] = {
                "licita_ya": {
                    "smart_search": None,
                    "lots": [],
                    "agency": None
                }
            }
        elif source == "cdmx":
            base_metadata["datos_especificos"] = {
                "cdmx": {
                    "hiring_method": None,
                    "hiring_method_name": None,
                    "consolidated": False
                }
            }
        elif source == "comprasmx":
            base_metadata["datos_especificos"] = {
                "comprasmx": {
                    "pagina_origen": None,
                    "metodo_extraccion": "scraping"
                }
            }

        return base_metadata

    def calculate_completeness_score(self, record: Dict[str, Any]) -> float:
        """
        Calculate completeness score for a record.

        Args:
            record: Record to analyze

        Returns:
            Completeness score between 0.0 and 1.0
        """
        required_fields = [
            'tender_id', 'fuente', 'titulo', 'texto_semantico'
        ]

        important_fields = [
            'descripcion', 'entidad', 'fecha_catalogacion'
        ]

        optional_fields = [
            'estado', 'ciudad', 'fecha_apertura', 'valor_estimado',
            'tipo_licitacion', 'url_original'
        ]

        # Score calculation
        required_score = sum(1 for field in required_fields if record.get(field)) / len(required_fields)
        important_score = sum(1 for field in important_fields if record.get(field)) / len(important_fields)
        optional_score = sum(1 for field in optional_fields if record.get(field)) / len(optional_fields)

        # Weighted average
        total_score = (required_score * 0.6) + (important_score * 0.3) + (optional_score * 0.1)

        return min(1.0, total_score)

    def normalize_record(self, raw_record: Dict[str, Any], source: str) -> Dict[str, Any]:
        """
        Normalize a raw record from a specific source.

        Args:
            raw_record: Raw record dictionary
            source: Source name

        Returns:
            Normalized record dictionary
        """
        # Start with a metadata template
        normalized_data = {'metadata': self.create_metadata_template(source)}

        # Basic fields
        normalized_data['tender_id'] = str(raw_record.get('id') or raw_record.get('tender_id') or '')
        normalized_data['fuente'] = source
        normalized_data['titulo'] = self.normalize_text(raw_record.get('title') or raw_record.get('titulo'))
        normalized_data['descripcion'] = self.normalize_text(raw_record.get('description') or raw_record.get('descripcion'))
        normalized_data['url_original'] = self.normalize_text(raw_record.get('url') or raw_record.get('url_original'))

        # Dates
        normalized_data['fecha_catalogacion'] = self.normalize_date(raw_record.get('publish_date') or raw_record.get('fecha_publicacion'))
        normalized_data['fecha_apertura'] = self.normalize_date(raw_record.get('open_date') or raw_record.get('fecha_apertura'))

        # Financial
        normalized_data['valor_estimado'] = self.normalize_amount(raw_record.get('amount') or raw_record.get('valor_estimado'))

        # Entity and location
        entity_name = self.normalize_entity_name(raw_record.get('entity') or raw_record.get('entidad') or '')
        location_info = self.normalize_location(raw_record.get('location') or raw_record.get('ubicacion') or '')
        normalized_data['entidad'] = entity_name
        normalized_data['estado'] = location_info['estado']
        normalized_data['ciudad'] = location_info['ciudad']

        # Tender type
        normalized_data['tipo_licitacion'] = self.normalize_tender_type(
            raw_record.get('tender_type') or raw_record.get('tipo_licitacion') or '',
            title=normalized_data['titulo'],
            description=normalized_data['descripcion']
        )

        # Add raw record to metadata for traceability
        normalized_data['metadata']['datos_especificos']['raw_data'] = raw_record

        # Calculate completeness and add to metadata
        completeness = self.calculate_completeness_score(normalized_data)
        normalized_data['metadata']['calidad_datos']['completitud'] = completeness

        return normalized_data