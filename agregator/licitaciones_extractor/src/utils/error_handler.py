"""
Production-ready error handling and data validation for licitaciones system.
"""

from typing import Dict, Any, List, Optional, Callable, Union
from datetime import datetime, date
from dataclasses import dataclass
from enum import Enum
import traceback
import logging

try:
    from src.utils.logger import get_logger
except ImportError:
    import sys
    from pathlib import Path
    src_path = Path(__file__).parent.parent
    sys.path.insert(0, str(src_path))
    from utils.logger import get_logger


class ErrorSeverity(Enum):
    """Error severity levels."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class ErrorCode(Enum):
    """Standardized error codes."""
    VALIDATION_FAILED = "VAL001"
    MISSING_REQUIRED_FIELD = "VAL002"
    INVALID_DATA_TYPE = "VAL003"
    INVALID_DATE_FORMAT = "VAL004"
    INVALID_AMOUNT_FORMAT = "VAL005"
    DUPLICATE_RECORD = "DUP001"
    DATABASE_ERROR = "DB001"
    API_ERROR = "API001"
    PROCESSING_ERROR = "PROC001"
    MEMORY_ERROR = "MEM001"
    CONFIGURATION_ERROR = "CFG001"


@dataclass
class ValidationError:
    """Detailed validation error information."""
    code: ErrorCode
    field: str
    value: Any
    message: str
    severity: ErrorSeverity
    context: Dict[str, Any]
    timestamp: datetime

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for logging/storage."""
        return {
            "code": self.code.value,
            "field": self.field,
            "value": str(self.value) if self.value is not None else None,
            "message": self.message,
            "severity": self.severity.value,
            "context": self.context,
            "timestamp": self.timestamp.isoformat()
        }


@dataclass
class ProcessingResult:
    """Result of processing with error tracking."""
    success: bool
    data: Any
    errors: List[ValidationError]
    warnings: List[ValidationError]
    metadata: Dict[str, Any]

    @property
    def has_critical_errors(self) -> bool:
        """Check if result has critical errors."""
        return any(error.severity == ErrorSeverity.CRITICAL for error in self.errors)

    @property
    def error_summary(self) -> Dict[str, int]:
        """Get error count by severity."""
        summary = {severity.value: 0 for severity in ErrorSeverity}
        for error in self.errors:
            summary[error.severity.value] += 1
        return summary


class ProductionValidator:
    """
    Production-ready validator with comprehensive error handling.
    """

    def __init__(self):
        """Initialize validator."""
        self.logger = get_logger("production_validator")
        self._required_fields = {
            'tender_id', 'fuente', 'titulo', 'texto_semantico'
        }
        self._optional_fields = {
            'descripcion', 'entidad', 'estado', 'ciudad', 'fecha_catalogacion',
            'fecha_apertura', 'valor_estimado', 'tipo_licitacion', 'url_original',
            'metadata', 'embeddings'
        }

    def validate_record(self, record: Dict[str, Any],
                       strict: bool = True) -> ProcessingResult:
        """
        Validate a single record with comprehensive error tracking.

        Args:
            record: Record to validate
            strict: If True, fails on any validation error

        Returns:
            ProcessingResult with validation details
        """
        errors = []
        warnings = []
        validated_record = record.copy()

        # Validate required fields
        errors.extend(self._validate_required_fields(record))

        # Validate data types and formats
        errors.extend(self._validate_data_types(record))

        # Validate business rules
        warnings.extend(self._validate_business_rules(record))

        # Clean and normalize data if not in strict mode
        if not strict:
            validated_record = self._clean_and_normalize(record, errors)

        success = len(errors) == 0 if strict else not any(
            error.severity == ErrorSeverity.CRITICAL for error in errors
        )

        return ProcessingResult(
            success=success,
            data=validated_record,
            errors=errors,
            warnings=warnings,
            metadata={
                "validation_timestamp": datetime.utcnow().isoformat(),
                "strict_mode": strict,
                "record_id": record.get('tender_id', 'unknown')
            }
        )

    def _validate_required_fields(self, record: Dict[str, Any]) -> List[ValidationError]:
        """Validate required fields are present."""
        errors = []

        for field in self._required_fields:
            value = record.get(field)
            if not value or (isinstance(value, str) and not value.strip()):
                errors.append(ValidationError(
                    code=ErrorCode.MISSING_REQUIRED_FIELD,
                    field=field,
                    value=value,
                    message=f"Required field '{field}' is missing or empty",
                    severity=ErrorSeverity.CRITICAL,
                    context={"required_fields": list(self._required_fields)},
                    timestamp=datetime.utcnow()
                ))

        return errors

    def _validate_data_types(self, record: Dict[str, Any]) -> List[ValidationError]:
        """Validate data types and formats."""
        errors = []

        # Validate string fields
        string_fields = ['tender_id', 'fuente', 'titulo', 'descripcion', 'entidad',
                        'estado', 'ciudad', 'tipo_licitacion', 'url_original', 'texto_semantico']

        for field in string_fields:
            value = record.get(field)
            if value is not None and not isinstance(value, str):
                errors.append(ValidationError(
                    code=ErrorCode.INVALID_DATA_TYPE,
                    field=field,
                    value=value,
                    message=f"Field '{field}' must be a string, got {type(value).__name__}",
                    severity=ErrorSeverity.HIGH,
                    context={"expected_type": "string", "actual_type": type(value).__name__},
                    timestamp=datetime.utcnow()
                ))

        # Validate date fields
        date_fields = ['fecha_catalogacion', 'fecha_apertura']
        for field in date_fields:
            value = record.get(field)
            if value is not None:
                if isinstance(value, str):
                    # Try to parse date string
                    if not self._is_valid_date_string(value):
                        errors.append(ValidationError(
                            code=ErrorCode.INVALID_DATE_FORMAT,
                            field=field,
                            value=value,
                            message=f"Field '{field}' has invalid date format",
                            severity=ErrorSeverity.MEDIUM,
                            context={"expected_formats": ["YYYY-MM-DD", "DD/MM/YYYY"]},
                            timestamp=datetime.utcnow()
                        ))
                elif not isinstance(value, (date, datetime)):
                    errors.append(ValidationError(
                        code=ErrorCode.INVALID_DATA_TYPE,
                        field=field,
                        value=value,
                        message=f"Field '{field}' must be a date, got {type(value).__name__}",
                        severity=ErrorSeverity.HIGH,
                        context={"expected_type": "date", "actual_type": type(value).__name__},
                        timestamp=datetime.utcnow()
                    ))

        # Validate numeric fields
        if 'valor_estimado' in record:
            value = record['valor_estimado']
            if value is not None:
                if isinstance(value, str):
                    if not self._is_valid_numeric_string(value):
                        errors.append(ValidationError(
                            code=ErrorCode.INVALID_AMOUNT_FORMAT,
                            field='valor_estimado',
                            value=value,
                            message="Invalid amount format",
                            severity=ErrorSeverity.MEDIUM,
                            context={"expected": "numeric value or parseable string"},
                            timestamp=datetime.utcnow()
                        ))
                elif not isinstance(value, (int, float)):
                    errors.append(ValidationError(
                        code=ErrorCode.INVALID_DATA_TYPE,
                        field='valor_estimado',
                        value=value,
                        message=f"Amount must be numeric, got {type(value).__name__}",
                        severity=ErrorSeverity.MEDIUM,
                        context={"expected_type": "numeric", "actual_type": type(value).__name__},
                        timestamp=datetime.utcnow()
                    ))

        # Validate metadata structure
        if 'metadata' in record:
            metadata = record['metadata']
            if metadata is not None and not isinstance(metadata, dict):
                errors.append(ValidationError(
                    code=ErrorCode.INVALID_DATA_TYPE,
                    field='metadata',
                    value=type(metadata).__name__,
                    message="Metadata must be a dictionary",
                    severity=ErrorSeverity.HIGH,
                    context={"expected_type": "dict", "actual_type": type(metadata).__name__},
                    timestamp=datetime.utcnow()
                ))

        return errors

    def _validate_business_rules(self, record: Dict[str, Any]) -> List[ValidationError]:
        """Validate business-specific rules."""
        warnings = []

        # Check title length
        titulo = record.get('titulo', '')
        if isinstance(titulo, str):
            if len(titulo) < 10:
                warnings.append(ValidationError(
                    code=ErrorCode.VALIDATION_FAILED,
                    field='titulo',
                    value=len(titulo),
                    message="Title is very short, may indicate poor data quality",
                    severity=ErrorSeverity.LOW,
                    context={"min_recommended_length": 10, "actual_length": len(titulo)},
                    timestamp=datetime.utcnow()
                ))
            elif len(titulo) > 500:
                warnings.append(ValidationError(
                    code=ErrorCode.VALIDATION_FAILED,
                    field='titulo',
                    value=len(titulo),
                    message="Title is very long, may indicate data issues",
                    severity=ErrorSeverity.LOW,
                    context={"max_recommended_length": 500, "actual_length": len(titulo)},
                    timestamp=datetime.utcnow()
                ))

        # Check if semantic text is meaningful
        texto_semantico = record.get('texto_semantico', '')
        if isinstance(texto_semantico, str) and len(texto_semantico.strip()) < 20:
            warnings.append(ValidationError(
                code=ErrorCode.VALIDATION_FAILED,
                field='texto_semantico',
                value=len(texto_semantico),
                message="Semantic text is too short for meaningful embeddings",
                severity=ErrorSeverity.MEDIUM,
                context={"min_recommended_length": 20, "actual_length": len(texto_semantico)},
                timestamp=datetime.utcnow()
            ))

        # Validate amount reasonableness
        valor_estimado = record.get('valor_estimado')
        if isinstance(valor_estimado, (int, float)):
            if valor_estimado <= 0:
                warnings.append(ValidationError(
                    code=ErrorCode.VALIDATION_FAILED,
                    field='valor_estimado',
                    value=valor_estimado,
                    message="Amount should be positive",
                    severity=ErrorSeverity.LOW,
                    context={"value": valor_estimado},
                    timestamp=datetime.utcnow()
                ))
            elif valor_estimado > 1e12:  # 1 trillion
                warnings.append(ValidationError(
                    code=ErrorCode.VALIDATION_FAILED,
                    field='valor_estimado',
                    value=valor_estimado,
                    message="Amount seems unreasonably large",
                    severity=ErrorSeverity.MEDIUM,
                    context={"value": valor_estimado, "max_reasonable": 1e12},
                    timestamp=datetime.utcnow()
                ))

        return warnings

    def _is_valid_date_string(self, date_str: str) -> bool:
        """Check if string is a valid date."""
        import re
        from datetime import datetime

        date_patterns = [
            r'^\d{4}-\d{2}-\d{2}$',  # YYYY-MM-DD
            r'^\d{2}/\d{2}/\d{4}$',  # DD/MM/YYYY
            r'^\d{2}-\d{2}-\d{4}$',  # DD-MM-YYYY
        ]

        for pattern in date_patterns:
            if re.match(pattern, date_str):
                try:
                    # Try to parse to ensure it's a real date
                    if '-' in date_str and len(date_str.split('-')[0]) == 4:
                        datetime.strptime(date_str, '%Y-%m-%d')
                    elif '/' in date_str:
                        datetime.strptime(date_str, '%d/%m/%Y')
                    elif '-' in date_str:
                        datetime.strptime(date_str, '%d-%m-%Y')
                    return True
                except ValueError:
                    continue

        return False

    def _is_valid_numeric_string(self, value_str: str) -> bool:
        """Check if string represents a valid number."""
        import re

        # Remove common currency symbols and whitespace
        cleaned = re.sub(r'[\$€£¥₹₽,\s]', '', value_str)

        try:
            float(cleaned)
            return True
        except ValueError:
            return False

    def _clean_and_normalize(self, record: Dict[str, Any],
                           errors: List[ValidationError]) -> Dict[str, Any]:
        """
        Clean and normalize record data to fix non-critical errors.

        Args:
            record: Original record
            errors: List of validation errors

        Returns:
            Cleaned record
        """
        cleaned_record = record.copy()

        # Fix string encoding issues
        string_fields = ['titulo', 'descripcion', 'entidad', 'texto_semantico']
        for field in string_fields:
            if field in cleaned_record and isinstance(cleaned_record[field], str):
                cleaned_record[field] = self._fix_encoding(cleaned_record[field])

        # Ensure required fields have default values if missing
        if not cleaned_record.get('tender_id'):
            cleaned_record['tender_id'] = f"auto_generated_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"

        if not cleaned_record.get('texto_semantico'):
            # Create from available fields
            parts = []
            if cleaned_record.get('titulo'):
                parts.append(f"Título: {cleaned_record['titulo']}")
            if cleaned_record.get('descripcion'):
                parts.append(f"Descripción: {cleaned_record['descripcion']}")
            if cleaned_record.get('entidad'):
                parts.append(f"Entidad: {cleaned_record['entidad']}")

            cleaned_record['texto_semantico'] = " | ".join(parts) if parts else "Información no disponible"

        # Ensure metadata has proper structure
        if not isinstance(cleaned_record.get('metadata'), dict):
            cleaned_record['metadata'] = {
                "fuente_original": cleaned_record.get('fuente', ''),
                "fecha_extraccion": datetime.utcnow().isoformat(),
                "parametros_busqueda": {},
                "datos_especificos": {},
                "calidad_datos": {
                    "completitud": 0.5,  # Default medium completeness
                    "confiabilidad": 0.5
                }
            }

        return cleaned_record

    def _fix_encoding(self, text: str) -> str:
        """Fix common encoding issues in text."""
        import html

        # Decode HTML entities
        fixed = html.unescape(text)

        # Fix common encoding issues
        replacements = {
            'Ã¡': 'á', 'Ã©': 'é', 'Ã­': 'í', 'Ã³': 'ó', 'Ãº': 'ú', 'Ã±': 'ñ',
            'â€œ': '"', 'â€': '"', 'â€™': "'", 'â€"': '-', 'Â': '',
        }

        for old, new in replacements.items():
            fixed = fixed.replace(old, new)

        return fixed.strip()


class ErrorRecoveryManager:
    """
    Manages error recovery strategies for different error types.
    """

    def __init__(self):
        """Initialize error recovery manager."""
        self.logger = get_logger("error_recovery")
        self.recovery_strategies = {
            ErrorCode.MISSING_REQUIRED_FIELD: self._recover_missing_field,
            ErrorCode.INVALID_DATE_FORMAT: self._recover_invalid_date,
            ErrorCode.INVALID_AMOUNT_FORMAT: self._recover_invalid_amount,
            ErrorCode.INVALID_DATA_TYPE: self._recover_invalid_type,
        }

    def attempt_recovery(self, record: Dict[str, Any],
                        error: ValidationError) -> Optional[Dict[str, Any]]:
        """
        Attempt to recover from a validation error.

        Args:
            record: Record with error
            error: Validation error to recover from

        Returns:
            Recovered record or None if recovery failed
        """
        if error.code in self.recovery_strategies:
            try:
                return self.recovery_strategies[error.code](record, error)
            except Exception as e:
                self.logger.warning(f"Recovery failed for {error.code.value}: {e}")
                return None
        return None

    def _recover_missing_field(self, record: Dict[str, Any],
                              error: ValidationError) -> Optional[Dict[str, Any]]:
        """Recover from missing required field."""
        recovered = record.copy()
        field = error.field

        if field == 'tender_id':
            # Generate ID from available data
            id_parts = [
                record.get('fuente', 'unknown'),
                record.get('titulo', '')[:20],
                str(hash(str(record)))[:8]
            ]
            recovered[field] = '_'.join(filter(None, id_parts))

        elif field == 'texto_semantico':
            # Create from available text fields
            parts = []
            for text_field in ['titulo', 'descripcion', 'entidad']:
                if record.get(text_field):
                    parts.append(str(record[text_field]))
            recovered[field] = ' | '.join(parts) if parts else "Sin información disponible"

        elif field in ['titulo', 'fuente']:
            # These are critical, cannot recover
            return None

        return recovered

    def _recover_invalid_date(self, record: Dict[str, Any],
                             error: ValidationError) -> Optional[Dict[str, Any]]:
        """Recover from invalid date format."""
        recovered = record.copy()
        field = error.field
        value = error.value

        if isinstance(value, str):
            # Try common date parsing
            import re
            from datetime import datetime

            # Extract numbers that might be date components
            numbers = re.findall(r'\d+', value)
            if len(numbers) >= 3:
                try:
                    # Try different interpretations
                    if len(numbers[0]) == 4:  # YYYY format
                        date_obj = datetime(int(numbers[0]), int(numbers[1]), int(numbers[2]))
                    else:  # DD/MM/YYYY format
                        date_obj = datetime(int(numbers[2]), int(numbers[1]), int(numbers[0]))

                    recovered[field] = date_obj.date()
                    return recovered
                except ValueError:
                    pass

        # If recovery fails, set to None
        recovered[field] = None
        return recovered

    def _recover_invalid_amount(self, record: Dict[str, Any],
                               error: ValidationError) -> Optional[Dict[str, Any]]:
        """Recover from invalid amount format."""
        recovered = record.copy()
        field = error.field
        value = error.value

        if isinstance(value, str):
            import re
            # Extract numeric part
            numeric_str = re.sub(r'[^\d.,]', '', value)

            if numeric_str:
                try:
                    # Handle decimal separators
                    if ',' in numeric_str and '.' in numeric_str:
                        # Assume last separator is decimal
                        if numeric_str.rfind(',') > numeric_str.rfind('.'):
                            numeric_str = numeric_str.replace('.', '').replace(',', '.')
                        else:
                            numeric_str = numeric_str.replace(',', '')
                    elif ',' in numeric_str:
                        # Might be decimal or thousands separator
                        if re.match(r'^\d+(,\d{2})$', numeric_str):
                            numeric_str = numeric_str.replace(',', '.')
                        else:
                            numeric_str = numeric_str.replace(',', '')

                    recovered[field] = float(numeric_str)
                    return recovered
                except ValueError:
                    pass

        # Set to None if cannot recover
        recovered[field] = None
        return recovered

    def _recover_invalid_type(self, record: Dict[str, Any],
                             error: ValidationError) -> Optional[Dict[str, Any]]:
        """Recover from invalid data type."""
        recovered = record.copy()
        field = error.field
        value = error.value

        # Try to convert to expected type
        try:
            if field in ['tender_id', 'fuente', 'titulo', 'descripcion', 'entidad', 'estado', 'ciudad']:
                # Convert to string
                recovered[field] = str(value) if value is not None else ""
                return recovered
        except Exception:
            pass

        return None