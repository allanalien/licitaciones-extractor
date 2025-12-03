"""
Database models for licitaciones extractor.
"""

from sqlalchemy import Column, String, Text, TIMESTAMP, DATE, DECIMAL, Boolean, Integer, Index
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.sql import func
from datetime import datetime, date
from typing import Dict, Any, Optional, List
import uuid

from src.database.connection import Base

class Update(Base):
    """Model for the updates table storing tender information."""

    __tablename__ = 'updates'

    # Primary key
    id = Column(Integer, primary_key=True, autoincrement=True)

    # Unique tender identifier
    tender_id = Column(String(255), unique=True, nullable=False, index=True)

    # Source information
    fuente = Column(String(50), nullable=False, index=True)

    # Timestamps
    fecha_extraccion = Column(TIMESTAMP, default=func.current_timestamp(), nullable=False, index=True)
    fecha_catalogacion = Column(DATE, nullable=True)
    fecha_apertura = Column(DATE, nullable=True)

    # Tender content
    titulo = Column(Text, nullable=True)
    descripcion = Column(Text, nullable=True)
    texto_semantico = Column(Text, nullable=False)

    # Structured data - renamed from metadata to avoid SQLAlchemy conflict
    meta_data = Column(JSONB, nullable=False, default={})

    # Vector embeddings (using pgvector extension)
    # Note: Requires pgvector extension to be installed in PostgreSQL
    # embeddings = Column(Vector(1536), nullable=True)
    # For now, we'll store as JSONB array until pgvector is set up
    embeddings = Column(JSONB, nullable=True)

    # Geographic and entity information
    entidad = Column(String(255), nullable=True)
    estado = Column(String(100), nullable=True)
    ciudad = Column(String(100), nullable=True)

    # Financial information
    valor_estimado = Column(DECIMAL(15, 2), nullable=True)

    # Tender type
    tipo_licitacion = Column(String(100), nullable=True)

    # Source URL
    url_original = Column(Text, nullable=True)

    # Processing status
    procesado = Column(Boolean, default=False, nullable=False)

    # Additional indexes
    __table_args__ = (
        Index('idx_fecha_extraccion', 'fecha_extraccion'),
        Index('idx_fuente', 'fuente'),
        Index('idx_tender_id', 'tender_id'),
        Index('idx_procesado', 'procesado'),
        Index('idx_entidad', 'entidad'),
        Index('idx_estado', 'estado'),
        Index('idx_fecha_catalogacion', 'fecha_catalogacion'),
    )

    def __init__(self, **kwargs):
        """Initialize Update model with default metadata structure."""
        # Set default meta_data if not provided
        if 'meta_data' not in kwargs or not kwargs['meta_data']:
            kwargs['meta_data'] = self._get_default_metadata()

        super().__init__(**kwargs)

    def _get_default_metadata(self) -> Dict[str, Any]:
        """Get default metadata structure based on ComprasMX format."""
        return {
            "fuente_original": self.fuente if hasattr(self, 'fuente') else "",
            "fecha_extraccion": datetime.now().isoformat(),
            "parametros_busqueda": {},
            "datos_especificos": {
                "id": "",
                "rfc": "",
                "proveedor": "",
                "estatus_drc": "",
                "importe_drc": 0,
                "institucion": "",
                "url_anuncio": "",
                "fecha_de_fallo": "",
                "estatus_contrato": "no especificado",
                "fecha_de_apertura": "no especificado",
                "tipo_procedimiento": "",
                "fecha_de_publicacion": "",
                "monto_sin_imp__maximo": 0,
                "monto_sin_imp__minimo": 0
            },
            "calidad_datos": {
                "completitud": 0.0,
                "confiabilidad": 0.0
            }
        }

    def set_embeddings(self, embeddings_vector: list):
        """
        Set embeddings vector.

        Args:
            embeddings_vector: List of float values representing the embedding
        """
        self.embeddings = embeddings_vector

    def get_embeddings(self) -> Optional[list]:
        """
        Get embeddings vector.

        Returns:
            List of float values or None if not set
        """
        return self.embeddings if self.embeddings else None

    def set_metadata_field(self, field_path: str, value: Any):
        """
        Set a specific field in metadata using dot notation.

        Args:
            field_path: Dot-separated path (e.g., "datos_especificos.licita_ya.smart_search")
            value: Value to set
        """
        if not self.meta_data:
            self.meta_data = self._get_default_metadata()

        # Navigate to the correct nested dictionary
        keys = field_path.split('.')
        current = self.meta_data
        for key in keys[:-1]:
            if not isinstance(current, dict):
                current = {}
            if key not in current:
                current[key] = {}
            current = current[key]

        # Set the final value
        current[keys[-1]] = value

    def get_metadata_field(self, field_path: str, default: Any = None) -> Any:
        """
        Get a specific field from metadata using dot notation.

        Args:
            field_path: Dot-separated path
            default: Default value if field doesn't exist

        Returns:
            Field value or default
        """
        if not self.meta_data:
            return default

        keys = field_path.split('.')
        current = self.meta_data
        try:
            for key in keys:
                current = current[key]
            return current
        except (KeyError, TypeError):
            return default

    def calculate_completeness(self) -> float:
        """
        Calculate data completeness score (0.0 to 1.0).

        Returns:
            Completeness score
        """
        required_fields = [
            self.tender_id,
            self.fuente,
            self.titulo,
            self.texto_semantico
        ]

        optional_fields = [
            self.descripcion,
            self.entidad,
            self.estado,
            self.ciudad,
            self.fecha_catalogacion,
            self.fecha_apertura,
            self.valor_estimado,
            self.tipo_licitacion,
            self.url_original
        ]

        # Required fields must be present
        if not all(field is not None and str(field).strip() for field in required_fields):
            return 0.0

        # Calculate based on optional fields present
        filled_optional = sum(1 for field in optional_fields if field is not None and str(field).strip())
        completeness = filled_optional / len(optional_fields)

        # Update metadata
        self.set_metadata_field("calidad_datos.completitud", completeness)

        return completeness

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert model to dictionary.

        Returns:
            Dictionary representation of the model
        """
        return {
            'id': self.id,
            'tender_id': self.tender_id,
            'fuente': self.fuente,
            'fecha_extraccion': self.fecha_extraccion.isoformat() if self.fecha_extraccion else None,
            'fecha_catalogacion': self.fecha_catalogacion.isoformat() if self.fecha_catalogacion else None,
            'fecha_apertura': self.fecha_apertura.isoformat() if self.fecha_apertura else None,
            'titulo': self.titulo,
            'descripcion': self.descripcion,
            'texto_semantico': self.texto_semantico,
            'metadata': self.meta_data,
            'embeddings': self.embeddings,
            'entidad': self.entidad,
            'estado': self.estado,
            'ciudad': self.ciudad,
            'valor_estimado': float(self.valor_estimado) if self.valor_estimado else None,
            'tipo_licitacion': self.tipo_licitacion,
            'url_original': self.url_original,
            'procesado': self.procesado
        }

    def __repr__(self):
        return f"<Update(tender_id='{self.tender_id}', fuente='{self.fuente}', titulo='{self.titulo[:50]}...')>"