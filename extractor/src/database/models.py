from sqlalchemy import create_engine, Column, Integer, String, DateTime, Text, Float, Boolean, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import ARRAY
from pgvector.sqlalchemy import Vector
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

load_dotenv()

Base = declarative_base()

class Update(Base):
    __tablename__ = 'updates'

    id = Column(Integer, primary_key=True, autoincrement=True)
    licitacion_id = Column(String, nullable=True)  # ID original de la licitación
    metadata_json = Column('metadata', JSON, nullable=False)  # JSON con todos los metadatos estructurados
    texto_semantico = Column(Text, nullable=False)  # Texto descriptivo para RAG

    # Vector embedding usando pgvector
    embedding = Column(Vector(1536))  # Vector de 1536 dimensiones (compatible con OpenAI)
    vector_procesado = Column(Boolean, default=False)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def to_dict(self):
        return {
            'id': self.id,
            'licitacion_id': self.licitacion_id,
            'metadata': self.metadata_json,
            'texto_semantico': self.texto_semantico,
            'embedding': self.embedding,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }

class DatabaseManager:
    def __init__(self):
        # Usar la URL de PostgreSQL proporcionada
        self.database_url = "postgresql://neondb_owner:npg_Tr1wXonS8EZy@ep-fragrant-feather-age2rjov-pooler.c-2.eu-central-1.aws.neon.tech/neondb?sslmode=require"
        self.engine = create_engine(self.database_url)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)

    def create_tables(self):
        """Crear todas las tablas"""
        Base.metadata.create_all(bind=self.engine)

    def get_session(self):
        """Obtener sesión de base de datos"""
        return self.SessionLocal()

    def save_update(self, metadata, texto_semantico, fuente):
        """Guardar update en la base de datos"""
        session = self.get_session()
        try:
            # Verificar si ya existe basado en ID de licitación en metadata
            licitacion_id = metadata.get('id')
            if licitacion_id:
                from sqlalchemy import text
                existing = session.query(Update).filter(
                    text("metadata->>'id' = :id")
                ).params(id=licitacion_id).first()
            else:
                existing = None

            if not existing:
                update = Update(
                    licitacion_id=licitacion_id,
                    metadata_json=metadata,
                    texto_semantico=texto_semantico
                )
                session.add(update)
                session.commit()
                return update.id
            else:
                # Actualizar si hay cambios
                existing.metadata_json = metadata
                existing.texto_semantico = texto_semantico
                existing.updated_at = datetime.utcnow()
                session.commit()
                return existing.id

        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

    def get_unprocessed_updates(self):
        """Obtener updates no procesadas para vectorización"""
        session = self.get_session()
        try:
            return session.query(Update).filter(Update.embedding == None).all()
        finally:
            session.close()

    def update_embedding(self, update_id, embedding_vector):
        """Actualizar embedding de un update"""
        session = self.get_session()
        try:
            update = session.query(Update).filter_by(id=update_id).first()
            if update:
                update.embedding = embedding_vector
                update.updated_at = datetime.utcnow()
                session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

    def get_yesterday_date_range(self):
        """Obtener rango de fechas del día anterior"""
        yesterday = datetime.now() - timedelta(days=1)
        start_date = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)
        return start_date, end_date