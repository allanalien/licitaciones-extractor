import os
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

class DatabaseConnection:
    """Conexión directa a PostgreSQL para la tabla licitaciones"""

    def __init__(self):
        self.database_url = os.getenv('DATABASE_URL')
        if not self.database_url:
            self.database_url = "postgresql://neondb_owner:npg_Tr1wXonS8EZy@ep-fragrant-feather-age2rjov-pooler.c-2.eu-central-1.aws.neon.tech/neondb?sslmode=require"

    def get_connection(self):
        """Obtener conexión a la base de datos"""
        return psycopg2.connect(self.database_url)

    def save_licitacion(self, licitacion_data):
        """Guardar una licitación en la tabla licitaciones"""
        conn = None
        cursor = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Insertar en la tabla licitaciones
            query = """
                INSERT INTO licitaciones (
                    titulo, descripcion, fecha_publicacion, fecha_limite,
                    dependencia, url, fuente, monto, moneda, categoria,
                    ubicacion, contacto, requisitos, estado, numero_licitacion
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (url) DO UPDATE SET
                    titulo = EXCLUDED.titulo,
                    descripcion = EXCLUDED.descripcion,
                    fecha_publicacion = EXCLUDED.fecha_publicacion,
                    fecha_limite = EXCLUDED.fecha_limite,
                    updated_at = CURRENT_TIMESTAMP
                RETURNING id;
            """

            values = (
                licitacion_data.get('titulo', ''),
                licitacion_data.get('descripcion', ''),
                licitacion_data.get('fecha_publicacion'),
                licitacion_data.get('fecha_limite'),
                licitacion_data.get('dependencia', ''),
                licitacion_data.get('url', ''),
                licitacion_data.get('fuente', ''),
                licitacion_data.get('monto'),
                licitacion_data.get('moneda'),
                licitacion_data.get('categoria'),
                licitacion_data.get('ubicacion'),
                licitacion_data.get('contacto'),
                licitacion_data.get('requisitos'),
                licitacion_data.get('estado'),
                licitacion_data.get('numero_licitacion')
            )

            cursor.execute(query, values)
            result = cursor.fetchone()
            conn.commit()

            if result:
                logger.info(f"Licitación guardada con ID: {result[0]} - {licitacion_data.get('titulo', '')[:50]}")
                return result[0]
            return None

        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Error guardando licitación: {e}")
            return None
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def get_licitaciones_count(self):
        """Obtener el número total de licitaciones"""
        conn = None
        cursor = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM licitaciones")
            count = cursor.fetchone()[0]
            return count
        except Exception as e:
            logger.error(f"Error obteniendo conteo: {e}")
            return 0
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def search_licitaciones(self, keyword, limit=10):
        """Buscar licitaciones por palabra clave"""
        conn = None
        cursor = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)

            query = """
                SELECT * FROM licitaciones
                WHERE titulo ILIKE %s OR descripcion ILIKE %s
                ORDER BY fecha_publicacion DESC
                LIMIT %s
            """

            search_term = f"%{keyword}%"
            cursor.execute(query, (search_term, search_term, limit))
            results = cursor.fetchall()
            return results

        except Exception as e:
            logger.error(f"Error buscando licitaciones: {e}")
            return []
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()