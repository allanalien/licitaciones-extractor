import requests
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any
from loguru import logger
import time
import os
from ..database.models import DatabaseManager
from ..utils.vector_manager import VectorManager

class TianguisDigitalExtractor:
    def __init__(self):
        self.base_url = "https://datosabiertostianguisdigital.cdmx.gob.mx/api/v1"
        self.contrataciones_api = "http://www.contratosabiertos.cdmx.gob.mx/api"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': os.getenv('USER_AGENT', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'),
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        })
        self.db_manager = DatabaseManager()
        self.vector_manager = VectorManager()
        self.max_retries = int(os.getenv('MAX_RETRIES', 3))
        self.request_delay = int(os.getenv('REQUEST_DELAY', 2))

    def get_yesterday_date_range(self) -> tuple:
        """Obtener rango de fechas del día anterior"""
        yesterday = datetime.now() - timedelta(days=1)
        start_date = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)
        return start_date, end_date

    def make_request(self, url: str, params: dict = None) -> dict:
        """Realizar petición HTTP con reintentos"""
        for attempt in range(self.max_retries):
            try:
                time.sleep(self.request_delay)
                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                logger.warning(f"Intento {attempt + 1} fallido para {url}: {e}")
                if attempt == self.max_retries - 1:
                    logger.error(f"Falló después de {self.max_retries} intentos: {url}")
                    raise e
                time.sleep(5 * (attempt + 1))  # Backoff exponencial

    def search_licitaciones(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Buscar licitaciones en Tianguis Digital usando la API real"""
        try:
            # Usar el endpoint real que funciona
            search_url = f"{self.base_url}/plannings"

            params = {
                'hiring_method': '1,2,3',  # Tipos de contratación (1=Licitación, 2=Invitación, 3=Adjudicación)
                'consolidated': 'FALSE',
                'start_date': start_date.strftime('%d/%m/%Y'),
                'end_date': end_date.strftime('%d/%m/%Y')
            }

            logger.info(f"Consultando Tianguis Digital API...")
            logger.info(f"URL: {search_url}")
            logger.info(f"Parámetros: {params}")

            response_data = self.make_request(search_url, params)

            all_licitaciones = []

            # La API devuelve una lista de objetos con estructura OCDS
            if isinstance(response_data, list):
                all_licitaciones = response_data
            elif response_data.get('releases'):
                all_licitaciones = response_data['releases']
            elif response_data.get('data'):
                all_licitaciones = response_data['data']
            else:
                logger.warning("Respuesta de API no reconocida")
                return []

            logger.info(f"Encontradas {len(all_licitaciones)} licitaciones en Tianguis Digital")
            return all_licitaciones

        except Exception as e:
            logger.error(f"Error buscando en Tianguis Digital: {e}")
            return []

    def get_licitacion_details(self, licitacion_id: str) -> Dict:
        """Obtener detalles completos de una licitación"""
        try:
            detail_url = f"{self.base_url}/licitaciones/{licitacion_id}"
            return self.make_request(detail_url)
        except Exception as e:
            logger.error(f"Error obteniendo detalles de licitación {licitacion_id}: {e}")
            return {}

    def create_metadata(self, raw_data: Dict) -> Dict:
        """Crear metadata siguiendo el formato requerido"""
        try:
            # Extraer información básica
            ocid = raw_data.get('ocid', raw_data.get('id', ''))

            # Título
            titulo = ''
            if raw_data.get('planning', {}).get('budget', {}).get('project'):
                titulo = raw_data['planning']['budget']['project']
            elif raw_data.get('tender', {}).get('items', {}).get('description'):
                if isinstance(raw_data['tender']['items']['description'], list):
                    titulo = raw_data['tender']['items']['description'][0] if raw_data['tender']['items']['description'] else ''
                else:
                    titulo = raw_data['tender']['items']['description']

            # Entidad
            entidad = ''
            if raw_data.get('buyer', {}).get('name'):
                entidad = raw_data['buyer']['name']
            elif raw_data.get('buyer', {}).get('parties', {}).get('name'):
                entidad = raw_data['buyer']['parties']['name']

            # Monto
            monto = 0.0
            if raw_data.get('planning', {}).get('budget', {}).get('amount', {}).get('amount'):
                try:
                    monto = float(raw_data['planning']['budget']['amount']['amount'])
                except (ValueError, TypeError):
                    monto = 0.0

            # Fecha de publicación
            fecha_publicacion = None
            if raw_data.get('date'):
                try:
                    fecha_str = raw_data['date']
                    if '-' in fecha_str and len(fecha_str) == 10:
                        fecha_publicacion = datetime.strptime(fecha_str, '%d-%m-%Y').strftime('%Y-%m-%d')
                except (ValueError, TypeError):
                    pass

            # Tipo de procedimiento
            tipo_procedimiento = raw_data.get('tender', {}).get('procurementMethodRationale', 'NO ESPECIFICADO')

            # Estado
            estado = raw_data.get('tender', {}).get('status', 'PUBLICADO')

            # URL
            url_anuncio = raw_data.get('url', f"https://datosabiertostianguisdigital.cdmx.gob.mx/contrataciones-abiertas")

            # Crear metadata similar al ejemplo
            metadata = {
                "id": ocid,
                "institucion": entidad,
                "titulo": titulo if titulo else "Sin título",
                "tipo_procedimiento": tipo_procedimiento,
                "importe_drc": monto,
                "monto_sin_imp__maximo": monto * 1.16 if monto > 0 else 0,  # Estimado con IVA
                "monto_sin_imp__minimo": monto,
                "fecha_de_publicacion": fecha_publicacion,
                "fecha_de_apertura": "no especificado",
                "fecha_de_fallo": "no especificado",
                "estatus_drc": estado,
                "estatus_contrato": "PUBLICADO",
                "url_anuncio": url_anuncio,
                "fuente": "tianguis_digital",
                "rfc": "no especificado",
                "proveedor": "no especificado"
            }

            return metadata

        except Exception as e:
            logger.error(f"Error creando metadata: {e}")
            return {}

    def create_texto_semantico(self, raw_data: Dict, metadata: Dict) -> str:
        """Crear texto semántico siguiendo el formato del ejemplo"""
        try:
            # Extraer título y descripción
            titulo = metadata.get('titulo', 'Sin título')
            institucion = metadata.get('institucion', 'Sin institución')
            tipo_procedimiento = metadata.get('tipo_procedimiento', 'NO ESPECIFICADO')
            monto = metadata.get('importe_drc', 0)
            fecha_publicacion = metadata.get('fecha_de_publicacion', 'no especificado')
            estado = metadata.get('estatus_drc', 'PUBLICADO')
            url = metadata.get('url_anuncio', '')
            id_licitacion = metadata.get('id', '')

            # Descripción detallada
            descripcion_parts = []
            if raw_data.get('tender', {}).get('items', {}).get('description'):
                item_desc = raw_data['tender']['items']['description']
                if isinstance(item_desc, list):
                    descripcion_parts.extend([desc for desc in item_desc if desc])
                elif item_desc:
                    descripcion_parts.append(item_desc)

            if raw_data.get('tender', {}).get('procurementMethodRationale'):
                descripcion_parts.append(f"Método: {raw_data['tender']['procurementMethodRationale']}")

            descripcion = '. '.join(descripcion_parts) if descripcion_parts else 'Sin descripción detallada'

            # Formatear monto
            monto_str = f"${monto:,.2f} MXN" if monto > 0 else "Monto no especificado"

            # Crear texto semántico
            texto_semantico = f"""Licitación {id_licitacion}: {titulo.upper()}. Institución: {institucion}. Tipo de procedimiento: {tipo_procedimiento}. Monto estimado: {monto_str}. Fecha de publicación: {fecha_publicacion}. Descripción: {descripcion}. Estado: {estado}. URL: {url}."""

            return texto_semantico

        except Exception as e:
            logger.error(f"Error creando texto semántico: {e}")
            return f"Error procesando licitación: {str(e)}"

    def classify_sector(self, text: str) -> str:
        """Clasificar sector basado en el contenido"""
        text_lower = text.lower()

        sectores = {
            'construccion': ['construcción', 'obra', 'infraestructura', 'edificio', 'carretera'],
            'tecnologia': ['tecnología', 'software', 'sistema', 'informático', 'digital'],
            'salud': ['salud', 'médico', 'hospital', 'clínica', 'medicamento'],
            'educacion': ['educación', 'escuela', 'universidad', 'capacitación'],
            'servicios': ['servicios', 'consultoría', 'asesoría', 'mantenimiento'],
            'seguridad': ['seguridad', 'vigilancia', 'protección'],
            'transporte': ['transporte', 'vehículo', 'autobús', 'logística']
        }

        for sector, keywords in sectores.items():
            if any(keyword in text_lower for keyword in keywords):
                return sector

        return 'otros'

    def extract_keywords(self, text: str) -> List[str]:
        """Extraer palabras clave relevantes"""
        # Lista básica de palabras clave importantes
        important_words = ['construcción', 'tecnología', 'servicios', 'consultoría', 'infraestructura',
                          'mantenimiento', 'adquisición', 'suministro', 'desarrollo', 'implementación']

        text_lower = text.lower()
        keywords = [word for word in important_words if word in text_lower]

        return keywords

    def process_licitaciones(self, licitaciones: List[Dict]) -> int:
        """Procesar y almacenar licitaciones"""
        processed_count = 0

        for licitacion in licitaciones:
            try:
                # Crear metadata y texto semántico
                metadata = self.create_metadata(licitacion)
                if not metadata or not metadata.get('titulo'):
                    logger.warning("Datos insuficientes para procesar licitación")
                    continue

                texto_semantico = self.create_texto_semantico(licitacion, metadata)

                # Guardar en base de datos usando nueva estructura
                update_id = self.db_manager.save_update(
                    metadata=metadata,
                    texto_semantico=texto_semantico,
                    fuente='tianguis_digital'
                )

                # Generar y almacenar embedding en ChromaDB y PostgreSQL
                embedding = self.vector_manager.store_in_vector_db({
                    'id': metadata.get('id'),
                    'titulo': metadata.get('titulo'),
                    'texto_semantico': texto_semantico,
                    'fuente': 'tianguis_digital'
                })

                # Almacenar embedding en PostgreSQL con pgvector
                if embedding and update_id:
                    self.db_manager.update_embedding(update_id, embedding)

                processed_count += 1
                logger.info(f"Licitación procesada: {metadata.get('titulo', 'Sin título')[:50]}...")

            except Exception as e:
                logger.error(f"Error procesando licitación: {e}")
                continue

        return processed_count

    def extract_yesterday_data(self) -> Dict[str, Any]:
        """Extraer datos del día anterior"""
        logger.info("Iniciando extracción de Tianguis Digital...")

        try:
            # Obtener rango de fechas
            start_date, end_date = self.get_yesterday_date_range()
            logger.info(f"Buscando licitaciones desde {start_date} hasta {end_date}")

            # Buscar licitaciones
            licitaciones = self.search_licitaciones(start_date, end_date)

            if not licitaciones:
                logger.info("No se encontraron licitaciones en el rango de fechas")
                return {
                    'status': 'success',
                    'source': 'tianguis_digital',
                    'total_found': 0,
                    'total_processed': 0,
                    'message': 'No hay nuevas licitaciones'
                }

            # Procesar licitaciones
            processed_count = self.process_licitaciones(licitaciones)

            result = {
                'status': 'success',
                'source': 'tianguis_digital',
                'total_found': len(licitaciones),
                'total_processed': processed_count,
                'date_range': f"{start_date.date()} - {end_date.date()}"
            }

            logger.info(f"Extracción completada: {processed_count}/{len(licitaciones)} procesadas")
            return result

        except Exception as e:
            logger.error(f"Error en extracción de Tianguis Digital: {e}")
            return {
                'status': 'error',
                'source': 'tianguis_digital',
                'error': str(e)
            }