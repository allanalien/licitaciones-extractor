import requests
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any
from loguru import logger
import time
import os
from ..database.models import DatabaseManager
from ..utils.vector_manager import VectorManager

class LicitaYaExtractor:
    def __init__(self):
        self.base_url = "https://www.licitaya.com.mx/api/v1"
        self.api_key = os.getenv('LICITA_YA_API_KEY')
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': os.getenv('USER_AGENT', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'),
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        })

        # Keywords para búsqueda (agregamos "alimentos" como keyword importante)
        keywords_env = os.getenv('LICITA_YA_KEYWORDS', 'construcción,infraestructura,tecnología,servicios,consultoría,alimentos,obra,adquisición,suministro')
        self.search_keywords = [k.strip() for k in keywords_env.split(',')]

        # Límite de licitaciones a procesar para evitar timeouts
        self.max_items = int(os.getenv('LICITA_YA_MAX_ITEMS', 20))

        self.db_manager = DatabaseManager()
        self.vector_manager = VectorManager()
        self.max_retries = int(os.getenv('MAX_RETRIES', 3))
        self.request_delay = int(os.getenv('REQUEST_DELAY', 2))

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
                time.sleep(5 * (attempt + 1))

    def search_by_keyword(self, keyword: str, target_date: datetime) -> List[Dict]:
        """Buscar licitaciones por palabra clave en Licita Ya usando el endpoint correcto"""
        try:
            # Endpoint correcto según documentación oficial
            search_url = f"{self.base_url}/tender/search"

            # Parámetros según documentación oficial de Licita Ya
            params = {
                'api_key': self.api_key,
                'keyword': keyword,
                'date': target_date.strftime('%Y%m%d'),  # Formato YYYYMMDD
                'page': 1
            }

            all_results = []
            page = 1

            while True:
                params['page'] = page
                logger.info(f"Buscando '{keyword}' página {page} en Licita Ya...")

                try:
                    response_data = self.make_request(search_url, params)

                    # Manejar diferentes formatos de respuesta
                    if isinstance(response_data, list):
                        results = response_data
                    elif response_data.get('licitaciones'):
                        results = response_data['licitaciones']
                    elif response_data.get('data'):
                        results = response_data['data']
                    elif response_data.get('results'):
                        results = response_data['results']
                    else:
                        logger.warning(f"Formato de respuesta no reconocido para '{keyword}'")
                        break

                    if not results:
                        break

                    all_results.extend(results)

                    # Si obtuvo menos de 50 registros (límite aproximado por página), es la última página
                    if len(results) < 50:
                        break

                    page += 1

                    # Límite de seguridad para evitar bucles infinitos
                    if page > 10:
                        break

                except requests.exceptions.HTTPError as e:
                    if e.response.status_code == 404:
                        logger.info(f"No se encontraron resultados para '{keyword}'")
                        break
                    elif e.response.status_code == 403:
                        logger.error(f"Error de autenticación API key para '{keyword}' - Verificar IP única por día")
                        break
                    elif e.response.status_code == 401:
                        logger.error(f"API key inválida o límite diario alcanzado para '{keyword}'")
                        break
                    raise e

            logger.info(f"Encontradas {len(all_results)} licitaciones para '{keyword}'")
            return all_results

        except Exception as e:
            logger.error(f"Error buscando '{keyword}' en Licita Ya: {e}")
            return []

    def search_combined_keywords(self, target_date: datetime) -> List[Dict]:
        """Buscar licitaciones usando keywords prioritarias para optimizar consultas API"""
        try:
            # Priorizar keywords más importantes y limitar consultas
            priority_keywords = ['alimentos', 'medicinas', 'equipo', 'obra', 'construcción', 'tecnología', 'servicios']

            # Usar solo keywords prioritarias si hay muchas configuradas
            if len(self.search_keywords) > 6:
                keywords_to_use = [kw for kw in self.search_keywords if kw in priority_keywords][:6]
                logger.info(f"Usando keywords prioritarias para optimizar API: {keywords_to_use}")
            else:
                keywords_to_use = self.search_keywords[:6]  # Máximo 6 keywords

            all_licitaciones = []
            seen_ids = set()

            # Hacer consultas individuales pero limitadas
            for keyword in keywords_to_use:
                try:
                    logger.info(f"Buscando con keyword: '{keyword}'")
                    results = self.search_by_keyword(keyword, target_date)

                    # Agregar resultados únicos
                    for licitacion in results:
                        licitacion_id = (licitacion.get('id') or
                                       licitacion.get('tender_id') or
                                       licitacion.get('url') or
                                       f"licita_ya_{hash(str(licitacion))}")

                        if licitacion_id and licitacion_id not in seen_ids:
                            seen_ids.add(licitacion_id)
                            licitacion['search_keyword'] = keyword
                            licitacion['unique_id'] = licitacion_id
                            all_licitaciones.append(licitacion)

                    # Pausa entre keywords para no saturar la API
                    time.sleep(1)

                    # Si ya tenemos suficientes licitaciones, no hacer más consultas
                    if len(all_licitaciones) >= self.max_items:
                        break

                except Exception as e:
                    logger.warning(f"Error con keyword '{keyword}': {e}")
                    continue

            logger.info(f"Total de licitaciones únicas encontradas: {len(all_licitaciones)}")

            # Limitar el número de licitaciones a procesar
            if len(all_licitaciones) > self.max_items:
                logger.info(f"Limitando procesamiento a {self.max_items} licitaciones de {len(all_licitaciones)} encontradas")
                all_licitaciones = all_licitaciones[:self.max_items]

            return all_licitaciones

        except Exception as e:
            logger.error(f"Error en búsqueda de keywords: {e}")
            return []

    def search_all_keywords(self, target_date: datetime) -> List[Dict]:
        """Wrapper para mantener compatibilidad - ahora usa búsqueda optimizada"""
        return self.search_combined_keywords(target_date)

    def get_tender_details(self, tender_id: str) -> Dict:
        """Obtener detalles completos de una licitación"""
        try:
            detail_url = f"{self.base_url}/tenders/{tender_id}"
            return self.make_request(detail_url)
        except Exception as e:
            logger.error(f"Error obteniendo detalles de tender {tender_id}: {e}")
            return {}

    def enrich_with_web_search(self, licitacion_data: Dict) -> Dict:
        """Enriquecer datos con búsqueda web (búsqueda básica simulada)"""
        try:
            # Buscar información adicional sobre la entidad
            entidad = licitacion_data.get('entidad', '')
            titulo = licitacion_data.get('titulo', '')

            # Simular enriquecimiento de datos
            # En una implementación real, aquí se haría web scraping o llamadas a APIs adicionales
            enriched_data = licitacion_data.copy()

            # Agregar información adicional basada en patrones conocidos
            if 'gobierno' in entidad.lower() or 'municipal' in entidad.lower():
                enriched_data['tipo_entidad'] = 'gobierno_municipal'
            elif 'estado' in entidad.lower() or 'estatal' in entidad.lower():
                enriched_data['tipo_entidad'] = 'gobierno_estatal'
            elif 'federal' in entidad.lower() or 'nacional' in entidad.lower():
                enriched_data['tipo_entidad'] = 'gobierno_federal'
            else:
                enriched_data['tipo_entidad'] = 'privado'

            # Clasificación de urgencia basada en fechas
            if enriched_data.get('fecha_cierre'):
                try:
                    fecha_cierre = enriched_data['fecha_cierre']
                    if isinstance(fecha_cierre, str):
                        fecha_cierre = datetime.strptime(fecha_cierre, '%Y-%m-%d %H:%M:%S')

                    days_until_close = (fecha_cierre - datetime.now()).days

                    if days_until_close <= 7:
                        enriched_data['urgencia'] = 'alta'
                    elif days_until_close <= 30:
                        enriched_data['urgencia'] = 'media'
                    else:
                        enriched_data['urgencia'] = 'baja'

                except Exception:
                    enriched_data['urgencia'] = 'desconocida'

            return enriched_data

        except Exception as e:
            logger.error(f"Error enriqueciendo datos: {e}")
            return licitacion_data

    def normalize_licitacion_data(self, raw_data: Dict) -> Dict:
        """Normalizar datos de licitación al formato estándar para LicitaYa"""
        try:
            normalized = {
                'titulo': (raw_data.get('tender_object', '') or
                          raw_data.get('title', '') or
                          raw_data.get('titulo', '')).strip(),
                'descripcion': (raw_data.get('description', '') or
                              raw_data.get('descripcion', '') or
                              raw_data.get('tender_object', '')).strip(),
                'entidad': (raw_data.get('agency', '') or
                           raw_data.get('entity', '') or
                           raw_data.get('buyer', {}).get('name', '')).strip(),
                'url_original': raw_data.get('url', ''),
                'fuente': 'licita_ya',
                'pais': raw_data.get('country', ''),
                'estado': raw_data.get('state', ''),
                'ciudad': raw_data.get('city', ''),
                'codigo_postal': raw_data.get('zip', ''),
                'ciudad': raw_data.get('city', ''),
                'codigo_postal': raw_data.get('zip', ''),
                'unique_id': raw_data.get('unique_id') or raw_data.get('url') or f"licita_ya_{hash(str(raw_data))}",
                'estado_geo': raw_data.get('state', '') # Preservar estado geográfico separado del estatus
            }

            # Procesar monto
            if raw_data.get('value') or raw_data.get('monto'):
                try:
                    monto_value = raw_data.get('value', {}) if isinstance(raw_data.get('value'), dict) else raw_data.get('monto', 0)
                    if isinstance(monto_value, dict):
                        normalized['monto'] = float(monto_value.get('amount', 0))
                        normalized['moneda'] = monto_value.get('currency', 'USD')
                    else:
                        normalized['monto'] = float(monto_value)
                        normalized['moneda'] = raw_data.get('currency', 'USD')
                except (ValueError, TypeError):
                    normalized['monto'] = 0.0
                    normalized['moneda'] = 'USD'
            else:
                normalized['monto'] = 0.0
                normalized['moneda'] = 'USD'

            # Procesar fechas
            date_mapping = {
                'fecha_publicacion': ['published_date', 'publication_date', 'fecha_publicacion'],
                'fecha_apertura': ['opening_date', 'submission_start', 'fecha_apertura'],
                'fecha_cierre': ['closing_date', 'deadline', 'submission_end', 'fecha_cierre']
            }

            for norm_field, possible_fields in date_mapping.items():
                date_value = None
                for field in possible_fields:
                    if raw_data.get(field):
                        date_value = raw_data[field]
                        break

                if date_value:
                    try:
                        if isinstance(date_value, str):
                            # Intentar varios formatos de fecha
                            formats = ['%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d', '%d/%m/%Y']
                            for fmt in formats:
                                try:
                                    normalized[norm_field] = datetime.strptime(date_value, fmt)
                                    break
                                except ValueError:
                                    continue
                        elif isinstance(date_value, datetime):
                            normalized[norm_field] = date_value
                    except Exception:
                        normalized[norm_field] = None
                else:
                    normalized[norm_field] = None

            # Estado
            status_mapping = {
                'active': 'activo',
                'open': 'activo',
                'closed': 'cerrado',
                'cancelled': 'cancelado',
                'awarded': 'adjudicado'
            }
            status = raw_data.get('status', 'active')
            normalized['estado'] = status_mapping.get(status.lower(), status)

            # Categorización
            normalized['sector'] = self.classify_sector(normalized['titulo'] + ' ' + normalized.get('descripcion', ''))
            normalized['categoria'] = raw_data.get('category', raw_data.get('cpv_code', ''))

            # Palabras clave incluyendo la keyword de búsqueda
            keywords = self.extract_keywords(normalized['titulo'] + ' ' + normalized.get('descripcion', ''))
            if raw_data.get('search_keyword'):
                keywords.append(raw_data['search_keyword'])

            normalized['palabras_clave'] = list(set(keywords))  # Remover duplicados

            return normalized

        except Exception as e:
            logger.error(f"Error normalizando datos: {e}")
            return {}

    def classify_sector(self, text: str) -> str:
        """Clasificar sector basado en el contenido"""
        text_lower = text.lower()

        sectores = {
            'construccion': ['construction', 'building', 'infrastructure', 'construcción', 'obra', 'infraestructura'],
            'tecnologia': ['technology', 'software', 'it', 'digital', 'tecnología', 'sistema'],
            'salud': ['health', 'medical', 'hospital', 'salud', 'médico'],
            'educacion': ['education', 'school', 'training', 'educación', 'capacitación'],
            'servicios': ['services', 'consulting', 'advisory', 'servicios', 'consultoría'],
            'seguridad': ['security', 'safety', 'seguridad', 'vigilancia'],
            'transporte': ['transport', 'vehicle', 'logistics', 'transporte', 'vehículo']
        }

        for sector, keywords in sectores.items():
            if any(keyword in text_lower for keyword in keywords):
                return sector

        return 'otros'

    def extract_keywords(self, text: str) -> List[str]:
        """Extraer palabras clave relevantes"""
        important_words = ['construction', 'technology', 'services', 'consulting', 'infrastructure',
                          'maintenance', 'procurement', 'supply', 'development', 'implementation',
                          'construcción', 'tecnología', 'servicios', 'consultoría', 'infraestructura']

        text_lower = text.lower()
        keywords = [word for word in important_words if word in text_lower]
        return keywords

    def create_metadata(self, normalized_data: Dict) -> Dict:
        """Crear metadata usando datos ya normalizados"""
        try:
            # Formatear fechas a string para JSON
            fecha_pub = normalized_data.get('fecha_publicacion')
            if isinstance(fecha_pub, datetime):
                fecha_pub = fecha_pub.strftime('%Y-%m-%d')
            
            fecha_apertura = normalized_data.get('fecha_apertura')
            if isinstance(fecha_apertura, datetime):
                fecha_apertura = fecha_apertura.strftime('%Y-%m-%d')

            # Metadata estandarizada
            metadata = {
                "id": normalized_data.get('unique_id'),
                "institucion": normalized_data.get('entidad', 'NO ESPECIFICADO'),
                "titulo": normalized_data.get('titulo', 'Sin título'),
                "tipo_procedimiento": "NO ESPECIFICADO",
                "importe_drc": normalized_data.get('monto', 0.0),
                "monto_sin_imp__maximo": normalized_data.get('monto', 0.0),
                "monto_sin_imp__minimo": normalized_data.get('monto', 0.0),
                "fecha_de_publicacion": fecha_pub or "no especificado",
                "fecha_de_apertura": fecha_apertura or "no especificado",
                "fecha_de_fallo": "no especificado",
                "estatus_drc": normalized_data.get('estado', 'PUBLICADO').upper(),
                "estatus_contrato": "PUBLICADO",
                "url_anuncio": normalized_data.get('url_original', ''),
                "fuente": "licita_ya",
                "rfc": "no especificado",
                "proveedor": "no especificado",
                "pais": normalized_data.get('pais', ''),
                "estado": normalized_data.get('estado_geo', ''),  # Usar estado_geo para diferenciar de estatus
                "ciudad": normalized_data.get('ciudad', ''),
                "search_keyword": normalized_data.get('palabras_clave', [])[0] if normalized_data.get('palabras_clave') else ''
            }

            return metadata

        except Exception as e:
            logger.error(f"Error creando metadata: {e}")
            return {}

    def create_texto_semantico(self, normalized_data: Dict, metadata: Dict) -> str:
        """Crear texto semántico completo y uniforme"""
        try:
            # Extraer información clave desde metadata (ya normalizada)
            titulo = metadata.get('titulo', 'Sin título')
            institucion = metadata.get('institucion', 'Sin institución')
            url = metadata.get('url_anuncio', '')
            id_licitacion = metadata.get('id', '')
            fecha_pub = metadata.get('fecha_de_publicacion', '')
            monto = metadata.get('importe_drc', 0.0)
            moneda = normalized_data.get('moneda', 'MXN')
            
            # Descripción detallada
            descripcion_parts = []
            
            # Usar descripción normalizada si existe
            desc_norm = normalized_data.get('descripcion')
            if desc_norm and desc_norm != titulo:
                descripcion_parts.append(desc_norm)

            search_keyword = metadata.get('search_keyword', '')
            if search_keyword:
                descripcion_parts.append(f"Categoría: {search_keyword}")

            ubicacion = []
            if metadata.get('ciudad'): ubicacion.append(metadata['ciudad'])
            if metadata.get('estado'): ubicacion.append(metadata['estado']) # Ojo: aqui metadata 'estado' es geo? No, en metadata mapeé geo a estado? 
            # En create_metadata mapeé estado_geo -> estado (en el dict normalized_data necesito asegurar que exista estado_geo)
            
            ubicacion_str = ", ".join(ubicacion)
            if ubicacion_str:
                descripcion_parts.append(f"Ubicación: {ubicacion_str}")

            descripcion = '. '.join(descripcion_parts) if descripcion_parts else 'Sin descripción detallada'
            
            # Formatear monto
            monto_str = f"${monto:,.2f} {moneda}" if monto > 0 else "Monto no especificado"

            # Crear texto semántico uniforme
            texto_semantico = f"""Licitación {id_licitacion}: {titulo.upper()}. Institución: {institucion}. Monto estimado: {monto_str}. Fecha de publicación: {fecha_pub}. Descripción: {descripcion}. Estado: {metadata.get('estatus_drc', 'PUBLICADO')}. URL: {url}."""

            return texto_semantico

        except Exception as e:
            logger.error(f"Error creando texto semántico: {e}")
            return f"Error procesando licitación: {str(e)}"

    def process_licitaciones(self, licitaciones: List[Dict]) -> int:
        """Procesar y almacenar licitaciones"""
        processed_count = 0

        for licitacion in licitaciones:
            try:
                # 1. Normalizar datos primero
                normalized = self.normalize_licitacion_data(licitacion)
                if not normalized or not normalized.get('titulo'):
                    logger.warning("Datos insuficientes para procesar licitación")
                    continue

                # 2. Crear metadata desde datos normalizados
                metadata = self.create_metadata(normalized)
                
                # 3. Crear texto semántico
                texto_semantico = self.create_texto_semantico(normalized, metadata)

                # Guardar en base de datos usando nueva estructura
                update_id = self.db_manager.save_update(
                    metadata=metadata,
                    texto_semantico=texto_semantico,
                    fuente='licita_ya'
                )

                # Generar y almacenar embedding en ChromaDB y PostgreSQL
                embedding = self.vector_manager.store_in_vector_db({
                    'id': metadata.get('id'),
                    'titulo': metadata.get('titulo'),
                    'texto_semantico': texto_semantico,
                    'fuente': 'licita_ya'
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

    def extract_keyword_based_data(self) -> Dict[str, Any]:
        """Extraer datos basados en palabras clave para un rango de días"""
        logger.info("Iniciando extracción de Licita Ya...")

        try:
            # Configurable lookback days (default 3 days to catch missed updates)
            lookback_days = int(os.getenv('LICITA_YA_LOOKBACK_DAYS', 3))
            today = datetime.now()
            
            all_licitaciones = []
            seen_ids = set()

            # Iterate from today backwards
            for i in range(lookback_days):
                target_date = today - timedelta(days=i)
                logger.info(f"Procesando fecha objetivo: {target_date.date()}")
                
                # Buscar por todas las keywords para esta fecha
                daily_licitaciones = self.search_all_keywords(target_date)
                
                for lic in daily_licitaciones:
                    lic_id = (lic.get('unique_id') or lic.get('id') or lic.get('url'))
                    if lic_id and lic_id not in seen_ids:
                        seen_ids.add(lic_id)
                        all_licitaciones.append(lic)
            
            if not all_licitaciones:
                logger.info("No se encontraron licitaciones en el rango de fechas especificado")
                return {
                    'status': 'success',
                    'source': 'licita_ya',
                    'total_found': 0,
                    'total_processed': 0,
                    'keywords_used': self.search_keywords,
                    'message': 'No hay nuevas licitaciones'
                }

            # Procesar licitaciones acumuladas
            processed_count = self.process_licitaciones(all_licitaciones)

            result = {
                'status': 'success',
                'source': 'licita_ya',
                'total_found': len(all_licitaciones),
                'total_processed': processed_count,
                'keywords_used': self.search_keywords,
                'date_range': f"{lookback_days} days until {today.strftime('%Y-%m-%d')}"
            }

            logger.info(f"Extracción completada: {processed_count}/{len(all_licitaciones)} procesadas")
            return result

        except Exception as e:
            logger.error(f"Error en extracción de Licita Ya: {e}")
            return {
                'status': 'error',
                'source': 'licita_ya',
                'error': str(e),
                'keywords_used': self.search_keywords
            }