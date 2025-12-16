import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any
from loguru import logger
import time
import os
import re
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from ..database.models import DatabaseManager
from ..utils.vector_manager import VectorManager

class ComprasMXExtractor:
    def __init__(self):
        self.base_url = "https://comprasmx.buengobierno.gob.mx"
        self.search_url = f"{self.base_url}/sitiopublico/#/"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': os.getenv('USER_AGENT', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'es-ES,es;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive'
        })

        self.db_manager = DatabaseManager()
        self.vector_manager = VectorManager()
        self.max_retries = int(os.getenv('MAX_RETRIES', 3))
        self.request_delay = int(os.getenv('REQUEST_DELAY', 2))

        # Configurar Selenium
        self.driver = None
        self.setup_selenium()

    def setup_selenium(self):
        """Configurar driver de Selenium"""
        try:
            chrome_options = Options()
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--window-size=1920,1080')
            chrome_options.add_argument(f"--user-agent={os.getenv('USER_AGENT', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')}")

            from selenium.webdriver.chrome.service import Service
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(
                service=service,
                options=chrome_options
            )
            logger.info("Driver de Selenium configurado correctamente")

        except Exception as e:
            logger.error(f"Error configurando Selenium: {e}")
            self.driver = None

    def get_yesterday_date_range(self) -> tuple:
        """Obtener rango de fechas del día anterior"""
        yesterday = datetime.now() - timedelta(days=1)
        start_date = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)
        return start_date, end_date

    def search_licitaciones(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Buscar licitaciones en ComprasMX usando la tabla principal"""
        try:
            if not self.driver:
                logger.error("Driver de Selenium no disponible")
                return []

            logger.info(f"Navegando a ComprasMX para buscar licitaciones del {start_date.date()}")

            # Navegar a la página principal
            self.driver.get(self.search_url)
            time.sleep(5)  # Esperar a que cargue la SPA Angular

            # Configurar filtro de fecha para el día anterior
            self.set_date_filter(start_date)

            # Esperar a que se actualice la tabla
            time.sleep(3)

            # Extraer todas las licitaciones de la tabla
            table_licitaciones = self.extract_table_data()

            # Limitar el número de licitaciones a procesar para evitar timeouts
            max_items = int(os.getenv('COMPRASMX_MAX_ITEMS', 50))
            if len(table_licitaciones) > max_items:
                logger.info(f"Limitando procesamiento a {max_items} licitaciones de {len(table_licitaciones)} encontradas")
                table_licitaciones = table_licitaciones[:max_items]

            # Para cada licitación, obtener detalles completos si tiene URL, sino usar datos de tabla
            detailed_licitaciones = []
            for i, licitacion in enumerate(table_licitaciones):
                try:
                    logger.info(f"Procesando licitación {i+1}/{len(table_licitaciones)}: {licitacion.get('titulo', 'Sin título')[:50]}...")

                    # Si tenemos URL de detalle, obtener información adicional
                    if licitacion.get('detail_url'):
                        details = self.get_licitacion_details(licitacion.get('detail_url'))
                        if details:
                            # Combinar datos de la tabla con datos del detalle
                            licitacion.update(details)
                    else:
                        # Si no hay URL de detalle, usar solo datos de la tabla
                        logger.info(f"Sin URL de detalle para licitación {i+1}, usando solo datos de tabla")
                        # Enriquecer con datos de la tabla
                        self.enrich_from_table_data(licitacion)

                    detailed_licitaciones.append(licitacion)
                    time.sleep(1)  # Pausa más corta

                except Exception as e:
                    logger.error(f"Error procesando licitación {i+1}: {e}")
                    # Agregar la licitación con solo datos de tabla si hay error
                    self.enrich_from_table_data(licitacion)
                    detailed_licitaciones.append(licitacion)
                    continue

            logger.info(f"Encontradas {len(detailed_licitaciones)} licitaciones completas en ComprasMX")
            return detailed_licitaciones

        except Exception as e:
            logger.error(f"Error general en búsqueda de ComprasMX: {e}")
            return []

    def set_date_filter(self, target_date: datetime):
        """Configurar filtro de fecha en la tabla de ComprasMX"""
        try:
            wait = WebDriverWait(self.driver, 10)

            # Buscar el control de fecha - puede estar en diferentes lugares
            date_selectors = [
                "input[type='date']",
                "input[placeholder*='fecha']",
                ".date-picker input",
                "[ng-model*='fecha']",
                "[ng-model*='date']"
            ]

            date_input = None
            for selector in date_selectors:
                try:
                    date_input = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, selector)))
                    break
                except:
                    continue

            if date_input:
                # Formatear fecha para el input (YYYY-MM-DD)
                formatted_date = target_date.strftime('%Y-%m-%d')
                logger.info(f"Configurando fecha a: {formatted_date}")

                # Limpiar y establecer fecha
                date_input.clear()
                date_input.send_keys(formatted_date)
                time.sleep(1)

                # Buscar botón de aplicar/buscar
                search_buttons = [
                    "button[type='submit']",
                    "input[type='submit']",
                    "button:contains('Buscar')",
                    "button:contains('Aplicar')",
                    ".btn-search",
                    ".btn-primary"
                ]

                for btn_selector in search_buttons:
                    try:
                        search_btn = self.driver.find_element(By.CSS_SELECTOR, btn_selector)
                        search_btn.click()
                        logger.info("Filtro de fecha aplicado")
                        return
                    except:
                        continue

                logger.info("Fecha configurada, esperando actualización automática")
            else:
                logger.warning("No se encontró control de fecha, extrayendo datos sin filtro")

        except Exception as e:
            logger.warning(f"Error configurando filtro de fecha: {e}")

    def extract_table_data(self) -> List[Dict]:
        """Extraer datos de la tabla principal de licitaciones"""
        try:
            wait = WebDriverWait(self.driver, 10)

            # Buscar tabla de licitaciones
            table_selectors = [
                "table",
                ".table",
                ".data-table",
                "[role='grid']",
                ".grid-container"
            ]

            table = None
            for selector in table_selectors:
                try:
                    table = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
                    break
                except:
                    continue

            if not table:
                logger.error("No se encontró tabla de licitaciones")
                return []

            # Extraer filas de datos
            rows = table.find_elements(By.TAG_NAME, "tr")
            if len(rows) < 2:  # Al menos header + 1 fila de datos
                logger.warning("Tabla encontrada pero sin datos")
                return []

            licitaciones = []
            header_row = rows[0]
            headers = [th.text.strip().lower() for th in header_row.find_elements(By.TAG_NAME, "th")]

            logger.info(f"Encontrados {len(rows)-1} registros en la tabla")

            for i, row in enumerate(rows[1:], 1):  # Saltar header
                try:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if len(cells) == 0:
                        continue

                    # Extraer datos básicos de la fila
                    licitacion = {
                        'table_index': i,
                        'raw_cells': [cell.text.strip() for cell in cells]
                    }

                    # Mapear campos comunes
                    for j, cell in enumerate(cells):
                        text = cell.text.strip()
                        if j < len(headers):
                            header = headers[j]
                            if 'titulo' in header or 'nombre' in header or 'descripcion' in header:
                                licitacion['titulo'] = text
                            elif 'fecha' in header:
                                licitacion['fecha'] = text
                            elif 'entidad' in header or 'institucion' in header:
                                licitacion['entidad'] = text
                            elif 'estado' in header or 'estatus' in header:
                                licitacion['estado'] = text

                    # Buscar link de detalle en la fila - probar múltiples estrategias
                    detail_links = row.find_elements(By.TAG_NAME, "a")
                    detail_url = None

                    for link in detail_links:
                        href = link.get_attribute("href")
                        if href:
                            # Verificar diferentes patrones de URL de detalle
                            if any(pattern in href.lower() for pattern in ['detalle', 'detail', 'ver', 'view', '#/sitiopublico/']):
                                detail_url = href
                                break

                    # Si no encontramos link directo, buscar en botones o elementos clickeables
                    if not detail_url:
                        clickable_elements = row.find_elements(By.CSS_SELECTOR, "[onclick], [data-href], .clickable")
                        for element in clickable_elements:
                            onclick = element.get_attribute("onclick") or ""
                            data_href = element.get_attribute("data-href") or ""
                            if onclick or data_href:
                                # Extraer ID o parámetros para construir URL
                                import re
                                id_match = re.search(r"(\d+)", onclick + data_href)
                                if id_match:
                                    detail_url = f"{self.base_url}/sitiopublico/#/detalle/{id_match.group(1)}"
                                    break

                    if detail_url:
                        licitacion['detail_url'] = detail_url

                    # Usar texto de la primera celda como título si no se encontró
                    if 'titulo' not in licitacion and cells:
                        licitacion['titulo'] = cells[0].text.strip()

                    licitaciones.append(licitacion)

                except Exception as e:
                    logger.error(f"Error procesando fila {i}: {e}")
                    continue

            logger.info(f"Extraídas {len(licitaciones)} licitaciones de la tabla")
            return licitaciones

        except Exception as e:
            logger.error(f"Error extrayendo datos de tabla: {e}")
            return []

    def get_licitacion_details(self, detail_url: str) -> Dict:
        """Obtener detalles completos de una licitación navegando a su página específica"""
        try:
            if not detail_url:
                return {}

            logger.debug(f"Navegando a detalles: {detail_url}")

            # Navegar a la página de detalle con timeout más corto
            self.driver.set_page_load_timeout(15)  # 15 segundos máximo para cargar página
            self.driver.get(detail_url)
            time.sleep(2)  # Tiempo más corto de espera

            # Esperar a que se cargue el contenido con timeout reducido
            wait = WebDriverWait(self.driver, 8)
            try:
                wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            except:
                logger.warning(f"Timeout esperando contenido en {detail_url}")
                return {}

            details = {}

            # Extraer información de detalle usando diferentes estrategias
            self.extract_detail_sections(details)
            self.extract_detail_fields(details)
            self.extract_detail_tables(details)

            logger.debug(f"Detalles extraídos: {len(details)} campos")
            return details

        except Exception as e:
            logger.warning(f"Error obteniendo detalles de {detail_url}: {e}")
            return {}

    def extract_detail_sections(self, details: Dict):
        """Extraer información de secciones de detalle"""
        try:
            # Buscar secciones comunes
            section_selectors = [
                ".detail-section",
                ".info-section",
                ".card",
                ".panel",
                "[class*='detalle']"
            ]

            for selector in section_selectors:
                try:
                    sections = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for section in sections:
                        text = section.text.strip()
                        if text:
                            # Buscar patrones específicos
                            if 'monto' in text.lower() or 'importe' in text.lower():
                                details['monto_text'] = text
                            elif 'fecha' in text.lower():
                                details['fecha_text'] = text
                            elif 'procedimiento' in text.lower():
                                details['procedimiento_text'] = text
                except:
                    continue

        except Exception as e:
            logger.debug(f"Error extrayendo secciones: {e}")

    def extract_detail_fields(self, details: Dict):
        """Extraer campos específicos de la página de detalle"""
        try:
            # Buscar campos de formulario o texto
            field_mappings = {
                'titulo': ['h1', 'h2', '.title', '.titulo', '[class*="titulo"]'],
                'descripcion': ['.description', '.descripcion', '.detalle-descripcion'],
                'monto': ['[class*="monto"]', '[class*="importe"]'],
                'fecha_publicacion': ['[class*="fecha"]', '[class*="publication"]'],
                'entidad': ['[class*="entidad"]', '[class*="institucion"]'],
                'procedimiento': ['[class*="procedimiento"]', '[class*="method"]']
            }

            for field, selectors in field_mappings.items():
                for selector in selectors:
                    try:
                        elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                        if elements:
                            text = elements[0].text.strip()
                            if text and len(text) < 500:  # Evitar texto demasiado largo
                                details[field] = text
                                break
                    except:
                        continue

        except Exception as e:
            logger.debug(f"Error extrayendo campos: {e}")

    def extract_detail_tables(self, details: Dict):
        """Extraer información de tablas en la página de detalle"""
        try:
            tables = self.driver.find_elements(By.TAG_NAME, "table")
            table_data = []

            for table in tables:
                try:
                    rows = table.find_elements(By.TAG_NAME, "tr")
                    for row in rows:
                        cells = row.find_elements(By.TAG_NAME, "td")
                        if len(cells) >= 2:
                            key = cells[0].text.strip().lower()
                            value = cells[1].text.strip()

                            # Mapear campos comunes
                            if 'monto' in key or 'importe' in key:
                                details['monto'] = value
                            elif 'fecha' in key:
                                details['fecha_publicacion'] = value
                            elif 'procedimiento' in key or 'tipo' in key:
                                details['tipo_procedimiento'] = value
                            elif 'entidad' in key or 'institución' in key:
                                details['entidad'] = value
                            elif 'estado' in key or 'estatus' in key:
                                details['estado'] = value

                            table_data.append((key, value))

                except:
                    continue

            if table_data:
                details['table_data'] = table_data

        except Exception as e:
            logger.debug(f"Error extrayendo tablas: {e}")

    def enrich_from_table_data(self, licitacion: Dict):
        """Enriquecer licitación usando solo datos de la tabla cuando no hay detalle disponible"""
        try:
            raw_cells = licitacion.get('raw_cells', [])

            # Intentar extraer información de las celdas de la tabla
            if len(raw_cells) >= 2:
                # La primera celda suele ser el título o descripción
                if not licitacion.get('titulo') and raw_cells[0]:
                    licitacion['titulo'] = raw_cells[0].strip()

                # La segunda celda puede contener entidad o fecha
                if len(raw_cells) > 1 and not licitacion.get('entidad'):
                    licitacion['entidad'] = raw_cells[1].strip() if raw_cells[1] else 'NO ESPECIFICADO'

                # Buscar números que puedan ser montos
                for cell in raw_cells:
                    if cell and '$' in cell:
                        licitacion['monto'] = cell.strip()
                        break

                # Buscar fechas
                for cell in raw_cells:
                    if cell and ('/' in cell or '-' in cell):
                        import re
                        if re.match(r'\d{1,2}[/-]\d{1,2}[/-]\d{4}', cell):
                            licitacion['fecha'] = cell.strip()
                            break

            # Establecer valores por defecto
            if not licitacion.get('titulo'):
                licitacion['titulo'] = 'Procedimiento de contratación'
            if not licitacion.get('entidad'):
                licitacion['entidad'] = 'NO ESPECIFICADO'

            logger.debug(f"Licitación enriquecida con datos de tabla: {licitacion.get('titulo')[:30]}...")

        except Exception as e:
            logger.error(f"Error enriqueciendo licitación con datos de tabla: {e}")

    def create_metadata(self, raw_data: Dict) -> Dict:
        """Crear metadata siguiendo el formato requerido"""
        try:
            # Generar ID único para ComprasMX
            detail_url = raw_data.get('detail_url', '')
            if detail_url and '/detalle/' in detail_url:
                compras_id = detail_url.split('/detalle/')[-1].split('/')[0]
            else:
                compras_id = f"compras-mx-{hash(str(raw_data))}"

            # Título
            titulo = ''
            if raw_data.get('titulo'):
                titulo = raw_data['titulo']
            elif raw_data.get('descripcion'):
                titulo = raw_data['descripcion'][:100]  # Limitar longitud

            # Entidad
            entidad = raw_data.get('entidad', 'NO ESPECIFICADO')

            # Monto - extraer números del texto
            monto = 0.0
            monto_text = raw_data.get('monto', raw_data.get('monto_text', ''))
            if monto_text:
                # Buscar números en el texto
                numbers = re.findall(r'[\d,]+\.?\d*', monto_text.replace(',', ''))
                if numbers:
                    try:
                        monto = float(numbers[0].replace(',', ''))
                    except:
                        monto = 0.0

            # Fecha de publicación
            fecha_publicacion = None
            fecha_text = raw_data.get('fecha_publicacion', raw_data.get('fecha', raw_data.get('fecha_text', '')))
            if fecha_text:
                try:
                    # Intentar diferentes formatos de fecha
                    for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%d-%m-%Y']:
                        try:
                            fecha_obj = datetime.strptime(fecha_text[:10], fmt)
                            fecha_publicacion = fecha_obj.strftime('%Y-%m-%d')
                            break
                        except:
                            continue
                except:
                    pass

            # Tipo de procedimiento
            tipo_procedimiento = raw_data.get('tipo_procedimiento',
                                            raw_data.get('procedimiento',
                                                       raw_data.get('procedimiento_text', 'NO ESPECIFICADO')))

            # Estado
            estado = raw_data.get('estado', 'PUBLICADO')

            # URL
            url_anuncio = raw_data.get('detail_url', 'https://comprasmx.buengobierno.gob.mx/sitiopublico/#/')

            # Crear metadata similar al formato de Tianguis Digital
            metadata = {
                "id": compras_id,
                "institucion": entidad,
                "titulo": titulo if titulo else "Sin título",
                "tipo_procedimiento": tipo_procedimiento,
                "importe_drc": monto,
                "monto_sin_imp__maximo": monto * 1.16 if monto > 0 else 0,
                "monto_sin_imp__minimo": monto,
                "fecha_de_publicacion": fecha_publicacion,
                "fecha_de_apertura": "no especificado",
                "fecha_de_fallo": "no especificado",
                "estatus_drc": estado,
                "estatus_contrato": "PUBLICADO",
                "url_anuncio": url_anuncio,
                "fuente": "compras_mx",
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
            # Extraer información clave
            titulo = metadata.get('titulo', 'Sin título')
            institucion = metadata.get('institucion', 'Sin institución')
            tipo_procedimiento = metadata.get('tipo_procedimiento', 'NO ESPECIFICADO')
            monto = metadata.get('importe_drc', 0)
            fecha_publicacion = metadata.get('fecha_de_publicacion', 'no especificado')
            estado = metadata.get('estatus_drc', 'PUBLICADO')
            url = metadata.get('url_anuncio', '')
            id_licitacion = metadata.get('id', '')

            # Descripción detallada combinando datos disponibles
            descripcion_parts = []

            if raw_data.get('descripcion'):
                descripcion_parts.append(raw_data['descripcion'])

            if raw_data.get('procedimiento_text'):
                descripcion_parts.append(f"Método: {raw_data['procedimiento_text']}")

            if raw_data.get('table_data'):
                table_info = [f"{k}: {v}" for k, v in raw_data['table_data'][:3]]  # Primeras 3 entradas
                if table_info:
                    descripcion_parts.append(". ".join(table_info))

            descripcion = '. '.join(descripcion_parts) if descripcion_parts else 'Sin descripción detallada'

            # Formatear monto
            monto_str = f"${monto:,.2f} MXN" if monto > 0 else "Monto no especificado"

            # Crear texto semántico
            texto_semantico = f"""Licitación {id_licitacion}: {titulo.upper()}. Institución: {institucion}. Tipo de procedimiento: {tipo_procedimiento}. Monto estimado: {monto_str}. Fecha de publicación: {fecha_publicacion}. Descripción: {descripcion}. Estado: {estado}. URL: {url}."""

            return texto_semantico

        except Exception as e:
            logger.error(f"Error creando texto semántico: {e}")
            return f"Error procesando licitación: {str(e)}"

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
                    fuente='compras_mx'
                )

                # Generar y almacenar embedding en ChromaDB y PostgreSQL
                embedding = self.vector_manager.store_in_vector_db({
                    'id': metadata.get('id'),
                    'titulo': metadata.get('titulo'),
                    'texto_semantico': texto_semantico,
                    'fuente': 'compras_mx'
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
        logger.info("Iniciando extracción de ComprasMX...")

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
                    'source': 'compras_mx',
                    'total_found': 0,
                    'total_processed': 0,
                    'message': 'No hay nuevas licitaciones'
                }

            # Procesar licitaciones
            processed_count = self.process_licitaciones(licitaciones)

            result = {
                'status': 'success',
                'source': 'compras_mx',
                'total_found': len(licitaciones),
                'total_processed': processed_count,
                'date_range': f"{start_date.date()} - {end_date.date()}"
            }

            logger.info(f"Extracción completada: {processed_count}/{len(licitaciones)} procesadas")
            return result

        except Exception as e:
            logger.error(f"Error en extracción de ComprasMX: {e}")
            return {
                'status': 'error',
                'source': 'compras_mx',
                'error': str(e)
            }
        finally:
            # Cerrar driver de Selenium
            if self.driver:
                try:
                    self.driver.quit()
                except:
                    pass

    def __del__(self):
        """Limpiar recursos al destruir el objeto"""
        if hasattr(self, 'driver') and self.driver:
            try:
                self.driver.quit()
            except:
                pass