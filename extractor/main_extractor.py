#!/usr/bin/env python3
"""
Script Principal del Extractor de Licitaciones
Ejecuta los 3 extractores y configura la base de datos vectorial automáticamente.
"""

import os
import sys
import json
import asyncio
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from loguru import logger
from dotenv import load_dotenv

# Agregar directorio src al path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.database.models import DatabaseManager
from src.utils.vector_manager import VectorManager
from src.extractors.tianguis_digital_extractor import TianguisDigitalExtractor
from src.extractors.licita_ya_extractor import LicitaYaExtractor
from src.extractors.compras_mx_extractor import ComprasMXExtractor

class MainExtractor:
    def __init__(self):
        # Cargar variables de entorno
        load_dotenv()

        # Configurar logging
        self.setup_logging()

        # Inicializar componentes
        self.db_manager = DatabaseManager()
        self.vector_manager = VectorManager()

        # Inicializar extractores
        self.tianguis_extractor = TianguisDigitalExtractor()
        self.licita_ya_extractor = LicitaYaExtractor()
        self.compras_mx_extractor = ComprasMXExtractor()

        logger.info("Extractor principal inicializado correctamente")

    def setup_logging(self):
        """Configurar sistema de logging"""
        log_file = os.getenv('LOG_FILE', 'logs/extractor.log')
        log_level = os.getenv('LOG_LEVEL', 'INFO')

        # Crear directorio de logs si no existe
        os.makedirs(os.path.dirname(log_file), exist_ok=True)

        # Configurar logger
        logger.remove()  # Remover configuración por defecto

        # Agregar handler para archivo
        logger.add(
            log_file,
            rotation="10 MB",
            retention="30 days",
            level=log_level,
            format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} | {message}"
        )

        # Agregar handler para consola
        logger.add(
            sys.stdout,
            level=log_level,
            format="{time:HH:mm:ss} | {level} | {message}"
        )

    def initialize_database(self):
        """Inicializar base de datos y tablas"""
        try:
            logger.info("Inicializando base de datos...")
            self.db_manager.create_tables()
            logger.info("Base de datos inicializada correctamente")
            return True
        except Exception as e:
            logger.error(f"Error inicializando base de datos: {e}")
            return False

    def run_extractor_sequential(self, extractor_func, name):
        """Ejecutar un extractor de forma secuencial"""
        try:
            logger.info(f"Iniciando extracción: {name}")
            start_time = datetime.now()

            result = extractor_func()

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            logger.info(f"Extracción {name} completada en {duration:.2f} segundos")
            return result

        except Exception as e:
            logger.error(f"Error en extracción {name}: {e}")
            return {
                'status': 'error',
                'source': name.lower().replace(' ', '_'),
                'error': str(e)
            }

    def run_extractor_parallel(self, extractor_func, name):
        """Ejecutar un extractor en paralelo"""
        return self.run_extractor_sequential(extractor_func, name)

    def run_all_extractors_sequential(self):
        """Ejecutar todos los extractores de forma secuencial"""
        logger.info("Ejecutando extractores en modo secuencial...")

        results = []

        # 1. Tianguis Digital (API - rápido)
        result1 = self.run_extractor_sequential(
            self.tianguis_extractor.extract_yesterday_data,
            "Tianguis Digital"
        )
        results.append(result1)

        # 2. Licita Ya (API con keywords - moderado)
        result2 = self.run_extractor_sequential(
            self.licita_ya_extractor.extract_keyword_based_data,
            "Licita Ya"
        )
        results.append(result2)

        # 3. ComprasMX (Web scraping - lento)
        result3 = self.run_extractor_sequential(
            self.compras_mx_extractor.extract_yesterday_data,
            "ComprasMX"
        )
        results.append(result3)

        return results

    def run_all_extractors_parallel(self):
        """Ejecutar todos los extractors en paralelo"""
        logger.info("Ejecutando extractores en modo paralelo...")

        extractors = [
            (self.tianguis_extractor.extract_yesterday_data, "Tianguis Digital"),
            (self.licita_ya_extractor.extract_keyword_based_data, "Licita Ya"),
            (self.compras_mx_extractor.extract_yesterday_data, "ComprasMX")
        ]

        results = []

        with ThreadPoolExecutor(max_workers=3) as executor:
            # Enviar tareas al pool
            futures = [
                executor.submit(self.run_extractor_parallel, func, name)
                for func, name in extractors
            ]

            # Recoger resultados
            for future in futures:
                try:
                    result = future.result(timeout=1800)  # 30 minutos timeout
                    results.append(result)
                except Exception as e:
                    logger.error(f"Error en extractor paralelo: {e}")
                    results.append({
                        'status': 'error',
                        'error': str(e)
                    })

        return results

    def process_unprocessed_vectors(self):
        """Procesar licitaciones que no tienen embeddings"""
        try:
            logger.info("Procesando licitaciones pendientes para vectorización...")

            unprocessed = self.db_manager.get_unprocessed_licitaciones()

            if not unprocessed:
                logger.info("No hay licitaciones pendientes de vectorización")
                return 0

            processed_count = 0

            for licitacion in unprocessed:
                try:
                    # Convertir a diccionario
                    licitacion_dict = licitacion.to_dict()

                    # Generar embedding y almacenar en vector DB
                    embedding = self.vector_manager.store_in_vector_db(licitacion_dict)

                    if embedding:
                        # Actualizar en base de datos
                        self.db_manager.update_embedding(licitacion.id, embedding)
                        processed_count += 1

                        logger.info(f"Vector procesado para: {licitacion.titulo[:50]}...")

                except Exception as e:
                    logger.error(f"Error procesando vector para licitación {licitacion.id}: {e}")
                    continue

            logger.info(f"Vectorización completada: {processed_count}/{len(unprocessed)} procesadas")
            return processed_count

        except Exception as e:
            logger.error(f"Error en procesamiento de vectores: {e}")
            return 0

    def generate_summary_report(self, results):
        """Generar reporte de resumen"""
        try:
            summary = {
                'timestamp': datetime.now().isoformat(),
                'total_sources': len(results),
                'successful_extractions': len([r for r in results if r.get('status') == 'success']),
                'failed_extractions': len([r for r in results if r.get('status') == 'error']),
                'total_found': sum(r.get('total_found', 0) for r in results if r.get('status') == 'success'),
                'total_processed': sum(r.get('total_processed', 0) for r in results if r.get('status') == 'success'),
                'sources_detail': results,
                'vector_db_stats': self.vector_manager.get_collection_stats()
            }

            # Guardar reporte
            report_file = f"logs/extraction_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(summary, f, indent=2, ensure_ascii=False, default=str)

            logger.info(f"Reporte guardado en: {report_file}")

            # Log resumen
            logger.info("=" * 60)
            logger.info("RESUMEN DE EXTRACCIÓN")
            logger.info("=" * 60)
            logger.info(f"Fuentes exitosas: {summary['successful_extractions']}/{summary['total_sources']}")
            logger.info(f"Total encontrado: {summary['total_found']}")
            logger.info(f"Total procesado: {summary['total_processed']}")
            logger.info(f"Base vectorial: {summary['vector_db_stats'].get('total_documents', 0)} documentos")
            logger.info("=" * 60)

            return summary

        except Exception as e:
            logger.error(f"Error generando reporte: {e}")
            return None

    def run_extraction(self, parallel=False):
        """Ejecutar proceso completo de extracción"""
        try:
            start_time = datetime.now()
            logger.info(f"Iniciando proceso de extracción completo - {start_time}")

            # 1. Inicializar base de datos
            if not self.initialize_database():
                logger.error("Fallo en inicialización de base de datos. Abortando.")
                return False

            # 2. Ejecutar extractores
            if parallel:
                results = self.run_all_extractors_parallel()
            else:
                results = self.run_all_extractors_sequential()

            # 3. Procesar vectores pendientes
            vector_count = self.process_unprocessed_vectors()

            # 4. Generar reporte
            summary = self.generate_summary_report(results)

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            logger.info(f"Proceso completo finalizado en {duration:.2f} segundos")

            return True

        except Exception as e:
            logger.error(f"Error en proceso principal: {e}")
            return False

def main():
    """Función principal"""
    import argparse

    parser = argparse.ArgumentParser(description='Extractor de Licitaciones - Sistema RAG')
    parser.add_argument('--parallel', action='store_true', help='Ejecutar extractores en paralelo')
    parser.add_argument('--only-vectors', action='store_true', help='Solo procesar vectores pendientes')
    parser.add_argument('--test-connection', action='store_true', help='Probar conexiones')

    args = parser.parse_args()

    try:
        extractor = MainExtractor()

        if args.test_connection:
            logger.info("Probando conexiones...")

            # Probar base de datos
            try:
                extractor.db_manager.create_tables()
                logger.info("✅ Conexión a PostgreSQL exitosa")
            except Exception as e:
                logger.error(f"❌ Error conectando a PostgreSQL: {e}")

            # Probar vector database
            try:
                stats = extractor.vector_manager.get_collection_stats()
                logger.info(f"✅ Conexión a Vector DB exitosa: {stats}")
            except Exception as e:
                logger.error(f"❌ Error conectando a Vector DB: {e}")

            return

        if args.only_vectors:
            logger.info("Procesando solo vectores pendientes...")
            count = extractor.process_unprocessed_vectors()
            logger.info(f"Procesados {count} vectores")
            return

        # Ejecutar extracción completa
        success = extractor.run_extraction(parallel=args.parallel)

        if success:
            logger.info("🎉 Extracción completada exitosamente")
        else:
            logger.error("❌ Extracción falló")
            sys.exit(1)

    except KeyboardInterrupt:
        logger.info("Proceso interrumpido por el usuario")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Error fatal: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()