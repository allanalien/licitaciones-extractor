#!/usr/bin/env python3
"""
Worker para Railway - Ejecuta la extracción con schedule
"""

import os
import sys
import time
import schedule
from datetime import datetime
from loguru import logger
from dotenv import load_dotenv

# Importar el extractor principal
from main_extractor import MainExtractor

load_dotenv()

class ExtractionWorker:
    def __init__(self):
        self.extractor = MainExtractor()
        logger.info("Worker de extracción inicializado")

    def run_extraction(self):
        """Ejecutar proceso de extracción completo"""
        try:
            logger.info(f"🚀 Iniciando extracción programada - {datetime.now()}")

            # Inicializar base de datos
            self.extractor.initialize_database()

            # Ejecutar extractores
            results = self.extractor.run_all_extractors_sequential()

            # Procesar vectores pendientes
            processed = self.extractor.process_unprocessed_vectors()

            # Generar resumen
            summary = self.extractor.generate_summary(results)

            logger.info(f"✅ Extracción completada exitosamente")
            logger.info(f"📊 Resumen: {summary}")

            return summary

        except Exception as e:
            logger.error(f"❌ Error en extracción: {e}")
            return {'status': 'error', 'error': str(e)}

    def setup_schedule(self):
        """Configurar el schedule para ejecución diaria"""
        # Ejecutar todos los días a las 6:00 AM
        schedule.every().day.at("06:00").do(self.run_extraction)

        # También ejecutar cada 12 horas para no perder datos
        schedule.every(12).hours.do(self.run_extraction)

        logger.info("📅 Schedule configurado:")
        logger.info("  - Ejecución diaria a las 6:00 AM")
        logger.info("  - Ejecución adicional cada 12 horas")

    def run_once_on_start(self):
        """Ejecutar una vez al iniciar si no hay datos recientes"""
        try:
            import psycopg2
            from datetime import timedelta

            conn = psycopg2.connect(os.getenv('DATABASE_URL'))
            cursor = conn.cursor()

            # Verificar última ejecución
            cursor.execute("""
                SELECT MAX(created_at) FROM licitaciones
            """)
            last_run = cursor.fetchone()[0]

            cursor.close()
            conn.close()

            if not last_run or (datetime.now() - last_run).days > 0:
                logger.info("🔄 Ejecutando extracción inicial...")
                self.run_extraction()
            else:
                logger.info("✅ Datos recientes encontrados, esperando próximo schedule")

        except Exception as e:
            logger.warning(f"No se pudo verificar última ejecución: {e}")
            logger.info("🔄 Ejecutando extracción inicial por precaución...")
            self.run_extraction()

    def start(self):
        """Iniciar el worker"""
        logger.info("🤖 Worker de extracción iniciado")
        logger.info(f"📍 Environment: {os.getenv('ENVIRONMENT', 'production')}")
        logger.info(f"🔗 Database URL configured: {'✅' if os.getenv('DATABASE_URL') else '❌'}")
        logger.info(f"🌐 Port: {os.getenv('PORT', 8080)}")

        # Configurar schedule
        self.setup_schedule()

        # Ejecutar una vez al iniciar si es necesario
        self.run_once_on_start()

        # Loop principal
        logger.info("💫 Worker en ejecución continua...")
        logger.info("📊 Monitoreando en /health, /api/status y /api/worker/status")

        iteration = 0
        while True:
            try:
                schedule.run_pending()
                iteration += 1

                # Log cada 10 minutos
                if iteration % 10 == 0:
                    logger.info(f"💓 Worker alive - Iteration {iteration}")
                    logger.info(f"📅 Next scheduled runs: {[str(job.next_run) for job in schedule.jobs]}")

                time.sleep(60)  # Verificar cada minuto
            except KeyboardInterrupt:
                logger.info("Worker detenido por el usuario")
                break
            except Exception as e:
                logger.error(f"Error en el loop del worker: {e}")
                time.sleep(60)

if __name__ == '__main__':
    worker = ExtractionWorker()
    worker.start()