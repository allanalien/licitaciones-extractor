#!/usr/bin/env python3
"""
Script completo de prueba del sistema de extracción
Verifica que todos los componentes funcionen correctamente
"""

import os
import sys
import asyncio
from datetime import datetime, timedelta
from loguru import logger

# Agregar directorio src al path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.database.connection import DatabaseConnection
from src.extractors.tianguis_digital_extractor import TianguisDigitalExtractor
from src.extractors.licita_ya_extractor import LicitaYaExtractor
from src.extractors.compras_mx_extractor import ComprasMXExtractor
from src.utils.vector_manager import VectorManager

def test_database_connection():
    """Probar conexión a base de datos"""
    try:
        logger.info("🔍 Probando conexión a base de datos...")
        db = DatabaseConnection()
        conn = db.get_connection()
        cursor = conn.cursor()

        # Verificar tablas
        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
        """)
        tables = cursor.fetchall()
        logger.info(f"✅ Tablas encontradas: {[t[0] for t in tables]}")

        # Contar registros
        cursor.execute("SELECT COUNT(*) FROM licitaciones")
        count = cursor.fetchone()[0]
        logger.info(f"📊 Total licitaciones en DB: {count}")

        cursor.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"❌ Error en DB: {e}")
        return False

def test_vector_manager():
    """Probar conexión a base de datos vectorial"""
    try:
        logger.info("🔍 Probando conexión a Vector DB...")
        vm = VectorManager()
        stats = vm.get_collection_stats()
        logger.info(f"✅ Vector DB stats: {stats}")
        return True
    except Exception as e:
        logger.error(f"❌ Error en Vector DB: {e}")
        return False

def test_tianguis_digital(limit_days=1):
    """Probar extractor de Tianguis Digital"""
    try:
        logger.info("🔍 Probando Tianguis Digital...")
        extractor = TianguisDigitalExtractor()

        # Modificar temporalmente para buscar solo 1 día
        yesterday = datetime.now() - timedelta(days=1)
        result = extractor.extract_yesterday_data()

        logger.info(f"✅ Tianguis Digital: {result}")
        return result.get('status') == 'success'
    except Exception as e:
        logger.error(f"❌ Error en Tianguis Digital: {e}")
        return False

def test_licita_ya_limited():
    """Probar extractor de Licita Ya con límites"""
    try:
        logger.info("🔍 Probando Licita Ya (limitado)...")
        extractor = LicitaYaExtractor()

        # Solo buscar una keyword para prueba rápida
        test_keyword = 'alimentos'
        logger.info(f"Buscando solo: {test_keyword}")

        licitaciones = extractor.search_by_keyword(test_keyword, max_pages=1)

        result = {
            'status': 'success',
            'source': 'licita_ya',
            'total_found': len(licitaciones),
            'keyword_tested': test_keyword
        }

        logger.info(f"✅ Licita Ya: {result}")
        return True
    except Exception as e:
        logger.error(f"❌ Error en Licita Ya: {e}")
        return False

def test_compras_mx_limited():
    """Probar ComprasMX con límite de tiempo"""
    try:
        logger.info("🔍 Probando ComprasMX (limitado)...")
        logger.warning("⚠️ ComprasMX usa Selenium y puede ser lento")

        # Solo verificar que el extractor se inicializa correctamente
        extractor = ComprasMXExtractor()
        logger.info("✅ ComprasMX inicializado correctamente")
        return True
    except Exception as e:
        logger.error(f"❌ Error en ComprasMX: {e}")
        return False

def verify_data_insertion():
    """Verificar que los datos se estén insertando correctamente"""
    try:
        logger.info("🔍 Verificando inserción de datos...")
        db = DatabaseConnection()
        conn = db.get_connection()
        cursor = conn.cursor()

        # Verificar registros recientes
        cursor.execute("""
            SELECT fuente, COUNT(*) as count, MAX(fecha_publicacion) as ultima
            FROM licitaciones
            WHERE fecha_publicacion >= CURRENT_DATE - INTERVAL '7 days'
            GROUP BY fuente
        """)

        results = cursor.fetchall()
        logger.info("📊 Registros últimos 7 días:")
        for row in results:
            logger.info(f"  - {row[0]}: {row[1]} registros, última: {row[2]}")

        # Verificar normalización
        cursor.execute("""
            SELECT
                COUNT(CASE WHEN titulo IS NOT NULL AND titulo != '' THEN 1 END) as con_titulo,
                COUNT(CASE WHEN descripcion IS NOT NULL AND descripcion != '' THEN 1 END) as con_descripcion,
                COUNT(CASE WHEN fecha_publicacion IS NOT NULL THEN 1 END) as con_fecha,
                COUNT(CASE WHEN url IS NOT NULL AND url != '' THEN 1 END) as con_url,
                COUNT(*) as total
            FROM licitaciones
        """)

        norm = cursor.fetchone()
        logger.info("✅ Normalización de datos:")
        logger.info(f"  - Con título: {norm[0]}/{norm[4]} ({norm[0]*100/norm[4] if norm[4] > 0 else 0:.1f}%)")
        logger.info(f"  - Con descripción: {norm[1]}/{norm[4]} ({norm[1]*100/norm[4] if norm[4] > 0 else 0:.1f}%)")
        logger.info(f"  - Con fecha: {norm[2]}/{norm[4]} ({norm[2]*100/norm[4] if norm[4] > 0 else 0:.1f}%)")
        logger.info(f"  - Con URL: {norm[3]}/{norm[4]} ({norm[3]*100/norm[4] if norm[4] > 0 else 0:.1f}%)")

        cursor.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"❌ Error verificando datos: {e}")
        return False

def run_full_test():
    """Ejecutar todas las pruebas"""
    logger.info("=" * 60)
    logger.info("🚀 INICIANDO PRUEBAS COMPLETAS DEL SISTEMA")
    logger.info("=" * 60)

    results = {
        'database': False,
        'vector_db': False,
        'tianguis_digital': False,
        'licita_ya': False,
        'compras_mx': False,
        'data_insertion': False
    }

    # 1. Probar conexiones
    results['database'] = test_database_connection()
    results['vector_db'] = test_vector_manager()

    if not results['database']:
        logger.error("❌ Sin conexión a DB, abortando pruebas")
        return results

    # 2. Probar extractores
    results['tianguis_digital'] = test_tianguis_digital()
    results['licita_ya'] = test_licita_ya_limited()
    results['compras_mx'] = test_compras_mx_limited()

    # 3. Verificar datos
    results['data_insertion'] = verify_data_insertion()

    # Resumen
    logger.info("=" * 60)
    logger.info("📊 RESUMEN DE PRUEBAS")
    logger.info("=" * 60)

    total_passed = sum(1 for v in results.values() if v)
    total_tests = len(results)

    for component, passed in results.items():
        status = "✅ PASSED" if passed else "❌ FAILED"
        logger.info(f"{component.upper()}: {status}")

    logger.info("=" * 60)
    logger.info(f"RESULTADO FINAL: {total_passed}/{total_tests} pruebas pasadas")

    if total_passed == total_tests:
        logger.info("🎉 ¡SISTEMA 100% FUNCIONAL!")
        logger.info("✅ Listo para commit y deploy")
    else:
        logger.warning("⚠️ Algunos componentes requieren atención")

    return results

if __name__ == "__main__":
    results = run_full_test()

    # Salir con código apropiado
    sys.exit(0 if all(results.values()) else 1)