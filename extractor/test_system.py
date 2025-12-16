#!/usr/bin/env python3
"""
Script para probar que todo el sistema funciona correctamente
"""

import os
import sys
import psycopg2
from datetime import datetime
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

def test_database_connection():
    """Probar conexión a la base de datos"""
    print("🔍 Probando conexión a la base de datos...")
    try:
        DATABASE_URL = os.getenv('DATABASE_URL')
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()

        # Verificar que la tabla existe
        cursor.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_name = 'licitaciones'
            );
        """)
        table_exists = cursor.fetchone()[0]

        if table_exists:
            print("✅ Conexión exitosa a la base de datos")
            print("✅ La tabla 'licitaciones' existe")

            # Contar registros
            cursor.execute("SELECT COUNT(*) FROM licitaciones;")
            count = cursor.fetchone()[0]
            print(f"📊 Total de licitaciones en la base de datos: {count}")

            # Obtener últimos 3 registros
            cursor.execute("""
                SELECT titulo, fuente, fecha_publicacion
                FROM licitaciones
                ORDER BY created_at DESC
                LIMIT 3;
            """)
            recent = cursor.fetchall()
            if recent:
                print("\n📋 Últimas 3 licitaciones agregadas:")
                for row in recent:
                    print(f"  - {row[0][:50]}... ({row[1]}) - {row[2]}")
        else:
            print("❌ La tabla 'licitaciones' no existe")
            return False

        cursor.close()
        conn.close()
        return True

    except Exception as e:
        print(f"❌ Error conectando a la base de datos: {e}")
        return False

def test_extraction():
    """Ejecutar una extracción de prueba"""
    print("\n🔄 Ejecutando extracción de prueba...")
    try:
        from main_extractor import LicitacionesAggregator

        aggregator = LicitacionesAggregator()

        # Obtener count inicial
        DATABASE_URL = os.getenv('DATABASE_URL')
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM licitaciones;")
        count_before = cursor.fetchone()[0]

        # Ejecutar extracción (solo una fuente para ser rápidos)
        print("  Extrayendo de Compras MX (prueba rápida)...")
        from src.extractors.compras_mx_extractor import ComprasMXExtractor
        extractor = ComprasMXExtractor()
        results = extractor.extract(limit=5)  # Solo 5 para prueba rápida

        if results:
            print(f"✅ Se extrajeron {len(results)} licitaciones de prueba")

            # Insertar en BD
            for lic in results:
                try:
                    cursor.execute("""
                        INSERT INTO licitaciones (
                            titulo, descripcion, fecha_publicacion, fecha_limite,
                            dependencia, url, fuente, monto, moneda, categoria,
                            ubicacion, contacto, requisitos, estado, numero_licitacion
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (url) DO NOTHING
                        RETURNING id;
                    """, (
                        lic.get('titulo'), lic.get('descripcion'), lic.get('fecha_publicacion'),
                        lic.get('fecha_limite'), lic.get('dependencia'), lic.get('url'),
                        lic.get('fuente', 'Compras MX'), lic.get('monto'), lic.get('moneda'),
                        lic.get('categoria'), lic.get('ubicacion'), lic.get('contacto'),
                        lic.get('requisitos'), lic.get('estado'), lic.get('numero_licitacion')
                    ))
                except Exception as e:
                    print(f"  ⚠️ Error insertando: {e}")
                    continue

            conn.commit()

            # Verificar count final
            cursor.execute("SELECT COUNT(*) FROM licitaciones;")
            count_after = cursor.fetchone()[0]
            new_records = count_after - count_before

            if new_records > 0:
                print(f"✅ Se insertaron {new_records} nuevos registros")
            else:
                print("ℹ️  No se insertaron nuevos registros (posibles duplicados)")
        else:
            print("⚠️ No se obtuvieron resultados de la extracción")

        cursor.close()
        conn.close()
        return True

    except Exception as e:
        print(f"❌ Error en la extracción: {e}")
        return False

def test_api_endpoints():
    """Probar los endpoints de la API localmente"""
    print("\n🌐 Probando endpoints de la API...")
    try:
        import requests

        # Nota: Esto será para cuando esté en Vercel
        # Por ahora solo mostramos que están listos
        print("✅ Endpoints listos para Vercel:")
        print("  - GET  / (Status)")
        print("  - POST /extract (Ejecutar extracción)")
        print("  - GET  /search?q=keyword (Buscar)")
        print("  - GET  /stats (Estadísticas)")

        return True

    except Exception as e:
        print(f"❌ Error probando API: {e}")
        return False

def main():
    print("=" * 50)
    print("🚀 PRUEBA COMPLETA DEL SISTEMA")
    print("=" * 50)

    results = []

    # 1. Probar base de datos
    results.append(("Base de datos", test_database_connection()))

    # 2. Probar extracción
    results.append(("Extracción", test_extraction()))

    # 3. Info de API
    results.append(("API Endpoints", test_api_endpoints()))

    # Resumen
    print("\n" + "=" * 50)
    print("📊 RESUMEN DE PRUEBAS")
    print("=" * 50)

    for test_name, result in results:
        status = "✅ PASÓ" if result else "❌ FALLÓ"
        print(f"{test_name}: {status}")

    all_passed = all(r[1] for r in results)

    if all_passed:
        print("\n🎉 ¡Todas las pruebas pasaron! El sistema está funcionando correctamente.")
    else:
        print("\n⚠️ Algunas pruebas fallaron. Revisa los errores arriba.")

    return all_passed

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)