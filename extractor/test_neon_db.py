#!/usr/bin/env python3
"""
Script para verificar la base de datos Neon con la tabla 'updates'
"""

import os
import psycopg2
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

def check_neon_database():
    """Verificar la estructura de la tabla 'updates' en Neon"""
    try:
        DATABASE_URL = os.getenv('DATABASE_URL')
        print("🔍 Conectando a Neon Database...")

        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()

        # Ver todas las tablas
        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)
        tables = cursor.fetchall()
        print(f"\n📋 Tablas en la base de datos:")
        for table in tables:
            print(f"  - {table[0]}")

        # Verificar si existe la tabla updates
        cursor.execute("""
            SELECT column_name, data_type, character_maximum_length
            FROM information_schema.columns
            WHERE table_name = 'updates'
            ORDER BY ordinal_position;
        """)

        columns = cursor.fetchall()
        if columns:
            print(f"\n✅ Tabla 'updates' encontrada con las siguientes columnas:")
            for col in columns:
                print(f"  - {col[0]}: {col[1]} ({col[2] if col[2] else 'N/A'})")
        else:
            print("\n⚠️ La tabla 'updates' no existe")

            # Verificar si existe licitaciones
            cursor.execute("""
                SELECT column_name, data_type, character_maximum_length
                FROM information_schema.columns
                WHERE table_name = 'licitaciones'
                ORDER BY ordinal_position;
            """)

            columns = cursor.fetchall()
            if columns:
                print(f"\n✅ Tabla 'licitaciones' encontrada con las siguientes columnas:")
                for col in columns:
                    print(f"  - {col[0]}: {col[1]} ({col[2] if col[2] else 'N/A'})")

        # Contar registros en updates
        try:
            cursor.execute("SELECT COUNT(*) FROM updates;")
            count = cursor.fetchone()[0]
            print(f"\n📊 Total de registros en 'updates': {count}")

            # Mostrar últimos registros
            cursor.execute("""
                SELECT titulo, fuente, fecha_publicacion
                FROM updates
                ORDER BY created_at DESC
                LIMIT 3;
            """)
            recent = cursor.fetchall()
            if recent:
                print("\n📋 Últimos 3 registros:")
                for row in recent:
                    print(f"  - {row[0][:50]}... ({row[1]}) - {row[2]}")
        except Exception as e:
            print(f"  ℹ️ No se pudo leer la tabla 'updates': {e}")

            # Intentar con licitaciones
            try:
                cursor.execute("SELECT COUNT(*) FROM licitaciones;")
                count = cursor.fetchone()[0]
                print(f"\n📊 Total de registros en 'licitaciones': {count}")
            except:
                pass

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    check_neon_database()