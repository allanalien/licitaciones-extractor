#!/usr/bin/env python3
"""
Script para configurar la base de datos
"""

import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def setup_database():
    """Crear tablas necesarias en la base de datos"""
    try:
        DATABASE_URL = os.getenv('DATABASE_URL')
        print(f"Conectando a la base de datos...")

        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()

        # Crear tabla de licitaciones
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS licitaciones (
                id SERIAL PRIMARY KEY,
                titulo VARCHAR(500) NOT NULL,
                descripcion TEXT,
                fecha_publicacion DATE,
                fecha_limite DATE,
                dependencia VARCHAR(300),
                url VARCHAR(500) UNIQUE NOT NULL,
                fuente VARCHAR(100) NOT NULL,
                monto DECIMAL(15, 2),
                moneda VARCHAR(10),
                categoria VARCHAR(100),
                ubicacion VARCHAR(200),
                contacto TEXT,
                requisitos TEXT,
                estado VARCHAR(50),
                numero_licitacion VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # Crear índices
        indices = [
            "CREATE INDEX IF NOT EXISTS idx_licitaciones_fecha_publicacion ON licitaciones(fecha_publicacion);",
            "CREATE INDEX IF NOT EXISTS idx_licitaciones_fecha_limite ON licitaciones(fecha_limite);",
            "CREATE INDEX IF NOT EXISTS idx_licitaciones_fuente ON licitaciones(fuente);",
            "CREATE INDEX IF NOT EXISTS idx_licitaciones_categoria ON licitaciones(categoria);",
            "CREATE INDEX IF NOT EXISTS idx_licitaciones_created_at ON licitaciones(created_at);"
        ]

        for idx in indices:
            cursor.execute(idx)

        conn.commit()
        print("✅ Tabla 'licitaciones' creada exitosamente")

        # Verificar
        cursor.execute("SELECT COUNT(*) FROM licitaciones;")
        count = cursor.fetchone()[0]
        print(f"📊 Registros actuales en la tabla: {count}")

        cursor.close()
        conn.close()
        return True

    except Exception as e:
        print(f"❌ Error configurando la base de datos: {e}")
        return False

if __name__ == "__main__":
    setup_database()