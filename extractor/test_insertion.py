#!/usr/bin/env python3
"""
Prueba rápida de inserción en la tabla updates
"""

import os
import sys
from datetime import datetime
from dotenv import load_dotenv

# Agregar src al path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.database.models import DatabaseManager

load_dotenv()

def test_insertion():
    """Probar inserción directa en la tabla updates"""
    db_manager = DatabaseManager()

    # Crear datos de prueba
    metadata = {
        'id': 'test_' + datetime.now().strftime('%Y%m%d%H%M%S'),
        'titulo': 'Licitación de Prueba',
        'descripcion': 'Esta es una licitación de prueba',
        'dependencia': 'Dependencia Test',
        'fecha_publicacion': datetime.now().isoformat(),
        'url': 'https://test.com/licitacion_prueba',
        'fuente': 'Test'
    }

    texto_semantico = """
    Licitación de Prueba
    Esta es una licitación de prueba para verificar la inserción en la base de datos.
    Dependencia: Dependencia Test
    Fecha: {}
    """.format(datetime.now().strftime('%Y-%m-%d'))

    try:
        # Intentar guardar
        update_id = db_manager.save_update(
            metadata=metadata,
            texto_semantico=texto_semantico,
            fuente='Test'
        )

        if update_id:
            print(f"✅ Inserción exitosa! ID: {update_id}")

            # Verificar en la BD
            import psycopg2
            conn = psycopg2.connect(os.getenv('DATABASE_URL'))
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM updates WHERE id = %s", (update_id,))
            exists = cursor.fetchone()[0] > 0

            if exists:
                print("✅ Verificado en la base de datos")
            else:
                print("❌ No se encontró en la base de datos")

            cursor.close()
            conn.close()
        else:
            print("❌ No se pudo insertar")

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    test_insertion()