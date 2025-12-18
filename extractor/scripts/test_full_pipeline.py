import sys
import os
import logging
from datetime import datetime

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.database.models import DatabaseManager, Update
from src.utils.vector_manager import VectorManager
from src.extractors.licita_ya_extractor import LicitaYaExtractor

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_pipeline():
    logger.info("Starting Full Pipeline Test")
    
    # 1. Initialize Components
    try:
        db = DatabaseManager()
        vector_manager = VectorManager()
        extractor = LicitaYaExtractor()
        logger.info("✅ Components initialized successfully")
    except Exception as e:
        logger.error(f"❌ Component initialization failed: {e}")
        return

    # 2. Mock Data (Simulating a raw API response)
    mock_id = f"test_pipeline_{int(datetime.now().timestamp())}"
    raw_data = {
        "title": "Licitación de Prueba Pipeline Completo",
        "description": "Esta es una prueba de inserción y vectorización end-to-end.",
        "agency": "Agencia de Pruebas",
        "url": f"https://test.com/{mock_id}",
        "unique_id": mock_id,
        "monto": 12345.67,
        "currency": "MXN",
        "published_date": datetime.now().strftime('%Y-%m-%d'),
        "status": "active",
        "search_keyword": "pruebas"
    }

    try:
        # 3. Test Normalization
        normalized = extractor.normalize_licitacion_data(raw_data)
        logger.info(f"✅ Normalization successful: {normalized.get('titulo')}")

        # 4. Test Metadata Creation
        metadata = extractor.create_metadata(normalized)
        logger.info("✅ Metadata creation successful")

        # 5. Test Semantic Text Creation
        texto_semantico = extractor.create_texto_semantico(normalized, metadata)
        logger.info(f"✅ Semantic text created: {texto_semantico[:50]}...")

        # 6. Test Database Insertion
        update_id = db.save_update(metadata, texto_semantico, 'test_pipeline')
        if update_id:
            logger.info(f"✅ Database insertion successful. ID: {update_id}")
        else:
            logger.error("❌ Database insertion failed (no ID returned)")
            return

        # 7. Test Vectorization
        # Using store_in_vector_db which handles both generation and storage
        embedding = vector_manager.store_in_vector_db({
            'id': metadata.get('id'),
            'titulo': metadata.get('titulo'),
            'texto_semantico': texto_semantico,
            'fuente': 'test_pipeline',
            'monto': metadata.get('importe_drc'),
            'fecha_publicacion': metadata.get('fecha_de_publicacion')
        })

        if embedding:
            logger.info("✅ Vector generated successfully")
            
            # 8. Test Update Embedding in DB
            db.update_embedding(update_id, embedding)
            logger.info("✅ Embedding saved to Database")
        else:
            logger.error("❌ Vector generation failed")

        # 9. Verification Query
        session = db.get_session()
        saved_record = session.query(Update).filter_by(id=update_id).first()
        if saved_record:
            logger.info(f"✅ Record retrieved for verification. ID: {saved_record.id}")
            if saved_record.embedding is not None:
                logger.info("✅ Record has embedding stored in PGVector")
            else:
                logger.error("❌ Record missing embedding in DB")
        session.close()

    except Exception as e:
        logger.error(f"❌ Pipeline test failed with error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_pipeline()
