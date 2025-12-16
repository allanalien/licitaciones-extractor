import openai
import json
import numpy as np
from typing import List, Dict, Any
from sentence_transformers import SentenceTransformer
import chromadb
from chromadb.config import Settings
import os
from loguru import logger

class VectorManager:
    def __init__(self):
        self.openai_api_key = os.getenv('OPENAI_API_KEY')
        if self.openai_api_key:
            openai.api_key = self.openai_api_key

        # Configurar ChromaDB local como respaldo
        try:
            self.chroma_client = chromadb.PersistentClient(path="./vector_db")
            # Cargar modelo local para embeddings
            self.local_model = SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')

            # Crear colección para licitaciones
            try:
                self.collection = self.chroma_client.get_collection("licitaciones")
            except:
                self.collection = self.chroma_client.create_collection("licitaciones")
        except Exception as e:
            logger.warning(f"Error inicializando ChromaDB: {e}. Usando solo embeddings simples.")
            self.chroma_client = None
            self.collection = None
            # Solo cargar modelo local
            self.local_model = SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')

    def generate_embedding(self, text: str, method: str = "openai") -> List[float]:
        """Generar embedding para un texto"""
        try:
            if method == "openai" and self.openai_api_key:
                response = openai.Embedding.create(
                    input=text,
                    model="text-embedding-ada-002"
                )
                return response['data'][0]['embedding']
            else:
                # Usar modelo local (384 dimensiones)
                embedding = self.local_model.encode(text)
                embedding_list = embedding.tolist()

                # Expandir de 384 a 1536 dimensiones repitiendo el vector 4 veces
                expanded_embedding = embedding_list * 4  # 384 * 4 = 1536
                return expanded_embedding
        except Exception as e:
            logger.error(f"Error generando embedding: {e}")
            # Fallback al modelo local con expansión
            embedding = self.local_model.encode(text)
            embedding_list = embedding.tolist()
            expanded_embedding = embedding_list * 4  # 384 * 4 = 1536
            return expanded_embedding

    def prepare_text_for_embedding(self, licitacion_data: Dict[str, Any]) -> str:
        """Preparar texto de licitación para embedding"""
        # Combinar campos relevantes para crear un texto rico
        text_parts = []

        if licitacion_data.get('titulo'):
            text_parts.append(f"Título: {licitacion_data['titulo']}")

        if licitacion_data.get('descripcion'):
            text_parts.append(f"Descripción: {licitacion_data['descripcion']}")

        if licitacion_data.get('entidad'):
            text_parts.append(f"Entidad: {licitacion_data['entidad']}")

        if licitacion_data.get('sector'):
            text_parts.append(f"Sector: {licitacion_data['sector']}")

        if licitacion_data.get('categoria'):
            text_parts.append(f"Categoría: {licitacion_data['categoria']}")

        if licitacion_data.get('palabras_clave'):
            keywords = ", ".join(licitacion_data['palabras_clave']) if isinstance(licitacion_data['palabras_clave'], list) else str(licitacion_data['palabras_clave'])
            text_parts.append(f"Palabras clave: {keywords}")

        return " | ".join(text_parts)

    def store_in_vector_db(self, licitacion_data: Dict[str, Any]) -> str:
        """Almacenar licitación en base de datos vectorial"""
        try:
            # Usar texto_semantico si está disponible, sino preparar texto
            if licitacion_data.get('texto_semantico'):
                text = licitacion_data['texto_semantico']
            else:
                text = self.prepare_text_for_embedding(licitacion_data)

            # Generar embedding
            embedding = self.generate_embedding(text)

            # Preparar metadatos
            metadata = {
                'id': str(licitacion_data.get('id', '')),
                'titulo': licitacion_data.get('titulo', ''),
                'entidad': licitacion_data.get('entidad', ''),
                'fuente': licitacion_data.get('fuente', ''),
                'monto': str(licitacion_data.get('monto', 0)),
                'fecha_publicacion': str(licitacion_data.get('fecha_publicacion', '')),
                'sector': licitacion_data.get('sector', ''),
                'url_original': licitacion_data.get('url_original', '')
            }

            # Generar ID único
            doc_id = f"{licitacion_data.get('fuente', '')}_{licitacion_data.get('id', '')}"

            # Intentar almacenar en ChromaDB si está disponible (sin bloquear si falla)
            try:
                if self.collection:
                    # Usar embedding original de 384 dimensiones para ChromaDB
                    embedding_384 = self.local_model.encode(text).tolist()
                    self.collection.add(
                        embeddings=[embedding_384],
                        documents=[text],
                        metadatas=[metadata],
                        ids=[doc_id]
                    )
                    logger.info(f"Licitación almacenada en ChromaDB: {doc_id}")
            except Exception as chroma_e:
                logger.warning(f"Error almacenando en ChromaDB (ignorando): {chroma_e}")

            logger.info(f"Vector embedding generado para: {doc_id}")
            return embedding

        except Exception as e:
            logger.error(f"Error generando embedding: {e}")
            return None

    def search_similar(self, query: str, n_results: int = 5) -> List[Dict]:
        """Buscar licitaciones similares"""
        try:
            # Generar embedding para la consulta
            query_embedding = self.generate_embedding(query)

            # Buscar en ChromaDB
            results = self.collection.query(
                query_embeddings=[query_embedding],
                n_results=n_results
            )

            return results

        except Exception as e:
            logger.error(f"Error en búsqueda vectorial: {e}")
            return []

    def get_collection_stats(self) -> Dict:
        """Obtener estadísticas de la colección"""
        try:
            if self.collection:
                count = self.collection.count()
                return {
                    'total_documents': count,
                    'collection_name': 'licitaciones'
                }
            else:
                return {
                    'total_documents': 0,
                    'collection_name': 'licitaciones (sin ChromaDB)',
                    'status': 'embeddings_only'
                }
        except Exception as e:
            logger.error(f"Error obteniendo estadísticas: {e}")
            return {}

    def cleanup_old_vectors(self, days_old: int = 30):
        """Limpiar vectores antiguos (implementación futura)"""
        # TODO: Implementar limpieza de vectores antiguos basada en fecha
        pass