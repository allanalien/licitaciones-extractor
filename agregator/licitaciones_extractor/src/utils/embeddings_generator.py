"""
Embeddings generator for creating vector representations of licitaciones text.
"""

import openai
import numpy as np
from typing import List, Optional, Union
import time
import hashlib
from functools import lru_cache

from src.config.settings import settings
from src.utils.logger import get_logger


class EmbeddingsGenerator:
    """Generates embeddings using OpenAI's text-embedding-ada-002 model."""

    def __init__(self):
        """Initialize the embeddings generator."""
        self.logger = get_logger("embeddings")
        self.client = openai.OpenAI(api_key=settings.embeddings.openai_api_key)
        self.model = settings.embeddings.model
        self.max_retries = settings.embeddings.max_retries
        self.retry_delay = settings.embeddings.retry_delay_seconds
        self.max_tokens = settings.embeddings.max_tokens

        # Cache for embeddings to avoid redundant API calls
        self._embedding_cache = {}
        self._cache_hits = 0
        self._cache_misses = 0

    def generate_embeddings(self, text: str) -> Optional[List[float]]:
        """
        Generate embeddings for a given text.

        Args:
            text: Input text to generate embeddings for

        Returns:
            List of embedding values or None if generation fails
        """
        if not text or not text.strip():
            self.logger.logger.warning("Empty or None text provided for embedding generation")
            return None

        # Clean and truncate text
        clean_text = self._preprocess_text(text)

        # Check cache first
        text_hash = self._get_text_hash(clean_text)
        if text_hash in self._embedding_cache:
            self._cache_hits += 1
            self.logger.logger.debug(f"Cache hit for text hash: {text_hash[:8]}...")
            return self._embedding_cache[text_hash]

        self._cache_misses += 1

        # Generate embeddings with retry logic
        for attempt in range(self.max_retries):
            try:
                self.logger.logger.debug(f"Generating embeddings (attempt {attempt + 1}/{self.max_retries})")

                response = self.client.embeddings.create(
                    model=self.model,
                    input=clean_text
                )

                embeddings = response.data[0].embedding

                # Cache the result
                self._embedding_cache[text_hash] = embeddings

                self.logger.logger.debug(f"Successfully generated embeddings of dimension: {len(embeddings)}")
                return embeddings

            except openai.RateLimitError as e:
                self.logger.logger.warning(f"Rate limit hit on attempt {attempt + 1}: {e}")
                if attempt < self.max_retries - 1:
                    wait_time = self.retry_delay * (2 ** attempt)  # Exponential backoff
                    self.logger.logger.info(f"Waiting {wait_time}s before retry due to rate limit...")
                    time.sleep(wait_time)
                else:
                    self.logger.logger.error("Rate limit exceeded, max retries reached")

            except openai.APIError as e:
                self.logger.logger.error(f"OpenAI API error on attempt {attempt + 1}: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                else:
                    self.logger.logger.error("API error, max retries reached")

            except Exception as e:
                self.logger.logger.error(f"Unexpected error generating embeddings on attempt {attempt + 1}: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                else:
                    self.logger.logger.error("Unexpected error, max retries reached")

        return None

    def generate_embeddings_batch(self, texts: List[str], batch_size: int = 100) -> List[Optional[List[float]]]:
        """
        Generate embeddings for multiple texts in batches.

        Args:
            texts: List of input texts
            batch_size: Number of texts to process per batch

        Returns:
            List of embedding lists (None for failed generations)
        """
        if not texts:
            return []

        self.logger.logger.info(f"Generating embeddings for {len(texts)} texts in batches of {batch_size}")

        all_embeddings = []

        for i in range(0, len(texts), batch_size):
            batch = texts[i:i + batch_size]
            batch_number = (i // batch_size) + 1
            total_batches = ((len(texts) - 1) // batch_size) + 1

            self.logger.logger.info(f"Processing batch {batch_number}/{total_batches}")

            batch_embeddings = []
            for text in batch:
                embedding = self.generate_embeddings(text)
                batch_embeddings.append(embedding)

            all_embeddings.extend(batch_embeddings)

            # Rate limiting between batches
            if i + batch_size < len(texts):
                time.sleep(1)  # 1 second between batches

        success_count = sum(1 for emb in all_embeddings if emb is not None)
        self.logger.logger.info(f"Successfully generated {success_count}/{len(texts)} embeddings")

        return all_embeddings

    def _preprocess_text(self, text: str) -> str:
        """
        Preprocess text for embedding generation.

        Args:
            text: Raw input text

        Returns:
            Cleaned and truncated text
        """
        # Remove excessive whitespace and newlines
        clean_text = ' '.join(text.split())

        # Truncate if too long (approximate token count)
        # Rough estimation: 1 token ≈ 4 characters for Spanish text
        if len(clean_text) > self.max_tokens * 4:
            clean_text = clean_text[:self.max_tokens * 4]
            self.logger.logger.debug(f"Text truncated to {len(clean_text)} characters")

        return clean_text

    def _get_text_hash(self, text: str) -> str:
        """
        Generate a hash for text to use as cache key.

        Args:
            text: Input text

        Returns:
            SHA256 hash of the text
        """
        return hashlib.sha256(text.encode('utf-8')).hexdigest()

    def get_cache_statistics(self) -> dict:
        """
        Get embedding cache statistics.

        Returns:
            Dictionary with cache statistics
        """
        total_requests = self._cache_hits + self._cache_misses
        hit_rate = self._cache_hits / total_requests if total_requests > 0 else 0

        return {
            'cache_hits': self._cache_hits,
            'cache_misses': self._cache_misses,
            'total_requests': total_requests,
            'hit_rate': hit_rate,
            'cached_items': len(self._embedding_cache)
        }

    def clear_cache(self):
        """Clear the embedding cache."""
        self._embedding_cache.clear()
        self._cache_hits = 0
        self._cache_misses = 0
        self.logger.logger.info("Embedding cache cleared")

    def validate_embeddings(self, embeddings: List[float]) -> bool:
        """
        Validate that embeddings are properly formatted.

        Args:
            embeddings: List of embedding values

        Returns:
            True if valid, False otherwise
        """
        if not embeddings:
            return False

        # Check if all values are finite numbers
        if not all(isinstance(x, (int, float)) and np.isfinite(x) for x in embeddings):
            return False

        # Check expected dimension for text-embedding-ada-002
        expected_dim = settings.embeddings.expected_dimension
        if len(embeddings) != expected_dim:
            self.logger.logger.warning(f"Unexpected embedding dimension: {len(embeddings)}, expected: {expected_dim}")
            return False

        return True

    def cosine_similarity(self, embeddings1: List[float], embeddings2: List[float]) -> float:
        """
        Calculate cosine similarity between two embedding vectors.

        Args:
            embeddings1: First embedding vector
            embeddings2: Second embedding vector

        Returns:
            Cosine similarity score between -1 and 1
        """
        if not self.validate_embeddings(embeddings1) or not self.validate_embeddings(embeddings2):
            raise ValueError("Invalid embeddings provided for similarity calculation")

        # Convert to numpy arrays
        vec1 = np.array(embeddings1)
        vec2 = np.array(embeddings2)

        # Calculate cosine similarity
        dot_product = np.dot(vec1, vec2)
        norms = np.linalg.norm(vec1) * np.linalg.norm(vec2)

        if norms == 0:
            return 0.0

        return float(dot_product / norms)

    def find_most_similar(self, query_embeddings: List[float],
                         candidate_embeddings: List[List[float]],
                         top_k: int = 5) -> List[tuple]:
        """
        Find most similar embeddings to a query.

        Args:
            query_embeddings: Query embedding vector
            candidate_embeddings: List of candidate embedding vectors
            top_k: Number of top similar embeddings to return

        Returns:
            List of (index, similarity_score) tuples, sorted by similarity desc
        """
        if not self.validate_embeddings(query_embeddings):
            raise ValueError("Invalid query embeddings")

        similarities = []
        for i, candidate in enumerate(candidate_embeddings):
            if self.validate_embeddings(candidate):
                similarity = self.cosine_similarity(query_embeddings, candidate)
                similarities.append((i, similarity))

        # Sort by similarity in descending order
        similarities.sort(key=lambda x: x[1], reverse=True)

        return similarities[:top_k]