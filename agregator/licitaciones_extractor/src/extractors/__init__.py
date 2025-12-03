"""
Extractors package for licitaciones extraction system.

This package contains all data extractors for different sources.
"""

from .base_extractor import BaseExtractor, ExtractionResult
from .licita_ya_extractor import LicitaYaExtractor
from .cdmx_extractor import CDMXExtractor
from .comprasmx_scraper import ComprasMXScraper

__all__ = [
    'BaseExtractor',
    'ExtractionResult',
    'LicitaYaExtractor',
    'CDMXExtractor',
    'ComprasMXScraper'
]

# Registry of available extractors
AVAILABLE_EXTRACTORS = {
    'licita_ya': LicitaYaExtractor,
    'cdmx': CDMXExtractor,
    'comprasmx': ComprasMXScraper
}


def get_extractor(source_name: str, config: dict = None):
    """
    Factory function to get an extractor instance.

    Args:
        source_name: Name of the source ('licita_ya', 'cdmx', 'comprasmx')
        config: Configuration dictionary for the extractor

    Returns:
        Extractor instance

    Raises:
        ValueError: If source_name is not supported
    """
    if source_name not in AVAILABLE_EXTRACTORS:
        available = ', '.join(AVAILABLE_EXTRACTORS.keys())
        raise ValueError(f"Unknown extractor '{source_name}'. Available extractors: {available}")

    extractor_class = AVAILABLE_EXTRACTORS[source_name]
    return extractor_class(config)


def list_available_extractors():
    """
    Get list of available extractor names.

    Returns:
        List of available extractor names
    """
    return list(AVAILABLE_EXTRACTORS.keys())