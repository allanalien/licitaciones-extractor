"""
Utilities package for licitaciones extraction system.

This package contains utility classes and functions used across the system.
"""

from .logger import get_logger
from .data_normalizer import DataNormalizer
from .text_processor import TextProcessor

__all__ = [
    'get_logger',
    'DataNormalizer',
    'TextProcessor'
]

# Singleton instances for utilities that can be shared
_data_normalizer = None
_text_processor = None


def get_data_normalizer():
    """
    Get singleton DataNormalizer instance.

    Returns:
        DataNormalizer instance
    """
    global _data_normalizer
    if _data_normalizer is None:
        _data_normalizer = DataNormalizer()
    return _data_normalizer


def get_text_processor():
    """
    Get singleton TextProcessor instance.

    Returns:
        TextProcessor instance
    """
    global _text_processor
    if _text_processor is None:
        _text_processor = TextProcessor()
    return _text_processor