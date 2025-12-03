"""
Helper methods for ComprasMX scraper optimization.
"""

import re
from typing import Dict, Any, Optional
from datetime import datetime, date


def extract_entity_from_text(text: str) -> str:
    """
    Extract entity/institution name from text.

    Args:
        text: Text to search in

    Returns:
        Extracted entity name or empty string
    """
    entity_keywords = [
        'secretaría',
        'instituto',
        'comisión',
        'gobierno',
        'municipio',
        'ayuntamiento',
        'semar',
        'pemex',
        'cfe',
        'imss',
        'issste'
    ]

    text_lower = text.lower()
    words = text.split()

    for i, word in enumerate(words):
        word_lower = word.lower()
        for keyword in entity_keywords:
            if keyword in word_lower:
                # Return the word and potentially the next 1-2 words
                entity_parts = words[i:i+3]
                return ' '.join(entity_parts)[:50]

    return ""


def extract_date_from_text(text: str, normalize_func=None):
    """
    Extract date from text using patterns.

    Args:
        text: Text to search
        normalize_func: Function to normalize dates

    Returns:
        Normalized date or None
    """
    date_patterns = [
        r'(\d{1,2}/\d{1,2}/\d{4})',
        r'(\d{4}-\d{1,2}-\d{1,2})',
        r'(\d{1,2}-\d{1,2}-\d{4})',
        r'(\d{1,2}\s+\w+\s+\d{4})'
    ]

    for pattern in date_patterns:
        match = re.search(pattern, text)
        if match:
            date_str = match.group(1)
            if normalize_func:
                return normalize_func(date_str)
            return date_str

    return None


def extract_amount_from_text(text: str, normalize_func=None):
    """
    Extract monetary amount from text.

    Args:
        text: Text to search
        normalize_func: Function to normalize amounts

    Returns:
        Extracted amount or None
    """
    # Look for currency patterns
    amount_patterns = [
        r'\$[\d,]+\.?\d*',
        r'[\d,]+\.?\d*\s*pesos',
        r'[\d,]+\.?\d*\s*mxn',
        r'[\d,]{4,}'
    ]

    for pattern in amount_patterns:
        match = re.search(pattern, text.lower())
        if match:
            amount_str = match.group(0)
            if normalize_func:
                return normalize_func(amount_str)
            return amount_str

    return None


def extract_location_from_text(text: str) -> str:
    """
    Extract location from text.

    Args:
        text: Text to search

    Returns:
        Location name or empty string
    """
    mexican_states = [
        'aguascalientes', 'baja california', 'bcs', 'campeche', 'chiapas',
        'chihuahua', 'cdmx', 'coahuila', 'colima', 'durango', 'guanajuato',
        'guerrero', 'hidalgo', 'jalisco', 'méxico', 'michoacán', 'morelos',
        'nayarit', 'nuevo león', 'oaxaca', 'puebla', 'querétaro',
        'quintana roo', 'san luis potosí', 'sinaloa', 'sonora', 'tabasco',
        'tamaulipas', 'tlaxcala', 'veracruz', 'yucatán', 'zacatecas'
    ]

    text_lower = text.lower()
    for state in mexican_states:
        if state in text_lower:
            return state.title()

    return ""


def infer_tender_type_from_text(text: str) -> str:
    """
    Infer tender type from text content.

    Args:
        text: Text to analyze

    Returns:
        Inferred tender type
    """
    text_lower = text.lower()

    if 'licitación pública' in text_lower:
        return 'Licitación Pública'
    elif 'invitación' in text_lower:
        return 'Invitación Restringida'
    elif 'adjudicación directa' in text_lower:
        return 'Adjudicación Directa'
    elif 'concurso' in text_lower:
        return 'Concurso'
    else:
        return 'Licitación'


def calculate_completeness(fields: Dict[str, Any]) -> float:
    """
    Calculate data completeness score.

    Args:
        fields: Dictionary of fields to check

    Returns:
        Completeness score between 0 and 1
    """
    total_fields = len(fields)
    complete_fields = sum(1 for value in fields.values() if value)
    return complete_fields / total_fields if total_fields > 0 else 0.0


def looks_like_date(text: str) -> bool:
    """
    Check if text looks like a date.

    Args:
        text: Text to check

    Returns:
        True if text appears to be a date
    """
    date_patterns = [
        r'\d{1,2}/\d{1,2}/\d{4}',
        r'\d{4}-\d{1,2}-\d{1,2}',
        r'\d{1,2}-\d{1,2}-\d{4}',
        r'\d{1,2}\s+\w+\s+\d{4}'
    ]

    for pattern in date_patterns:
        if re.search(pattern, text):
            return True
    return False


def looks_like_reference(text: str) -> bool:
    """
    Check if text looks like a reference number.

    Args:
        text: Text to check

    Returns:
        True if text appears to be a reference
    """
    # Check for common reference patterns
    if len(text) < 5 or len(text) > 50:
        return False

    # Must contain both letters and numbers
    has_letters = any(c.isalpha() for c in text)
    has_numbers = any(c.isdigit() for c in text)

    # Common reference patterns
    reference_patterns = [
        r'^[A-Z]{2,4}-\d+',
        r'\d{4}-[A-Z]+',
        r'[A-Z]+\d{6,}',
        r'\w+-\w+-\w+'
    ]

    if has_letters and has_numbers:
        for pattern in reference_patterns:
            if re.search(pattern, text.upper()):
                return True

    return False