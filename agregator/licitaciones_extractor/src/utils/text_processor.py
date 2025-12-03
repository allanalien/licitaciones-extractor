"""
Text processor utility for licitaciones extraction system.
"""

import re
from typing import Dict, Any, List, Optional
from datetime import date

try:
    from ..utils.logger import get_logger
    from ..config.keywords import keyword_manager
except ImportError:
    import sys
    from pathlib import Path
    src_path = Path(__file__).parent.parent
    sys.path.insert(0, str(src_path))
    from utils.logger import get_logger
    from config.keywords import keyword_manager


class TextProcessor:
    """
    Utility class for processing and generating semantic text for tenders.

    Handles text cleaning, keyword extraction, and semantic text generation
    optimized for embedding generation and retrieval.
    """

    def __init__(self):
        """Initialize the text processor."""
        self.logger = get_logger("text_processor")
        self.stopwords = self._get_spanish_stopwords()

    def _get_spanish_stopwords(self) -> set:
        """
        Get Spanish stopwords set.

        Returns:
            Set of Spanish stopwords
        """
        return {
            'el', 'la', 'de', 'que', 'y', 'a', 'en', 'un', 'es', 'se', 'no', 'te', 'lo', 'le', 'da', 'su',
            'por', 'son', 'con', 'para', 'como', 'las', 'ha', 'me', 'si', 'sin', 'sobre', 'este', 'ya',
            'entre', 'cuando', 'todo', 'esta', 'ser', 'son', 'dos', 'también', 'fue', 'había', 'era',
            'muy', 'años', 'hasta', 'desde', 'está', 'mi', 'porque', 'qué', 'sólo', 'han', 'yo', 'hay',
            'vez', 'puede', 'todos', 'así', 'nos', 'ni', 'parte', 'tiene', 'él', 'uno', 'donde', 'bien',
            'tiempo', 'mismo', 'ese', 'ahora', 'cada', 'e', 'vida', 'otro', 'después', 'te', 'otros',
            'aunque', 'esa', 'esos', 'esas', 'ante', 'bajo', 'cabe', 'contra', 'durante', 'mediante',
            'salvo', 'según', 'so', 'tras', 'versus', 'vía'
        }

    def create_semantic_text(self, record: Dict[str, Any]) -> str:
        """
        Create semantic text optimized for embeddings and search.

        Args:
            record: Tender record with normalized data

        Returns:
            Semantic text string optimized for vector embeddings
        """
        components = []

        # Title (high weight)
        titulo = record.get('titulo', '').strip()
        if titulo:
            components.append(f"TÍTULO: {self._clean_and_enhance_text(titulo)}")

        # Entity (important for filtering)
        entidad = record.get('entidad', '').strip()
        if entidad:
            components.append(f"ENTIDAD: {entidad}")

        # Description (main content)
        descripcion = record.get('descripcion', '').strip()
        if descripcion:
            enhanced_desc = self._clean_and_enhance_text(descripcion)
            components.append(f"DESCRIPCIÓN: {enhanced_desc}")

        # Tender type (categorical information)
        tipo_licitacion = record.get('tipo_licitacion', '').strip()
        if tipo_licitacion:
            components.append(f"TIPO: {tipo_licitacion}")

        # Geographic context
        estado = record.get('estado', '').strip()
        ciudad = record.get('ciudad', '').strip()

        if estado or ciudad:
            location_parts = [p for p in [ciudad, estado] if p]
            components.append(f"UBICACIÓN: {', '.join(location_parts)}")

        # Amount information (if significant)
        valor_estimado = record.get('valor_estimado')
        if valor_estimado and valor_estimado > 0:
            amount_range = self._categorize_amount(valor_estimado)
            components.append(f"MONTO: {amount_range}")

        # Date information
        fecha_catalogacion = record.get('fecha_catalogacion')
        if fecha_catalogacion and isinstance(fecha_catalogacion, date):
            year = fecha_catalogacion.year
            month_name = self._get_spanish_month_name(fecha_catalogacion.month)
            components.append(f"PERIODO: {month_name} {year}")

        # Keywords enhancement
        all_text = ' '.join([titulo, descripcion])
        relevant_keywords = keyword_manager.get_relevant_keywords(all_text)
        if relevant_keywords:
            components.append(f"CATEGORÍAS: {', '.join(relevant_keywords)}")

        # Join all components
        semantic_text = ' | '.join(components)

        # Final cleaning and optimization
        return self._optimize_for_embeddings(semantic_text)

    def _clean_and_enhance_text(self, text: str) -> str:
        """
        Clean and enhance text for better semantic understanding.

        Args:
            text: Text to clean and enhance

        Returns:
            Enhanced text
        """
        if not text:
            return ""

        # Basic cleaning
        cleaned = text.strip()

        # Remove excessive punctuation
        cleaned = re.sub(r'[.]{3,}', '...', cleaned)
        cleaned = re.sub(r'[-]{2,}', ' - ', cleaned)

        # Normalize spacing
        cleaned = re.sub(r'\s+', ' ', cleaned)

        # Expand common abbreviations
        cleaned = self._expand_abbreviations(cleaned)

        # Enhance with synonyms and related terms
        cleaned = self._enhance_with_synonyms(cleaned)

        return cleaned.strip()

    def _expand_abbreviations(self, text: str) -> str:
        """
        Expand common abbreviations in tender texts.

        Args:
            text: Text with abbreviations

        Returns:
            Text with expanded abbreviations
        """
        abbreviations = {
            r'\bADQUIS\b': 'ADQUISICIÓN',
            r'\bMANTO\b': 'MANTENIMIENTO',
            r'\bSERV\b': 'SERVICIOS',
            r'\bEQUIP\b': 'EQUIPAMIENTO',
            r'\bINFRAEST\b': 'INFRAESTRUCTURA',
            r'\bTECNOL\b': 'TECNOLOGÍA',
            r'\bMED\b': 'MÉDICO MEDICINA',
            r'\bALIM\b': 'ALIMENTARIO ALIMENTOS',
            r'\bCONST\b': 'CONSTRUCCIÓN',
            r'\bESP\b': 'ESPECIALIZADO',
            r'\bGRAL\b': 'GENERAL',
            r'\bPROF\b': 'PROFESIONAL',
            r'\bSUMIN\b': 'SUMINISTRO',
            r'\bMAT\b': 'MATERIAL MATERIALES',
            r'\bSEG\b': 'SEGURIDAD',
            r'\bTRANSP\b': 'TRANSPORTE',
            r'\bEDUC\b': 'EDUCACIÓN EDUCATIVO',
            r'\bCAP\b': 'CAPACITACIÓN',
            r'\bADMIN\b': 'ADMINISTRATIVO ADMINISTRACIÓN',
            r'\bOPER\b': 'OPERATIVO OPERACIÓN',
            r'\bDESAR\b': 'DESARROLLO',
            r'\bIMPL\b': 'IMPLEMENTACIÓN',
            r'\bSIST\b': 'SISTEMA SISTEMAS',
            r'\bINST\b': 'INSTALACIÓN INSTITUTO',
            r'\bREP\b': 'REPARACIÓN',
            r'\bACTUAL\b': 'ACTUALIZACIÓN',
        }

        expanded = text
        for abbr_pattern, expansion in abbreviations.items():
            expanded = re.sub(abbr_pattern, expansion, expanded, flags=re.I)

        return expanded

    def _enhance_with_synonyms(self, text: str) -> str:
        """
        Enhance text with relevant synonyms and related terms.

        Args:
            text: Original text

        Returns:
            Enhanced text with synonyms
        """
        # Common synonym groups for tender contexts
        synonym_groups = {
            'hospital': ['hospitalario', 'clínico', 'médico', 'salud'],
            'escuela': ['educativo', 'académico', 'enseñanza', 'educación'],
            'oficina': ['administrativo', 'gestión', 'administración'],
            'laboratorio': ['análisis', 'pruebas', 'diagnóstico'],
            'seguridad': ['protección', 'vigilancia', 'resguardo'],
            'limpieza': ['aseo', 'higiene', 'sanitización'],
            'alimentos': ['alimentación', 'comida', 'nutrición'],
            'medicamentos': ['fármacos', 'medicina', 'medicinas'],
            'vehículos': ['transporte', 'automotor', 'movilidad'],
            'computadoras': ['informática', 'tecnología', 'sistemas'],
            'construcción': ['edificación', 'obra', 'infraestructura'],
            'capacitación': ['entrenamiento', 'formación', 'adiestramiento'],
            'consultoría': ['asesoría', 'servicios profesionales', 'consulta']
        }

        enhanced = text.lower()
        found_enhancements = []

        for keyword, synonyms in synonym_groups.items():
            if keyword in enhanced:
                found_enhancements.extend(synonyms)

        if found_enhancements:
            # Add the most relevant synonyms (limit to avoid text bloat)
            unique_enhancements = list(set(found_enhancements))[:3]
            return f"{text} {' '.join(unique_enhancements)}"

        return text

    def _categorize_amount(self, amount: float) -> str:
        """
        Categorize amount into ranges for semantic understanding.

        Args:
            amount: Amount value

        Returns:
            Amount category string
        """
        if amount < 10000:
            return "RANGO_BAJO"
        elif amount < 100000:
            return "RANGO_MEDIO_BAJO"
        elif amount < 1000000:
            return "RANGO_MEDIO"
        elif amount < 10000000:
            return "RANGO_MEDIO_ALTO"
        elif amount < 100000000:
            return "RANGO_ALTO"
        else:
            return "RANGO_MUY_ALTO"

    def _get_spanish_month_name(self, month: int) -> str:
        """
        Get Spanish month name.

        Args:
            month: Month number (1-12)

        Returns:
            Spanish month name
        """
        months = [
            'ENERO', 'FEBRERO', 'MARZO', 'ABRIL', 'MAYO', 'JUNIO',
            'JULIO', 'AGOSTO', 'SEPTIEMBRE', 'OCTUBRE', 'NOVIEMBRE', 'DICIEMBRE'
        ]

        if 1 <= month <= 12:
            return months[month - 1]
        return 'MES_DESCONOCIDO'

    def _optimize_for_embeddings(self, text: str) -> str:
        """
        Optimize text for embedding generation.

        Args:
            text: Text to optimize

        Returns:
            Optimized text
        """
        # Ensure reasonable length (embeddings work best with focused content)
        if len(text) > 8000:  # Conservative limit for embedding models
            text = self._truncate_intelligently(text, 8000)

        # Remove redundant information
        text = self._remove_redundancy(text)

        # Final cleanup
        text = re.sub(r'\s+', ' ', text.strip())

        return text

    def _truncate_intelligently(self, text: str, max_length: int) -> str:
        """
        Truncate text intelligently preserving important information.

        Args:
            text: Text to truncate
            max_length: Maximum length

        Returns:
            Truncated text
        """
        if len(text) <= max_length:
            return text

        # Split by sections
        sections = text.split(' | ')

        # Priority order: TÍTULO, ENTIDAD, TIPO, DESCRIPCIÓN, others
        priority_order = ['TÍTULO:', 'ENTIDAD:', 'TIPO:', 'DESCRIPCIÓN:', 'UBICACIÓN:', 'CATEGORÍAS:']

        result_sections = []
        current_length = 0

        # Add high-priority sections first
        for priority in priority_order:
            for section in sections:
                if section.startswith(priority) and section not in result_sections:
                    if current_length + len(section) <= max_length:
                        result_sections.append(section)
                        current_length += len(section) + 3  # Account for ' | '
                    break

        # Add remaining sections if space allows
        for section in sections:
            if section not in result_sections:
                if current_length + len(section) <= max_length:
                    result_sections.append(section)
                    current_length += len(section) + 3
                else:
                    break

        return ' | '.join(result_sections)

    def _remove_redundancy(self, text: str) -> str:
        """
        Remove redundant information from text.

        Args:
            text: Text to clean

        Returns:
            Text with reduced redundancy
        """
        # Split into words and remove duplicates while preserving order
        words = text.split()
        seen = set()
        filtered_words = []

        for word in words:
            # Normalize for comparison
            normalized_word = word.lower().strip('.,;:')

            # Keep if not seen or if it's an important structural word
            if normalized_word not in seen or word in ['|', 'TÍTULO:', 'ENTIDAD:', 'DESCRIPCIÓN:', 'TIPO:', 'UBICACIÓN:']:
                filtered_words.append(word)
                if normalized_word not in ['|', ':']:
                    seen.add(normalized_word)

        return ' '.join(filtered_words)

    def extract_keywords(self, text: str) -> List[str]:
        """
        Extract relevant keywords from text.

        Args:
            text: Text to analyze

        Returns:
            List of extracted keywords
        """
        if not text:
            return []

        # Clean text
        cleaned = re.sub(r'[^\w\s]', ' ', text.lower())
        words = cleaned.split()

        # Filter out stopwords and short words
        keywords = [word for word in words if len(word) > 2 and word not in self.stopwords]

        # Count frequency and get most common
        from collections import Counter
        word_counts = Counter(keywords)

        # Return top keywords
        return [word for word, count in word_counts.most_common(20)]

    def calculate_text_quality(self, text: str) -> Dict[str, float]:
        """
        Calculate quality metrics for text.

        Args:
            text: Text to analyze

        Returns:
            Dictionary with quality metrics
        """
        if not text:
            return {
                'length_score': 0.0,
                'diversity_score': 0.0,
                'informativeness_score': 0.0,
                'overall_quality': 0.0
            }

        # Length score (optimal range: 100-2000 characters)
        text_length = len(text)
        if text_length < 50:
            length_score = text_length / 50
        elif text_length <= 2000:
            length_score = 1.0
        else:
            length_score = max(0.5, 2000 / text_length)

        # Diversity score (based on unique words)
        words = re.findall(r'\w+', text.lower())
        if words:
            unique_words = set(words)
            diversity_score = min(1.0, len(unique_words) / len(words))
        else:
            diversity_score = 0.0

        # Informativeness score (based on non-stopwords)
        if words:
            content_words = [w for w in words if w not in self.stopwords and len(w) > 2]
            informativeness_score = len(content_words) / len(words)
        else:
            informativeness_score = 0.0

        # Overall quality (weighted average)
        overall_quality = (length_score * 0.3 + diversity_score * 0.4 + informativeness_score * 0.3)

        return {
            'length_score': length_score,
            'diversity_score': diversity_score,
            'informativeness_score': informativeness_score,
            'overall_quality': overall_quality
        }

    def validate_semantic_text(self, semantic_text: str) -> List[str]:
        """
        Validate semantic text quality and structure.

        Args:
            semantic_text: Semantic text to validate

        Returns:
            List of validation warnings/errors
        """
        issues = []

        if not semantic_text:
            issues.append("Semantic text is empty")
            return issues

        if len(semantic_text) < 20:
            issues.append("Semantic text is too short")

        if len(semantic_text) > 10000:
            issues.append("Semantic text is too long")

        # Check for required sections
        required_sections = ['TÍTULO:', 'ENTIDAD:']
        for section in required_sections:
            if section not in semantic_text:
                issues.append(f"Missing required section: {section}")

        # Check text quality
        quality_metrics = self.calculate_text_quality(semantic_text)
        if quality_metrics['overall_quality'] < 0.3:
            issues.append("Low text quality score")

        # Check for too much repetition
        words = semantic_text.lower().split()
        if len(words) > 0:
            from collections import Counter
            word_counts = Counter(words)
            most_common_freq = word_counts.most_common(1)[0][1] if word_counts else 0
            if most_common_freq > len(words) * 0.3:
                issues.append("Excessive word repetition detected")

        return issues