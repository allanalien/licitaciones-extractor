"""
Keywords configuration for licitaciones extractor.
"""

from typing import List, Dict, Set

# Corporate keywords prioritarios según el plan
CORPORATE_KEYWORDS = [
    "alimentos",
    "medicinas",
    "obra publica",
    "equipo tecnologico",
    "servicios profesionales",
    "construccion",
    "salud",
    "educacion",
    "seguridad",
    "transporte"
]

# Extended keywords by category for better search coverage
KEYWORD_CATEGORIES = {
    "alimentos": [
        "alimentos",
        "alimentacion",
        "catering",
        "comida",
        "desayunos",
        "suministros alimentarios",
        "servicios alimentarios",
        "despensa",
        "viveres",
        "productos alimenticios"
    ],
    "medicinas": [
        "medicinas",
        "medicamentos",
        "farmacos",
        "equipo medico",
        "instrumental medico",
        "dispositivos medicos",
        "material de curacion",
        "insumos medicos",
        "reactivos",
        "vacunas"
    ],
    "obra_publica": [
        "obra publica",
        "infraestructura",
        "construccion publica",
        "obras civiles",
        "carreteras",
        "puentes",
        "edificios publicos",
        "urbanizacion",
        "pavimentacion",
        "drenaje"
    ],
    "equipo_tecnologico": [
        "equipo tecnologico",
        "hardware",
        "software",
        "computadoras",
        "servidores",
        "tecnologia",
        "sistemas informaticos",
        "equipos de computo",
        "telecomunicaciones",
        "redes"
    ],
    "servicios_profesionales": [
        "servicios profesionales",
        "consultoria",
        "asesoria",
        "capacitacion",
        "auditoria",
        "estudios",
        "proyectos",
        "diseño",
        "planeacion",
        "supervision"
    ],
    "construccion": [
        "construccion",
        "edificacion",
        "obra civil",
        "arquitectura",
        "ingenieria",
        "remodelacion",
        "rehabilitacion",
        "mantenimiento",
        "acabados",
        "instalaciones"
    ],
    "salud": [
        "salud",
        "servicios medicos",
        "atencion medica",
        "hospitales",
        "clinicas",
        "servicios hospitalarios",
        "laboratorio",
        "diagnostico",
        "cirugia",
        "emergencias"
    ],
    "educacion": [
        "educacion",
        "servicios educativos",
        "capacitacion",
        "formacion",
        "entrenamiento",
        "cursos",
        "talleres",
        "material educativo",
        "mobiliario escolar",
        "equipamiento educativo"
    ],
    "seguridad": [
        "seguridad",
        "vigilancia",
        "servicios de seguridad",
        "equipos de seguridad",
        "monitoreo",
        "proteccion",
        "resguardo",
        "custodio",
        "sistemas de seguridad",
        "video vigilancia"
    ],
    "transporte": [
        "transporte",
        "vehiculos",
        "servicios logisticos",
        "traslado",
        "movilidad",
        "flotillas",
        "combustible",
        "refacciones",
        "mantenimiento vehicular",
        "servicios de transporte"
    ]
}

# Keywords for filtering out irrelevant results
EXCLUSION_KEYWORDS = [
    "cancelado",
    "suspendido",
    "desierto",
    "revocado",
    "anulado",
    "vencido"
]

# Geographic keywords for Mexico
MEXICAN_STATES = [
    "aguascalientes", "baja california", "baja california sur", "campeche",
    "chiapas", "chihuahua", "coahuila", "colima", "durango", "guanajuato",
    "guerrero", "hidalgo", "jalisco", "mexico", "michoacan", "morelos",
    "nayarit", "nuevo leon", "oaxaca", "puebla", "queretaro", "quintana roo",
    "san luis potosi", "sinaloa", "sonora", "tabasco", "tamaulipas",
    "tlaxcala", "veracruz", "yucatan", "zacatecas", "ciudad de mexico", "cdmx"
]

# Major Mexican cities
MAJOR_CITIES = [
    "mexico", "guadalajara", "monterrey", "puebla", "tijuana", "leon",
    "juarez", "torreon", "queretaro", "merida", "mexicali", "aguascalientes",
    "cuernavaca", "saltillo", "hermosillo", "culiacan", "chihuahua",
    "san luis potosi", "toluca", "cancun", "veracruz", "villahermosa",
    "tuxtla gutierrez", "tepic", "durango", "morelia", "xalapa"
]

class KeywordManager:
    """Manages keyword operations for extraction."""

    def __init__(self):
        """Initialize keyword manager."""
        self.corporate_keywords = CORPORATE_KEYWORDS
        self.keyword_categories = KEYWORD_CATEGORIES
        self.exclusion_keywords = EXCLUSION_KEYWORDS

    def get_primary_keywords(self) -> List[str]:
        """
        Get primary corporate keywords for extraction.

        Returns:
            List of primary keywords
        """
        return self.corporate_keywords.copy()

    def get_expanded_keywords(self, category: str = None) -> List[str]:
        """
        Get expanded keywords for a specific category or all categories.

        Args:
            category: Specific category to get keywords for

        Returns:
            List of expanded keywords
        """
        if category and category in self.keyword_categories:
            return self.keyword_categories[category].copy()

        # Return all expanded keywords
        all_keywords = []
        for keywords in self.keyword_categories.values():
            all_keywords.extend(keywords)

        return list(set(all_keywords))  # Remove duplicates

    def get_category_for_keyword(self, keyword: str) -> str:
        """
        Get category for a given keyword.

        Args:
            keyword: Keyword to find category for

        Returns:
            Category name or 'unknown' if not found
        """
        for category, keywords in self.keyword_categories.items():
            if keyword.lower() in [k.lower() for k in keywords]:
                return category
        return "unknown"

    def should_exclude(self, text: str) -> bool:
        """
        Check if text contains exclusion keywords.

        Args:
            text: Text to check

        Returns:
            True if text should be excluded
        """
        text_lower = text.lower()
        return any(exclusion in text_lower for exclusion in self.exclusion_keywords)

    def get_relevant_keywords(self, text: str, threshold: int = 1) -> List[str]:
        """
        Get relevant keywords found in text.

        Args:
            text: Text to analyze
            threshold: Minimum number of matches required

        Returns:
            List of relevant keywords found
        """
        text_lower = text.lower()
        found_keywords = []

        for keyword in self.get_expanded_keywords():
            if keyword.lower() in text_lower:
                found_keywords.append(keyword)

        return found_keywords if len(found_keywords) >= threshold else []

    def get_geographic_context(self, text: str) -> Dict[str, List[str]]:
        """
        Extract geographic context from text.

        Args:
            text: Text to analyze

        Returns:
            Dictionary with states and cities found
        """
        text_lower = text.lower()

        found_states = [state for state in MEXICAN_STATES if state in text_lower]
        found_cities = [city for city in MAJOR_CITIES if city in text_lower]

        return {
            "states": found_states,
            "cities": found_cities
        }

    def prioritize_keywords(self, keywords: List[str]) -> List[str]:
        """
        Prioritize keywords based on corporate importance.

        Args:
            keywords: List of keywords to prioritize

        Returns:
            Prioritized list of keywords
        """
        prioritized = []

        # First, add corporate keywords in order
        for corp_keyword in self.corporate_keywords:
            if corp_keyword in keywords:
                prioritized.append(corp_keyword)

        # Then add remaining keywords
        for keyword in keywords:
            if keyword not in prioritized:
                prioritized.append(keyword)

        return prioritized

# Global keyword manager instance
keyword_manager = KeywordManager()