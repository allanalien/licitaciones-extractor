#!/usr/bin/env python3
"""
Test script for semantic text generation improvements.
"""

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from src.utils.unified_normalizer import UnifiedNormalizer
from src.scheduler.daily_job import ExtractionOrchestrator

def test_semantic_text_generation():
    """Test the improved semantic text generation."""
    print("🧪 Testing Semantic Text Generation Improvements")
    print("=" * 60)

    # Create test data similar to the problematic examples
    test_records = [
        {
            # Bad example - Tianguis Digital with minimal info
            "source": "cdmx",
            "record": {
                "planning_id": "tianguis_ocds-87sd3t-273654",
                "name": "Sin título especificado",
                "description": "Sin descripción disponible",
                "entity": "ALCALDÍA MIGUEL HIDALGO",
                "planning_date": "2024-12-01"
            },
            "expected_improvement": True
        },
        {
            # Good example - ComprasMX with rich info
            "source": "comprasmx",
            "record": {
                "tender_id": "E-2025-00039990",
                "titulo": "01-24-121 SERVICIOS PROFESIONALES PARA LA ELABORACION DEL AVALÚO",
                "entidad": "INSTITUTO DE ADMINISTRACION Y AVALUOS DE BIENES NACIONALES",
                "descripcion": "ATENDER LA SOLICITUD DE SERVICIOS VALUATORIO, JUSTIPRECIACIONES DE RENTA Y OTROS TRABAJOS VALUATORIO",
                "valor_estimado": 4474.23,
                "fecha_catalogacion": "2025-05-13",
                "tipo_proceso": "ADJUDICACIÓN DIRECTA POR PATENTES, LICENCIAS, OFERENTE ÚNICO U OBRAS DE ARTE",
                "proveedor_ganador": "ANAYA AMOR ARQUITECTOS SA DE CV",
                "url_original": "https://comprasmx.buengobierno.gob.mx/sitiopublico/#/sitiopublico/detalle/fad0d3cf27cc4bf2b2e04f3fa1bdba1b/procedimiento"
            },
            "expected_improvement": False  # Already good
        },
        {
            # Edge case - minimal data
            "source": "cdmx",
            "record": {
                "planning_id": "test-123",
                "entity": "TEST ENTITY",
                "hiring_method_name": "Licitación Pública Internacional"
            },
            "expected_improvement": True
        }
    ]

    # Initialize normalizer
    normalizer = UnifiedNormalizer()

    # Test orchestrator semantic text generation
    orchestrator = ExtractionOrchestrator()

    for i, test_case in enumerate(test_records, 1):
        print(f"\n📋 Test Case {i}: {test_case['source'].upper()}")
        print("-" * 40)

        try:
            # Normalize the record
            normalized = normalizer.normalize_single_record(
                test_case["record"],
                test_case["source"]
            )

            if not normalized:
                print("❌ Record was rejected during normalization")
                continue

            print(f"✅ Normalized Title: {normalized['titulo']}")
            print(f"📄 Description: {normalized['descripcion'][:100]}...")
            print(f"🏛️  Entity: {normalized['entidad']}")

            # Generate semantic text
            semantic_text = orchestrator._generate_semantic_text(normalized)

            print(f"\n🔤 Generated Semantic Text:")
            print("-" * 30)
            print(semantic_text)

            # Analyze quality
            quality_score = analyze_semantic_quality(semantic_text)
            print(f"\n📊 Quality Score: {quality_score}/10")

            if quality_score >= 7:
                print("✅ PASS - Good semantic text quality")
            elif quality_score >= 5:
                print("⚠️ ACCEPTABLE - Average quality")
            else:
                print("❌ FAIL - Poor semantic text quality")

        except Exception as e:
            print(f"❌ ERROR: {e}")

    print("\n" + "=" * 60)
    print("🎯 Test Summary:")
    print("The semantic text should now include:")
    print("- ✅ Meaningful titles (not 'Sin título')")
    print("- ✅ Complete information (institution, type, amount, dates)")
    print("- ✅ Proper formatting and structure")
    print("- ✅ Source URL when available")
    print("- ✅ Fallback handling for missing data")

def analyze_semantic_quality(semantic_text: str) -> int:
    """
    Analyze semantic text quality on a scale of 1-10.

    Args:
        semantic_text: The generated semantic text

    Returns:
        Quality score (1-10)
    """
    if not semantic_text:
        return 0

    score = 0

    # Length check (should be substantial)
    if len(semantic_text) > 100:
        score += 2
    elif len(semantic_text) > 50:
        score += 1

    # Information richness
    info_indicators = [
        "Licitación", "Institución:", "Tipo de procedimiento:",
        "Monto estimado:", "Fecha", "Descripción:", "Estado:", "URL:"
    ]

    present_indicators = sum(1 for indicator in info_indicators if indicator in semantic_text)
    score += min(6, present_indicators)  # Max 6 points for information

    # Quality checks
    if "Sin título" not in semantic_text:
        score += 1

    if "no especificado" not in semantic_text or "especificado" in semantic_text:
        score += 1

    return min(10, score)

def main():
    """Run the semantic text improvement tests."""
    test_semantic_text_generation()

if __name__ == "__main__":
    main()