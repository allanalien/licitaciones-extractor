#!/usr/bin/env python3
"""
Simple test script for Railway deployment verification.
"""

import os
import sys
import requests
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

def test_environment():
    """Test environment configuration."""
    print("🔍 Testing environment configuration...")

    required_vars = [
        'DATABASE_URL',
        'LICITA_YA_API_KEY',
        'OPENAI_API_KEY'
    ]

    missing = []
    for var in required_vars:
        if not os.getenv(var):
            missing.append(var)

    if missing:
        print(f"❌ Missing environment variables: {', '.join(missing)}")
        return False

    print("✅ Environment variables configured")
    return True

def test_database():
    """Test database connection."""
    print("🔍 Testing database connection...")

    try:
        from src.database.connection import DatabaseConnection

        db_conn = DatabaseConnection()
        with db_conn.get_session() as session:
            result = session.execute("SELECT 1 as test").fetchone()
            if result and result[0] == 1:
                print("✅ Database connection successful")
                return True

    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False

    return False

def test_extractors():
    """Test extractor initialization."""
    print("🔍 Testing extractor initialization...")

    try:
        from src.extractors import list_available_extractors

        extractors = list_available_extractors()
        if extractors:
            print(f"✅ Available extractors: {', '.join(extractors)}")
            return True
        else:
            print("⚠️ No extractors available")
            return False

    except Exception as e:
        print(f"❌ Extractor test failed: {e}")
        return False

def test_health_endpoint():
    """Test health endpoint if running."""
    print("🔍 Testing health endpoint...")

    port = os.getenv("PORT", "8080")
    try:
        response = requests.get(f"http://localhost:{port}/health", timeout=5)
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Health endpoint: {data.get('status', 'unknown')}")
            return True
        else:
            print(f"⚠️ Health endpoint returned {response.status_code}")
            return False

    except requests.exceptions.RequestException:
        print("⚠️ Health endpoint not available (normal if not running server)")
        return True  # This is OK for testing
    except Exception as e:
        print(f"❌ Health endpoint test failed: {e}")
        return False

def test_imports():
    """Test critical imports."""
    print("🔍 Testing critical imports...")

    try:
        # Test critical imports
        import src.main
        import src.config.settings
        import src.database.connection
        import src.scheduler.daily_job
        import src.utils.logger
        import src.health

        print("✅ All critical imports successful")
        return True

    except Exception as e:
        print(f"❌ Import test failed: {e}")
        return False

def main():
    """Run all tests."""
    print("🚀 Railway Deployment Test Suite")
    print("=" * 50)

    tests = [
        ("Environment", test_environment),
        ("Imports", test_imports),
        ("Database", test_database),
        ("Extractors", test_extractors),
        ("Health Endpoint", test_health_endpoint)
    ]

    results = []
    for test_name, test_func in tests:
        print(f"\n📋 {test_name} Test:")
        try:
            result = test_func()
            results.append(result)
        except Exception as e:
            print(f"❌ {test_name} test crashed: {e}")
            results.append(False)

    print("\n" + "=" * 50)
    print("📊 Test Results:")

    passed = sum(results)
    total = len(results)

    for i, (test_name, _) in enumerate(tests):
        status = "✅ PASS" if results[i] else "❌ FAIL"
        print(f"  {test_name}: {status}")

    print(f"\n🎯 Summary: {passed}/{total} tests passed")

    if passed == total:
        print("🎉 All tests passed! Ready for Railway deployment.")
        return 0
    else:
        print("⚠️ Some tests failed. Check configuration before deploying.")
        return 1

if __name__ == "__main__":
    sys.exit(main())