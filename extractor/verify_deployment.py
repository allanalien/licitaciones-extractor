#!/usr/bin/env python3
"""
Script de verificación para Railway deployment
Verifica que todos los componentes estén funcionando correctamente
"""

import requests
import psycopg2
import os
import sys
from datetime import datetime, timedelta
from dotenv import load_dotenv
from tabulate import tabulate

load_dotenv()

class DeploymentVerifier:
    def __init__(self, base_url=None):
        self.base_url = base_url or os.getenv('RAILWAY_URL', 'http://localhost:8080')
        self.db_url = os.getenv('DATABASE_URL') or os.getenv('NEON_DATABASE_URL')
        self.checks = []

    def check_health_endpoint(self):
        """Verificar endpoint de salud"""
        try:
            response = requests.get(f"{self.base_url}/health", timeout=5)
            if response.status_code == 200:
                self.checks.append(["Health Check", "✅ PASS", response.json()])
            else:
                self.checks.append(["Health Check", "❌ FAIL", f"Status: {response.status_code}"])
        except Exception as e:
            self.checks.append(["Health Check", "❌ FAIL", str(e)])

    def check_database_connection(self):
        """Verificar conexión a base de datos"""
        try:
            conn = psycopg2.connect(self.db_url)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM licitaciones")
            count = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            self.checks.append(["Database", "✅ PASS", f"{count} licitaciones"])
        except Exception as e:
            self.checks.append(["Database", "❌ FAIL", str(e)])

    def check_recent_extractions(self):
        """Verificar extracciones recientes"""
        try:
            conn = psycopg2.connect(self.db_url)
            cursor = conn.cursor()

            # Verificar últimas extracciones
            cursor.execute("""
                SELECT fuente, COUNT(*), MAX(created_at)
                FROM licitaciones
                WHERE created_at > NOW() - INTERVAL '24 hours'
                GROUP BY fuente
            """)

            results = cursor.fetchall()
            cursor.close()
            conn.close()

            if results:
                for fuente, count, last_update in results:
                    time_ago = datetime.now() - last_update
                    hours = time_ago.total_seconds() / 3600
                    self.checks.append([
                        f"Extractor: {fuente}",
                        "✅ ACTIVE",
                        f"{count} nuevas, hace {hours:.1f}h"
                    ])
            else:
                self.checks.append(["Extractors", "⚠️ WARN", "Sin datos en 24h"])

        except Exception as e:
            self.checks.append(["Recent Extractions", "❌ FAIL", str(e)])

    def check_api_endpoints(self):
        """Verificar endpoints principales de la API"""
        endpoints = [
            ('/api/licitaciones', 'Licitaciones API'),
            ('/api/stats', 'Statistics API'),
            ('/api/dashboard', 'Dashboard API')
        ]

        for endpoint, name in endpoints:
            try:
                response = requests.get(f"{self.base_url}{endpoint}", timeout=5)
                if response.status_code == 200:
                    data = response.json()
                    if 'licitaciones' in data:
                        count = len(data['licitaciones'])
                        self.checks.append([name, "✅ PASS", f"{count} items"])
                    else:
                        self.checks.append([name, "✅ PASS", "OK"])
                else:
                    self.checks.append([name, "⚠️ WARN", f"Status: {response.status_code}"])
            except Exception as e:
                self.checks.append([name, "❌ FAIL", str(e)])

    def check_worker_status(self):
        """Verificar estado del worker"""
        try:
            response = requests.get(f"{self.base_url}/api/worker/status", timeout=5)
            if response.status_code == 200:
                data = response.json()
                self.checks.append(["Worker", "✅ RUNNING", data.get('status', 'Unknown')])
            else:
                self.checks.append(["Worker", "⚠️ UNKNOWN", "No status endpoint"])
        except:
            self.checks.append(["Worker", "⚠️ UNKNOWN", "Status endpoint not available"])

    def run_all_checks(self):
        """Ejecutar todas las verificaciones"""
        print("🔍 Verificando deployment en Railway...")
        print(f"📍 URL: {self.base_url}")
        print("=" * 60)

        self.check_health_endpoint()
        self.check_database_connection()
        self.check_recent_extractions()
        self.check_api_endpoints()
        self.check_worker_status()

        # Mostrar resultados
        print("\n📊 RESULTADOS DE VERIFICACIÓN:")
        print(tabulate(self.checks, headers=["Component", "Status", "Details"], tablefmt="grid"))

        # Resumen
        failed = sum(1 for check in self.checks if "FAIL" in check[1])
        warned = sum(1 for check in self.checks if "WARN" in check[1])
        passed = sum(1 for check in self.checks if "PASS" in check[1] or "RUNNING" in check[1] or "ACTIVE" in check[1])

        print("\n📈 RESUMEN:")
        print(f"  ✅ Passed: {passed}")
        print(f"  ⚠️  Warnings: {warned}")
        print(f"  ❌ Failed: {failed}")

        if failed == 0:
            print("\n🎉 ¡Deployment funcionando correctamente!")
            return 0
        else:
            print("\n⚠️ Hay problemas que requieren atención")
            return 1

if __name__ == "__main__":
    # Obtener URL de Railway si está disponible
    railway_url = sys.argv[1] if len(sys.argv) > 1 else None

    verifier = DeploymentVerifier(railway_url)
    exit_code = verifier.run_all_checks()
    sys.exit(exit_code)