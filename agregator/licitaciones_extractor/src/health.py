"""
Simple health check endpoint for Railway deployment monitoring.
"""

from flask import Flask, jsonify
from datetime import datetime
import os
import threading
import time
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Health check Flask app
health_app = Flask(__name__)

# Global status tracking
app_status = {
    "status": "starting",
    "last_check": None,
    "scheduler_running": False,
    "database_connected": False,
    "extractors_count": 0,
    "last_extraction": None
}

@health_app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint for Railway."""
    try:
        from src.database.connection import DatabaseConnection

        # Test database connection
        try:
            db_conn = DatabaseConnection()
            with db_conn.get_session() as session:
                session.execute("SELECT 1")
            app_status["database_connected"] = True
        except Exception as e:
            app_status["database_connected"] = False
            app_status["db_error"] = str(e)

        # Update status
        app_status["last_check"] = datetime.now().isoformat()
        app_status["status"] = "healthy" if app_status["database_connected"] else "unhealthy"

        return jsonify({
            "status": app_status["status"],
            "timestamp": app_status["last_check"],
            "database": "connected" if app_status["database_connected"] else "disconnected",
            "scheduler": "running" if app_status.get("scheduler_running", False) else "stopped",
            "environment": os.getenv("ENVIRONMENT", "development"),
            "version": "1.0.0"
        }), 200 if app_status["database_connected"] else 503

    except Exception as e:
        return jsonify({
            "status": "error",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }), 500

@health_app.route('/status', methods=['GET'])
def detailed_status():
    """Detailed status endpoint."""
    try:
        from src.database.connection import DatabaseConnection
        from src.extractors import list_available_extractors

        # Get detailed information
        status_info = {
            "application": "licitaciones-extractor",
            "version": "1.0.0",
            "environment": os.getenv("ENVIRONMENT", "development"),
            "timestamp": datetime.now().isoformat(),
            "uptime": "unknown",  # Could calculate from start time
            "database": {
                "connected": False,
                "url_configured": bool(os.getenv("DATABASE_URL") or os.getenv("POSTGRES_URL"))
            },
            "extractors": {
                "available": [],
                "count": 0
            },
            "configuration": {
                "licita_ya_configured": bool(os.getenv("LICITA_YA_API_KEY")),
                "openai_configured": bool(os.getenv("OPENAI_API_KEY")),
                "extraction_time": os.getenv("EXTRACTION_TIME", "02:00"),
                "timezone": os.getenv("EXTRACTION_TIMEZONE", "America/Mexico_City")
            }
        }

        # Test database
        try:
            db_conn = DatabaseConnection()
            with db_conn.get_session() as session:
                result = session.execute("SELECT COUNT(*) as count FROM updates LIMIT 1")
                status_info["database"]["connected"] = True
                status_info["database"]["records_count"] = "accessible"
        except Exception as e:
            status_info["database"]["connected"] = False
            status_info["database"]["error"] = str(e)

        # Get available extractors
        try:
            extractors = list_available_extractors()
            status_info["extractors"]["available"] = extractors
            status_info["extractors"]["count"] = len(extractors)
        except Exception as e:
            status_info["extractors"]["error"] = str(e)

        return jsonify(status_info), 200

    except Exception as e:
        return jsonify({
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }), 500

def start_health_server(port=8080):
    """Start the health check server."""
    health_app.run(host='0.0.0.0', port=port, debug=False)

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    start_health_server(port)