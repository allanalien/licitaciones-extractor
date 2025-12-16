from http.server import BaseHTTPRequestHandler
import json
import sys
import os
from datetime import datetime, timedelta

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database.connection import DatabaseConnection

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        """Handle GET request for statistics"""
        try:
            db = DatabaseConnection()
            conn = db.get_connection()
            cursor = conn.cursor()

            # Get total count
            cursor.execute("SELECT COUNT(*) FROM licitaciones")
            total_count = cursor.fetchone()[0]

            # Get count by source
            cursor.execute("""
                SELECT fuente, COUNT(*) as count
                FROM licitaciones
                GROUP BY fuente
                ORDER BY count DESC
            """)
            by_source = {row[0]: row[1] for row in cursor.fetchall()}

            # Get count for last 7 days
            seven_days_ago = datetime.now() - timedelta(days=7)
            cursor.execute("""
                SELECT COUNT(*)
                FROM licitaciones
                WHERE created_at >= %s
            """, (seven_days_ago,))
            recent_count = cursor.fetchone()[0]

            # Get count of active licitaciones (fecha_limite in future)
            cursor.execute("""
                SELECT COUNT(*)
                FROM licitaciones
                WHERE fecha_limite >= CURRENT_DATE
            """)
            active_count = cursor.fetchone()[0]

            cursor.close()
            conn.close()

            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()

            response = {
                "status": "success",
                "stats": {
                    "total_licitaciones": total_count,
                    "active_licitaciones": active_count,
                    "recent_additions": recent_count,
                    "by_source": by_source,
                    "last_updated": datetime.now().isoformat()
                }
            }

            self.wfile.write(json.dumps(response).encode())

        except Exception as e:
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.end_headers()

            error_response = {
                "status": "error",
                "message": str(e)
            }

            self.wfile.write(json.dumps(error_response).encode())