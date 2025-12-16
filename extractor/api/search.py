from http.server import BaseHTTPRequestHandler
import json
import sys
import os
from urllib.parse import parse_qs, urlparse

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database.connection import DatabaseConnection

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        """Handle GET request for searching licitaciones"""
        parsed_path = urlparse(self.path)
        query_params = parse_qs(parsed_path.query)

        try:
            keyword = query_params.get('q', [''])[0]
            limit = int(query_params.get('limit', ['10'])[0])

            if not keyword:
                self.send_response(400)
                self.send_header('Content-type', 'application/json')
                self.end_headers()

                response = {
                    "status": "error",
                    "message": "Query parameter 'q' is required"
                }

                self.wfile.write(json.dumps(response).encode())
                return

            # Search in database
            db = DatabaseConnection()
            conn = db.get_connection()
            cursor = conn.cursor()

            query = """
                SELECT id, titulo, descripcion, fecha_publicacion, fecha_limite,
                       dependencia, url, fuente, created_at
                FROM licitaciones
                WHERE titulo ILIKE %s OR descripcion ILIKE %s
                ORDER BY fecha_publicacion DESC
                LIMIT %s
            """

            search_term = f"%{keyword}%"
            cursor.execute(query, (search_term, search_term, limit))

            results = []
            for row in cursor.fetchall():
                results.append({
                    "id": row[0],
                    "titulo": row[1],
                    "descripcion": row[2],
                    "fecha_publicacion": str(row[3]) if row[3] else None,
                    "fecha_limite": str(row[4]) if row[4] else None,
                    "dependencia": row[5],
                    "url": row[6],
                    "fuente": row[7],
                    "created_at": str(row[8])
                })

            cursor.close()
            conn.close()

            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()

            response = {
                "status": "success",
                "count": len(results),
                "results": results
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