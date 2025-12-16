from http.server import BaseHTTPRequestHandler
import json
import sys
import os

# Add parent directory to path to import main_extractor
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from main_extractor import LicitacionesAggregator

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        """Handle GET request"""
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

        response = {
            "status": "ok",
            "message": "Licitaciones Extractor API",
            "endpoints": {
                "/": "API status and information",
                "/extract": "POST - Trigger extraction process"
            }
        }

        self.wfile.write(json.dumps(response).encode())

    def do_POST(self):
        """Handle POST request for extraction"""
        if self.path == '/extract':
            try:
                # Initialize aggregator
                aggregator = LicitacionesAggregator()

                # Run extraction
                results = aggregator.run_all_extractors()

                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()

                response = {
                    "status": "success",
                    "message": "Extraction completed",
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
        else:
            self.send_response(404)
            self.send_header('Content-type', 'application/json')
            self.end_headers()

            error_response = {
                "status": "error",
                "message": "Endpoint not found"
            }

            self.wfile.write(json.dumps(error_response).encode())