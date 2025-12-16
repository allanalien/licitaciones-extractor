#!/usr/bin/env python3
"""
Aplicación Web para Railway - API REST y Panel de Control
"""

import os
import json
from flask import Flask, jsonify, request, render_template_string
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

app = Flask(__name__)
PORT = int(os.environ.get("PORT", 8080))

DATABASE_URL = os.getenv('DATABASE_URL')

# HTML Template para el panel de control
DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Licitaciones Extractor - Panel de Control</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
        }
        .header {
            background: white;
            padding: 30px;
            border-radius: 15px;
            margin-bottom: 30px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.1);
        }
        h1 {
            color: #333;
            margin-bottom: 10px;
        }
        .stats {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }
        .stat-card {
            background: white;
            padding: 25px;
            border-radius: 10px;
            box-shadow: 0 5px 15px rgba(0,0,0,0.1);
        }
        .stat-number {
            font-size: 36px;
            font-weight: bold;
            color: #667eea;
        }
        .stat-label {
            color: #666;
            margin-top: 5px;
        }
        .recent-list {
            background: white;
            padding: 25px;
            border-radius: 10px;
            box-shadow: 0 5px 15px rgba(0,0,0,0.1);
        }
        .licitacion-item {
            padding: 15px;
            border-bottom: 1px solid #eee;
        }
        .licitacion-item:last-child {
            border-bottom: none;
        }
        .licitacion-title {
            font-weight: 600;
            color: #333;
            margin-bottom: 5px;
        }
        .licitacion-meta {
            color: #666;
            font-size: 14px;
        }
        .badge {
            display: inline-block;
            padding: 3px 8px;
            border-radius: 4px;
            font-size: 12px;
            font-weight: 600;
            margin-right: 10px;
        }
        .badge-tianguis { background: #e3f2fd; color: #1976d2; }
        .badge-licita { background: #fff3e0; color: #f57c00; }
        .badge-compras { background: #f3e5f5; color: #7b1fa2; }
        .actions {
            margin-top: 30px;
            background: white;
            padding: 25px;
            border-radius: 10px;
            text-align: center;
        }
        .btn {
            display: inline-block;
            padding: 12px 30px;
            background: #667eea;
            color: white;
            text-decoration: none;
            border-radius: 5px;
            margin: 0 10px;
            transition: all 0.3s;
        }
        .btn:hover {
            background: #5a67d8;
            transform: translateY(-2px);
        }
        .btn-secondary {
            background: #48bb78;
        }
        .btn-secondary:hover {
            background: #38a169;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🏛️ Licitaciones Extractor</h1>
            <p style="color: #666;">Sistema de extracción automática de licitaciones gubernamentales</p>
        </div>

        <div class="stats">
            <div class="stat-card">
                <div class="stat-number">{{ stats.total }}</div>
                <div class="stat-label">Total Licitaciones</div>
            </div>
            <div class="stat-card">
                <div class="stat-number">{{ stats.today }}</div>
                <div class="stat-label">Agregadas Hoy</div>
            </div>
            <div class="stat-card">
                <div class="stat-number">{{ stats.week }}</div>
                <div class="stat-label">Última Semana</div>
            </div>
            <div class="stat-card">
                <div class="stat-number">{{ stats.sources }}</div>
                <div class="stat-label">Fuentes Activas</div>
            </div>
        </div>

        <div class="recent-list">
            <h2 style="margin-bottom: 20px;">📋 Últimas Licitaciones</h2>
            {% for item in recent %}
            <div class="licitacion-item">
                <div class="licitacion-title">{{ item.titulo[:100] }}...</div>
                <div class="licitacion-meta">
                    <span class="badge badge-{{ item.fuente_class }}">{{ item.fuente }}</span>
                    <span>📅 {{ item.fecha }}</span>
                    <span>🏛️ {{ item.dependencia[:50] }}</span>
                </div>
            </div>
            {% endfor %}
        </div>

        <div class="actions">
            <a href="/api/extract" class="btn">🔄 Ejecutar Extracción Manual</a>
            <a href="/api/stats" class="btn btn-secondary">📊 Ver Estadísticas JSON</a>
        </div>
    </div>
</body>
</html>
"""

def get_db_connection():
    """Obtener conexión a la base de datos"""
    return psycopg2.connect(DATABASE_URL)

@app.route('/')
def dashboard():
    """Panel de control principal"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        # Estadísticas
        cursor.execute("SELECT COUNT(*) as total FROM updates")
        total = cursor.fetchone()['total']

        cursor.execute("""
            SELECT COUNT(*) as today FROM updates
            WHERE DATE(created_at) = CURRENT_DATE
        """)
        today = cursor.fetchone()['today']

        cursor.execute("""
            SELECT COUNT(*) as week FROM updates
            WHERE created_at >= CURRENT_DATE - INTERVAL '7 days'
        """)
        week = cursor.fetchone()['week']

        cursor.execute("""
            SELECT COUNT(DISTINCT metadata->>'fuente') as sources
            FROM updates
        """)
        sources = cursor.fetchone()['sources']

        # Últimas licitaciones
        cursor.execute("""
            SELECT
                metadata->>'titulo' as titulo,
                metadata->>'fuente' as fuente,
                metadata->>'dependencia' as dependencia,
                metadata->>'fecha_publicacion' as fecha,
                created_at
            FROM updates
            ORDER BY created_at DESC
            LIMIT 10
        """)
        recent = cursor.fetchall()

        # Formatear datos
        for item in recent:
            if item['fuente']:
                if 'tianguis' in item['fuente'].lower():
                    item['fuente_class'] = 'tianguis'
                elif 'licita' in item['fuente'].lower():
                    item['fuente_class'] = 'licita'
                else:
                    item['fuente_class'] = 'compras'
            else:
                item['fuente_class'] = 'compras'

            if item['fecha']:
                try:
                    fecha = datetime.fromisoformat(item['fecha'].replace('Z', '+00:00'))
                    item['fecha'] = fecha.strftime('%d/%m/%Y')
                except:
                    item['fecha'] = 'N/A'
            else:
                item['fecha'] = 'N/A'

        cursor.close()
        conn.close()

        stats = {
            'total': total,
            'today': today,
            'week': week,
            'sources': sources
        }

        return render_template_string(DASHBOARD_HTML, stats=stats, recent=recent)

    except Exception as e:
        logger.error(f"Error en dashboard: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/health')
def health():
    """Endpoint de salud para Railway"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        conn.close()
        return jsonify({'status': 'healthy', 'database': 'connected'}), 200
    except:
        return jsonify({'status': 'unhealthy', 'database': 'disconnected'}), 500

@app.route('/api/stats')
def api_stats():
    """API de estadísticas"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        cursor.execute("""
            SELECT
                COUNT(*) as total,
                COUNT(CASE WHEN DATE(created_at) = CURRENT_DATE THEN 1 END) as today,
                COUNT(CASE WHEN created_at >= CURRENT_DATE - INTERVAL '7 days' THEN 1 END) as last_week,
                COUNT(DISTINCT metadata->>'fuente') as sources
            FROM updates
        """)
        stats = cursor.fetchone()

        cursor.execute("""
            SELECT
                metadata->>'fuente' as fuente,
                COUNT(*) as count
            FROM updates
            GROUP BY metadata->>'fuente'
            ORDER BY count DESC
        """)
        by_source = cursor.fetchall()

        cursor.close()
        conn.close()

        return jsonify({
            'status': 'success',
            'stats': stats,
            'by_source': by_source,
            'timestamp': datetime.now().isoformat()
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/search')
def api_search():
    """API de búsqueda"""
    try:
        query = request.args.get('q', '')
        limit = int(request.args.get('limit', 10))

        if not query:
            return jsonify({'error': 'Query parameter q is required'}), 400

        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        cursor.execute("""
            SELECT
                id,
                licitacion_id,
                metadata,
                texto_semantico,
                created_at
            FROM updates
            WHERE
                texto_semantico ILIKE %s OR
                metadata::text ILIKE %s
            ORDER BY created_at DESC
            LIMIT %s
        """, (f'%{query}%', f'%{query}%', limit))

        results = cursor.fetchall()
        cursor.close()
        conn.close()

        return jsonify({
            'status': 'success',
            'query': query,
            'count': len(results),
            'results': results
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/extract', methods=['GET', 'POST'])
def api_extract():
    """Ejecutar extracción manual"""
    return jsonify({
        'status': 'info',
        'message': 'La extracción se ejecuta automáticamente mediante el worker. Use railway run python main_extractor.py para ejecución manual.'
    })

if __name__ == '__main__':
    logger.info(f"Starting web server on port {PORT}")
    app.run(host='0.0.0.0', port=PORT, debug=False)