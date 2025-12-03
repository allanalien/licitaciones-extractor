"""
Dashboard system for real-time monitoring of tender extraction system
"""

import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any
from flask import Flask, render_template_string, jsonify, request
from flask_cors import CORS
import plotly.graph_objs as go
import plotly.utils

from .metrics_collector import MetricsCollector
from .data_quality import DataQualityAnalyzer
from .performance_monitor import PerformanceMonitor
from ..database.connection import DatabaseConnection
from ..utils.logger import get_logger


class Dashboard:
    """Web-based dashboard for monitoring system metrics"""

    def __init__(self, host: str = "127.0.0.1", port: int = 5000):
        self.app = Flask(__name__)
        CORS(self.app)
        self.host = host
        self.port = port
        self.logger = get_logger(self.__class__.__name__)

        # Initialize monitoring components
        self.db_connection = DatabaseConnection()
        self.metrics_collector = MetricsCollector(self.db_connection)
        self.quality_analyzer = DataQualityAnalyzer(self.db_connection)
        self.performance_monitor = PerformanceMonitor()

        # Setup routes
        self._setup_routes()

    def _setup_routes(self):
        """Setup Flask routes for the dashboard"""

        @self.app.route('/')
        def index():
            """Main dashboard page"""
            return render_template_string(DASHBOARD_TEMPLATE)

        @self.app.route('/api/metrics/overview')
        def metrics_overview():
            """Get overview metrics"""
            try:
                metrics = self.metrics_collector.collect_all_metrics()
                return jsonify(metrics)
            except Exception as e:
                self.logger.error(f"Error getting overview metrics: {e}")
                return jsonify({"error": str(e)}), 500

        @self.app.route('/api/metrics/extraction')
        def extraction_metrics():
            """Get extraction metrics"""
            try:
                source = request.args.get('source')
                metrics = self.metrics_collector.collect_extraction_metrics(source)
                return jsonify(metrics)
            except Exception as e:
                self.logger.error(f"Error getting extraction metrics: {e}")
                return jsonify({"error": str(e)}), 500

        @self.app.route('/api/metrics/performance')
        def performance_metrics():
            """Get performance metrics"""
            try:
                metrics = self.metrics_collector.collect_performance_metrics()
                return jsonify(metrics)
            except Exception as e:
                self.logger.error(f"Error getting performance metrics: {e}")
                return jsonify({"error": str(e)}), 500

        @self.app.route('/api/metrics/quality')
        def quality_metrics():
            """Get data quality metrics"""
            try:
                completeness = self.quality_analyzer.analyze_completeness()
                duplicates = self.quality_analyzer.analyze_duplicates()
                freshness = self.quality_analyzer.analyze_data_freshness()

                return jsonify({
                    "completeness": completeness,
                    "duplicates": duplicates,
                    "freshness": freshness
                })
            except Exception as e:
                self.logger.error(f"Error getting quality metrics: {e}")
                return jsonify({"error": str(e)}), 500

        @self.app.route('/api/charts/extraction-trend')
        def extraction_trend():
            """Get extraction trend chart data"""
            try:
                chart_data = self._generate_extraction_trend_chart()
                return jsonify(chart_data)
            except Exception as e:
                self.logger.error(f"Error generating extraction trend: {e}")
                return jsonify({"error": str(e)}), 500

        @self.app.route('/api/charts/quality-overview')
        def quality_overview():
            """Get quality overview chart data"""
            try:
                chart_data = self._generate_quality_overview_chart()
                return jsonify(chart_data)
            except Exception as e:
                self.logger.error(f"Error generating quality overview: {e}")
                return jsonify({"error": str(e)}), 500

        @self.app.route('/api/charts/performance-history')
        def performance_history():
            """Get performance history chart data"""
            try:
                chart_data = self._generate_performance_history_chart()
                return jsonify(chart_data)
            except Exception as e:
                self.logger.error(f"Error generating performance history: {e}")
                return jsonify({"error": str(e)}), 500

        @self.app.route('/api/reports/quality')
        def quality_report():
            """Get quality report"""
            try:
                report = self.quality_analyzer.generate_quality_report()
                return jsonify({"report": report})
            except Exception as e:
                self.logger.error(f"Error generating quality report: {e}")
                return jsonify({"error": str(e)}), 500

        @self.app.route('/api/health')
        def health_check():
            """Health check endpoint"""
            try:
                # Check database connection
                with self.db_connection.get_session() as session:
                    session.execute("SELECT 1")

                return jsonify({
                    "status": "healthy",
                    "timestamp": datetime.now().isoformat()
                })
            except Exception as e:
                return jsonify({
                    "status": "unhealthy",
                    "error": str(e),
                    "timestamp": datetime.now().isoformat()
                }), 503

    def _generate_extraction_trend_chart(self) -> Dict:
        """Generate extraction trend chart data"""
        try:
            # Get extraction data for last 30 days
            from sqlalchemy import text

            with self.db_connection.get_session() as session:
                query = """
                    SELECT
                        DATE(fecha_extraccion) as date,
                        fuente,
                        COUNT(*) as count
                    FROM updates
                    WHERE fecha_extraccion >= CURRENT_DATE - INTERVAL '30 days'
                    GROUP BY DATE(fecha_extraccion), fuente
                    ORDER BY date, fuente
                """

                result = session.execute(text(query))

                # Organize data by source
                data_by_source = {}
                for row in result:
                    date = str(row[0])
                    source = row[1]
                    count = row[2]

                    if source not in data_by_source:
                        data_by_source[source] = {"dates": [], "counts": []}

                    data_by_source[source]["dates"].append(date)
                    data_by_source[source]["counts"].append(count)

                # Create Plotly traces
                traces = []
                for source, data in data_by_source.items():
                    trace = go.Scatter(
                        x=data["dates"],
                        y=data["counts"],
                        mode='lines+markers',
                        name=source.upper(),
                        hovertemplate='%{y} records<br>%{x}'
                    )
                    traces.append(trace)

                # Create layout
                layout = go.Layout(
                    title='Extraction Trend (Last 30 Days)',
                    xaxis=dict(title='Date'),
                    yaxis=dict(title='Records Extracted'),
                    hovermode='x unified'
                )

                # Convert to JSON
                fig = go.Figure(data=traces, layout=layout)
                return json.loads(plotly.utils.PlotlyJSONEncoder().encode(fig))

        except Exception as e:
            self.logger.error(f"Error generating extraction trend chart: {e}")
            return {}

    def _generate_quality_overview_chart(self) -> Dict:
        """Generate quality overview chart data"""
        try:
            # Get quality metrics
            completeness = self.quality_analyzer.analyze_completeness()

            sources = []
            completeness_values = []
            colors = []

            for source, metrics in completeness.items():
                sources.append(source.upper())
                overall = metrics.get('overall_completeness', 0)
                completeness_values.append(overall)

                # Color based on quality level
                if overall >= 95:
                    colors.append('green')
                elif overall >= 80:
                    colors.append('yellow')
                else:
                    colors.append('red')

            # Create bar chart
            trace = go.Bar(
                x=sources,
                y=completeness_values,
                marker=dict(color=colors),
                text=[f'{v:.1f}%' for v in completeness_values],
                textposition='auto',
                hovertemplate='%{x}<br>Completeness: %{y:.1f}%'
            )

            layout = go.Layout(
                title='Data Quality Overview by Source',
                xaxis=dict(title='Source'),
                yaxis=dict(title='Overall Completeness (%)', range=[0, 100]),
                showlegend=False
            )

            fig = go.Figure(data=[trace], layout=layout)
            return json.loads(plotly.utils.PlotlyJSONEncoder().encode(fig))

        except Exception as e:
            self.logger.error(f"Error generating quality overview chart: {e}")
            return {}

    def _generate_performance_history_chart(self) -> Dict:
        """Generate performance history chart data"""
        try:
            # Load metrics history
            metrics_file = Path("logs/metrics.json")
            if not metrics_file.exists():
                return {}

            with open(metrics_file, 'r') as f:
                metrics_history = json.load(f)

            # Extract performance data
            timestamps = []
            cpu_usage = []
            memory_usage = []
            db_response = []

            for metric in metrics_history[-48:]:  # Last 48 hours of data
                perf = metric.get('performance_metrics', {})
                if perf:
                    timestamps.append(perf.get('timestamp', ''))
                    cpu_usage.append(perf.get('cpu_usage', 0))
                    memory_usage.append(perf.get('memory_usage', 0))
                    db_response.append(perf.get('db_response_time_ms', 0))

            # Create traces
            traces = [
                go.Scatter(
                    x=timestamps,
                    y=cpu_usage,
                    mode='lines',
                    name='CPU Usage (%)',
                    yaxis='y'
                ),
                go.Scatter(
                    x=timestamps,
                    y=memory_usage,
                    mode='lines',
                    name='Memory Usage (%)',
                    yaxis='y'
                ),
                go.Scatter(
                    x=timestamps,
                    y=db_response,
                    mode='lines',
                    name='DB Response (ms)',
                    yaxis='y2'
                )
            ]

            layout = go.Layout(
                title='System Performance History',
                xaxis=dict(title='Time'),
                yaxis=dict(title='Usage (%)', side='left', range=[0, 100]),
                yaxis2=dict(title='Response Time (ms)', side='right', overlaying='y'),
                hovermode='x unified'
            )

            fig = go.Figure(data=traces, layout=layout)
            return json.loads(plotly.utils.PlotlyJSONEncoder().encode(fig))

        except Exception as e:
            self.logger.error(f"Error generating performance history chart: {e}")
            return {}

    def run(self):
        """Start the dashboard server"""
        self.logger.info(f"Starting dashboard on http://{self.host}:{self.port}")
        self.app.run(host=self.host, port=self.port, debug=False)


# HTML template for the dashboard
DASHBOARD_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Tender Extraction System - Monitoring Dashboard</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <script src="https://code.jquery.com/jquery-3.6.0.min.js"></script>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }
        .container {
            max-width: 1400px;
            margin: 0 auto;
        }
        h1 {
            color: white;
            text-align: center;
            margin-bottom: 30px;
            font-size: 2.5rem;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.1);
        }
        .metrics-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }
        .metric-card {
            background: white;
            border-radius: 10px;
            padding: 20px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
            transition: transform 0.3s ease;
        }
        .metric-card:hover {
            transform: translateY(-5px);
            box-shadow: 0 6px 12px rgba(0,0,0,0.15);
        }
        .metric-title {
            font-size: 0.9rem;
            color: #718096;
            text-transform: uppercase;
            letter-spacing: 1px;
            margin-bottom: 10px;
        }
        .metric-value {
            font-size: 2rem;
            font-weight: bold;
            color: #2d3748;
        }
        .metric-change {
            font-size: 0.85rem;
            margin-top: 5px;
        }
        .metric-change.positive { color: #48bb78; }
        .metric-change.negative { color: #f56565; }
        .chart-container {
            background: white;
            border-radius: 10px;
            padding: 20px;
            margin-bottom: 20px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }
        .refresh-btn {
            position: fixed;
            bottom: 30px;
            right: 30px;
            background: #667eea;
            color: white;
            border: none;
            border-radius: 50%;
            width: 60px;
            height: 60px;
            font-size: 1.5rem;
            cursor: pointer;
            box-shadow: 0 4px 12px rgba(102, 126, 234, 0.4);
            transition: all 0.3s ease;
        }
        .refresh-btn:hover {
            background: #764ba2;
            transform: rotate(180deg);
        }
        .status-indicator {
            display: inline-block;
            width: 10px;
            height: 10px;
            border-radius: 50%;
            margin-right: 5px;
        }
        .status-healthy { background: #48bb78; }
        .status-warning { background: #f6e05e; }
        .status-critical { background: #f56565; }
        .loading {
            text-align: center;
            color: white;
            padding: 20px;
        }
        .error {
            background: #f56565;
            color: white;
            padding: 15px;
            border-radius: 10px;
            margin-bottom: 20px;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>🎯 Tender Extraction Monitoring Dashboard</h1>

        <div class="metrics-grid" id="metrics-overview">
            <div class="loading">Loading metrics...</div>
        </div>

        <div class="chart-container">
            <div id="extraction-trend-chart"></div>
        </div>

        <div class="chart-container">
            <div id="quality-overview-chart"></div>
        </div>

        <div class="chart-container">
            <div id="performance-history-chart"></div>
        </div>

        <button class="refresh-btn" onclick="refreshDashboard()">↻</button>
    </div>

    <script>
        // Auto-refresh every 30 seconds
        setInterval(refreshDashboard, 30000);

        // Initial load
        $(document).ready(function() {
            refreshDashboard();
        });

        function refreshDashboard() {
            loadMetricsOverview();
            loadExtractionTrend();
            loadQualityOverview();
            loadPerformanceHistory();
        }

        function loadMetricsOverview() {
            $.get('/api/metrics/overview', function(data) {
                let html = '';

                // Extract key metrics
                const extraction = data.extraction_metrics || {};
                const performance = data.performance_metrics || {};
                const errors = data.error_metrics || {};
                const quality = data.data_quality_metrics || {};

                // Calculate totals
                let totalRecords = 0;
                let todayRecords = 0;
                for (let source in extraction) {
                    totalRecords += extraction[source].total_records || 0;
                    todayRecords += extraction[source].today_count || 0;
                }

                // Create metric cards
                html += createMetricCard('Total Records', totalRecords.toLocaleString(), 'records');
                html += createMetricCard('Today\\'s Records', todayRecords.toLocaleString(), 'new');
                html += createMetricCard('CPU Usage', (performance.cpu_usage || 0).toFixed(1) + '%',
                    performance.cpu_usage > 80 ? 'critical' : 'healthy');
                html += createMetricCard('Memory Usage', (performance.memory_usage || 0).toFixed(1) + '%',
                    performance.memory_usage > 80 ? 'critical' : 'healthy');
                html += createMetricCard('DB Response', (performance.db_response_time_ms || 0) + 'ms',
                    performance.db_response_time_ms > 1000 ? 'warning' : 'healthy');
                html += createMetricCard('Error Rate', (errors.error_rate || 0).toFixed(2) + '%',
                    errors.error_rate > 5 ? 'critical' : 'healthy');

                $('#metrics-overview').html(html);
            }).fail(function() {
                $('#metrics-overview').html('<div class="error">Failed to load metrics</div>');
            });
        }

        function createMetricCard(title, value, status) {
            let statusClass = '';
            let statusIndicator = '';

            if (status === 'healthy') {
                statusIndicator = '<span class="status-indicator status-healthy"></span>';
            } else if (status === 'warning') {
                statusIndicator = '<span class="status-indicator status-warning"></span>';
            } else if (status === 'critical') {
                statusIndicator = '<span class="status-indicator status-critical"></span>';
            }

            return `
                <div class="metric-card">
                    <div class="metric-title">${statusIndicator}${title}</div>
                    <div class="metric-value">${value}</div>
                </div>
            `;
        }

        function loadExtractionTrend() {
            $.get('/api/charts/extraction-trend', function(data) {
                if (data && data.data) {
                    Plotly.newPlot('extraction-trend-chart', data.data, data.layout);
                }
            });
        }

        function loadQualityOverview() {
            $.get('/api/charts/quality-overview', function(data) {
                if (data && data.data) {
                    Plotly.newPlot('quality-overview-chart', data.data, data.layout);
                }
            });
        }

        function loadPerformanceHistory() {
            $.get('/api/charts/performance-history', function(data) {
                if (data && data.data) {
                    Plotly.newPlot('performance-history-chart', data.data, data.layout);
                }
            });
        }
    </script>
</body>
</html>
"""