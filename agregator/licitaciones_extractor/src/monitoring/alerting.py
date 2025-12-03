"""
Alerting system for critical errors and monitoring thresholds
"""

import json
import smtplib
import requests
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Dict, List, Optional, Any
from enum import Enum
from dataclasses import dataclass

from ..config.settings import Settings
from ..utils.logger import get_logger


class AlertLevel(Enum):
    """Alert severity levels"""
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"
    EMERGENCY = "emergency"


class AlertChannel(Enum):
    """Available alert channels"""
    LOG = "log"
    EMAIL = "email"
    WEBHOOK = "webhook"
    SLACK = "slack"
    CONSOLE = "console"


@dataclass
class Alert:
    """Alert data structure"""
    level: AlertLevel
    title: str
    message: str
    source: str
    timestamp: datetime
    metadata: Optional[Dict] = None
    error_details: Optional[str] = None


class AlertingSystem:
    """Manages system alerts and notifications"""

    def __init__(self, settings: Optional[Settings] = None):
        self.settings = settings or Settings()
        self.logger = get_logger(self.__class__.__name__)
        self.alert_history: List[Alert] = []
        self.alert_counts: Dict[AlertLevel, int] = {level: 0 for level in AlertLevel}

        # Alert thresholds
        self.thresholds = {
            "extraction_failure_rate": 0.2,  # 20% failure rate
            "db_response_time_ms": 5000,     # 5 seconds
            "memory_usage_percent": 90,       # 90% memory
            "cpu_usage_percent": 90,          # 90% CPU
            "duplicate_rate": 0.15,           # 15% duplicates
            "completeness_rate": 0.5,         # Below 50% completeness
            "hours_without_data": 24          # 24 hours without new data
        }

        # Channel configurations
        self.channels = self._configure_channels()

    def _configure_channels(self) -> Dict[AlertChannel, Dict]:
        """Configure alert channels based on settings"""
        channels = {}

        # Always enable log channel
        channels[AlertChannel.LOG] = {"enabled": True}

        # Email configuration
        if hasattr(self.settings, 'EMAIL_ENABLED') and self.settings.EMAIL_ENABLED:
            channels[AlertChannel.EMAIL] = {
                "enabled": True,
                "smtp_server": getattr(self.settings, 'SMTP_SERVER', 'smtp.gmail.com'),
                "smtp_port": getattr(self.settings, 'SMTP_PORT', 587),
                "from_email": getattr(self.settings, 'FROM_EMAIL', ''),
                "to_emails": getattr(self.settings, 'TO_EMAILS', []),
                "smtp_username": getattr(self.settings, 'SMTP_USERNAME', ''),
                "smtp_password": getattr(self.settings, 'SMTP_PASSWORD', '')
            }

        # Webhook configuration
        if hasattr(self.settings, 'WEBHOOK_URL'):
            channels[AlertChannel.WEBHOOK] = {
                "enabled": True,
                "url": self.settings.WEBHOOK_URL,
                "headers": getattr(self.settings, 'WEBHOOK_HEADERS', {})
            }

        # Slack configuration
        if hasattr(self.settings, 'SLACK_WEBHOOK_URL'):
            channels[AlertChannel.SLACK] = {
                "enabled": True,
                "webhook_url": self.settings.SLACK_WEBHOOK_URL,
                "channel": getattr(self.settings, 'SLACK_CHANNEL', '#alerts')
            }

        # Console always enabled for critical alerts
        channels[AlertChannel.CONSOLE] = {"enabled": True}

        return channels

    def send_alert(self, alert: Alert, channels: Optional[List[AlertChannel]] = None):
        """Send an alert through specified channels"""
        # Record alert
        self.alert_history.append(alert)
        self.alert_counts[alert.level] += 1

        # Determine channels based on alert level if not specified
        if channels is None:
            channels = self._get_channels_for_level(alert.level)

        # Send through each channel
        for channel in channels:
            if channel in self.channels and self.channels[channel].get("enabled"):
                try:
                    self._send_to_channel(alert, channel)
                except Exception as e:
                    self.logger.error(f"Failed to send alert via {channel.value}: {e}")

    def _get_channels_for_level(self, level: AlertLevel) -> List[AlertChannel]:
        """Determine which channels to use based on alert level"""
        if level == AlertLevel.INFO:
            return [AlertChannel.LOG]
        elif level == AlertLevel.WARNING:
            return [AlertChannel.LOG, AlertChannel.CONSOLE]
        elif level == AlertLevel.CRITICAL:
            return [AlertChannel.LOG, AlertChannel.CONSOLE, AlertChannel.EMAIL, AlertChannel.SLACK]
        else:  # EMERGENCY
            return list(AlertChannel)  # All channels

    def _send_to_channel(self, alert: Alert, channel: AlertChannel):
        """Send alert to specific channel"""
        if channel == AlertChannel.LOG:
            self._send_to_log(alert)
        elif channel == AlertChannel.CONSOLE:
            self._send_to_console(alert)
        elif channel == AlertChannel.EMAIL:
            self._send_email(alert)
        elif channel == AlertChannel.WEBHOOK:
            self._send_webhook(alert)
        elif channel == AlertChannel.SLACK:
            self._send_slack(alert)

    def _send_to_log(self, alert: Alert):
        """Log the alert"""
        log_data = {
            "alert_level": alert.level.value,
            "title": alert.title,
            "message": alert.message,
            "source": alert.source,
            "timestamp": alert.timestamp.isoformat(),
            "metadata": alert.metadata
        }

        if alert.level == AlertLevel.CRITICAL or alert.level == AlertLevel.EMERGENCY:
            self.logger.error(json.dumps(log_data))
        elif alert.level == AlertLevel.WARNING:
            self.logger.warning(json.dumps(log_data))
        else:
            self.logger.info(json.dumps(log_data))

    def _send_to_console(self, alert: Alert):
        """Print alert to console with formatting"""
        # Color codes for terminal
        colors = {
            AlertLevel.INFO: '\033[94m',      # Blue
            AlertLevel.WARNING: '\033[93m',   # Yellow
            AlertLevel.CRITICAL: '\033[91m',  # Red
            AlertLevel.EMERGENCY: '\033[95m'  # Magenta
        }
        reset = '\033[0m'

        color = colors.get(alert.level, '')
        icon = self._get_alert_icon(alert.level)

        print(f"\n{color}{'='*60}")
        print(f"{icon} ALERT: {alert.title}")
        print(f"Level: {alert.level.value.upper()}")
        print(f"Source: {alert.source}")
        print(f"Time: {alert.timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}")
        print(f"Message: {alert.message}")
        if alert.error_details:
            print(f"Error Details: {alert.error_details}")
        if alert.metadata:
            print(f"Metadata: {json.dumps(alert.metadata, indent=2)}")
        print(f"{'='*60}{reset}\n")

    def _send_email(self, alert: Alert):
        """Send alert via email"""
        config = self.channels.get(AlertChannel.EMAIL, {})
        if not config.get("enabled") or not config.get("to_emails"):
            return

        try:
            # Create message
            msg = MIMEMultipart('alternative')
            msg['Subject'] = f"[{alert.level.value.upper()}] {alert.title}"
            msg['From'] = config['from_email']
            msg['To'] = ', '.join(config['to_emails'])

            # Create HTML body
            html = self._format_email_html(alert)
            msg.attach(MIMEText(html, 'html'))

            # Send email
            with smtplib.SMTP(config['smtp_server'], config['smtp_port']) as server:
                server.starttls()
                if config.get('smtp_username') and config.get('smtp_password'):
                    server.login(config['smtp_username'], config['smtp_password'])
                server.send_message(msg)

            self.logger.info(f"Email alert sent: {alert.title}")

        except Exception as e:
            self.logger.error(f"Failed to send email alert: {e}")

    def _send_webhook(self, alert: Alert):
        """Send alert via webhook"""
        config = self.channels.get(AlertChannel.WEBHOOK, {})
        if not config.get("enabled") or not config.get("url"):
            return

        try:
            payload = {
                "level": alert.level.value,
                "title": alert.title,
                "message": alert.message,
                "source": alert.source,
                "timestamp": alert.timestamp.isoformat(),
                "metadata": alert.metadata,
                "error_details": alert.error_details
            }

            response = requests.post(
                config['url'],
                json=payload,
                headers=config.get('headers', {}),
                timeout=10
            )
            response.raise_for_status()

            self.logger.info(f"Webhook alert sent: {alert.title}")

        except Exception as e:
            self.logger.error(f"Failed to send webhook alert: {e}")

    def _send_slack(self, alert: Alert):
        """Send alert to Slack"""
        config = self.channels.get(AlertChannel.SLACK, {})
        if not config.get("enabled") or not config.get("webhook_url"):
            return

        try:
            # Format Slack message
            color = {
                AlertLevel.INFO: "#36a64f",      # Green
                AlertLevel.WARNING: "#ff9900",   # Orange
                AlertLevel.CRITICAL: "#ff0000",  # Red
                AlertLevel.EMERGENCY: "#9900ff"  # Purple
            }.get(alert.level, "#808080")

            icon = self._get_alert_icon(alert.level)

            payload = {
                "channel": config.get("channel", "#alerts"),
                "attachments": [{
                    "color": color,
                    "title": f"{icon} {alert.title}",
                    "text": alert.message,
                    "fields": [
                        {"title": "Level", "value": alert.level.value.upper(), "short": True},
                        {"title": "Source", "value": alert.source, "short": True},
                        {"title": "Time", "value": alert.timestamp.strftime('%Y-%m-%d %H:%M:%S'), "short": False}
                    ],
                    "footer": "Tender Extraction System",
                    "ts": int(alert.timestamp.timestamp())
                }]
            }

            if alert.error_details:
                payload["attachments"][0]["fields"].append({
                    "title": "Error Details",
                    "value": alert.error_details,
                    "short": False
                })

            response = requests.post(config['webhook_url'], json=payload, timeout=10)
            response.raise_for_status()

            self.logger.info(f"Slack alert sent: {alert.title}")

        except Exception as e:
            self.logger.error(f"Failed to send Slack alert: {e}")

    def _format_email_html(self, alert: Alert) -> str:
        """Format alert as HTML for email"""
        icon = self._get_alert_icon(alert.level)
        color = {
            AlertLevel.INFO: "#4CAF50",
            AlertLevel.WARNING: "#FF9800",
            AlertLevel.CRITICAL: "#F44336",
            AlertLevel.EMERGENCY: "#9C27B0"
        }.get(alert.level, "#757575")

        html = f"""
        <html>
        <body style="font-family: Arial, sans-serif;">
            <div style="background-color: {color}; color: white; padding: 20px; border-radius: 5px;">
                <h2>{icon} {alert.title}</h2>
            </div>
            <div style="padding: 20px; background-color: #f5f5f5; margin-top: 10px; border-radius: 5px;">
                <p><strong>Level:</strong> {alert.level.value.upper()}</p>
                <p><strong>Source:</strong> {alert.source}</p>
                <p><strong>Time:</strong> {alert.timestamp.strftime('%Y-%m-%d %H:%M:%S')}</p>
                <hr>
                <p><strong>Message:</strong></p>
                <p>{alert.message}</p>
        """

        if alert.error_details:
            html += f"""
                <hr>
                <p><strong>Error Details:</strong></p>
                <pre style="background-color: #fff; padding: 10px; border-radius: 3px;">{alert.error_details}</pre>
            """

        if alert.metadata:
            html += f"""
                <hr>
                <p><strong>Additional Information:</strong></p>
                <pre style="background-color: #fff; padding: 10px; border-radius: 3px;">{json.dumps(alert.metadata, indent=2)}</pre>
            """

        html += """
            </div>
            <div style="padding: 10px; text-align: center; color: #888; font-size: 12px;">
                Tender Extraction Monitoring System
            </div>
        </body>
        </html>
        """

        return html

    def _get_alert_icon(self, level: AlertLevel) -> str:
        """Get icon for alert level"""
        return {
            AlertLevel.INFO: "ℹ️",
            AlertLevel.WARNING: "⚠️",
            AlertLevel.CRITICAL: "🚨",
            AlertLevel.EMERGENCY: "🆘"
        }.get(level, "📢")

    def check_metrics_thresholds(self, metrics: Dict[str, Any]):
        """Check metrics against thresholds and raise alerts"""
        alerts_raised = []

        # Check performance metrics
        if metrics.get("performance_metrics"):
            perf = metrics["performance_metrics"]

            # CPU usage
            if perf.get("cpu_usage", 0) > self.thresholds["cpu_usage_percent"]:
                alert = Alert(
                    level=AlertLevel.WARNING,
                    title="High CPU Usage",
                    message=f"CPU usage is at {perf['cpu_usage']:.1f}%",
                    source="Performance Monitor",
                    timestamp=datetime.now(),
                    metadata={"threshold": self.thresholds["cpu_usage_percent"]}
                )
                self.send_alert(alert)
                alerts_raised.append(alert)

            # Memory usage
            if perf.get("memory_usage", 0) > self.thresholds["memory_usage_percent"]:
                alert = Alert(
                    level=AlertLevel.CRITICAL,
                    title="Critical Memory Usage",
                    message=f"Memory usage is at {perf['memory_usage']:.1f}%",
                    source="Performance Monitor",
                    timestamp=datetime.now(),
                    metadata={"threshold": self.thresholds["memory_usage_percent"]}
                )
                self.send_alert(alert)
                alerts_raised.append(alert)

            # Database response time
            if perf.get("db_response_time_ms", 0) > self.thresholds["db_response_time_ms"]:
                alert = Alert(
                    level=AlertLevel.WARNING,
                    title="Slow Database Response",
                    message=f"Database response time is {perf['db_response_time_ms']}ms",
                    source="Database Monitor",
                    timestamp=datetime.now(),
                    metadata={"threshold": self.thresholds["db_response_time_ms"]}
                )
                self.send_alert(alert)
                alerts_raised.append(alert)

        # Check data quality metrics
        if metrics.get("data_quality_metrics"):
            quality = metrics["data_quality_metrics"]

            # Duplicate rate
            if quality.get("duplicate_count", 0) > 0:
                dup_summary = quality.get("duplicates", {}).get("summary", {})
                dup_rate = dup_summary.get("duplicate_rate", 0) / 100
                if dup_rate > self.thresholds["duplicate_rate"]:
                    alert = Alert(
                        level=AlertLevel.WARNING,
                        title="High Duplicate Rate",
                        message=f"Duplicate rate is {dup_rate*100:.1f}%",
                        source="Data Quality Monitor",
                        timestamp=datetime.now(),
                        metadata={"threshold": self.thresholds["duplicate_rate"]*100}
                    )
                    self.send_alert(alert)
                    alerts_raised.append(alert)

        # Check extraction metrics
        if metrics.get("extraction_metrics"):
            for source, source_metrics in metrics["extraction_metrics"].items():
                # Check data freshness
                if source_metrics.get("last_extraction"):
                    last_extraction = datetime.fromisoformat(source_metrics["last_extraction"])
                    hours_since = (datetime.now() - last_extraction).total_seconds() / 3600
                    if hours_since > self.thresholds["hours_without_data"]:
                        alert = Alert(
                            level=AlertLevel.CRITICAL,
                            title=f"No Data from {source}",
                            message=f"No data extracted from {source} for {hours_since:.1f} hours",
                            source="Extraction Monitor",
                            timestamp=datetime.now(),
                            metadata={"last_extraction": source_metrics["last_extraction"]}
                        )
                        self.send_alert(alert)
                        alerts_raised.append(alert)

        return alerts_raised

    def get_alert_summary(self, hours: int = 24) -> Dict[str, Any]:
        """Get summary of recent alerts"""
        cutoff = datetime.now() - timedelta(hours=hours)
        recent_alerts = [a for a in self.alert_history if a.timestamp >= cutoff]

        summary = {
            "total_alerts": len(recent_alerts),
            "by_level": {},
            "by_source": {},
            "recent_alerts": []
        }

        # Count by level
        for level in AlertLevel:
            level_alerts = [a for a in recent_alerts if a.level == level]
            summary["by_level"][level.value] = len(level_alerts)

        # Count by source
        sources = set(a.source for a in recent_alerts)
        for source in sources:
            source_alerts = [a for a in recent_alerts if a.source == source]
            summary["by_source"][source] = len(source_alerts)

        # Recent alerts (last 10)
        for alert in recent_alerts[-10:]:
            summary["recent_alerts"].append({
                "level": alert.level.value,
                "title": alert.title,
                "message": alert.message,
                "source": alert.source,
                "timestamp": alert.timestamp.isoformat()
            })

        return summary