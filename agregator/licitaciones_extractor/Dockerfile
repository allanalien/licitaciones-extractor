# Production Dockerfile for Licitaciones Extractor
FROM python:3.12-slim

LABEL maintainer="licitaciones-extractor"
LABEL version="1.0.0"
LABEL description="Sistema automatizado de extracción de licitaciones públicas"

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONPATH=/app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    curl \
    chromium \
    chromium-driver \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Set Chrome/Chromium path for Selenium
ENV CHROME_BIN=/usr/bin/chromium
ENV CHROMEDRIVER_PATH=/usr/bin/chromedriver

# Create app directory
WORKDIR /app

# Create non-root user for security
RUN groupadd -r appuser && useradd -r -g appuser appuser

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ ./src/
COPY .env.example ./

# Create necessary directories
RUN mkdir -p logs data \
    && chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "from src.database.connection import DatabaseConnection; dc = DatabaseConnection(); dc.test_connection()" || exit 1

# Default command
CMD ["python", "src/main.py", "--mode=scheduler"]

# Expose ports (if needed for dashboard)
EXPOSE 5000