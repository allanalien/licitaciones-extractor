#!/bin/bash

# Script para ejecutar la extracción diaria de licitaciones
# Uso: ./run_daily_extraction.sh [--parallel] [--only-vectors] [--test-connection]

# Configurar variables de entorno
export PYTHONPATH="${PYTHONPATH}:$(pwd)"

# Crear directorio de logs si no existe
mkdir -p logs

# Activar entorno virtual si existe
if [ -f "venv/bin/activate" ]; then
    source venv/bin/activate
    echo "Entorno virtual activado"
fi

# Verificar que existe el archivo .env
if [ ! -f ".env" ]; then
    echo "⚠️ Archivo .env no encontrado. Copiando desde .env.example..."
    cp .env.example .env
    echo "🔧 Por favor configura las variables en .env antes de continuar"
    exit 1
fi

# Instalar dependencias si es necesario
if [ ! -d "venv" ]; then
    echo "📦 Instalando dependencias..."
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
fi

# Timestamp para logs
TIMESTAMP=$(date "+%Y-%m-%d %H:%M:%S")
echo "🚀 Iniciando extracción de licitaciones - $TIMESTAMP"

# Ejecutar script principal
python3 main_extractor.py "$@"

# Verificar resultado
if [ $? -eq 0 ]; then
    echo "✅ Extracción completada exitosamente - $(date '+%Y-%m-%d %H:%M:%S')"

    # Opcional: Comprimir logs antiguos
    find logs/ -name "*.log" -mtime +7 -exec gzip {} \;

else
    echo "❌ Error en la extracción - $(date '+%Y-%m-%d %H:%M:%S')"
    exit 1
fi