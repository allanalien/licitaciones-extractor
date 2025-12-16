#!/bin/bash

# Script para configurar cron job para ejecutar extracción diaria
# Ejecuta todos los días a las 6:00 AM

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CRON_JOB="0 6 * * * cd $SCRIPT_DIR && ./run_daily_extraction.sh >> logs/cron.log 2>&1"

echo "Configurando cron job para extracción diaria..."
echo "Directorio del script: $SCRIPT_DIR"
echo "Cron job a instalar: $CRON_JOB"

# Backup del crontab actual
crontab -l > crontab_backup_$(date +%Y%m%d_%H%M%S).txt

# Agregar el nuevo job (evitar duplicados)
(crontab -l 2>/dev/null | grep -v "run_daily_extraction.sh"; echo "$CRON_JOB") | crontab -

echo "✅ Cron job instalado exitosamente"
echo "📅 El extractor se ejecutará todos los días a las 6:00 AM"
echo "📋 Para ver los cron jobs actuales: crontab -l"
echo "🗂️ Logs del cron en: $SCRIPT_DIR/logs/cron.log"

# Crear archivo de log para cron
mkdir -p "$SCRIPT_DIR/logs"
touch "$SCRIPT_DIR/logs/cron.log"

echo ""
echo "Para desinstalar el cron job, ejecuta:"
echo "crontab -l | grep -v 'run_daily_extraction.sh' | crontab -"