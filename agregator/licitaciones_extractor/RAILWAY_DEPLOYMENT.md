# Deployment en Railway

Esta guía te ayudará a deployar el Sistema de Extracción de Licitaciones en Railway.

## 🚀 Pasos de Deployment

### 1. Preparar el Repositorio

```bash
# Asegúrate de estar en el directorio del proyecto
cd licitaciones_extractor

# Commit todos los cambios
git add .
git commit -m "Railway deployment configuration"
git push origin main
```

### 2. Crear Cuenta en Railway

1. Ve a [railway.app](https://railway.app)
2. Regístrate con GitHub
3. Conecta tu repositorio

### 3. Crear el Proyecto

1. **New Project** → **Deploy from GitHub repo**
2. Selecciona tu repositorio `agregator`
3. Railway detectará automáticamente el subdirectorio `licitaciones_extractor`

### 4. Configurar la Base de Datos

1. En tu proyecto Railway, click **"+ New"**
2. Selecciona **"Database"** → **"PostgreSQL"**
3. Railway creará automáticamente la base de datos
4. La variable `DATABASE_URL` se configurará automáticamente

### 5. Configurar Variables de Ambiente

En Railway → **Variables**, agrega:

```env
# REQUERIDAS
LICITA_YA_API_KEY=tu_api_key_de_licita_ya
OPENAI_API_KEY=tu_api_key_de_openai

# OPCIONALES (ya tienen defaults)
EXTRACTION_TIME=02:00
EXTRACTION_TIMEZONE=America/Mexico_City
LOG_LEVEL=INFO
ENVIRONMENT=production
```

### 6. Deploy

1. Railway empezará el deployment automáticamente
2. Puedes ver los logs en tiempo real
3. El health check estará en: `https://your-app.railway.app/health`

## 🔧 Configuración Avanzada

### Health Checks

El sistema incluye endpoints de monitoreo:

- **`/health`** - Health check básico
- **`/status`** - Estado detallado del sistema

### Logs

Ver logs en tiempo real:
```bash
railway logs
```

### Scaling

Railway escala automáticamente basado en uso. Para configuración manual:
1. Project Settings → Service Settings
2. Configura CPU/Memory según necesidades

## 🎯 Verificación Post-Deployment

### 1. Verificar Health Check
```bash
curl https://your-app.railway.app/health
```

Debería responder:
```json
{
  "status": "healthy",
  "database": "connected",
  "scheduler": "running"
}
```

### 2. Verificar Scheduler

El sistema debería:
- ✅ Iniciar automáticamente el scheduler
- ✅ Conectar a la base de datos
- ✅ Ejecutar extracciones diariamente a las 2:00 AM (México)

### 3. Monitorear Logs

```bash
railway logs --tail
```

Busca:
- `"Starting continuous scheduler"`
- `"Health check server started"`
- `"Database initialized successfully"`
- `"Next scheduled run: ..."`

## 📊 Monitoreo

### Variables de Ambiente Importantes

| Variable | Descripción | Default |
|----------|-------------|---------|
| `DATABASE_URL` | 🔄 Automático via Railway PostgreSQL | - |
| `LICITA_YA_API_KEY` | ⚠️ **Requerida** - API key de LicitaYa | - |
| `OPENAI_API_KEY` | ⚠️ **Requerida** - API key de OpenAI | - |
| `EXTRACTION_TIME` | Hora de extracción diaria (HH:MM) | `02:00` |
| `EXTRACTION_TIMEZONE` | Zona horaria | `America/Mexico_City` |
| `LOG_LEVEL` | Nivel de logging | `INFO` |

### Dashboard de Métricas

Acceder al dashboard: `https://your-app.railway.app/status`

### Comandos Útiles

```bash
# Conectar a Railway CLI
npm install -g @railway/cli
railway login

# Ver logs
railway logs

# Variables de ambiente
railway variables

# Connect a la base de datos
railway connect postgres
```

## 🔧 Troubleshooting

### Error: "Database connection failed"
1. Verifica que PostgreSQL service esté running
2. Check `DATABASE_URL` variable
3. Ver logs: `railway logs`

### Error: "Scheduler not starting"
1. Verifica `EXTRACTION_TIME` format (HH:MM)
2. Check `EXTRACTION_TIMEZONE` value
3. Ver health check: `/health`

### Error: "API keys not configured"
1. Configura `LICITA_YA_API_KEY`
2. Configura `OPENAI_API_KEY`
3. Redeploy después de agregar variables

### Performance Issues
1. Railway → Service Settings
2. Incrementar Memory/CPU
3. Monitorear via `/status` endpoint

## 📱 URLs Importantes

- **App**: `https://your-app.railway.app`
- **Health**: `https://your-app.railway.app/health`
- **Status**: `https://your-app.railway.app/status`
- **Railway Dashboard**: `https://railway.app/dashboard`

## 🎉 ¡Listo!

Tu sistema de extracción de licitaciones estará:
- ✅ Ejecutándose 24/7
- ✅ Extrayendo datos diariamente
- ✅ Monitoreado con health checks
- ✅ Escalando automáticamente
- ✅ Con base de datos PostgreSQL incluida

### Próximos Pasos
1. Configurar alertas de monitoreo
2. Revisar logs diarios
3. Analizar datos extraídos
4. Optimizar keywords según resultados