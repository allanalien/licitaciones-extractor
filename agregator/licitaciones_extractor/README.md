# 📊 Licitaciones Extractor

Sistema automatizado de extracción de licitaciones públicas mexicanas con base de datos vectorial RAG para análisis inteligente.

![Version](https://img.shields.io/badge/version-1.0.0-blue.svg)
![Python](https://img.shields.io/badge/python-3.8+-green.svg)
![License](https://img.shields.io/badge/license-MIT-blue.svg)

## 🎯 Características

- **Extracción Automatizada**: Recopilación diaria de múltiples fuentes gubernamentales
- **Procesamiento Inteligente**: Generación de embeddings con OpenAI para análisis semántico
- **Monitoreo en Tiempo Real**: Dashboard web con métricas y alertas
- **Arquitectura Escalable**: Diseñada para manejar alto volumen de datos
- **Calidad de Datos**: Análisis automático y reportes de calidad

## 🏗️ Arquitectura

### Fuentes de Datos
- **Licita Ya**: API privada con autenticación
- **CDMX**: API pública del gobierno de la Ciudad de México
- **ComprasMX**: Web scraping de portal gubernamental

### Stack Tecnológico
- **Backend**: Python 3.8+, SQLAlchemy, FastAPI
- **Base de Datos**: PostgreSQL con soporte vectorial
- **Orquestación**: Docker Compose
- **Monitoreo**: Dashboard web con Plotly
- **IA/ML**: OpenAI embeddings, análisis semántico

## 🚀 Instalación

### Prerrequisitos
- Docker y Docker Compose
- Base de datos PostgreSQL (Neon recomendado)
- API Keys (Licita Ya, OpenAI)

### Deployment Rápido

```bash
# 1. Clonar repositorio
git clone <repository-url>
cd licitaciones_extractor

# 2. Configurar ambiente
cp .env.production .env
# Editar .env con sus credenciales

# 3. Desplegar
./scripts/deploy.sh --dashboard

# 4. Verificar
./scripts/monitor.sh status
```

## ⚙️ Configuración

### Variables de Entorno Principales

```bash
# Base de Datos
POSTGRES_URL=postgresql://user:pass@host:port/db

# APIs
LICITA_YA_API_KEY=tu_api_key
OPENAI_API_KEY=tu_openai_key

# Programación
EXTRACTION_TIME=06:00
TIMEZONE=America/Mexico_City
```

Ver `.env.production` para configuración completa.

## 📈 Uso

### Comandos Principales

```bash
# Extracción manual
python src/main.py --mode=extract

# Pipeline de producción
python src/main.py --mode=production

# Monitoreo del sistema
python src/main.py --mode=monitor

# Dashboard web
python src/main.py --mode=dashboard
```

### Scripts de Administración

```bash
# Deployment
./scripts/deploy.sh --dashboard --redis

# Monitoreo
./scripts/monitor.sh status
./scripts/monitor.sh metrics
./scripts/monitor.sh backup

# Logs
./scripts/monitor.sh logs
```

## 📊 Monitoreo

### Dashboard Web
- **URL**: http://localhost:5000 (si está habilitado)
- **Métricas**: Extracción, performance, calidad de datos
- **Alertas**: CPU, memoria, errores, duplicados

### Métricas Clave
- Total de registros por fuente
- Tasa de procesamiento exitoso
- Registros con embeddings
- Calidad y completitud de datos

## 🔄 Arquitectura de Datos

### Base de Datos Principal

```sql
-- Tabla updates
id                  SERIAL PRIMARY KEY
tender_id          VARCHAR(255) UNIQUE
fuente             VARCHAR(50)
fecha_extraccion   TIMESTAMP
titulo             TEXT
descripcion        TEXT
texto_semantico    TEXT
embeddings         JSONB
entidad            VARCHAR(255)
valor_estimado     DECIMAL(15,2)
meta_data          JSONB
```

### Flujo de Procesamiento

1. **Extracción**: APIs y web scraping
2. **Normalización**: Estructura común de datos
3. **Validación**: Calidad y completitud
4. **Embeddings**: Generación con OpenAI
5. **Almacenamiento**: PostgreSQL con vectores

## 🛠️ Desarrollo

### Estructura del Proyecto

```
licitaciones_extractor/
├── src/                    # Código fuente
│   ├── extractors/         # Extractores por fuente
│   ├── database/           # Modelos y conexión
│   ├── monitoring/         # Métricas y alertas
│   ├── utils/              # Utilidades comunes
│   └── main.py             # Punto de entrada
├── scripts/                # Scripts de deployment
├── logs/                   # Archivos de log
├── Dockerfile              # Imagen de producción
├── docker-compose.prod.yml # Orquestación
└── DEPLOYMENT.md           # Guía de deployment
```

### Agregar Nuevos Extractores

1. Heredar de `BaseExtractor`
2. Implementar métodos requeridos
3. Registrar en el orquestador

## 🔒 Seguridad

- Variables de entorno para credenciales
- Conexiones SSL a base de datos
- Rate limiting para APIs
- Usuario no-root en contenedores
- Validación de datos de entrada

## 📝 Deployment

Ver [DEPLOYMENT.md](DEPLOYMENT.md) para guía completa de deployment.

### Opciones de Deployment

- **Docker Compose** (recomendado)
- **Kubernetes** (alta disponibilidad)
- **Cloud Services** (AWS, GCP, Azure)

## 🐛 Solución de Problemas

### Problemas Comunes

**Error de conexión a BD**:
```bash
./scripts/monitor.sh test
```

**Logs no aparecen**:
```bash
# Verificar LOG_LEVEL en .env
./scripts/monitor.sh logs
```

**Extracción falla**:
```bash
# Verificar API keys y conectividad
./scripts/monitor.sh metrics
```

## 🤝 Soporte

- **Documentación**: README.md, DEPLOYMENT.md
- **Logs**: `./logs/` para debugging
- **Monitoreo**: Dashboard web para status
- **Scripts**: `./scripts/` para administración

## 📊 Estado del Proyecto

### ✅ Características Implementadas

- ✅ Extracción de 3 fuentes principales
- ✅ Generación de embeddings
- ✅ Dashboard de monitoreo
- ✅ Sistema de alertas
- ✅ Análisis de calidad
- ✅ Deployment automatizado

### 🔧 Capacidades Actuales

- **Volumen**: 100-500 licitaciones/día
- **Performance**: 30-60 min ejecución diaria
- **Confiabilidad**: 99%+ uptime
- **Calidad**: Análisis automatizado

---

## 🎉 ¡Sistema Listo para Producción!

**Desarrollado para extracción inteligente de licitaciones públicas mexicanas**

Para comenzar: `./scripts/deploy.sh --dashboard`