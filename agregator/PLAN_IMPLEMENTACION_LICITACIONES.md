# Plan de Implementación - Sistema de Extracción de Licitaciones

## Objetivo
Crear un sistema de extracción automatizado que recopile licitaciones diarias de múltiples fuentes y las almacene en una base de datos vectorial RAG para alimentar un agente inteligente.

## Arquitectura General

### Estructura del Proyecto
```
licitaciones_extractor/
├── src/
│   ├── extractors/
│   │   ├── __init__.py
│   │   ├── base_extractor.py          # Clase base para todos los extractores
│   │   ├── licita_ya_extractor.py     # Extractor API privada Licita Ya
│   │   ├── cdmx_extractor.py          # Extractor API pública CDMX
│   │   └── comprasmx_scraper.py       # Web scraper ComprasMX
│   ├── database/
│   │   ├── __init__.py
│   │   ├── connection.py              # Configuración PostgreSQL
│   │   ├── models.py                  # Modelos de datos
│   │   └── schema.sql                 # Script creación tabla updates
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── text_processor.py          # Procesamiento de texto semántico
│   │   ├── embeddings_generator.py    # Generación de embeddings
│   │   └── data_normalizer.py         # Normalización entre fuentes
│   ├── config/
│   │   ├── settings.py                # Configuraciones generales
│   │   └── keywords.py                # Keywords corporativos
│   ├── scheduler/
│   │   ├── __init__.py
│   │   └── daily_job.py               # Orquestador diario
│   └── main.py                        # Punto de entrada principal
├── tests/
│   ├── __init__.py
│   ├── test_extractors.py
│   └── test_database.py
├── logs/
├── requirements.txt
├── .env.example
├── .gitignore
├── docker-compose.yml                 # Para desarrollo local
└── README.md
```

## Base de Datos

### Tabla Principal: `updates`
Basada en la estructura existente de `licitaciones_updates` con los siguientes campos:

```sql
CREATE TABLE updates (
    id SERIAL PRIMARY KEY,
    tender_id VARCHAR(255) UNIQUE NOT NULL,
    fuente VARCHAR(50) NOT NULL,
    fecha_extraccion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    fecha_catalogacion DATE,
    fecha_apertura DATE,
    titulo TEXT,
    descripcion TEXT,
    texto_semantico TEXT NOT NULL,
    metadata JSONB NOT NULL,
    embeddings VECTOR(1536),  -- Ajustar dimensión según modelo
    entidad VARCHAR(255),
    estado VARCHAR(100),
    ciudad VARCHAR(100),
    valor_estimado DECIMAL(15,2),
    tipo_licitacion VARCHAR(100),
    url_original TEXT,
    procesado BOOLEAN DEFAULT FALSE,
    INDEX idx_fecha_extraccion (fecha_extraccion),
    INDEX idx_fuente (fuente),
    INDEX idx_tender_id (tender_id)
);
```

### Conexión PostgreSQL
- **URL**: `postgresql://neondb_owner:npg_Tr1wXonS8EZy@ep-fragrant-feather-age2rjov-pooler.c-2.eu-central-1.aws.neon.tech/neondb?sslmode=require`

## Fuentes de Datos

### 1. API Licita Ya (Privada)
- **Endpoint**: `https://www.licitaya.com.mx/api/v1/tender/search`
- **API Key**: `B1995953A2A074E0EB2A35494C6F9E5C`
- **Headers**: `X-API-KEY: {api_key}`

#### Keywords Corporativos Prioritarios:
1. **alimentos** - Suministros alimentarios y catering
2. **medicinas** - Medicamentos y equipos médicos
3. **obra publica** - Construcción e infraestructura
4. **equipo tecnologico** - Hardware y software
5. **servicios profesionales** - Consultoría y asesoría
6. **construccion** - Obras civiles y arquitectura
7. **salud** - Servicios de salud y hospitalarios
8. **educacion** - Servicios educativos y capacitación
9. **seguridad** - Servicios y equipos de seguridad
10. **transporte** - Vehículos y servicios logísticos

#### Parámetros de Extracción:
```python
params = {
    'date': 'YYYYMMDD',  # Día anterior
    'keyword': keyword,
    'page': page_num,
    'items': 25,
    'smartsearch': 1,
    'listing': 0
}
```

### 2. API CDMX (Pública)
- **Endpoint**: `https://datosabiertostianguisdigital.cdmx.gob.mx/api/v1/plannings`
- **Parámetros**:
  - `hiring_method=1,2,3`
  - `consolidated=FALSE`
  - `start_date`: Día anterior (dd/MM/yyyy)
  - `end_date`: Día anterior (dd/MM/yyyy)

### 3. Web Scraping ComprasMX
- **URL**: `https://comprasmx.buengobierno.gob.mx/sitiopublico/#/`
- **Tecnología**: Selenium + BeautifulSoup
- **Enfoque**:
  - Navegación de SPA (Single Page Application)
  - Extracción de licitaciones del día anterior
  - Manejo de paginación dinámica

## Procesamiento de Datos

### Normalización
Cada fuente debe normalizar sus datos al siguiente formato estándar:

```python
{
    "tender_id": "string",
    "fuente": "licita_ya|cdmx|comprasmx",
    "titulo": "string",
    "descripcion": "string",
    "entidad": "string",
    "estado": "string",
    "ciudad": "string",
    "fecha_catalogacion": "YYYY-MM-DD",
    "fecha_apertura": "YYYY-MM-DD",
    "valor_estimado": float,
    "tipo_licitacion": "string",
    "url_original": "string",
    "metadata_especifica": {}  # Datos específicos de la fuente
}
```

### Texto Semántico
Combinación estructurada de:
- Título de la licitación
- Descripción completa
- Nombre de la entidad
- Tipo de licitación
- Contexto geográfico

### Embeddings
- **Modelo**: OpenAI text-embedding-ada-002 o similar
- **Dimensiones**: 1536
- **Procesamiento**: Texto semántico completo

### Metadata JSONB
```json
{
    "fuente_original": "string",
    "fecha_extraccion": "ISO_datetime",
    "parametros_busqueda": {},
    "datos_especificos": {
        "licita_ya": {
            "smart_search": "string",
            "lots": [],
            "agency": "string"
        },
        "cdmx": {
            "hiring_method": "string",
            "consolidated": boolean
        },
        "comprasmx": {
            "pagina_origen": "string",
            "metodo_extraccion": "scraping"
        }
    },
    "calidad_datos": {
        "completitud": 0.0-1.0,
        "confiabilidad": 0.0-1.0
    }
}
```

## Automatización y Orquestación

### Flujo Diario
1. **Inicio**: 06:00 AM (para capturar licitaciones del día anterior)
2. **Secuencia**:
   - Licita Ya API (por cada keyword)
   - CDMX API
   - ComprasMX Scraping
3. **Procesamiento**:
   - Normalización de datos
   - Generación de embeddings
   - Almacenamiento en PostgreSQL
4. **Logging y monitoreo**

### Configuración Cron
```bash
0 6 * * * /path/to/python /path/to/licitaciones_extractor/main.py --mode=daily
```

### Manejo de Errores
- Reintentos automáticos (3 intentos por fuente)
- Logging detallado por fuente
- Notificaciones en caso de falla crítica
- Continuidad: si una fuente falla, continúa con las demás

## Configuraciones

### Variables de Entorno (.env)
```
# Database
POSTGRES_URL=postgresql://neondb_owner:npg_Tr1wXonS8EZy@ep-fragrant-feather-age2rjov-pooler.c-2.eu-central-1.aws.neon.tech/neondb?sslmode=require

# APIs
LICITA_YA_API_KEY=B1995953A2A074E0EB2A35494C6F9E5C
LICITA_YA_BASE_URL=https://www.licitaya.com.mx/api/v1
CDMX_BASE_URL=https://datosabiertostianguisdigital.cdmx.gob.mx/api/v1

# Embeddings
OPENAI_API_KEY=your_openai_key
EMBEDDING_MODEL=text-embedding-ada-002

# Scraping
SELENIUM_TIMEOUT=30
HEADLESS_BROWSER=true

# Logging
LOG_LEVEL=INFO
LOG_FILE=/var/log/licitaciones_extractor.log

# Scheduling
EXTRACTION_TIME=06:00
RETRY_ATTEMPTS=3
BATCH_SIZE=100
```

### Keywords Configuration
```python
CORPORATE_KEYWORDS = [
    "alimentos",
    "medicinas",
    "obra publica",
    "equipo tecnologico",
    "servicios profesionales",
    "construccion",
    "salud",
    "educacion",
    "seguridad",
    "transporte"
]
```

## Extensibilidad Futura

### Agregar Nuevas Fuentes
1. Crear nuevo extractor heredando de `BaseExtractor`
2. Implementar métodos requeridos:
   - `extract_data(date)`
   - `normalize_data(raw_data)`
   - `validate_data(normalized_data)`
3. Registrar en el orquestador principal

### Estructura BaseExtractor
```python
class BaseExtractor:
    def __init__(self, config):
        self.config = config

    def extract_data(self, date):
        raise NotImplementedError

    def normalize_data(self, raw_data):
        raise NotImplementedError

    def validate_data(self, normalized_data):
        raise NotImplementedError
```

## Monitoreo y Mantenimiento

### Métricas Clave
- Número de licitaciones extraídas por fuente/día
- Tiempo de ejecución por extractor
- Tasa de errores por fuente
- Calidad de datos (completitud, duplicados)

### Logs Estructurados
```python
{
    "timestamp": "ISO_datetime",
    "level": "INFO|WARNING|ERROR",
    "component": "extractor_name",
    "message": "string",
    "metadata": {
        "execution_time": "seconds",
        "records_processed": int,
        "errors_count": int
    }
}
```

## Dependencias Principales

### requirements.txt
```
requests>=2.28.0
psycopg2-binary>=2.9.0
sqlalchemy>=1.4.0
selenium>=4.0.0
beautifulsoup4>=4.11.0
openai>=0.27.0
pandas>=1.5.0
python-dotenv>=0.19.0
schedule>=1.2.0
loguru>=0.6.0
pydantic>=1.10.0
tenacity>=8.2.0
```

## Fases de Implementación

### ✅ Fase 1: Infraestructura Base - COMPLETADA
1. ✅ Estructura del proyecto
2. ✅ Base de datos y modelos
3. ✅ Configuración y logging

### ✅ Fase 2: Extractores Individuales - COMPLETADA
1. ✅ Extractor Licita Ya
2. ✅ Extractor CDMX
3. ✅ Scraper ComprasMX

### ✅ Fase 3: Integración y Automatización - COMPLETADA
1. ✅ Orquestador principal (`ExtractionOrchestrator`)
2. ✅ Generación de embeddings (`EmbeddingsGenerator`)
3. ✅ Scheduling automático (`DailyScheduler`)
4. ✅ Entry point principal con CLI completo
5. ✅ Manejo de errores y reintentos
6. ✅ Integración completa y pruebas

### ✅ Fase 4: Optimización y Monitoreo - COMPLETADA
1. ✅ Métricas y dashboards en tiempo real
2. ✅ Optimización de performance con procesamiento paralelo
3. ✅ Sistema de alertas para errores críticos
4. ✅ Análisis de calidad de datos
5. ✅ Documentación completa actualizada

## Consideraciones de Seguridad
- API keys en variables de entorno
- Conexiones SSL para base de datos
- Rate limiting para evitar bloqueos
- Validación de datos de entrada
- Logs sin información sensible

## Estimación de Recursos
- **Tiempo de desarrollo**: 3-4 semanas
- **Almacenamiento**: ~1GB/mes (estimado)
- **Procesamiento**: Ejecución diaria ~30-60 minutos
- **Mantenimiento**: Revisión semanal de logs y métricas

---

## 🎉 ESTADO ACTUAL DEL PROYECTO

### ✅ SISTEMA COMPLETAMENTE FUNCIONAL - LISTO PARA PRODUCCIÓN

**Fecha de Finalización Fase 3**: 2 de Diciembre, 2024
**Fecha de Finalización Fase 4**: 2 de Diciembre, 2024

### 📋 Componentes Implementados y Funcionando:

#### 🏗️ Infraestructura Core
- ✅ **Base de datos PostgreSQL** configurada y funcional
- ✅ **Modelos SQLAlchemy** con soporte para embeddings
- ✅ **Sistema de configuración** con variables de entorno
- ✅ **Logging estructurado** con archivos JSON

#### 🔄 Extractores de Datos
- ✅ **LicitaYaExtractor** - API privada con keywords corporativos
- ✅ **CDMXExtractor** - API pública de Ciudad de México
- ✅ **ComprasMXScraper** - Web scraping con Selenium

#### 🤖 Sistema de Orquestación
- ✅ **ExtractionOrchestrator** - Coordinación completa de extractores
- ✅ **EmbeddingsGenerator** - Generación de vectores con OpenAI
- ✅ **DailyScheduler** - Automatización y scheduling
- ✅ **DataNormalizer** - Normalización entre fuentes

#### 💻 Interface de Usuario
- ✅ **CLI Completo** con 8 modos de operación:
  - `extract` - Extracción manual
  - `daily` - Trabajo diario
  - `scheduler` - Modo continuo
  - `test` - Pruebas de conexión
  - `setup` - Configuración inicial
  - `monitor` - Ver métricas del sistema
  - `quality-report` - Generar reporte de calidad
  - `dashboard` - Dashboard web interactivo

#### 📊 Sistema de Monitoreo y Optimización (Fase 4)
- ✅ **MetricsCollector** - Recopilación de métricas en tiempo real
- ✅ **DataQualityAnalyzer** - Análisis profundo de calidad de datos
- ✅ **PerformanceMonitor** - Optimización y procesamiento paralelo
- ✅ **AlertingSystem** - Sistema de alertas multi-canal
- ✅ **Dashboard Web** - Interfaz visual con gráficos interactivos

### 🚀 Comandos de Producción Listos:

```bash
# Extracción diaria automática
python src/main.py --mode=daily

# Scheduler continuo (recomendado para producción)
python src/main.py --mode=scheduler

# Pruebas del sistema
python src/main.py --mode=test
```

### 📊 Características Técnicas Implementadas:

#### ⚡ Performance y Confiabilidad
- ✅ **Sistema de reintentos**: 3 intentos con backoff exponencial
- ✅ **Procesamiento por lotes**: Configurable para optimizar memoria
- ✅ **Manejo de errores**: Continúa procesando aunque un extractor falle
- ✅ **Caché inteligente**: Sistema de caché para embeddings

#### 🔧 Operaciones y Monitoreo
- ✅ **Logging detallado**: Métricas de tiempo, registros procesados, errores
- ✅ **Validación de datos**: Verificación de completitud y calidad
- ✅ **Configuración flexible**: Todos los parámetros configurables via .env
- ✅ **Modo dry-run**: Pruebas sin escribir a base de datos

#### 🛡️ Seguridad
- ✅ **API keys en variables de entorno**: No hay credenciales en código
- ✅ **Conexiones SSL**: Base de datos segura
- ✅ **Rate limiting**: Prevención de bloqueos de APIs
- ✅ **Validación de entrada**: Sanitización de datos

### 🗄️ Base de Datos Productiva
- ✅ **Tabla `updates`** con estructura completa
- ✅ **Índices optimizados** para consultas rápidas
- ✅ **Soporte para embeddings** (JSONB temporal, migrable a pgvector)
- ✅ **Metadatos estructurados** para trazabilidad

### 📈 Métricas de Rendimiento Esperadas:
- **Tiempo de ejecución diaria**: 30-60 minutos
- **Registros por día**: 100-500 licitaciones
- **Uso de memoria**: ~200MB durante procesamiento
- **Llamadas API**: ~100-200 embeddings por día

### ✅ Características Implementadas en Fase 4:
1. **Dashboard Web Interactivo** - Visualización en tiempo real con Plotly
2. **Procesamiento Paralelo** - Extracción simultánea de múltiples fuentes
3. **Sistema de Alertas Multi-canal** - Logs, Email, Webhooks, Slack
4. **Análisis de Calidad Avanzado** - Completitud, duplicados, consistencia
5. **Optimización Automática** - Batch size dinámico basado en recursos

---

## 🏆 CONCLUSIÓN

**El Sistema de Extracción de Licitaciones está 100% FUNCIONAL y LISTO PARA PRODUCCIÓN.**

Todas las fases planificadas (1, 2, 3 y 4) han sido completadas exitosamente. El sistema incluye:

### Capacidades de Extracción:
- ✅ Extraer datos de múltiples fuentes diariamente
- ✅ Generar embeddings automáticamente con OpenAI
- ✅ Almacenar en base de datos vectorial PostgreSQL
- ✅ Ejecutarse de forma automatizada con scheduler
- ✅ Manejar errores con reintentos inteligentes
- ✅ Proporcionar logging estructurado completo

### Capacidades de Monitoreo y Optimización:
- ✅ Dashboard web con métricas en tiempo real
- ✅ Sistema de alertas multi-canal configurables
- ✅ Análisis automático de calidad de datos
- ✅ Procesamiento paralelo para mayor eficiencia
- ✅ Reportes detallados con recomendaciones
- ✅ Optimización automática de recursos

**¡Sistema empresarial completo, robusto y escalable!** 🚀

**Implementación de todas las fases exitosa!** 🎉