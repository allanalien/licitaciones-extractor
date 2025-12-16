# Sistema Extractor de Licitaciones RAG

Sistema automatizado para extraer licitaciones de múltiples fuentes y crear una base de datos vectorial para un agente financiero de mercado.

## 🎯 Características

- **3 Extractores especializados**:
  - **Tianguis Digital**: API oficial del gobierno mexicano
  - **Licita Ya**: API con búsqueda por palabras clave
  - **ComprasMX**: Web scraping del portal gubernamental

- **Base de datos vectorial**: Conversión automática para RAG
- **Ejecución automática**: Script principal que coordina todo
- **Programación**: Cron job para ejecución diaria
- **PostgreSQL**: Almacenamiento estructurado con conexión Neon
- **Monitoreo**: Logs detallados y reportes de extracción

## 🏗️ Estructura del Proyecto

```
extractor/
├── main_extractor.py              # Script principal
├── requirements.txt               # Dependencias Python
├── .env.example                  # Variables de entorno
├── run_daily_extraction.sh       # Script de ejecución diaria
├── setup_cron.sh                # Configuración de cron job
├── src/
│   ├── database/
│   │   └── models.py             # Modelos PostgreSQL
│   ├── extractors/
│   │   ├── tianguis_digital_extractor.py
│   │   ├── licita_ya_extractor.py
│   │   └── compras_mx_extractor.py
│   └── utils/
│       └── vector_manager.py     # Gestión de embeddings
└── logs/                         # Archivos de log
```

## ⚙️ Instalación y Configuración

### 1. Instalar dependencias

```bash
pip install -r requirements.txt
```

### 2. Configurar variables de entorno

```bash
cp .env.example .env
# Editar .env con tus configuraciones
```

**Variables importantes**:
```env
# Base de datos PostgreSQL (ya configurada)
DATABASE_URL=postgresql://neondb_owner:npg_Tr1wXonS8EZy@ep-fragrant-feather-age2rjov-pooler.c-2.eu-central-1.aws.neon.tech/neondb?sslmode=require

# API de Licita Ya (ya configurada)
LICITA_YA_API_KEY=B1995953A2A074E0EB2A35494C6F9E5C

# OpenAI para embeddings (opcional - usa modelo local como respaldo)
OPENAI_API_KEY=tu_api_key_aqui

# Keywords para Licita Ya
LICITA_YA_KEYWORDS=construcción,infraestructura,tecnología,servicios,consultoría
```

### 3. Probar conexiones

```bash
python3 main_extractor.py --test-connection
```

## 🚀 Uso

### Extracción manual

```bash
# Extracción secuencial (recomendado)
python3 main_extractor.py

# Extracción paralela (más rápido)
python3 main_extractor.py --parallel

# Solo procesar vectores pendientes
python3 main_extractor.py --only-vectors
```

### Extracción automatizada

```bash
# Ejecutar una vez
./run_daily_extraction.sh

# Configurar ejecución diaria automática
./setup_cron.sh
```

## 📊 Funcionamiento

### 1. Tianguis Digital
- **Método**: API REST
- **Datos**: Licitaciones del día anterior
- **Características**: Rápido, datos estructurados

### 2. Licita Ya
- **Método**: API con autenticación
- **Datos**: Búsqueda por keywords (últimos 7 días)
- **Características**: Incluye enriquecimiento web, datos internacionales

### 3. ComprasMX
- **Método**: Web scraping con Selenium
- **Datos**: Licitaciones del día anterior
- **Características**: Más lento, datos locales México

### 4. Procesamiento RAG
- **Embeddings**: OpenAI text-embedding-ada-002 o modelo local
- **Vector DB**: ChromaDB local + PostgreSQL
- **Normalización**: Formato estándar para todas las fuentes

## 📈 Monitoreo

### Logs
- **Archivo**: `logs/extractor.log`
- **Rotación**: 10MB, 30 días
- **Nivel**: INFO (configurable)

### Reportes
- **Ubicación**: `logs/extraction_report_YYYYMMDD_HHMMSS.json`
- **Contenido**: Estadísticas detalladas por fuente

### Cron logs
- **Archivo**: `logs/cron.log`
- **Contenido**: Salida de ejecuciones automáticas

## 🔧 Personalización

### Agregar nuevas fuentes
1. Crear extractor en `src/extractors/`
2. Implementar método `extract_yesterday_data()`
3. Agregar al `main_extractor.py`

### Modificar keywords
Editar `LICITA_YA_KEYWORDS` en `.env`:
```env
LICITA_YA_KEYWORDS=palabra1,palabra2,palabra3
```

### Cambiar programación
Editar `setup_cron.sh` para cambiar horario:
```bash
# Cambiar de 6:00 AM a 8:00 PM
CRON_JOB="0 20 * * * ..."
```

## 🛠️ Comandos Útiles

```bash
# Ver estado de la base de datos
python3 -c "from src.database.models import DatabaseManager; db = DatabaseManager(); db.create_tables(); print('OK')"

# Verificar vector database
python3 -c "from src.utils.vector_manager import VectorManager; vm = VectorManager(); print(vm.get_collection_stats())"

# Limpiar logs antiguos
find logs/ -name "*.log" -mtime +30 -delete

# Ver cron jobs
crontab -l

# Desinstalar cron job
crontab -l | grep -v 'run_daily_extraction.sh' | crontab -
```

## 🔍 Solución de Problemas

### Error de conexión PostgreSQL
- Verificar URL en `.env`
- Comprobar conectividad de red
- Validar credenciales

### Error en Selenium
- Instalar Chrome/Chromium
- Verificar ChromeDriver
- Ejecutar en modo headless

### Problemas de API
- Verificar keys en `.env`
- Comprobar límites de rate
- Validar formatos de fecha

### Vector Database
- Verificar espacio en disco
- Comprobar permisos de escritura
- Validar modelo de embeddings

## 📧 Soporte

Para problemas o mejoras, revisar los logs en `logs/` y verificar las configuraciones en `.env`.

---

**Sistema desarrollado para automatizar la recolección de licitaciones y crear una base de datos vectorial RAG para análisis financiero de mercado.**