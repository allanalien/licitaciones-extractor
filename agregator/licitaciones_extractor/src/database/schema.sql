-- Schema creation script for licitaciones extractor
-- PostgreSQL version

-- Create the updates table
CREATE TABLE IF NOT EXISTS updates (
    id SERIAL PRIMARY KEY,
    tender_id VARCHAR(255) UNIQUE NOT NULL,
    fuente VARCHAR(50) NOT NULL,
    fecha_extraccion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    fecha_catalogacion DATE,
    fecha_apertura DATE,
    titulo TEXT,
    descripcion TEXT,
    texto_semantico TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}',
    embeddings JSONB,  -- Will be changed to VECTOR(1536) when pgvector is available
    entidad VARCHAR(255),
    estado VARCHAR(100),
    ciudad VARCHAR(100),
    valor_estimado DECIMAL(15,2),
    tipo_licitacion VARCHAR(100),
    url_original TEXT,
    procesado BOOLEAN DEFAULT FALSE
);

-- Create indexes for optimal performance
CREATE INDEX IF NOT EXISTS idx_fecha_extraccion ON updates (fecha_extraccion);
CREATE INDEX IF NOT EXISTS idx_fuente ON updates (fuente);
CREATE INDEX IF NOT EXISTS idx_tender_id ON updates (tender_id);
CREATE INDEX IF NOT EXISTS idx_procesado ON updates (procesado);
CREATE INDEX IF NOT EXISTS idx_entidad ON updates (entidad);
CREATE INDEX IF NOT EXISTS idx_estado ON updates (estado);
CREATE INDEX IF NOT EXISTS idx_fecha_catalogacion ON updates (fecha_catalogacion);

-- Create JSONB indexes for metadata queries
CREATE INDEX IF NOT EXISTS idx_metadata_gin ON updates USING gin (metadata);

-- Create partial indexes for common queries
CREATE INDEX IF NOT EXISTS idx_unprocessed ON updates (fecha_extraccion) WHERE procesado = FALSE;
CREATE INDEX IF NOT EXISTS idx_recent_extractions ON updates (fecha_extraccion) WHERE fecha_extraccion >= CURRENT_DATE - INTERVAL '7 days';

-- Optional: Create pgvector extension and update embeddings column type
-- Uncomment the following lines when pgvector extension is available:

-- CREATE EXTENSION IF NOT EXISTS vector;
-- ALTER TABLE updates ALTER COLUMN embeddings TYPE vector(1536);
-- CREATE INDEX IF NOT EXISTS idx_embeddings ON updates USING ivfflat (embeddings vector_cosine_ops);

-- Create a view for recent unprocessed updates
CREATE OR REPLACE VIEW recent_unprocessed AS
SELECT *
FROM updates
WHERE procesado = FALSE
  AND fecha_extraccion >= CURRENT_DATE - INTERVAL '3 days'
ORDER BY fecha_extraccion DESC;

-- Create a view for daily statistics
CREATE OR REPLACE VIEW daily_stats AS
SELECT
    fecha_extraccion::date as fecha,
    fuente,
    COUNT(*) as total_extracciones,
    COUNT(CASE WHEN procesado THEN 1 END) as procesadas,
    COUNT(CASE WHEN NOT procesado THEN 1 END) as pendientes,
    AVG((metadata->>'calidad_datos'->>'completitud')::float) as completitud_promedio
FROM updates
GROUP BY fecha_extraccion::date, fuente
ORDER BY fecha DESC, fuente;