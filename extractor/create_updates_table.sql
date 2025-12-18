-- Crear tabla de updates para el tracking de extracciones
CREATE TABLE IF NOT EXISTS updates (
    id SERIAL PRIMARY KEY,
    licitacion_id INTEGER REFERENCES licitaciones(id) ON DELETE CASCADE,
    metadata JSONB NOT NULL,
    texto_semantico TEXT,
    vector_procesado BOOLEAN DEFAULT FALSE,
    fuente VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Crear índices para mejorar búsquedas
CREATE INDEX IF NOT EXISTS idx_updates_created_at ON updates(created_at);
CREATE INDEX IF NOT EXISTS idx_updates_fuente ON updates(fuente);
CREATE INDEX IF NOT EXISTS idx_updates_licitacion_id ON updates(licitacion_id);
CREATE INDEX IF NOT EXISTS idx_updates_vector_procesado ON updates(vector_procesado);
CREATE INDEX IF NOT EXISTS idx_updates_metadata ON updates USING gin(metadata);

-- Función para actualizar updated_at automáticamente
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Trigger para actualizar updated_at
CREATE TRIGGER update_updates_updated_at BEFORE UPDATE
    ON updates FOR EACH ROW EXECUTE PROCEDURE
    update_updated_at_column();