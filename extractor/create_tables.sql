-- Crear tabla de licitaciones si no existe
CREATE TABLE IF NOT EXISTS licitaciones (
    id SERIAL PRIMARY KEY,
    titulo VARCHAR(500) NOT NULL,
    descripcion TEXT,
    fecha_publicacion DATE,
    fecha_limite DATE,
    dependencia VARCHAR(300),
    url VARCHAR(500) UNIQUE NOT NULL,
    fuente VARCHAR(100) NOT NULL,
    monto DECIMAL(15, 2),
    moneda VARCHAR(10),
    categoria VARCHAR(100),
    ubicacion VARCHAR(200),
    contacto TEXT,
    requisitos TEXT,
    estado VARCHAR(50),
    numero_licitacion VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Crear índices para mejorar búsquedas
CREATE INDEX IF NOT EXISTS idx_licitaciones_fecha_publicacion ON licitaciones(fecha_publicacion);
CREATE INDEX IF NOT EXISTS idx_licitaciones_fecha_limite ON licitaciones(fecha_limite);
CREATE INDEX IF NOT EXISTS idx_licitaciones_fuente ON licitaciones(fuente);
CREATE INDEX IF NOT EXISTS idx_licitaciones_categoria ON licitaciones(categoria);
CREATE INDEX IF NOT EXISTS idx_licitaciones_created_at ON licitaciones(created_at);