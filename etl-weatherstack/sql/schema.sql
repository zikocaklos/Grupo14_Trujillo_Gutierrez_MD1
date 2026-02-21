-- Crear esquema para data warehousing de clima

-- Tabla principal de datos climáticos
CREATE TABLE IF NOT EXISTS clima (
    id SERIAL PRIMARY KEY,
    ciudad VARCHAR(100) NOT NULL,
    pais VARCHAR(100),
    temperatura NUMERIC(5,2),
    sensacion_termica NUMERIC(5,2),
    temp_minima NUMERIC(5,2),
    temp_maxima NUMERIC(5,2),
    presion INTEGER,
    humedad INTEGER,
    descripcion VARCHAR(200),
    viento_velocidad NUMERIC(5,2),
    nubosidad INTEGER,
    timestamp TIMESTAMP,
    fecha_ingesta TIMESTAMP DEFAULT NOW(),
    UNIQUE(ciudad, timestamp)
);

-- Índices para optimizar consultas
CREATE INDEX IF NOT EXISTS idx_clima_ciudad ON clima(ciudad);
CREATE INDEX IF NOT EXISTS idx_clima_timestamp ON clima(timestamp);
CREATE INDEX IF NOT EXISTS idx_clima_ciudad_timestamp ON clima(ciudad, timestamp);
CREATE INDEX IF NOT EXISTS idx_clima_fecha_ingesta ON clima(fecha_ingesta);

-- Tabla de métricas de ingesta
CREATE TABLE IF NOT EXISTS metricas_ingesta (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    registros_procesados INTEGER,
    registros_insertados INTEGER,
    estado VARCHAR(50),
    fecha_registro TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_metricas_timestamp ON metricas_ingesta(timestamp);
CREATE INDEX IF NOT EXISTS idx_metricas_estado ON metricas_ingesta(estado);

-- Vista materializada para resumen diario
CREATE OR REPLACE VIEW clima_diario AS
SELECT
    DATE(timestamp) as fecha,
    ciudad,
    pais,
    COUNT(*) as observaciones,
    AVG(temperatura) as temp_promedio,
    MAX(temperatura) as temp_maxima,
    MIN(temperatura) as temp_minima,
    AVG(humedad) as humedad_promedio,
    AVG(presion) as presion_promedio,
    ARRAY_AGG(DISTINCT descripcion) as descripciones
FROM clima
WHERE timestamp IS NOT NULL
GROUP BY DATE(timestamp), ciudad, pais
ORDER BY fecha DESC, ciudad;

-- Vista para análisis mensual
CREATE OR REPLACE VIEW clima_mensual AS
SELECT
    DATE_TRUNC('month', timestamp)::DATE as mes,
    ciudad,
    pais,
    AVG(temperatura) as temp_promedio_mes,
    MAX(temperatura) as temp_maxima_mes,
    MIN(temperatura) as temp_minima_mes,
    AVG(humedad) as humedad_promedio_mes,
    COUNT(*) as observaciones_mes
FROM clima
WHERE timestamp IS NOT NULL
GROUP BY DATE_TRUNC('month', timestamp), ciudad, pais
ORDER BY mes DESC, ciudad;

-- Vista para anomalías de temperatura
CREATE OR REPLACE VIEW clima_anomalias AS
SELECT
    ciudad,
    DATE(timestamp) as fecha,
    MAX(temperatura) as temp_maxima,
    MIN(temperatura) as temp_minima,
    AVG(temperatura) as temp_promedio,
    (MAX(temperatura) - MIN(temperatura)) as amplitud_termica
FROM clima
WHERE timestamp IS NOT NULL
GROUP BY ciudad, DATE(timestamp)
HAVING (MAX(temperatura) - MIN(temperatura)) > 10
ORDER BY fecha DESC, amplitud_termica DESC;

-- Tabla de hecho para análisis multidimensional
CREATE TABLE IF NOT EXISTS hecho_clima (
    id SERIAL PRIMARY KEY,
    id_clima INTEGER REFERENCES clima(id),
    ciudad VARCHAR(100),
    date_key DATE,
    temperatura_promedio NUMERIC(5,2),
    temperatura_maxima NUMERIC(5,2),
    temperatura_minima NUMERIC(5,2),
    humedad_promedio INTEGER,
    presion_promedio INTEGER,
    condicion_clima VARCHAR(200),
    fecha_ingesta TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_hecho_ciudad_date ON hecho_clima(ciudad, date_key);

-- Tabla de dimensión de tiempo
CREATE TABLE IF NOT EXISTS dim_tiempo (
    date_key DATE PRIMARY KEY,
    fecha DATE,
    año INTEGER,
    mes INTEGER,
    dia INTEGER,
    trimestre INTEGER,
    semana INTEGER,
    dia_semana INTEGER,
    es_fin_semana BOOLEAN
);

-- Table de dimensión de ubicación
CREATE TABLE IF NOT EXISTS dim_ubicacion (
    id SERIAL PRIMARY KEY,
    ciudad VARCHAR(100),
    pais VARCHAR(100),
    latitud NUMERIC(9,6),
    longitud NUMERIC(9,6),
    region VARCHAR(100),
    UNIQUE(ciudad, pais)
);

-- Permisos y seguridad
GRANT SELECT ON clima TO dataminer;
GRANT SELECT ON metricas_ingesta TO dataminer;
GRANT SELECT ON clima_diario TO dataminer;
GRANT SELECT ON clima_mensual TO dataminer;
GRANT DELETE, INSERT, UPDATE ON clima TO dataminer;
GRANT DELETE, INSERT, UPDATE ON metricas_ingesta TO dataminer;

-- Comentarios para documentación
COMMENT ON TABLE clima IS 'Tabla principal con datos climáticos en tiempo real de OpenWeather';
COMMENT ON TABLE metricas_ingesta IS 'Métricas de ejecución del pipeline ETL';
COMMENT ON VIEW clima_diario IS 'Resumen diario de datos climáticos por ciudad';
COMMENT ON VIEW clima_mensual IS 'Análisis mensual de climatología por ciudad';
COMMENT ON VIEW clima_anomalias IS 'Detección de anomalías de temperatura con amplitud > 10°C';

-- Habilitar extensión de UUID si es necesario
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Log de auditoria (opcional)
CREATE TABLE IF NOT EXISTS auditoria_cambios (
    id SERIAL PRIMARY KEY,
    tabla VARCHAR(50),
    operacion VARCHAR(10),
    usuario VARCHAR(100) DEFAULT CURRENT_USER,
    fecha_cambio TIMESTAMP DEFAULT NOW(),
    detalles JSONB
);

CREATE INDEX IF NOT EXISTS idx_auditoria_tabla_fecha ON auditoria_cambios(tabla, fecha_cambio);
