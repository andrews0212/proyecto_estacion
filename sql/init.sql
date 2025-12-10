-- Crear la tabla sensor_readings si no existe
CREATE TABLE IF NOT EXISTS sensor_readings (
    id SERIAL PRIMARY KEY,
    sensor_id VARCHAR(50) NOT NULL,
    temperature DECIMAL(5,2),
    humidity DECIMAL(5,2),
    pressure DECIMAL(7,2),
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    location VARCHAR(100),
    status VARCHAR(20) DEFAULT 'active'
);

-- Insertar datos de ejemplo
INSERT INTO sensor_readings (sensor_id, temperature, humidity, pressure, location, status) VALUES
('TEMP_001', 23.5, 65.2, 1013.25, 'Oficina Principal', 'active'),
('TEMP_002', 21.8, 58.7, 1012.85, 'Sala de Servidores', 'active'),
('TEMP_003', 25.1, 72.3, 1014.10, 'Almacén', 'active'),
('TEMP_004', 19.6, 45.8, 1011.95, 'Exterior', 'active'),
('TEMP_005', 22.3, 62.1, 1013.50, 'Laboratorio', 'active');

-- Crear índices para optimizar consultas
CREATE INDEX IF NOT EXISTS idx_sensor_readings_timestamp ON sensor_readings(timestamp);
CREATE INDEX IF NOT EXISTS idx_sensor_readings_sensor_id ON sensor_readings(sensor_id);
CREATE INDEX IF NOT EXISTS idx_sensor_readings_status ON sensor_readings(status);