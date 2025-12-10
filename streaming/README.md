# 🔄 Sistema de Streaming en Tiempo Real

Sistema de procesamiento de datos de sensores en streaming:
- **PostgreSQL** → **Kafka** → **ClickHouse** → **Grafana**

## 📋 Arquitectura

```
PostgreSQL (BD Real)          PostgreSQL (Simulado)
     ↓                              ↓
Producer Real                  Producer Simulado
     ↓                              ↓
         → Kafka (streaming) ←
                  ↓
         Consumer ClickHouse
                  ↓
            ClickHouse (BD)
                  ↓
              Grafana (Visualización)
```

## 🚀 Componentes

### 1. **kafka_producer_simulated.py**
Genera datos simulados de sensores para pruebas.

**Uso:**
```bash
python streaming/kafka_producer_simulated.py
```

**Características:**
- 3 sensores simulados
- Datos realistas (temperatura, humedad, presión, PM2.5, luz)
- Envía datos cada 10 segundos
- Ideal para desarrollo y pruebas

### 2. **kafka_producer_real.py**
Lee datos reales de PostgreSQL y los envía a Kafka.

**Uso:**
```bash
python streaming/kafka_producer_real.py
```

**Características:**
- Conecta a PostgreSQL
- Lee datos incrementales (solo nuevos registros)
- Guarda checkpoint en `last_kafka_timestamp.txt`
- Envía datos cada 10 segundos
- Para uso en producción

**Requiere:**
- PostgreSQL ejecutándose
- Tabla `sensor_readings` con datos

### 3. **kafka_consumer_clickhouse.py**
Consumer que recibe datos de Kafka y los inserta en ClickHouse.

**Uso:**
```bash
python streaming/kafka_consumer_clickhouse.py
```

**Características:**
- Conecta a Kafka
- Inserta en ClickHouse tabla `sensor_streaming`
- Crea la tabla automáticamente
- Procesa hasta 100 mensajes por lote
- Logging detallado

**Requiere:**
- ClickHouse ejecutándose
- Kafka ejecutándose

## 📊 Flujo de Ejecución

### Para Pruebas (con datos simulados):

1. **Terminal 1**: Iniciar producer simulado
```bash
python streaming/kafka_producer_simulated.py
```

2. **Terminal 2**: Iniciar consumer ClickHouse
```bash
python streaming/kafka_consumer_clickhouse.py
```

3. **Grafana**: Acceder a http://localhost:3000
   - Usuario: admin
   - Contraseña: admin

### Para Producción (con datos reales):

1. **Terminal 1**: Asegurar que PostgreSQL tiene datos
```bash
python manage.py incremental
```

2. **Terminal 1**: Iniciar producer real
```bash
python streaming/kafka_producer_real.py
```

3. **Terminal 2**: Iniciar consumer ClickHouse
```bash
python streaming/kafka_consumer_clickhouse.py
```

4. **Grafana**: Crear dashboard con tabla `sensor_streaming`

## 🔧 Configuración

### Kafka
- Host: `localhost:9092`
- Topic: `sensor_data_streaming`
- Consumer Group: `clickhouse_consumer`

### ClickHouse
- Host: `localhost:8123`
- Base de datos: `meteo`
- Tabla: `sensor_streaming`
- Usuario: `default`
- Contraseña: `clickhouse`

### PostgreSQL
- Host: `localhost`
- Puerto: `5432`
- Base de datos: `proyecto_estacion`
- Usuario: `proyecto_user`
- Contraseña: `proyecto_pass`

## 📈 Queries Útiles en ClickHouse

### Ver últimos datos
```sql
SELECT * FROM sensor_streaming 
ORDER BY timestamp DESC 
LIMIT 100;
```

### Datos por sensor
```sql
SELECT 
    sensor_id,
    COUNT(*) as total,
    AVG(temperature) as temp_promedio,
    AVG(humidity) as humidity_promedio
FROM sensor_streaming
GROUP BY sensor_id;
```

### Datos últimos 10 minutos
```sql
SELECT *
FROM sensor_streaming
WHERE timestamp > now() - INTERVAL 10 MINUTE
ORDER BY timestamp DESC;
```

### Alertas de PM2.5 alto
```sql
SELECT *
FROM sensor_streaming
WHERE pm25 > 100
ORDER BY timestamp DESC;
```

## 📦 Dependencias

```
kafka-python
psycopg2
clickhouse-driver
```

**Instalar:**
```bash
pip install kafka-python psycopg2 clickhouse-driver
```

## ⚡ Tips

1. **Monitoreo en tiempo real**: Abre dos terminales
   - Una con producer
   - Otra con consumer
   - Vigila los logs

2. **Datos históricos**: El producer real se ajusta con `last_kafka_timestamp.txt`

3. **Simulado vs Real**: Cambiar de simulado a real solo requiere cambiar el producer

4. **Debugging**: Los logs muestran timestamps y detalles de cada operación

## 🐛 Solución de Problemas

### Error: "No se puede conectar a Kafka"
- Verifica: `docker-compose ps` (Kafka debe estar corriendo)

### Error: "No se puede conectar a ClickHouse"
- Verifica: `docker-compose ps` (ClickHouse debe estar corriendo)

### Error: "No se puede conectar a PostgreSQL"
- Verifica: `docker ps | grep postgres` (PostgreSQL debe estar corriendo)

### No hay datos en ClickHouse
- Verifica que el consumer esté ejecutándose
- Verifica que el producer esté enviando datos a Kafka
- Revisa los logs para errores

## 📞 Próximos Pasos

1. Crear dashboard en Grafana con los datos en tiempo real
2. Configurar alertas basadas en umbrales de PM2.5
3. Crear agregaciones en ClickHouse para reportes históricos
