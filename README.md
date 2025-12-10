# Sistema ETL de Datos de Sensores

Este proyecto implementa un pipeline ETL completo para datos de sensores utilizando arquitectura de capas Bronze, Silver y Gold con MinIO como data lake y PySpark para procesamiento.

## 🏗️ Arquitectura

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   PostgreSQL    │    │  Bronze Layer   │    │  Silver Layer   │    │   Gold Layer    │
│   (Fuente)      │───▶│  (Raw Data)     │───▶│ (Clean Data)    │───▶│    (KPIs)       │
└─────────────────┘    └─────────────────┘    └─────────────────┘    └─────────────────┘
                                │                        │                        │
                                ▼                        ▼                        ▼
                        ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
                        │     MinIO       │    │    PySpark      │    │   Dashboard     │
                        │  Data Storage   │    │   Processing    │    │   & Exports     │
                        └─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 📦 Componentes

### Capas de Datos
- **Bronze**: Datos crudos extraídos de PostgreSQL
- **Silver**: Datos limpios y transformados con PySpark
- **Gold**: KPIs y métricas para consumo

### Servicios
- **PostgreSQL**: Base de datos fuente con tabla `sensor_readings`
- **MinIO**: Data Lake compatible con S3
- **ClickHouse**: OLAP para análisis (opcional)
- **Grafana**: Visualización y dashboards
- **Kafka**: Streaming de datos (opcional)

## 🚀 Instalación y Configuración

### 1. Clonar y configurar el entorno

```bash
# Navegar al directorio del proyecto
cd proyecto_estacion

# Crear red Docker (opcional)
docker network create estacion-network
```

### 2. Configurar variables de entorno

El archivo `.env` ya está configurado con valores por defecto. Modifica según sea necesario:

```env
# Base de datos
DB_HOST=localhost
DB_PORT=5432
DB_NAME=postgres
DB_USER=postgres
DB_PASSWORD=postgres

# MinIO
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
```

### 3. Levantar los servicios

```bash
# Iniciar todos los servicios
docker-compose up -d

# Verificar que los servicios estén ejecutándose
docker-compose ps
```

### 4. Verificar conexiones

```bash
# Probar conexiones del sistema ETL
cd etl
python manage.py test
```

## 🔧 Uso del Sistema

### Comandos Principales

```bash
# Ver estado del sistema
python manage.py status

# Carga inicial de datos (primera vez)
python manage.py initial-load

# Actualización incremental
python manage.py incremental

# Pipeline completo
python manage.py full-pipeline

# Exportar KPIs
python manage.py export-kpis

# Listar archivos en data lake
python manage.py list-files

# Limpiar datos antiguos
python manage.py cleanup
```

### Ejecución Automática

El sistema incluye un scheduler que ejecuta automáticamente:
- **ETL incremental**: Cada 30 minutos (configurable)
- **Pipeline completo**: Diariamente a las 2:00 AM
- **Limpieza**: Semanalmente los domingos a las 3:00 AM
- **Health check**: Cada hora

```bash
# Iniciar scheduler
python scheduler.py
```

## 📊 Notebooks de Análisis

### Silver Layer - Limpieza de Datos
- **Archivo**: `notebooks/silver_data_cleaning.ipynb`
- **Descripción**: Limpieza y transformación con PySpark
- **Funciones**: Eliminación de duplicados, validación, imputación

### Gold Layer - Análisis de KPIs
- **Archivo**: `notebooks/gold_kpis_analysis.ipynb`
- **Descripción**: Generación de KPIs y visualizaciones
- **Funciones**: Métricas, alertas, dashboards, exportación

## 🗃️ Estructura de Datos

### Tabla de Origen (sensor_readings)
```sql
CREATE TABLE sensor_readings (
    id SERIAL PRIMARY KEY,
    sensor_id VARCHAR(50) NOT NULL,
    temperature DECIMAL(5,2),
    humidity DECIMAL(5,2),
    pressure DECIMAL(7,2),
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    location VARCHAR(100),
    status VARCHAR(20) DEFAULT 'active'
);
```

### Capas de MinIO
- `bronze/sensor_data/`: Datos crudos por lotes
- `silver/sensor_data/`: Datos limpios consolidados
- `gold/sensor_kpis/`: KPIs y métricas
- `gold/exports/`: Archivos para consumo externo

## 📈 KPIs Generados

### Generales
- Total de lecturas procesadas
- Número de sensores activos
- Promedios globales (temperatura, humedad, presión)

### Por Sensor
- Estadísticas por sensor individual
- Calidad de datos por sensor
- Detección de anomalías

### Por Ubicación
- Condiciones ambientales por zona
- Índices de confort
- Distribución de sensores

### Calidad de Datos
- Puntuación de completitud
- Tasa de anomalías
- Registros de alta calidad

### Alertas
- Temperaturas extremas
- Humedad fuera de rango
- Presión anómala

## 🌐 Acceso a Interfaces Web

- **MinIO Console**: http://localhost:9090
  - Usuario: `minioadmin` / Contraseña: `minioadmin`
- **Grafana**: http://localhost:3000
  - Usuario: `admin` / Contraseña: `admin`
- **ClickHouse**: http://localhost:8123

## 🔄 Flujo de Procesamiento

1. **Extracción (Bronze)**:
   - Conexión a PostgreSQL
   - Extracción en lotes configurables
   - Almacenamiento en MinIO como Parquet

2. **Transformación (Silver)**:
   - Carga de datos Bronze
   - Limpieza con PySpark:
     - Eliminación de duplicados
     - Validación de rangos
     - Imputación de valores faltantes
     - Enriquecimiento de datos
   - Almacenamiento de datos limpios

3. **Agregación (Gold)**:
   - Procesamiento de datos Silver
   - Generación de KPIs
   - Exportación para consumo

## ⚙️ Configuración Avanzada

### Cambiar IP de Base de Datos
Modifica el archivo `.env`:
```env
DB_HOST=nueva.ip.de.database
```

### Ajustar Tamaño de Lotes
```env
BATCH_SIZE=2000
```

### Cambiar Intervalo de Procesamiento
```env
PROCESSING_INTERVAL_MINUTES=15
```

### Configuración de Spark
```env
SPARK_DRIVER_MEMORY=4g
SPARK_EXECUTOR_MEMORY=4g
SPARK_EXECUTOR_CORES=4
```

## 🐛 Resolución de Problemas

### Error de Conexión a PostgreSQL
1. Verificar que el servicio esté ejecutándose
2. Comprobar credenciales en `.env`
3. Verificar conectividad de red

### Error de MinIO
1. Verificar que el servicio esté activo: `docker-compose ps`
2. Comprobar acceso web en http://localhost:9090
3. Verificar permisos de bucket

### Error en PySpark
1. Verificar memoria disponible
2. Ajustar configuración en `.env`
3. Revisar logs en `logs/etl.log`

### Sin Datos en Bronze
```bash
# Ejecutar carga inicial
python manage.py initial-load
```

## 📝 Logs

Los logs del sistema se almacenan en:
- `logs/etl.log`: Log principal del ETL
- Docker logs: `docker-compose logs etl-service`

## 🤝 Consumo por Otros Módulos

Los KPIs se exportan automáticamente en formato Parquet y están disponibles para consumo:

```python
# Ejemplo de consumo
from etl.utils import MinIOClient
from etl.config import Config

minio_client = MinIOClient()
kpis_df = minio_client.download_parquet_as_dataframe(
    f"{Config.GOLD_PATH}/exports/latest_kpis.parquet"
)
```

## 📞 Soporte

Para soporte técnico o preguntas sobre el sistema:
1. Revisar logs en `logs/etl.log`
2. Ejecutar `python manage.py status` para diagnóstico
3. Consultar notebooks para ejemplos de uso