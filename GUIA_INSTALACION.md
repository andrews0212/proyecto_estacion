# 🌤️ Estación Meteorológica IoT - Guía de Instalación

Sistema completo de monitorización meteorológica en tiempo real usando:
- **Kafka** - Streaming de datos
- **ClickHouse** - Base de datos OLAP
- **Grafana** - Visualización
- **MinIO** - Almacenamiento S3
- **Python** - ETL y procesamiento

---

## 📋 Requisitos Previos

### Software necesario:
1. **Docker Desktop** - [Descargar](https://www.docker.com/products/docker-desktop/)
2. **Python 3.10+** - [Descargar](https://www.python.org/downloads/)
3. **Git** - [Descargar](https://git-scm.com/downloads)

### Verificar instalación:
```bash
docker --version
python --version
git --version
```

---

## 🚀 Instalación Paso a Paso

### Paso 1: Clonar el repositorio
```bash
git clone https://github.com/andrews0212/proyecto_estacion.git
cd proyecto_estacion
```

### Paso 2: Crear entorno virtual de Python
```bash
# Windows
python -m venv .venv
.venv\Scripts\activate

# Linux/Mac
python3 -m venv .venv
source .venv/bin/activate
```

### Paso 3: Instalar dependencias de Python
```bash
pip install -r requirements.txt
```

### Paso 4: Iniciar los contenedores Docker
```bash
docker-compose up -d
```

Esperar ~30 segundos a que todos los servicios arranquen.

### Paso 5: Verificar que los servicios están corriendo
```bash
docker ps
```

Deberías ver 5 contenedores:
- `zookeeper`
- `kafka`
- `minio`
- `clickhouse`
- `grafana`

### Paso 6: Crear la base de datos y tabla en ClickHouse
```bash
python scripts/init_clickhouse.py
```

### Paso 7: Configurar Grafana (datasource + dashboard)
```bash
python scripts/setup_grafana.py
```

---

## 🎯 Uso del Sistema

### Opción A: Simulador de datos (para pruebas)
```bash
python scripts/simulate_realtime.py
```
Inserta datos cada 2 segundos. Presiona `Ctrl+C` para detener.

### Opción B: Streaming real con Kafka
Terminal 1 - Productor (envía datos):
```bash
python streaming/kafka_producer_simulated.py
```

Terminal 2 - Consumidor (guarda en ClickHouse):
```bash
python streaming/kafka_consumer_clickhouse.py
```

### Opción C: ETL programado (cada 5 minutos)
```bash
python scheduler.py
```

---

## 🖥️ Acceso a las Interfaces Web

| Servicio | URL | Usuario | Contraseña |
|----------|-----|---------|------------|
| **Grafana** | http://localhost:3000 | admin | admin |
| **MinIO Console** | http://localhost:9090 | minioadmin | minioadmin |
| **ClickHouse HTTP** | http://localhost:8123 | default | clickhouse |

### Dashboard de Grafana:
👉 http://localhost:3000/d/estacion-meteo/

---

## 📁 Estructura del Proyecto

```
proyecto_estacion/
├── docker-compose.yml      # Configuración de contenedores
├── requirements.txt        # Dependencias Python
├── scheduler.py            # ETL programado
│
├── scripts/
│   ├── setup_grafana.py    # Configura Grafana automáticamente
│   ├── simulate_realtime.py # Simulador de datos
│   └── init_clickhouse.py  # Inicializa la base de datos
│
├── streaming/
│   ├── kafka_producer_simulated.py  # Productor Kafka
│   └── kafka_consumer_clickhouse.py # Consumidor Kafka
│
├── etl/
│   ├── orchestrator.py     # Orquestador ETL
│   ├── layers/             # Capas Bronze/Silver/Gold
│   └── utils/              # Utilidades
│
├── grafana/
│   └── dashboard_estacion_meteo.json  # Dashboard exportado
│
├── clickhouse/
│   ├── config.xml          # Configuración ClickHouse
│   └── users.xml           # Usuarios ClickHouse
│
└── sql/
    └── init.sql            # Script SQL inicial
```

---

## 🔧 Comandos Útiles

### Ver logs de un contenedor:
```bash
docker logs grafana
docker logs clickhouse
docker logs kafka
```

### Reiniciar un servicio:
```bash
docker restart grafana
```

### Detener todo:
```bash
docker-compose down
```

### Eliminar todo (incluyendo datos):
```bash
docker-compose down -v
```

### Consultar ClickHouse directamente:
```bash
docker exec -it clickhouse clickhouse-client --user default --password clickhouse
```

```sql
SELECT count() FROM meteo.sensor_streaming;
SELECT * FROM meteo.sensor_streaming ORDER BY inserted_at DESC LIMIT 10;
```

---

## 🐛 Solución de Problemas

### Grafana muestra "No data"
1. Verificar que hay datos en ClickHouse:
   ```bash
   docker exec clickhouse clickhouse-client --user default --password clickhouse -q "SELECT count() FROM meteo.sensor_streaming"
   ```
2. Re-ejecutar setup de Grafana:
   ```bash
   python scripts/setup_grafana.py
   ```

### Error de conexión a Kafka
1. Verificar que Kafka está corriendo:
   ```bash
   docker ps | grep kafka
   ```
2. Reiniciar Kafka:
   ```bash
   docker restart kafka
   ```

### ClickHouse no arranca
1. Ver logs:
   ```bash
   docker logs clickhouse
   ```
2. Verificar configuración XML en `clickhouse/`

---

## 📊 Flujo de Datos

```
┌─────────────┐     ┌─────────┐     ┌────────────┐     ┌─────────┐
│   Sensores  │ ──► │  Kafka  │ ──► │ ClickHouse │ ──► │ Grafana │
│  (IoT/Sim)  │     │         │     │   (OLAP)   │     │  (UI)   │
└─────────────┘     └─────────┘     └────────────┘     └─────────┘
                                           │
                                           ▼
                                    ┌────────────┐
                                    │   MinIO    │
                                    │ (Archivos) │
                                    └────────────┘
```

---

## 📝 Notas Importantes

1. **Primera vez**: Ejecutar los pasos del 1 al 7 en orden.
2. **Siguientes veces**: Solo `docker-compose up -d` y luego el script que necesites.
3. **Los datos persisten** en volúmenes Docker incluso si apagas el PC.
4. **El dashboard se guarda** en Grafana automáticamente.

---

## 🆘 Soporte

Si tienes problemas, revisa:
1. Que Docker Desktop esté corriendo
2. Que no haya otros servicios en los puertos 3000, 8123, 9092, 9000, 9090
3. Los logs de los contenedores

---

**¡Listo! Tu estación meteorológica está funcionando.** 🌤️
