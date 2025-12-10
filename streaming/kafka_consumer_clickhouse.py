#!/usr/bin/env python3
"""
Consumer de Kafka que recibe datos de sensores y los inserta en ClickHouse
"""

import json
import logging
from datetime import datetime
from kafka import KafkaConsumer
import clickhouse_driver

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuración de Kafka
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "sensor_data_streaming"
KAFKA_GROUP = "clickhouse_consumer"

# Configuración de ClickHouse
CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = 9010  # Puerto remapeado desde contenedor 9000 → host 9010
CLICKHOUSE_DB = "meteo"
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASSWORD = "clickhouse"

def crear_consumer_kafka():
    """Crear consumer de Kafka"""
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=[KAFKA_BROKER],
            group_id=None,  # Sin grupo para evitar coordinador
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            consumer_timeout_ms=1000,
            max_poll_records=100
        )
        logger.info(f"✅ Conectado a Kafka: {KAFKA_BROKER}")
        logger.info(f"📊 Topic: {KAFKA_TOPIC}")
        logger.info(f"👥 Consumer sin grupo (direct subscription)")
        return consumer
    except Exception as e:
        logger.error(f"❌ Error conectando a Kafka: {e}")
        return None

def conectar_clickhouse():
    """Conectar a ClickHouse"""
    try:
        client = clickhouse_driver.Client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            database=CLICKHOUSE_DB,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD
        )
        logger.info(f"✅ Conectado a ClickHouse: {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}")
        return client
    except Exception as e:
        logger.error(f"❌ Error conectando a ClickHouse: {e}")
        return None

def crear_tabla_clickhouse(client):
    """Crear tabla en ClickHouse si no existe - campos idénticos a PostgreSQL"""
    try:
        query = """
        CREATE TABLE IF NOT EXISTS sensor_streaming (
            id UInt32,
            sensor_id String,
            temperature Float32,
            humidity Float32,
            pressure Float32,
            timestamp String,
            pm25 Float32,
            light UInt32,
            uv_level UInt32,
            rain_raw UInt32,
            wind_raw UInt32,
            vibration Boolean,
            location String,
            ingestion_timestamp String,
            batch_id String,
            inserted_at DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY (timestamp, sensor_id)
        """
        
        client.execute(query)
        logger.info("✅ Tabla 'sensor_streaming' verificada/creada")
    except Exception as e:
        logger.error(f"⚠️  Error creando tabla: {e}")

def insertar_en_clickhouse(client, dato):
    """Insertar un dato en ClickHouse"""
    try:
        query = """
        INSERT INTO sensor_streaming 
        (id, sensor_id, temperature, humidity, pressure, timestamp, pm25, 
         light, uv_level, rain_raw, wind_raw, vibration, location, 
         ingestion_timestamp, batch_id)
        VALUES
        """
        
        # Preparar el dato con los campos correctos
        valores = (
            int(dato.get('id', 0)),
            dato.get('sensor_id', 'Unknown'),
            float(dato.get('temperature', 0)),
            float(dato.get('humidity', 0)),
            float(dato.get('pressure', 0)),
            dato.get('timestamp', ''),
            float(dato.get('pm25', 0)),
            int(dato.get('light', 0)),
            int(dato.get('uv_level', 0)),
            int(dato.get('rain_raw', 0)),
            int(dato.get('wind_raw', 0)),
            bool(dato.get('vibration', False)),
            dato.get('location', 'Unknown'),
            dato.get('ingestion_timestamp', ''),
            dato.get('batch_id', '')
        )
        
        client.execute(query, [valores])
        return True
    except Exception as e:
        logger.error(f"Error insertando en ClickHouse: {e}")
        return False

def main():
    """Función principal"""
    print("\n")
    print("╔" + "="*68 + "╗")
    print("║ 🔄 KAFKA → CLICKHOUSE CONSUMER                               ║")
    print("║ Recibe datos de sensores y los almacena en ClickHouse        ║")
    print("╚" + "="*68 + "╝\n")
    
    # Conectar a Kafka
    consumer = crear_consumer_kafka()
    if not consumer:
        return
    
    # Conectar a ClickHouse
    client = conectar_clickhouse()
    if not client:
        return
    
    # Crear tabla
    crear_tabla_clickhouse(client)
    
    logger.info("⛔ Presiona Ctrl+C para detener\n")
    
    contador_mensajes = 0
    contador_insertados = 0
    contador_errores = 0
    
    try:
        for message in consumer:
            contador_mensajes += 1
            
            try:
                dato = message.value
                timestamp = datetime.now().strftime('%H:%M:%S')
                
                # Insertar en ClickHouse
                if insertar_en_clickhouse(client, dato):
                    contador_insertados += 1
                    logger.info(
                        f"[{timestamp}] ✅ {dato.get('sensor_id', 'Unknown'):20} | "
                        f"T: {dato.get('temperature', 0):6.1f}°C | "
                        f"H: {dato.get('humidity', 0):6.1f}% | "
                        f"PM2.5: {dato.get('pm25', 0):6.1f}"
                    )
                else:
                    contador_errores += 1
            
            except Exception as e:
                contador_errores += 1
                logger.error(f"Error procesando mensaje: {e}")
    
    except KeyboardInterrupt:
        logger.info("\n\n⛔ DETENIENDO CONSUMER")
        logger.info(f"📊 Mensajes recibidos: {contador_mensajes}")
        logger.info(f"✅ Datos insertados: {contador_insertados}")
        logger.info(f"❌ Errores: {contador_errores}")
        logger.info("="*70)
        consumer.close()
        logger.info("Conexión cerrada")

if __name__ == "__main__":
    main()
