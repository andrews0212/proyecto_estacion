#!/usr/bin/env python3
"""
Script para enviar datos reales de sensores desde PostgreSQL a Kafka
Conecta directamente a la base de datos y envía datos en streaming
"""

import json
import time
import logging
from datetime import datetime, timedelta
from kafka import KafkaProducer
import psycopg2
from psycopg2.extras import RealDictCursor

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuración de PostgreSQL
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "proyecto_estacion",
    "user": "proyecto_user",
    "password": "proyecto_pass"
}

# Configuración de Kafka
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "sensor_data_streaming"

# Configuración de polling
POLLING_INTERVAL = 10  # Segundos entre consultas
LAST_TIMESTAMP_FILE = "last_kafka_timestamp.txt"

def crear_productor_kafka():
    """Crear productor de Kafka"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=3,
            retry_backoff_ms=100
        )
        logger.info(f"✅ Conexión a Kafka exitosa: {KAFKA_BROKER}")
        return producer
    except Exception as e:
        logger.error(f"❌ Error conectando a Kafka: {e}")
        return None

def conectar_postgresql():
    """Conectar a PostgreSQL"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        logger.info(f"✅ Conexión a PostgreSQL exitosa: {DB_CONFIG['host']}")
        return conn
    except Exception as e:
        logger.error(f"❌ Error conectando a PostgreSQL: {e}")
        return None

def obtener_ultimo_timestamp():
    """Obtener el último timestamp procesado"""
    try:
        with open(LAST_TIMESTAMP_FILE, 'r') as f:
            timestamp = f.read().strip()
            return timestamp
    except FileNotFoundError:
        # Si no existe, usar hace 1 hora
        return (datetime.now() - timedelta(hours=1)).isoformat()

def guardar_ultimo_timestamp(timestamp):
    """Guardar el último timestamp procesado"""
    try:
        with open(LAST_TIMESTAMP_FILE, 'w') as f:
            f.write(timestamp)
    except Exception as e:
        logger.error(f"Error guardando timestamp: {e}")

def obtener_nuevos_datos(conn, ultimo_timestamp):
    """Obtener nuevos datos desde PostgreSQL después del último timestamp"""
    try:
        query = """
            SELECT 
                timestamp,
                sensor_id,
                temperature,
                humidity,
                pressure,
                pm25,
                light,
                CASE 
                    WHEN pm25 > 150 THEN 'Peligroso'
                    WHEN pm25 > 100 THEN 'Muy insalubre'
                    WHEN pm25 > 50 THEN 'Insalubre'
                    WHEN pm25 > 25 THEN 'Moderado'
                    ELSE 'Bueno'
                END as air_quality
            FROM sensor_readings
            WHERE timestamp > %s
            ORDER BY timestamp ASC
            LIMIT 100
        """
        
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (ultimo_timestamp,))
            datos = cur.fetchall()
            return [dict(row) for row in datos]
    
    except Exception as e:
        logger.error(f"Error consultando PostgreSQL: {e}")
        return []

def formatear_dato(row):
    """Formatear dato de PostgreSQL para Kafka"""
    return {
        "timestamp": row['timestamp'].isoformat() if isinstance(row['timestamp'], object) else str(row['timestamp']),
        "sensor_id": row['sensor_id'],
        "sensor_name": f"Sensor {row['sensor_id'][-2:]}",
        "location": "Estación",
        "temperature": round(float(row['temperature']), 2),
        "humidity": round(float(row['humidity']), 2),
        "pressure": round(float(row['pressure']), 2),
        "pm25": round(float(row['pm25']), 2),
        "light": round(float(row['light']), 2),
        "air_quality": row['air_quality']
    }

def main():
    """Función principal"""
    print("\n")
    print("╔" + "="*68 + "╗")
    print("║ 🔄 STREAMING DE DATOS REALES PostgreSQL → Kafka              ║")
    print("║ Modo: PRODUCCIÓN (datos de sensores reales)                  ║")
    print("╚" + "="*68 + "╝\n")
    
    # Conectar a Kafka
    producer = crear_productor_kafka()
    if not producer:
        return
    
    # Conectar a PostgreSQL
    conn = conectar_postgresql()
    if not conn:
        return
    
    logger.info(f"📊 Tema de Kafka: {KAFKA_TOPIC}")
    logger.info(f"🗄️  Base de datos: {DB_CONFIG['database']}")
    logger.info(f"⏱️  Intervalo de polling: {POLLING_INTERVAL}s")
    logger.info("⛔ Presiona Ctrl+C para detener\n")
    
    contador_ciclos = 0
    contador_datos = 0
    
    try:
        while True:
            contador_ciclos += 1
            timestamp_actual = datetime.now().strftime('%H:%M:%S')
            
            # Obtener último timestamp procesado
            ultimo_timestamp = obtener_ultimo_timestamp()
            
            # Obtener nuevos datos
            datos = obtener_nuevos_datos(conn, ultimo_timestamp)
            
            if datos:
                logger.info(f"[{timestamp_actual}] 📥 {len(datos)} nuevos registros encontrados")
                
                # Enviar cada dato a Kafka
                for row in datos:
                    try:
                        dato = formatear_dato(row)
                        producer.send(KAFKA_TOPIC, value=dato)
                        
                        logger.info(
                            f"  📤 {dato['sensor_id']:20} | "
                            f"T: {dato['temperature']:6.1f}°C | "
                            f"H: {dato['humidity']:6.1f}% | "
                            f"PM2.5: {dato['pm25']:6.1f} | "
                            f"Calidad: {dato['air_quality']}"
                        )
                        
                        contador_datos += 1
                        
                        # Guardar timestamp después de cada envío
                        guardar_ultimo_timestamp(dato['timestamp'])
                    
                    except Exception as e:
                        logger.error(f"Error enviando dato: {e}")
                
                logger.info(f"✅ Lote #{contador_ciclos} completado\n")
            else:
                logger.info(f"[{timestamp_actual}] ⏳ Sin nuevos datos (Ciclo #{contador_ciclos})\n")
            
            # Esperar antes de siguiente consulta
            time.sleep(POLLING_INTERVAL)
    
    except KeyboardInterrupt:
        logger.info("\n\n⛔ DETENIENDO STREAMING DE DATOS")
        logger.info(f"⏰ Ciclos ejecutados: {contador_ciclos}")
        logger.info(f"📊 Datos enviados: {contador_datos}")
        logger.info("="*70)
        
        producer.flush()
        producer.close()
        conn.close()
        logger.info("Conexiones cerradas")

if __name__ == "__main__":
    main()
