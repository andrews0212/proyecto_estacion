#!/usr/bin/env python3
"""
Script para generar datos simulados de sensores y enviarlos a Kafka
Usa los mismos campos que los datos reales de PostgreSQL
"""

import json
import time
import logging
import random
from datetime import datetime
from kafka import KafkaProducer

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ==================== CONFIGURACIÓN ====================

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "sensor_data_streaming"

# Sensores simulados - IDs reales de tus datos
SENSORS = [
    "192.168.1.50",
    "10.207.51.79",
    "192.168.1.51",
]

# ==================== FUNCIONES ====================

def generate_sensor_data(sensor_id):
    """Generar datos realistas de sensor con campos idénticos a BD real"""
    
    # Variaciones realistas basadas en los datos reales
    base_temp = random.uniform(5, 35)
    base_humidity = random.uniform(20, 95)
    base_pressure = random.uniform(990, 1030)
    base_pm25 = random.uniform(0, 150)
    
    return {
        "id": random.randint(1, 50000),
        "sensor_id": sensor_id,
        "temperature": round(base_temp, 2),
        "humidity": round(base_humidity, 2),
        "pressure": round(base_pressure, 1),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "pm25": round(base_pm25, 1),
        "light": random.randint(0, 100000),
        "uv_level": random.randint(0, 15),
        "rain_raw": random.randint(0, 800),
        "wind_raw": random.randint(0, 400),
        "vibration": random.choice([True, False]),
        "location": "Unknown",
        "ingestion_timestamp": datetime.utcnow().isoformat(),
        "batch_id": f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    }

def run_producer():
    """Ejecutar el productor de Kafka"""
    try:
        # Conexión a Kafka
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=3,
            retry_backoff_ms=100
        )
        logger.info(f"✅ Conexión a Kafka exitosa: {KAFKA_BROKER}")
        logger.info(f"📊 Tema de Kafka: {KAFKA_TOPIC}")
        logger.info(f"🎲 Sensores simulados: {len(SENSORS)}")
        logger.info(f"⏱️  Enviando datos cada 10 segundos...")
        logger.info(f"⛔ Presiona Ctrl+C para detener")
        
        batch_num = 0
        while True:
            batch_num += 1
            for sensor_id in SENSORS:
                data = generate_sensor_data(sensor_id)
                producer.send(KAFKA_TOPIC, value=data)
                
                # Log formateado
                time_str = data['timestamp'].split('T')[1][:8] if 'T' in data['timestamp'] else "??:??:??"
                logger.info(
                    f"[{time_str}] 📤 Sensor {sensor_id:<18} | "
                    f"T: {data['temperature']:>6.1f}°C | "
                    f"H: {data['humidity']:>6.1f}% | "
                    f"PM2.5: {data['pm25']:>6.1f} | "
                    f"Presión: {data['pressure']:>7.1f}"
                )
            
            producer.flush()
            logger.info(f"⏳ Esperando 10 segundos... (Lote #{batch_num})")
            time.sleep(10)
            
    except KeyboardInterrupt:
        logger.info("\n\n⛔ DETENIENDO GENERADOR DE DATOS")
        logger.info(f"⏰ Datos enviados: {batch_num} lotes")
        logger.info("="*70)
    except Exception as e:
        logger.error(f"❌ Error: {str(e)}")
    finally:
        producer.close()
        logger.info("Conexión cerrada")

if __name__ == "__main__":
    print("""
╔════════════════════════════════════════════════════════════════╗
║ 🔄 GENERADOR DE DATOS SIMULADOS → KAFKA                     ║
║ Campos idénticos a PostgreSQL local (sin ubicación ni nombre)║
╚════════════════════════════════════════════════════════════════╝
    """)
    run_producer()
