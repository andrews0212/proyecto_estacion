#!/usr/bin/env python3
"""
Inserta datos de simulación en ClickHouse para pruebas de visualización en Grafana.
"""

import random
import time
from datetime import datetime, timedelta
from clickhouse_driver import Client

def main():
    print("🔌 Conectando a ClickHouse...")
    client = Client(
        host='localhost',
        port=9010,
        user='default',
        password='clickhouse',
        database='meteo'
    )
    
    # Verificar conexión
    result = client.execute("SELECT 1")
    print("✅ Conexión exitosa")
    
    # Verificar tabla
    tables = client.execute("SHOW TABLES FROM meteo")
    print(f"📋 Tablas en meteo: {[t[0] for t in tables]}")
    
    # Generar datos de simulación
    print("\n📊 Insertando datos de simulación...")
    
    sensors = ["SENSOR-001", "SENSOR-002", "SENSOR-003"]
    locations = ["Madrid", "Barcelona", "Valencia"]
    
    batch_size = 50
    data = []
    
    base_time = datetime.now()
    
    for i in range(batch_size):
        sensor_idx = i % len(sensors)
        timestamp = (base_time - timedelta(seconds=i*5)).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        
        row = {
            'sensor_id': sensors[sensor_idx],
            'temperature': round(random.uniform(15, 35), 2),
            'humidity': round(random.uniform(30, 80), 2),
            'pressure': round(random.uniform(1000, 1025), 2),
            'timestamp': timestamp,
            'pm25': round(random.uniform(5, 50), 2),
            'light': random.randint(100, 2000),
            'uv_level': random.randint(1, 10),
            'rain_raw': random.randint(0, 100),
            'wind_raw': random.randint(0, 50),
            'vibration': random.choice([True, False]),
            'location': locations[sensor_idx],
            'ingestion_timestamp': datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
            'batch_id': f"SIM-{datetime.now().strftime('%Y%m%d%H%M%S')}"
        }
        data.append(row)
    
    # Insertar datos
    columns = list(data[0].keys())
    values = [[row[col] for col in columns] for row in data]
    
    client.execute(
        f"INSERT INTO sensor_streaming ({', '.join(columns)}) VALUES",
        values
    )
    
    print(f"✅ {batch_size} registros insertados")
    
    # Verificar conteo
    count = client.execute("SELECT count() FROM sensor_streaming")[0][0]
    print(f"📈 Total registros en la tabla: {count}")
    
    # Mostrar últimos datos
    print("\n📋 Últimos 5 registros:")
    latest = client.execute("""
        SELECT sensor_id, round(temperature, 1), round(humidity, 1), round(pressure, 1), inserted_at
        FROM sensor_streaming 
        ORDER BY inserted_at DESC 
        LIMIT 5
    """)
    for row in latest:
        print(f"   {row[0]}: {row[1]}°C, {row[2]}%, {row[3]} hPa @ {row[4]}")
    
    print("\n✅ Datos listos para visualizar en Grafana")
    print("👉 http://localhost:3000/d/estacion-meteo/")


if __name__ == "__main__":
    main()
