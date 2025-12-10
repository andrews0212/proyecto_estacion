#!/usr/bin/env python3
"""
Simulador de datos en tiempo real para ClickHouse.
Inserta datos cada pocos segundos simulando sensores IoT.
"""

import random
import time
from datetime import datetime
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
    client.execute("SELECT 1")
    print("✅ Conexión exitosa")
    
    sensors = ["SENSOR-001", "SENSOR-002", "SENSOR-003", "SENSOR-004"]
    locations = ["Madrid", "Barcelona", "Valencia", "Sevilla"]
    
    # Valores base que irán variando ligeramente
    base_temp = random.uniform(18, 25)
    base_humidity = random.uniform(45, 65)
    base_pressure = random.uniform(1010, 1020)
    
    print("\n🚀 SIMULADOR DE DATOS EN TIEMPO REAL")
    print("=" * 50)
    print("Insertando datos cada 2 segundos...")
    print("Presiona Ctrl+C para detener\n")
    
    batch_count = 0
    total_inserted = 0
    
    try:
        while True:
            batch_count += 1
            
            # Variar ligeramente los valores base (simula cambio climático gradual)
            base_temp += random.uniform(-0.5, 0.5)
            base_temp = max(5, min(40, base_temp))  # Limitar entre 5 y 40
            
            base_humidity += random.uniform(-2, 2)
            base_humidity = max(20, min(95, base_humidity))
            
            base_pressure += random.uniform(-1, 1)
            base_pressure = max(990, min(1040, base_pressure))
            
            # Generar datos para cada sensor
            data = []
            now = datetime.now()
            
            for i, sensor in enumerate(sensors):
                # Cada sensor tiene pequeñas variaciones
                temp = base_temp + random.uniform(-3, 3)
                humidity = base_humidity + random.uniform(-5, 5)
                pressure = base_pressure + random.uniform(-2, 2)
                
                row = {
                    'sensor_id': sensor,
                    'temperature': round(temp, 2),
                    'humidity': round(humidity, 2),
                    'pressure': round(pressure, 2),
                    'timestamp': now.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                    'pm25': round(random.uniform(5, 80), 2),
                    'light': random.randint(100, 2500),
                    'uv_level': random.randint(1, 11),
                    'rain_raw': random.randint(0, 100),
                    'wind_raw': random.randint(0, 60),
                    'vibration': random.choice([True, False]),
                    'location': locations[i],
                    'ingestion_timestamp': now.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                    'batch_id': f"RT-{now.strftime('%Y%m%d%H%M%S')}"
                }
                data.append(row)
            
            # Insertar datos
            columns = list(data[0].keys())
            values = [[row[col] for col in columns] for row in data]
            
            client.execute(
                f"INSERT INTO sensor_streaming ({', '.join(columns)}) VALUES",
                values
            )
            
            total_inserted += len(data)
            
            # Mostrar progreso
            print(f"📊 Lote #{batch_count}: {len(data)} registros | Total: {total_inserted} | "
                  f"Temp: {base_temp:.1f}°C | Hum: {base_humidity:.1f}% | "
                  f"{now.strftime('%H:%M:%S')}")
            
            # Esperar antes del siguiente lote
            time.sleep(2)
            
    except KeyboardInterrupt:
        print(f"\n\n⛔ Simulación detenida")
        print(f"📈 Total registros insertados: {total_inserted}")
        print(f"📊 Lotes procesados: {batch_count}")
        
        # Mostrar conteo final
        count = client.execute("SELECT count() FROM sensor_streaming")[0][0]
        print(f"📋 Total en la tabla: {count}")
        print("\n👉 Visualiza en Grafana: http://localhost:3000/d/estacion-meteo/")


if __name__ == "__main__":
    main()
