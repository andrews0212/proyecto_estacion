#!/usr/bin/env python3
"""
Inicializa la base de datos y tabla en ClickHouse.
Ejecutar después de docker-compose up -d
"""

import time
from clickhouse_driver import Client

def wait_for_clickhouse():
    """Esperar a que ClickHouse esté listo"""
    print("⏳ Esperando a que ClickHouse esté listo...")
    for i in range(30):
        try:
            client = Client(
                host='localhost',
                port=9010,
                user='default',
                password='clickhouse'
            )
            client.execute("SELECT 1")
            print("✅ ClickHouse está listo")
            return client
        except Exception as e:
            time.sleep(2)
    
    print("❌ ClickHouse no respondió después de 60 segundos")
    return None


def init_database(client):
    """Crear base de datos y tabla"""
    print("\n🔧 Creando base de datos 'meteo'...")
    client.execute("CREATE DATABASE IF NOT EXISTS meteo")
    
    print("🔧 Creando tabla 'sensor_streaming'...")
    client.execute("""
        CREATE TABLE IF NOT EXISTS meteo.sensor_streaming (
            id UInt32 DEFAULT rowNumberInAllBlocks(),
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
            vibration Bool,
            location String,
            ingestion_timestamp String,
            batch_id String,
            inserted_at DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY (inserted_at, sensor_id)
        PARTITION BY toYYYYMM(inserted_at)
    """)
    
    print("✅ Base de datos y tabla creadas correctamente")


def verify_setup(client):
    """Verificar la configuración"""
    print("\n📋 Verificando configuración...")
    
    # Verificar base de datos
    dbs = client.execute("SHOW DATABASES")
    print(f"   Bases de datos: {[db[0] for db in dbs]}")
    
    # Verificar tabla
    tables = client.execute("SHOW TABLES FROM meteo")
    print(f"   Tablas en meteo: {[t[0] for t in tables]}")
    
    # Verificar estructura
    columns = client.execute("DESCRIBE meteo.sensor_streaming")
    print(f"   Columnas: {len(columns)}")
    
    # Contar registros
    count = client.execute("SELECT count() FROM meteo.sensor_streaming")[0][0]
    print(f"   Registros existentes: {count}")
    
    print("\n✅ ClickHouse configurado correctamente")


def main():
    print("=" * 50)
    print("🗄️  INICIALIZADOR DE CLICKHOUSE")
    print("=" * 50)
    
    client = wait_for_clickhouse()
    if not client:
        return 1
    
    try:
        init_database(client)
        verify_setup(client)
        
        print("\n" + "=" * 50)
        print("🎉 ClickHouse listo para recibir datos!")
        print("=" * 50)
        
        return 0
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1


if __name__ == "__main__":
    exit(main())
