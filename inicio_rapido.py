#!/usr/bin/env python3
"""
🚀 SCRIPT DE INICIO RÁPIDO
Ejecuta todo lo necesario para poner en marcha el sistema desde cero.
"""

import subprocess
import time
import sys
import os

def run_command(cmd, description, shell=True):
    """Ejecutar comando y mostrar resultado"""
    print(f"\n{'='*60}")
    print(f"📌 {description}")
    print(f"{'='*60}")
    print(f"$ {cmd}\n")
    
    result = subprocess.run(cmd, shell=shell, capture_output=False)
    return result.returncode == 0


def check_docker():
    """Verificar que Docker está corriendo"""
    print("\n🔍 Verificando Docker...")
    result = subprocess.run("docker info", shell=True, capture_output=True)
    if result.returncode != 0:
        print("❌ Docker no está corriendo. Por favor inicia Docker Desktop.")
        return False
    print("✅ Docker está corriendo")
    return True


def main():
    print("""
╔══════════════════════════════════════════════════════════════╗
║     🌤️  ESTACIÓN METEOROLÓGICA - INICIO RÁPIDO              ║
╚══════════════════════════════════════════════════════════════╝
    """)
    
    # Verificar Docker
    if not check_docker():
        return 1
    
    # Paso 1: Iniciar contenedores
    run_command(
        "docker-compose up -d",
        "Paso 1/4: Iniciando contenedores Docker..."
    )
    
    # Esperar a que los servicios arranquen
    print("\n⏳ Esperando 30 segundos a que los servicios arranquen...")
    for i in range(30, 0, -5):
        print(f"   {i} segundos restantes...")
        time.sleep(5)
    
    # Paso 2: Inicializar ClickHouse
    run_command(
        f"{sys.executable} scripts/init_clickhouse.py",
        "Paso 2/4: Inicializando ClickHouse..."
    )
    
    # Paso 3: Configurar Grafana
    run_command(
        f"{sys.executable} scripts/setup_grafana.py",
        "Paso 3/4: Configurando Grafana..."
    )
    
    # Paso 4: Insertar datos de prueba
    run_command(
        f"{sys.executable} scripts/insert_test_data.py",
        "Paso 4/4: Insertando datos de prueba..."
    )
    
    # Resumen final
    print("""
╔══════════════════════════════════════════════════════════════╗
║                    🎉 ¡INSTALACIÓN COMPLETA!                 ║
╚══════════════════════════════════════════════════════════════╝

📊 ACCESOS:
   • Grafana Dashboard: http://localhost:3000/d/estacion-meteo/
     Usuario: admin | Contraseña: admin
   
   • MinIO Console: http://localhost:9090
     Usuario: minioadmin | Contraseña: minioadmin

🚀 PRÓXIMOS PASOS:

   1. Ver datos en tiempo real (simulador):
      python scripts/simulate_realtime.py

   2. Usar Kafka para streaming:
      Terminal 1: python streaming/kafka_producer_simulated.py
      Terminal 2: python streaming/kafka_consumer_clickhouse.py

   3. ETL programado cada 5 minutos:
      python scheduler.py

📖 Para más información, lee: GUIA_INSTALACION.md

══════════════════════════════════════════════════════════════
    """)
    
    return 0


if __name__ == "__main__":
    exit(main())
