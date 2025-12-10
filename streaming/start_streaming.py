#!/usr/bin/env python3
"""
Script para elegir y ejecutar el sistema de streaming completo
Opción 1: Modo Simulado (para pruebas sin sensores)
Opción 2: Modo Real (para datos de sensores reales)
"""

import subprocess
import sys
import time
from datetime import datetime

def mostrar_menu():
    """Mostrar menú de opciones"""
    print("\n")
    print("╔" + "="*68 + "╗")
    print("║ 🔄 SISTEMA DE STREAMING EN TIEMPO REAL                       ║")
    print("║ PostgreSQL → Kafka → ClickHouse → Grafana                    ║")
    print("╚" + "="*68 + "╝\n")
    
    print("📋 Elige el modo de ejecución:\n")
    print("  1️⃣  SIMULADO   - Datos generados automáticamente (pruebas)")
    print("  2️⃣  REAL       - Datos desde PostgreSQL en vivo")
    print("  3️⃣  SOLO CONSUMER - Solo recibir datos de Kafka a ClickHouse")
    print("  4️⃣  SALIR\n")

def ejecutar_simulado():
    """Ejecutar sistema en modo simulado"""
    print("\n")
    print("╔" + "="*68 + "╗")
    print("║ 🎲 MODO SIMULADO - Dos terminales necesarias                ║")
    print("╚" + "="*68 + "╝\n")
    
    print("Se abrirán dos procesos:")
    print("  • Producer Simulado → Kafka")
    print("  • Consumer Kafka → ClickHouse\n")
    
    print("📌 Para usar:")
    print("  1. Abre DOS terminales en la carpeta del proyecto")
    print("  2. Terminal 1: python streaming/kafka_producer_simulated.py")
    print("  3. Terminal 2: python streaming/kafka_consumer_clickhouse.py")
    print("  4. Accede a Grafana: http://localhost:3000\n")
    
    input("Presiona Enter para continuar...")

def ejecutar_real():
    """Ejecutar sistema en modo real"""
    print("\n")
    print("╔" + "="*68 + "╗")
    print("║ 📊 MODO REAL - Datos de sensores en PostgreSQL              ║")
    print("╚" + "="*68 + "╝\n")
    
    print("Se abrirán dos procesos:")
    print("  • Producer Real (PostgreSQL) → Kafka")
    print("  • Consumer Kafka → ClickHouse\n")
    
    print("📌 Para usar:")
    print("  1. Asegúrate que PostgreSQL tiene datos de sensores")
    print("  2. Abre DOS terminales en la carpeta del proyecto")
    print("  3. Terminal 1: python streaming/kafka_producer_real.py")
    print("  4. Terminal 2: python streaming/kafka_consumer_clickhouse.py")
    print("  5. Accede a Grafana: http://localhost:3000\n")
    
    input("Presiona Enter para continuar...")

def ejecutar_consumer_solo():
    """Ejecutar solo el consumer"""
    print("\n")
    print("╔" + "="*68 + "╗")
    print("║ 🔄 SOLO CONSUMER - Kafka → ClickHouse                       ║")
    print("╚" + "="*68 + "╝\n")
    
    print("Iniciando Consumer de Kafka a ClickHouse...\n")
    
    try:
        subprocess.run(["python", "streaming/kafka_consumer_clickhouse.py"])
    except KeyboardInterrupt:
        print("\n⛔ Consumer detenido")
    except Exception as e:
        print(f"❌ Error: {e}")

def main():
    """Función principal"""
    while True:
        mostrar_menu()
        
        try:
            opcion = input("Selecciona una opción (1-4): ").strip()
            
            if opcion == "1":
                ejecutar_simulado()
            elif opcion == "2":
                ejecutar_real()
            elif opcion == "3":
                ejecutar_consumer_solo()
                break
            elif opcion == "4":
                print("\n👋 Saliendo...")
                sys.exit(0)
            else:
                print("❌ Opción inválida")
        
        except KeyboardInterrupt:
            print("\n\n👋 Saliendo...")
            sys.exit(0)
        except Exception as e:
            print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()
