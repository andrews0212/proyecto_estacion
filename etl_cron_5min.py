#!/usr/bin/env python3
"""
Script simple para ejecutar la tubería ETL cada 5 minutos
"""

import os
import time
from datetime import datetime
import subprocess

def ejecutar_ciclo():
    """Ejecutar un ciclo completo de la tubería"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"\n{'='*70}")
    print(f"🚀 CICLO ETL INICIADO - {timestamp}")
    print(f"{'='*70}\n")
    
    # Silver
    print(f"1️⃣  Procesando Silver...")
    subprocess.run(["python", "scripts/run_silver_notebook.py"], cwd=os.getcwd())
    
    # Gold
    print(f"\n2️⃣  Procesando Gold...")
    subprocess.run(["python", "scripts/run_gold_notebook.py"], cwd=os.getcwd())
    
    # Export
    print(f"\n3️⃣  Exportando datos...")
    subprocess.run(["python", "file/export_gold.py"], cwd=os.getcwd())
    
    print(f"\n✅ CICLO COMPLETADO A LAS {datetime.now().strftime('%H:%M:%S')}")
    print(f"{'='*70}\n")

def main():
    """Ejecutar tubería cada 5 minutos indefinidamente"""
    print(f"\n")
    print(f"╔{'='*68}╗")
    print(f"║ 🔄 TUBERÍA ETL - CADA 5 MINUTOS                              ║")
    print(f"║ Iniciado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}                               ║")
    print(f"║ Presiona Ctrl+C para detener                                ║")
    print(f"╚{'='*68}╝")
    
    try:
        ciclo = 0
        while True:
            ciclo += 1
            print(f"\n⏰ CICLO #{ciclo}")
            ejecutar_ciclo()
            
            # Esperar 5 minutos antes del siguiente ciclo
            print(f"⏳ Esperando 5 minutos hasta el próximo ciclo...")
            print(f"⏰ Próxima ejecución: {datetime.now().strftime('%H:%M:%S')}")
            
            for i in range(300, 0, -60):  # 300 segundos = 5 minutos
                print(f"   [{i}s] Esperando...", end='\r')
                time.sleep(min(60, i))
            
            print(f"\n")
    
    except KeyboardInterrupt:
        print(f"\n\n⛔ TUBERÍA DETENIDA")
        print(f"⏰ Hora de parada: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"📊 Total de ciclos ejecutados: {ciclo}")
        print(f"{'='*70}")

if __name__ == "__main__":
    main()
