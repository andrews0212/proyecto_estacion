#!/usr/bin/env python3
"""
Módulo File: Extrae datos de la capa Gold de MinIO y los guarda localmente
"""
import os
import sys
import pandas as pd
import json
from datetime import datetime

# Agregar el directorio del proyecto al path
project_root = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(project_root, 'etl'))

from config import Config
from utils.minio_client import MinIOClient

class FileModule:
    def __init__(self):
        self.minio_client = MinIOClient()
        self.output_dir = os.path.join(os.path.dirname(__file__), 'data')
        
        # Crear directorio de salida si no existe
        os.makedirs(self.output_dir, exist_ok=True)
    
    def clean_data_directory(self):
        """Limpiar todo el contenido del directorio data"""
        try:
            print("=== LIMPIANDO DIRECTORIO DATA ===") 
            
            if os.path.exists(self.output_dir):
                import shutil
                # Eliminar todo el contenido del directorio
                for filename in os.listdir(self.output_dir):
                    file_path = os.path.join(self.output_dir, filename)
                    if os.path.isfile(file_path):
                        os.remove(file_path)
                        print(f"Eliminado: {filename}")
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                        print(f"Directorio eliminado: {filename}")
                        
                print("✅ Directorio data limpiado completamente")
            
            # Recrear directorio vacío
            os.makedirs(self.output_dir, exist_ok=True)
            return True
            
        except Exception as e:
            print(f"❌ Error limpiando directorio: {e}")
            return False
    
    def export_gold_kpis(self) -> bool:
        """Exportar KPIs principales desde Gold a archivo local"""
        try:
            print("=== EXPORTANDO MÉTRICAS KPI GOLD ===")
            
            # Descargar KPIs principales
            kpis_path = f"{Config.GOLD_PATH}/kpis.parquet"
            kpis_df = self.minio_client.download_parquet_as_dataframe(kpis_path)
            
            # Exportar como CSV con nombre fijo (sobrescribir)
            csv_path = os.path.join(self.output_dir, 'metricas_kpi_gold.csv')
            kpis_df.to_csv(csv_path, index=False)
            print(f"✅ Métricas KPI exportadas: metricas_kpi_gold.csv")
            
            return True
            
        except Exception as e:
            print(f"❌ Error exportando KPIs: {e}")
            return False
    
    def export_sensor_statistics(self) -> bool:
        """Exportar datos limpios de Silver a archivo local"""
        try:
            print("=== EXPORTANDO DATOS LIMPIOS DE SILVER ===")
            
            # Descargar datos limpios de Silver
            silver_path = f"{Config.SILVER_PATH}/cleaned_data.parquet"
            silver_df = self.minio_client.download_parquet_as_dataframe(silver_path)
            
            # Exportar como CSV con nombre fijo (sobrescribir)
            csv_path = os.path.join(self.output_dir, 'datos_limpios_silver.csv')
            silver_df.to_csv(csv_path, index=False)
            print(f"✅ Datos limpios exportados: datos_limpios_silver.csv ({len(silver_df)} registros)")
            
            return True
            
        except Exception as e:
            print(f"❌ Error exportando datos limpios: {e}")
            return False
    
    def export_all_gold_data(self) -> bool:
        """Exportar datos a archivos locales"""
        try:
            print("=== EXPORTANDO DATOS A ARCHIVOS LOCALES ===")
            
            # Limpiar directorio data primero
            if not self.clean_data_directory():
                return False
            
            success_kpis = self.export_gold_kpis()
            success_silver = self.export_sensor_statistics()
            
            if success_kpis and success_silver:
                print(f"\n✅ EXPORTACIÓN COMPLETADA")
                print(f"📁 Directorio: {self.output_dir}")
                print(f"📄 Archivos disponibles:")
                
                # Listar archivos creados
                for file_name in sorted(os.listdir(self.output_dir)):
                    file_path = os.path.join(self.output_dir, file_name)
                    file_size = os.path.getsize(file_path)
                    print(f"   - {file_name} ({file_size} bytes)")
                
                return True
            else:
                print("❌ Error en la exportación")
                return False
                
        except Exception as e:
            print(f"❌ Error exportando datos: {e}")
            return False
    
    def list_local_files(self):
        """Listar archivos locales disponibles"""
        print("=== ARCHIVOS LOCALES DISPONIBLES ===")
        
        if not os.path.exists(self.output_dir):
            print("❌ Directorio de exportación no existe")
            return
        
        files = os.listdir(self.output_dir)
        if not files:
            print("📁 Directorio vacío")
            return
        
        print(f"📁 Directorio: {self.output_dir}")
        
        for file_name in sorted(files):
            file_path = os.path.join(self.output_dir, file_name)
            if os.path.isfile(file_path):
                file_size = os.path.getsize(file_path)
                mod_time = datetime.fromtimestamp(os.path.getmtime(file_path))
                print(f"   - {file_name}")
                print(f"     Tamaño: {file_size} bytes")
                print(f"     Modificado: {mod_time.strftime('%Y-%m-%d %H:%M:%S')}")
                print()

def main():
    """Función principal para ejecutar el módulo"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Módulo File - Exportar datos de Gold')
    parser.add_argument('--action', choices=['kpis', 'stats', 'log', 'all', 'list'], 
                       default='all', help='Acción a realizar')
    
    args = parser.parse_args()
    
    file_module = FileModule()
    
    if args.action == 'kpis':
        success = file_module.export_gold_kpis()
    elif args.action == 'stats':
        success = file_module.export_sensor_statistics()
    elif args.action == 'log':
        success = file_module.export_execution_log()
    elif args.action == 'all':
        success = file_module.export_all_gold_data()
    elif args.action == 'list':
        file_module.list_local_files()
        return
    
    if 'success' in locals():
        sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()