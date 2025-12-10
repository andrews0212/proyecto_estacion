#!/usr/bin/env python3
"""
Script independiente para ejecutar notebooks Gold
"""
import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime

# Agregar el directorio del proyecto al path
project_root = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(project_root, 'etl'))

from config import Config
from utils.minio_client import MinIOClient

def main():
    print("=== EJECUTANDO NOTEBOOK GOLD DE FORMA INDEPENDIENTE ===")
    
    minio_client = MinIOClient()
    
    try:
        # Obtener archivos Silver
        silver_files = minio_client.list_objects(Config.SILVER_PATH)
        silver_files = [f for f in silver_files if f.endswith('.parquet') and 'log' not in f]
        
        print(f"Archivos Silver encontrados: {len(silver_files)}")
        
        if not silver_files:
            print("No hay archivos Silver para procesar")
            return False
        
        # Cargar y consolidar datos Silver
        dfs = []
        for file_path in silver_files:
            try:
                df = minio_client.download_parquet_as_dataframe(file_path)
                dfs.append(df)
                print(f"Cargado: {file_path}")
            except Exception as e:
                print(f"Error cargando {file_path}: {e}")
        
        if not dfs:
            print("No se pudieron cargar archivos Silver")
            return False
        
        # Consolidar datos
        consolidated_df = pd.concat(dfs, ignore_index=True)
        print(f"Datos consolidados: {len(consolidated_df)} registros")
        
        # Renombrar columna 'ip' a 'sensor_id' si existe
        if 'ip' in consolidated_df.columns and 'sensor_id' not in consolidated_df.columns:
            consolidated_df['sensor_id'] = consolidated_df['ip']
        
        # Generar KPIs
        print("Generando KPIs...")
        
        kpis = {}
        
        # KPIs básicos
        kpis['total_readings'] = len(consolidated_df)
        kpis['unique_sensors'] = consolidated_df['sensor_id'].nunique() if 'sensor_id' in consolidated_df.columns else 0
        
        # KPIs de temperatura
        if 'temperature' in consolidated_df.columns:
            temp_data = consolidated_df['temperature'].dropna()
            kpis['temperature_avg'] = float(temp_data.mean()) if len(temp_data) > 0 else 0.0
            kpis['temperature_max'] = float(temp_data.max()) if len(temp_data) > 0 else 0.0
            kpis['temperature_min'] = float(temp_data.min()) if len(temp_data) > 0 else 0.0
            kpis['temperature_std'] = float(temp_data.std()) if len(temp_data) > 0 else 0.0
        
        # KPIs de humedad
        if 'humidity' in consolidated_df.columns:
            humidity_data = consolidated_df['humidity'].dropna()
            kpis['humidity_avg'] = float(humidity_data.mean()) if len(humidity_data) > 0 else 0.0
            kpis['humidity_max'] = float(humidity_data.max()) if len(humidity_data) > 0 else 0.0
            kpis['humidity_min'] = float(humidity_data.min()) if len(humidity_data) > 0 else 0.0
        
        # KPIs de calidad del aire
        if 'pm25' in consolidated_df.columns:
            pm25_data = consolidated_df['pm25'].dropna()
            kpis['pm25_avg'] = float(pm25_data.mean()) if len(pm25_data) > 0 else 0.0
            kpis['pm25_max'] = float(pm25_data.max()) if len(pm25_data) > 0 else 0.0
            
            # Clasificar calidad del aire
            if kpis['pm25_avg'] <= 12:
                air_quality = "Buena"
            elif kpis['pm25_avg'] <= 35.4:
                air_quality = "Moderada"
            elif kpis['pm25_avg'] <= 55.4:
                air_quality = "Insalubre para grupos sensibles"
            else:
                air_quality = "Insalubre"
            
            kpis['air_quality_category'] = air_quality
        
        # KPIs de presión
        if 'pressure' in consolidated_df.columns:
            pressure_data = consolidated_df['pressure'].dropna()
            kpis['pressure_avg'] = float(pressure_data.mean()) if len(pressure_data) > 0 else 0.0
            kpis['pressure_max'] = float(pressure_data.max()) if len(pressure_data) > 0 else 0.0
            kpis['pressure_min'] = float(pressure_data.min()) if len(pressure_data) > 0 else 0.0
        
        # KPIs de alertas
        alerts = {}
        if 'temperature' in consolidated_df.columns:
            temp_high = (consolidated_df['temperature'] > 30).sum()
            temp_low = (consolidated_df['temperature'] < 10).sum()
            alerts['high_temperature'] = int(temp_high)
            alerts['low_temperature'] = int(temp_low)
        
        if 'humidity' in consolidated_df.columns:
            humidity_high = (consolidated_df['humidity'] > 80).sum()
            humidity_low = (consolidated_df['humidity'] < 20).sum()
            alerts['high_humidity'] = int(humidity_high)
            alerts['low_humidity'] = int(humidity_low)
        
        kpis['alerts'] = alerts
        
        # Estadísticas por sensor
        sensor_stats = []
        if 'sensor_id' in consolidated_df.columns:
            for sensor_id in consolidated_df['sensor_id'].unique():
                sensor_data = consolidated_df[consolidated_df['sensor_id'] == sensor_id]
                
                stats = {
                    'sensor_id': sensor_id,
                    'readings_count': len(sensor_data),
                }
                
                if 'temperature' in sensor_data.columns:
                    temp_data = sensor_data['temperature'].dropna()
                    if len(temp_data) > 0:
                        stats['temp_avg'] = float(temp_data.mean())
                        stats['temp_max'] = float(temp_data.max())
                        stats['temp_min'] = float(temp_data.min())
                
                if 'humidity' in sensor_data.columns:
                    humid_data = sensor_data['humidity'].dropna()
                    if len(humid_data) > 0:
                        stats['humidity_avg'] = float(humid_data.mean())
                
                sensor_stats.append(stats)
        
        print(f"KPIs generados:")
        for key, value in kpis.items():
            if key != 'alerts' and not isinstance(value, dict):
                print(f"   {key}: {value}")
        
        print(f"Alertas: {sum(alerts.values())} total")
        
        # Crear DataFrames para guardar (archivo único que se sobrescribe)
        timestamp = datetime.now()
        
        # KPIs principales
        kpis_data = {
            'timestamp': timestamp.isoformat(),
            'generation_time': timestamp,
            **kpis
        }
        
        # Convertir alertas dict a string para guardar
        if 'alerts' in kpis_data:
            kpis_data['alerts_json'] = str(kpis_data['alerts'])
            del kpis_data['alerts']
        
        kpis_df = pd.DataFrame([kpis_data])
        
        # Guardar KPIs principales en archivo único
        kpis_path = f"{Config.GOLD_PATH}/kpis.parquet"
        
        success = minio_client.upload_dataframe_as_parquet(kpis_df, kpis_path)
        
        if success:
            print(f"KPIs guardados: {kpis_path}")
            
            # Guardar estadísticas por sensor
            if sensor_stats:
                sensor_stats_df = pd.DataFrame(sensor_stats)
                sensor_stats_path = f"{Config.GOLD_PATH}/sensor_statistics.parquet"
                minio_client.upload_dataframe_as_parquet(sensor_stats_df, sensor_stats_path)
                print(f"Estadísticas por sensor guardadas: {sensor_stats_path}")
            
            # Log de ejecución (sobrescribir con información más reciente)
            log_data = {
                'script': 'gold_kpis_analysis_script',
                'execution_time': timestamp.isoformat(),
                'kpis_generated': len([k for k in kpis.keys() if not isinstance(kpis[k], dict)]),
                'sensors_analyzed': len(sensor_stats),
                'output_files': ['kpis.parquet', 'sensor_statistics.parquet'] if sensor_stats else ['kpis.parquet'],
                'temperature_avg': kpis.get('temperature_avg', 0),
                'air_quality': kpis.get('air_quality_category', 'N/A'),
                'total_sensors': kpis.get('unique_sensors', 0),
                'status': 'completed'
            }
            
            log_df = pd.DataFrame([log_data])
            log_path = f"{Config.GOLD_PATH}/execution_log.parquet"
            minio_client.upload_dataframe_as_parquet(log_df, log_path)
            print("Log de ejecución guardado")
            
            # Mostrar resumen final
            gold_files = minio_client.list_objects(Config.GOLD_PATH)
            print(f"\nProcesamiento completado!")
            print(f"Total archivos en Gold: {len(gold_files)}")
            print(f"Temperatura promedio: {kpis.get('temperature_avg', 0):.2f}°C")
            print(f"Calidad del aire: {kpis.get('air_quality_category', 'N/A')}")
            print(f"Sensores únicos: {kpis.get('unique_sensors', 0)}")
            
            return True
        else:
            print("Error al guardar KPIs")
            return False
            
    except Exception as e:
        print(f"Error durante el procesamiento: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)