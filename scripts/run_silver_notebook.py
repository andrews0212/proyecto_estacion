#!/usr/bin/env python3
"""
Script independiente para ejecutar notebooks Silver
"""
import os
import sys
import pandas as pd
from datetime import datetime

# Agregar el directorio del proyecto al path
project_root = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(project_root, 'etl'))

from config import Config
from utils.minio_client import MinIOClient

def main():
    print("=== EJECUTANDO NOTEBOOK SILVER DE FORMA INDEPENDIENTE ===")
    
    # Importar dependencias
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col, when, isnan, isnull, avg, stddev
    except ImportError as e:
        print(f"❌ Error importando PySpark: {e}")
        return False
    
    minio_client = MinIOClient()
    
    # Configurar Spark con configuración simplificada
    spark = SparkSession.builder \
        .appName("SilverDataCleaning") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .getOrCreate()
    
    try:
        print("Spark configurado exitosamente")
        
        # Obtener archivos Bronze
        bronze_files = minio_client.list_objects(Config.BRONZE_PATH)
        print(f"Archivos Bronze encontrados: {len(bronze_files)}")
        
        if not bronze_files:
            print("No hay archivos Bronze para procesar")
            return False
        
        # Cargar datos Bronze (más archivos para tener más variedad)
        dfs = []
        for file_path in bronze_files[-8:]:  # Últimos 8 archivos para más variedad
            try:
                df_pd = minio_client.download_parquet_as_dataframe(file_path)
                dfs.append(df_pd)
                print(f"Cargado: {file_path}")
            except Exception as e:
                print(f"Error cargando {file_path}: {e}")
        
        if not dfs:
            print("No se pudieron cargar archivos Bronze")
            return False
        
        # Consolidar datos
        consolidated_df = pd.concat(dfs, ignore_index=True)
        print(f"Datos consolidados: {len(consolidated_df)} registros")
        
        # Convertir a Spark DataFrame
        spark_df = spark.createDataFrame(consolidated_df)
        
        # Limpieza de datos usando PySpark
        print("Iniciando limpieza de datos...")
        
        # 1. Eliminar duplicados exactos (solo si todos los valores son idénticos)
        initial_count = spark_df.count()
        # Solo eliminar duplicados completos (todas las columnas iguales)
        spark_df = spark_df.dropDuplicates()
        final_count = spark_df.count()
        duplicates_removed = initial_count - final_count
        print(f"Duplicados exactos eliminados: {duplicates_removed}")
        print(f"Registros mantenidos: {final_count}")
        
        # 2. Filtrar outliers en temperatura
        outliers_removed = 0
        if 'temperature' in spark_df.columns:
            temp_stats = spark_df.select(avg('temperature').alias('mean'), stddev('temperature').alias('std')).collect()[0]
            temp_mean = temp_stats['mean']
            temp_std = temp_stats['std']
            
            if temp_mean is not None and temp_std is not None and temp_std > 0:
                # Hacer filtro menos estricto (5 desviaciones en lugar de 3)
                lower_bound = temp_mean - (5 * temp_std)
                upper_bound = temp_mean + (5 * temp_std)
                
                before_outliers = spark_df.count()
                spark_df = spark_df.filter(
                    (col('temperature') >= lower_bound) & 
                    (col('temperature') <= upper_bound)
                )
                after_outliers = spark_df.count()
                outliers_removed = before_outliers - after_outliers
                print(f"Outliers de temperatura eliminados: {outliers_removed}")
        
        # 3. Imputar valores nulos con la media (solo para columnas numéricas)
        numeric_cols = ['temperature', 'humidity', 'pressure', 'pm25', 'light']
        for col_name in numeric_cols:
            if col_name in spark_df.columns:
                mean_val = spark_df.select(avg(col_name)).collect()[0][0]
                if mean_val is not None:
                    spark_df = spark_df.fillna({col_name: mean_val})
                    print(f"Valores nulos imputados en {col_name} con media: {mean_val:.2f}")
        
        # 4. Enriquecimiento - skipear por ahora debido a problemas con formato timestamp
        print("Enriquecimiento temporal skipeado (timestamp tiene formato complejo)")
        
        # Convertir de vuelta a Pandas para guardar
        cleaned_df = spark_df.toPandas()
        print(f"Limpieza completada: {len(cleaned_df)} registros finales")
        
        # Guardar en capa Silver (archivo único que se sobrescribe)
        silver_path = f"{Config.SILVER_PATH}/cleaned_data.parquet"
        
        success = minio_client.upload_dataframe_as_parquet(cleaned_df, silver_path)
        
        if success:
            print(f"Datos guardados en Silver: {silver_path}")
            
            # Crear log de ejecución
            log_data = {
                'script': 'silver_data_cleaning_script',
                'execution_time': datetime.now().isoformat(),
                'records_processed': len(cleaned_df),
                'duplicates_removed': duplicates_removed,
                'outliers_removed': outliers_removed if 'outliers_removed' in locals() else 0,
                'output_file': 'cleaned_data.parquet',
                'status': 'completed'
            }
            
            # Sobrescribir log de ejecución con la información más reciente
            log_df = pd.DataFrame([log_data])
            log_path = f"{Config.SILVER_PATH}/execution_log.parquet"
            minio_client.upload_dataframe_as_parquet(log_df, log_path)
            print("Log de ejecución guardado")
            
            return True
        else:
            print("Error al guardar en Silver")
            return False
            
    except Exception as e:
        print(f"Error durante el procesamiento: {e}")
        return False
    finally:
        spark.stop()

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)