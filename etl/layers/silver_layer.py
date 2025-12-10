import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from utils import MinIOClient
from config import Config
import pandas as pd

logger = logging.getLogger(__name__)

class SilverLayer:
    def __init__(self):
        self.minio_client = MinIOClient()
        self.spark = self._create_spark_session()
    
    def _create_spark_session(self):
        """Crear sesión de Spark optimizada para el procesamiento"""
        try:
            spark = SparkSession.builder \
                .appName("SensorDataSilverLayer") \
                .config("spark.driver.memory", Config.SPARK_DRIVER_MEMORY) \
                .config("spark.executor.memory", Config.SPARK_EXECUTOR_MEMORY) \
                .config("spark.executor.cores", str(Config.SPARK_EXECUTOR_CORES)) \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .getOrCreate()
            
            spark.sparkContext.setLogLevel("WARN")
            logger.info("Sesión de Spark creada exitosamente")
            return spark
            
        except Exception as e:
            logger.error(f"Error al crear sesión de Spark: {e}")
            raise
    
    def process_bronze_to_silver(self) -> bool:
        """Procesar datos de Bronze a Silver con limpieza y transformaciones"""
        try:
            # Obtener archivos Bronze
            bronze_files = self.minio_client.list_objects(Config.BRONZE_PATH)
            
            if not bronze_files:
                logger.warning("No se encontraron archivos en la capa Bronze")
                return False
            
            logger.info(f"Procesando {len(bronze_files)} archivos Bronze")
            
            # Consolidar datos Bronze
            bronze_df_pandas = self._consolidate_bronze_data(bronze_files)
            
            if bronze_df_pandas.empty:
                logger.warning("No hay datos para procesar en Bronze")
                return False
            
            # Convertir a Spark DataFrame
            spark_df = self.spark.createDataFrame(bronze_df_pandas)
            
            # Aplicar transformaciones y limpieza
            cleaned_df = self._clean_and_transform_data(spark_df)
            
            # Convertir de vuelta a Pandas para guardar
            silver_df_pandas = cleaned_df.toPandas()
            
            # Guardar en capa Silver (archivo único que se sobrescribe)
            silver_path = f"{Config.SILVER_PATH}/cleaned_data.parquet"
            
            success = self.minio_client.upload_dataframe_as_parquet(
                silver_df_pandas, 
                silver_path
            )
            
            if success:
                logger.info(f"Datos Silver guardados exitosamente (sobrescrito): {silver_path}")
                logger.info(f"Registros procesados: {len(silver_df_pandas)}")
                return True
            else:
                logger.error("Error al guardar datos Silver")
                return False
                
        except Exception as e:
            logger.error(f"Error en procesamiento Bronze a Silver: {e}")
            return False
    
    def _consolidate_bronze_data(self, bronze_files: list) -> pd.DataFrame:
        """Consolidar archivos Bronze en un DataFrame"""
        dfs = []
        for file_path in bronze_files:
            try:
                df = self.minio_client.download_parquet_as_dataframe(file_path)
                dfs.append(df)
            except Exception as e:
                logger.warning(f"Error al leer archivo Bronze {file_path}: {e}")
                continue
        
        if dfs:
            return pd.concat(dfs, ignore_index=True)
        else:
            return pd.DataFrame()
    
    def _clean_and_transform_data(self, df):
        """Aplicar limpieza y transformaciones a los datos con PySpark"""
        try:
            logger.info("Iniciando limpieza y transformación de datos")
            
            # 1. Eliminar duplicados
            df = df.dropDuplicates(['sensor_id', 'timestamp'])
            
            # 2. Filtrar valores nulos críticos
            df = df.filter(
                col('sensor_id').isNotNull() & 
                col('timestamp').isNotNull()
            )
            
            # 3. Validar rangos de sensores (valores realistas)
            df = df.filter(
                (col('temperature').between(-50, 70)) &  # Temperatura en Celsius
                (col('humidity').between(0, 100)) &      # Humedad en %
                (col('pressure').between(800, 1200))     # Presión en hPa
            )
            
            # 4. Imputar valores nulos con la mediana por sensor
            from pyspark.sql.window import Window
            
            # Ventana por sensor para calcular estadísticas
            window_spec = Window.partitionBy('sensor_id')
            
            # Calcular medianas por sensor
            median_temp = df.select(
                percentile_approx('temperature', 0.5).alias('median_temp')
            ).collect()[0]['median_temp']
            
            median_humidity = df.select(
                percentile_approx('humidity', 0.5).alias('median_humidity')
            ).collect()[0]['median_humidity']
            
            median_pressure = df.select(
                percentile_approx('pressure', 0.5).alias('median_pressure')
            ).collect()[0]['median_pressure']
            
            # Imputar valores nulos
            df = df.fillna({
                'temperature': median_temp,
                'humidity': median_humidity, 
                'pressure': median_pressure,
                'location': 'Unknown'
            })
            
            # 5. Crear campos derivados
            df = df.withColumn(
                'temperature_fahrenheit', 
                col('temperature') * 1.8 + 32
            )
            
            df = df.withColumn(
                'comfort_index',
                when(
                    (col('temperature').between(20, 25)) & 
                    (col('humidity').between(40, 60)),
                    'Comfortable'
                ).when(
                    (col('temperature') < 18) | (col('temperature') > 28),
                    'Uncomfortable'
                ).when(
                    (col('humidity') < 30) | (col('humidity') > 70),
                    'Uncomfortable'
                ).otherwise('Moderate')
            )
            
            # 6. Estandarizar timestamps
            df = df.withColumn(
                'processing_timestamp',
                current_timestamp()
            )
            
            # 7. Agregar indicadores de calidad de datos
            df = df.withColumn(
                'data_quality_score',
                when(
                    col('temperature').isNotNull() &
                    col('humidity').isNotNull() &
                    col('pressure').isNotNull(),
                    100
                ).when(
                    col('temperature').isNotNull() &
                    col('humidity').isNotNull(),
                    75
                ).when(
                    col('temperature').isNotNull(),
                    50
                ).otherwise(25)
            )
            
            # 8. Crear categorías de sensores
            df = df.withColumn(
                'sensor_category',
                when(col('sensor_id').startswith('TEMP'), 'Temperature')
                .when(col('sensor_id').startswith('HUM'), 'Humidity')
                .when(col('sensor_id').startswith('PRES'), 'Pressure')
                .otherwise('Multi-sensor')
            )
            
            logger.info("Limpieza y transformación completada")
            return df
            
        except Exception as e:
            logger.error(f"Error en limpieza y transformación: {e}")
            raise
    
    def get_silver_data_summary(self) -> dict:
        """Obtener resumen de los datos en la capa Silver"""
        try:
            silver_files = self.minio_client.list_objects(Config.SILVER_PATH)
            
            if not silver_files:
                return {'message': 'No hay archivos en la capa Silver'}
            
            # Leer el archivo más reciente
            latest_file = max(silver_files)
            df_pandas = self.minio_client.download_parquet_as_dataframe(latest_file)
            
            summary = {
                'total_records': len(df_pandas),
                'unique_sensors': df_pandas['sensor_id'].nunique(),
                'date_range': {
                    'start': df_pandas['timestamp'].min().strftime('%Y-%m-%d %H:%M:%S'),
                    'end': df_pandas['timestamp'].max().strftime('%Y-%m-%d %H:%M:%S')
                },
                'data_quality': {
                    'avg_quality_score': df_pandas['data_quality_score'].mean(),
                    'complete_records': (df_pandas['data_quality_score'] == 100).sum()
                },
                'sensor_stats': {
                    'avg_temperature': df_pandas['temperature'].mean(),
                    'avg_humidity': df_pandas['humidity'].mean(),
                    'avg_pressure': df_pandas['pressure'].mean()
                }
            }
            
            return summary
            
        except Exception as e:
            logger.error(f"Error al obtener resumen Silver: {e}")
            return {'error': str(e)}
    
    def close_spark_session(self):
        """Cerrar la sesión de Spark"""
        if self.spark:
            self.spark.stop()
            logger.info("Sesión de Spark cerrada")