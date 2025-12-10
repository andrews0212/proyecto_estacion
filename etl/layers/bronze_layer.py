import logging
import pandas as pd
from datetime import datetime
from utils import DatabaseClient, MinIOClient
from config import Config

logger = logging.getLogger(__name__)

class BronzeLayer:
    def __init__(self):
        self.db_client = DatabaseClient()
        self.minio_client = MinIOClient()
        self.last_processed_id = 0
    
    def extract_and_store_batch(self) -> bool:
        """Extraer datos en lotes y almacenarlos en la capa Bronze"""
        try:
            # Obtener nuevos datos desde el último ID procesado
            df = self.db_client.get_new_sensor_data(self.last_processed_id)
            
            if df.empty:
                logger.info("No hay nuevos datos para procesar")
                return True
            
            # Agregar metadatos de ingesta
            df['ingestion_timestamp'] = datetime.now()
            df['batch_id'] = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            # Crear nombre de archivo con timestamp
            timestamp = datetime.now().strftime('%Y/%m/%d/%H%M%S')
            file_path = f"{Config.BRONZE_PATH}/{timestamp}.parquet"
            
            # Subir a MinIO
            success = self.minio_client.upload_dataframe_as_parquet(df, file_path)
            
            if success:
                # Actualizar el último ID procesado
                self.last_processed_id = df['id'].max()
                logger.info(f"Lote procesado exitosamente. Último ID: {self.last_processed_id}")
                logger.info(f"Registros procesados: {len(df)}")
                return True
            else:
                logger.error("Error al subir el lote a MinIO")
                return False
                
        except Exception as e:
            logger.error(f"Error en extracción de capa Bronze: {e}")
            return False
    
    def extract_all_data(self) -> bool:
        """Extraer todos los datos en lotes"""
        try:
            total_records = self.db_client.get_total_records()
            logger.info(f"Total de registros a procesar: {total_records}")
            
            offset = 0
            batch_count = 0
            
            while offset < total_records:
                # Obtener lote de datos
                df = self.db_client.get_sensor_data_batch(offset, Config.BATCH_SIZE)
                
                if df.empty:
                    break
                
                # Agregar metadatos
                df['ingestion_timestamp'] = datetime.now()
                df['batch_id'] = f"initial_batch_{batch_count:04d}"
                
                # Crear nombre de archivo
                timestamp = datetime.now().strftime('%Y/%m/%d')
                file_path = f"{Config.BRONZE_PATH}/initial_load/{timestamp}/batch_{batch_count:04d}.parquet"
                
                # Subir a MinIO
                success = self.minio_client.upload_dataframe_as_parquet(df, file_path)
                
                if not success:
                    logger.error(f"Error al subir lote {batch_count}")
                    return False
                
                offset += Config.BATCH_SIZE
                batch_count += 1
                logger.info(f"Lote {batch_count} procesado - Offset: {offset}")
            
            logger.info(f"Extracción inicial completada. {batch_count} lotes procesados")
            return True
            
        except Exception as e:
            logger.error(f"Error en extracción completa: {e}")
            return False
    
    def get_bronze_files(self) -> list:
        """Obtener lista de archivos en la capa Bronze"""
        return self.minio_client.list_objects(Config.BRONZE_PATH)
    
    def consolidate_bronze_files(self) -> pd.DataFrame:
        """Consolidar todos los archivos Bronze en un solo DataFrame"""
        try:
            bronze_files = self.get_bronze_files()
            
            if not bronze_files:
                logger.warning("No se encontraron archivos en la capa Bronze")
                return pd.DataFrame()
            
            dfs = []
            for file_path in bronze_files:
                try:
                    df = self.minio_client.download_parquet_as_dataframe(file_path)
                    dfs.append(df)
                except Exception as e:
                    logger.warning(f"Error al leer archivo {file_path}: {e}")
                    continue
            
            if dfs:
                consolidated_df = pd.concat(dfs, ignore_index=True)
                logger.info(f"Consolidados {len(dfs)} archivos Bronze con {len(consolidated_df)} registros")
                return consolidated_df
            else:
                logger.warning("No se pudo consolidar ningún archivo Bronze")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Error al consolidar archivos Bronze: {e}")
            return pd.DataFrame()