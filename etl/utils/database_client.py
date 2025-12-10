import logging
import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
from config import Config

logger = logging.getLogger(__name__)

class DatabaseClient:
    def __init__(self):
        self.engine = create_engine(Config.get_database_url())
    
    def get_sensor_data_batch(self, offset: int = 0, limit: int = None) -> pd.DataFrame:
        """Obtener datos de sensores en lotes"""
        if limit is None:
            limit = Config.BATCH_SIZE
            
        query = f"""
        SELECT 
            id,
            ip as sensor_id,
            temperature,
            humidity,
            pressure,
            timestamp,
            pm25,
            light,
            uv_level,
            rain_raw,
            wind_raw,
            vibration,
            'Unknown' as location
        FROM {Config.DB_TABLE}
        ORDER BY timestamp DESC
        OFFSET {offset}
        LIMIT {limit}
        """
        
        try:
            df = pd.read_sql(query, self.engine)
            logger.info(f"Extraídos {len(df)} registros desde offset {offset}")
            return df
        except Exception as e:
            logger.error(f"Error al extraer datos: {e}")
            raise
    
    def get_new_sensor_data(self, last_processed_id: int = 0) -> pd.DataFrame:
        """Obtener nuevos datos de sensores desde el último ID procesado"""
        query = f"""
        SELECT 
            id,
            ip as sensor_id,
            temperature,
            humidity,
            pressure,
            timestamp,
            pm25,
            light,
            uv_level,
            rain_raw,
            wind_raw,
            vibration,
            'Unknown' as location
        FROM {Config.DB_TABLE}
        WHERE id > {last_processed_id}
        ORDER BY id ASC
        """
        
        try:
            df = pd.read_sql(query, self.engine)
            logger.info(f"Extraídos {len(df)} nuevos registros desde ID {last_processed_id}")
            return df
        except Exception as e:
            logger.error(f"Error al extraer nuevos datos: {e}")
            raise
    
    def get_total_records(self) -> int:
        """Obtener el número total de registros"""
        query = f"SELECT COUNT(*) as total FROM {Config.DB_TABLE}"
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query))
                total = result.fetchone()[0]
                logger.info(f"Total de registros activos: {total}")
                return total
        except Exception as e:
            logger.error(f"Error al obtener total de registros: {e}")
            raise
    
    def get_max_id(self) -> int:
        """Obtener el ID máximo de los registros"""
        query = f"SELECT COALESCE(MAX(id), 0) as max_id FROM {Config.DB_TABLE}"
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query))
                max_id = result.fetchone()[0]
                logger.info(f"ID máximo encontrado: {max_id}")
                return max_id
        except Exception as e:
            logger.error(f"Error al obtener ID máximo: {e}")
            raise
    
    def test_connection(self) -> bool:
        """Probar la conexión a la base de datos"""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                logger.info("Conexión a la base de datos exitosa")
                return True
        except Exception as e:
            logger.error(f"Error de conexión a la base de datos: {e}")
            return False