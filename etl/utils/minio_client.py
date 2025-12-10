import io
import logging
from minio import Minio
from minio.error import S3Error
import pandas as pd
from config import Config

logger = logging.getLogger(__name__)

class MinIOClient:
    def __init__(self):
        self.client = Minio(
            Config.MINIO_ENDPOINT,
            access_key=Config.MINIO_ACCESS_KEY,
            secret_key=Config.MINIO_SECRET_KEY,
            secure=Config.MINIO_SECURE
        )
        self._ensure_bucket_exists()
    
    def _ensure_bucket_exists(self):
        """Crear bucket si no existe"""
        try:
            if not self.client.bucket_exists(Config.MINIO_BUCKET):
                self.client.make_bucket(Config.MINIO_BUCKET)
                logger.info(f"Bucket '{Config.MINIO_BUCKET}' creado exitosamente")
        except S3Error as e:
            logger.error(f"Error al crear bucket: {e}")
            raise
    
    def upload_dataframe_as_parquet(self, df: pd.DataFrame, path: str) -> bool:
        """Subir DataFrame como archivo Parquet a MinIO"""
        try:
            # Convertir DataFrame a Parquet en memoria
            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
            parquet_buffer.seek(0)
            
            # Subir a MinIO
            self.client.put_object(
                Config.MINIO_BUCKET,
                path,
                parquet_buffer,
                length=len(parquet_buffer.getvalue()),
                content_type='application/octet-stream'
            )
            
            logger.info(f"Archivo subido exitosamente: {path}")
            return True
            
        except Exception as e:
            logger.error(f"Error al subir archivo {path}: {e}")
            return False
    
    def download_parquet_as_dataframe(self, path: str) -> pd.DataFrame:
        """Descargar archivo Parquet de MinIO como DataFrame"""
        try:
            response = self.client.get_object(Config.MINIO_BUCKET, path)
            df = pd.read_parquet(io.BytesIO(response.read()))
            response.close()
            response.release_conn()
            
            logger.info(f"Archivo descargado exitosamente: {path}")
            return df
            
        except Exception as e:
            logger.error(f"Error al descargar archivo {path}: {e}")
            raise
    
    def list_objects(self, prefix: str = None):
        """Listar objetos en el bucket"""
        try:
            objects = self.client.list_objects(
                Config.MINIO_BUCKET, 
                prefix=prefix, 
                recursive=True
            )
            return [obj.object_name for obj in objects]
        except Exception as e:
            logger.error(f"Error al listar objetos: {e}")
            return []
    
    def delete_object(self, path: str) -> bool:
        """Eliminar objeto de MinIO"""
        try:
            self.client.remove_object(Config.MINIO_BUCKET, path)
            logger.info(f"Archivo eliminado exitosamente: {path}")
            return True
        except Exception as e:
            logger.error(f"Error al eliminar archivo {path}: {e}")
            return False