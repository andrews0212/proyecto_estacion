import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # Database Configuration
    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_PORT = int(os.getenv('DB_PORT', 5432))
    DB_NAME = os.getenv('DB_NAME', 'postgres')
    DB_USER = os.getenv('DB_USER', 'postgres')
    DB_PASSWORD = os.getenv('DB_PASSWORD', '1234')
    DB_TABLE = os.getenv('DB_TABLE', 'sensor_readings')
    
    # MinIO Configuration
    MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
    MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
    MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
    MINIO_BUCKET = os.getenv('MINIO_BUCKET', 'datalake')
    MINIO_SECURE = os.getenv('MINIO_SECURE', 'false').lower() == 'true'
    
    # ETL Paths
    BRONZE_PATH = os.getenv('BRONZE_PATH', 'bronze/sensor_data')
    SILVER_PATH = os.getenv('SILVER_PATH', 'silver/sensor_data')
    GOLD_PATH = os.getenv('GOLD_PATH', 'gold/sensor_kpis')
    
    # Processing Configuration
    BATCH_SIZE = int(os.getenv('BATCH_SIZE', 1000))
    PROCESSING_INTERVAL_MINUTES = int(os.getenv('PROCESSING_INTERVAL_MINUTES', 30))
    
    # Spark Configuration
    SPARK_DRIVER_MEMORY = os.getenv('SPARK_DRIVER_MEMORY', '2g')
    SPARK_EXECUTOR_MEMORY = os.getenv('SPARK_EXECUTOR_MEMORY', '2g')
    SPARK_EXECUTOR_CORES = int(os.getenv('SPARK_EXECUTOR_CORES', 2))
    
    # Logging
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    LOG_PATH = os.getenv('LOG_PATH', 'logs/etl.log')
    
    @classmethod
    def get_database_url(cls):
        return f"postgresql://{cls.DB_USER}:{cls.DB_PASSWORD}@{cls.DB_HOST}:{cls.DB_PORT}/{cls.DB_NAME}"