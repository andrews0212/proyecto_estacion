import logging
import os
from config import Config

def setup_logging():
    """Configurar el sistema de logging"""
    # Crear directorio de logs si no existe
    log_dir = os.path.dirname(Config.LOG_PATH)
    os.makedirs(log_dir, exist_ok=True)
    
    # Configurar formato de logging
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'
    
    # Configurar logging con encoding UTF-8 para soportar emojis
    logging.basicConfig(
        level=getattr(logging, Config.LOG_LEVEL),
        format=log_format,
        datefmt=date_format,
        handlers=[
            logging.FileHandler(Config.LOG_PATH, encoding='utf-8'),
            logging.StreamHandler()  # También mostrar en consola
        ]
    )
    
    logger = logging.getLogger(__name__)
    logger.info("Sistema de logging configurado correctamente")
    return logger