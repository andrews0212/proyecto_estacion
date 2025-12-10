import logging
import time
from datetime import datetime
from utils import setup_logging, DatabaseClient, MinIOClient, NotebookExecutor
from layers import BronzeLayer, SilverLayer, GoldLayer
from config import Config

logger = setup_logging()

class ETLOrchestrator:
    def __init__(self):
        self.bronze_layer = BronzeLayer()
        self.silver_layer = SilverLayer()
        self.gold_layer = GoldLayer()
        self.db_client = DatabaseClient()
        self.minio_client = MinIOClient()
        self.notebook_executor = NotebookExecutor()
    
    def run_full_etl_pipeline(self) -> bool:
        """Ejecutar el pipeline ETL completo: Bronze -> Silver -> Gold"""
        try:
            logger.info("=== Iniciando pipeline ETL completo ===")
            start_time = datetime.now()
            
            # 1. Verificar conexiones
            if not self._check_connections():
                logger.error("Error en las conexiones, abortando pipeline")
                return False
            
            # 2. Capa Bronze - Extracción
            logger.info("--- Iniciando capa Bronze ---")
            if not self.bronze_layer.extract_and_store_batch():
                logger.error("Error en capa Bronze")
                return False
            
            # 2. Ejecutar Notebooks ETL (Silver y Gold)
            logger.info("--- Ejecutando Notebooks ETL ---")
            if not self.notebook_executor.execute_etl_notebooks_sequence():
                logger.warning("Error en ejecución de notebooks, intentando procesamiento directo")
                
                # Fallback: ejecutar procesamiento directo si los notebooks fallan
                logger.info("--- Fallback: Procesamiento directo Silver ---")
                if not self.silver_layer.process_bronze_to_silver():
                    logger.error("Error en capa Silver")
                    return False
                
                logger.info("--- Fallback: Procesamiento directo Gold ---")
                if not self.gold_layer.process_silver_to_gold():
                    logger.error("Error en capa Gold")
                    return False
            
            # 5. Exportar KPIs para módulo
            logger.info("--- Exportando KPIs ---")
            export_path = self.gold_layer.export_kpis_for_module()
            if export_path:
                logger.info(f"KPIs exportados en: {export_path}")
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            logger.info(f"=== Pipeline ETL completado exitosamente en {duration:.2f} segundos ===")
            return True
            
        except Exception as e:
            logger.error(f"Error en pipeline ETL: {e}")
            return False
        finally:
            # Cerrar sesión de Spark
            self.silver_layer.close_spark_session()
    
    def run_initial_data_load(self) -> bool:
        """Cargar todos los datos iniciales (para la primera ejecución)"""
        try:
            logger.info("=== Iniciando carga inicial de datos ===")
            
            # Verificar conexiones
            if not self._check_connections():
                logger.error("Error en las conexiones, abortando carga inicial")
                return False
            
            # Extraer todos los datos a Bronze
            logger.info("Extrayendo todos los datos a capa Bronze...")
            if not self.bronze_layer.extract_all_data():
                logger.error("Error en carga inicial a Bronze")
                return False
            
            # Ejecutar notebooks ETL
            logger.info("Ejecutando notebooks para procesamiento Silver y Gold...")
            if not self.notebook_executor.execute_etl_notebooks_sequence():
                logger.warning("Error en notebooks, usando procesamiento directo")
                
                # Fallback al procesamiento directo
                if not self.silver_layer.process_bronze_to_silver():
                    logger.error("Error en procesamiento a Silver")
                    return False
                
                if not self.gold_layer.process_silver_to_gold():
                    logger.error("Error en procesamiento a Gold")
                    return False
            
            logger.info("=== Carga inicial completada exitosamente ===")
            return True
            
        except Exception as e:
            logger.error(f"Error en carga inicial: {e}")
            return False
        finally:
            self.silver_layer.close_spark_session()
    
    def run_incremental_update(self) -> bool:
        """Ejecutar actualización incremental (solo nuevos datos)"""
        try:
            logger.info("=== Iniciando actualización incremental ===")
            
            # Solo procesar nuevos datos
            if not self.bronze_layer.extract_and_store_batch():
                logger.info("No hay nuevos datos para procesar")
                return True
            
            # Ejecutar notebooks para procesar los nuevos datos
            logger.info("Ejecutando notebooks para actualización incremental...")
            if not self.notebook_executor.execute_etl_notebooks_sequence():
                logger.warning("Error en notebooks, usando procesamiento directo incremental")
                
                # Fallback al procesamiento directo
                if not self.silver_layer.process_bronze_to_silver():
                    logger.error("Error en procesamiento incremental Silver")
                    return False
                
                if not self.gold_layer.process_silver_to_gold():
                    logger.error("Error en procesamiento incremental Gold")
                    return False
            
            logger.info("=== Actualización incremental completada ===")
            return True
            
        except Exception as e:
            logger.error(f"Error en actualización incremental: {e}")
            return False
        finally:
            self.silver_layer.close_spark_session()
    
    def _check_connections(self) -> bool:
        """Verificar todas las conexiones necesarias"""
        logger.info("Verificando conexiones...")
        
        # Verificar base de datos
        if not self.db_client.test_connection():
            logger.error("Error de conexión a la base de datos")
            return False
        
        # Verificar MinIO
        try:
            self.minio_client.list_objects()
            logger.info("Conexión a MinIO exitosa")
        except Exception as e:
            logger.error(f"Error de conexión a MinIO: {e}")
            return False
        
        logger.info("Todas las conexiones verificadas exitosamente")
        return True
    
    def get_pipeline_status(self) -> dict:
        """Obtener el estado del pipeline"""
        try:
            status = {
                'timestamp': datetime.now().isoformat(),
                'database_connection': self.db_client.test_connection(),
                'layers': {
                    'bronze': {
                        'files': len(self.minio_client.list_objects(Config.BRONZE_PATH))
                    },
                    'silver': {
                        'files': len(self.minio_client.list_objects(Config.SILVER_PATH)),
                        'summary': self.silver_layer.get_silver_data_summary()
                    },
                    'gold': {
                        'files': len(self.minio_client.list_objects(Config.GOLD_PATH))
                    }
                }
            }
            
            return status
            
        except Exception as e:
            logger.error(f"Error al obtener estado del pipeline: {e}")
            return {'error': str(e)}
    
    def cleanup_old_data(self, days_to_keep: int = 30):
        """Limpiar datos antiguos (opcional)"""
        try:
            logger.info(f"Limpiando datos anteriores a {days_to_keep} días")
            
            # Implementar lógica de limpieza si es necesario
            # Por ahora, solo loggeamos la acción
            logger.info("Limpieza de datos completada")
            
        except Exception as e:
            logger.error(f"Error en limpieza de datos: {e}")

if __name__ == "__main__":
    orchestrator = ETLOrchestrator()
    
    # Verificar si es la primera ejecución
    try:
        bronze_files = orchestrator.minio_client.list_objects(Config.BRONZE_PATH)
        
        if not bronze_files:
            logger.info("Primera ejecución detectada, realizando carga inicial...")
            orchestrator.run_initial_data_load()
        else:
            logger.info("Ejecutando pipeline incremental...")
            orchestrator.run_full_etl_pipeline()
            
    except Exception as e:
        logger.error(f"Error en ejecución principal: {e}")