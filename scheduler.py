import schedule
import time
import logging
import sys
import os
from datetime import datetime

# Agregar el directorio ETL al path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'etl'))

from orchestrator import ETLOrchestrator
from utils import setup_logging
from config import Config

# Configurar logging
logger = setup_logging()

class ETLScheduler:
    def __init__(self):
        self.orchestrator = ETLOrchestrator()
        self.running = True
    
    def run_scheduled_etl(self):
        """Ejecutar ETL programado"""
        try:
            logger.info(f"=== Ejecutando ETL programado - {datetime.now()} ===")
            success = self.orchestrator.run_incremental_update()
            
            if success:
                logger.info("ETL programado completado exitosamente")
            else:
                logger.error("ETL programado falló")
                
        except Exception as e:
            logger.error(f"Error en ETL programado: {e}")
    
    def run_daily_full_pipeline(self):
        """Ejecutar pipeline completo diario"""
        try:
            logger.info(f"=== Ejecutando pipeline completo diario - {datetime.now()} ===")
            success = self.orchestrator.run_full_etl_pipeline()
            
            if success:
                logger.info("Pipeline diario completado exitosamente")
            else:
                logger.error("Pipeline diario falló")
                
        except Exception as e:
            logger.error(f"Error en pipeline diario: {e}")
    
    def run_weekly_cleanup(self):
        """Ejecutar limpieza semanal"""
        try:
            logger.info(f"=== Ejecutando limpieza semanal - {datetime.now()} ===")
            self.orchestrator.cleanup_old_data(days_to_keep=30)
            logger.info("Limpieza semanal completada")
            
        except Exception as e:
            logger.error(f"Error en limpieza semanal: {e}")
    
    def health_check(self):
        """Verificar el estado del sistema"""
        try:
            status = self.orchestrator.get_pipeline_status()
            logger.info(f"Health check - Estado del pipeline: {status}")
            
        except Exception as e:
            logger.error(f"Error en health check: {e}")
    
    def setup_schedules(self):
        """Configurar los horarios de ejecución"""
        # ETL incremental cada X minutos (configurado en .env)
        interval = Config.PROCESSING_INTERVAL_MINUTES
        schedule.every(interval).minutes.do(self.run_scheduled_etl)
        logger.info(f"ETL incremental programado cada {interval} minutos")
        
        # Pipeline completo diario a las 2:00 AM
        schedule.every().day.at("02:00").do(self.run_daily_full_pipeline)
        logger.info("Pipeline completo programado diariamente a las 02:00")
        
        # Limpieza semanal los domingos a las 3:00 AM
        schedule.every().sunday.at("03:00").do(self.run_weekly_cleanup)
        logger.info("Limpieza semanal programada los domingos a las 03:00")
        
        # Health check cada hora
        schedule.every().hour.do(self.health_check)
        logger.info("Health check programado cada hora")
    
    def start(self):
        """Iniciar el scheduler"""
        logger.info("=== Iniciando ETL Scheduler ===")
        
        # Configurar horarios
        self.setup_schedules()
        
        # Ejecutar verificación inicial
        self.health_check()
        
        # Verificar si necesitamos carga inicial
        try:
            status = self.orchestrator.get_pipeline_status()
            if status.get('layers', {}).get('bronze', {}).get('files', 0) == 0:
                logger.info("No se encontraron datos en Bronze, ejecutando carga inicial...")
                self.orchestrator.run_initial_data_load()
        except Exception as e:
            logger.warning(f"No se pudo verificar estado inicial: {e}")
        
        logger.info("Scheduler configurado y ejecutándose...")
        logger.info("Trabajos programados:")
        for job in schedule.jobs:
            logger.info(f"  - {job}")
        
        # Bucle principal
        try:
            while self.running:
                schedule.run_pending()
                time.sleep(30)  # Revisar cada 30 segundos
                
        except KeyboardInterrupt:
            logger.info("Recibida señal de interrupción, deteniendo scheduler...")
            self.stop()
        except Exception as e:
            logger.error(f"Error en scheduler: {e}")
            self.stop()
    
    def stop(self):
        """Detener el scheduler"""
        logger.info("Deteniendo ETL Scheduler...")
        self.running = False
        
        # Cerrar recursos si es necesario
        try:
            if hasattr(self.orchestrator.silver_layer, 'spark'):
                self.orchestrator.silver_layer.close_spark_session()
        except Exception as e:
            logger.warning(f"Error al cerrar recursos: {e}")
        
        logger.info("ETL Scheduler detenido")

def main():
    """Función principal"""
    try:
        scheduler = ETLScheduler()
        scheduler.start()
        
    except Exception as e:
        logger.error(f"Error al inicializar scheduler: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())