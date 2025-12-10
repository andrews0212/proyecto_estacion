#!/usr/bin/env python3
"""
Script de gestión para el sistema ETL de sensores
Permite ejecutar diferentes operaciones del pipeline
"""

import argparse
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils import setup_logging, DatabaseClient, MinIOClient
from layers import BronzeLayer, SilverLayer, GoldLayer
from orchestrator import ETLOrchestrator
from config import Config

logger = setup_logging()

def run_initial_load():
    """Ejecutar carga inicial de todos los datos"""
    logger.info("Iniciando carga inicial de datos")
    orchestrator = ETLOrchestrator()
    success = orchestrator.run_initial_data_load()
    
    if success:
        logger.info("✅ Carga inicial completada exitosamente")
        return 0
    else:
        logger.error("❌ Error en carga inicial")
        return 1

def run_incremental():
    """Ejecutar actualización incremental"""
    logger.info("Iniciando actualización incremental")
    orchestrator = ETLOrchestrator()
    success = orchestrator.run_incremental_update()
    
    if success:
        logger.info("✅ Actualización incremental completada")
        return 0
    else:
        logger.error("❌ Error en actualización incremental")
        return 1

def run_full_pipeline():
    """Ejecutar pipeline completo"""
    logger.info("Iniciando pipeline completo")
    orchestrator = ETLOrchestrator()
    success = orchestrator.run_full_etl_pipeline()
    
    if success:
        logger.info("✅ Pipeline completo exitoso")
        return 0
    else:
        logger.error("❌ Error en pipeline completo")
        return 1

def check_status():
    """Verificar estado del sistema"""
    logger.info("Verificando estado del sistema")
    orchestrator = ETLOrchestrator()
    status = orchestrator.get_pipeline_status()
    
    print("=== ESTADO DEL SISTEMA ===")
    print(f"Timestamp: {status.get('timestamp', 'N/A')}")
    print(f"Base de datos: {'✅ Conectada' if status.get('database_connection', False) else '❌ Sin conexión'}")
    
    layers = status.get('layers', {})
    print(f"\n📁 CAPAS DE DATOS:")
    print(f"  Bronze: {layers.get('bronze', {}).get('files', 0)} archivos")
    print(f"  Silver: {layers.get('silver', {}).get('files', 0)} archivos")
    print(f"  Gold: {layers.get('gold', {}).get('files', 0)} archivos")
    
    silver_summary = layers.get('silver', {}).get('summary', {})
    if silver_summary and 'error' not in silver_summary:
        print(f"\n📊 RESUMEN SILVER:")
        print(f"  Registros totales: {silver_summary.get('total_records', 'N/A')}")
        print(f"  Sensores únicos: {silver_summary.get('unique_sensors', 'N/A')}")
        print(f"  Calidad promedio: {silver_summary.get('data_quality', {}).get('avg_quality_score', 'N/A')}")
    
    return 0

def test_connections():
    """Probar todas las conexiones"""
    logger.info("Probando conexiones del sistema")
    
    # Probar base de datos
    db_client = DatabaseClient()
    db_ok = db_client.test_connection()
    print(f"Base de datos: {'✅ OK' if db_ok else '❌ Error'}")
    
    # Probar MinIO
    try:
        minio_client = MinIOClient()
        minio_client.list_objects()
        print(f"MinIO: ✅ OK")
        minio_ok = True
    except Exception as e:
        print(f"MinIO: ❌ Error - {e}")
        minio_ok = False
    
    if db_ok and minio_ok:
        print("🎉 Todas las conexiones funcionan correctamente")
        return 0
    else:
        print("⚠️  Hay problemas con las conexiones")
        return 1

def export_kpis():
    """Exportar KPIs más recientes"""
    logger.info("Exportando KPIs")
    gold_layer = GoldLayer()
    
    export_path = gold_layer.export_kpis_for_module()
    
    if export_path:
        print(f"✅ KPIs exportados exitosamente")
        print(f"📁 Ubicación: {export_path}")
        return 0
    else:
        print("❌ Error al exportar KPIs")
        return 1

def cleanup_data():
    """Limpiar datos antiguos"""
    logger.info("Limpiando datos antiguos")
    orchestrator = ETLOrchestrator()
    orchestrator.cleanup_old_data(days_to_keep=30)
    print("✅ Limpieza completada")
    return 0

def list_files():
    """Listar archivos en todas las capas"""
    minio_client = MinIOClient()
    
    print("=== ARCHIVOS EN DATALAKE ===")
    
    # Bronze
    bronze_files = minio_client.list_objects(Config.BRONZE_PATH)
    print(f"\n📦 BRONZE ({len(bronze_files)} archivos):")
    for f in bronze_files[-5:]:  # Últimos 5
        print(f"  • {f}")
    if len(bronze_files) > 5:
        print(f"  ... y {len(bronze_files) - 5} más")
    
    # Silver
    silver_files = minio_client.list_objects(Config.SILVER_PATH)
    print(f"\n🥈 SILVER ({len(silver_files)} archivos):")
    for f in silver_files[-5:]:  # Últimos 5
        print(f"  • {f}")
    if len(silver_files) > 5:
        print(f"  ... y {len(silver_files) - 5} más")
    
    # Gold
    gold_files = minio_client.list_objects(Config.GOLD_PATH)
    print(f"\n🥇 GOLD ({len(gold_files)} archivos):")
    for f in gold_files[-5:]:  # Últimos 5
        print(f"  • {f}")
    if len(gold_files) > 5:
        print(f"  ... y {len(gold_files) - 5} más")
    
    return 0

def main():
    parser = argparse.ArgumentParser(
        description="Sistema de gestión ETL para datos de sensores",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos de uso:
  python manage.py status              # Ver estado del sistema
  python manage.py test               # Probar conexiones
  python manage.py initial-load       # Carga inicial de datos
  python manage.py incremental        # Actualización incremental
  python manage.py full-pipeline      # Pipeline completo
  python manage.py export-kpis        # Exportar KPIs
  python manage.py list-files         # Listar archivos
  python manage.py cleanup            # Limpiar datos antiguos
        """
    )
    
    parser.add_argument(
        'command',
        choices=[
            'status', 'test', 'initial-load', 'incremental', 
            'full-pipeline', 'export-kpis', 'cleanup', 'list-files'
        ],
        help='Comando a ejecutar'
    )
    
    args = parser.parse_args()
    
    try:
        if args.command == 'status':
            return check_status()
        elif args.command == 'test':
            return test_connections()
        elif args.command == 'initial-load':
            return run_initial_load()
        elif args.command == 'incremental':
            return run_incremental()
        elif args.command == 'full-pipeline':
            return run_full_pipeline()
        elif args.command == 'export-kpis':
            return export_kpis()
        elif args.command == 'cleanup':
            return cleanup_data()
        elif args.command == 'list-files':
            return list_files()
        else:
            logger.error(f"Comando desconocido: {args.command}")
            return 1
            
    except KeyboardInterrupt:
        logger.info("Operación cancelada por el usuario")
        return 130
    except Exception as e:
        logger.error(f"Error inesperado: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())