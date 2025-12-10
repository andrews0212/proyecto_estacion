import logging
import pandas as pd
from datetime import datetime, timedelta
from utils import MinIOClient
from config import Config

logger = logging.getLogger(__name__)

class GoldLayer:
    def __init__(self):
        self.minio_client = MinIOClient()
    
    def _safe_float(self, value):
        """Convertir valor a float de forma segura, manejando numpy arrays"""
        import numpy as np
        if isinstance(value, (np.ndarray, np.generic)):
            return float(value.item() if hasattr(value, 'item') else value)
        elif pd.isna(value):
            return 0.0
        else:
            return float(value)
    
    def process_silver_to_gold(self) -> bool:
        """Procesar datos de Silver a Gold generando KPIs"""
        try:
            # Obtener archivos Silver
            silver_files = self.minio_client.list_objects(Config.SILVER_PATH)
            
            if not silver_files:
                logger.warning("No se encontraron archivos en la capa Silver")
                return False
            
            # Consolidar datos Silver
            silver_df = self._consolidate_silver_data(silver_files)
            
            if silver_df.empty:
                logger.warning("No hay datos para procesar en Silver")
                return False
            
            # Generar KPIs
            kpis = self._generate_kpis(silver_df)
            
            # Guardar KPIs en capa Gold (archivo único que se sobrescribe)
            gold_path = f"{Config.GOLD_PATH}/kpis.parquet"
            
            success = self.minio_client.upload_dataframe_as_parquet(
                kpis, 
                gold_path
            )
            
            if success:
                logger.info(f"KPIs guardados exitosamente en capa Gold (sobrescrito): {gold_path}")
                logger.info(f"KPIs generados: {len(kpis)} métricas")
                return True
            else:
                logger.error("Error al guardar KPIs en capa Gold")
                return False
                
        except Exception as e:
            logger.error(f"Error en procesamiento Silver a Gold: {e}")
            return False
    
    def _consolidate_silver_data(self, silver_files: list) -> pd.DataFrame:
        """Consolidar archivos Silver en un DataFrame"""
        dfs = []
        for file_path in silver_files:
            try:
                df = self.minio_client.download_parquet_as_dataframe(file_path)
                dfs.append(df)
            except Exception as e:
                logger.warning(f"Error al leer archivo Silver {file_path}: {e}")
                continue
        
        if dfs:
            consolidated_df = pd.concat(dfs, ignore_index=True)
            # Eliminar duplicados que puedan haber surgido de múltiples archivos
            consolidated_df = consolidated_df.drop_duplicates(
                subset=['sensor_id', 'timestamp'], 
                keep='last'
            )
            return consolidated_df
        else:
            return pd.DataFrame()
    
    def _generate_kpis(self, df: pd.DataFrame) -> pd.DataFrame:
        """Generar KPIs a partir de los datos limpios"""
        try:
            logger.info("Generando KPIs...")
            
            # Convertir timestamp si es necesario
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            kpis = []
            
            # 1. KPIs Generales
            general_kpis = self._generate_general_kpis(df)
            kpis.extend(general_kpis)
            
            # 2. KPIs por Sensor
            sensor_kpis = self._generate_sensor_kpis(df)
            kpis.extend(sensor_kpis)
            
            # 3. KPIs por Ubicación
            location_kpis = self._generate_location_kpis(df)
            kpis.extend(location_kpis)
            
            # 4. KPIs Temporales
            temporal_kpis = self._generate_temporal_kpis(df)
            kpis.extend(temporal_kpis)
            
            # 5. KPIs de Calidad de Datos
            quality_kpis = self._generate_quality_kpis(df)
            kpis.extend(quality_kpis)
            
            # 6. KPIs de Alertas
            alert_kpis = self._generate_alert_kpis(df)
            kpis.extend(alert_kpis)
            
            # Convertir a DataFrame
            kpis_df = pd.DataFrame(kpis)
            kpis_df['generated_at'] = datetime.now()
            
            logger.info(f"Generados {len(kpis_df)} KPIs")
            return kpis_df
            
        except Exception as e:
            logger.error(f"Error al generar KPIs: {e}")
            raise
    
    def _generate_general_kpis(self, df: pd.DataFrame) -> list:
        """KPIs generales del sistema"""
        kpis = []
        
        # Total de lecturas
        kpis.append({
            'kpi_name': 'total_readings',
            'kpi_category': 'general',
            'kpi_value': len(df),
            'kpi_unit': 'count',
            'description': 'Número total de lecturas de sensores'
        })
        
        # Número de sensores activos
        kpis.append({
            'kpi_name': 'active_sensors',
            'kpi_category': 'general',
            'kpi_value': int(df['sensor_id'].nunique()),
            'kpi_unit': 'count',
            'description': 'Número de sensores únicos activos'
        })
        
        # Promedios globales
        if 'temperature' in df.columns:
            kpis.append({
                'kpi_name': 'avg_temperature_global',
                'kpi_category': 'general',
                'kpi_value': float(df['temperature'].mean()),
                'kpi_unit': 'celsius',
                'description': 'Temperatura promedio global'
            })
        
        if 'humidity' in df.columns:
            kpis.append({
                'kpi_name': 'avg_humidity_global',
                'kpi_category': 'general',
                'kpi_value': float(df['humidity'].mean()),
                'kpi_unit': 'percent',
                'description': 'Humedad promedio global'
            })
        
        if 'pressure' in df.columns:
            kpis.append({
                'kpi_name': 'avg_pressure_global',
                'kpi_category': 'general',
                'kpi_value': float(df['pressure'].mean()),
                'kpi_unit': 'hpa',
                'description': 'Presión promedio global'
            })
        
        return kpis
    
    def _generate_sensor_kpis(self, df: pd.DataFrame) -> list:
        """KPIs por sensor individual"""
        kpis = []
        
        for sensor_id in df['sensor_id'].unique():
            sensor_data = df[df['sensor_id'] == sensor_id]
            
            # KPIs por sensor
            if 'temperature' in df.columns:
                kpis.append({
                    'kpi_name': f'avg_temperature_{sensor_id}',
                    'kpi_category': 'sensor',
                    'kpi_value': self._safe_float(sensor_data['temperature'].mean()),
                    'kpi_unit': 'celsius',
                    'description': f'Temperatura promedio del sensor {sensor_id}',
                    'sensor_id': sensor_id
                })
                
                kpis.append({
                    'kpi_name': f'max_temperature_{sensor_id}',
                    'kpi_category': 'sensor',
                    'kpi_value': self._safe_float(sensor_data['temperature'].max()),
                    'kpi_unit': 'celsius',
                    'description': f'Temperatura máxima del sensor {sensor_id}',
                    'sensor_id': sensor_id
                })
                
                kpis.append({
                    'kpi_name': f'min_temperature_{sensor_id}',
                    'kpi_category': 'sensor',
                    'kpi_value': self._safe_float(sensor_data['temperature'].min()),
                    'kpi_unit': 'celsius',
                    'description': f'Temperatura mínima del sensor {sensor_id}',
                    'sensor_id': sensor_id
                })
            
            # Lecturas por sensor
            kpis.append({
                'kpi_name': f'readings_count_{sensor_id}',
                'kpi_category': 'sensor',
                'kpi_value': len(sensor_data),
                'kpi_unit': 'count',
                'description': f'Número de lecturas del sensor {sensor_id}',
                'sensor_id': sensor_id
            })
        
        return kpis
    
    def _generate_location_kpis(self, df: pd.DataFrame) -> list:
        """KPIs por ubicación"""
        kpis = []
        
        if 'location' not in df.columns:
            return kpis
        
        for location in df['location'].unique():
            if pd.isna(location):
                continue
                
            location_data = df[df['location'] == location]
            
            kpis.append({
                'kpi_name': f'sensors_in_{location.lower().replace(" ", "_")}',
                'kpi_category': 'location',
                'kpi_value': int(location_data['sensor_id'].nunique()),
                'kpi_unit': 'count',
                'description': f'Número de sensores en {location}',
                'location': location
            })
            
            if 'temperature' in df.columns:
                kpis.append({
                    'kpi_name': f'avg_temperature_{location.lower().replace(" ", "_")}',
                    'kpi_category': 'location',
                    'kpi_value': self._safe_float(location_data['temperature'].mean()),
                    'kpi_unit': 'celsius',
                    'description': f'Temperatura promedio en {location}',
                    'location': location
                })
        
        return kpis
    
    def _generate_temporal_kpis(self, df: pd.DataFrame) -> list:
        """KPIs temporales (últimas 24 horas, última semana, etc.)"""
        kpis = []
        
        if 'timestamp' not in df.columns:
            return kpis
        
        now = datetime.now()
        last_24h = now - timedelta(hours=24)
        last_week = now - timedelta(days=7)
        
        # Datos de las últimas 24 horas
        recent_data = df[df['timestamp'] >= last_24h]
        if not recent_data.empty:
            kpis.append({
                'kpi_name': 'readings_last_24h',
                'kpi_category': 'temporal',
                'kpi_value': len(recent_data),
                'kpi_unit': 'count',
                'description': 'Lecturas en las últimas 24 horas'
            })
        
        # Datos de la última semana
        week_data = df[df['timestamp'] >= last_week]
        if not week_data.empty:
            kpis.append({
                'kpi_name': 'readings_last_week',
                'kpi_category': 'temporal',
                'kpi_value': len(week_data),
                'kpi_unit': 'count',
                'description': 'Lecturas en la última semana'
            })
        
        return kpis
    
    def _generate_quality_kpis(self, df: pd.DataFrame) -> list:
        """KPIs de calidad de datos"""
        kpis = []
        
        if 'data_quality_score' in df.columns:
            kpis.append({
                'kpi_name': 'avg_data_quality',
                'kpi_category': 'quality',
                'kpi_value': self._safe_float(df['data_quality_score'].mean()),
                'kpi_unit': 'score',
                'description': 'Puntuación promedio de calidad de datos'
            })
            
            kpis.append({
                'kpi_name': 'high_quality_readings',
                'kpi_category': 'quality',
                'kpi_value': int((df['data_quality_score'] >= 90).sum()),
                'kpi_unit': 'count',
                'description': 'Lecturas con alta calidad (>=90)'
            })
        
        # Porcentaje de completitud
        total_records = len(df)
        if total_records > 0:
            completeness = {
                'temperature': self._safe_float((df['temperature'].notna().sum() / total_records) * 100),
                'humidity': self._safe_float((df['humidity'].notna().sum() / total_records) * 100),
                'pressure': self._safe_float((df['pressure'].notna().sum() / total_records) * 100)
            }
            
            for field, percentage in completeness.items():
                kpis.append({
                    'kpi_name': f'completeness_{field}',
                    'kpi_category': 'quality',
                    'kpi_value': percentage,
                    'kpi_unit': 'percent',
                    'description': f'Porcentaje de completitud para {field}'
                })
        
        return kpis
    
    def _generate_alert_kpis(self, df: pd.DataFrame) -> list:
        """KPIs de alertas y condiciones críticas"""
        kpis = []
        
        # Alertas por temperatura extrema
        if 'temperature' in df.columns:
            high_temp_alerts = int((df['temperature'] > 30).sum())
            low_temp_alerts = int((df['temperature'] < 10).sum())
            
            kpis.extend([
                {
                    'kpi_name': 'high_temperature_alerts',
                    'kpi_category': 'alerts',
                    'kpi_value': high_temp_alerts,
                    'kpi_unit': 'count',
                    'description': 'Alertas de temperatura alta (>30°C)'
                },
                {
                    'kpi_name': 'low_temperature_alerts',
                    'kpi_category': 'alerts',
                    'kpi_value': low_temp_alerts,
                    'kpi_unit': 'count',
                    'description': 'Alertas de temperatura baja (<10°C)'
                }
            ])
        
        # Alertas por humedad extrema
        if 'humidity' in df.columns:
            high_humidity_alerts = int((df['humidity'] > 80).sum())
            low_humidity_alerts = int((df['humidity'] < 20).sum())
            
            kpis.extend([
                {
                    'kpi_name': 'high_humidity_alerts',
                    'kpi_category': 'alerts',
                    'kpi_value': high_humidity_alerts,
                    'kpi_unit': 'count',
                    'description': 'Alertas de humedad alta (>80%)'
                },
                {
                    'kpi_name': 'low_humidity_alerts',
                    'kpi_category': 'alerts',
                    'kpi_value': low_humidity_alerts,
                    'kpi_unit': 'count',
                    'description': 'Alertas de humedad baja (<20%)'
                }
            ])
        
        return kpis
    
    def get_latest_kpis(self) -> pd.DataFrame:
        """Obtener los KPIs más recientes"""
        try:
            gold_files = self.minio_client.list_objects(Config.GOLD_PATH)
            
            if not gold_files:
                logger.warning("No se encontraron archivos KPI en la capa Gold")
                return pd.DataFrame()
            
            # Obtener el archivo más reciente
            latest_file = max(gold_files)
            kpis_df = self.minio_client.download_parquet_as_dataframe(latest_file)
            
            logger.info(f"KPIs más recientes obtenidos de: {latest_file}")
            return kpis_df
            
        except Exception as e:
            logger.error(f"Error al obtener KPIs más recientes: {e}")
            return pd.DataFrame()
    
    def export_kpis_for_module(self, output_path: str = None) -> str:
        """Exportar KPIs para consumo por otros módulos"""
        try:
            kpis_df = self.get_latest_kpis()
            
            if kpis_df.empty:
                logger.warning("No hay KPIs para exportar")
                return None
            
            if output_path is None:
                output_path = f"gold_kpis_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
            
            # Guardar en la capa Gold para descarga
            export_path = f"{Config.GOLD_PATH}/exports/{output_path}"
            success = self.minio_client.upload_dataframe_as_parquet(kpis_df, export_path)
            
            if success:
                logger.info(f"KPIs exportados exitosamente: {export_path}")
                return export_path
            else:
                logger.error("Error al exportar KPIs")
                return None
                
        except Exception as e:
            logger.error(f"Error al exportar KPIs: {e}")
            return None