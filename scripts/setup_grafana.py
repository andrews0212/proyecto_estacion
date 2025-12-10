#!/usr/bin/env python3
"""
Script para configurar Grafana con ClickHouse y crear el dashboard profesional.
Ejecutar después de docker-compose up -d
"""

import requests
import time
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

GRAFANA_URL = "http://localhost:3000"
AUTH = ("admin", "admin")
HEADERS = {"Content-Type": "application/json"}


def wait_for_grafana():
    """Esperar a que Grafana esté listo"""
    logger.info("⏳ Esperando a que Grafana esté listo...")
    for i in range(30):
        try:
            resp = requests.get(f"{GRAFANA_URL}/api/health", timeout=5)
            if resp.status_code == 200:
                logger.info("✅ Grafana está listo")
                return True
        except:
            pass
        time.sleep(2)
    logger.error("❌ Grafana no respondió")
    return False


def delete_all_datasources():
    """Eliminar datasources existentes"""
    resp = requests.get(f"{GRAFANA_URL}/api/datasources", auth=AUTH)
    if resp.status_code == 200:
        for ds in resp.json():
            requests.delete(f"{GRAFANA_URL}/api/datasources/{ds['id']}", auth=AUTH)
            logger.info(f"   Datasource eliminado: {ds['name']}")


def delete_all_dashboards():
    """Eliminar dashboards existentes"""
    resp = requests.get(f"{GRAFANA_URL}/api/search", auth=AUTH)
    if resp.status_code == 200:
        for d in resp.json():
            if d['type'] == 'dash-db':
                requests.delete(f"{GRAFANA_URL}/api/dashboards/uid/{d['uid']}", auth=AUTH)
                logger.info(f"   Dashboard eliminado: {d['title']}")


def create_datasource():
    """Crear datasource ClickHouse con plugin oficial"""
    logger.info("🔧 Creando datasource ClickHouse...")
    
    config = {
        "name": "ClickHouse",
        "type": "grafana-clickhouse-datasource",
        "access": "proxy",
        "isDefault": True,
        "jsonData": {
            "host": "clickhouse",
            "port": 8123,
            "protocol": "http",
            "username": "default",
            "defaultDatabase": "meteo"
        },
        "secureJsonData": {
            "password": "clickhouse"
        }
    }
    
    resp = requests.post(f"{GRAFANA_URL}/api/datasources", auth=AUTH, json=config, headers=HEADERS)
    if resp.status_code == 200:
        uid = resp.json()['datasource']['uid']
        logger.info(f"✅ Datasource creado (UID: {uid})")
        return uid
    else:
        logger.error(f"❌ Error: {resp.text}")
        return None


def create_dashboard(ds_uid):
    """Crear dashboard profesional"""
    logger.info("📊 Creando dashboard...")
    
    dashboard = {
        "dashboard": {
            "title": "🌤️ Estación Meteorológica - Monitor en Tiempo Real",
            "uid": "estacion-meteo",
            "timezone": "browser",
            "refresh": "5s",
            "tags": ["meteorologia", "iot", "streaming"],
            "panels": [
                # === ROW 1: KPIs principales ===
                {
                    "id": 1,
                    "title": "📊 Total Registros",
                    "type": "stat",
                    "gridPos": {"x": 0, "y": 0, "w": 4, "h": 4},
                    "datasource": {"uid": ds_uid, "type": "grafana-clickhouse-datasource"},
                    "targets": [{"rawSql": "SELECT count() as total FROM meteo.sensor_streaming", "format": 1, "refId": "A", "queryType": "sql"}],
                    "options": {"colorMode": "background", "graphMode": "none", "textMode": "value"},
                    "fieldConfig": {"defaults": {"color": {"mode": "thresholds"}, "thresholds": {"steps": [{"color": "blue", "value": None}]}}}
                },
                {
                    "id": 2,
                    "title": "🌡️ Temperatura Actual",
                    "type": "stat",
                    "gridPos": {"x": 4, "y": 0, "w": 4, "h": 4},
                    "datasource": {"uid": ds_uid, "type": "grafana-clickhouse-datasource"},
                    "targets": [{"rawSql": "SELECT round(temperature, 1) as temp FROM meteo.sensor_streaming ORDER BY inserted_at DESC LIMIT 1", "format": 1, "refId": "A", "queryType": "sql"}],
                    "options": {"colorMode": "background", "graphMode": "none", "textMode": "value"},
                    "fieldConfig": {"defaults": {"unit": "celsius", "color": {"mode": "thresholds"}, "thresholds": {"steps": [{"color": "blue", "value": None}, {"color": "green", "value": 15}, {"color": "yellow", "value": 25}, {"color": "red", "value": 35}]}}}
                },
                {
                    "id": 3,
                    "title": "💧 Humedad Actual",
                    "type": "stat",
                    "gridPos": {"x": 8, "y": 0, "w": 4, "h": 4},
                    "datasource": {"uid": ds_uid, "type": "grafana-clickhouse-datasource"},
                    "targets": [{"rawSql": "SELECT round(humidity, 1) as hum FROM meteo.sensor_streaming ORDER BY inserted_at DESC LIMIT 1", "format": 1, "refId": "A", "queryType": "sql"}],
                    "options": {"colorMode": "background", "graphMode": "none", "textMode": "value"},
                    "fieldConfig": {"defaults": {"unit": "percent", "color": {"mode": "thresholds"}, "thresholds": {"steps": [{"color": "red", "value": None}, {"color": "yellow", "value": 30}, {"color": "green", "value": 50}, {"color": "blue", "value": 80}]}}}
                },
                {
                    "id": 4,
                    "title": "🔵 Presión Actual",
                    "type": "stat",
                    "gridPos": {"x": 12, "y": 0, "w": 4, "h": 4},
                    "datasource": {"uid": ds_uid, "type": "grafana-clickhouse-datasource"},
                    "targets": [{"rawSql": "SELECT round(pressure, 1) as pres FROM meteo.sensor_streaming ORDER BY inserted_at DESC LIMIT 1", "format": 1, "refId": "A", "queryType": "sql"}],
                    "options": {"colorMode": "background", "graphMode": "none", "textMode": "value"},
                    "fieldConfig": {"defaults": {"unit": "pressurehpa", "color": {"mode": "thresholds"}, "thresholds": {"steps": [{"color": "purple", "value": None}]}}}
                },
                {
                    "id": 5,
                    "title": "☀️ Luz",
                    "type": "stat",
                    "gridPos": {"x": 16, "y": 0, "w": 4, "h": 4},
                    "datasource": {"uid": ds_uid, "type": "grafana-clickhouse-datasource"},
                    "targets": [{"rawSql": "SELECT light FROM meteo.sensor_streaming ORDER BY inserted_at DESC LIMIT 1", "format": 1, "refId": "A", "queryType": "sql"}],
                    "options": {"colorMode": "background", "graphMode": "none", "textMode": "value"},
                    "fieldConfig": {"defaults": {"unit": "lux", "color": {"mode": "thresholds"}, "thresholds": {"steps": [{"color": "dark-blue", "value": None}, {"color": "yellow", "value": 500}, {"color": "orange", "value": 1000}]}}}
                },
                {
                    "id": 6,
                    "title": "🌬️ PM2.5",
                    "type": "stat",
                    "gridPos": {"x": 20, "y": 0, "w": 4, "h": 4},
                    "datasource": {"uid": ds_uid, "type": "grafana-clickhouse-datasource"},
                    "targets": [{"rawSql": "SELECT round(pm25, 1) FROM meteo.sensor_streaming ORDER BY inserted_at DESC LIMIT 1", "format": 1, "refId": "A", "queryType": "sql"}],
                    "options": {"colorMode": "background", "graphMode": "none", "textMode": "value"},
                    "fieldConfig": {"defaults": {"unit": "conμgm3", "color": {"mode": "thresholds"}, "thresholds": {"steps": [{"color": "green", "value": None}, {"color": "yellow", "value": 35}, {"color": "red", "value": 55}]}}}
                },
                
                # === ROW 2: Gráficos de series temporales ===
                {
                    "id": 10,
                    "title": "🌡️ Temperatura (últimos 100 registros)",
                    "type": "timeseries",
                    "gridPos": {"x": 0, "y": 4, "w": 12, "h": 8},
                    "datasource": {"uid": ds_uid, "type": "grafana-clickhouse-datasource"},
                    "targets": [{"rawSql": "SELECT inserted_at as time, temperature FROM meteo.sensor_streaming ORDER BY inserted_at DESC LIMIT 100", "format": 1, "refId": "A", "queryType": "sql"}],
                    "fieldConfig": {"defaults": {"unit": "celsius", "color": {"mode": "palette-classic"}, "custom": {"lineWidth": 2, "fillOpacity": 20, "showPoints": "never"}}}
                },
                {
                    "id": 11,
                    "title": "💧 Humedad (últimos 100 registros)",
                    "type": "timeseries",
                    "gridPos": {"x": 12, "y": 4, "w": 12, "h": 8},
                    "datasource": {"uid": ds_uid, "type": "grafana-clickhouse-datasource"},
                    "targets": [{"rawSql": "SELECT inserted_at as time, humidity FROM meteo.sensor_streaming ORDER BY inserted_at DESC LIMIT 100", "format": 1, "refId": "A", "queryType": "sql"}],
                    "fieldConfig": {"defaults": {"unit": "percent", "color": {"mode": "palette-classic"}, "custom": {"lineWidth": 2, "fillOpacity": 20, "showPoints": "never"}}}
                },
                
                # === ROW 3: Más gráficos ===
                {
                    "id": 12,
                    "title": "🔵 Presión Atmosférica",
                    "type": "timeseries",
                    "gridPos": {"x": 0, "y": 12, "w": 8, "h": 6},
                    "datasource": {"uid": ds_uid, "type": "grafana-clickhouse-datasource"},
                    "targets": [{"rawSql": "SELECT inserted_at as time, pressure FROM meteo.sensor_streaming ORDER BY inserted_at DESC LIMIT 100", "format": 1, "refId": "A", "queryType": "sql"}],
                    "fieldConfig": {"defaults": {"unit": "pressurehpa", "color": {"fixedColor": "purple", "mode": "fixed"}, "custom": {"lineWidth": 2, "fillOpacity": 10}}}
                },
                {
                    "id": 13,
                    "title": "☀️ Nivel de Luz",
                    "type": "timeseries",
                    "gridPos": {"x": 8, "y": 12, "w": 8, "h": 6},
                    "datasource": {"uid": ds_uid, "type": "grafana-clickhouse-datasource"},
                    "targets": [{"rawSql": "SELECT inserted_at as time, light FROM meteo.sensor_streaming ORDER BY inserted_at DESC LIMIT 100", "format": 1, "refId": "A", "queryType": "sql"}],
                    "fieldConfig": {"defaults": {"unit": "lux", "color": {"fixedColor": "yellow", "mode": "fixed"}, "custom": {"lineWidth": 2, "fillOpacity": 30}}}
                },
                {
                    "id": 14,
                    "title": "🌬️ Calidad del Aire (PM2.5)",
                    "type": "timeseries",
                    "gridPos": {"x": 16, "y": 12, "w": 8, "h": 6},
                    "datasource": {"uid": ds_uid, "type": "grafana-clickhouse-datasource"},
                    "targets": [{"rawSql": "SELECT inserted_at as time, pm25 FROM meteo.sensor_streaming ORDER BY inserted_at DESC LIMIT 100", "format": 1, "refId": "A", "queryType": "sql"}],
                    "fieldConfig": {"defaults": {"unit": "conμgm3", "color": {"fixedColor": "green", "mode": "fixed"}, "custom": {"lineWidth": 2, "fillOpacity": 15}}}
                },
                
                # === ROW 4: Estadísticas y tabla ===
                {
                    "id": 20,
                    "title": "📈 Estadísticas Generales",
                    "type": "table",
                    "gridPos": {"x": 0, "y": 18, "w": 8, "h": 6},
                    "datasource": {"uid": ds_uid, "type": "grafana-clickhouse-datasource"},
                    "targets": [{"rawSql": """
                        SELECT 
                            'Temperatura' as Metrica,
                            round(min(temperature), 1) as Minimo,
                            round(avg(temperature), 1) as Promedio,
                            round(max(temperature), 1) as Maximo
                        FROM meteo.sensor_streaming
                        UNION ALL
                        SELECT 
                            'Humedad',
                            round(min(humidity), 1),
                            round(avg(humidity), 1),
                            round(max(humidity), 1)
                        FROM meteo.sensor_streaming
                        UNION ALL
                        SELECT 
                            'Presion',
                            round(min(pressure), 1),
                            round(avg(pressure), 1),
                            round(max(pressure), 1)
                        FROM meteo.sensor_streaming
                    """, "format": 1, "refId": "A", "queryType": "sql"}]
                },
                {
                    "id": 21,
                    "title": "📋 Últimos 15 Registros",
                    "type": "table",
                    "gridPos": {"x": 8, "y": 18, "w": 16, "h": 6},
                    "datasource": {"uid": ds_uid, "type": "grafana-clickhouse-datasource"},
                    "targets": [{"rawSql": "SELECT inserted_at as Tiempo, sensor_id as Sensor, round(temperature,1) as Temp, round(humidity,1) as Hum, round(pressure,1) as Pres, light as Luz, round(pm25,1) as PM25 FROM meteo.sensor_streaming ORDER BY inserted_at DESC LIMIT 15", "format": 1, "refId": "A", "queryType": "sql"}]
                },
                
                # === ROW 5: Gauges ===
                {
                    "id": 30,
                    "title": "🌡️ Temperatura",
                    "type": "gauge",
                    "gridPos": {"x": 0, "y": 24, "w": 6, "h": 5},
                    "datasource": {"uid": ds_uid, "type": "grafana-clickhouse-datasource"},
                    "targets": [{"rawSql": "SELECT round(avg(temperature), 1) FROM meteo.sensor_streaming", "format": 1, "refId": "A", "queryType": "sql"}],
                    "fieldConfig": {"defaults": {"unit": "celsius", "min": -10, "max": 50, "thresholds": {"steps": [{"color": "blue", "value": None}, {"color": "green", "value": 15}, {"color": "yellow", "value": 25}, {"color": "red", "value": 35}]}}}
                },
                {
                    "id": 31,
                    "title": "💧 Humedad",
                    "type": "gauge",
                    "gridPos": {"x": 6, "y": 24, "w": 6, "h": 5},
                    "datasource": {"uid": ds_uid, "type": "grafana-clickhouse-datasource"},
                    "targets": [{"rawSql": "SELECT round(avg(humidity), 1) FROM meteo.sensor_streaming", "format": 1, "refId": "A", "queryType": "sql"}],
                    "fieldConfig": {"defaults": {"unit": "percent", "min": 0, "max": 100, "thresholds": {"steps": [{"color": "red", "value": None}, {"color": "yellow", "value": 30}, {"color": "green", "value": 50}, {"color": "blue", "value": 80}]}}}
                },
                {
                    "id": 32,
                    "title": "🔵 Presión",
                    "type": "gauge",
                    "gridPos": {"x": 12, "y": 24, "w": 6, "h": 5},
                    "datasource": {"uid": ds_uid, "type": "grafana-clickhouse-datasource"},
                    "targets": [{"rawSql": "SELECT round(avg(pressure), 1) FROM meteo.sensor_streaming", "format": 1, "refId": "A", "queryType": "sql"}],
                    "fieldConfig": {"defaults": {"unit": "pressurehpa", "min": 900, "max": 1100, "thresholds": {"steps": [{"color": "purple", "value": None}]}}}
                },
                {
                    "id": 33,
                    "title": "🌬️ PM2.5",
                    "type": "gauge",
                    "gridPos": {"x": 18, "y": 24, "w": 6, "h": 5},
                    "datasource": {"uid": ds_uid, "type": "grafana-clickhouse-datasource"},
                    "targets": [{"rawSql": "SELECT round(avg(pm25), 1) FROM meteo.sensor_streaming", "format": 1, "refId": "A", "queryType": "sql"}],
                    "fieldConfig": {"defaults": {"unit": "conμgm3", "min": 0, "max": 100, "thresholds": {"steps": [{"color": "green", "value": None}, {"color": "yellow", "value": 35}, {"color": "orange", "value": 55}, {"color": "red", "value": 75}]}}}
                }
            ]
        },
        "overwrite": True
    }
    
    resp = requests.post(f"{GRAFANA_URL}/api/dashboards/db", auth=AUTH, json=dashboard, headers=HEADERS)
    if resp.status_code == 200:
        logger.info("✅ Dashboard creado!")
        logger.info("👉 http://localhost:3000/d/estacion-meteo/")
    else:
        logger.error(f"❌ Error: {resp.text}")


def main():
    if not wait_for_grafana():
        return
    
    logger.info("🗑️ Limpiando configuración anterior...")
    delete_all_dashboards()
    delete_all_datasources()
    
    ds_uid = create_datasource()
    if ds_uid:
        create_dashboard(ds_uid)
        logger.info("\n✅ Grafana configurado correctamente!")
        logger.info("👉 Dashboard: http://localhost:3000/d/estacion-meteo/")


if __name__ == "__main__":
    main()
