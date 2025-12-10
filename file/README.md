# Módulo File

Este módulo extrae datos de la capa Gold desde MinIO y los guarda como archivos locales en formatos CSV y JSON.

## Archivos

- `file_module.py`: Módulo principal con la clase FileModule
- `export_gold.py`: Script simple para exportar todos los datos
- `data/`: Directorio donde se guardan los archivos exportados

## Uso

### Exportar todos los datos de Gold:
```bash
python file/export_gold.py
```

### Usar el módulo con opciones específicas:
```bash
# Exportar solo KPIs
python file/file_module.py --action kpis

# Exportar solo estadísticas por sensor  
python file/file_module.py --action stats

# Exportar solo log de ejecución
python file/file_module.py --action log

# Exportar todo
python file/file_module.py --action all

# Listar archivos locales
python file/file_module.py --action list
```

## Archivos Generados

El módulo genera dos tipos de archivos para cada dataset:

1. **Con timestamp**: Para historial (ej: `kpis_20251210_173000.csv`)
2. **Latest**: Estado actual (ej: `latest_kpis.csv`)

### Tipos de datos exportados:

- **KPIs**: Métricas principales (temperatura, humedad, calidad del aire, etc.)
- **Estadísticas por sensor**: Datos detallados de cada sensor
- **Log de ejecución**: Información sobre la última ejecución

### Formatos disponibles:
- **CSV**: Para análisis en Excel/pandas
- **JSON**: Para integración con APIs/aplicaciones web

## Integración

Este módulo se puede integrar fácilmente en:
- Dashboards web
- Reportes automatizados  
- APIs REST
- Análisis con Excel/Python
- Sistemas de monitoreo