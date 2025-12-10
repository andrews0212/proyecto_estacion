#!/usr/bin/env python3
"""
Script para verificar la estructura de la tabla sensor_readings
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'etl'))

from utils import DatabaseClient
import pandas as pd

def check_table_structure():
    """Verificar la estructura de la tabla sensor_readings"""
    db_client = DatabaseClient()
    
    try:
        # Obtener información sobre las columnas de la tabla
        query = """
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_name = 'sensor_readings'
        ORDER BY ordinal_position;
        """
        
        df = pd.read_sql(query, db_client.engine)
        
        if df.empty:
            print("❌ La tabla 'sensor_readings' no existe")
            return False
        
        print("✅ Estructura de la tabla 'sensor_readings':")
        print("="*50)
        for _, row in df.iterrows():
            nullable = "NULL" if row['is_nullable'] == 'YES' else "NOT NULL"
            print(f"  {row['column_name']:<20} {row['data_type']:<20} {nullable}")
        
        # Obtener una muestra de datos
        try:
            sample_query = "SELECT * FROM sensor_readings LIMIT 3"
            sample_df = pd.read_sql(sample_query, db_client.engine)
            
            print(f"\n📊 Muestra de datos ({len(sample_df)} registros):")
            print("="*50)
            print(sample_df.to_string(index=False))
            
        except Exception as e:
            print(f"⚠️  No se pudieron obtener datos de muestra: {e}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error al verificar la tabla: {e}")
        return False

if __name__ == "__main__":
    check_table_structure()