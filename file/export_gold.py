#!/usr/bin/env python3
"""
Script para extraer datos de Gold desde MinIO a archivos locales
"""
from file_module import FileModule

def main():
    print("=== MÓDULO FILE - EXTRACTOR DE DATOS GOLD ===")
    
    file_module = FileModule()
    
    # Exportar todos los datos de Gold
    success = file_module.export_all_gold_data()
    
    if success:
        print("\n🎉 Todos los datos de Gold han sido exportados exitosamente!")
        
        # Mostrar archivos disponibles
        print()
        file_module.list_local_files()
    else:
        print("\n❌ Error durante la exportación")

if __name__ == "__main__":
    main()