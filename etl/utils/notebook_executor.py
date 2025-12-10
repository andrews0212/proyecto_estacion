import subprocess
import logging
import os
from datetime import datetime
from config import Config

logger = logging.getLogger(__name__)

class NotebookExecutor:
    def __init__(self):
        # Los notebooks están en la raíz del proyecto, no en etl/notebooks
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
        self.notebooks_path = os.path.join(project_root, 'notebooks')
    
    def execute_notebook(self, notebook_name: str, output_name: str = None) -> bool:
        """Ejecutar un notebook y guardar el resultado"""
        try:
            notebook_path = os.path.join(self.notebooks_path, notebook_name)
            
            if not os.path.exists(notebook_path):
                logger.error(f"Notebook no encontrado: {notebook_path}")
                return False
            
            # Generar nombre de salida con timestamp si no se proporciona
            if output_name is None:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                base_name = notebook_name.replace('.ipynb', '')
                output_name = f"{base_name}_executed_{timestamp}.ipynb"
            
            output_path = os.path.join(self.notebooks_path, 'executed', output_name)
            
            # Crear directorio de salida si no existe
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Ejecutar notebook usando nbconvert con python -m
            cmd = [
                'python', '-m', 'nbconvert',
                '--to', 'notebook',
                '--execute',
                '--output', output_path,
                notebook_path
            ]
            
            logger.info(f"Ejecutando notebook: {notebook_name}")
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                logger.info(f"Notebook ejecutado exitosamente: {output_name}")
                return True
            else:
                logger.error(f"Error al ejecutar notebook {notebook_name}: {result.stderr}")
                return False
                
        except Exception as e:
            logger.error(f"Error al ejecutar notebook {notebook_name}: {e}")
            return False
    
    def execute_silver_cleaning_notebook(self) -> bool:
        """Ejecutar notebook de limpieza Silver usando script independiente"""
        try:
            import subprocess
            
            # Ejecutar script independiente
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
            script_path = os.path.join(project_root, 'scripts', 'run_silver_notebook.py')
            
            result = subprocess.run([
                'python', script_path
            ], capture_output=True, text=True, cwd=project_root)
            
            if result.returncode == 0:
                logger.info("✅ Notebook Silver ejecutado exitosamente")
                if result.stdout:
                    logger.info(result.stdout.strip())
                return True
            else:
                logger.error(f"❌ Error en notebook Silver: {result.stderr}")
                return False
                
        except Exception as e:
            logger.error(f"Error ejecutando script Silver: {e}")
            return False
    
    def execute_gold_kpis_notebook(self) -> bool:
        """Ejecutar notebook de KPIs Gold usando script independiente"""
        try:
            import subprocess
            
            # Ejecutar script independiente
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
            script_path = os.path.join(project_root, 'scripts', 'run_gold_notebook.py')
            
            result = subprocess.run([
                'python', script_path
            ], capture_output=True, text=True, cwd=project_root)
            
            if result.returncode == 0:
                logger.info("✅ Notebook Gold ejecutado exitosamente")
                if result.stdout:
                    logger.info(result.stdout.strip())
                return True
            else:
                logger.error(f"❌ Error en notebook Gold: {result.stderr}")
                return False
                
        except Exception as e:
            logger.error(f"Error ejecutando script Gold: {e}")
            return False
    
    def execute_etl_notebooks_sequence(self) -> bool:
        """Ejecutar secuencia completa de notebooks ETL"""
        try:
            logger.info("=== Iniciando ejecución de notebooks ETL ===")
            
            # 1. Ejecutar notebook Silver (limpieza)
            logger.info("--- Ejecutando notebook Silver ---")
            silver_success = self.execute_silver_cleaning_notebook()
            
            if not silver_success:
                logger.error("Error en notebook Silver, abortando secuencia")
                return False
            
            # 2. Ejecutar notebook Gold (KPIs)
            logger.info("--- Ejecutando notebook Gold ---")
            gold_success = self.execute_gold_kpis_notebook()
            
            if not gold_success:
                logger.error("Error en notebook Gold")
                return False
            
            logger.info("=== Notebooks ETL ejecutados exitosamente ===")
            return True
            
        except Exception as e:
            logger.error(f"Error en secuencia de notebooks: {e}")
            return False