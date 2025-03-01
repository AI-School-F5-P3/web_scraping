import pandas as pd
import numpy as np
import logging
import traceback
import json
import time
from datetime import datetime
import os
from dotenv import load_dotenv

# Importar componentes del proyecto
from scraping_flow import WebScrapingService
from task_manager import TaskManager
from database_supabase import SupabaseDatabaseManager
from redis_config import *

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Cargar variables de entorno
load_dotenv()

def diagnose_inconsistencies():
    """Diagnostica inconsistencias entre los contadores y la base de datos"""
    try:
        # Inicializar servicios
        db = SupabaseDatabaseManager()
        tm = TaskManager()
        
        # Inicializar WebScrapingService pasando directamente el objeto SupabaseDatabaseManager
        try:
            # Usar el objeto SupabaseDatabaseManager directamente
            scraper = WebScrapingService(db)
            logger.info("WebScrapingService inicializado correctamente con gestor de Supabase")
        except Exception as e:
            logger.error(f"No se pudo inicializar WebScrapingService: {str(e)}")
            # Continuar sin el scraper si no es crítico para el diagnóstico
        
        # Obtener estadísticas de la base de datos
        query_stats = """
        SELECT 
            COUNT(*) as total,
            COUNT(CASE WHEN processed = TRUE THEN 1 END) as processed,
            COUNT(CASE WHEN processed = FALSE OR processed IS NULL THEN 1 END) as unprocessed,
            COUNT(CASE WHEN url_exists = TRUE THEN 1 END) as with_url,
            COUNT(CASE WHEN url_exists = FALSE AND processed = TRUE THEN 1 END) as no_url_processed
        FROM sociedades
        """
        
        db_stats = db.execute_query(query_stats, return_df=True)
        
        if db_stats is not None and not db_stats.empty:
            # Verificar las columnas disponibles
            print("\n=== COLUMNAS DISPONIBLES EN EL RESULTADO ===")
            print(db_stats.columns.tolist())
            
            # Crear un diccionario para estadísticas
            stats_dict = {}
            
            # Extraer estadísticas de manera segura
            print("\n=== DIAGNÓSTICO DE INCONSISTENCIAS ===")
            
            # Si tenemos una fila de datos
            if len(db_stats) > 0:
                # Usar .get() para acceso seguro a las columnas
                for col in db_stats.columns:
                    stats_dict[col] = db_stats.iloc[0].get(col, "N/A")
                    print(f"{col}: {stats_dict[col]}")
            else:
                print("No hay datos en la respuesta.")
            
            # Obtener estadísticas de Redis
            try:
                redis_stats = tm.get_queue_stats()
                
                print("\n=== ESTADÍSTICAS DE REDIS ===")
                print(f"Pendientes: {redis_stats['pending']}")
                print(f"Procesando: {redis_stats['processing']}")
                print(f"Completadas: {redis_stats['completed']}")
                print(f"Fallidas: {redis_stats['failed']}")
                
                # Detectar posibles problemas (usando .get() para acceso seguro)
                processed_count = stats_dict.get('processed', 0)
                if processed_count and processed_count < redis_stats['completed']:
                    print("\n⚠️ ALERTA: Hay más tareas marcadas como completadas en Redis que registros procesados en la BD.")
                    print(f"   Diferencia: {redis_stats['completed'] - processed_count} registros")
                    print("   Esto puede indicar problemas en la actualización de la BD.")
                
                # Verificar si hay tareas en estado "procesando" durante demasiado tiempo
                processing_tasks_raw = tm.redis.lrange(REDIS_QUEUE_PROCESSING, 0, -1)
                if processing_tasks_raw:
                    print(f"\n=== TAREAS EN PROCESAMIENTO ({len(processing_tasks_raw)}) ===")
                    stalled_tasks = 0
                    for task_json in processing_tasks_raw[:10]:  # Mostrar solo las 10 primeras
                        try:
                            task_data = json.loads(task_json)
                            if 'started_at' in task_data:
                                started_time = datetime.fromtimestamp(task_data['started_at'])
                                elapsed = (datetime.now() - started_time).total_seconds() / 60  # minutos
                                if elapsed > 60:  # Más de 1 hora
                                    stalled_tasks += 1
                                    print(f"Tarea {task_data.get('task_id', 'desconocida')} para empresa {task_data.get('company_id', 'desconocida')} - Iniciada hace {elapsed:.1f} minutos")
                        except Exception as e:
                            print(f"Error parseando tarea: {str(e)}")
                    
                    if stalled_tasks > 0:
                        print(f"\n⚠️ ALERTA: Se encontraron {stalled_tasks} tareas posiblemente estancadas (>60 minutos)")
                
            except Exception as redis_error:
                print(f"\nNo se pudieron obtener estadísticas de Redis: {str(redis_error)}")
                traceback.print_exc()
                
            return stats_dict
        else:
            print("No se pudieron obtener estadísticas de la base de datos.")
            return {}
            
    except Exception as e:
        print(f"Error en diagnóstico: {str(e)}")
        traceback.print_exc()
        return {}

if __name__ == "__main__":
    print("Iniciando diagnóstico de inconsistencias en el sistema...")
    diagnose_inconsistencies()