import time
import logging
import argparse
import socket
import traceback
import os
from typing import Dict, Any, List
from datetime import datetime

from scraping_flow import WebScrapingService
from task_manager import TaskManager
from database_supabase import SupabaseDatabaseManager
from supabase_config import SUPABASE_DB_CONFIG

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"worker_{socket.gethostname()}_{os.getpid()}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ScrapingWorker:
    def __init__(self):
        self.task_manager = TaskManager()
        self.db = SupabaseDatabaseManager()
        self.scraper = WebScrapingService(SUPABASE_DB_CONFIG)
        self.worker_id = self.task_manager.worker_id
        logger.info(f"Worker initialized with ID: {self.worker_id}")
        
    def run(self, max_tasks=None, idle_timeout=60):
        """
        Ejecuta el worker hasta que se agoten las tareas o se detenga manualmente
        
        Args:
            max_tasks: Número máximo de tareas a procesar (None = sin límite)
            idle_timeout: Tiempo máximo de espera cuando no hay tareas (segundos)
        """
        logger.info(f"Starting worker with max_tasks={max_tasks}, idle_timeout={idle_timeout}")
        
        tasks_processed = 0
        idle_since = None
        
        try:
            while True:
                # Verificar si alcanzamos el límite de tareas
                if max_tasks and tasks_processed >= max_tasks:
                    logger.info(f"Reached max tasks limit: {max_tasks}")
                    break
                
                # Obtener próxima tarea
                task = self.task_manager.get_next_task()
                
                if task:
                    # Reiniciar contador de tiempo inactivo
                    idle_since = None
                    
                    try:
                        logger.info(f"Processing task {task.task_id} for company {task.company_id}")
                        
                        # Enviar heartbeat cada 5 segundos durante el procesamiento
                        def heartbeat_callback():
                            self.task_manager.heartbeat(task)
                        
                        # Procesar empresa
                        company_data = task.company_data
                        success, data = self.scraper.process_company(company_data)
                        
                        if success:
                            # Actualizar en base de datos
                            update_result = self.db.update_scraping_results(
                                [data], 
                                worker_id=self.worker_id
                            )
                            
                            if update_result["status"] == "success":
                                # Marcar como completada
                                self.task_manager.complete_task(
                                    task, 
                                    success=True, 
                                    result=data
                                )
                                logger.info(f"Task {task.task_id} completed successfully")
                            else:
                                # Error en la BD
                                self.task_manager.complete_task(
                                    task, 
                                    success=False, 
                                    error=f"Database error: {update_result.get('message')}"
                                )
                                logger.error(f"Database error for task {task.task_id}: {update_result.get('message')}")
                        else:
                            # No se encontró URL válida
                            self.task_manager.complete_task(
                                task, 
                                success=False, 
                                error=data.get('url_status_mensaje', "No se encontró URL válida")
                            )
                            logger.warning(f"No valid URL found for task {task.task_id}")
                            
                            # Marcar como procesado en BD aunque sea fallido
                            empty_data = {
                                'cod_infotel': company_data['cod_infotel'],
                                'url_exists': False,
                                'url_status': -1,
                                'url_status_mensaje': data.get('url_status_mensaje', "No se encontró URL válida"),
                                'worker_id': self.worker_id
                            }
                            self.db.update_scraping_results([empty_data], worker_id=self.worker_id)
                        
                        tasks_processed += 1
                        
                    except Exception as e:
                        logger.error(f"Error processing task {task.task_id}: {str(e)}")
                        traceback.print_exc()
                        
                        # Marcar como fallida
                        self.task_manager.complete_task(
                            task, 
                            success=False, 
                            error=str(e)
                        )
                
                else:
                    # No hay tareas, verificar tiempo inactivo
                    if idle_since is None:
                        idle_since = time.time()
                        logger.info("No tasks available, waiting...")
                    
                    # Salir si superamos el tiempo de inactividad máximo
                    idle_time = time.time() - idle_since
                    if idle_time > idle_timeout:
                        logger.info(f"Idle timeout reached after {idle_time:.1f} seconds")
                        break
                    
                    # Esperar un poco para no saturar Redis
                    time.sleep(5)
                    
                    # Mostrar estadísticas periódicamente
                    if int(idle_time) % 30 == 0:  # Cada 30 segundos
                        stats = self.task_manager.get_queue_stats()
                        logger.info(f"Queue stats: {stats}")
        
        except KeyboardInterrupt:
            logger.info("Worker stopped by user")
        
        except Exception as e:
            logger.error(f"Worker error: {str(e)}")
            traceback.print_exc()
        
        finally:
            logger.info(f"Worker finished. Processed {tasks_processed} tasks")

def main():
    parser = argparse.ArgumentParser(description="Distributed Scraping Worker")
    parser.add_argument(
        "--max-tasks", 
        type=int, 
        default=None, 
        help="Maximum number of tasks to process (default: unlimited)"
    )
    parser.add_argument(
        "--idle-timeout", 
        type=int, 
        default=60, 
        help="Maximum idle time in seconds before exiting (default: 60)"
    )
    
    args = parser.parse_args()
    
    worker = ScrapingWorker()
    worker.run(max_tasks=args.max_tasks, idle_timeout=args.idle_timeout)

if __name__ == "__main__":
    main()