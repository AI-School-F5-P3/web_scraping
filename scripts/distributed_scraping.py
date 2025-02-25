import time
import logging
from typing import Dict, Any, List, Optional
from scraping_flow import WebScrapingService
from task_manager import TaskManager
from config import DB_CONFIG

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DistributedWebScrapingService(WebScrapingService):
    """
    Versión distribuida del WebScrapingService que integra con Redis a través de TaskManager
    """
    def __init__(self, db_params=None, worker_id=None):
        # Inicializar la clase base
        super().__init__(db_params or DB_CONFIG)
        
        # Inicializar TaskManager para Redis
        self.task_manager = TaskManager()
        self.worker_id = worker_id
        
        logger.info(f"DistributedWebScrapingService inicializado con worker_id: {worker_id}")
    
    def process_next_task(self) -> Dict[str, Any]:
        """
        Procesa la siguiente tarea de la cola de Redis
        
        Returns:
            Dict: Resultado del procesamiento
        """
        # Obtener tarea de Redis
        task = self.task_manager.get_next_task()
        
        if not task:
            logger.info("No hay tareas pendientes en la cola")
            return {"status": "no_tasks"}
        
        try:
            # Extraer datos de la empresa
            company_data = task.get('company_data', {})
            company_id = task.get('company_id')
            
            logger.info(f"Procesando tarea {task.get('task_id')} para empresa {company_id}")
            
            # Usar la lógica existente para procesar la empresa
            success, result = self.process_company(company_data)
            
            # Actualizar la base de datos
            if success:
                update_result = self.update_company_data(company_id, result)
                
                # Marcar tarea como completada en Redis
                self.task_manager.complete_task(
                    task, 
                    success=True, 
                    result=result
                )
                
                return {
                    "status": "success",
                    "task_id": task.get('task_id'),
                    "company_id": company_id,
                    "result": result
                }
            else:
                # Marcar como procesado pero sin éxito
                empty_data = {
                    'cod_infotel': company_id,
                    'url_exists': False,
                    'url_status': -1,
                    'url_status_mensaje': result.get('url_status_mensaje', 'URL no válida')
                }
                self.update_company_data(company_id, empty_data)
                
                # Marcar tarea como fallida en Redis
                self.task_manager.complete_task(
                    task, 
                    success=False, 
                    error=result.get('url_status_mensaje', 'URL no válida')
                )
                
                return {
                    "status": "failed",
                    "task_id": task.get('task_id'),
                    "company_id": company_id,
                    "error": result.get('url_status_mensaje', 'URL no válida')
                }
                
        except Exception as e:
            logger.error(f"Error procesando tarea {task.get('task_id')}: {str(e)}")
            
            # Marcar tarea como fallida en Redis
            self.task_manager.complete_task(
                task, 
                success=False, 
                error=str(e)
            )
            
            return {
                "status": "error",
                "task_id": task.get('task_id'),
                "error": str(e)
            }
    
    def run_worker(self, max_tasks=None, idle_timeout=60):
        """
        Ejecuta un worker para procesar tareas continuamente
        
        Args:
            max_tasks: Número máximo de tareas a procesar (None = sin límite)
            idle_timeout: Tiempo máximo de espera cuando no hay tareas (segundos)
        """
        logger.info(f"Iniciando worker con max_tasks={max_tasks}, idle_timeout={idle_timeout}")
        
        tasks_processed = 0
        idle_since = None
        
        try:
            while True:
                # Verificar si alcanzamos el límite de tareas
                if max_tasks and tasks_processed >= max_tasks:
                    logger.info(f"Se alcanzó el límite de tareas: {max_tasks}")
                    break
                
                # Procesar siguiente tarea
                result = self.process_next_task()
                
                if result["status"] == "no_tasks":
                    # No hay tareas, verificar timeout
                    if idle_since is None:
                        idle_since = time.time()
                        logger.info("No hay tareas disponibles, esperando...")
                    
                    # Salir si superamos el tiempo de espera
                    idle_time = time.time() - idle_since
                    if idle_time > idle_timeout:
                        logger.info(f"Tiempo de espera superado después de {idle_time:.1f} segundos")
                        break
                    
                    # Esperar un poco para no saturar Redis
                    time.sleep(5)
                    
                    # Mostrar estadísticas periódicamente
                    if int(idle_time) % 30 == 0:  # Cada 30 segundos
                        stats = self.task_manager.get_queue_stats()
                        logger.info(f"Estadísticas de cola: {stats}")
                else:
                    # Tarea procesada, reiniciar contador de tiempo inactivo
                    idle_since = None
                    tasks_processed += 1
                    
                    logger.info(f"Tarea procesada: {result['status']}. Total: {tasks_processed}")
        
        except KeyboardInterrupt:
            logger.info("Worker detenido por el usuario")
        
        except Exception as e:
            logger.error(f"Error en el worker: {str(e)}")
            import traceback
            traceback.print_exc()
        
        finally:
            logger.info(f"Worker finalizado. Tareas procesadas: {tasks_processed}")
            return tasks_processed

def enqueue_companies(limit=100, reset_queues=False):
    """
    Obtiene empresas no procesadas de la base de datos y las encola en Redis
    
    Args:
        limit: Número máximo de empresas a encolar
        reset_queues: Si es True, resetea todas las colas antes de encolar
    
    Returns:
        int: Número de empresas encoladas
    """
    # Inicializar servicios
    service = WebScrapingService(DB_CONFIG)
    task_manager = TaskManager()
    
    # Resetear colas si se solicita
    if reset_queues:
        logger.warning("Reseteando todas las colas")
        task_manager.reset_queues()
    
    # Obtener empresas no procesadas
    companies = service.get_companies_to_process(limit=limit)
    
    if not companies:
        logger.warning("No se encontraron empresas para procesar")
        return 0
    
    # Encolar empresas
    enqueued = task_manager.enqueue_tasks(companies)
    logger.info(f"Se encolaron {enqueued} empresas para procesamiento")
    
    return enqueued

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Herramientas de scraping distribuido")
    
    subparsers = parser.add_subparsers(dest="command", help="Comando a ejecutar")
    
    # Subcomando para encolar empresas
    enqueue_parser = subparsers.add_parser("enqueue", help="Encolar empresas para procesamiento")
    enqueue_parser.add_argument(
        "--limit", 
        type=int, 
        default=100, 
        help="Número máximo de empresas a encolar"
    )
    enqueue_parser.add_argument(
        "--reset", 
        action="store_true",
        help="Resetear todas las colas antes de encolar"
    )
    
    # Subcomando para ejecutar worker
    worker_parser = subparsers.add_parser("worker", help="Ejecutar worker para procesar tareas")
    worker_parser.add_argument(
        "--max-tasks", 
        type=int, 
        default=None, 
        help="Número máximo de tareas a procesar"
    )
    worker_parser.add_argument(
        "--idle-timeout", 
        type=int, 
        default=60, 
        help="Tiempo máximo de espera cuando no hay tareas (segundos)"
    )
    
    args = parser.parse_args()
    
    if args.command == "enqueue":
        enqueue_companies(limit=args.limit, reset_queues=args.reset)
    elif args.command == "worker":
        import socket
        import os
        
        worker_id = f"{socket.gethostname()}_{os.getpid()}"
        service = DistributedWebScrapingService(worker_id=worker_id)
        service.run_worker(max_tasks=args.max_tasks, idle_timeout=args.idle_timeout)
    else:
        parser.print_help()