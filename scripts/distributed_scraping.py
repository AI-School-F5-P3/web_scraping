import os
import time
import logging
import socket
import json
from typing import Dict, Any, List, Optional, Tuple
import traceback

# Importaciones del sistema original
from scraping_flow import WebScrapingService, RateLimiter
from task_manager import TaskManager
from database_supabase import SupabaseDatabaseManager
# En vez de usar DB_CONFIG de config.py, usaremos la configuración de Supabase
from supabase_config import SUPABASE_DB_CONFIG

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DistributedWebScrapingService:
    """
    Servicio de scraping distribuido usando Redis para coordinación
    y Supabase para almacenamiento centralizado
    """
    def __init__(self, worker_id=None):
        # Generar ID de worker automáticamente si no se proporciona
        if worker_id is None:
            self.worker_id = f"{socket.gethostname()}_{os.getpid()}"
        else:
            self.worker_id = worker_id
            
        # Inicializar gestor de tareas (Redis)
        
        self.task_manager = TaskManager(worker_id=self.worker_id)
        # Inicializar conexión a base de datos (Supabase)
        self.db = SupabaseDatabaseManager()
        
        # Inicializar servicio de scraping original
        # Usamos SUPABASE_DB_CONFIG en lugar de DB_CONFIG
        self.scraper = WebScrapingService(SUPABASE_DB_CONFIG)
        
        logger.info(f"DistributedWebScrapingService inicializado con worker ID: {self.worker_id}")

    def process_next_task(self) -> Dict[str, Any]:
        """
        Obtiene y procesa la siguiente tarea de la cola de Redis
        
        Returns:
            Dict: Información sobre el resultado del procesamiento
        """
        # Obtener tarea de Redis
        task = self.task_manager.get_next_task()
        
        if not task:
            logger.info("No hay tareas pendientes en la cola")
            return {"status": "no_tasks"}
        
        try:
            # Extraer los datos de la empresa
            company_data = task.get('company_data', {})
            company_id = company_data.get('cod_infotel')
            
            logger.info(f"Procesando tarea {task.get('task_id')} para empresa {company_id}")
            
            # Usar la lógica existente para procesar la empresa
            success, result = self.scraper.process_company(company_data)
            
            # Actualizar la base de datos con los resultados
            if success:
                # Agregar worker_id a los resultados
                result['worker_id'] = self.worker_id
                
                # Actualizar en Supabase
                update_result = self.db.update_scraping_results([result], worker_id=self.worker_id)
                
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
                    'url_status_mensaje': result.get('url_status_mensaje', 'URL no válida'),
                    'worker_id': self.worker_id,
                    'processed': True
                }
                
                # Actualizar en Supabase
                self.db.update_scraping_results([empty_data], worker_id=self.worker_id)
                
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
            traceback.print_exc()
            
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
        Ejecuta un worker que procesa tareas continuamente
        
        Args:
            max_tasks: Número máximo de tareas a procesar (None = sin límite)
            idle_timeout: Tiempo máximo de espera cuando no hay tareas (segundos)
        """
        logger.info(f"Iniciando worker {self.worker_id} con max_tasks={max_tasks}, idle_timeout={idle_timeout}")
        
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
            traceback.print_exc()
        
        finally:
            logger.info(f"Worker finalizado. Tareas procesadas: {tasks_processed}")
            return tasks_processed

def enqueue_companies(limit=100, reset_queues=False):
    """
    Obtiene empresas no procesadas de Supabase y las encola en Redis
    """
    # Inicializar servicios
    db = SupabaseDatabaseManager()
    task_manager = TaskManager()
    
    # Resetear colas si se solicita
    if reset_queues:
        logger.warning("Reseteando todas las colas")
        task_manager.reset_queues()
    
    # Obtener empresas no procesadas
    query = """
        SELECT cod_infotel, nif, razon_social, domicilio, 
            cod_postal, nom_poblacion, nom_provincia, url
        FROM sociedades 
        WHERE processed = FALSE OR processed IS NULL
        LIMIT %s
    """
    
    companies_df = db.execute_query(query, params=(limit,), return_df=True)
    
    if companies_df is None or companies_df.empty:
        logger.warning("No se encontraron empresas para procesar")
        return 0
    
    # Convertir a lista de diccionarios
    companies = companies_df.to_dict('records')
    
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
    worker_parser.add_argument(
        "--worker-id",
        type=str,
        default=None,
        help="ID del worker (por defecto: hostname_pid)"
    )
    
    args = parser.parse_args()
    
    if args.command == "enqueue":
        enqueue_companies(limit=args.limit, reset_queues=args.reset)
    elif args.command == "worker":
        service = DistributedWebScrapingService(worker_id=args.worker_id)
        service.run_worker(max_tasks=args.max_tasks, idle_timeout=args.idle_timeout)
    else:
        parser.print_help()