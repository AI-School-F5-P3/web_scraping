import redis
import json
import time
import uuid
import logging
from typing import Dict, List, Any, Optional
from redis.exceptions import ConnectionError, TimeoutError
from redis_config import *

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TaskManager:
    def __init__(self, worker_id=None):
        """Inicializa el administrador de tareas con conexión a Redis"""
        self.worker_id = worker_id
        try:
            # Configurar la conexión a Redis con parámetros de conexión y health check
            self.redis = redis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                password=REDIS_PASSWORD,
                username=REDIS_USERNAME,
                socket_timeout=5,
                socket_connect_timeout=5,
                health_check_interval=30,
                client_name="scraper_task_manager",
                decode_responses=True,
                max_connections=50  # Aumentamos el límite de conexiones por pool
            )
            
            # Intentar aumentar el límite máximo de clientes en el servidor Redis
            try:
                # Primero verificamos el límite actual
                current_maxclients = self.redis.config_get('maxclients').get('maxclients')
                logger.info(f"Límite actual de clientes Redis: {current_maxclients}")
                
                # Intentamos establecer un nuevo límite (mayor)
                # Esto solo funcionará si tienes permisos para cambiar la configuración
                new_maxclients = 10000  # Ajusta según tus necesidades
                self.redis.config_set('maxclients', new_maxclients)
                logger.info(f"Nuevo límite de clientes Redis establecido: {new_maxclients}")
            except Exception as config_error:
                logger.warning(f"No se pudo cambiar el límite de clientes: {str(config_error)}")
                logger.info("Continuando con el límite actual...")
            
            # Verificar la conexión
            self.redis.ping()
            logger.info("Conexión a Redis establecida correctamente")
        except (ConnectionError, TimeoutError) as e:
            logger.error(f"Error conectando a Redis: {str(e)}")
            self.redis = None
    def enqueue_tasks(self, companies: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Alias para add_tasks_batch para mantener compatibilidad con código existente
        
        Args:
            companies: Lista de diccionarios con datos de empresas
            
        Returns:
            Dict con estadísticas del proceso (total, añadidas, fallidas)
        """
        logger.info(f"Encolando {len(companies)} empresas usando enqueue_tasks (alias de add_tasks_batch)")
        return self.add_tasks_batch(companies)        
    
    def get_queue_stats(self) -> Dict[str, int]:
        """Obtiene estadísticas de las colas de trabajo"""
        try:
            if not self.redis:
                raise ConnectionError("No hay conexión a Redis")
                
            pending = int(self.redis.get(REDIS_COUNTER_PENDING) or 0)
            processing = int(self.redis.get(REDIS_COUNTER_PROCESSING) or 0)
            completed = int(self.redis.get(REDIS_COUNTER_COMPLETED) or 0)
            failed = int(self.redis.get(REDIS_COUNTER_FAILED) or 0)
            
            return {
                "pending": pending,
                "processing": processing,
                "completed": completed,
                "failed": failed,
                "total": pending + processing + completed + failed
            }
        except Exception as e:
            logger.error(f"ConnectionError in get_queue_stats, returning zeros: {str(e)}")
            return {
                "pending": 0,
                "processing": 0,
                "completed": 0,
                "failed": 0,
                "total": 0
            }
    
    def add_task(self, company_data: Dict[str, Any]) -> Dict[str, Any]:
        """Añade una tarea a la cola de pendientes"""
        try:
            if not self.redis:
                raise ConnectionError("No hay conexión a Redis")
                
            task_id = str(uuid.uuid4())
            
            task = {
                "task_id": task_id,
                "company_id": company_data.get("cod_infotel"),
                "company_name": company_data.get("razon_social"),
                "url": company_data.get("url"),
                "status": "pending",
                "created_at": time.time(),
                "company_data": company_data
            }
            
            # Añadir a la cola de pendientes
            self.redis.lpush(REDIS_QUEUE_PENDING, json.dumps(task))
            
            # Incrementar contador
            self.redis.incr(REDIS_COUNTER_PENDING)
            
            logger.info(f"Tarea {task_id} añadida a la cola para empresa {company_data.get('razon_social')}")
            
            return {"status": "success", "task_id": task_id}
            
        except Exception as e:
            logger.error(f"Error añadiendo tarea: {str(e)}")
            return {"status": "error", "message": str(e)}
    
    def add_tasks_batch(self, companies: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Añade un lote de empresas a la cola de tareas"""
        results = {
            "total": len(companies),
            "added": 0,
            "failed": 0,
            "tasks": []
        }
        
        for company in companies:
            result = self.add_task(company)
            
            if result.get("status") == "success":
                results["added"] += 1
                results["tasks"].append({
                    "company_id": company.get("cod_infotel"),
                    "task_id": result.get("task_id"),
                    "status": "added"
                })
            else:
                results["failed"] += 1
                results["tasks"].append({
                    "company_id": company.get("cod_infotel"),
                    "status": "failed",
                    "error": result.get("message")
                })
        
        return results
    
    def get_next_task(self) -> Optional[Dict[str, Any]]:
        """Obtiene la siguiente tarea pendiente y la mueve a procesando"""
        try:
            if not self.redis:
                raise ConnectionError("No hay conexión a Redis")
                
            # Usar BRPOPLPUSH con timeout para mover atómicamente
            # Nota: En Redis 6.2+ usar BLMOVE pero mantenemos compatibilidad con versiones anteriores
            task_json = self.redis.brpoplpush(
                REDIS_QUEUE_PENDING,
                REDIS_QUEUE_PROCESSING,
                timeout=1
            )
            
            if not task_json:
                return None
                
            task = json.loads(task_json)
            
            # Actualizar estado y timestamp
            task["status"] = "processing"
            task["started_at"] = time.time()
            
            # Actualizar task en la cola de procesamiento
            self.redis.lrem(REDIS_QUEUE_PROCESSING, 1, task_json)
            self.redis.lpush(REDIS_QUEUE_PROCESSING, json.dumps(task))
            
            # Actualizar contadores
            self.redis.decr(REDIS_COUNTER_PENDING)
            self.redis.incr(REDIS_COUNTER_PROCESSING)
            
            logger.info(f"Obtenida tarea {task['task_id']} para empresa {task.get('company_name')}")
            
            return task
            
        except Exception as e:
            logger.error(f"Error obteniendo tarea: {str(e)}")
            return None
    
    
    def complete_task(self, task_id: str, result: Dict[str, Any], error: str = None) -> Dict[str, Any]:
        """Marca una tarea como completada"""
        try:
            if not self.redis:
                raise ConnectionError("No hay conexión a Redis")
            
            # Si se proporciona un error, añadirlo al result
            if error is not None:
                result["error"] = error
                
            # Buscar la tarea en la cola de procesamiento
            processing_tasks = self.redis.lrange(REDIS_QUEUE_PROCESSING, 0, -1)
            task_found = False
            
            for task_json in processing_tasks:
                task = json.loads(task_json)
                
                if task.get("task_id") == task_id:
                    # Actualizar tarea
                    task["status"] = "completed"
                    task["completed_at"] = time.time()
                    task["result"] = result
                    task["duration"] = task["completed_at"] - task.get("started_at", task.get("created_at"))
                    
                    # Remover de procesamiento
                    self.redis.lrem(REDIS_QUEUE_PROCESSING, 1, task_json)
                    
                    # Añadir a completadas
                    self.redis.lpush(REDIS_QUEUE_COMPLETED, json.dumps(task))
                    
                    # Actualizar contadores
                    self.redis.decr(REDIS_COUNTER_PROCESSING)
                    self.redis.incr(REDIS_COUNTER_COMPLETED)
                    
                    task_found = True
                    logger.info(f"Tarea {task_id} marcada como completada")
                    break
            
            if not task_found:
                # Cambiar de warning a info
                logger.info(f"Info: Tarea {task_id} no encontrada en cola de procesamiento - posiblemente ya procesada")
                
                # Incrementar contador de completadas de todos modos
                # Esto asume que la tarea se completó correctamente aunque no se encuentre en la cola
                self.redis.incr(REDIS_COUNTER_COMPLETED)
                
                return {"status": "info", "message": "Tarea no encontrada en cola, posiblemente ya procesada"}
                    
            return {"status": "success"}
                
        except Exception as e:
            logger.error(f"Error completando tarea: {str(e)}")
            return {"status": "error", "message": str(e)}
    
    def fail_task(self, task_id: str, error: str) -> Dict[str, Any]:
        """Marca una tarea como fallida"""
        try:
            if not self.redis:
                raise ConnectionError("No hay conexión a Redis")
                
            # Buscar la tarea en la cola de procesamiento
            processing_tasks = self.redis.lrange(REDIS_QUEUE_PROCESSING, 0, -1)
            task_found = False
            
            for task_json in processing_tasks:
                task = json.loads(task_json)
                
                if task.get("task_id") == task_id:
                    # Actualizar tarea
                    task["status"] = "failed"
                    task["failed_at"] = time.time()
                    task["error"] = error
                    task["duration"] = task["failed_at"] - task.get("started_at", task.get("created_at"))
                    
                    # Remover de procesamiento
                    self.redis.lrem(REDIS_QUEUE_PROCESSING, 1, task_json)
                    
                    # Añadir a fallidas
                    self.redis.lpush(REDIS_QUEUE_FAILED, json.dumps(task))
                    
                    # Actualizar contadores
                    self.redis.decr(REDIS_COUNTER_PROCESSING)
                    self.redis.incr(REDIS_COUNTER_FAILED)
                    
                    task_found = True
                    logger.info(f"Tarea {task_id} marcada como fallida: {error}")
                    break
            
            if not task_found:
                logger.warning(f"Tarea {task_id} no encontrada en cola de procesamiento")
                return {"status": "error", "message": "Tarea no encontrada"}
                
            return {"status": "success"}
            
        except Exception as e:
            logger.error(f"Error marcando tarea como fallida: {str(e)}")
            return {"status": "error", "message": str(e)}
    
    def cleanup_stalled_tasks(self, max_age_seconds: int = 3600) -> Dict[str, Any]:
        """Limpia tareas estancadas en procesamiento"""
        try:
            if not self.redis:
                raise ConnectionError("No hay conexión a Redis")
                
            now = time.time()
            processing_tasks = self.redis.lrange(REDIS_QUEUE_PROCESSING, 0, -1)
            
            stalled_count = 0
            
            for task_json in processing_tasks:
                task = json.loads(task_json)
                started_at = task.get("started_at", task.get("created_at"))
                
                if started_at and (now - started_at) > max_age_seconds:
                    # Tarea estancada
                    stalled_count += 1
                    
                    # Actualizar tarea
                    task["status"] = "failed"
                    task["failed_at"] = now
                    task["error"] = "Tarea estancada (timeout)"
                    task["duration"] = task["failed_at"] - started_at
                    
                    # Remover de procesamiento
                    self.redis.lrem(REDIS_QUEUE_PROCESSING, 1, task_json)
                    
                    # Añadir a fallidas
                    self.redis.lpush(REDIS_QUEUE_FAILED, json.dumps(task))
                    
                    logger.info(f"Tarea estancada {task.get('task_id')} movida a fallidas")
            
            # Actualizar contadores si hay tareas estancadas
            if stalled_count > 0:
                # Ajustar contador de procesamiento
                current_processing = int(self.redis.get(REDIS_COUNTER_PROCESSING) or 0)
                new_processing = max(0, current_processing - stalled_count)
                self.redis.set(REDIS_COUNTER_PROCESSING, new_processing)
                
                # Ajustar contador de fallidas
                current_failed = int(self.redis.get(REDIS_COUNTER_FAILED) or 0)
                self.redis.set(REDIS_COUNTER_FAILED, current_failed + stalled_count)
                
                logger.info(f"Se limpiaron {stalled_count} tareas estancadas")
            
            return {
                "status": "success",
                "stalled_tasks_cleared": stalled_count
            }
            
        except Exception as e:
            logger.error(f"Error limpiando tareas estancadas: {str(e)}")
            return {"status": "error", "message": str(e)}
            
    def reset_counters(self) -> Dict[str, Any]:
        """Reinicia los contadores basándose en el contenido de las colas"""
        try:
            if not self.redis:
                raise ConnectionError("No hay conexión a Redis")
                
            # Contar tareas en cada cola
            pending_count = self.redis.llen(REDIS_QUEUE_PENDING)
            processing_count = self.redis.llen(REDIS_QUEUE_PROCESSING)
            completed_count = self.redis.llen(REDIS_QUEUE_COMPLETED)
            failed_count = self.redis.llen(REDIS_QUEUE_FAILED)
            
            # Actualizar contadores
            self.redis.set(REDIS_COUNTER_PENDING, pending_count)
            self.redis.set(REDIS_COUNTER_PROCESSING, processing_count)
            self.redis.set(REDIS_COUNTER_COMPLETED, completed_count)
            self.redis.set(REDIS_COUNTER_FAILED, failed_count)
            
            logger.info(f"Contadores reiniciados: pending={pending_count}, processing={processing_count}, completed={completed_count}, failed={failed_count}")
            
            return {
                "status": "success",
                "counters": {
                    "pending": pending_count,
                    "processing": processing_count,
                    "completed": completed_count,
                    "failed": failed_count,
                    "total": pending_count + processing_count + completed_count + failed_count
                }
            }
            
        except Exception as e:
            logger.error(f"Error reiniciando contadores: {str(e)}")
            return {"status": "error", "message": str(e)}
            
    def close(self):
        """Cierra la conexión a Redis"""
        if self.redis:
            self.redis.close()
            logger.info("Conexión a Redis cerrada")