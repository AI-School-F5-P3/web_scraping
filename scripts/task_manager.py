import json
import time
import redis
import logging
import socket
import os
from typing import List, Dict, Any, Optional
from task import Task
from redis_config import *

logger = logging.getLogger(__name__)

class TaskManager:
    def __init__(self):
        self.redis = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            password=REDIS_PASSWORD,
            username=REDIS_USERNAME,
            decode_responses=True
        )
        self.hostname = socket.gethostname()
        self.worker_id = f"{self.hostname}_{os.getpid()}"
        logger.info(f"Initialized TaskManager with worker ID: {self.worker_id}")
    
    def enqueue_tasks(self, companies: List[Dict[str, Any]]) -> int:
        """Añade múltiples empresas a la cola de pendientes"""
        pipeline = self.redis.pipeline()
        
        count = 0
        for company in companies:
            task = Task(company_id=company["cod_infotel"], company_data=company)
            pipeline.lpush(REDIS_QUEUE_PENDING, task.to_json())
            pipeline.incr(REDIS_COUNTER_PENDING)
            count += 1
        
        pipeline.execute()
        logger.info(f"Enqueued {count} tasks")
        return count
    
    def get_next_task(self) -> Optional[Task]:
        """Obtiene la siguiente tarea pendiente y la marca como en procesamiento"""
        # Usar RPOPLPUSH para mover atómicamente de pending a processing
        task_json = self.redis.rpoplpush(REDIS_QUEUE_PENDING, REDIS_QUEUE_PROCESSING)
        
        if not task_json:
            return None
            
        task = Task.from_json(task_json)
        
        # Actualizar la tarea
        task.status = "processing"
        task.started_at = time.time()
        task.worker_id = self.worker_id
        
        # Reemplazar en la cola de processing
        self.redis.lrem(REDIS_QUEUE_PROCESSING, 1, task_json)
        self.redis.lpush(REDIS_QUEUE_PROCESSING, task.to_json())
        
        # Actualizar contadores
        self.redis.decr(REDIS_COUNTER_PENDING)
        self.redis.incr(REDIS_COUNTER_PROCESSING)
        
        # Establecer TTL para esta tarea
        self.redis.set(f"task:{task.task_id}:heartbeat", "1", ex=TASK_PROCESSING_TTL)
        
        logger.info(f"Starting task {task.task_id} for company {task.company_id}")
        return task
    
    def complete_task(self, task: Task, success: bool, result: Dict = None, error: str = None):
        """Marca una tarea como completada o fallida"""
        # Eliminar de la cola de processing
        self.redis.lrem(REDIS_QUEUE_PROCESSING, 1, task.to_json())
        
        # Actualizar la tarea
        task.completed_at = time.time()
        task.status = "completed" if success else "failed"
        task.result = result
        task.error = error
        
        # Añadir a la cola correspondiente
        if success:
            self.redis.lpush(REDIS_QUEUE_COMPLETED, task.to_json())
            self.redis.incr(REDIS_COUNTER_COMPLETED)
            
            # Registrar métricas de éxito
            processing_time = task.completed_at - task.started_at
            self.redis.lpush(f"{REDIS_METRICS_PREFIX}processing_times", processing_time)
            self.redis.ltrim(f"{REDIS_METRICS_PREFIX}processing_times", 0, 999)  # Mantener últimas 1000
        else:
            self.redis.lpush(REDIS_QUEUE_FAILED, task.to_json())
            self.redis.incr(REDIS_COUNTER_FAILED)
            
            # Registrar el error
            self.redis.lpush(f"{REDIS_METRICS_PREFIX}errors", error or "Unknown error")
            self.redis.ltrim(f"{REDIS_METRICS_PREFIX}errors", 0, 99)  # Mantener últimos 100
        
        # Decrementar contador de processing
        self.redis.decr(REDIS_COUNTER_PROCESSING)
        
        # Eliminar heartbeat
        self.redis.delete(f"task:{task.task_id}:heartbeat")
        
        logger.info(f"Task {task.task_id} marked as {'completed' if success else 'failed'}")
    
    def heartbeat(self, task: Task):
        """Actualiza el heartbeat de una tarea para evitar que expire"""
        self.redis.set(f"task:{task.task_id}:heartbeat", "1", ex=TASK_PROCESSING_TTL)
    
    def get_queue_stats(self) -> Dict[str, int]:
        """Obtiene estadísticas sobre las colas"""
        return {
            "pending": int(self.redis.get(REDIS_COUNTER_PENDING) or 0),
            "processing": int(self.redis.get(REDIS_COUNTER_PROCESSING) or 0),
            "completed": int(self.redis.get(REDIS_COUNTER_COMPLETED) or 0),
            "failed": int(self.redis.get(REDIS_COUNTER_FAILED) or 0)
        }
    
    def reset_queues(self):
        """Resetea todas las colas (¡usar con precaución!)"""
        self.redis.delete(
            REDIS_QUEUE_PENDING,
            REDIS_QUEUE_PROCESSING,
            REDIS_QUEUE_COMPLETED,
            REDIS_QUEUE_FAILED,
            REDIS_COUNTER_PENDING,
            REDIS_COUNTER_PROCESSING,
            REDIS_COUNTER_COMPLETED,
            REDIS_COUNTER_FAILED
        )
        logger.warning("All queues have been reset")