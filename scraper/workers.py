# scraper/workers.py
import redis
from config import Config
from .tasks import ScrapingTask
import logging
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

class ScrapingWorker:
    def __init__(self, worker_id: str):
        self.redis = redis.Redis.from_url(Config.REDIS_URL)
        self.worker_id = worker_id
        self.running = False

    def process_task(self, task: ScrapingTask) -> bool:
        try:
            logger.info(f"Worker {self.worker_id} processing task: {task.url}")
            task.last_attempt = datetime.now()
            task.retry_count += 1
            
            # Update stats atomically using pipeline
            pipe = self.redis.pipeline()
            pipe.incr("scraping:completed")
            pipe.execute()
            return True
            
        except Exception as e:
            logger.error(f"Error processing task {task.url}: {str(e)}")
            pipe = self.redis.pipeline()
            pipe.incr("scraping:failed")
            pipe.execute()
            return False

    def start(self):
        self.running = True
        logger.info(f"Worker {self.worker_id} started")
        
        while self.running:
            try:
                task_data = self.redis.brpop("scraping:pending", timeout=30)
                if task_data:
                    task = ScrapingTask.from_dict(eval(task_data[1]))
                    self.process_task(task)
            except Exception as e:
                logger.error(f"Worker error: {str(e)}")
                continue

    def stop(self):
        self.running = False
        logger.info(f"Worker {self.worker_id} stopped")
