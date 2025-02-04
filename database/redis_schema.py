# database/redis_schema.py
from enum import Enum
import redis
from config import Config

class RedisKeys:
    # Queues
    PENDING_QUEUE = "scraping:pending"
    PROCESSING_HASH = "scraping:processing"
    FAILED_SET = "scraping:failed"
    
    # Tracking
    COMPANY_PREFIX = "empresa:"
    PROGRESS_KEY = "scraping:progress"
    
    @classmethod
    def company_key(cls, nombre):
        return f"{cls.COMPANY_PREFIX}{nombre}"

class QueueManager:
    def __init__(self):
        self.redis = redis.Redis.from_url(Config.REDIS_URL)
    
    def add_to_pending(self, url):
        return self.redis.lpush(RedisKeys.PENDING_QUEUE, url)
    
    def mark_as_processing(self, url, worker_id):
        return self.redis.hset(RedisKeys.PROCESSING_HASH, url, worker_id)
    
    def mark_as_failed(self, url):
        self.redis.hdel(RedisKeys.PROCESSING_HASH, url)
        return self.redis.sadd(RedisKeys.FAILED_SET, url)
    
    def update_progress(self, completed, total):
        progress = (completed / total) * 100
        return self.redis.set(RedisKeys.PROGRESS_KEY, int(progress))
    
    def store_company_data(self, nombre, data):
        return self.redis.hset(RedisKeys.company_key(nombre), mapping=data)