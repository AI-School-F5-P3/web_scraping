# scraper/workers.py
import redis
from config import Config

r = redis.Redis.from_url(Config.REDIS_URL)

def process_task(task):
    """Ejemplo básico de procesamiento de tarea"""
    print(f"Procesando tarea: {task}")
    # Lógica de scraping aquí
    r.lpush('scraping:processed', task)

def start_worker():
    while True:
        task = r.brpop('scraping:pending', timeout=30)
        if task:
            process_task(task[1].decode())