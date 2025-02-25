import os
from dotenv import load_dotenv

load_dotenv()

REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")
REDIS_USERNAME = os.getenv("REDIS_USERNAME", "default")

# Colas de trabajo
REDIS_QUEUE_PENDING = "scraper:pending"
REDIS_QUEUE_PROCESSING = "scraper:processing"
REDIS_QUEUE_COMPLETED = "scraper:completed"
REDIS_QUEUE_FAILED = "scraper:failed"

# Contadores
REDIS_COUNTER_PENDING = "scraper:count:pending"
REDIS_COUNTER_PROCESSING = "scraper:count:processing"
REDIS_COUNTER_COMPLETED = "scraper:count:completed"
REDIS_COUNTER_FAILED = "scraper:count:failed"

# Configuración de rate limiting
REDIS_RATE_LIMIT_KEY = "scraper:rate_limit"
REDIS_RATE_LIMIT_MAX = 60  # solicitudes por minuto

# Tiempo de vida (TTL) para tareas en proceso
TASK_PROCESSING_TTL = 3600  # 1 hora

# Métricas
REDIS_METRICS_PREFIX = "scraper:metrics:"