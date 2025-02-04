# scraper/rate_limiter.py
import time
import redis
from config import Config

class RateLimiter:
    def __init__(self, requests_per_second=2):
        self.redis = redis.Redis.from_url(Config.REDIS_URL)
        self.requests_per_second = requests_per_second
        self.window_size = 1  # 1 second
        
    def _get_current_window(self):
        return int(time.time())
    
    def can_make_request(self, domain):
        """
        Comprueba si se puede hacer una petición al dominio
        usando una ventana deslizante
        """
        current = self._get_current_window()
        key = f"ratelimit:{domain}:{current}"
        
        pipe = self.redis.pipeline()
        pipe.incr(key)
        pipe.expire(key, self.window_size)
        requests_in_window = pipe.execute()[0]
        
        return requests_in_window <= self.requests_per_second
    
    def wait_if_needed(self, domain):
        """
        Espera si es necesario para respetar el rate limit
        """
        while not self.can_make_request(domain):
            time.sleep(0.1)