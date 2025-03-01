import redis
import os
from dotenv import load_dotenv

load_dotenv()

# Obtener variables de entorno
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")
REDIS_USERNAME = os.getenv("REDIS_USERNAME", "default")

# Imprimir valores (para debug)
print(f"Intentando conectar a Redis en {REDIS_HOST}:{REDIS_PORT}")
print(f"Usuario: {REDIS_USERNAME}")
print(f"Contraseña configurada: {'Sí' if REDIS_PASSWORD else 'No'}")

try:
    # Intentar conexión
    r = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        username=REDIS_USERNAME,
        socket_timeout=5,
        socket_connect_timeout=5,
        decode_responses=True
    )
    
    # Verificar conexión
    pong = r.ping()
    print(f"Conexión exitosa! Respuesta: {pong}")
    
    # Cerrar conexión
    r.close()
    
except Exception as e:
    print(f"Error al conectar: {type(e).__name__}: {str(e)}")