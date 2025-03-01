
import redis
import time
import os
from dotenv import load_dotenv

load_dotenv()

# Obtener variables de entorno
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")
REDIS_USERNAME = os.getenv("REDIS_USERNAME", "default")

def monitor_and_clean_connections(idle_threshold=300):
    """
    Monitorea y limpia conexiones inactivas a Redis
    
    Args:
        idle_threshold: Tiempo en segundos para considerar una conexión como inactiva
    """
    try:
        print(f"Conectando a Redis en {REDIS_HOST}:{REDIS_PORT}...")
        
        # Intentar conexión
        r = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            password=REDIS_PASSWORD,
            username=REDIS_USERNAME,
            socket_timeout=10,
            socket_connect_timeout=10,
            decode_responses=True
        )
        
        # Verificar conexión
        r.ping()
        print("Conexión establecida correctamente")
        
        # Obtener información del servidor
        info = r.info()
        print(f"Versión de Redis: {info.get('redis_version')}")
        print(f"Clientes conectados: {info.get('connected_clients')}")
        
        # Obtener lista de clientes
        try:
            clients = r.client_list()
            print(f"Total de clientes: {len(clients)}")
            
            # Identificar clientes por tiempo de inactividad
            idle_times = {}
            for threshold in [60, 300, 600, 1800, 3600]:
                count = sum(1 for c in clients if int(c.get('idle_seconds', 0)) > threshold)
                idle_times[threshold] = count
            
            print("Clientes inactivos:")
            print(f"- Más de 1 minuto: {idle_times[60]}")
            print(f"- Más de 5 minutos: {idle_times[300]}")
            print(f"- Más de 10 minutos: {idle_times[600]}")
            print(f"- Más de 30 minutos: {idle_times[1800]}")
            print(f"- Más de 1 hora: {idle_times[3600]}")
            
            # Preguntar si desea cerrar conexiones inactivas
            if idle_times[idle_threshold] > 0:
                choice = input(f"¿Deseas cerrar {idle_times[idle_threshold]} conexiones inactivas por más de {idle_threshold} segundos? (s/n): ")
                
                if choice.lower() == 's':
                    # Cerrar conexiones inactivas
                    closed = 0
                    for client in clients:
                        if int(client.get('idle_seconds', 0)) > idle_threshold:
                            try:
                                r.client_kill(addr=client['addr'])
                                closed += 1
                                print(f"Cerrada conexión: {client['addr']} (idle: {client['idle_seconds']}s)")
                            except Exception as e:
                                print(f"Error cerrando {client['addr']}: {e}")
                    
                    print(f"Se cerraron {closed} conexiones inactivas")
                    
                    # Verificar nuevamente
                    new_clients = r.client_list()
                    print(f"Clientes restantes: {len(new_clients)}")
                else:
                    print("No se cerraron conexiones")
            else:
                print(f"No hay conexiones inactivas por más de {idle_threshold} segundos")
                
        except Exception as e:
            print(f"Error obteniendo lista de clientes: {e}")
        
        # Cerrar conexión
        r.close()
        print("Conexión cerrada")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    monitor_and_clean_connections()