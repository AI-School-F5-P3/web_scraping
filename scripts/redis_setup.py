#!/usr/bin/env python3
"""
Script para configurar y optimizar Redis para el proyecto de web scraping.
Este script aumenta el límite de conexiones y realiza otras configuraciones.
"""

import redis
import os
import sys
import logging
from dotenv import load_dotenv
from redis.exceptions import ConnectionError, ResponseError, AuthenticationError

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Cargar variables de entorno
load_dotenv()

# Obtener configuración de Redis
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")
REDIS_USERNAME = os.getenv("REDIS_USERNAME", "default")

def setup_redis():
    """Configura Redis con parámetros optimizados"""
    try:
        # Conectar a Redis
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
        r.ping()
        logger.info("Conexión a Redis establecida correctamente")
        
        # Obtener información del servidor
        info = r.info()
        logger.info(f"Versión de Redis: {info.get('redis_version')}")
        logger.info(f"Modo de Redis: {info.get('redis_mode', 'standalone')}")
        logger.info(f"Memoria usada: {info.get('used_memory_human')}")
        logger.info(f"Sistema operativo: {info.get('os')}")
        
        # Verificar configuración actual
        current_config = r.config_get('*')
        current_maxclients = current_config.get('maxclients')
        current_timeout = current_config.get('timeout')
        
        logger.info(f"Configuración actual:")
        logger.info(f"- maxclients: {current_maxclients}")
        logger.info(f"- timeout: {current_timeout}")
        
        # Obtener estadísticas de clientes
        clients_info = r.info('clients')
        connected_clients = clients_info.get('connected_clients', 0)
        blocked_clients = clients_info.get('blocked_clients', 0)
        
        logger.info(f"Clientes conectados: {connected_clients}/{current_maxclients}")
        logger.info(f"Clientes bloqueados: {blocked_clients}")
        
        # Intentar aumentar el límite de clientes
        try:
            # Establecer un nuevo límite más alto
            new_maxclients = 10000
            r.config_set('maxclients', new_maxclients)
            logger.info(f"✅ Límite de clientes aumentado a {new_maxclients}")
            
            # Aumentar el timeout para conexiones inactivas (0 = nunca desconectar)
            # Recomendado solo si tienes pocos clientes o suficiente RAM
            # r.config_set('timeout', 300)  # 5 minutos
            # logger.info("✅ Timeout configurado a 5 minutos")
            
            # Guardar configuración para que persista entre reinicios
            try:
                r.config_rewrite()
                logger.info("✅ Configuración guardada permanentemente")
            except ResponseError as rw_error:
                logger.warning(f"No se pudo guardar la configuración permanentemente: {str(rw_error)}")
                logger.warning("La configuración se perderá si Redis se reinicia.")
            
        except ResponseError as config_error:
            if "unsupported CONFIG parameter" in str(config_error):
                logger.error("❌ Este servidor Redis no permite cambiar esta configuración.")
                logger.error("Es posible que no tengas permisos o estés usando un servicio gestionado.")
                logger.error("Contacta con el administrador del servicio para modificar estos parámetros.")
            else:
                logger.error(f"❌ Error configurando Redis: {str(config_error)}")
        
        # Verificar conexiones activas
        try:
            client_list = r.client_list()
            logger.info(f"Lista de clientes conectados: {len(client_list)}")
            
            # Mostrar los primeros 5 clientes
            for i, client in enumerate(client_list[:5]):
                logger.info(f"Cliente {i+1}: {client.get('addr')} - {client.get('name', 'sin nombre')} - Idle: {client.get('idle_seconds', 'N/A')}s")
            
            # Buscar clientes inactivos por largo tiempo (más de 1 hora)
            idle_clients = [c for c in client_list if int(c.get('idle_seconds', 0)) > 3600]
            if idle_clients:
                logger.warning(f"Hay {len(idle_clients)} clientes inactivos por más de 1 hora.")
                
                # Opcionalmente, cerrar clientes inactivos
                kill_idle = input("¿Deseas cerrar estos clientes inactivos? (s/n): ").lower() == 's'
                if kill_idle:
                    for client in idle_clients:
                        try:
                            r.client_kill_filter(addr=client.get('addr'))
                            logger.info(f"Cliente cerrado: {client.get('addr')}")
                        except Exception as kill_error:
                            logger.error(f"Error cerrando cliente {client.get('addr')}: {str(kill_error)}")
        except Exception as list_error:
            logger.warning(f"No se pudo obtener lista de clientes: {str(list_error)}")
        
        # Verificar memoria disponible
        memory_info = r.info('memory')
        used_memory = memory_info.get('used_memory', 0)
        used_memory_peak = memory_info.get('used_memory_peak', 0)
        
        # Convertir a MB para mejor legibilidad
        used_memory_mb = used_memory / (1024 * 1024)
        used_memory_peak_mb = used_memory_peak / (1024 * 1024)
        
        logger.info(f"Memoria utilizada: {used_memory_mb:.2f} MB (pico: {used_memory_peak_mb:.2f} MB)")
        
        # Verificar política de memoria
        maxmemory = memory_info.get('maxmemory', 0)
        maxmemory_policy = current_config.get('maxmemory-policy', 'desconocida')
        
        if maxmemory > 0:
            maxmemory_mb = maxmemory / (1024 * 1024)
            logger.info(f"Límite de memoria: {maxmemory_mb:.2f} MB, política: {maxmemory_policy}")
        else:
            logger.info(f"Sin límite de memoria configurado, política: {maxmemory_policy}")
        
        # Cerrar conexión
        r.close()
        logger.info("Conexión a Redis cerrada")
        
        logger.info("✅ Configuración de Redis completada")
        return True
        
    except AuthenticationError:
        logger.error("❌ Error de autenticación. Verifica usuario y contraseña.")
        return False
    except ConnectionError:
        logger.error("❌ Error de conexión a Redis. Verifica host y puerto.")
        return False
    except Exception as e:
        logger.error(f"❌ Error inesperado: {str(e)}")
        return False

def reset_queues():
    """Reinicia las colas de Redis (USAR CON PRECAUCIÓN)"""
    try:
        # Conectar a Redis
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
        r.ping()
        logger.info("Conexión a Redis establecida correctamente")
        
        # Listar colas existentes
        from redis_config import (
            REDIS_QUEUE_PENDING, REDIS_QUEUE_PROCESSING, 
            REDIS_QUEUE_COMPLETED, REDIS_QUEUE_FAILED,
            REDIS_COUNTER_PENDING, REDIS_COUNTER_PROCESSING,
            REDIS_COUNTER_COMPLETED, REDIS_COUNTER_FAILED
        )
        
        queues = [
            REDIS_QUEUE_PENDING, REDIS_QUEUE_PROCESSING, 
            REDIS_QUEUE_COMPLETED, REDIS_QUEUE_FAILED
        ]
        
        counters = [
            REDIS_COUNTER_PENDING, REDIS_COUNTER_PROCESSING,
            REDIS_COUNTER_COMPLETED, REDIS_COUNTER_FAILED
        ]
        
        # Mostrar estado actual
        logger.info("Estado actual de las colas:")
        for queue in queues:
            count = r.llen(queue)
            logger.info(f"- {queue}: {count} elementos")
        
        logger.info("Estado actual de los contadores:")
        for counter in counters:
            value = r.get(counter) or '0'
            logger.info(f"- {counter}: {value}")
        
        # Confirmar reinicio
        confirm = input("⚠️ ADVERTENCIA: Esto eliminará todas las tareas en proceso. ¿Estás seguro? (escribir 'CONFIRMAR' para proceder): ")
        
        if confirm != "CONFIRMAR":
            logger.info("Operación cancelada por el usuario.")
            return False
        
        # Reiniciar colas
        for queue in queues:
            r.delete(queue)
            logger.info(f"Cola {queue} reiniciada")
        
        # Reiniciar contadores
        for counter in counters:
            r.set(counter, 0)
            logger.info(f"Contador {counter} reiniciado")
        
        logger.info("✅ Colas y contadores reiniciados correctamente")
        
        # Cerrar conexión
        r.close()
        logger.info("Conexión a Redis cerrada")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error reiniciando colas: {str(e)}")
        return False

if __name__ == "__main__":
    print("=== Configuración de Redis para Web Scraper ===")
    print("1. Configurar parámetros de Redis (maxclients, etc.)")
    print("2. Reiniciar colas y contadores (PRECAUCIÓN)")
    print("3. Salir")
    
    option = input("\nSelecciona una opción (1-3): ")
    
    if option == "1":
        print("\nConfigurando parámetros de Redis...")
        setup_redis()
    elif option == "2":
        print("\nPreparando para reiniciar colas y contadores...")
        reset_queues()
    elif option == "3":
        print("Saliendo...")
    else:
        print("Opción no válida.")