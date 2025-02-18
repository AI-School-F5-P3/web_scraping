# config.py

import os
from dotenv import load_dotenv

load_dotenv()

# Configuración de Base de Datos
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "admin"),
    "database": os.getenv("DB_NAME", "web_scraping")
}

# Hardware Config
HARDWARE_CONFIG = {
    "gpu_memory": "0GB",  # Sin GPU dedicada
    "total_ram": "12GB",  # Dejando margen para el sistema operativo
    "max_workers": 4,     # Para un Ryzen 7 5700U (8 núcleos con SMT)
    "gpu_enabled": False, # GPU integrada Radeon no es ideal para ML
    "cuda_visible_devices": "",  # No aplica
    "chrome_options": [
        "--headless",
        "--no-sandbox",
        "--disable-gpu",
        "--disable-dev-shm-usage",
        "--enable-zero-copy",
        "--ignore-certificate-errors",
        "--disable-software-rasterizer",
        "--memory-pressure-off",
        "--js-flags=--expose-gc",  # Optimización de memoria JavaScript
        "--disable-notifications",
        "--disable-extensions"
    ]
}

# Lista oficial de provincias españolas
PROVINCIAS_ESPANA = [
    'Álava', 'Albacete', 'Alicante', 'Almería', 'Asturias', 'Ávila', 'Badajoz',
    'Barcelona', 'Burgos', 'Cáceres', 'Cádiz', 'Cantabria', 'Castellón',
    'Ciudad Real', 'Córdoba', 'Cuenca', 'Gerona', 'Granada', 'Guadalajara',
    'Guipúzcoa', 'Huelva', 'Huesca', 'Jaén', 'La Coruña', 'La Rioja', 'Las Palmas',
    'León', 'Lérida', 'Lugo', 'Madrid', 'Málaga', 'Murcia', 'Navarra', 'Orense',
    'Palencia', 'Pontevedra', 'Salamanca', 'Santa Cruz de Tenerife', 'Segovia',
    'Sevilla', 'Soria', 'Tarragona', 'Teruel', 'Toledo', 'Valencia', 'Valladolid',
    'Vizcaya', 'Zamora', 'Zaragoza'
]

# Configuración de Ollama
OLLAMA_ENDPOINT = "http://localhost:11434/api/generate"
LLM_MODELS = {
    "orquestador": "mistral:latest",  # Unificado a latest
    "base_datos": "mistral:latest",   # Unificado a latest
    "scraping": "mistral:latest"      # Unificado a latest
}
# Parámetros optimizados para Mistral 7B Instruct
OLLAMA_PARAMS = {
    "num_ctx": 2048,         # Reducido para mejor estabilidad
    "num_thread": 4,         # Mantenido para tu hardware
    "repeat_penalty": 1.1,   
    "temperature": 0.7,
    "top_k": 40,
    "top_p": 0.9,
    "num_gpu": 0,           # Añadido para explícitamente deshabilitar GPU
    "seed": 42,             # Añadido para consistencia
    "num_predict": 256,     # Añadido para limitar longitud de respuesta
    "stop": ["</answer>", "Human:", "Assistant:"],  # Tokens de parada
    "system": """Eres un asistente experto en extraer información de contacto de páginas web de empresas españolas.
                Tu tarea es identificar y extraer de forma precisa:
                - Teléfonos
                - Emails
                - Direcciones físicas
                - Redes sociales
                - Formularios de contacto"""
}

# Timeouts y reintentos
TIMEOUT_CONFIG = {
    "request_timeout": 60,    # Aumentado para dar más tiempo en hardware limitado
    "retry_attempts": 2,      # Reducido para evitar sobrecarga
    "retry_delay": 10
}

# Columnas requeridas para ingesta
REQUIRED_COLUMNS = [
    "COD_INFOTEL",
    "NIF",
    "RAZON_SOCIAL",
    "DOMICILIO", 
    "COD_POSTAL",
    "NOM_POBLACION",
    "NOM_PROVINCIA",
    "URL"
]



# Estados de URL
URL_STATUS_MESSAGES = {
    200: "OK",
    301: "Redirección Permanente",
    302: "Redirección Temporal",
    400: "Solicitud Incorrecta",
    403: "Acceso Prohibido",
    404: "No Encontrado",
    500: "Error Interno del Servidor",
    503: "Servicio No Disponible",
    504: "Tiempo de Espera Agotado"
}