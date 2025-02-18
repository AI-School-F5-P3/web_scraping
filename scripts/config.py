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
    "gpu_memory": "24GB",
    "total_ram": "128GB",
    "max_workers": 16,
    "gpu_enabled": True,
    "cuda_visible_devices": "0",  # Para NVIDIA GPU
    "chrome_options": [
        "--headless",
        "--no-sandbox",
        "--disable-gpu",
        "--disable-dev-shm-usage",
        "--enable-gpu-rasterization",
        "--enable-zero-copy",
        "--ignore-certificate-errors",
        "--disable-software-rasterizer"
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
    # "orquestador": "deepseek-r1:latest",
    "base_datos": "qwen2.5:7b",
    "scraping": "llama3.1:8b"
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

# Timeouts y reintentos
TIMEOUT_CONFIG = {
    "request_timeout": 30,
    "retry_attempts": 3,
    "retry_delay": 5
}

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