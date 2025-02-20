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

# Configuración de Hardware (personalizable por cada desarrollador)
HARDWARE_CONFIG = {
    "gpu_memory": os.getenv("GPU_MEMORY", "24GB"),
    "total_ram": os.getenv("TOTAL_RAM", "128GB"),
    "max_workers": int(os.getenv("MAX_WORKERS", "8")),
    "gpu_enabled": os.getenv("GPU_ENABLED", "True").lower() in ["true", "1"],
    "cuda_visible_devices": os.getenv("CUDA_VISIBLE_DEVICES", "0"),
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

# Configuración de Ollama (if you still use it for some tasks)
OLLAMA_ENDPOINT = "http://localhost:11434/api/generate"

# Configuración de Groq
# Add the Groq API key here and update the model for database queries.
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
if not GROQ_API_KEY:
    raise ValueError("GROQ_API_KEY not found in environment variables")
# Dado que la librería de Groq gestiona internamente el endpoint, no es necesario especificarlo.

# Configuración de Ollama
OLLAMA_ENDPOINT = "http://localhost:11434/api/generate"
LLM_MODELS = {
    # For querying the database, use the desired Groq model:
    "base_datos": "deepseek-r1-distill-llama-70b",
    # For scraping tasks, we can keep the existing model or update as needed:
    "scraping": "qwen-2.5-32b"
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