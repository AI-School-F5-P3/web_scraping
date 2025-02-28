# config.py

import os
from dotenv import load_dotenv

load_dotenv()

# Add LangSmith configurations
LANGSMITH_API_KEY = os.getenv("LANGSMITH_API_KEY")
LANGSMITH_PROJECT = "enterprise-analysis"
LANGSMITH_TRACE_ALL = True

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
    "chrome_options": os.getenv("CHROME_OPTIONS", "DEFAULT_CHROME_OPTIONS")
}
DEFAULT_CHROME_OPTIONS = [
        "--headless",
        "--no-sandbox",
        "--disable-gpu",
        "--disable-dev-shm-usage",
        "--enable-gpu-rasterization",
        "--enable-zero-copy",
        "--ignore-certificate-errors",
        "--disable-software-rasterizer"
    ]

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

# Configuración de Groq
# Add the Groq API key here and update the model for database queries.
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
if not GROQ_API_KEY:
    raise ValueError("GROQ_API_KEY not found in environment variables")
# Dado que la librería de Groq gestiona internamente el endpoint, no es necesario especificarlo.

# Modelos para consultas SQL (Groq)
SQL_MODELS = {
    "deepseek-r1": "deepseek-r1-distill-llama-70b",
    "gemma2": "gemma2-9b-it",
    "llama-3.3": "llama-3.3-70b-versatile",
    "qwen-2.5": "qwen-2.5-32b",
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
    'connect_timeout': 10,        # segundos para establecer conexión
    'read_timeout': 20,           # segundos para lectura
    'request_timeout': 30,        # timeout general
    'retry_count': 3,             # número de reintentos
    'retry_delay': 5,             # retraso base entre reintentos (segundos)
    'retry_backoff_factor': 0.5   # factor multiplicador para retroceso exponencial
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


# Configuración para el scraping
SCRAPING_CONFIG = {
    'max_urls_per_company': 10,       # Máximo número de URLs alternativas a verificar
    'max_parallel_requests': 4,       # Máximo número de solicitudes paralelas
    'rate_limit_per_minute': 30,      # Máximo número de solicitudes por minuto
    'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/91.0.4472.124 Safari/537.36'
}

# Configuración para la validación de datos
VALIDATION_CONFIG = {
    'max_url_length': 255,
    'max_company_name_length': 255,
    'valid_phone_prefixes': ['+34', '+1', '+44', '+33', '+49'],
    'valid_domains': ['.es', '.com', '.net', '.org', '.cat', '.eu', '.gal', '.eus']
}