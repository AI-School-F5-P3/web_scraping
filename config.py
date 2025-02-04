# config.py
import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    REDIS_TIMEOUT = 5  # segundos
    SQL_SERVER_CONN_STR = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={os.getenv('DB_SERVER')};"
        f"DATABASE={os.getenv('DB_NAME')};"
        f"UID={os.getenv('DB_USER')};"
        f"PWD={os.getenv('DB_PASSWORD')};"
        f"CONNECTION_TIMEOUT=60;"
    )
    LLM_PROVIDER = os.getenv("LLM_PROVIDER", "DEEPSEEK")  # Opciones: OPENAI/DEEPSEEK
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
    DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")