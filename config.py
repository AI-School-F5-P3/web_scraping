# config.py
from dotenv import load_dotenv
import os

load_dotenv()

class Config:
    # Existing Redis config remains the same
    REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    REDIS_TIMEOUT = 5
    
    # New MySQL config
    MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
    MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
    MYSQL_USER = os.getenv("MYSQL_USER", "root")
    MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
    MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "webscraping_db")
    
    # Create SQLAlchemy URL for MySQL
    SQLALCHEMY_DATABASE_URL = (
        f"mysql://{MYSQL_USER}:{MYSQL_PASSWORD}@"
        f"{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
    )
    LLM_PROVIDER = os.getenv("LLM_PROVIDER", "DEEPSEEK")  # Opciones: OPENAI/DEEPSEEK
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
    DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")