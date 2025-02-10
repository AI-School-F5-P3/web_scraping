# config.py
from dotenv import load_dotenv
import os

load_dotenv()

class Config:
    # Database configs
    REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    
    # MySQL configuration
    MYSQL_USER = os.getenv("MYSQL_USER", "root")
    MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
    MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
    MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
    MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "webscraping_db")
    
    # SQLAlchemy URL
    SQLALCHEMY_DATABASE_URL = (
        f"mysql://{MYSQL_USER}:{MYSQL_PASSWORD}@"
        f"{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
    )
    
    # LLM Configuration
    LLM_PROVIDER = os.getenv("LLM_PROVIDER", "OLLAMA")
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
    
    # Ollama Configuration (if needed)
    OLLAMA_BASE_URL = "http://localhost:11434"
    

    