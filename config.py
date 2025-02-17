# config.py
from dotenv import load_dotenv
import os
from enum import Enum

load_dotenv()

class LLMProvider(Enum):
    DEEPSEEK = "deepseek"
    GROQ = "groq"
    
    @classmethod
    def from_string(cls, value: str) -> 'LLMProvider':
        """Convert string to enum value, case-insensitive"""
        try:
            value = value.upper()
            if value == "DEEPSEEK":
                return cls.DEEPSEEK
            elif value == "GROQ":
                return cls.GROQ
            else:
                return cls.DEEPSEEK  # Default
        except (AttributeError, KeyError):
            return cls.DEEPSEEK  # Default if conversion fails

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
    LLM_PROVIDER = os.getenv("LLM_PROVIDER", "deepseek")  # Store as string
    OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
    OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "deepseek-r1:1.5b")

    # Configuración de Groq
    GROQ_API_KEY = os.getenv("GROQ_API_KEY")  # Tu clave de API de Groq
    GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")  # Declarar el modelo Groq aquí    
    