# chatbot/llm_manager.py
from typing import Optional, Union
from langchain_core.language_models.base import BaseLanguageModel
from langchain_ollama import ChatOllama
from langchain_openai import ChatOpenAI
from config import Config, LLMProvider
import logging

logger = logging.getLogger(__name__)

class LLMManager:
    @staticmethod
    def get_llm(provider: Optional[str] = None) -> BaseLanguageModel:
        """Get LLM instance based on provider string"""
        try:
            provider_str = provider or Config.LLM_PROVIDER
            provider_enum = LLMProvider.from_string(provider_str)
            
            if provider_enum == LLMProvider.DEEPSEEK:
                return ChatOllama(
                    model=Config.OLLAMA_MODEL,
                    base_url=Config.OLLAMA_BASE_URL,
                    temperature=0.7,
                    request_timeout=60  # Add timeout
                )
            elif provider_enum == LLMProvider.OPENAI:
                if not Config.OPENAI_API_KEY:
                    raise ValueError("OpenAI API key not configured")
                return ChatOpenAI(
                    model_name=Config.OPENAI_MODEL,
                    api_key=Config.OPENAI_API_KEY,
                    temperature=0.7
                )
            else:
                raise ValueError(f"Unsupported LLM provider: {provider_str}")
                
        except Exception as e:
            logger.error(f"Failed to initialize LLM: {str(e)}")
            # Return default model instead of raising
            return ChatOllama(
                model="deepseek-r1:1.5b",
                base_url="http://localhost:11434",
                temperature=0.7
            )

    @staticmethod
    def validate_ollama_connection() -> bool:
        """Validate that Ollama server is running and accessible"""
        try:
            llm = ChatOllama(
                model=Config.OLLAMA_MODEL,
                base_url=Config.OLLAMA_BASE_URL
            )
            # Try a simple completion to test connection
            llm.invoke("test")
            return True
        except Exception as e:
            logger.error(f"Ollama connection test failed: {str(e)}")
            return False