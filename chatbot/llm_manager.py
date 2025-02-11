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
    def get_llm(provider: Optional[Union[str, LLMProvider]] = None) -> BaseLanguageModel:
        """Get LLM instance based on provider"""
        # Convert string to enum if string provided
        if isinstance(provider, str):
            provider = LLMProvider.from_string(provider)
        else:
            provider = provider or Config.LLM_PROVIDER
            
        try:
            if provider == LLMProvider.DEEPSEEK:
                return ChatOllama(
                    model=Config.OLLAMA_MODEL,
                    base_url=Config.OLLAMA_BASE_URL,
                    temperature=0.7
                )
            elif provider == LLMProvider.OPENAI:
                if not Config.OPENAI_API_KEY:
                    raise ValueError("OpenAI API key not configured")
                return ChatOpenAI(
                    model_name=Config.OPENAI_MODEL,
                    api_key=Config.OPENAI_API_KEY,
                    temperature=0.7
                )
            else:
                raise ValueError(f"Unsupported LLM provider: {provider}")
        except Exception as e:
            logger.error(f"Failed to initialize LLM: {str(e)}")
            raise

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