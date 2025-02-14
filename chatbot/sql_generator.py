# chatbot/sql_generator.py
from typing import Dict, Any, List
import streamlit as st
from config import Config, LLMProvider
from chatbot.llm_manager import LLMManager
from langchain.schema import HumanMessage, SystemMessage
import logging

logger = logging.getLogger(__name__)

class SQLGenerator:
    def __init__(self):
        try:
            # Get provider string from session state or config
            provider = st.session_state.get("llm_provider", Config.LLM_PROVIDER)
            self.llm = LLMManager.get_llm(provider)
        except Exception as e:
            logger.error(f"Failed to initialize LLM in SQLGenerator: {str(e)}")
            raise
    
    def generate_sql(self, query: str) -> str:
        """Generate SQL query from natural language input"""
        system_prompt = """You are a SQL expert. Given a natural language query about companies data, 
        generate SQL for table 'empresas' with columns: id, codigo_infotel, nif, razon_social, 
        direccion, codigo_postal, poblacion, provincia, website, url_valid, telefonos, redes_sociales, 
        ecommerce, fecha_actualizacion, confidence_score. Generate only SQL, no explanations. Answer in Spanish."""
        
        try:
            messages = [
                SystemMessage(content=system_prompt),
                HumanMessage(content=query)
            ]
            
            # Agregar log para imprimir el prompt que se envía:
            logger.info(f"SQLGenerator.generate_sql - Prompt messages: {messages}")
            
            response = self.llm.predict_messages(messages)
            
            # Agregar log para imprimir la respuesta obtenida
            logger.info(f"SQLGenerator.generate_sql - LLM response: {response.content}")
            
            return response.content.strip()
                    
        except Exception as e:
            logger.error(f"SQL generation error: {str(e)}")
            return f"Error generating SQL: {str(e)}"