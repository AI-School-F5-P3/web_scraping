# chatbot/sql_generator.py
from typing import Dict, Any
import openai
from openai import OpenAI
import requests
import json
from config import Config
import streamlit as st
import time
import tiktoken

class SQLGenerator:
    def __init__(self):
        self.llm_provider = st.session_state.get("llm_provider", "DeepSeek")
        if self.llm_provider == "OpenAI":
            self.client = OpenAI(api_key=Config.OPENAI_API_KEY)
            self.token_limit = 4000  # Set a reasonable token limit
        self.last_call_time = time.time()
        self.min_time_between_calls = 1  # minimum seconds between API calls
        
    def count_tokens(self, text: str) -> int:
        """Count tokens in text for GPT-4"""
        if self.llm_provider == "OpenAI":
            encoding = tiktoken.encoding_for_model("gpt-4")
            return len(encoding.encode(text))
        return 0
        
    def generate_sql(self, query: str) -> str:
        # Add rate limiting
        time_since_last_call = time.time() - self.last_call_time
        if time_since_last_call < self.min_time_between_calls:
            time.sleep(self.min_time_between_calls - time_since_last_call)
        
        # Check token count for OpenAI
        if self.llm_provider == "OpenAI":
            token_count = self.count_tokens(query)
            if token_count > self.token_limit:
                return f"Query too long. Please reduce length (current tokens: {token_count}, limit: {self.token_limit})"
        
        self.last_call_time = time.time()
        
        """Generate SQL from natural language query"""
        system_prompt = """You are a SQL expert. Given a natural language query about companies data, 
        generate SQL for table 'empresas' with columns: id, codigo_infotel, nif, razon_social, 
        direccion, codigo_postal, poblacion, provincia, website, url_valid, telefonos, redes_sociales, 
        ecommerce, fecha_actualizacion, confidence_score. Generate only SQL, no explanations."""
        
        try:
            if self.llm_provider == "OpenAI":
                response = self.client.chat.completions.create(
                    model="gpt-4",
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": query}
                    ]
                )
                return response.choices[0].message.content
            else:  # Using Ollama
                response = requests.post(
                    f"{Config.OLLAMA_BASE_URL}/api/generate",
                    json={
                        "model": "deepseek-r1",
                        "prompt": f"{system_prompt}\n\nUser: {query}",
                        "stream": False
                    }
                )
                response_data = response.json()
                return response_data['response']
                
        except Exception as e:
            return f"Error generating SQL: {str(e)}"