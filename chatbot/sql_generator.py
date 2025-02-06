# chatbot/sql_generator.py
from typing import Dict, Any
import openai
from config import Config

class SQLGenerator:
    def __init__(self):
        self.openai_client = openai.Client(api_key=Config.OPENAI_API_KEY)
        
    def generate_sql(self, query: str) -> str:
        """Generate SQL from natural language query"""
        response = self.openai_client.chat.completions.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "You are a SQL expert. Generate only SQL, no explanations."},
                {"role": "user", "content": query}
            ]
        )
        return response.choices[0].message.content