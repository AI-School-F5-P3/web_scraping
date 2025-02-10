# chatbot/query_executor.py
from typing import Any, List, Dict
import pandas as pd
from sqlalchemy import text
from database.connectors import MySQLConnector

class QueryExecutor:
    def __init__(self):
        self.connector = MySQLConnector()
    
    def execute_query(self, sql: str) -> pd.DataFrame:
        """Execute SQL query and return results"""
        try:
            with self.connector.get_session() as session:
                # Wrap the SQL query in text() to properly handle SQL expressions
                result = session.execute(text(sql))
                return pd.DataFrame(result.fetchall(), columns=result.keys())
        except Exception as e:
            raise Exception(f"Error executing query: {str(e)}")
            
    def validate_query(self, sql: str) -> bool:
        """Validate SQL query before execution"""
        # Basic validation - check if it's a SELECT query
        sql_lower = sql.lower().strip()
        return sql_lower.startswith('select')