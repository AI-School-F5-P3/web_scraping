# chatbot/query_executor.py
from typing import Any, List, Dict
import pandas as pd
from sqlalchemy.orm import Session
from database.connectors import MySQLConnector

class QueryExecutor:
    def __init__(self):
        self.connector = MySQLConnector()
    
    def execute_query(self, sql: str) -> pd.DataFrame:
        """Execute SQL query and return results"""
        with self.connector.get_session() as session:
            result = session.execute(sql)
            return pd.DataFrame(result.fetchall(), columns=result.keys())
            
    def validate_query(self, sql: str) -> bool:
        """Validate SQL query before execution"""
        # Add validation logic
        return True