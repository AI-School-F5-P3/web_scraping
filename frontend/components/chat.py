# frontend/components/chat.py
import streamlit as st
import pandas as pd
from typing import List, Dict
from database.connectors import MySQLConnector
import sqlalchemy
from sqlalchemy import text
from chatbot.sql_generator import SQLGenerator
from chatbot.query_executor import QueryExecutor
import logging

class ChatInterface:
    def __init__(self):
        if "messages" not in st.session_state:
            st.session_state.messages = []
        self.sql_generator = SQLGenerator()
        self.query_executor = QueryExecutor()
        self.connector = MySQLConnector()

    def display_chat_history(self):
        """Display the chat history and results"""
        for message in st.session_state.messages:
            with st.chat_message(message["role"], avatar=message.get("avatar", message["role"])):
                st.markdown(message["content"])
                
                # If there are results, display them
                if "results" in message and not message["results"].empty:
                    st.dataframe(
                        message["results"],
                        use_container_width=True,
                        hide_index=True
                    )

    def process_query(self, query: str) -> tuple[str, pd.DataFrame]:
        """Process natural language query and return SQL + results"""
        try:
            # Add loading state
            with st.spinner('Procesando consulta...'):
                # Generate SQL from natural language
                sql = self.sql_generator.generate_sql(query)
                
                # Execute the query if valid
                if sql and self.query_executor.validate_query(sql):
                    results = self.query_executor.execute_query(sql)
                    return (
                        f"```sql\n{sql}\n```\n\nResultados encontrados: {len(results)} registros", 
                        results
                    )
                else:
                    return "No pude procesar esta consulta. ¿Podrías reformularla?", pd.DataFrame()
                
        except Exception as e:
            logging.error(f"Query processing error: {str(e)}")
            return "Lo siento, hubo un error procesando tu consulta.", pd.DataFrame()

    def add_message(self, role: str, content: str, results: pd.DataFrame = None):
        message = {
            "role": role,
            "content": content
        }
        if results is not None and not results.empty:
            message["results"] = results
        st.session_state.messages.append(message)
        # Force Streamlit to rerun and show the new message
        st.experimental_rerun()

    def clear_chat(self):
        """Clear the chat history"""
        st.session_state.messages = []