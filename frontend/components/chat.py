# frontend/components/chat.py
from typing import Optional, Dict, Any
import streamlit as st
from chatbot.agent_setup import ScrapingAgent
import pandas as pd

class ChatInterface:
    def __init__(self):
        self.agent = ScrapingAgent()
        
    def add_message(self, role: str, content: str, data: Optional[pd.DataFrame] = None):
        st.session_state.messages.append({
            "role": role,
            "content": content,
            "data": data
        })
    
    def process_query(self, query: str) -> Dict[str, Any]:
        """Process query using agents"""
        try:
            response = self.agent.process_query(query)
            return {
                "success": True,
                "response": response
            }
        except Exception as e:
            return {
                "success": False,
                "error": str(e)
            }
    
    def display_chat_history(self):
        for message in st.session_state.messages:
            with st.chat_message(message["role"]):
                st.markdown(message["content"])
                if message.get("data") is not None:
                    st.dataframe(message["data"])