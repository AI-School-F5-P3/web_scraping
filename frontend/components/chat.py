# frontend/components/chat.py
from typing import Optional, Dict, Any
import streamlit as st
from chatbot.agent_setup import ScrapingAgent
import pandas as pd
import time
import logging
logger = logging.getLogger(__name__)

class ChatInterface:
    def __init__(self, llm_provider: str):
        self.llm_provider = llm_provider
        self.agent = ScrapingAgent()
        if "messages" not in st.session_state:
            st.session_state.messages = []
        
    def add_message(self, role: str, content: str, data: Optional[pd.DataFrame] = None):
        """Add a message to the chat history"""
        if isinstance(content, dict):
            # Handle dictionary responses from agent
            if "response" in content:
                content = content["response"]
            elif "error" in content:
                content = f"Error: {content['error']}"
        
        st.session_state.messages.append({
            "role": role,
            "content": content,
            "data": data
        })
    
    def process_query(self, query: str, max_retries: int = 3) -> Dict[str, Any]:
        """Process query using agents with retries"""
        for attempt in range(max_retries):
            try:
                result = self.agent.process_query(query)
                return result
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(f"Failed after {max_retries} attempts: {str(e)}")
                    return {"success": False, "error": str(e)}
                time.sleep(2 ** attempt)  # Exponential backoff
    
    def clear_history(self):
        """Clear chat history"""
        st.session_state.messages = []
        # Force a rerun to update the UI immediately
        st.rerun()
    
    def display_chat_history(self):
        """Display chat history in a container"""
        # Create a container for the chat history
        chat_container = st.container()
        
        with chat_container:
            # Add clear history button at the top
            if st.session_state.messages:  # Only show if there are messages
                if st.button("🧹 Limpiar Historial", key=f"clear_history_{self.llm_provider}"):
                    self.clear_history()
            
            # Display messages
            for message in st.session_state.messages:
                with st.chat_message(message["role"]):
                    st.markdown(message["content"])
                    if message.get("data") is not None:
                        st.dataframe(message["data"])