# frontend/components/chat.py
from typing import Optional, Dict, Any
import streamlit as st
from chatbot.agent_setup import ScrapingAgent
import pandas as pd
import time
import logging
import traceback

logger = logging.getLogger(__name__)

class ChatInterface:
    def __init__(self, llm_provider: str):
        try:
            self.llm_provider = llm_provider
            self.agent = ScrapingAgent(llm_provider)
            if "messages" not in st.session_state:
                st.session_state.messages = []
        except Exception as e:
            logger.error(f"Failed to initialize ChatInterface: {str(e)}\n{traceback.format_exc()}")
            raise
        
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
        """Process query using agents with retries and better error handling"""
        if not query or len(query.strip()) < 3:
            return {"success": False, "error": "Query too short"}
            
        last_error = None
        for attempt in range(max_retries):
            try:
                result = self.agent.process_query(query)
                if not result:
                    raise ValueError("Empty result from agent")
                return result
            except Exception as e:
                last_error = e
                logger.warning(f"Attempt {attempt + 1} failed: {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                
                logger.error(f"All attempts failed: {str(e)}\n{traceback.format_exc()}")
                return {
                    "success": False,
                    "error": f"Error processing query: {str(last_error)}"
                }
    
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