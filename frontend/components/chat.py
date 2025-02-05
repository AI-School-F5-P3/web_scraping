# frontend/components/chat.py
import streamlit as st
import pandas as pd
from typing import List, Dict

class ChatInterface:
    def __init__(self):
        if "messages" not in st.session_state:
            st.session_state.messages = []

    def display_chat_history(self):
        for message in st.session_state.messages:
            with st.chat_message(message["role"], avatar=message.get("avatar")):
                st.markdown(message["content"])
                if "results" in message:
                    self.display_results(message["results"])

    def display_results(self, results: pd.DataFrame):
        st.dataframe(results, hide_index=True)
        self.add_download_button(results)

    def add_download_button(self, df: pd.DataFrame):
        st.download_button(
            label="Descargar CSV",
            data=df.to_csv(index=False).encode('utf-8'),
            file_name='resultados_consulta.csv',
            mime='text/csv'
        )

    def add_message(self, role: str, content: str, results: pd.DataFrame = None):
        message = {
            "role": role,
            "content": content,
            "avatar": "🧑💻" if role == "user" else "🤖"
        }
        if results is not None:
            message["results"] = results
        st.session_state.messages.append(message)