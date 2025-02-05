# frontend/components/metrics.py
import streamlit as st
import pandas as pd
import plotly.express as px
from typing import Dict, List

class MetricsInterface:
    def __init__(self):
        self.metrics_data = {}

    def display_progress_metrics(self, stats: Dict[str, float]):
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Empresas Procesadas", f"{stats['processed']:,}", f"{stats['progress']}%")
        with col2:
            st.metric("Tasa de Éxito", f"{stats['success_rate']}%", f"{stats['improvement']}%")

    def display_geographic_analysis(self, data: pd.DataFrame):
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.bar(
                data,
                x="Provincia",
                y="Empresas",
                title="Distribución por Provincia",
                color="Provincia",
                text_auto=True
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            fig = px.imshow(
                data.set_index('Provincia'),
                title="Indicadores de Contactabilidad",
                aspect="auto"
            )
            st.plotly_chart(fig, use_container_width=True)