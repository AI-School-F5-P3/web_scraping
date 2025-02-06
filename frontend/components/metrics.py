# frontend/components/metrics.py
import streamlit as st
import pandas as pd
import plotly.express as px
from typing import Dict, List
from database.connectors import MySQLConnector
import sqlalchemy
import logging
from sqlalchemy import inspect, func

class MetricsInterface:
    def __init__(self):
        self.connector = MySQLConnector()
        
    def display_geographic_analysis(self):
        """Display geographic analysis of company data"""
        try:
            # Fetch provincia data from MySQL
            with self.connector.get_session() as session:
                result = session.execute(sqlalchemy.text("""
                    SELECT provincia, COUNT(*) as empresa_count,
                           SUM(CASE WHEN url_valid = 1 THEN 1 ELSE 0 END) as valid_urls
                    FROM empresas 
                    GROUP BY provincia 
                    ORDER BY empresa_count DESC 
                    LIMIT 10
                """))
                provincia_data = pd.DataFrame(result.fetchall(), 
                                           columns=['Provincia', 'Empresas', 'URLs_Validas'])
            
            if provincia_data.empty:
                st.info("No hay datos disponibles para el análisis geográfico")
                return
            
            # Calculate contactability metrics
            provincia_data['Tasa_URLs'] = (provincia_data['URLs_Validas'] / 
                                         provincia_data['Empresas'] * 100).round(2)
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Companies per province bar chart
                fig1 = px.bar(
                    provincia_data,
                    x="Provincia",
                    y="Empresas",
                    title="Distribución por Provincia",
                    color="Provincia"
                )
                fig1.update_layout(showlegend=False)
                st.plotly_chart(fig1, use_container_width=True)

            with col2:
                # URL validity rate heatmap
                fig2 = px.imshow(
                    provincia_data[['Provincia', 'Tasa_URLs']].set_index('Provincia'),
                    title="Tasa de URLs Válidas por Provincia (%)",
                    aspect="auto",
                    color_continuous_scale="Blues"
                )
                st.plotly_chart(fig2, use_container_width=True)

        except Exception as e:
            st.error(f"Error en análisis geográfico: {str(e)}")
            logging.error(f"Geographic analysis error: {str(e)}")