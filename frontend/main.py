# frontend/main.py
import streamlit as st
import pandas as pd
import plotly.express as px
import time
from datetime import datetime
import sys
import os
from pathlib import Path
import redis
from typing import Dict, List, Optional
import json

# Add root directory to PYTHONPATH
current_dir = Path(__file__).parent
project_root = current_dir.parent
sys.path.append(str(project_root))

from scraper.main import RegistradoresScraper
from config import Config
from components.chat import ChatInterface
from components.metrics import MetricsInterface

def initialize_redis() -> Optional[redis.Redis]:
    """Initialize Redis connection"""
    try:
        r = redis.Redis.from_url(Config.REDIS_URL)
        r.ping()
        return r
    except redis.exceptions.ConnectionError:
        st.error("❌ Error de conexión a Redis")
        return None

def load_css():
    """Load custom CSS styles"""
    css_path = Path(__file__).parent / "assets" / "styles.css"
    if css_path.exists():
        with open(css_path) as f:
            st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

def initialize_session_state():
    """Initialize session state variables"""
    if "messages" not in st.session_state:
        st.session_state.messages = []
    if "llm_provider" not in st.session_state:
        st.session_state.llm_provider = Config.LLM_PROVIDER
    if "scraping_active" not in st.session_state:
        st.session_state.scraping_active = False

def get_scraping_stats(redis_client: redis.Redis) -> Dict:
    """Get current scraping statistics from Redis"""
    try:
        stats = redis_client.hgetall("scraping:stats")
        total = int(stats.get(b'total', 0))
        completed = int(stats.get(b'completed', 0))
        failed = int(stats.get(b'failed', 0))
        
        return {
            "total": total,
            "completed": completed,
            "failed": failed,
            "progress": round((completed / total * 100) if total > 0 else 0, 1),
            "success_rate": round((completed / (completed + failed) * 100) if (completed + failed) > 0 else 0, 1)
        }
    except Exception as e:
        st.error(f"Error al obtener estadísticas: {str(e)}")
        return {
            "total": 0,
            "completed": 0,
            "failed": 0,
            "progress": 0,
            "success_rate": 0
        }

def render_sidebar(redis_client: redis.Redis):
    """Render sidebar content"""
    with st.sidebar:
        st.header("⚙️ Configuración del Sistema")
        
        # LLM Provider selector
        llm_provider = st.selectbox(
            "Seleccionar Modelo de Lenguaje:",
            ("DeepSeek", "OpenAI"),
            index=0 if Config.LLM_PROVIDER == "DEEPSEEK" else 1,
            help="Elija el proveedor para el motor de consultas analíticas"
        )
        
        # Scraping control
        if st.button("🚀 Iniciar Web Scraping", disabled=st.session_state.scraping_active):
            try:
                scraper = RegistradoresScraper()
                scraper.run()
                st.session_state.scraping_active = True
                st.success("¡Scraping iniciado!")
            except Exception as e:
                st.error(f"Error al iniciar scraping: {str(e)}")
        
        # Stats section
        st.markdown("---")
        st.subheader("📈 Métricas en Vivo")
        
        stats = get_scraping_stats(redis_client)
        col1, col2 = st.columns(2)
        with col1:
            st.metric(
                "Empresas Procesadas",
                f"{stats['completed']:,}",
                f"{stats['progress']}%"
            )
        with col2:
            st.metric(
                "Tasa de Éxito",
                f"{stats['success_rate']}%",
                None
            )
        
        # Resources section
        st.markdown("---")
        st.markdown("""
        **🔗 Recursos:**
        - [Documentación](https://docs.example.com)
        - [Monitor](https://grafana.example.com)
        - [Soporte](mailto:support@example.com)
        """)

def render_main_content(chat: ChatInterface, metrics: MetricsInterface, redis_client: redis.Redis):
    """Render main content area"""
    st.title("📊 Panel de Inteligencia Empresarial")
    st.markdown("### Consultas en Lenguaje Natural")
    
    # Chat interface
    chat_container = st.container()
    with chat_container:
        chat.display_chat_history()
        
        # Query input
        query = st.chat_input("Escribe tu consulta (ej: 'Empresas en Madrid con ecommerce')")
        if query:
            chat.add_message("user", query)
            
            # Process query (implement your query processing logic here)
            with st.spinner("Procesando consulta..."):
                time.sleep(1.5)  # Simulate processing
                
                # Example response
                response = f"**Respuesta usando {st.session_state.llm_provider}:**\n```sql\nSELECT * FROM empresas WHERE provincia = 'Madrid' AND ecommerce = 1;\n```"
                
                # Example results
                results = pd.DataFrame({
                    "Empresa": ["Tech Corp", "Digital Solutions"],
                    "Provincia": ["Madrid", "Madrid"],
                    "Teléfono": ["+34911234567", ""],
                    "Ecommerce": ["Sí", "No"]
                })
                
                chat.add_message("assistant", response, results)
    
    # Analytics section
    st.markdown("---")
    render_analytics(metrics)

def render_analytics(metrics: MetricsInterface):
    """Render analytics section"""
    st.markdown("## 📊 Análisis Geográfico")
    
    # Sample data for visualization
    provincia_data = pd.DataFrame({
        "Provincia": ["Madrid", "Barcelona", "Valencia"],
        "Empresas": [45000, 32000, 15000]
    })
    
    contactabilidad_data = pd.DataFrame({
        "Provincia": ["Madrid", "Barcelona", "Valencia"],
        "Teléfonos Válidos (%)": [78.4, 65.2, 82.1],
        "Webs Activas (%)": [92.3, 88.7, 85.4]
    })
    
    metrics.display_geographic_analysis(provincia_data)
    
def render_preview_data(redis_client: redis.Redis):
    """Render preview of scraped data"""
    st.markdown("---")
    st.markdown("## 📋 Vista Previa de Datos Scrapeados")
    
    try:
        # Get latest scraped companies
        latest_keys = redis_client.keys("scraping_result:*")[-10:]
        preview_data = []
        
        for key in latest_keys:
            data = redis_client.hgetall(key)
            if data:
                # Convert bytes to string and parse JSON data
                company_data = {
                    k.decode('utf-8'): v.decode('utf-8')
                    for k, v in data.items()
                }
                
                # Parse social media JSON if exists
                try:
                    social_media = json.loads(company_data.get('social_media', '{}'))
                    social_links = ', '.join(social_media.keys())
                except json.JSONDecodeError:
                    social_links = ''
                
                preview_data.append({
                    "URL": company_data.get('url', ''),
                    "Teléfonos": company_data.get('phones', ''),
                    "Redes Sociales": social_links,
                    "E-commerce": "Sí" if company_data.get('ecommerce') == 'True' else "No",
                    "Confianza": f"{company_data.get('confidence_score', '0')}%",
                    "Última Actualización": datetime.fromisoformat(company_data.get('last_updated', datetime.now().isoformat())).strftime('%Y-%m-%d %H:%M')
                })
        
        if preview_data:
            df_preview = pd.DataFrame(preview_data)
            st.dataframe(
                df_preview,
                hide_index=True,
                use_container_width=True,
                column_config={
                    "URL": st.column_config.LinkColumn("URL"),
                    "E-commerce": st.column_config.CheckboxColumn("E-commerce"),
                    "Confianza": st.column_config.ProgressColumn(
                        "Índice de Confianza",
                        help="Porcentaje de confianza en los datos extraídos",
                        format="%d%%",
                        min_value=0,
                        max_value=100
                    ),
                    "Última Actualización": st.column_config.DatetimeColumn(
                        "Actualizado",
                        format="DD/MM/YY HH:mm"
                    )
                }
            )
        else:
            st.info("🗃️ Aún no hay datos scrapeados. Ejecuta el scraping para ver información.")
            
    except Exception as e:
        st.error(f"Error al cargar vista previa: {str(e)}")

def display_error_monitoring():
    """Display error monitoring section"""
    st.markdown("---")
    st.markdown("## 🚨 Monitoreo de Errores")
    
    # Placeholder for error monitoring - implement your error tracking logic here
    errors_data = pd.DataFrame({
        "Timestamp": pd.date_range(start="2024-02-01", periods=5, freq="D"),
        "Error": [
            "Connection timeout",
            "Rate limit exceeded",
            "Parse error",
            "Invalid URL",
            "Connection refused"
        ],
        "Count": [5, 3, 2, 1, 1]
    })
    
    st.dataframe(
        errors_data,
        hide_index=True,
        use_container_width=True,
        column_config={
            "Timestamp": st.column_config.DatetimeColumn(
                "Fecha",
                format="DD/MM/YY HH:mm"
            ),
            "Count": st.column_config.NumberColumn(
                "Ocurrencias",
                help="Número de veces que se ha producido el error"
            )
        }
    )

def main():
    """Main application entry point"""
    # Initialize app
    st.set_page_config(
        page_title="Business Intelligence Dashboard",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Load custom CSS
    load_css()
    
    # Initialize session state
    initialize_session_state()
    
    # Initialize components
    redis_client = initialize_redis()
    chat = ChatInterface()
    metrics = MetricsInterface()
    
    if redis_client is None:
        st.error("No se puede continuar sin conexión a Redis")
        return
    
    # Render layout
    render_sidebar(redis_client)
    render_main_content(chat, metrics, redis_client)
    render_preview_data(redis_client)
    
    # Only show error monitoring if there are errors
    if redis_client.scard("scraping:failed") > 0:
        display_error_monitoring()
    
    # Cleanup on session end
    if st.session_state.get('scraping_active'):
        try:
            progress = float(redis_client.get("scraping:progress") or 0)
            if progress >= 100:
                st.session_state.scraping_active = False
        except (TypeError, ValueError):
            pass

if __name__ == "__main__":
    main()