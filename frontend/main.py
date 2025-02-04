# frontend/main.py
import streamlit as st
import pandas as pd
import plotly.express as px
import time
import sys
import os
from pathlib import Path
import redis

# Añadir directorio raíz al PYTHONPATH
current_dir = Path(__file__).parent
project_root = current_dir.parent  # Solo un .parent ya que estamos en frontend/
sys.path.append(str(project_root))

from scraper.main import RegistradoresScraper
from config import Config

# Inicializa la conexión Redis
try:
    r = redis.Redis.from_url(Config.REDIS_URL)
    r.ping()  # Test de conexión
except redis.exceptions.ConnectionError:
    st.error("Error de conexión a Redis")
    r = None  # Manejar casos donde Redis no está disponible
    
# Modificar el progreso:
progress = r.get('scraping:progress') if r else 0


# Configuración inicial de la página
st.set_page_config(
    page_title="Business Intelligence Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Cargar estilos CSS personalizados
def load_css():
    with open("frontend/assets/styles.css") as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

load_css()

# Inicializar estado de la sesión
if "messages" not in st.session_state:
    st.session_state.messages = []
    
if "llm_provider" not in st.session_state:
    st.session_state.llm_provider = Config.LLM_PROVIDER

# Barra lateral con controles
with st.sidebar:
    st.header("⚙️ Configuración del Sistema")
    
    # Selector de proveedor LLM
    llm_provider = st.selectbox(
        "Seleccionar Modelo de Lenguaje:",
        ("DeepSeek", "OpenAI"),
        index=0 if Config.LLM_PROVIDER == "DEEPSEEK" else 1,
        help="Elija el proveedor para el motor de consultas analíticas"
    )
    
# Añadir botón de scraping
    if st.button("🚀 Iniciar Web Scraping"):
        with st.spinner("Escrapeando datos..."):
            scraper = RegistradoresScraper()
            scraper.run()
            st.success("Scraping completado!")

    
    # Separador visual
    st.markdown("---")
    
    # Estadísticas en tiempo real
    st.subheader("📈 Métricas en Vivo")
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Empresas Procesadas", "124,850", "98%")
    with col2:
        st.metric("Tasa de Éxito", "92.3%", "+2.1%")
    
    # Enlaces útiles
    st.markdown("---")
    st.markdown("""
    **🔗 Recursos:**
    - [Documentación](https://tu-docs.com)
    - [Panel de Control](https://grafana.tu-app.com)
    - [Soporte Técnico](mailto:soporte@empresa.com)
    """)

# Área principal del dashboard
st.title("📊 Panel de Inteligencia Empresarial")
st.markdown("### Consultas en Lenguaje Natural")

# Historial de chat
chat_container = st.container()
with chat_container:
    for message in st.session_state.messages:
        with st.chat_message(message["role"], avatar=message.get("avatar")):
            st.markdown(message["content"])
            
            if "results" in message:
                st.dataframe(message["results"], hide_index=True)
                st.download_button(
                    label="Descargar CSV",
                    data=message["results"].to_csv(index=False).encode('utf-8'),
                    file_name='resultados_consulta.csv',
                    mime='text/csv'
                )

# Input de usuario
user_query = st.chat_input("Escribe tu consulta (ej: 'Empresas en Madrid con ecommerce')")

if user_query:
    # Mostrar mensaje del usuario
    with chat_container:
        st.chat_message("user", avatar="🧑💻").markdown(user_query)
        st.session_state.messages.append({
            "role": "user",
            "content": user_query,
            "avatar": "🧑💻"
        })
    
    # Simular procesamiento (implementar lógica real aquí)
    with st.spinner("Procesando consulta..."):
        time.sleep(1.5)
        
        # Respuesta generada
        with chat_container:
            response = f"**Respuesta usando {llm_provider}:**\n```sql\nSELECT * FROM empresas WHERE provincia = 'Madrid' AND ecommerce = 1;\n```"
            
            # Mensaje del sistema
            with st.chat_message("assistant", avatar="🤖"):
                st.markdown(response)
                
                # Resultados de ejemplo
                sample_data = pd.DataFrame({
                    "Empresa": ["Tech Corp", "Digital Solutions"],
                    "Provincia": ["Madrid", "Madrid"],
                    "Teléfono": ["+34911234567", ""],
                    "Ecommerce": ["Sí", "No"]
                })
                
                st.dataframe(sample_data, hide_index=True)
                
                # Visualización
                fig = px.pie(
                    sample_data,
                    names="Ecommerce",
                    title="Distribución de Ecommerce",
                    color_discrete_sequence=px.colors.qualitative.Pastel
                )
                st.plotly_chart(fig, use_container_width=True)
            
            # Guardar en historial
            st.session_state.messages.append({
                "role": "assistant",
                "content": response,
                "results": sample_data,
                "avatar": "🤖"
            })

# Sección de análisis visual
st.markdown("---")
st.markdown("## Análisis Geográfico")

col1, col2 = st.columns(2)
with col1:
    # Gráfico de distribución por provincia
    provincia_data = pd.DataFrame({
        "Provincia": ["Madrid", "Barcelona", "Valencia"],
        "Empresas": [45000, 32000, 15000]
    })
    
    fig = px.bar(
        provincia_data,
        x="Provincia",
        y="Empresas",
        title="Distribución por Provincia",
        color="Provincia",
        text_auto=True
    )
    st.plotly_chart(fig, use_container_width=True)

with col2:
    # Mapa de calor de contactabilidad
    contactabilidad_data = pd.DataFrame({
        "Provincia": ["Madrid", "Barcelona", "Valencia"],
        "Teléfonos Válidos (%)": [78.4, 65.2, 82.1],
        "Webs Activas (%)": [92.3, 88.7, 85.4]
    })
    
    fig = px.imshow(
        contactabilidad_data.set_index('Provincia'),
        labels=dict(x="Métrica", y="Provincia", color="Porcentaje"),
        title="Indicadores de Contactabilidad",
        aspect="auto"
    )
    st.plotly_chart(fig, use_container_width=True)
    
try:
    r.ping()
except redis.exceptions.ConnectionError:
    st.error("❌ No se puede conectar a Redis. Verifica que el servicio está corriendo.")
    
# Mostrar últimos registros scrapeados
st.markdown("---")
st.markdown("## 📋 Vista Previa de Datos Scrapeados")

try:
    # Obtener últimos 10 registros de Redis
    latest_companies = r.keys("empresa:*")[-10:]  # Últimas 10 empresas
    preview_data = []

    for company_key in latest_companies:
        company_data = r.hgetall(company_key)
        preview_data.append({
            "Nombre": company_data.get(b'nombre', b'').decode('utf-8'),
            "Provincia": company_data.get(b'provincia', b'').decode('utf-8'),
            "Procesado": company_data.get(b'processed', b'No').decode('utf-8')
        })

    if preview_data:
        df_preview = pd.DataFrame(preview_data)
        st.dataframe(
            df_preview,
            column_config={
                "Nombre": "Empresa",
                "Provincia": "Ubicación",
                "Procesado": st.column_config.CheckboxColumn("Procesado")
            },
            hide_index=True,
            use_container_width=True
        )
    else:
        st.info("🗃️ Aún no hay datos scrapeados. Ejecuta el scraping para ver información.")

except Exception as e:
    st.error(f"Error cargando vista previa: {str(e)}")