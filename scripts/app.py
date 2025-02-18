# app.py

import streamlit as st
import pandas as pd
from datetime import datetime
import time
from agents import OrchestratorAgent, DBAgent, ScrapingAgent
from database import DatabaseManager
from scraping import ProWebScraper
from config import REQUIRED_COLUMNS, PROVINCIAS_ESPANA
from fastapi import FastAPI
from pydantic import BaseModel
from agents import OrchestratorAgent, DBAgent, ScrapingAgent 
import matplotlib

app = FastAPI()
orchestrator = OrchestratorAgent()
db_agent = DBAgent()
scraping_agent = ScrapingAgent()

class UserRequest(BaseModel):
    query: str

@app.post("/process")
def process_request(request: UserRequest):
    """
    Recibe una orden desde el frontend y activa el agente correcto.
    """
    result = orchestrator.process(request.query)

    if not result["valid"]:
        return {"error": "Consulta no válida para empresas en España."}

    return {"response": result["response"], "context": result.get("context", {})}

@app.post("/db-query")
def generate_sql(request: UserRequest):
    """
    Genera una consulta SQL basada en lenguaje natural.
    """
    result = db_agent.generate_query(request.query)
    return result

@app.post("/scrape")
def scrape_website(request: UserRequest):
    """
    Planifica el scraping de una URL.
    """
    result = scraping_agent.plan_scraping(request.query)
    return result
import streamlit as st
import pandas as pd
from datetime import datetime
import time
import requests
import logging
from agents import OrchestratorAgent, DBAgent, ScrapingAgent
from database import DatabaseManager
from scraping import ProWebScraper
from config import REQUIRED_COLUMNS, PROVINCIAS_ESPANA, OLLAMA_ENDPOINT

# Configuración del logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('app.log'),
        logging.StreamHandler()
    ]
)

class EnterpriseApp:
    def __init__(self):
        self.init_session_state()
        self.check_llm_status(optional=True)  # Hacemos la verificación opcional
        self.db = DatabaseManager()
        self.scraper = ProWebScraper()
        self.setup_agents()
        self.load_data_from_db()
        
        st.set_page_config(
            page_title="Sistema Empresarial de Análisis",
            page_icon="🏢",
            layout="wide",
            initial_sidebar_state="expanded"
        )

    def check_llm_status(self, optional=True):
        """
        Verifica que el modelo Mistral esté funcionando correctamente
        
        Args:
            optional (bool): Si es True, no lanzará una excepción si falla
        """
        try:
            # Intenta hacer una petición simple a Ollama con timeout más largo
            response = requests.post(
                OLLAMA_ENDPOINT,
                json={
                    "model": "mistral",
                    "prompt": "test",
                    "stream": False
                },
                timeout=15  # Aumentamos el timeout a 15 segundos
            )
            
            if response.status_code == 200:
                logging.info("✅ Mistral LLM está funcionando correctamente")
                st.session_state.llm_status = "operational"
            else:
                error_msg = f"❌ Error al conectar con Mistral LLM: Status code {response.status_code}"
                logging.error(error_msg)
                st.session_state.llm_status = "error"
                if not optional:
                    raise Exception(error_msg)
                
        except requests.exceptions.Timeout:
            error_msg = "❌ Timeout al conectar con Ollama. El servicio está tardando demasiado en responder."
            logging.error(error_msg)
            st.session_state.llm_status = "timeout"
            if not optional:
                raise Exception(error_msg)
                
        except requests.exceptions.ConnectionError:
            error_msg = "❌ No se pudo conectar con Ollama. ¿Está el servicio en ejecución?"
            logging.error(error_msg)
            st.session_state.llm_status = "connection_error"
            if not optional:
                raise Exception(error_msg)
            
        except Exception as e:
            error_msg = f"❌ Error inesperado al verificar Mistral LLM: {str(e)}"
            logging.error(error_msg)
            st.session_state.llm_status = "unknown_error"
            if not optional:
                raise Exception(error_msg)

    def render_sidebar(self):
        """Renderiza la barra lateral con opciones de carga y filtros"""
        with st.sidebar:
            # Mostrar estado del LLM y botón para reintento
            st.subheader("🤖 Estado del LLM")
            
            llm_status = st.session_state.get("llm_status", "unknown")
            
            if llm_status == "operational":
                st.success("✅ LLM Operativo")
            elif llm_status == "timeout":
                st.error("❌ Timeout en LLM")
                if st.button("Reintentar conexión"):
                    self.check_llm_status(optional=True)
            elif llm_status == "connection_error":
                st.error("❌ No se puede conectar con Ollama")
                if st.button("Reintentar conexión"):
                    self.check_llm_status(optional=True)
            elif llm_status == "error":
                st.error("❌ Error en LLM")
                if st.button("Reintentar conexión"):
                    self.check_llm_status(optional=True)
            else:
                st.error("❌ Estado desconocido del LLM")
                if st.button("Reintentar conexión"):
                    self.check_llm_status(optional=True)

    def init_session_state(self):
        """Inicializa variables de estado"""
        if "current_batch" not in st.session_state:
            st.session_state.current_batch = None
        if "processing_status" not in st.session_state:
            st.session_state.processing_status = None
        if "last_query" not in st.session_state:
            st.session_state.last_query = None
        if "show_sql" not in st.session_state:
            st.session_state.show_sql = False
            
    def load_data_from_db(self):
        """Si no hay datos en sesión, se cargan desde la BD"""
        if st.session_state.current_batch is None:
            df = self.db.execute_query("SELECT * FROM sociedades", return_df=True)
            if df is not None and not df.empty:
                # Normalizar nombres de columnas a minúsculas
                df.columns = df.columns.str.strip().str.lower()
                st.session_state.current_batch = {
                    "id": "loaded_from_db",
                    "data": df,
                    "total_records": len(df),
                    "timestamp": datetime.now()
                }

    def setup_agents(self):
        """Configuración de agentes inteligentes"""
        self.orchestrator = OrchestratorAgent()
        self.db_agent = DBAgent()
        self.scraping_agent = ScrapingAgent()

    def render_sidebar(self):
        """Renderiza la barra lateral con opciones de carga y filtros"""
        with st.sidebar:
            # st.image("logo.png", width=200)  # Asegúrate de tener el logo en tu directorio
            st.title("Control Panel")
            
            # Sección de carga de archivos
            st.header("📤 Carga de Datos")
            uploaded_file = st.file_uploader(
                "Seleccionar archivo (CSV/XLSX)",
                type=["csv", "xlsx"],
                help="Formatos soportados: CSV, Excel"
            )
            
            if uploaded_file:
                self.handle_file_upload(uploaded_file)
            
            # Filtros
            if st.session_state.current_batch:
                st.header("🔍 Filtros")
                selected_provincia = st.selectbox(
                    "Provincia",
                    ["Todas"] + PROVINCIAS_ESPANA
                )
                
                has_web = st.checkbox("Solo con web", value=False)
                has_ecommerce = st.checkbox("Solo con e-commerce", value=False)
                
                if st.button("Aplicar Filtros"):
                    self.apply_filters(selected_provincia, has_web, has_ecommerce)
                st.subheader("🤖 Estado del LLM")
                            
                llm_status = st.session_state.get("llm_status", "unknown")
                            
                if llm_status == "operational":
                    st.success("✅ LLM Operativo")
                elif llm_status == "timeout":
                    st.error("❌ Timeout en LLM")
                    if st.button("Reintentar conexión"):
                        self.check_llm_status(optional=True)
                elif llm_status == "connection_error":
                    st.error("❌ No se puede conectar con Ollama")
                    if st.button("Reintentar conexión"):
                                self.check_llm_status(optional=True)
                elif llm_status == "error":
                    st.error("❌ Error en LLM")
                    if st.button("Reintentar conexión"):
                                    self.check_llm_status(optional=True)
                    else:
                        st.error("❌ Estado desconocido del LLM")
                        if st.button("Reintentar conexión"):
                            self.check_llm_status(optional=True)
                    
    def render_main_content(self):
        """Renderiza el contenido principal"""
        st.title("Sistema de Análisis Empresarial 🏢")
        
        # Tabs principales
        tab1, tab2, tab3, tab4 = st.tabs([
            "📊 Dashboard",
            "🔍 Consultas",
            "🌐 Web Scraping",
            "📈 Análisis"
        ])
        
        with tab1:
            self.render_dashboard()
            
        with tab2:
            self.render_queries()
            
        with tab3:
            self.render_scraping()
            
        with tab4:
            self.render_analysis()

    def handle_file_upload(self, file):
        """Procesa la carga de archivos"""
        try:
            # Mostrar spinner durante la carga
            with st.spinner("Procesando archivo..."):
                # Leer archivo
                if file.name.endswith('.csv'):
                    df = pd.read_csv(file, header=0, sep=';', encoding='utf-8')
                else:
                    df = pd.read_excel(file, header=0)
                    
                # Mostrar columnas detectadas para depuración
                st.write("Columnas detectadas:", df.columns.tolist())
                
                # Validar columnas
                missing_cols = [col for col in REQUIRED_COLUMNS if col not in df.columns]
                if missing_cols:
                    st.error(f"Faltan columnas requeridas: {', '.join(missing_cols)}")
                    return
                
                # Normalizar nombres de columnas
                df.columns = [col.strip().lower() for col in df.columns]
                
                # Generar ID de lote
                batch_id = f"BATCH_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                
                # Guardar en base de datos
                result = self.db.save_batch(df, batch_id, st.session_state.get("user", "streamlit_user"))
                
                st.write("Resultado de save_batch:", result)
                
                if result["status"] == "success":
                    st.session_state.current_batch = {
                        "id": batch_id,
                        "data": df,
                        "total_records": len(df),
                        "timestamp": datetime.now()
                    }
                    st.success(f"✅ Archivo procesado exitosamente: {result['inserted']} registros")
                else:
                    st.error(f"❌ Error al procesar archivo: {result['message']}")
                
        except Exception as e:
            st.error(f"❌ Error al procesar archivo: {str(e)}")

    def render_dashboard(self):
        """Renderiza el dashboard con estadísticas"""
        if not st.session_state.current_batch:
            st.info("👆 Carga un archivo para ver las estadísticas")
            return

        df = st.session_state.current_batch["data"]
        df.columns = df.columns.str.strip().str.lower()
        
        # Estadísticas generales
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Registros", f"{st.session_state.current_batch['total_records']:,}")
        
        total_with_web = len(st.session_state.current_batch['data'][
            st.session_state.current_batch['data']['url'].notna()
        ])
        with col2:
            st.metric("Con Web", f"{total_with_web:,}")
        
        unique_provinces = st.session_state.current_batch['data']['nom_provincia'].nunique()
        with col3:
            st.metric("Provincias", unique_provinces)
        
        with col4:
            st.metric("Lote", st.session_state.current_batch['id'])
        
        # Gráficos
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Distribución por Provincia")
            prov_counts = st.session_state.current_batch['data']['nom_provincia'].value_counts()
            st.bar_chart(prov_counts)
            
        with col2:
            st.subheader("Estado de URLs")
            
            # Evaluamos si cada URL es realmente válida: debe ser una cadena y no estar vacía
            valid_url = st.session_state.current_batch['data']['url'].apply(
                lambda x: isinstance(x, str) and x.strip() != '' and 
                        (x.strip().lower().startswith("http://") or 
                        x.strip().lower().startswith("https://") or 
                        x.strip().lower().startswith("www."))
            )
            url_status = valid_url.value_counts()
            
            # Generamos las etiquetas de forma dinámica
            labels = ["Con URL" if val is True else "Sin URL" for val in url_status.index]
            sizes = url_status.values
            default_colors = ['#66b3ff', '#ff9999']
            colors = default_colors[:len(sizes)]
            
            # Crear el gráfico de pastel usando matplotlib
            import matplotlib.pyplot as plt
            fig, ax = plt.subplots()
            ax.pie(sizes, labels=labels, colors=colors, autopct='%1.1f%%', startangle=90)
            ax.axis('equal')  # Mantiene el gráfico circular
            st.pyplot(fig)

    def render_queries(self):
        """Renderiza la sección de consultas"""
        st.subheader("🔍 Consultas Avanzadas")
        
        # Input de consulta
        query = st.text_area(
            "Escribe tu consulta en lenguaje natural",
            placeholder="Ejemplo: Mostrar empresas de Madrid con e-commerce",
            help="Puedes preguntar sobre cualquier aspecto de los datos"
        )
        
        col1, col2 = st.columns([1, 4])
        with col1:
            if st.button("Ejecutar"):
                self.process_query(query)
        with col2:
            st.checkbox("Mostrar SQL", value=False, key="show_sql")
        
        # Mostrar resultados
        if st.session_state.last_query:
            with st.expander("📝 Última consulta", expanded=True):
                if st.session_state.show_sql and "sql" in st.session_state.last_query:
                    st.code(st.session_state.last_query["sql"], language="sql")
                if "results" in st.session_state.last_query:
                    st.dataframe(st.session_state.last_query["results"], use_container_width=True)
                if "scraping_plan" in st.session_state.last_query:
                    st.json(st.session_state.last_query["scraping_plan"])

    def render_scraping(self):
        """Renderiza la sección de web scraping"""
        st.subheader("🌐 Web Scraping")
        
        # Intentar cargar datos si no hay nada
        if st.session_state.current_batch is None:
            self.load_data_from_db()
        
        if not st.session_state.current_batch:
            st.warning("⚠️ Primero debes cargar un archivo con URLs")
            return
        
        col1, col2 = st.columns(2)
        
        with col1:
            limit = st.number_input(
                "Límite de URLs a procesar",
                min_value=1,
                max_value=1000,
                value=50
            )
            
        with col2:
            if st.button("Iniciar Scraping"):
                self.process_scraping(limit)
                
        # Mostrar progreso si está procesando
        if st.session_state.processing_status:
            st.progress(st.session_state.processing_status["progress"])
            st.write(f"Procesando: {st.session_state.processing_status['current_url']}")

    def render_analysis(self):
        """Renderiza la sección de análisis"""
        st.subheader("📈 Análisis de Datos")
        
        if not st.session_state.current_batch:
            st.warning("⚠️ Carga datos para realizar análisis")
            return
        
        # Opciones de análisis
        analysis_type = st.selectbox(
            "Tipo de Análisis",
            [
                "Distribución Geográfica",
                "Análisis de E-commerce",
                "Presencia Digital",
                "Contactabilidad"
            ]
        )
        
        if st.button("Generar Análisis"):
            self.generate_analysis(analysis_type)


    def process_query(self, query: str):
        """Procesa consultas en lenguaje natural usando el orquestador"""
        try:
            with st.spinner("Procesando consulta..."):
                orchestrated_response = self.orchestrator.process(query)
                
                if not orchestrated_response["valid"]:
                    st.error(orchestrated_response["response"])
                    return
                
                # Se interpreta la respuesta para decidir qué agente usar
                actions = orchestrated_response["response"].lower()
                
                if "sql" in actions:
                    query_info = self.db_agent.generate_query(query)
                    results = self.db.execute_query(query_info["query"], return_df=True)
                    st.session_state.last_query = {
                        "sql": query_info["query"],
                        "results": results
                    }
                elif "scraping" in actions:
                    scraping_plan = self.scraping_agent.plan_scraping(query)
                    st.session_state.last_query = {
                        "scraping_plan": scraping_plan,
                        "message": "Plan de scraping generado."
                    }
                else:
                    st.info("La consulta no requiere acción (SQL o scraping).")
                
        except Exception as e:
            st.error(f"Error al procesar consulta: {str(e)}")

    def process_scraping(self, limit: int):
        """Procesa el scraping de URLs"""
        try:
            urls_df = self.db.get_urls_for_scraping(
                batch_id=st.session_state.current_batch["id"],
                limit=limit
            )
            
            if urls_df.empty:
                st.warning("No hay URLs pendientes de procesar")
                return
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            total_urls = len(urls_df)
            results = []
            
            for idx, row in urls_df.iterrows():
                # Actualizar progreso
                progress = (idx + 1) / total_urls
                progress_bar.progress(progress)
                status_text.text(f"Procesando URL {idx + 1}/{total_urls}: {row['url']}")
                
                # Realizar scraping
                result = self.scraper.scrape_url(row['url'], {
                    'cod_infotel': row['cod_infotel']
                })
                results.append(result)
                
                # Breve pausa
                time.sleep(0.5)
            
            # Actualizar resultados en base de datos
            self.db.update_scraping_results(
                results,
                st.session_state.current_batch["id"]
            )
            
            st.success(f"✅ Scraping completado: {len(results)} URLs procesadas")
            
        except Exception as e:
            st.error(f"❌ Error durante el scraping: {str(e)}")

    def generate_analysis(self, analysis_type: str):
        """Genera análisis específicos"""
        try:
            if analysis_type == "Distribución Geográfica":
                self.show_geographic_analysis()
            elif analysis_type == "Análisis de E-commerce":
                self.show_ecommerce_analysis()
            elif analysis_type == "Presencia Digital":
                self.show_digital_presence_analysis()
            else:
                self.show_contactability_analysis()
                
        except Exception as e:
            st.error(f"Error generando análisis: {str(e)}")

    def apply_filters(self, provincia: str, has_web: bool, has_ecommerce: bool):
        """Aplica filtros a los datos actuales"""
        try:
            df = st.session_state.current_batch['data'].copy()
            
            if provincia != "Todas":
                df = df[df['nom_provincia'] == provincia]
                
            if has_web:
                df = df[df['url'].notna()]
                
            if has_ecommerce:
                df = df[df['e_commerce'] == True]
                
            st.session_state.current_batch['filtered_data'] = df
            st.success("Filtros aplicados correctamente")
            
        except Exception as e:
            st.error(f"Error aplicando filtros: {str(e)}")

    # Métodos placeholder para análisis (puedes personalizarlos)
    def show_geographic_analysis(self):
        st.write("### Análisis Geográfico")
        df = st.session_state.current_batch['data']
        prov_counts = df['nom_provincia'].value_counts()
        st.bar_chart(prov_counts)

    def show_ecommerce_analysis(self):
        st.write("### Análisis de E-commerce")
        df = st.session_state.current_batch['data']
        if 'e_commerce' in df.columns:
            ecommerce_counts = df['e_commerce'].value_counts()
            st.bar_chart(ecommerce_counts)
        else:
            st.info("No hay datos de e-commerce disponibles.")

    def show_digital_presence_analysis(self):
        st.write("### Presencia Digital")
        df = st.session_state.current_batch['data']
        presence = df['url'].notna().value_counts()
        st.bar_chart(presence)

    def show_contactability_analysis(self):
        st.write("### Análisis de Contactabilidad")
        df = st.session_state.current_batch['data']
        if 'nif' in df.columns:
            count_nif = df['nif'].notna().sum()
            st.write(f"Empresas con NIF: {count_nif}")
        else:
            st.info("No hay datos de contacto disponibles.")

    def run(self):
        """Ejecuta la aplicación"""
        self.render_sidebar()
        self.render_main_content()

if __name__ == "__main__":
    app = EnterpriseApp()
    app.run()