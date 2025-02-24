# app.py

import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
import time
from agents import DBAgent, ScrapingAgent  # Removed OrchestratorAgent
from database import DatabaseManager
from scraping import ProWebScraper
from config import REQUIRED_COLUMNS, PROVINCIAS_ESPANA, SQL_MODELS, SCRAPING_MODELS
from agents import CustomLLM
import matplotlib.pyplot as plt
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
import unicodedata
from langchain.callbacks import StreamlitCallbackHandler
from config import LANGSMITH_API_KEY, LANGSMITH_PROJECT
import os

class EnterpriseApp:
    def __init__(self):
        self.init_session_state()
        self.db = DatabaseManager()
        self.scraper = ProWebScraper()
        self.setup_agents()
        self.load_data_from_db()
        # Cargar datos de la BD si no hay nada en session_state
        self.load_data_from_db()
        
        # Initialize StreamlitCallbackHandler for real-time updates
        self.st_callback = StreamlitCallbackHandler(st.container())
        
        # Add LangSmith metrics to sidebar
        with st.sidebar:
            if st.checkbox("Show LangSmith Metrics"):
                self.render_langsmith_metrics()
            
        # Enhanced page configuration
        st.set_page_config(
            page_title="Sistema Empresarial de Análisis",
            page_icon="🏢",
            layout="wide",
            initial_sidebar_state="expanded"
        )
        
        # Add custom CSS with adjusted sidebar spacing
        st.markdown("""
            <style>
                /* Main container styling */
                .main {
                    padding: 2rem;
                }
                
                /* Sidebar styling with reduced spacing */
                .css-1d391kg {
                    padding: 1rem 0.5rem;
                }
                
                /* Reduce spacing between sidebar elements */
                .sidebar .element-container {
                    margin-bottom: 0.5rem !important;
                }
                
                /* Sidebar headers with less margin */
                .sidebar h1, .sidebar h2, .sidebar h3 {
                    margin-bottom: 0.5rem !important;
                    margin-top: 0.5rem !important;
                }
                
                /* Card-like containers */
                .stMetric {
                    background-color: #ffffff;
                    border-radius: 0.5rem;
                    padding: 1rem;
                    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                }
                
                /* Tab styling */
                .stTabs [data-baseweb="tab-list"] {
                    gap: 2rem;
                    margin-bottom: 2rem;
                }
                
                .stTabs [data-baseweb="tab"] {
                    background-color: #f8f9fa;
                    border-radius: 0.5rem;
                    padding: 0.5rem 2rem;
                    font-weight: 500;
                }
                
                .stTabs [data-baseweb="tab"][aria-selected="true"] {
                    background-color: #1f77b4;
                    color: white;
                }
                
                /* Button styling */
                .stButton > button {
                    width: 100%;
                    border-radius: 0.5rem;
                    padding: 0.5rem 1rem;
                    background-color: #1f77b4;
                    color: white;
                }
                
                .stButton > button:hover {
                    background-color: #155987;
                }
                
                /* Reduce spacing in selectbox */
                .stSelectbox {
                    margin-bottom: 0.5rem !important;
                }
                
                                /* Change tab underline color to purple (#642678) */
                .stTabs [data-baseweb="tab-highlight"] {
                    background-color: #642678 !important;
                }
                
                /* Also update the active tab background to match */
                .stTabs [data-baseweb="tab"][aria-selected="true"] {
                    background-color: #642678;
                    color: white;
                }
            </style>
        """, unsafe_allow_html=True)

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
        # Initialize with default models from config
        if "sql_model" not in st.session_state:
            st.session_state.sql_model = list(SQL_MODELS.keys())[0]
        if "scraping_model" not in st.session_state:
            st.session_state.scraping_model = list(SCRAPING_MODELS.keys())[0]
            
    def render_langsmith_metrics(self):
        """Renders LangSmith metrics in the sidebar"""
        try:
            client = langsmith.Client()
            runs = client.list_runs(
                project_name=LANGSMITH_PROJECT,
                execution_order=1,
                limit=100
            )
            
            st.markdown("### 📊 LangSmith Metrics")
            
            # Calculate and display metrics
            total_runs = len(runs)
            successful_runs = sum(1 for run in runs if run.error is None)
            avg_latency = sum(run.end_time - run.start_time for run in runs) / total_runs if total_runs > 0 else 0
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Runs", total_runs)
            with col2:
                st.metric("Success Rate", f"{(successful_runs/total_runs)*100:.1f}%" if total_runs > 0 else "N/A")
            with col3:
                st.metric("Avg Latency", f"{avg_latency:.2f}s")
            
            # Show recent runs
            with st.expander("Recent Runs"):
                for run in list(runs)[:5]:
                    st.write(f"Type: {run.run_type}")
                    st.write(f"Status: {'✅ Success' if run.error is None else '❌ Failed'}")
                    st.write("---")
                    
        except Exception as e:
            st.error(f"Error fetching LangSmith metrics: {str(e)}")
            
    def load_data_from_db(self):
        """Si no hay datos en sesión, se cargan desde la BD"""
        if st.session_state.current_batch is None:
            df = self.db.execute_query("SELECT * FROM sociedades", return_df=True)
            if df is not None and not df.empty:
                # Normalizar nombres de columnas a minúsculas
                df.columns = df.columns.str.strip().str.lower()
                
                # Asegurar limpieza de espacios en blanco
                df['nom_provincia'] = df['nom_provincia'].astype(str).str.strip()
                
                # Check for duplicates by a unique identifier (e.g., NIF or cod_infotel)
                if 'cod_infotel' in df.columns:
                    df = df.drop_duplicates(subset=['cod_infotel'], keep='first')
                st.session_state.current_batch = {
                    "id": "loaded_from_db",
                    "data": df,
                    "total_records": len(df),
                    "timestamp": datetime.now()
                }

    def setup_agents(self):
        """Configuración de agentes inteligentes teniendo en cuenta el modelo seleccionado"""
        try:
            # Crear nuevas instancias de agentes con los modelos seleccionados
            self.db_agent = DBAgent()
            modelo_sql = SQL_MODELS.get(st.session_state.sql_model, st.session_state.sql_model)
            self.db_agent.llm = CustomLLM(modelo_sql, provider="groq")
            
            self.scraping_agent = ScrapingAgent()
            modelo_scraping = SCRAPING_MODELS.get(st.session_state.scraping_model, st.session_state.scraping_model)
            self.scraping_agent.llm = CustomLLM(modelo_scraping, provider="groq")
        except Exception as e:
            st.error(f"Error al configurar agentes: {str(e)}")

    def render_sidebar(self):
        """Renderiza la barra lateral con opciones de carga y filtros"""
        with st.sidebar:
            st.image("images/logo.png", width=200)
            
            # Model Selection Section
            st.subheader("🤖 Configuración de Modelos")
            
            # Diccionario de modelos SQL
            sql_models = SQL_MODELS
            
            selected_sql_model = st.selectbox(
                "Modelo para Consultas SQL",
                list(sql_models.keys()),
                index=0,
                help="Selecciona el modelo Groq para consultas SQL"
            )
            
            # Scraping model selection
            scraping_models = SCRAPING_MODELS
            
            selected_scraping_model = st.selectbox(
                "Modelo para Web Scraping",
                list(scraping_models.keys()),
                index=0,
                help="Selecciona el modelo para análisis de web scraping"
            )
            
            # Update models if changed
            if selected_sql_model != st.session_state.sql_model:
                st.session_state.sql_model = selected_sql_model
                self.setup_agents()
                
            if selected_scraping_model != st.session_state.scraping_model:
                st.session_state.scraping_model = selected_scraping_model
                self.setup_agents()
            
            # File Upload Section
            st.subheader("📤 Carga de Datos")
            uploaded_file = st.file_uploader(
                "Seleccionar archivo (CSV/XLSX)",
                type=["csv", "xlsx"],
                help="Formatos soportados: CSV, Excel"
            )
            
            if uploaded_file:
                self.handle_file_upload(uploaded_file)
            
            # Filters Section
            if st.session_state.current_batch:
                st.subheader("🔍 Filtros")
                selected_provincia = st.selectbox(
                    "Provincia",
                    ["Todas"] + PROVINCIAS_ESPANA
                )
                
                has_web = st.checkbox("Solo con web", value=False)
                has_ecommerce = st.checkbox("Solo con e-commerce", value=False)
                
                if st.button("Aplicar Filtros"):
                    self.apply_filters(selected_provincia, has_web, has_ecommerce)
            
            # Database Reset Button
            # Database Reset Button
            if st.button("Borrar BBDD", help="Reinicia la base de datos"):
                self.db.reset_database()
                st.session_state.current_batch = None
                st.success("Base de datos reiniciada exitosamente.")
                st.rerun()

    def render_main_content(self):
        """Renderiza el contenido principal con UI mejorada"""
        st.title("Sistema de Análisis Empresarial 🏢")
        
        # Enhanced tabs with custom styling and icons
        tabs = st.tabs([
            "📊  DASHBOARD  ",
            "🔍  CONSULTAS  ",
            "🌐  WEB SCRAPING  ",
            "📈  ANÁLISIS  "
        ])
        
        with tabs[0]:
            self.render_dashboard()
        with tabs[1]:
            self.render_queries()
        with tabs[2]:
            self.render_scraping()
        with tabs[3]:
            self.render_analysis()

    def handle_file_upload(self, file):
        """Procesa la carga de archivos y actualiza la BD, ignorando duplicados silenciosamente"""
        try:
            with st.spinner("Procesando archivo..."):
                # Leer archivo
                if file.name.endswith('.csv'):
                    df = pd.read_csv(file, header=0, sep=';', encoding='utf-8')
                else:
                    df = pd.read_excel(file, header=0)
                    
                # Validar columnas
                missing_cols = [col for col in REQUIRED_COLUMNS if col not in df.columns]
                if missing_cols:
                    st.error(f"Faltan columnas requeridas: {', '.join(missing_cols)}")
                    return
                
                # Normalizar nombres de columnas
                df.columns = [col.strip().lower() for col in df.columns]

                # Obtener registros existentes para comparación
                existing_records = self.db.execute_query(
                    "SELECT cod_infotel FROM sociedades WHERE deleted = FALSE",
                    return_df=True
                )
                
                if existing_records is not None and not existing_records.empty:
                    existing_codes = set(existing_records['cod_infotel'].values)
                    
                    # Filtrar solo los registros que no existen en la BD
                    df = df[~df['cod_infotel'].isin(existing_codes)]
                    
                    if len(df) == 0:
                        st.info("No hay nuevos registros para procesar. Todos los registros ya existen en la base de datos.")
                        return

                # Limpiar datos antes de guardar
                df = df.replace(r'^\s*$', None, regex=True)
                df = df.replace({np.nan: None})
                
                # Guardar en base de datos
                result = self.db.save_batch(df, check_duplicates=True)
                
                if result["status"] == "success":
                    st.success(f"✅ Archivo procesado exitosamente: {result['inserted']} nuevos registros añadidos")
                    # Recargar datos de la BD para actualizar el dashboard
                    self.load_data_from_db()
                elif result["status"] == "partial":
                    st.warning(
                        f"⚠️ Procesamiento parcial: {result['inserted']}/{result['total']} registros. "
                        f"Errores: {'; '.join(result['errors'])}"
                    )
                else:
                    st.error(f"❌ Error al procesar archivo: {result['message']}\nDetalles: {result['errors']}")
                    
        except Exception as e:
            import traceback
            error_details = traceback.format_exc()
            st.error(f"❌ Error al procesar archivo: {str(e)}\n\nDetalles:\n{error_details}")

    def render_dashboard(self):
        """Renderiza el dashboard con datos de la BD"""
        # Obtener datos actualizados de la BD
        df = self.db.execute_query("SELECT * FROM sociedades WHERE deleted = FALSE", return_df=True)
        
        if df is None or df.empty:
            st.info("👆 No hay datos en la base de datos. Carga un archivo para ver las estadísticas")
            return

        # Normalizar nombres de columnas
        df.columns = df.columns.str.strip().str.lower()
        
        # Enhanced metrics display
        st.markdown("### 📊 Estadísticas Generales")
        metrics_container = st.container()
        col1, col2, col3, col4 = metrics_container.columns(4)
        
        with col1:
            st.metric(
                "Total Registros",
                f"{len(df):,}",
                delta=None,
            )
        
        # Contar URLs válidas (existentes y accesibles)
        total_with_web = df['url_exists'].sum() if 'url_exists' in df.columns else 0
        with col2:
            st.metric(
                "Con Web Activa",
                f"{total_with_web:,}",
                delta=f"{(total_with_web/len(df)*100):.1f}%" if len(df) > 0 else None
            )
        
        unique_provinces = df['nom_provincia'].nunique()
        with col3:
            st.metric(
                "Provincias",
                unique_provinces,
                delta=None
            )
        
        # Obtener la fecha de última actualización de la BD
        last_update = df['fecha_actualizacion'].max() if 'fecha_actualizacion' in df.columns else None
        with col4:
            st.metric(
                "Última actualización",
                last_update.strftime("%d-%m-%Y") if last_update else "No disponible"
            )
        
        # Enhanced charts section
        st.markdown("### 📈 Visualización de Datos")
        chart_col1, chart_col2 = st.columns(2)
        
        with chart_col1:
            st.markdown("#### Distribución por Provincia")
            prov_counts = df['nom_provincia'].value_counts().head(10)
            fig, ax = plt.subplots(figsize=(10, 6))
            bars = ax.bar(prov_counts.index, prov_counts.values)
            plt.xticks(rotation=45, ha='right')
            plt.title("Top 10 Provincias")
            st.pyplot(fig)
            
        with chart_col2:
            st.markdown("#### Estado de URLs")
            self.render_url_status_chart(df)

    def render_queries(self):
        """Renderiza la sección de consultas (SQL)"""
        st.subheader("🔍 Consultas Avanzadas (SQL)")

        query = st.text_area(
            "Escribe tu consulta en lenguaje natural",
            placeholder="Ejemplo: Dame las 10 primeras empresas de Madrid",
            help="Se traducirá a una consulta SQL"
        )

        col1, col2 = st.columns([1, 4])
        with col1:
            execute_button = st.button("Ejecutar Consulta")
        with col2:
            st.checkbox("Mostrar SQL", value=False, key="show_sql")
        
        if execute_button and query:
            self.process_query(query)
            
        # Show results only if we have a last query
        last_query = st.session_state.get("last_query", None)
        if last_query and "results" in last_query:
            if st.session_state.show_sql and "sql" in last_query:
                st.code(last_query["sql"], language="sql")
                
        # Show explanation if available
        if last_query and "explanation" in last_query and last_query["explanation"]:
            with st.expander("Explicación LLM"):
                st.write(last_query["explanation"])

    def render_scraping(self):
        """Renderiza la sección de web scraping"""
        st.subheader("🌐 Web Scraping")
        
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
                
        if st.session_state.processing_status:
            st.progress(st.session_state.processing_status["progress"])
            st.write(f"Procesando: {st.session_state.processing_status['current_url']}")

    def render_analysis(self):
        """Renderiza la sección de análisis"""
        st.subheader("📈 Análisis de Datos")
        
        if not st.session_state.current_batch:
            st.warning("⚠️ Carga datos para realizar análisis")
            return
        
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
            
    def remove_accents(self, text):
        """Elimina acentos de una cadena de texto."""
        return ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')

    def is_count_query(self, query):
        """Detecta si la consulta del usuario es una consulta de conteo."""
        query_normalized = self.remove_accents(query.lower())

        # Patrón para detectar frases relacionadas con conteo
        count_patterns = [
            r"\bcuantas\b", 
            r"\bcuantos\b", 
            r"\bnumero de\b", 
            r"\btotal de\b"
        ]
        
        return any(re.search(pattern, query_normalized) for pattern in count_patterns)

    def process_query(self, query: str):
        try:
            with st.spinner("Procesando consulta..."):
                # Generate query
                query_info = self.db_agent.generate_query(query)
                
                # Check for errors
                if query_info.get("error"):
                    st.error(query_info["error"])
                    return
                    
                # Execute query
                results = self.db.execute_query(query_info["query"], return_df=True)
                
                # Handle different query types
                if query_info["query_type"] == "count":
                    value = results.iloc[0, 0]
                    st.metric("Total", f"{value:,}")
                elif query_info["query_type"] == "aggregate":
                    st.dataframe(results)
                    # Add visualization if needed
                else:
                    st.dataframe(results)
                
                # Show SQL if requested
                if st.session_state.show_sql:
                    st.code(query_info["query"], language="sql")
                    
                # Show explanation
                if query_info["explanation"]:
                    with st.expander("Explicación"):
                        st.write(query_info["explanation"])
                        
        except Exception as e:
            st.error(f"Error al procesar consulta: {str(e)}")

    def process_scraping(self, limit: int):
        """Procesa el scraping de URLs utilizando procesamiento paralelo y muestra detalles."""
        try:
            urls_df = self.db.get_urls_for_scraping(limit=limit)
            if urls_df.empty:
                st.warning("No hay URLs pendientes de procesar")
                return

            total_urls = len(urls_df)
            progress_bar = st.progress(0)
            status_text = st.empty()
            results = []

            # Registrar tiempo de inicio
            start_time = time.perf_counter()

            def scrape_row(idx, row):
                url_display = row['url'] if pd.notna(row['url']) else "URL no disponible"
                result = self.scraping_agent.plan_scraping(row['url'])
                # Añadir más información relevante al resultado
                result.update({
                    'cod_infotel': row['cod_infotel'],
                    'original_url': row['url'],
                    'phones_found': len(result.get('phones', [])),
                    'social_media_found': len(result.get('social_media', {})),
                    'has_ecommerce': result.get('is_ecommerce', False)
                })
                return idx, result

            with ThreadPoolExecutor(max_workers=8) as executor:
                futures = {executor.submit(scrape_row, idx, row): idx for idx, row in urls_df.iterrows()}
                completed = 0
                for future in as_completed(futures):
                    idx, result = future.result()
                    results.append(result)
                    completed += 1
                    progress_bar.progress(completed / total_urls)
                    status_text.text(f"Procesando URL {completed}/{total_urls}")
            
            end_time = time.perf_counter()
            elapsed = end_time - start_time

            # Mostrar resultados en formato más útil
            st.subheader("Resultados del Web Scraping")
            
            # Métricas resumidas
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("URLs Procesadas", len(results))
            with col2:
                st.metric("Tiempo Total", f"{elapsed:.2f}s")
            with col3:
                st.metric("URLs Válidas", sum(1 for r in results if r.get('url_exists', False)))
            with col4:
                st.metric("Con E-commerce", sum(1 for r in results if r.get('has_ecommerce', False)))

            # Tabla de resultados detallados
            results_df = pd.DataFrame(results)
            st.dataframe(
                results_df[[
                    'cod_infotel', 'original_url', 'url_exists', 
                    'phones_found', 'social_media_found', 'has_ecommerce'
                ]],
                use_container_width=True,
            )

            # Preguntar al usuario si desea aplicar los cambios
            if st.checkbox("Actualizar la base de datos con los nuevos datos"):
                update_result = self.db.update_scraping_results(results=results)
                st.success(f"✅ Scraping completado: {len(results)} URLs procesadas y BD actualizada.")
            else:
                st.info("No se actualizaron los registros en la BD.")
                
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

    # Métodos placeholder para análisis
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
            
    def render_url_status_chart(self, df):
        """Renderiza el gráfico de estado de URLs usando datos de la BD"""
        # Usar el campo url_exists de la BD
        count_valid = df['url_exists'].sum() if 'url_exists' in df.columns else 0
        count_invalid = len(df) - count_valid

        # Configurar los datos del gráfico
        counts = [count_valid, count_invalid]
        labels = [
            f"URLs Activas\n({count_valid:,})",
            f"URLs Inactivas\n({count_invalid:,})"
        ]
        colors = ['#3498db', '#e74c3c']

        fig, ax = plt.subplots(figsize=(8, 8))
        wedges, texts, autotexts = ax.pie(
            counts,
            labels=labels,
            colors=colors,
            autopct='%1.1f%%',
            startangle=90
        )
        
        # Enhance the appearance of the pie chart
        plt.setp(autotexts, size=9, weight="bold")
        plt.setp(texts, size=10)
        ax.axis('equal')
        st.pyplot(fig)

    def run(self):
        """Ejecuta la aplicación"""
        self.render_sidebar()
        self.render_main_content()

if __name__ == "__main__":
    app = EnterpriseApp()
    app.run()
