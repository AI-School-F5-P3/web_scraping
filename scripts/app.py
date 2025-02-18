# app.py

import streamlit as st
import pandas as pd
from datetime import datetime
import time
from agents import DBAgent, ScrapingAgent  # Removed OrchestratorAgent
from database import DatabaseManager
from scraping import ProWebScraper
from config import REQUIRED_COLUMNS, PROVINCIAS_ESPANA
import matplotlib.pyplot as plt
from concurrent.futures import ThreadPoolExecutor, as_completed

class EnterpriseApp:
    def __init__(self):
        self.init_session_state()
        self.db = DatabaseManager()
        self.scraper = ProWebScraper()
        self.setup_agents()
        # Cargar datos de la BD si no hay nada en session_state
        self.load_data_from_db()
        
        st.set_page_config(
            page_title="Sistema Empresarial de Análisis",
            page_icon="🏢",
            layout="wide",
            initial_sidebar_state="expanded"
        )

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
                    
            if st.button("Borrar BBDD"):
                self.db.reset_database()
                # Limpiar la variable que contiene los datos cargados
                st.session_state.current_batch = None
                st.success("Base de datos reiniciada exitosamente.")
                st.experimental_rerun()  # Fuerza la recarga de la app

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
            with st.spinner("Procesando archivo..."):
                # Leer archivo
                if file.name.endswith('.csv'):
                    df = pd.read_csv(file, header=0, sep=';', encoding='utf-8')
                else:
                    df = pd.read_excel(file, header=0)
                    
                st.write("Columnas detectadas:", df.columns.tolist())
                
                # Validar columnas
                missing_cols = [col for col in REQUIRED_COLUMNS if col not in df.columns]
                if missing_cols:
                    st.error(f"Faltan columnas requeridas: {', '.join(missing_cols)}")
                    return
                
                # Normalizar nombres de columnas
                df.columns = [col.strip().lower() for col in df.columns]
                
                # Guardar en base de datos sin batch_id ni created_by
                result = self.db.save_batch(df)
                
                st.write("Resultado de save_batch:", result)
                
                if result["status"] == "success":
                    st.session_state.current_batch = {
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
            st.metric("Última actualización", st.session_state.current_batch['timestamp'].strftime("%Y-%m-%d %H:%M:%S"))
        
        # Gráficos
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Distribución por Provincia")
            prov_counts = df['nom_provincia'].value_counts()
            st.bar_chart(prov_counts)
            
        with col2:
            st.subheader("Estado de URLs")
            valid_url = df['url'].apply(
                lambda x: isinstance(x, str) and x.strip() != '' and 
                          (x.strip().lower().startswith("http://") or 
                           x.strip().lower().startswith("https://") or 
                           x.strip().lower().startswith("www."))
            )
            url_status = valid_url.value_counts()
            labels = ["Con URL" if val is True else "Sin URL" for val in url_status.index]
            sizes = url_status.values
            default_colors = ['#66b3ff', '#ff9999']
            colors = default_colors[:len(sizes)]
            
            import matplotlib.pyplot as plt
            fig, ax = plt.subplots()
            ax.pie(sizes, labels=labels, colors=colors, autopct='%1.1f%%', startangle=90)
            ax.axis('equal')
            st.pyplot(fig)

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
            if st.button("Ejecutar Consulta"):
                self.process_query(query)
        with col2:
            st.checkbox("Mostrar SQL", value=False, key="show_sql")
        
        if st.session_state.last_query:
            with st.expander("📝 Última consulta", expanded=True):
                if st.session_state.show_sql and "sql" in st.session_state.last_query:
                    st.code(st.session_state.last_query["sql"], language="sql")
                # If the result is an aggregate (e.g., a single number) show it as a metric
                results = st.session_state.last_query.get("results")
                if results is not None:
                    if isinstance(results, pd.DataFrame) and results.shape[0] == 1 and results.shape[1] == 1:
                        value = results.iloc[0, 0]
                        st.metric("Resultado", value)
                    else:
                        st.dataframe(results, use_container_width=True)
                if "explanation" in st.session_state.last_query:
                    with st.expander("Explicación LLM"):
                        st.write(st.session_state.last_query["explanation"])

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

    def process_query(self, query: str):
        """Procesa consultas en lenguaje natural usando DBAgent para generar SQL"""
        try:
            with st.spinner("Procesando consulta..."):
                query_info = self.db_agent.generate_query(query)
                results = self.db.execute_query(query_info["query"], return_df=True)
                st.session_state.last_query = {
                    "sql": query_info["query"],
                    "results": results
                }
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
                # Aquí podrías actualizar un log específico para esta tarea si se requiere.
                result = self.scraping_agent.plan_scraping(row['url'])
                result['cod_infotel'] = row['cod_infotel']
                result['original_url'] = row['url']  # Guardamos la URL original para comparación
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
            st.write(f"Tiempo total de scraping: {elapsed:.2f} segundos")

            # Mostrar resultados detallados en un DataFrame temporal
            results_df = pd.DataFrame(results)
            st.subheader("Resultados del Web Scraping (previo a actualizar la BD)")
            st.dataframe(results_df)

            # Preguntar al usuario si desea aplicar los cambios a la BD
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

    def run(self):
        """Ejecuta la aplicación"""
        self.render_sidebar()
        self.render_main_content()

if __name__ == "__main__":
    app = EnterpriseApp()
    app.run()
