# app.py

import streamlit as st
# Configurar la página antes de cualquier otro comando de Streamlit
st.set_page_config(
    page_title="Sistema Empresarial de Análisis",
    page_icon="🏢",
    layout="wide",
    initial_sidebar_state="expanded"
)
import pandas as pd
import numpy as np
from datetime import datetime
import time
import matplotlib.pyplot as plt
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
import unicodedata
import logging
from agents import DBAgent
from database import DatabaseManager
from scraping import ProWebScraper
from config import REQUIRED_COLUMNS, PROVINCIAS_ESPANA, SQL_MODELS, DB_CONFIG
from agents import CustomLLM
from scraping_flow import WebScrapingService
import os
from dashboard import ScrapingDashboard
from rag_system import FinancialRAGSystem

# Configure logger
logger = logging.getLogger(__name__)

class EnterpriseApp:
    def __init__(self):
        self.init_session_state()
        self.db = DatabaseManager()
        self.scraper = ProWebScraper()
        self.setup_agents()
        # Initialize the scraping dashboard
        self.scraping_dashboard = ScrapingDashboard(use_sidebar=False)
        # Initialize the RAG system
        self.setup_rag_system()
        # Load data from DB if session_state is empty
        self.load_data_from_db()    
        
        # Add custom CSS
        self.apply_custom_styling()

    def apply_custom_styling(self):
        """Apply custom CSS styling to the app from an external file"""
        css_file = os.path.join(os.path.dirname(__file__), 'styles.css')
        
        if os.path.exists(css_file):
            with open(css_file, 'r') as file:
                st.markdown(f"<style>{file.read()}</style>", unsafe_allow_html=True)
        else:
            st.warning("CSS file not found! Make sure `style.css` exists in the same directory.")

    def init_session_state(self):
        """Initialize session state variables"""
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
        # Add active tab tracking
        if "active_tab" not in st.session_state:
            st.session_state.active_tab = 0
        # Chat history for the query interface
        if "chat_history" not in st.session_state:
            st.session_state.chat_history = []
        # Current company context for RAG queries
        if "current_company" not in st.session_state:
            st.session_state.current_company = None
        # RAG system model selection
        if "rag_model" not in st.session_state:
            st.session_state.rag_model = list(SQL_MODELS.keys())[0]  # Use SQL models for RAG too
            
    def setup_rag_system(self):
        """Initialize the Financial RAG System"""
        try:
            # Create the RAG system with selected model
            self.rag_system = FinancialRAGSystem(
                groq_model=SQL_MODELS.get(st.session_state.rag_model, st.session_state.rag_model)
            )
        except Exception as e:
            st.error(f"Error setting up RAG system: {str(e)}")
            
    def load_data_from_db(self):
        """Load data from database if session is empty"""
        if st.session_state.current_batch is None:
            df = self.db.execute_query("SELECT * FROM sociedades", return_df=True)
            if df is not None and not df.empty:
                # Normalize column names to lowercase
                df.columns = df.columns.str.strip().str.lower()
                
                # Clean whitespace
                df['nom_provincia'] = df['nom_provincia'].astype(str).str.strip()
                
                # Check for duplicates by unique identifier
                if 'cod_infotel' in df.columns:
                    df = df.drop_duplicates(subset=['cod_infotel'], keep='first')
                st.session_state.current_batch = {
                    "id": "loaded_from_db",
                    "data": df,
                    "total_records": len(df),
                    "timestamp": datetime.now()
                }

    def setup_agents(self):
        """Configure intelligent agents based on selected models"""
        try:
            # Create new agent instances with selected models
            self.db_agent = DBAgent()
            modelo_sql = SQL_MODELS.get(st.session_state.sql_model, st.session_state.sql_model)
            self.db_agent.llm = CustomLLM(modelo_sql, provider="groq")
        except Exception as e:
            st.error(f"Error setting up agents: {str(e)}")

    def render_sidebar(self):
        """Render sidebar with loading options and filters"""
        with st.sidebar:
            st.image("images/logo.png", width=200)
            
            # Get the current active tab
            active_tab = st.session_state.active_tab
            
            # Show different sidebar content based on active tab
            if active_tab == 2:  # Web Scraping tab is active
                self.render_scraping_sidebar()
            else:
                # Model Selection Section
                st.subheader("🤖 Model Configuration")
                
                selected_sql_model = st.selectbox(
                    "SQL Query Model",
                    list(SQL_MODELS.keys()),
                    index=0,
                    help="Select Groq model for SQL queries"
                )
                
                # Add RAG model selection
                selected_rag_model = st.selectbox(
                    "Financial Information Model",
                    list(SQL_MODELS.keys()),
                    index=0,
                    help="Select Groq model for financial information"
                )
                
                # Update models if changed
                models_changed = False
                if selected_sql_model != st.session_state.sql_model:
                    st.session_state.sql_model = selected_sql_model
                    models_changed = True
                    
                if selected_rag_model != st.session_state.rag_model:
                    st.session_state.rag_model = selected_rag_model
                    models_changed = True
                    
                if models_changed:
                    self.setup_agents()
                    self.setup_rag_system()
                
                # File Upload Section
                st.subheader("📤 Data Upload")
                uploaded_file = st.file_uploader(
                    "Select file (CSV/XLSX)",
                    type=["csv", "xlsx"],
                    help="Supported formats: CSV, Excel"
                )
                
                if uploaded_file:
                    self.handle_file_upload(uploaded_file)
                
                # Filters Section
                if st.session_state.current_batch:
                    st.subheader("🔍 Filters")
                    selected_provincia = st.selectbox(
                        "Province",
                        ["All"] + PROVINCIAS_ESPANA
                    )
                    
                    has_web = st.checkbox("Only with website", value=False)
                    has_ecommerce = st.checkbox("Only with e-commerce", value=False)
                    
                    if st.button("Apply Filters"):
                        self.apply_filters(selected_provincia, has_web, has_ecommerce)
                
                # Database Reset Button
                if st.button("Reset Database", help="Reset the database"):
                    self.db.reset_database()
                    st.session_state.current_batch = None
                    st.success("Database reset successfully.")
                    st.rerun()
                    
    def render_scraping_sidebar(self):
        """Render sidebar controls for the scraping dashboard"""
        # We're directly calling the dashboard's sidebar control rendering method
        self.scraping_dashboard._render_sidebar_controls()

    def render_main_content(self):
        st.title("Sistema de Análisis Empresarial 🏢")
        
        # Use a radio button that looks like tabs
        tab_options = ["📊  DASHBOARD  ", "🔍  CONSULTAS  ", "🌐  WEB SCRAPING  "]
        
        # Get the previously selected tab
        prev_tab_index = st.session_state.active_tab
        
        # Add the radio selector for tabs
        selected_tab = st.radio("", tab_options, index=prev_tab_index, horizontal=True, key="tab_selector")
        
        # Map selection to index
        tab_index = tab_options.index(selected_tab)
        
        # Force a rerun if the tab has changed
        if tab_index != prev_tab_index:
            st.session_state.active_tab = tab_index
            st.experimental_rerun()
        
        # Display content based on selected tab
        if tab_index == 0:
            self.render_dashboard()
        elif tab_index == 1:
            self.render_queries()
        else:
            self.render_scraping()

    def handle_file_upload(self, file):
        """Process file upload and update database, silently ignoring duplicates"""
        try:
            with st.spinner("Processing file..."):
                # Read file
                if file.name.endswith('.csv'):
                    df = pd.read_csv(file, header=0, sep=';', encoding='utf-8')
                else:
                    df = pd.read_excel(file, header=0)
                    
                # Validate columns
                missing_cols = [col for col in REQUIRED_COLUMNS if col not in df.columns]
                if missing_cols:
                    st.error(f"Missing required columns: {', '.join(missing_cols)}")
                    return
                
                # Normalize column names
                df.columns = [col.strip().lower() for col in df.columns]

                # Get existing records for comparison
                existing_records = self.db.execute_query(
                    "SELECT cod_infotel FROM sociedades WHERE deleted = FALSE",
                    return_df=True
                )
                
                if existing_records is not None and not existing_records.empty:
                    existing_codes = set(existing_records['cod_infotel'].values)
                    
                    # Filter out records that already exist in the DB
                    df = df[~df['cod_infotel'].isin(existing_codes)]
                    
                    if len(df) == 0:
                        st.info("No new records to process. All records already exist in the database.")
                        return

                # Clean data before saving
                df = df.replace(r'^\s*$', None, regex=True)
                df = df.replace({np.nan: None})
                
                # Save to database
                result = self.db.save_batch(df, check_duplicates=True)
                
                if result["status"] == "success":
                    st.success(f"✅ File processed successfully: {result['inserted']} new records added")
                    # Reload data from DB to update the dashboard
                    self.load_data_from_db()
                elif result["status"] == "partial":
                    st.warning(
                        f"⚠️ Partial processing: {result['inserted']}/{result['total']} records. "
                        f"Errors: {'; '.join(result['errors'])}"
                    )
                else:
                    st.error(f"❌ Error processing file: {result['message']}\nDetails: {result['errors']}")
                    
        except Exception as e:
            import traceback
            error_details = traceback.format_exc()
            st.error(f"❌ Error processing file: {str(e)}\n\nDetails:\n{error_details}")

    def render_dashboard(self):
        """Render dashboard with DB data"""
        # Get updated data from DB
        df = self.db.execute_query("SELECT * FROM sociedades WHERE deleted = FALSE", return_df=True)
        
        if df is None or df.empty:
            st.info("👆 No data in database. Upload a file to see statistics")
            return

        # Normalize column names
        df.columns = df.columns.str.strip().str.lower()
        
        # Enhanced metrics display
        st.markdown("### 📊 General Statistics")
        metrics_container = st.container()
        col1, col2, col3, col4 = metrics_container.columns(4)
        
        with col1:
            st.metric(
                "Total Records",
                f"{len(df):,}",
                delta=None,
            )
        
        # Count valid URLs (existing and accessible)
        total_with_web = df['url_exists'].sum() if 'url_exists' in df.columns else 0
        with col2:
            st.metric(
                "With Active Website",
                f"{total_with_web:,}",
                delta=f"{(total_with_web/len(df)*100):.1f}%" if len(df) > 0 else None
            )
        
        unique_provinces = df['nom_provincia'].nunique()
        with col3:
            st.metric(
                "Provinces",
                unique_provinces,
                delta=None
            )
        
        # Get last update date from DB
        last_update = df['fecha_actualizacion'].max() if 'fecha_actualizacion' in df.columns else None
        with col4:
            st.metric(
                "Last update",
                last_update.strftime("%d-%m-%Y") if last_update else "Not available"
            )
        
        # Enhanced charts section
        st.markdown("### 📈 Data Visualization")
        chart_col1, chart_col2 = st.columns(2)
        
        with chart_col1:
            st.markdown("#### Distribution by Province")
            prov_counts = df['nom_provincia'].value_counts().head(10)
            fig, ax = plt.subplots(figsize=(10, 6))
            ax.bar(prov_counts.index, prov_counts.values)
            plt.xticks(rotation=45, ha='right')
            plt.title("Top 10 Provinces")
            st.pyplot(fig)
            
        with chart_col2:
            st.markdown("#### URL Status")
            self.render_url_status_chart(df)
            
        # --- Nueva sección: Integración de Análisis Interactivo ---
        st.markdown("### 📈 Interactive Analysis")
        analysis_type = st.selectbox(
            "Analysis Type",
            ["Geographic Distribution", "E-commerce Analysis", "Digital Presence", "Contactability"],
            key="dashboard_analysis_type"
        )
        if st.button("Generate Analysis", key="dashboard_generate_analysis"):
            self.generate_analysis(analysis_type)

    def render_queries(self):
        """Render unified query interface for both SQL and RAG queries"""
        st.subheader("🔍 Advanced Queries")
        
        # Create a container for the query input
        query_container = st.container()
        
        # Create tabs for chat history and configuration
        chat_tab, config_tab = st.tabs(["Consultas", "Configuración"])
        
        with config_tab:
            st.checkbox("Show SQL", value=False, key="show_sql", 
                        help="Display the SQL query generated from your natural language question")
            
            # Add clear history button
            if st.button("Borrar historial de consultas"):
                st.session_state.chat_history = []
                st.success("Historial de consultas borrado")
                st.experimental_rerun()
            
            # Company context for RAG queries
            if st.session_state.current_batch is not None and not st.session_state.current_batch['data'].empty:
                st.markdown("### Contexto para consultas financieras")
                st.info("Selecciona una empresa para hacer preguntas específicas sobre su información financiera.")
                
                # Get unique company names
                df = st.session_state.current_batch['data']
                company_names = df['razon_social'].dropna().unique().tolist()
                
                if company_names:
                    selected_company = st.selectbox(
                        "Selecciona una empresa",
                        ["Ninguna"] + company_names,
                        index=0,
                        help="Selecciona una empresa para preguntas específicas sobre información financiera"
                    )
                    
                    if selected_company != "Ninguna":
                        if st.button("Establecer como contexto actual"):
                            st.session_state.current_company = selected_company
                            st.success(f"✅ {selected_company} establecida como contexto actual")
                            
                            # Add a button to search for financial information
                            with st.spinner(f"Buscando información financiera de {selected_company}..."):
                                company_info = self.rag_system.search_company_info(selected_company)
                                if company_info and not company_info.get('error'):
                                    st.success(f"Información financiera encontrada para {selected_company}")
                                    st.json(company_info)
                                else:
                                    st.warning(f"No se encontró información financiera para {selected_company}")
                    elif st.session_state.current_company:
                        if st.button("Limpiar contexto actual"):
                            st.session_state.current_company = None
                            st.success("Contexto limpiado")
        
        with chat_tab:
            # Display chat history
            for message in reversed(st.session_state.chat_history):
                role = message["role"]
                content = message["content"]
                
                if role == "user":
                    st.markdown(f"""<div class="user-message">
                                <strong>👤 Tú:</strong> {content}
                                </div>""", unsafe_allow_html=True)
                else:
                    # Check if it's an error or special message
                    message_type = message.get("type", "normal")
                    if message_type == "error" or (message_type == "sql" and "No se encontraron resultados" in content):
                        st.markdown(f"""<div class="alert-message">
                                    <strong>🤖 Asistente:</strong> {content}
                                    </div>""", unsafe_allow_html=True)
                    else:
                        st.markdown(f"""<div class="assistant-message">
                                    <strong>🤖 Asistente:</strong> {content}
                                    </div>""", unsafe_allow_html=True)
                    
                    # Show SQL if requested and available
                    if st.session_state.show_sql and "sql" in message:
                        with st.expander("SQL Generado"):
                            st.code(message["sql"], language="sql")
                    
                    # Show data if available
                    if "data" in message and message["data"] is not None:
                        with st.expander("Resultados"):
                            st.dataframe(message["data"])
            
        # Query input
        with query_container:
            query = st.text_area(
                "Escribe tu consulta en lenguaje natural",
                placeholder="Ejemplo: Dame las primeras 10 empresas en Madrid, o ¿Cuál es la información financiera de Empresa X?",
                help="Se traducirá a una consulta SQL o recuperará información financiera"
            )
            
            if st.button("Enviar Consulta"):
                if query:
                    # Add user message to chat history
                    st.session_state.chat_history.append({"role": "user", "content": query})
                    
                    # Process the query
                    self.process_unified_query(query)
                    
                    # Rerun to update the UI
                    st.experimental_rerun()

    def process_unified_query(self, query: str):
        """Process a unified query - handle both SQL and RAG responses"""
        # Check if it's explicitly asking about a company
        company_pattern = re.compile(r'sobre\s+([A-Za-z0-9\s]+)', re.IGNORECASE)
        company_match = company_pattern.search(query)
        
        # If a company is mentioned directly in the query, use it
        if company_match:
            company_name = company_match.group(1).strip()
            
            with st.spinner(f"Buscando información sobre {company_name}..."):
                try:
                    # Search for company info
                    company_info = self.rag_system.search_company_info(company_name)
                    
                    # Set a token limit (e.g., 1000 tokens)
                    MAX_TOKENS = 1000
                    
                    # Get RAG answer
                    answer = self.rag_system.answer_financial_question(company_name, query)
                    
                    # Truncate answer if too long
                    if len(answer.split()) > MAX_TOKENS:
                        truncated_answer = " ".join(answer.split()[:MAX_TOKENS])
                        truncated_answer += "... [Respuesta truncada por longitud]"
                        answer = truncated_answer
                    
                    # Add assistant message to chat history
                    st.session_state.chat_history.append({
                        "role": "assistant", 
                        "content": answer,
                        "type": "financial",
                        "company": company_name
                    })
                    return
                except Exception as e:
                    error_msg = f"Error buscando información: {str(e)}"
                    st.session_state.chat_history.append({
                        "role": "assistant", 
                        "content": error_msg,
                        "type": "error"
                    })
                    return
        
        # Otherwise check if a company is already selected in configuration
        elif st.session_state.current_company and any(keyword in query.lower() for keyword in [
            "financial", "finances", "revenue", "income", "profit", "empleados", 
            "facturación", "ingresos", "beneficio", "financiera", "financieras", "financiero"
        ]):
            # Process using RAG for financial information
            with st.spinner(f"Getting financial information for {st.session_state.current_company}..."):
                try:
                    answer = self.rag_system.answer_financial_question(
                        st.session_state.current_company, query
                    )
                    
                    # Add assistant message to chat history
                    st.session_state.chat_history.append({
                        "role": "assistant", 
                        "content": answer,
                        "type": "financial",
                        "company": st.session_state.current_company
                    })
                except Exception as e:
                    error_msg = f"Error getting financial information: {str(e)}"
                    st.session_state.chat_history.append({
                        "role": "assistant", 
                        "content": error_msg,
                        "type": "error"
                    })
            return
            
        # Otherwise process as a SQL query
        try:
            with st.spinner("Processing query..."):
                # Generate query
                query_info = self.db_agent.generate_query(query)
                
                # Check for errors
                if query_info.get("error"):
                    st.session_state.chat_history.append({
                        "role": "assistant", 
                        "content": query_info["error"],
                        "type": "error"
                    })
                    return
                    
                # Execute query
                results = self.db.execute_query(query_info["query"], return_df=True)
                
                # Generate response message
                if query_info["query_type"] == "count":
                    value = results.iloc[0, 0]
                    response = f"Total: {value:,}"
                elif query_info["query_type"] == "aggregate":
                    response = "Aquí están los resultados de tu consulta:"
                else:
                    if results is not None and not results.empty:
                        response = f"Encontré {len(results)} resultados para tu consulta:"
                    else:
                        response = "No se encontraron resultados para tu consulta."
                
                # Add explanation if available
                if query_info.get("explanation"):
                    response += f"\n\n{query_info['explanation']}"
                
                # Add assistant message to chat history
                st.session_state.chat_history.append({
                    "role": "assistant", 
                    "content": response,
                    "type": "sql",
                    "sql": query_info["query"],
                    "data": results,
                    "query_type": query_info["query_type"]
                })
                    
        except Exception as e:
            error_msg = f"Error processing query: {str(e)}"
            st.session_state.chat_history.append({
                "role": "assistant", 
                "content": error_msg,
                "type": "error"
            })

    def render_scraping(self):
        """Render web scraping section with integrated dashboard"""
        st.subheader("🌐 Web Scraping")
        
        # Option to start or stop the dashboard
        if "dashboard_running" not in st.session_state:
            st.session_state.dashboard_running = False
        
        if not st.session_state.dashboard_running:
            st.info("Este módulo integra la funcionalidad de scraping con Supabase. Pulsa el botón para iniciar el proceso.")
            if st.button("Iniciar Scraping con Supabase"):
                st.session_state.dashboard_running = True
                st.experimental_rerun()
        else:
            if st.button("Detener Dashboard"):
                st.session_state.dashboard_running = False
                st.experimental_rerun()
            
            # Run the dashboard without its sidebar (we're using our own in the main app)
            self.scraping_dashboard.run()
                
    def get_remaining_count(self):
        """Get the count of remaining companies to process"""
        try:
            query = "SELECT COUNT(*) FROM sociedades WHERE processed = FALSE"
            result = self.db.execute_query(query, return_df=True)
            if result is not None and not result.empty:
                return result.iloc[0, 0]
            return 0
        except Exception as e:
            logger.error(f"Error getting remaining count: {e}")
            return 0

    def render_analysis(self):
        """Render analysis section"""
        st.subheader("📈 Data Analysis")
        
        if not st.session_state.current_batch:
            st.warning("⚠️ Load data to perform analysis")
            return
        
        analysis_type = st.selectbox(
            "Analysis Type",
            [
                "Geographic Distribution",
                "E-commerce Analysis",
                "Digital Presence",
                "Contactability"
            ]
        )
        
        if st.button("Generate Analysis"):
            self.generate_analysis(analysis_type)

    def process_query(self, query: str):
        """Process a natural language query to SQL and execute it"""
        try:
            with st.spinner("Processing query..."):
                # Generate query
                query_info = self.db_agent.generate_query(query)
                
                # Check for errors
                if query_info.get("error"):
                    st.error(query_info["error"])
                    return
                    
                # Execute query
                results = self.db.execute_query(query_info["query"], return_df=True)
                
                # Store results in session state
                st.session_state.last_query = {
                    "query": query,
                    "sql": query_info["query"],
                    "query_type": query_info["query_type"],
                    "results": results,
                    "explanation": query_info.get("explanation", "")
                }
                
                # Handle different query types
                if query_info["query_type"] == "count":
                    value = results.iloc[0, 0]
                    st.metric("Total", f"{value:,}")
                elif query_info["query_type"] == "aggregate":
                    st.dataframe(results)
                else:
                    st.dataframe(results)
                    
        except Exception as e:
            st.error(f"Error processing query: {str(e)}")

    def generate_analysis(self, analysis_type: str):
        """Generate specific analyses"""
        try:
            if analysis_type == "Geographic Distribution":
                self.show_geographic_analysis()
            elif analysis_type == "E-commerce Analysis":
                self.show_ecommerce_analysis()
            elif analysis_type == "Digital Presence":
                self.show_digital_presence_analysis()
            else:
                self.show_contactability_analysis()
        except Exception as e:
            st.error(f"Error generating analysis: {str(e)}")

    def apply_filters(self, provincia: str, has_web: bool, has_ecommerce: bool):
        """Apply filters to current data"""
        try:
            df = st.session_state.current_batch['data'].copy()
            
            if provincia != "All":
                df = df[df['nom_provincia'] == provincia]
                
            if has_web:
                df = df[df['url'].notna()]
                
            if has_ecommerce:
                df = df[df['e_commerce'] == True]
                
            st.session_state.current_batch['filtered_data'] = df
            st.success("Filters applied successfully")
        except Exception as e:
            st.error(f"Error applying filters: {str(e)}")

    def render_url_status_chart(self, df):
        """Render URL status chart using DB data"""
        # Use url_exists field from DB
        count_valid = df['url_exists'].sum() if 'url_exists' in df.columns else 0
        count_invalid = len(df) - count_valid

        # Configure chart data
        counts = [count_valid, count_invalid]
        labels = [
            f"Active URLs\n({count_valid:,})",
            f"Inactive URLs\n({count_invalid:,})"
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
        
        # Enhance pie chart appearance
        plt.setp(autotexts, size=9, weight="bold")
        plt.setp(texts, size=10)
        ax.axis('equal')
        st.pyplot(fig)

    # Analysis methods
    def show_geographic_analysis(self):
        st.write("### Geographic Analysis")
        df = st.session_state.current_batch['data']
        prov_counts = df['nom_provincia'].value_counts()
        st.bar_chart(prov_counts)

    def show_ecommerce_analysis(self):
        st.write("### E-commerce Analysis")
        df = st.session_state.current_batch['data']
        if 'e_commerce' in df.columns:
            ecommerce_counts = df['e_commerce'].value_counts()
            st.bar_chart(ecommerce_counts)
        else:
            st.info("No e-commerce data available.")

    def show_digital_presence_analysis(self):
        st.write("### Digital Presence")
        df = st.session_state.current_batch['data']
        presence = df['url'].notna().value_counts()
        st.bar_chart(presence)

    def show_contactability_analysis(self):
        st.write("### Contactability Analysis")
        df = st.session_state.current_batch['data']
        if 'nif' in df.columns:
            count_nif = df['nif'].notna().sum()
            st.write(f"Companies with NIF: {count_nif}")
        else:
            st.info("No contact data available.")

    def run(self):
        """Run the application"""
        self.render_sidebar()
        self.render_main_content()

if __name__ == "__main__":
    app = EnterpriseApp()
    app.run()