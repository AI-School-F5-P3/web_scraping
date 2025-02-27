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
from config import REQUIRED_COLUMNS, PROVINCIAS_ESPANA, SQL_MODELS, SCRAPING_MODELS, DB_CONFIG
from agents import CustomLLM
from scraping_flow import WebScrapingService
import os
from dashboard import ScrapingDashboard

# Configure logger
logger = logging.getLogger(__name__)

class EnterpriseApp:
    def __init__(self):
        self.init_session_state()
        self.db = DatabaseManager()
        self.scraper = ProWebScraper()
        self.setup_agents()
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
        if "scraping_model" not in st.session_state:
            st.session_state.scraping_model = list(SCRAPING_MODELS.keys())[0]
            
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
            
            # Model Selection Section
            st.subheader("🤖 Model Configuration")
            
            selected_sql_model = st.selectbox(
                "SQL Query Model",
                list(SQL_MODELS.keys()),
                index=0,
                help="Select Groq model for SQL queries"
            )
            
            # Update models if changed
            if selected_sql_model != st.session_state.sql_model:
                st.session_state.sql_model = selected_sql_model
                self.setup_agents()
            
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

    def render_main_content(self):
        st.title("Business Analysis System 🏢")
        
        # Solo 3 pestañas: Dashboard, Queries y Web Scraping
        tabs = st.tabs([
            "📊  DASHBOARD  ",
            "🔍  QUERIES  ",
            "🌐  WEB SCRAPING  "
        ])
        
        with tabs[0]:
            self.render_dashboard()
        with tabs[1]:
            self.render_queries()
        with tabs[2]:
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
        """Render queries section (SQL)"""
        st.subheader("🔍 Advanced Queries (SQL)")

        query = st.text_area(
            "Write your query in natural language",
            placeholder="Example: Give me the first 10 companies in Madrid",
            help="Will be translated to an SQL query"
        )

        col1, col2 = st.columns([1, 4])
        with col1:
            execute_button = st.button("Execute Query")
        with col2:
            st.checkbox("Show SQL", value=False, key="show_sql")
        
        if execute_button and query:
            self.process_query(query)
            
        # Show results only if we have a last query
        last_query = st.session_state.get("last_query", None)
        if last_query and "results" in last_query:
            if st.session_state.show_sql and "sql" in last_query:
                st.code(last_query["sql"], language="sql")
                
        # Show explanation if available
        if last_query and "explanation" in last_query and last_query["explanation"]:
            with st.expander("LLM Explanation"):
                st.write(last_query["explanation"])

    def render_scraping(self):
        """Render web scraping section with Supabase integration"""
        st.subheader("🌐 Web Scraping")
        
        st.info("Este módulo integra la funcionalidad de scraping con Supabase. Pulsa el botón para iniciar el proceso.")
        
        # Botón para iniciar el scraping basado en Supabase (dashboard)
        if st.button("Start Scraping with Supabase"):
            # Importa la clase del dashboard (asegúrate de que dashboard.py esté en el mismo directorio o en el path)
            from dashboard import ScrapingDashboard
            
            # Instanciar y ejecutar el dashboard
            dashboard_instance = ScrapingDashboard()
            dashboard_instance.run()
        else:
            st.write("Pulsa el botón para iniciar el proceso de scraping.")
                
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