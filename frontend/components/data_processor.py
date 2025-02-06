# frontend/components/data_processor.py
import streamlit as st
import pandas as pd
import numpy as np
import re
from urllib.parse import urlparse
from typing import Dict, List, Tuple
import logging
from io import BytesIO
from database.connectors import MySQLConnector
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataProcessor:
    def __init__(self):
        self.processed_count = 0
        self.df_clean = None 
        
    def normalize_postal_code(self, postal_code: str) -> str:
        """Normalize postal code to 5 digits"""
        try:
            postal_code = str(postal_code).strip()
            postal_code = re.sub(r'\D', '', postal_code)
            return postal_code.zfill(5)
        except Exception as e:
            logger.warning(f"Error normalizing postal code {postal_code}: {str(e)}")
            return postal_code

    def normalize_url(self, url: str) -> Tuple[str, bool]:
        """Normalize URL and check if it's potentially valid"""
        if pd.isna(url) or not url:
            return "", False

        url = str(url).strip().lower()
        
        if re.search(r'busqueda/access|\.org/\w+/\d+', url):
            return "", False

        if not url.startswith(('http://', 'https://')):
            url = f'https://{url}'

        try:
            parsed = urlparse(url)
            is_valid = bool(parsed.netloc and parsed.scheme in ['http', 'https'])
            
            if not re.match(r'^[a-zA-Z0-9]', parsed.netloc):
                return "", False
                
            return url, is_valid
        except Exception:
            return "", False

    def normalize_nif(self, nif: str) -> str:
        """Normalize NIF/CIF format"""
        if pd.isna(nif):
            return ""
        
        nif = str(nif).strip().upper()
        nif = re.sub(r'[^A-Z0-9]', '', nif)
        return nif

    def process_dataframe(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """Process and normalize the dataframe, return stats"""
        logger.info("Starting data normalization process...")
        
        total_records = len(df)
        df_clean = df.copy()
        
        # Normalize columns
        df_clean['COD_POSTAL'] = df_clean['COD_POSTAL'].apply(self.normalize_postal_code)
        df_clean['NIF'] = df_clean['NIF'].apply(self.normalize_nif)
        df_clean['RAZON_SOCIAL'] = df_clean['RAZON_SOCIAL'].str.strip()
        
        # Process URLs
        url_results = df_clean['URL'].apply(self.normalize_url)
        df_clean['URL'] = [result[0] for result in url_results]
        df_clean['URL_VALID'] = [result[1] for result in url_results]
        
        # Additional cleaning
        df_clean['NOM_PROVINCIA'] = df_clean['NOM_PROVINCIA'].str.strip().str.title()
        df_clean['NOM_POBLACION'] = df_clean['NOM_POBLACION'].str.strip().str.title()
        df_clean['DOMICILIO'] = df_clean['DOMICILIO'].str.strip()
        
        # Remove rows with critical missing data
        df_clean = df_clean.dropna(subset=['NIF', 'RAZON_SOCIAL'])
        
        # Calculate statistics
        stats = {
            'total_records': total_records,
            'processed_records': len(df_clean),
            'invalid_records': total_records - len(df_clean),
            'valid_urls': df_clean['URL_VALID'].sum(),
            'provinces_count': df_clean['NOM_PROVINCIA'].nunique()
        }
        
        logger.info(f"Data normalization completed. Processed {len(df_clean)} records.")
        return df_clean, stats

class DataProcessorInterface:
    def __init__(self):
        self.processor = DataProcessor()
        self.db_connector = MySQLConnector()
        self.df_clean = None  # Initialize df_clean as None
        self.provinces = ["Todas"]  # Initialize provinces list

    def render_upload_section(self, main_area: bool = False):
        """Render the file upload section and data processing interface"""
        # If called from main area, use full width
        container = st if main_area else st.sidebar
        
        uploaded_file = container.file_uploader(
            "Cargar archivo Excel de empresas",
            type=['xlsx', 'xls'],
            help="Selecciona el archivo Excel con los datos de empresas"
        )
        
        if uploaded_file is not None:
            try:
                # Show loading message while reading file
                with st.spinner('Leyendo archivo Excel...'):
                    df = pd.read_excel(uploaded_file)
                
                # Process data with progress bar
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                total_records = len(df)
                chunk_size = 1000
                processed_records = 0
                
                chunks = [df[i:i + chunk_size] for i in range(0, len(df), chunk_size)]
                processed_data = []
                
                for i, chunk in enumerate(chunks):
                    chunk_clean, _ = self.processor.process_dataframe(chunk)
                    processed_data.append(chunk_clean)
                    processed_records += len(chunk)
                    
                    # Update progress
                    progress = processed_records / total_records
                    progress_bar.progress(progress)
                    status_text.text(f"Procesando registros... {processed_records:,}/{total_records:,}")
                
                # Combine all processed chunks
                self.df_clean = pd.concat(processed_data, ignore_index=True)
                status_text.text("✅ Procesamiento completado!")
                progress_bar.empty()
                
                # Update provinces list
                self.provinces = ["Todas"] + sorted(self.df_clean['NOM_PROVINCIA'].unique().tolist())
                
                # Display stats and preview
                container.markdown("### 📊 Estadísticas de Procesamiento")
                col1, col2, col3 = container.columns(3)
                col1.metric("Registros Totales", f"{total_records:,}")
                col2.metric("Registros Válidos", f"{len(self.df_clean):,}")
                col3.metric("URLs Válidas", f"{self.df_clean['URL_VALID'].sum():,}")
                
                # Preview data
                container.markdown("### 👁️ Vista Previa")
                container.dataframe(
                    self.df_clean.head(10),
                    use_container_width=True,
                    hide_index=True
                )
                
                # Export options
                col1, col2 = container.columns(2)
                with col1:
                    if container.button("💾 Guardar en Base de Datos"):
                        self._save_to_database(self.df_clean)
                
                with col2:
                    output = BytesIO()
                    with pd.ExcelWriter(output, engine='openpyxl') as writer:
                        self.df_clean.to_excel(writer, index=False)
                    
                    container.download_button(
                        "📥 Descargar Excel Procesado",
                        data=output.getvalue(),
                        file_name="datos_procesados.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                
            except Exception as e:
                container.error(f"Error: {str(e)}")
                logger.error(f"Processing error: {str(e)}")
        
        # Add delete section
        self._render_delete_section(container)

    def _render_delete_section(self, container):
        """Render the delete section UI"""
        container.markdown("### 🗑️ Eliminar Datos")
        with container.form("delete_form"):
            delete_provincia = st.selectbox(
                "Provincia a eliminar",
                options=self.provinces
            )
            delete_submitted = st.form_submit_button("🗑️ Eliminar Registros")
            
            if delete_submitted:
                criteria = {}
                if delete_provincia != "Todas":
                    criteria['provincia'] = delete_provincia
                    
                with st.spinner('Eliminando registros...'):
                    deleted = self.db_connector.delete_companies(criteria)
                    if deleted > 0:
                        st.success(f"✅ {deleted} registros eliminados exitosamente")
                    else:
                        st.warning("No se encontraron registros para eliminar")

    def _save_to_database(self, df: pd.DataFrame):
        """Save processed data to database"""
        try:
            # Transform data to match database schema
            companies_data = [
                {
                    'nif': row['NIF'],
                    'razon_social': row['RAZON_SOCIAL'],
                    'provincia': row['NOM_PROVINCIA'],
                    'website': row['URL'],
                    'direccion': row['DOMICILIO'],
                    'codigo_postal': row['COD_POSTAL'],
                    'poblacion': row['NOM_POBLACION'],
                    'codigo_infotel': row['COD_INFOTEL'],
                    'url_valid': row['URL_VALID'],
                    'confidence_score': 100
                }
                for _, row in df.iterrows()
            ]
            
            # Bulk insert to database
            with st.spinner('Guardando datos en la base de datos...'):
                success = self.db_connector.bulk_insert_companies(companies_data)
                if success:
                    st.success(f"✅ {len(companies_data):,} registros guardados exitosamente")
                else:
                    st.error("❌ Error al guardar los datos")
                
        except Exception as e:
            st.error(f"Error al guardar en la base de datos: {str(e)}")
            logger.error(f"Database error: {str(e)}")