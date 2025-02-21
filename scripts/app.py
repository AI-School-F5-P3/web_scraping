# app.py

import streamlit as st
import pandas as pd
from datetime import datetime
import pyperclip
import matplotlib.pyplot as plt
from database import DatabaseManager
from config import REQUIRED_COLUMNS, PROVINCIAS_ESPANA
import numpy as np
import re
from typing import List, Dict, Any, Optional
import io
import os
from pathlib import Path

# Configuración de la página
st.set_page_config(
    page_title="Sistema Empresarial de Análisis",
    page_icon="🏢",
    layout="wide",
    initial_sidebar_state="expanded"
)

class FileProcessor:
    """Procesa y valida archivos de entrada"""
    def __init__(self, required_columns: list):
        self.required_columns = [col.upper() for col in required_columns]
        
    def process_file(self, file_content: bytes, filename: str) -> Dict[str, Any]:
        """Procesa el archivo subido y valida su estructura"""
        try:
            file_extension = filename.lower().split('.')[-1]
            
            if file_extension == 'csv':
                df = self._read_csv(file_content)
            elif file_extension in ['xlsx', 'xls']:
                df = self._read_excel(file_content)
            else:
                return {
                    "status": "error",
                    "message": f"Formato no soportado: {file_extension}"
                }
            
            validation_result = self._validate_and_clean_dataframe(df)
            if validation_result["status"] == "error":
                return validation_result
                
            return {
                "status": "success",
                "data": validation_result["data"],
                "rows_processed": len(validation_result["data"])
            }
            
        except Exception as e:
            return {
                "status": "error",
                "message": f"Error procesando archivo: {str(e)}"
            }
    
    def _read_csv(self, content: bytes) -> pd.DataFrame:
        """Lee archivos CSV probando diferentes configuraciones"""
        content_io = io.BytesIO(content)
        encodings = ['utf-8', 'latin1', 'iso-8859-1', 'cp1252']
        delimiters = [',', ';', '|', '\t']
        
        for encoding in encodings:
            for delimiter in delimiters:
                try:
                    content_io.seek(0)
                    df = pd.read_csv(
                        content_io,
                        encoding=encoding,
                        delimiter=delimiter,
                        dtype=str,
                        keep_default_na=False
                    )
                    if self._check_required_columns(df):
                        return df
                except:
                    continue
                    
        raise ValueError("No se pudo leer el archivo CSV")

    def _read_excel(self, content: bytes) -> pd.DataFrame:
        """Lee archivos Excel"""
        try:
            df = pd.read_excel(
                io.BytesIO(content),
                dtype=str,
                keep_default_na=False
            )
            return df
        except Exception as e:
            raise ValueError(f"Error leyendo Excel: {str(e)}")

    def _validate_and_clean_dataframe(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Valida y limpia el DataFrame"""
        try:
            # Normalizar columnas
            df.columns = [col.upper().strip() for col in df.columns]
            
            # Verificar columnas requeridas
            if not self._check_required_columns(df):
                missing_cols = [col for col in self.required_columns if col not in df.columns]
                return {
                    "status": "error",
                    "message": f"Faltan columnas: {', '.join(missing_cols)}"
                }
            
            # Limpiar datos
            df = self._clean_data(df)
            
            return {
                "status": "success",
                "data": df
            }
            
        except Exception as e:
            return {
                "status": "error",
                "message": f"Error en validación: {str(e)}"
            }

    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Limpia y normaliza los datos"""
        # Reemplazar valores nulos
        df = df.replace(['', 'nan', 'NaN', 'none', 'None', 'NULL'], np.nan)
        
        # Limpiar espacios
        for col in df.columns:
            if df[col].dtype == object:
                df[col] = df[col].str.strip()
        
        # Normalizar campos específicos
        df['COD_POSTAL'] = df['COD_POSTAL'].str.zfill(5)
        df['NIF'] = df['NIF'].str.upper()
        df['URL'] = df['URL'].apply(self._normalize_url)
        
        return df

    def _normalize_url(self, url: Optional[str]) -> Optional[str]:
        """Normaliza URLs"""
        if pd.isna(url) or not isinstance(url, str):
            return None
        url = url.strip().lower()
        if not url.startswith(('http://', 'https://')):
            url = 'http://' + url
        return url

    def _check_required_columns(self, df: pd.DataFrame) -> bool:
        """Verifica columnas requeridas"""
        df_columns = [col.upper().strip() for col in df.columns]
        return all(col in df_columns for col in self.required_columns)
    
class EnterpriseApp:
    def __init__(self):
        """Inicializa la aplicación"""
        self.init_session_state()
        self.db = DatabaseManager()
        self.file_processor = FileProcessor(REQUIRED_COLUMNS)
        self.setup_paths()

    def setup_paths(self):
        """Configura rutas de recursos"""
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(current_dir)
        self.image_path = os.path.join(project_root, "images", "experian_dark.png")

    def init_session_state(self):
        """Inicializa estados de la aplicación"""
        initial_states = {
            "current_batch": None,         # Datos del lote actual
            "preview_data": None,          # Datos en previsualización
            "selected_rows": [],           # Filas seleccionadas para eliminar
            "delete_mode": None,           # Modo de eliminación
            "current_user_id": None        # ID de usuario actual
        }
        
        for key, value in initial_states.items():
            if key not in st.session_state:
                st.session_state[key] = value

    def render_sidebar(self):
        """Renderiza la barra lateral"""
        with st.sidebar:
            # Logo
            try:
                if os.path.exists(self.image_path):
                    st.image(self.image_path, width=200)
                else:
                    st.write("🏢 Sistema Empresarial de Análisis")
            except Exception:
                st.write("🏢 Sistema Empresarial de Análisis")

            st.title("Control Panel")

            # Sección de carga existente
            st.header("🔄 Cargar Existente")
            search_value = st.text_input(
                "Introduce Lote o Identificador",
                placeholder="Ej: BATCH_20250219 o usuario1",
                key="search_input"
            )
            
            if st.button("🔍 Buscar", key="btn_search", use_container_width=True):
                if search_value:
                    self.load_existing_data(search_value)
                else:
                    st.warning("Introduce un valor para buscar")

            st.divider()

            # Sección de nueva carga
            st.header("📤 Nueva Carga")
            user_id = st.text_input(
                "Identificador de Usuario",
                placeholder="Introduce tu identificador",
                key="user_id_input"
            )

            uploaded_file = st.file_uploader(
                "Seleccionar archivo (CSV/XLSX/XLS)",
                type=["csv", "xlsx", "xls"],
                help="Formatos soportados: CSV, Excel",
                key="file_uploader",
                disabled=not user_id
            )

            if not user_id and uploaded_file:
                st.warning("⚠️ Introduce un identificador")
            elif user_id and uploaded_file:
                batch_id = f"BATCH_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                st.info(f"📋 Número de lote: {batch_id}")
                
                if st.button("📋 Copiar Número de Lote", key="btn_copy_batch"):
                    pyperclip.copy(batch_id)
                    st.success("✅ Copiado!")
                
                st.session_state.current_user_id = user_id
                st.session_state.current_batch_id = batch_id
                self.handle_file_upload(uploaded_file)

    def handle_file_upload(self, file):
        """Procesa la carga de archivos"""
        try:
            with st.spinner("Procesando archivo..."):
                file_content = file.read()
                result = self.file_processor.process_file(file_content, file.name)
            
                if result["status"] == "error":
                    st.error(f"❌ {result['message']}")
                    return
            
                # Guardar datos en preview_data para previsualización
                st.session_state.preview_data = result["data"]
            
        except Exception as e:
            st.error(f"❌ Error en la carga del archivo: {str(e)}")

    def render_main_content(self):
        """Renderiza el contenido principal de la aplicación"""
        st.title("Sistema de Análisis Empresarial 🏢")
        
        # Tabs principales siempre visibles
        tab1, tab2, tab3, tab4, tab5 = st.tabs([
            "📊 Dashboard",
            "🔍 Consultas",
            "🌐 Web Scraping",
            "📈 Análisis",
            "⚙️ Gestión"
        ])

        # Mostrar datos según el estado actual
        if st.session_state.preview_data is not None:
            self.show_preview_data()
        elif st.session_state.current_batch is not None:
            self.show_loaded_data()

    def show_preview_data(self):
        """Muestra previsualización de datos con opciones de guardar/descartar"""
        df = st.session_state.preview_data
        
        # Panel de información
        col1, col2, col3 = st.columns([2, 2, 1])
        with col1:
            st.info(f"📦 Lote: {st.session_state.current_batch_id}")
        with col2:
            st.info(f"👤 Usuario: {st.session_state.current_user_id}")
        with col3:
            if st.button("📋 Copiar Lote", key="btn_copy_preview"):
                pyperclip.copy(st.session_state.current_batch_id)
                st.success("✅ Copiado!")

        # Mostrar datos
        st.dataframe(df, use_container_width=True)

        # Opciones de guardar/descartar
        col1, col2 = st.columns(2)
        with col1:
            if st.button("💾 Guardar en Base de Datos", key="btn_save"):
                self.save_to_database()
        with col2:
            if st.button("❌ Descartar", key="btn_discard"):
                st.session_state.preview_data = None
                st.session_state.current_batch_id = None
                st.rerun()

        # Mostrar dashboard si hay datos
        if df is not None:
            self.show_dashboard(df)

    def show_loaded_data(self):
        """Muestra datos cargados desde la base de datos"""
        df = st.session_state.current_batch['data']
        
        # Panel de información
        col1, col2, col3 = st.columns([2, 2, 1])
        with col1:
            st.info(f"📦 Lote: {st.session_state.current_batch['id']}")
        with col2:
            st.info(f"👤 Usuario: {st.session_state.current_batch['created_by']}")
        with col3:
            if st.button("📋 Copiar Lote", key="btn_copy_loaded"):
                pyperclip.copy(st.session_state.current_batch['id'])
                st.success("✅ Copiado!")

        # DataFrame con selección
        df_editable = df.copy()
        df_editable.insert(0, "Seleccionar", False)
        
        edited_df = st.data_editor(
            df_editable,
            hide_index=True,
            column_config={
                "Seleccionar": st.column_config.CheckboxColumn(
                    "Seleccionar",
                    help="Seleccionar filas para eliminar"
                )
            },
            use_container_width=True
        )

        # Botón para eliminar selección
        selected_indices = [i for i, selected in enumerate(edited_df["Seleccionar"]) if selected]
        if selected_indices:
            if st.button(f"🗑️ Eliminar {len(selected_indices)} registros seleccionados"):
                self.delete_selected_rows(selected_indices)

        # Mostrar dashboard
        self.show_dashboard(df)

    def load_existing_data(self, search_value: str):
        """Carga datos existentes por lote o identificador"""
        try:
            with st.spinner("Buscando datos..."):
                results = self.db.get_batch(search_value)
            
                if results is not None and not results.empty:
                    st.session_state.current_batch = {
                        "id": results['lote_id'].iloc[0],
                        "data": results,
                        "created_by": results['created_by'].iloc[0],
                        "total_records": len(results)
                    }
                    st.success(f"✅ Datos cargados: {len(results)} registros")
                else:
                    st.error("❌ No se encontraron datos")
            
        except Exception as e:
            st.error(f"❌ Error al cargar datos: {str(e)}")

    def save_to_database(self):
        """Guarda los datos en la base de datos"""
        try:
            with st.spinner("Guardando datos..."):
                result = self.db.save_batch(
                    st.session_state.preview_data,
                    st.session_state.current_batch_id,
                    st.session_state.current_user_id
                )
                
                if result["status"] == "success":
                    st.success("✅ Datos guardados exitosamente en la base de datos")
                    st.session_state.preview_data = None
                    st.session_state.current_batch_id = None
                    st.rerun()
                else:
                    st.error(f"❌ Error al guardar: {result.get('message', 'Error desconocido')}")
        
        except Exception as e:
            st.error(f"❌ Error guardando datos: {str(e)}")

    def delete_selected_rows(self, indices: List[int]):
        """Elimina las filas seleccionadas de la base de datos"""
        try:
            df = st.session_state.current_batch['data']
            rows_to_delete = df.iloc[indices]
            
            result = self.db.delete_records(
                rows_to_delete['cod_infotel'].tolist(),
                st.session_state.current_batch['id']
            )
            
            if result["status"] == "success":
                st.success(f"✅ {len(indices)} registros eliminados correctamente")
                # Actualizar DataFrame
                df = df.drop(indices)
                st.session_state.current_batch['data'] = df
                st.session_state.current_batch['total_records'] = len(df)
                st.rerun()
            else:
                st.error(f"❌ Error al eliminar registros: {result['message']}")
                
        except Exception as e:
            st.error(f"❌ Error durante la eliminación: {str(e)}")

    def show_dashboard(self, df: pd.DataFrame):
        """Muestra el dashboard con métricas y visualizaciones"""
        st.divider()
        st.subheader("📊 Dashboard")
        
        # Métricas principales
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Registros", len(df))
        with col2:
            total_with_web = df['URL'].notna().sum()
            st.metric("URLs Disponibles", total_with_web)
        with col3:
            unique_provinces = df['NOM_PROVINCIA'].nunique()
            st.metric("Provincias Únicas", unique_provinces)
        with col4:
            unique_cities = df['NOM_POBLACION'].nunique()
            st.metric("Poblaciones Únicas", unique_cities)

        # Gráficos
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Distribución por Provincia")
            province_counts = df['NOM_PROVINCIA'].value_counts().head(10)
            st.bar_chart(province_counts)
            
        with col2:
            st.subheader("Estado de URLs")
            url_status = df['URL'].notna().value_counts()
            fig, ax = plt.subplots()
            ax.pie(
                url_status.values,
                labels=['Con URL', 'Sin URL'],
                autopct='%1.1f%%'
            )
            st.pyplot(fig)

    def run(self):
        """Ejecuta la aplicación"""
        self.render_sidebar()
        self.render_main_content()


if __name__ == "__main__":
    app = EnterpriseApp()
    app.run()
    