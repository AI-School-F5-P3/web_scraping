# app.py

import streamlit as st
import pandas as pd
from datetime import datetime
import time
import pyperclip
import matplotlib.pyplot as plt
from agents import OrchestratorAgent, DBAgent, ScrapingAgent
from database import DatabaseManager
from scraping import ProWebScraper
from config import REQUIRED_COLUMNS, PROVINCIAS_ESPANA
import numpy as np
import re
from typing import Dict, Any, Optional
import io
import os
from pathlib import Path


class FileProcessor:
    def __init__(self, required_columns: list):
        self.required_columns = [col.upper() for col in required_columns]
        
    def process_file(self, file_content: bytes, filename: str) -> Dict[str, Any]:
        """
        Procesa el archivo subido y valida su estructura
        """
        try:
            print(f"Iniciando procesamiento de archivo: {filename}")  # Debug log
            
            # Determinar el tipo de archivo por su extensión
            file_extension = filename.lower().split('.')[-1]
            print(f"Extensión del archivo: {file_extension}")  # Debug log
            
            # Leer el archivo según su tipo
            if file_extension == 'csv':
                print("Procesando como CSV")  # Debug log
                df = self._read_csv(file_content)
            elif file_extension in ['xlsx', 'xls']:
                print("Procesando como Excel")  # Debug log
                df = self._read_excel(file_content)
            else:
                print(f"Extensión no soportada: {file_extension}")  # Debug log
                return {
                    "status": "error",
                    "message": f"Formato de archivo no soportado: {file_extension}"
                }
            
            print(f"Archivo leído exitosamente. Dimensiones: {df.shape}")  # Debug log
            
            # Validar y limpiar el DataFrame
            print("Iniciando validación y limpieza")  # Debug log
            validation_result = self._validate_and_clean_dataframe(df)
            print(f"Resultado de validación: {validation_result['status']}")  # Debug log
            
            if validation_result["status"] == "error":
                return validation_result
                
            return {
                "status": "success",
                "data": validation_result["data"],
                "rows_processed": len(validation_result["data"]),
                "columns": validation_result["data"].columns.tolist()
            }
            
        except Exception as e:
            import traceback
            error_details = traceback.format_exc()
            print("Error detallado en process_file:", error_details)  # Debug log
            
            return {
                "status": "error",
                "message": f"Error procesando archivo: {str(e)}",
                "details": error_details
            }
    
    def _read_csv(self, content: bytes) -> pd.DataFrame:
        """
        Lee archivos CSV probando diferentes encodings y delimitadores
        """
        content_io = io.BytesIO(content)
        encodings = ['utf-8', 'latin1', 'iso-8859-1', 'cp1252']
        delimiters = [',', ';', '|', '\t']
        
        for encoding in encodings:
            for delimiter in delimiters:
                try:
                    # Reiniciar el puntero del BytesIO
                    content_io.seek(0)
                    df = pd.read_csv(
                        content_io,
                        encoding=encoding,
                        delimiter=delimiter,
                        dtype=str,
                        keep_default_na=False
                    )
                    # Si encontramos las columnas requeridas, retornamos el DataFrame
                    if self._check_required_columns(df):
                        return df
                except:
                    continue
                    
        raise ValueError("No se pudo leer el archivo CSV con ninguna configuración")
    
    def _read_excel(self, content: bytes) -> pd.DataFrame:
        """
        Lee archivos Excel (XLS/XLSX)
        """
        try:
            df = pd.read_excel(
                io.BytesIO(content),
                dtype=str,
                keep_default_na=False
            )
            return df
        except Exception as e:
            raise ValueError(f"Error leyendo archivo Excel: {str(e)}")
    
    def _check_required_columns(self, df: pd.DataFrame) -> bool:
        """
        Verifica si el DataFrame tiene las columnas requeridas
        """
        df_columns = [col.upper().strip() for col in df.columns]
        return all(col in df_columns for col in self.required_columns)
    
    def _validate_and_clean_dataframe(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Valida y limpia el DataFrame
        """
        try:
            # Normalizar nombres de columnas
            df.columns = [col.upper().strip() for col in df.columns]
            
            # Verificar columnas requeridas
            if not self._check_required_columns(df):
                missing_cols = [col for col in self.required_columns if col not in df.columns]
                return {
                    "status": "error",
                    "message": f"Faltan columnas requeridas: {', '.join(missing_cols)}"
                }
            
            # Limpiar y validar datos
            df = self._clean_data(df)
            
            # Validar NIF
            invalid_nifs = df[~df['NIF'].apply(self._validate_nif)]['NIF'].tolist()
            if invalid_nifs:
                return {
                    "status": "error",
                    "message": f"NIFs inválidos encontrados: {', '.join(invalid_nifs[:5])}..."
                }
            
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
        """
        Limpia y normaliza los datos del DataFrame
        """
        # Reemplazar valores nulos o vacíos
        df = df.replace(['', 'nan', 'NaN', 'none', 'None', 'NULL'], np.nan)
        
        # Limpiar espacios en blanco
        for col in df.columns:
            if df[col].dtype == object:
                df[col] = df[col].str.strip()
        
        # Normalizar campos específicos
        df['COD_POSTAL'] = df['COD_POSTAL'].str.zfill(5)
        df['NIF'] = df['NIF'].str.upper()
        
        # Normalizar URLs
        df['URL'] = df['URL'].apply(self._normalize_url)
        
        return df
    
    def _normalize_url(self, url: Optional[str]) -> Optional[str]:
        """
        Normaliza URLs
        """
        if pd.isna(url) or not isinstance(url, str):
            return None
            
        url = url.strip().lower()
        if not url.startswith(('http://', 'https://')):
            url = 'http://' + url
            
        return url
    
    def _validate_nif(self, nif: str) -> bool:
        """
        Valida el formato del NIF/CIF español
        """
        if pd.isna(nif):
            return False
            
        nif = nif.upper().strip()
        
        # Patrones para diferentes tipos de identificadores fiscales
        patterns = {
            'DNI': r'^[0-9]{8}[A-Z]$',
            'NIE': r'^[XYZ][0-9]{7}[A-Z]$',
            'CIF': r'^[ABCDEFGHJKLMNPQRSUVW][0-9]{7}[0-9A-J]$'
        }
        
        return any(re.match(pattern, nif) for pattern in patterns.values())


class EnterpriseApp:
    def __init__(self):
        self.init_session_state()
        self.db = DatabaseManager()
        self.scraper = ProWebScraper()
        self.setup_agents()
        self.file_processor = FileProcessor(REQUIRED_COLUMNS)
        self.setup_paths()
        st.set_page_config(
            page_title="Sistema Empresarial de Análisis",
            page_icon="🏢",
            layout="wide",
            initial_sidebar_state="expanded"
        )

    def setup_paths(self):
        """Configura las rutas de la aplicación"""
        # Obtener la ruta del script actual
        current_dir = os.path.dirname(os.path.abspath(__file__))
        # Subir un nivel para llegar a la raíz del proyecto
        project_root = os.path.dirname(current_dir)
        # Definir la ruta de la imagen
        self.image_path = os.path.join(project_root, "images", "experian_dark.png")
        
    def init_session_state(self):
        """Inicializa variables de estado"""
        if "current_batch" not in st.session_state:
            st.session_state.current_batch = None
        if "current_user_id" not in st.session_state:
            st.session_state.current_user_id = None
        if "current_batch_id" not in st.session_state:
            st.session_state.current_batch_id = None
        if "processing_status" not in st.session_state:
            st.session_state.processing_status = None
        if "last_query" not in st.session_state:
            st.session_state.last_query = None
        if "show_sql" not in st.session_state:
            st.session_state.show_sql = False
        if "file_processing_errors" not in st.session_state:
            st.session_state.file_processing_errors = []
        if "preview_data" not in st.session_state:
            st.session_state.preview_data = None
        if "uploaded_file" not in st.session_state:
            st.session_state.uploaded_file = None

    def setup_agents(self):
        """Configuración de agentes inteligentes"""
        self.orchestrator = OrchestratorAgent()
        self.db_agent = DBAgent()
        self.scraping_agent = ScrapingAgent()

    def render_sidebar(self):
        """Renderiza la barra lateral con opciones de carga de archivo"""
        with st.sidebar:
            try:
                if os.path.exists(self.image_path):
                    st.image(self.image_path, width=200)
                else:
                    st.write("🏢 Sistema Empresarial de Análisis")
            except Exception as e:
                st.write("🏢 Sistema Empresarial de Análisis")
        
            st.title("Control Panel")
        
            # Sección para cargar lotes existentes
            st.header("🔄 Cargar Existente")
            col1, col2 = st.columns([3, 1])
            with col1:
                search_value = st.text_input(
                    "Introduce Lote o Identificador",
                    placeholder="Ej: BATCH_20250219 o usuario1",
                    key="search_input"
                )
            with col2:
                if st.button("🔍 Buscar", key="btn_search"):
                    if search_value:
                        self.load_existing_data(search_value)
                    else:
                        st.warning("Introduce un valor para buscar")
        
            st.divider()
        
            # Sección para nueva carga
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
                disabled=not user_id  # Deshabilitar si no hay identificador
            )
        
            if not user_id and uploaded_file:
                st.warning("⚠️ Introduce un identificador antes de subir archivos")
        
            if user_id and uploaded_file:
                # Generar y mostrar número de lote
                batch_id = f"BATCH_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                st.info(f"📋 Número de lote: {batch_id}")
            
                # Botón para copiar el número de lote
                if st.button("📋 Copiar Número de Lote", key="btn_copy"):
                    pyperclip.copy(batch_id)
                    st.success("✅ Número de lote copiado!")
            
                # Guardar en session state y procesar
                st.session_state.current_user_id = user_id
                st.session_state.current_batch_id = batch_id
                st.session_state.uploaded_file = uploaded_file

    def load_existing_data(self, search_value: str):
        """Carga datos existentes por lote o identificador"""
        try:
            with st.spinner("Buscando datos..."):
                # Consultar la base de datos
                query = """
                SELECT DISTINCT ON (s.cod_infotel)
                    s.*
                FROM sociedades s
                WHERE (s.lote_id = %s OR s.created_by = %s)
                    AND s.deleted = FALSE
                ORDER BY s.cod_infotel, s.created_at DESC
                """
        
                results = self.db.execute_query(query, (search_value, search_value), return_df=True)
        
                if results is not None and not results.empty:
                    # Normalizar nombres de columnas a mayúsculas
                    results.columns = [col.upper() for col in results.columns]
                
                    # Guardar en session state
                    st.session_state.current_batch = {
                        "id": results['LOTE_ID'].iloc[0],
                        "data": results,
                        "total_records": len(results),
                        "timestamp": datetime.now()
                    }
                    st.session_state.current_user_id = results['CREATED_BY'].iloc[0]
            
                    st.success(f"✅ Datos cargados: {len(results)} registros")
                else:
                    st.error("❌ No se encontraron datos con ese valor")
            
        except Exception as e:
            st.error(f"❌ Error al cargar datos: {str(e)}")

    def handle_file_upload(self, file):
        """Procesa la carga de archivos"""
        try:
            with st.spinner("Procesando archivo..."):
                file_content = file.read()
                result = self.file_processor.process_file(file_content, file.name)
            
                if result["status"] == "error":
                    st.error(f"❌ {result['message']}")
                    return
            
                # Guardar datos en session_state para previsualización
                st.session_state.preview_data = result["data"]
            
        except Exception as e:
            st.error(f"❌ Error en la carga del archivo: {str(e)}")
            st.session_state.file_processing_errors.append({
                "timestamp": datetime.now(),
                "error": str(e),
                "file": file.name
            })

    def _show_data_preview(self, df):
        """Muestra previsualización de los datos y opciones de guardado"""
        st.subheader("📊 Previsualización de Datos")
    
        # Mostrar resumen
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Registros", len(df))
        with col2:
            st.metric("Empresas con URL", df['URL'].notna().sum())
        with col3:
            st.metric("Provincias", df['NOM_PROVINCIA'].nunique())
    
        # Mostrar datos
        st.dataframe(df, use_container_width=True)
    
        # Opciones
        col1, col2 = st.columns(2)
    
        with col1:
            if st.button("💾 Guardar en Base de Datos", key="btn_save_db"):
                self._save_to_database(df)
            
        with col2:
            if st.button("❌ Descartar", key="btn_discard"):
                if st.session_state.current_batch:
                    if self.delete_data(st.session_state.current_batch["id"]):
                        st.success("✅ Datos eliminados correctamente")
                        st.rerun()
                else:
                    st.session_state.preview_data = None
                    st.rerun()

    def _save_to_database(self, df):
        """Guarda los datos en la base de datos"""
        try:
            with st.spinner("Guardando datos..."):
                # Generar ID de lote
                batch_id = f"BATCH_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
                # Verificar que el DataFrame tenga todas las columnas necesarias
                required_columns = [
                    'COD_INFOTEL', 'NIF', 'RAZON_SOCIAL', 'DOMICILIO',
                    'COD_POSTAL', 'NOM_POBLACION', 'NOM_PROVINCIA', 'URL'
                ]
            
                missing_cols = [col for col in required_columns if col not in df.columns]
                if missing_cols:
                    st.error(f"❌ Faltan columnas requeridas: {', '.join(missing_cols)}")
                    return
            
                # Preparar los datos para guardar
                save_df = df.copy()
            
                # Asegurar que todas las columnas estén presentes y con valores por defecto
                for col in ['URL_EXISTS', 'URL_LIMPIA', 'URL_STATUS', 'URL_STATUS_MENSAJE',
                        'TELEFONO_1', 'TELEFONO_2', 'TELEFONO_3', 'FACEBOOK', 'TWITTER',
                        'LINKEDIN', 'INSTAGRAM', 'E_COMMERCE']:
                    if col not in save_df.columns:
                        save_df[col] = None
            
                # Guardar en base de datos
                db_result = self.db.save_batch(save_df, batch_id, st.session_state.get("user", "streamlit_user"))
            
                if db_result["status"] == "success":
                    st.session_state.current_batch = {
                        "id": batch_id,
                        "data": df,
                        "total_records": len(df),
                        "timestamp": datetime.now()
                    }
                    st.success(f"✅ Datos guardados exitosamente: {len(df)} registros")
                    st.session_state.preview_data = None  # Limpiar previsualización
                
                    # Mostrar resumen completo
                    self._show_upload_summary(df)
                else:
                    st.error(f"❌ Error al guardar: {db_result.get('message', 'Error desconocido')}")
                
        except Exception as e:
            st.error(f"❌ Error guardando datos: {str(e)}")
            # Log detallado del error
            print(f"Error detallado: {str(e)}")

    def _show_upload_summary(self, df: pd.DataFrame):
        """Muestra un resumen de los datos cargados"""
        with st.expander("📊 Resumen de la carga", expanded=True):
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Total Registros", len(df))
                st.metric("Empresas con URL", df['URL'].notna().sum())
                
            with col2:
                st.metric("Provincias Únicas", df['NOM_PROVINCIA'].nunique())
                provinces_count = df['NOM_PROVINCIA'].value_counts().head(5)
                st.write("Top 5 Provincias:")
                st.write(provinces_count)
                
            with col3:
                st.metric("Códigos Postales Únicos", df['COD_POSTAL'].nunique())
                st.metric("NIFs Únicos", df['NIF'].nunique())

    def render_main_content(self):
        """Renderiza el contenido principal"""
        st.title("Sistema de Análisis Empresarial 🏢")
    
        # Panel informativo cuando hay datos cargados
        if st.session_state.current_batch:
            with st.container():
                info_col1, info_col2, info_col3 = st.columns([2, 2, 1])
                with info_col1:
                    st.info(f"📦 Lote actual: {st.session_state.current_batch['id']}")
                with info_col2:
                    st.info(f"👤 Identificador: {st.session_state.current_user_id}")
                with info_col3:
                    if st.button("📋 Copiar Lote", key="btn_copy_current"):
                        pyperclip.copy(st.session_state.current_batch['id'])
                        st.success("✅ Copiado!")
    
    # Tabs principales
        tab1, tab2, tab3, tab4, tab5 = st.tabs([
            "📊 Dashboard",
            "🔍 Consultas",
            "🌐 Web Scraping",
            "📈 Análisis",
            "⚙️ Gestión"
        ])
    
        # Procesar archivo si hay uno nuevo
        if hasattr(st.session_state, 'uploaded_file') and st.session_state.uploaded_file:
            self.handle_file_upload(st.session_state.uploaded_file)
            # Limpiar el archivo subido después de procesarlo
            st.session_state.uploaded_file = None
    
        # Contenido de las tabs
        with tab1:
            if st.session_state.preview_data is not None:
                self._show_data_preview(st.session_state.preview_data)
            else:
                self.render_dashboard()
            
        with tab2:
            self.render_queries()
        with tab3:
            self.render_scraping()
        with tab4:
            self.render_analysis()
        with tab5:
            self.render_management()

    def render_dashboard(self):
        """Renderiza el dashboard con estadísticas"""
        if not st.session_state.current_batch:
            st.info("👆 Carga un archivo para ver las estadísticas")
            return
        
        # Estadísticas generales
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Registros", f"{st.session_state.current_batch['total_records']:,}")
        
        total_with_web = len(st.session_state.current_batch['data'][
            st.session_state.current_batch['data']['URL'].notna()
        ])
        with col2:
            st.metric("Con Web", f"{total_with_web:,}")
        
        unique_provinces = st.session_state.current_batch['data']['NOM_PROVINCIA'].nunique()
        with col3:
            st.metric("Provincias", unique_provinces)
        
        with col4:
            st.metric("Lote", st.session_state.current_batch['id'])
        
        # Gráficos
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Distribución por Provincia")
            prov_counts = st.session_state.current_batch['data']['NOM_PROVINCIA'].value_counts()
            st.bar_chart(prov_counts)
            
        with col2:
            st.subheader("Estado de URLs")
            url_status = st.session_state.current_batch['data']['URL'].notna().value_counts()
            if not url_status.empty:
                fig, ax = plt.subplots()
                labels = ['URLs Válidas' if x else 'URLs Faltantes' for x in url_status.index]
                ax.pie(url_status.values, labels=labels, autopct='%1.1f%%', startangle=90)
                ax.axis('equal')  # Equal aspect ratio ensures the pie chart is circular
                st.pyplot(fig)
            else:
                st.write("No hay datos para mostrar")

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
                if st.session_state.show_sql:
                    st.code(st.session_state.last_query["sql"], language="sql")
                st.dataframe(
                    st.session_state.last_query["results"],
                    use_container_width=True
                )

    def render_scraping(self):
        """Renderiza la sección de web scraping"""
        st.subheader("🌐 Web Scraping")
        
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

    def render_management(self):
        """Renderiza la sección de gestión"""
        st.subheader("⚙️ Gestión de Datos")
        
        if st.session_state.current_batch:
            col1, col2 = st.columns(2)
            
            with col1:
                st.info(f"Lote actual: {st.session_state.current_batch['id']}")
                st.metric("Registros", st.session_state.current_batch['total_records'])
                
            with col2:
                st.warning("⚠️ Zona de Peligro")
                self.delete_current_batch()
        else:
            st.info("No hay datos cargados actualmente")

    def delete_current_batch(self):
        """Elimina el lote actual de la base de datos"""
        if not st.session_state.current_batch:
            st.warning("No hay datos cargados para eliminar")
            return
        
        try:
            # Confirmar eliminación
            if st.button("🗑️ Eliminar Datos Actuales", key="btn_delete_batch"):
                # Usar columns para mejor organización visual
                col1, col2 = st.columns([1, 3])
                with col1:
                    confirm = st.checkbox("¿Estás seguro?", key="confirm_delete")
                with col2:
                    if confirm:
                        if st.button("✔️ Confirmar Eliminación", key="btn_confirm_delete"):
                            with st.spinner("Eliminando datos..."):
                                result = self.db.delete_batch(st.session_state.current_batch["id"])
                            
                                if result["status"] == "success":
                                    st.success("✅ Datos eliminados correctamente")
                                    # Limpiar todos los estados relacionados
                                    st.session_state.current_batch = None
                                    st.session_state.preview_data = None
                                    st.session_state.current_user_id = None
                                    st.rerun()
                                else:
                                    st.error(f"❌ Error al eliminar: {result.get('message', 'Error desconocido')}")
                                
        except Exception as e:
            st.error(f"❌ Error durante la eliminación: {str(e)}")

    def process_query(self, query: str):
        """Procesa consultas en lenguaje natural"""
        try:
            with st.spinner("Procesando consulta..."):
                # Generar SQL
                query_info = self.db_agent.generate_query(query)
                
                # Ejecutar consulta
                results = self.db.execute_query(
                    query_info["query"],
                    return_df=True
                )
                
                # Guardar resultados
                st.session_state.last_query = {
                    "sql": query_info["query"],
                    "results": results
                }
                
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

    def show_geographic_analysis(self):
        """Muestra análisis geográfico de los datos"""
        df = st.session_state.current_batch['data']
        
        st.subheader("📍 Distribución Geográfica")
        
        # Distribución por provincia
        province_dist = df['NOM_PROVINCIA'].value_counts()
        st.bar_chart(province_dist)
        
        # Mapa de calor por código postal
        st.subheader("🗺️ Concentración por Código Postal")
        postal_dist = df['COD_POSTAL'].value_counts().head(20)
        st.bar_chart(postal_dist)

    def show_ecommerce_analysis(self):
        """Muestra análisis de E-commerce"""
        df = st.session_state.current_batch['data']
        
        st.subheader("🛍️ Análisis de E-commerce")
        
        total_urls = df['URL'].notna().sum()
        ecommerce_count = df['E_COMMERCE'].fillna(False).sum()
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.metric("Total URLs", total_urls)
            st.metric("Con E-commerce", ecommerce_count)
            
        with col2:
            if total_urls > 0:
                ecommerce_ratio = (ecommerce_count / total_urls) * 100
                st.metric("% E-commerce", f"{ecommerce_ratio:.1f}%")

    def show_digital_presence_analysis(self):
        """Muestra análisis de presencia digital"""
        df = st.session_state.current_batch['data']
        
        st.subheader("🌐 Presencia Digital")
        
        # Análisis de redes sociales
        social_columns = ['FACEBOOK', 'TWITTER', 'LINKEDIN', 'INSTAGRAM']
        social_presence = {
            network: df[network].notna().sum()
            for network in social_columns
        }
        
        st.bar_chart(social_presence)

    def show_contactability_analysis(self):
        """Muestra análisis de contactabilidad"""
        df = st.session_state.current_batch['data']
        
        st.subheader("📞 Análisis de Contactabilidad")
        
        # Conteo de medios de contacto
        contact_methods = {
            'Teléfono 1': df['TELEFONO_1'].notna().sum(),
            'Teléfono 2': df['TELEFONO_2'].notna().sum(),
            'Teléfono 3': df['TELEFONO_3'].notna().sum(),
            'Web': df['URL'].notna().sum(),
            'Redes Sociales': df[['FACEBOOK', 'TWITTER', 'LINKEDIN', 'INSTAGRAM']].notna().any(axis=1).sum()
        }
        
        st.bar_chart(contact_methods)

    def apply_filters(self, provincia: str, has_web: bool, has_ecommerce: bool):
        """Aplica filtros a los datos actuales"""
        try:
            df = st.session_state.current_batch['data'].copy()
            
            if provincia != "Todas":
                df = df[df['NOM_PROVINCIA'] == provincia]
                
            if has_web:
                df = df[df['URL'].notna()]
                
            if has_ecommerce:
                df = df[df['E_COMMERCE'] == True]
                
            st.session_state.current_batch['filtered_data'] = df
            st.success(f"Filtros aplicados. Mostrando {len(df)} registros.")
            
        except Exception as e:
            st.error(f"Error aplicando filtros: {str(e)}")

    def run(self):
        """Ejecuta la aplicación"""
        self.render_sidebar()
        self.render_main_content()


if __name__ == "__main__":
    app = EnterpriseApp()
    app.run()