#!/usr/bin/env python3
# dashboard.py
import streamlit as st
import pandas as pd
import numpy as np
import time
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import json
from task_manager import TaskManager
from database_supabase import SupabaseDatabaseManager
from redis_config import REDIS_QUEUE_PROCESSING, REDIS_QUEUE_PENDING, REDIS_QUEUE_COMPLETED, REDIS_QUEUE_FAILED
from task import Task

# Estilos CSS
st.markdown("""
<style>
    .metric-card {
        background-color: #f9f9f9;
        border-radius: 10px;
        padding: 15px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        text-align: center;
    }
    .metric-value {
        font-size: 36px;
        font-weight: bold;
        margin: 10px 0;
    }
    .metric-label {
        font-size: 14px;
        color: #666;
    }
    .success-metric {
        color: #28a745;
    }
    .warning-metric {
        color: #ffc107;
    }
    .danger-metric {
        color: #dc3545;
    }
</style>
""", unsafe_allow_html=True)

class ScrapingDashboard:
    def __init__(self, use_sidebar=True):
        self.task_manager = TaskManager()
        self.db = SupabaseDatabaseManager()
        self.use_sidebar = use_sidebar
        
        # Inicializar estado
        if 'last_refresh' not in st.session_state:
            st.session_state.last_refresh = datetime.now()
        
        if 'refresh_counter' not in st.session_state:
            st.session_state.refresh_counter = 0
            
        if 'auto_refresh' not in st.session_state:
            st.session_state.auto_refresh = True
            
        if 'reset_confirmation' not in st.session_state:
            st.session_state.reset_confirmation = False
        
        if 'history' not in st.session_state:
            st.session_state.history = {
                'timestamps': [],
                'pending': [],
                'processing': [],
                'completed': [],
                'failed': []
            }
    
    def increment_refresh_counter(self):
        st.session_state.refresh_counter += 1
        st.session_state.last_refresh = datetime.now()
    
    def toggle_auto_refresh(self):
        st.session_state.auto_refresh = not st.session_state.auto_refresh
    
    def toggle_reset_confirmation(self):
        st.session_state.reset_confirmation = not st.session_state.reset_confirmation
        
    def reset_queues(self):
        self.task_manager.reset_queues()
        st.session_state.reset_confirmation = False
        self.increment_refresh_counter()
    
    def get_queue_stats(self):
        """Obtiene estadísticas de las colas"""
        try:
            return self.task_manager.get_queue_stats()
        except Exception as e:
            # Devolver valores predeterminados en caso de error
            print(f"Error getting queue stats: {str(e)}")
            return {"pending": 0, "processing": 0, "completed": 0, "failed": 0}
    
    def get_progress_data(self):
        """Obtiene datos de progreso"""
        # Obtener total de empresas
        total_query = "SELECT COUNT(*) FROM sociedades"
        processed_query = "SELECT COUNT(*) FROM sociedades WHERE processed = TRUE"
        
        total = self.db.execute_query(total_query)
        processed = self.db.execute_query(processed_query)

        # Calcular progreso - asegurarse de que los valores son números
        if isinstance(total, (int, float)) and isinstance(processed, (int, float)):
            progress = (processed / total) * 100 if total > 0 else 0
        else:
            # Manejar caso donde los valores no son numéricos
            total = 0
            processed = 0
            progress = 0    
        
        return {
            'total': total,
            'processed': processed,
            'progress': progress
        }
    
    def get_active_workers(self):
        """Obtiene la lista de workers activos desde Redis"""
        try:
            # En Redis, los workers activos tendrían un heartbeat
            # Buscar todos los heartbeats activos
            active_workers = {}
            
            # Patrón para buscar todos los heartbeats de tareas
            heartbeat_keys = self.task_manager.redis.keys("task:*:heartbeat")
            
            if not heartbeat_keys:
                return []
            
            # Para cada heartbeat, obtener la tarea correspondiente
            for key in heartbeat_keys:
                # Extraer el task_id del patrón "task:{task_id}:heartbeat"
                task_id = key.split(':')[1]
                
                # Buscar esta tarea en la cola de procesamiento
                processing_queue = self.task_manager.redis.lrange(REDIS_QUEUE_PROCESSING, 0, -1)
                
                for task_json in processing_queue:
                    task = Task.from_json(task_json)
                    
                    if task.task_id == task_id:
                        worker_id = task.worker_id
                        
                        if worker_id not in active_workers:
                            active_workers[worker_id] = {
                                'worker_id': worker_id,
                                'tasks': 0,
                                'last_update': time.time(),
                                'last_company': task.company_data.get('razon_social', 'Desconocida')
                            }
                        
                        active_workers[worker_id]['tasks'] += 1
                        
                        # Actualizar si esta tarea es más reciente
                        if task.started_at > active_workers[worker_id]['last_update']:
                            active_workers[worker_id]['last_update'] = task.started_at
                            active_workers[worker_id]['last_company'] = task.company_data.get('razon_social', 'Desconocida')
            
            # Convertir el diccionario a una lista de registros
            workers_list = list(active_workers.values())
            
            # Si la lista está vacía, intentar un enfoque alternativo:
            # Revisar todas las tareas en procesamiento para extraer workers
            if not workers_list:
                processing_tasks = self.task_manager.redis.lrange(REDIS_QUEUE_PROCESSING, 0, -1)
                processing_workers = {}
                
                for task_json in processing_tasks:
                    task = Task.from_json(task_json)
                    
                    if task.worker_id:
                        if task.worker_id not in processing_workers:
                            processing_workers[task.worker_id] = {
                                'worker_id': task.worker_id,
                                'tasks': 0,
                                'last_update': task.started_at or time.time(),
                                'last_company': task.company_data.get('razon_social', 'Desconocida')
                            }
                        
                        processing_workers[task.worker_id]['tasks'] += 1
                
                workers_list = list(processing_workers.values())
            
            # También podemos obtener información de métricas adicionales
            # Como los tiempos de procesamiento promedio por worker
            
            return workers_list
        
        except Exception as e:
            print(f"Error al obtener workers activos desde Redis: {str(e)}")
            return []
    
    def get_processing_rates(self):
        """Obtiene tasas de procesamiento por worker"""
        query = """
        SELECT 
            worker_id,
            DATE_TRUNC('hour', fecha_actualizacion) as hour,
            COUNT(*) as count
        FROM sociedades
        WHERE fecha_actualizacion > NOW() - INTERVAL '24 hours'
            AND worker_id IS NOT NULL
        GROUP BY worker_id, DATE_TRUNC('hour', fecha_actualizacion)
        ORDER BY hour
        """
        
        df = self.db.execute_query(query, return_df=True)
        if df is None or df.empty:
            return pd.DataFrame()
        
        return df
    
    def get_success_rate(self):
        """Obtiene tasa de éxito general"""
        query = """
        SELECT
            COUNT(*) as total,
            COUNT(CASE WHEN url_exists = TRUE THEN 1 END) as success,
            COUNT(CASE WHEN url_exists = FALSE THEN 1 END) as failed
        FROM sociedades
        WHERE processed = TRUE
        """
        
        df = self.db.execute_query(query, return_df=True)

        # Si df no es un DataFrame, forzarlo a uno vacío
        if not isinstance(df, pd.DataFrame):
            df = pd.DataFrame(columns=['total', 'success', 'failed'])

        if df.empty:
            return {'total': 0, 'success': 0, 'failed': 0, 'rate': 0}
        
        # Extrae valores del dataframe
        total = df.iloc[0]['total'] if 'total' in df.columns else 0
        success = df.iloc[0]['success'] if 'success' in df.columns else 0
        failed = df.iloc[0]['failed'] if 'failed' in df.columns else 0
        
        # Calcula la tasa
        rate = (success / total * 100) if total > 0 else 0
        
        return {
            'total': total,
            'success': success,
            'failed': failed,
            'rate': rate
        }
    
    def get_recent_results(self, limit=20):
        """Obtiene resultados recientes"""
        query = f"""
        SELECT 
            cod_infotel, razon_social, url, url_valida, url_exists,
            telefono_1, fecha_actualizacion, worker_id, e_commerce
        FROM sociedades
        WHERE processed = TRUE
        ORDER BY fecha_actualizacion DESC
        LIMIT {limit}
        """
        
        df = self.db.execute_query(query, return_df=True)
        if df is None or df.empty:
            return pd.DataFrame()
        
        return df

    def reload_pending_tasks(self, batch_size=None):
        """Recarga todas las tareas pendientes desde la base de datos sin límite predeterminado"""
        try:
            # Consulta base para obtener empresas no procesadas
            query = """
            SELECT cod_infotel, razon_social, url 
            FROM sociedades 
            WHERE processed = FALSE
            """
            
            # Añadir LIMIT solo si se especifica batch_size
            if batch_size:
                query += f" LIMIT {batch_size}"
                
            # Obtener los datos
            pending_tasks = self.db.execute_query(query, return_df=True)
                
            if pending_tasks is None or pending_tasks.empty:
                st.session_state.task_reload_message = "No hay tareas pendientes para recargar"
                self.increment_refresh_counter()
                return 0
            
            # Convertir DataFrame a lista de diccionarios
            companies_list = pending_tasks.to_dict('records')
            total_found = len(companies_list)
            
            # Get current pending tasks to avoid duplicates
            pending_tasks_json = self.task_manager.redis.lrange(REDIS_QUEUE_PENDING, 0, -1)
            current_company_ids = []
            for task_json in pending_tasks_json:
                try:
                    task_data = json.loads(task_json)
                    if 'company_id' in task_data:
                        current_company_ids.append(task_data['company_id'])
                except:
                    pass
            
            # Filter out companies already in queue
            new_companies = [
                company for company in companies_list 
                if company['cod_infotel'] not in current_company_ids
            ]
            
            # Procesar en lotes si hay muchas empresas para evitar sobrecarga
            total_enqueued = 0
            processing_batch_size = 5000  # Tamaño de lote para procesamiento interno
            
            for i in range(0, len(new_companies), processing_batch_size):
                process_batch = new_companies[i:i+processing_batch_size]
                enqueued = self.task_manager.enqueue_tasks(process_batch)
                total_enqueued += enqueued
                # Pequeña pausa para evitar sobrecargar Redis
                time.sleep(0.1)
            
            # Mensaje de éxito
            message = f"Tareas recargadas: {total_enqueued} nuevas de {total_found} encontradas"
            st.session_state.task_reload_message = message
            
            self.increment_refresh_counter()
            return total_enqueued
                
        except Exception as e:
            st.session_state.task_reload_message = f"Error al recargar tareas: {str(e)}"
            import traceback
            traceback.print_exc()
            self.increment_refresh_counter()
            return -1
        
    def render_metrics_section(self):
        """Renderiza sección de métricas principales"""
        st.markdown("## 📊 Métricas en Tiempo Real")
        
        # Obtener datos
        queue_stats = self.get_queue_stats()
        success_rate = self.get_success_rate()
        
        # Actualizar historial para gráficos
        now = datetime.now()
        st.session_state.history['timestamps'].append(now)
        st.session_state.history['pending'].append(queue_stats['pending'])
        st.session_state.history['processing'].append(queue_stats['processing'])
        st.session_state.history['completed'].append(queue_stats['completed'])
        st.session_state.history['failed'].append(queue_stats['failed'])
        
        # Limitar historial a últimas 100 muestras
        if len(st.session_state.history['timestamps']) > 100:
            st.session_state.history['timestamps'] = st.session_state.history['timestamps'][-100:]
            st.session_state.history['pending'] = st.session_state.history['pending'][-100:]
            st.session_state.history['processing'] = st.session_state.history['processing'][-100:]
            st.session_state.history['completed'] = st.session_state.history['completed'][-100:]
            st.session_state.history['failed'] = st.session_state.history['failed'][-100:]
        
        # Crear fila de métricas
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown(
                f"""
                <div class="metric-card">
                    <div class="metric-label">Pendientes</div>
                    <div class="metric-value warning-metric">{queue_stats['pending']}</div>
                    <div>En cola</div>
                </div>
                """, 
                unsafe_allow_html=True
            )
        
        with col2:
            st.markdown(
                f"""
                <div class="metric-card">
                    <div class="metric-label">En Procesamiento</div>
                    <div class="metric-value">{queue_stats['processing']}</div>
                    <div>En curso</div>
                </div>
                """, 
                unsafe_allow_html=True
            )
        
        with col3:
            st.markdown(
                f"""
                <div class="metric-card">
                    <div class="metric-label">Completadas</div>
                    <div class="metric-value success-metric">{queue_stats['completed']}</div>
                    <div>Éxitos</div>
                </div>
                """, 
                unsafe_allow_html=True
            )
    def render_workers_section(self):
        """Renderiza sección de workers activos con datos de Redis"""
        st.markdown("## 👷 Workers Activos")
        
        workers = self.get_active_workers()
        
        if workers:
            # Crear DataFrame
            workers_df = pd.DataFrame(workers)
            
            # Formatear columna de tiempo
            if 'last_update' in workers_df.columns:
                # Calcular hace cuánto tiempo fue la última actividad
                now = time.time()
                
                def format_time_ago(timestamp):
                    if not timestamp:
                        return "No disponible"
                    
                    try:
                        seconds = now - float(timestamp)
                        
                        if seconds < 60:
                            return f"hace {int(seconds)} segundos"
                        elif seconds < 3600:
                            return f"hace {int(seconds // 60)} minutos"
                        else:
                            hours = int(seconds // 3600)
                            minutes = int((seconds % 3600) // 60)
                            return f"hace {hours} horas y {minutes} minutos"
                    except Exception as e:
                        print(f"Error al formatear tiempo: {e}")
                        return "Error de formato"
                
                workers_df['ultima_actividad'] = workers_df['last_update'].apply(format_time_ago)
            else:
                workers_df['ultima_actividad'] = "No disponible"
            
            # Renombrar columnas para mostrar
            columns_mapping = {
                'worker_id': 'Worker ID',
                'tasks': 'Tareas Actuales',
                'last_company': 'Empresa Actual',
                'ultima_actividad': 'Última Actividad'
            }
            
            # Seleccionar las columnas que existen
            display_columns = [col for col in columns_mapping.keys() if col in workers_df.columns]
            
            # Renombrar columnas
            for old_col, new_col in columns_mapping.items():
                if old_col in workers_df.columns:
                    workers_df.rename(columns={old_col: new_col}, inplace=True)
            
            # Mostrar DataFrame 
            if display_columns:
                display_cols = [columns_mapping[col] for col in display_columns if col in columns_mapping]
                st.dataframe(workers_df[display_cols], use_container_width=True)
            else:
                st.warning("Los datos de workers no contienen las columnas esperadas")
        else:
            st.warning("No hay workers activos en este momento")
    
    def render_charts_section(self):
        """Renderiza sección de gráficos"""
        st.markdown("## 📈 Actividad y Tendencias")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Gráfico de actividad de colas
            df_history = pd.DataFrame({
                'Tiempo': st.session_state.history['timestamps'],
                'Pendientes': st.session_state.history['pending'],
                'Procesando': st.session_state.history['processing'],
                'Completadas': st.session_state.history['completed'],
                'Fallidas': st.session_state.history['failed']
            })
            
            # Crear gráfico de áreas apiladas
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=df_history['Tiempo'], 
                y=df_history['Pendientes'],
                mode='lines',
                name='Pendientes',
                line=dict(width=0.5, color='rgb(255, 193, 7)'),
                stackgroup='one'
            ))
            fig.add_trace(go.Scatter(
                x=df_history['Tiempo'], 
                y=df_history['Procesando'],
                mode='lines',
                name='Procesando',
                line=dict(width=0.5, color='rgb(0, 123, 255)'),
                stackgroup='one'
            ))
            fig.add_trace(go.Scatter(
                x=df_history['Tiempo'], 
                y=df_history['Completadas'],
                mode='lines',
                name='Completadas',
                line=dict(width=0.5, color='rgb(40, 167, 69)'),
                stackgroup='one'
            ))
            fig.add_trace(go.Scatter(
                x=df_history['Tiempo'], 
                y=df_history['Fallidas'],
                mode='lines',
                name='Fallidas',
                line=dict(width=0.5, color='rgb(220, 53, 69)'),
                stackgroup='one'
            ))
            
            fig.update_layout(
                title='Actividad de Colas en Tiempo Real',
                xaxis_title='Tiempo',
                yaxis_title='Cantidad de Tareas',
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
            )
            
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Ahora llamamos a la función separada para renderizar los workers
            self.render_workers_section()

    def render_recent_results_section(self):
        """Renderiza sección de resultados recientes"""
        st.markdown("## 🔍 Resultados Recientes")
        
        results_df = self.get_recent_results()
        
        if not results_df.empty:
            # Formatear dataframe para mostrar
            results_df['url_status'] = results_df['url_exists'].apply(
                lambda x: "✅ Válida" if x else "❌ No válida"
            )
            
            # Verificar y convertir la columna fecha_actualizacion a datetime si es necesario
            if 'fecha_actualizacion' in results_df.columns:
                try:
                    # Intentar convertir a datetime
                    results_df['fecha_actualizacion'] = pd.to_datetime(results_df['fecha_actualizacion'], errors='coerce')
                    # Crear columna formateada solo si la conversión fue exitosa
                    mask = results_df['fecha_actualizacion'].notna()
                    results_df.loc[mask, 'fecha'] = results_df.loc[mask, 'fecha_actualizacion'].dt.strftime('%Y-%m-%d %H:%M:%S')
                    results_df.loc[~mask, 'fecha'] = "Fecha no disponible"
                except:
                    # Si falla, crear una columna con un valor predeterminado
                    results_df['fecha'] = "Formato de fecha incorrecto"
            else:
                # Si no existe la columna, crear una con valor predeterminado
                results_df['fecha'] = "Fecha no disponible"
            
            # Determinar qué columnas mostrar según las disponibles
            columns_to_show = []
            for col in ['cod_infotel', 'razon_social', 'url', 'url_valida', 'url_status', 'telefono_1', 'e_commerce', 'worker_id', 'fecha']:
                if col in results_df.columns:
                    columns_to_show.append(col)
            
            # Renombrar columnas para mostrar nombres más legibles
            column_names = {
                'cod_infotel': 'ID',
                'razon_social': 'Empresa',
                'url': 'URL Original',
                'url_valida': 'URL Validada',
                'url_status': 'Estado',
                'telefono_1': 'Teléfono',
                'e_commerce': 'E-commerce',
                'worker_id': 'Procesado por',
                'fecha': 'Fecha'
            }
            
            # Crear un nuevo dataframe con los nombres de columna cambiados
            display_df = results_df[columns_to_show].copy()
            for col in columns_to_show:
                if col in column_names:
                    display_df.rename(columns={col: column_names[col]}, inplace=True)
            
            # Tabla interactiva
            st.dataframe(display_df, use_container_width=True)
        else:
            st.info("No hay resultados recientes para mostrar")
    
    def run(self):
        """Ejecuta el dashboard"""
        st.title("🕸️ Dashboard de Scraping Distribuido")

        # Sidebar
        with st.sidebar:
            st.header("Controles")

            # Botón de actualización manual
            if st.button("Actualizar Datos", key="refresh_button"):
                self.increment_refresh_counter()
                st.session_state.last_refresh = time.strftime('%H:%M:%S')
                st.success(f"Datos actualizados correctamente ({st.session_state.last_refresh})")

            # Muestra la última actualización
            if "last_refresh" not in st.session_state:
                st.session_state.last_refresh = time.strftime('%H:%M:%S')
            st.info(f"Última actualización: {st.session_state.last_refresh}")

            # Control de auto-refresh
            if "auto_refresh" not in st.session_state:
                st.session_state.auto_refresh = False

            auto_refresh = st.checkbox("Auto-refresh", value=st.session_state.auto_refresh)
            if auto_refresh != st.session_state.auto_refresh:
                st.session_state.auto_refresh = auto_refresh

            refresh_interval = st.slider(
                "Intervalo de actualización (segundos)", 
                min_value=1, 
                max_value=60, 
                value=5,
                disabled=not st.session_state.auto_refresh
            )

            # Si auto-refresh está activado, actualizar automáticamente
            if st.session_state.auto_refresh:
                time.sleep(refresh_interval)  # Espera el intervalo definido
                #st.experimental_set_query_params(refresh=str(time.time()))  # Fuerz

            # Añadir opciones para cargar datos
            st.subheader("Administración")
            
            # Botón para enqueue de tareas
            if st.button("Recargar Tareas Pendientes", key="reload_tasks"):
                self.reload_pending_tasks()
                
            # Mostrar mensaje de confirmación si existe
            if 'task_reload_message' in st.session_state:
                st.success(st.session_state.task_reload_message)
                # Limpiar mensaje después de mostrarlo
                if time.time() - st.session_state.last_refresh.timestamp() > 3:
                    del st.session_state.task_reload_message
            
            # Botón para reiniciar colas
            if st.button("Reiniciar Colas", key="reset_queues"):
                self.toggle_reset_confirmation()
            
            # Confirmación para reiniciar colas
            if st.session_state.reset_confirmation:
                st.warning("Esta acción eliminará todas las tareas en las colas. ¿Está seguro?")
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("Confirmar", key="confirm_reset"):
                        self.reset_queues()
                        st.success("Colas reiniciadas con éxito")
                with col2:
                    if st.button("Cancelar", key="cancel_reset"):
                        st.session_state.reset_confirmation = False
        
        # Cuerpo principal
        self.render_metrics_section()
        self.render_charts_section()
        self.render_recent_results_section()
        
        # Auto-refresh controlado por checkbox
        if st.session_state.auto_refresh:
            #time.sleep(0.1)  # Pequeña pausa para no bloquear la UI
            st.empty()  # Elemento vacío para forzar rerun sin interferir con la UI
            
            # Solo hacer auto-refresh si ha pasado el intervalo configurado
            if (datetime.now() - st.session_state.last_refresh).total_seconds() >= refresh_interval:
                self.increment_refresh_counter()
                st.experimental_rerun()

def main():
    dashboard = ScrapingDashboard()
    dashboard.run()

if __name__ == "__main__":
    main()