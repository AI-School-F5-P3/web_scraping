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
    def __init__(self):
        self.task_manager = TaskManager()
        self.db = SupabaseDatabaseManager()
        
        # Inicializar estado
        if 'last_refresh' not in st.session_state:
            st.session_state.last_refresh = datetime.now()
        
        if 'history' not in st.session_state:
            st.session_state.history = {
                'timestamps': [],
                'pending': [],
                'processing': [],
                'completed': [],
                'failed': []
            }
    
    def get_queue_stats(self):
        """Obtiene estadísticas de las colas"""
        return self.task_manager.get_queue_stats()
    
    def get_progress_data(self):
        """Obtiene datos de progreso"""
        # Obtener total de empresas
        total_query = "SELECT COUNT(*) FROM sociedades"
        processed_query = "SELECT COUNT(*) FROM sociedades WHERE processed = TRUE"
        
        total = self.db.execute_query(total_query)
        processed = self.db.execute_query(processed_query)
        
        # Calcular progreso
        progress = (processed / total) * 100 if total > 0 else 0
        
        return {
            'total': total,
            'processed': processed,
            'progress': progress
        }
    
    def get_active_workers(self):
        """Obtiene la lista de workers activos en los últimos 5 minutos"""
        query = """
        SELECT worker_id, COUNT(*) as tasks, MAX(fecha_actualizacion) as last_update
        FROM sociedades
        WHERE worker_id IS NOT NULL
          AND fecha_actualizacion > NOW() - INTERVAL '5 minutes'
        GROUP BY worker_id
        ORDER BY last_update DESC
        """
        
        workers_df = self.db.execute_query(query, return_df=True)
        if not isinstance(workers_df, pd.DataFrame):
            workers_df = pd.DataFrame(columns=['worker_id'])  # Define columnas según la consulta

            if workers_df.empty:
                return []
            
            
        return workers_df.to_dict('records')
    
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
    
    def render_metrics_section(self):
        """Renderiza sección de métricas principales"""
        st.markdown("## 📊 Métricas en Tiempo Real")
        
        # Obtener datos
        queue_stats = self.get_queue_stats()
        progress_data = self.get_progress_data()
        success_rate = self.get_success_rate()
        active_workers = self.get_active_workers()
        
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
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            st.markdown(
                f"""
                <div class="metric-card">
                    <div class="metric-label">Progreso Total</div>
                    <div class="metric-value">{progress_data['progress']:.1f}%</div>
                    <div>{progress_data['processed']} / {progress_data['total']}</div>
                </div>
                """, 
                unsafe_allow_html=True
            )
        
        with col2:
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
        
        with col3:
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
        
        with col4:
            st.markdown(
                f"""
                <div class="metric-card">
                    <div class="metric-label">Completadas</div>
                    <div class="metric-value success-metric">{queue_stats['completed']}</div>
                    <div>Éxitos: {success_rate['rate']:.1f}%</div>
                </div>
                """, 
                unsafe_allow_html=True
            )
        
        with col5:
            st.markdown(
                f"""
                <div class="metric-card">
                    <div class="metric-label">Workers Activos</div>
                    <div class="metric-value">{len(active_workers)}</div>
                    <div>Últimos 5min</div>
                </div>
                """, 
                unsafe_allow_html=True
            )
    
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
            # [resto del código para crear el gráfico de actividad...]
            
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Gráfico de tasa de procesamiento por worker
            rates_df = self.get_processing_rates()
            
            if not rates_df.empty and 'hour' in rates_df.columns:
                # Asegurar que 'hour' es datetime
                rates_df['hour'] = pd.to_datetime(rates_df['hour'])
                
                # Asegurarse de que tenemos worker_id también
                if 'worker_id' in rates_df.columns:
                    # Pivotear para tener workers como columnas
                    pivot_df = rates_df.pivot_table(
                        index='hour', 
                        columns='worker_id', 
                        values='count',
                        aggfunc='sum',
                        fill_value=0
                    ).reset_index()
                    
                    # Crear gráfico de líneas
                    fig = go.Figure()
                    
                    for col in pivot_df.columns:
                        if col != 'hour':
                            fig.add_trace(go.Scatter(
                                x=pivot_df['hour'],
                                y=pivot_df[col],
                                mode='lines+markers',
                                name=col
                            ))
                    
                    fig.update_layout(
                        title='Tasa de Procesamiento por Worker',
                        xaxis_title='Hora',
                        yaxis_title='Empresas procesadas',
                        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No hay datos de workers para mostrar tasas de procesamiento")
            else:
                st.info("No hay datos suficientes para mostrar tasas de procesamiento")
    
    def render_workers_section(self):
        """Renderiza sección de workers activos"""
        st.markdown("## 👷 Workers Activos")
        
        workers = self.get_active_workers()
        
        if workers:
            # Crear DataFrame para mostrar tabla
            workers_df = pd.DataFrame(workers)
            
            # Verificar si existe la columna 'last_update'
            if 'last_update' in workers_df.columns:
                workers_df['last_activity'] = workers_df['last_update'].apply(
                    lambda x: f"hace {(datetime.now() - x).seconds} segundos" if isinstance(x, datetime) else "desconocido"
                )
            else:
                # Si no existe, crear una columna de actividad con valor por defecto
                workers_df['last_activity'] = "No disponible"
            
            # Determinar qué columnas mostrar basado en las disponibles
            display_columns = []
            if 'worker_id' in workers_df.columns:
                display_columns.append('worker_id')
                # Renombrar columna para mostrar nombre más legible
                workers_df.rename(columns={'worker_id': 'Worker ID'}, inplace=True)
            if 'tasks' in workers_df.columns:
                display_columns.append('tasks')
                workers_df.rename(columns={'tasks': 'Tareas Procesadas'}, inplace=True)
            display_columns.append('last_activity')
            workers_df.rename(columns={'last_activity': 'Última Actividad'}, inplace=True)
            
            # Mostrar tabla solo si hay columnas para mostrar
            if display_columns:
                # Usar versión simple de dataframe sin column_config
                st.dataframe(
                    workers_df[[col.replace('worker_id', 'Worker ID')
                            .replace('tasks', 'Tareas Procesadas')
                            .replace('last_activity', 'Última Actividad') 
                            for col in display_columns]],
                    use_container_width=True
                )
            else:
                st.warning("Los datos de workers no contienen las columnas esperadas")
        else:
            st.warning("No hay workers activos en los últimos 5 minutos")
    
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
            
            if st.button("Actualizar Datos"):
                st.session_state.last_refresh = datetime.now()
                st.experimental_rerun()
            
            st.info(f"Última actualización: {st.session_state.last_refresh.strftime('%H:%M:%S')}")
            
            # Añadir opciones para cargar datos
            st.subheader("Administración")
            
            # Botón para enqueue de tareas
            if st.button("Recargar Tareas Pendientes"):
                # Este botón podría ejecutar una función para recargar tareas no procesadas
                # desde la BD a Redis
                st.info("Esta función requiere implementación específica")
            
            # Botón para reiniciar colas
            if st.button("Reiniciar Colas"):
                if st.checkbox("Confirmar reinicio"):
                    self.task_manager.reset_queues()
                    st.success("Colas reiniciadas con éxito")
        
        # Cuerpo principal
        self.render_metrics_section()
        self.render_charts_section()
        
        # Dividir en dos columnas
        col1, col2 = st.columns(2)
        
        with col1:
            self.render_workers_section()
        
        with col2:
            # Estadísticas adicionales
            st.markdown("## 📊 Estadísticas de Éxito")
            
            success_rate = self.get_success_rate()
            
            fig = go.Figure()
            fig.add_trace(go.Pie(
                labels=['Éxitos', 'Fallidos'],
                values=[success_rate['success'], success_rate['failed']],
                marker=dict(colors=['rgb(40, 167, 69)', 'rgb(220, 53, 69)']),
                hole=.4
            ))
            
            fig.update_layout(
                title=f"Tasa de Éxito: {success_rate['rate']:.1f}%",
                legend=dict(orientation="h", yanchor="bottom", y=-0.1, xanchor="center", x=0.5)
            )
            
            st.plotly_chart(fig, use_container_width=True)
        
        # Sección de resultados recientes
        self.render_recent_results_section()
        
        # Auto-refresh
        time.sleep(10)
        st.experimental_rerun()

def main():
    dashboard = ScrapingDashboard()
    dashboard.run()

if __name__ == "__main__":
    main()