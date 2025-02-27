# dashboard.py
import streamlit as st
import pandas as pd
import time
import plotly.graph_objects as go
from datetime import datetime
from task_manager import TaskManager
from database_supabase import SupabaseDatabaseManager
import os

class ScrapingDashboard:
    def __init__(self, use_sidebar=True):
        self.task_manager = TaskManager()
        self.db = SupabaseDatabaseManager()
        self._init_session_state()
        # Add custom CSS
        self.load_css()
        # Control whether to use sidebar or not
        self.use_sidebar = use_sidebar
        
    def load_css(self):
        """Apply custom CSS styling to the app from an external file"""
        css_file = os.path.join(os.path.dirname(__file__), 'styles.css')
        
        if os.path.exists(css_file):
            with open(css_file, 'r') as file:
                st.markdown(f"<style>{file.read()}</style>", unsafe_allow_html=True)
        else:
            st.warning("CSS file not found! Make sure `style.css` exists in the same directory.")
    
    def _init_session_state(self):
        defaults = {
            'last_refresh': datetime.now(),
            'refresh_counter': 0,
            'auto_refresh': True,
            'reset_confirmation': False,
            'history': {
                'timestamps': [],
                'pending': [],
                'processing': [],
                'completed': [],
                'failed': []
            }
        }
        for key, value in defaults.items():
            if key not in st.session_state:
                st.session_state[key] = value
    
    def increment_refresh(self):
        st.session_state.refresh_counter += 1
        st.session_state.last_refresh = datetime.now()
    
    def toggle(self, key):
        st.session_state[key] = not st.session_state[key]
    
    def reset_queues(self):
        self.task_manager.reset_queues()
        st.session_state.reset_confirmation = False
        self.increment_refresh()
    
    def get_queue_stats(self):
        return self.task_manager.get_queue_stats()
    
    def get_progress_data(self):
        total = self.db.execute_query("SELECT COUNT(*) FROM sociedades")
        processed = self.db.execute_query("SELECT COUNT(*) FROM sociedades WHERE processed = TRUE")
        progress = (processed / total * 100) if total > 0 else 0
        return {'total': total, 'processed': processed, 'progress': progress}
    
    def get_active_workers(self):
        query = """
        SELECT worker_id, COUNT(*) as tasks, MAX(fecha_actualizacion) as last_update
        FROM sociedades
        WHERE worker_id IS NOT NULL AND fecha_actualizacion > NOW() - INTERVAL '5 minutes'
        GROUP BY worker_id
        ORDER BY last_update DESC
        """
        workers_df = self.db.execute_query(query, return_df=True)
        if isinstance(workers_df, pd.DataFrame) and not workers_df.empty:
            return workers_df.to_dict('records')
        return []
    
    def get_processing_rates(self):
        query = """
        SELECT worker_id, DATE_TRUNC('hour', fecha_actualizacion) as hour, COUNT(*) as count
        FROM sociedades
        WHERE fecha_actualizacion > NOW() - INTERVAL '24 hours' AND worker_id IS NOT NULL
        GROUP BY worker_id, DATE_TRUNC('hour', fecha_actualizacion)
        ORDER BY hour
        """
        df = self.db.execute_query(query, return_df=True)
        if isinstance(df, pd.DataFrame) and not df.empty:
            return df
        return pd.DataFrame()
    
    def get_success_rate(self):
        query = """
        SELECT COUNT(*) as total,
               COUNT(CASE WHEN url_exists = TRUE THEN 1 END) as success,
               COUNT(CASE WHEN url_exists = FALSE THEN 1 END) as failed
        FROM sociedades
        WHERE processed = TRUE
        """
        df = self.db.execute_query(query, return_df=True)
        if isinstance(df, pd.DataFrame) and not df.empty:
            total = df.iloc[0].get('total', 0)
            success = df.iloc[0].get('success', 0)
            failed = df.iloc[0].get('failed', 0)
            rate = (success / total * 100) if total > 0 else 0
            return {'total': total, 'success': success, 'failed': failed, 'rate': rate}
        return {'total': 0, 'success': 0, 'failed': 0, 'rate': 0}
    
    def get_recent_results(self, limit=20):
        query = f"""
        SELECT cod_infotel, razon_social, url, url_valida, url_exists,
               telefono_1, fecha_actualizacion, worker_id, e_commerce
        FROM sociedades
        WHERE processed = TRUE
        ORDER BY fecha_actualizacion DESC
        LIMIT {limit}
        """
        df = self.db.execute_query(query, return_df=True)
        if isinstance(df, pd.DataFrame) and not df.empty:
            return df
        return pd.DataFrame()
    
    def reload_pending_tasks(self):
        query = """
        SELECT cod_infotel, razon_social, url
        FROM sociedades
        WHERE processed = FALSE
        """
        try:
            pending_tasks = self.db.execute_query(query, return_df=True)
            if not (isinstance(pending_tasks, pd.DataFrame) and not pending_tasks.empty):
                st.session_state.task_reload_message = "No hay tareas pendientes para recargar"
                self.increment_refresh()
                return 0
            count = 0
            for _, row in pending_tasks.iterrows():
                self.task_manager.enqueue_task(row.to_dict())
                count += 1
            st.session_state.task_reload_message = f"Tareas pendientes recargadas correctamente ({count})"
            self.increment_refresh()
            return count
        except Exception as e:
            st.error(f"Error al recargar tareas: {str(e)}")
            self.increment_refresh()
            return -1   
    
    def update_history(self, queue_stats):
        now = datetime.now()
        history = st.session_state.history
        history['timestamps'].append(now)
        history['pending'].append(queue_stats.get('pending', 0))
        history['processing'].append(queue_stats.get('processing', 0))
        history['completed'].append(queue_stats.get('completed', 0))
        history['failed'].append(queue_stats.get('failed', 0))
        # Limitar historial a las últimas 100 muestras
        for key in history:
            if len(history[key]) > 100:
                history[key] = history[key][-100:]
    
    def render_metrics_section(self):
        st.markdown("## 📊 Métricas en Tiempo Real")
        queue_stats = self.get_queue_stats()
        progress_data = self.get_progress_data()
        success_rate = self.get_success_rate()
        active_workers = self.get_active_workers()
        self.update_history(queue_stats)
        
        cols = st.columns(5)
        metrics = [
            {
                "label": "Progreso Total",
                "value": f"{progress_data['progress']:.1f}%",
                "extra": f"{progress_data['processed']} / {progress_data['total']}"
            },
            {
                "label": "Pendientes",
                "value": f"{queue_stats.get('pending', 0)}",
                "extra": "En cola",
                "class": "warning-metric"
            },
            {
                "label": "En Procesamiento",
                "value": f"{queue_stats.get('processing', 0)}",
                "extra": "En curso"
            },
            {
                "label": "Completadas",
                "value": f"{queue_stats.get('completed', 0)}",
                "extra": f"Éxitos: {success_rate['rate']:.1f}%",
                "class": "success-metric"
            },
            {
                "label": "Workers Activos",
                "value": f"{len(active_workers)}",
                "extra": "Últimos 5min"
            }
        ]
        for col, metric in zip(cols, metrics):
            css_class = metric.get("class", "")
            col.markdown(
                f"""
                <div class="metric-card">
                    <div class="metric-label">{metric['label']}</div>
                    <div class="metric-value {css_class}">{metric['value']}</div>
                    <div>{metric['extra']}</div>
                </div>
                """, unsafe_allow_html=True
            )
    
    def render_charts_section(self):
        st.markdown("## 📈 Actividad y Tendencias")
        col1, col2 = st.columns(2)
        
        with col1:
            df_history = pd.DataFrame({
                'Tiempo': st.session_state.history['timestamps'],
                'Pendientes': st.session_state.history['pending'],
                'Procesando': st.session_state.history['processing'],
                'Completadas': st.session_state.history['completed'],
                'Fallidas': st.session_state.history['failed']
            })
            fig = go.Figure()
            colors = {
                'Pendientes': 'rgb(255, 193, 7)',
                'Procesando': 'rgb(0, 123, 255)',
                'Completadas': 'rgb(40, 167, 69)',
                'Fallidas': 'rgb(220, 53, 69)'
            }
            for key, color in colors.items():
                fig.add_trace(go.Scatter(
                    x=df_history['Tiempo'], 
                    y=df_history[key],
                    mode='lines',
                    name=key,
                    line=dict(width=0.5, color=color),
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
            rates_df = self.get_processing_rates()
            if not rates_df.empty and 'hour' in rates_df.columns and 'worker_id' in rates_df.columns:
                rates_df['hour'] = pd.to_datetime(rates_df['hour'])
                pivot_df = rates_df.pivot_table(
                    index='hour', 
                    columns='worker_id', 
                    values='count',
                    aggfunc='sum',
                    fill_value=0
                ).reset_index()
                fig = go.Figure()
                for col_name in pivot_df.columns:
                    if col_name != 'hour':
                        fig.add_trace(go.Scatter(
                            x=pivot_df['hour'],
                            y=pivot_df[col_name],
                            mode='lines+markers',
                            name=col_name
                        ))
                fig.update_layout(
                    title='Tasa de Procesamiento por Worker',
                    xaxis_title='Hora',
                    yaxis_title='Empresas procesadas',
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No hay datos suficientes para mostrar tasas de procesamiento")
    
    def render_workers_section(self):
        st.markdown("## 👷 Workers Activos")
        workers = self.get_active_workers()
        if workers:
            workers_df = pd.DataFrame(workers)
            if 'last_update' in workers_df.columns:
                workers_df['Última Actividad'] = workers_df['last_update'].apply(
                    lambda x: f"hace {(datetime.now() - x).seconds} segundos" if isinstance(x, datetime) else "desconocido"
                )
            else:
                workers_df['Última Actividad'] = "No disponible"
            rename_map = {'worker_id': 'Worker ID', 'tasks': 'Tareas Procesadas'}
            workers_df.rename(columns=rename_map, inplace=True)
            display_columns = [col for col in ['Worker ID', 'Tareas Procesadas', 'Última Actividad'] if col in workers_df.columns]
            st.dataframe(workers_df[display_columns], use_container_width=True)
        else:
            st.warning("No hay workers activos en los últimos 5 minutos")
    
    def render_recent_results_section(self):
        st.markdown("## 🔍 Resultados Recientes")
        results_df = self.get_recent_results()
        if results_df.empty:
            st.info("No hay resultados recientes para mostrar")
            return
        results_df['url_status'] = results_df['url_exists'].apply(lambda x: "✅ Válida" if x else "❌ No válida")
        if 'fecha_actualizacion' in results_df.columns:
            results_df['fecha'] = pd.to_datetime(results_df['fecha_actualizacion'], errors='coerce')\
                .dt.strftime('%Y-%m-%d %H:%M:%S')
            results_df['fecha'].fillna("Fecha no disponible", inplace=True)
        else:
            results_df['fecha'] = "Fecha no disponible"
        cols_to_show = [col for col in ['cod_infotel', 'razon_social', 'url', 'url_valida', 'url_status', 
                                         'telefono_1', 'e_commerce', 'worker_id', 'fecha'] if col in results_df.columns]
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
        display_df = results_df[cols_to_show].rename(columns=column_names)
        st.dataframe(display_df, use_container_width=True)
    
    def run(self, container=None):
        """
        Run the dashboard with an optional container
        If container is provided, dashboard will render in that container instead of the main area
        """
        if container:
            with container:
                self._run_content()
        else:
            st.title("🕸️ Dashboard de Scraping Distribuido")
            self._run_content()
    
    def _run_content(self):
        # Sidebar controls (only if use_sidebar is True)
        if self.use_sidebar:
            with st.sidebar:
                self._render_sidebar_controls()
        
        # Main content (always show)
        self.render_metrics_section()
        self.render_charts_section()
        col1, col2 = st.columns(2)
        with col1:
            self.render_workers_section()
        with col2:
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
        self.render_recent_results_section()
        
        # Auto-refresh handling
        if 'auto_refresh' in st.session_state and st.session_state.auto_refresh:
            time.sleep(0.1)
            refresh_interval = st.session_state.get('refresh_interval', 10)
            if (datetime.now() - st.session_state.last_refresh).total_seconds() >= refresh_interval:
                self.increment_refresh()
                st.experimental_rerun()
    
    def _render_sidebar_controls(self):
        """Render the sidebar controls"""
        st.header("Controles")
        if st.button("Actualizar Datos", key="refresh_button"):
            self.increment_refresh()
            st.success("Datos actualizados correctamente")
        st.info(f"Última actualización: {st.session_state.last_refresh.strftime('%H:%M:%S')}")
        auto_refresh = st.checkbox("Auto-refresh", value=st.session_state.auto_refresh)
        if auto_refresh != st.session_state.auto_refresh:
            self.toggle('auto_refresh')
        st.session_state['refresh_interval'] = st.slider(
            "Intervalo de actualización (segundos)", 
            5, 60, 10,
            disabled=not st.session_state.auto_refresh
        )
        st.subheader("Administración")
        if st.button("Recargar Tareas Pendientes", key="reload_tasks"):
            self.reload_pending_tasks()
        if 'task_reload_message' in st.session_state:
            st.success(st.session_state.task_reload_message)
            if time.time() - st.session_state.last_refresh.timestamp() > 3:
                del st.session_state.task_reload_message
        if st.button("Reiniciar Colas", key="reset_queues"):
            self.toggle('reset_confirmation')
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

def main():
    dashboard = ScrapingDashboard()
    dashboard.run()

if __name__ == "__main__":
    main()