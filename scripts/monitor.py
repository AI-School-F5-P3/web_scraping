import time
#!/usr/bin/env python3
# monitor.py
import time
import argparse
import logging
import json
import os
from datetime import datetime
from rich.console import Console
from rich.table import Table
from rich.live import Live
from rich.panel import Panel
from rich.progress import Progress, BarColumn, TextColumn
from task_manager import TaskManager
from database_supabase import SupabaseDatabaseManager

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"monitor_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ScrapingMonitor:
    def __init__(self):
        self.task_manager = TaskManager()
        self.db = SupabaseDatabaseManager()
        self.console = Console()
        self.start_time = time.time()
    
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
        if workers_df is None or workers_df.empty:
            return []
            
        return workers_df.to_dict('records')
    
    def get_recent_errors(self, limit=10):
        """Obtiene los errores recientes de Redis"""
        errors = []
        for i in range(limit):
            error = self.task_manager.redis.lindex(f"scraper:metrics:errors", i)
            if error:
                errors.append(error)
            else:
                break
        return errors
    
    def get_metrics(self):
        """Obtiene métricas de rendimiento"""
        # Obtener tiempos de procesamiento recientes
        processing_times = []
        for i in range(100):  # Últimos 100
            time_str = self.task_manager.redis.lindex(f"scraper:metrics:processing_times", i)
            if time_str:
                processing_times.append(float(time_str))
            else:
                break
        
        # Calcular estadísticas básicas
        if processing_times:
            avg_time = sum(processing_times) / len(processing_times)
            min_time = min(processing_times)
            max_time = max(processing_times)
        else:
            avg_time = min_time = max_time = 0
        
        # Obtener tasas de procesamiento por worker
        query = """
        SELECT worker_id, 
               COUNT(*) as total,
               COUNT(CASE WHEN url_exists = TRUE THEN 1 END) as success,
               COUNT(CASE WHEN url_exists = FALSE THEN 1 END) as failed,
               MAX(fecha_actualizacion) - MIN(fecha_actualizacion) as time_span
        FROM sociedades
        WHERE worker_id IS NOT NULL
          AND fecha_actualizacion > NOW() - INTERVAL '1 hour'
        GROUP BY worker_id
        """
        
        worker_stats_df = self.db.execute_query(query, return_df=True)
        worker_stats = []
        
        if worker_stats_df is not None and not worker_stats_df.empty:
            for _, row in worker_stats_df.iterrows():
                # Calcular tasa de procesamiento (tareas/minuto)
                if row['time_span'] and row['time_span'].total_seconds() > 0:
                    rate = row['total'] / (row['time_span'].total_seconds() / 60)
                else:
                    rate = 0
                
                worker_stats.append({
                    'worker_id': row['worker_id'],
                    'total': row['total'],
                    'success': row['success'],
                    'failed': row['failed'],
                    'rate': rate
                })
        
        return {
            'processing_times': {
                'avg': avg_time,
                'min': min_time,
                'max': max_time,
                'count': len(processing_times)
            },
            'worker_stats': worker_stats
        }
    
    def get_progress_data(self):
        """Obtiene datos para la barra de progreso"""
        # Obtener estadísticas de las colas
        queue_stats = self.task_manager.get_queue_stats()
        
        # Obtener total de empresas
        total_query = "SELECT COUNT(*) FROM sociedades"
        processed_query = "SELECT COUNT(*) FROM sociedades WHERE processed = TRUE"
        
        total = self.db.execute_query(total_query)
        processed = self.db.execute_query(processed_query)
        
        # Calcular progreso
        progress = (processed / total) * 100 if total > 0 else 0
        
        # Calcular ETA
        if queue_stats['completed'] > 0 and time.time() > self.start_time:
            elapsed = time.time() - self.start_time
            rate = queue_stats['completed'] / elapsed  # tareas/segundo
            remaining = total - processed
            eta_seconds = remaining / rate if rate > 0 else 0
            
            # Formatear ETA
            if eta_seconds > 3600:
                eta = f"{eta_seconds/3600:.1f} horas"
            elif eta_seconds > 60:
                eta = f"{eta_seconds/60:.1f} minutos"
            else:
                eta = f"{eta_seconds:.0f} segundos"
        else:
            eta = "Calculando..."
            rate = 0
        
        return {
            'total': total,
            'processed': processed,
            'pending': queue_stats['pending'],
            'processing': queue_stats['processing'],
            'completed': queue_stats['completed'],
            'failed': queue_stats['failed'],
            'progress': progress,
            'eta': eta,
            'rate': rate
        }
    
    def run(self, refresh_rate=5, output_file=None):
        """Ejecuta el monitor con actualización en tiempo real"""
        try:
            with Live(refresh_per_second=1/refresh_rate) as live:
                while True:
                    # Obtener datos
                    queue_stats = self.task_manager.get_queue_stats()
                    workers = self.get_active_workers()
                    progress_data = self.get_progress_data()
                    metrics = self.get_metrics()
                    errors = self.get_recent_errors()
                    
                    # Crear tabla de estadísticas
                    stats_table = Table(title="Estado de las Colas")
                    stats_table.add_column("Cola")
                    stats_table.add_column("Cantidad")
                    stats_table.add_column("Porcentaje")
                    
                    total_tasks = sum(queue_stats.values())
                    
                    for queue, count in queue_stats.items():
                        percent = f"{count/total_tasks*100:.1f}%" if total_tasks > 0 else "0%"
                        stats_table.add_row(queue.capitalize(), str(count), percent)
                    
                    # Crear tabla de workers
                    workers_table = Table(title=f"Workers Activos ({len(workers)})")
                    workers_table.add_column("Worker ID")
                    workers_table.add_column("Tareas")
                    workers_table.add_column("Última Actualización")
                    
                    for worker in workers:
                        # Formatear tiempo transcurrido desde la última actualización
                        if isinstance(worker['last_update'], datetime):
                            now = datetime.now()
                            seconds = (now - worker['last_update']).total_seconds()
                            if seconds < 60:
                                time_ago = f"hace {seconds:.0f}s"
                            else:
                                time_ago = f"hace {seconds/60:.1f}m"
                        else:
                            time_ago = "desconocido"
                            
                        workers_table.add_row(
                            worker['worker_id'],
                            str(worker['tasks']),
                            time_ago
                        )
                    
                    # Crear barra de progreso
                    progress = Progress(
                        TextColumn("[bold blue]{task.description}"),
                        BarColumn(),
                        TextColumn("[progress.percentage]{task.percentage:>3.1f}%"),
                        TextColumn("({task.completed}/{task.total})")
                    )
                    
                    task = progress.add_task(
                        "Progreso Total", 
                        total=progress_data['total'],
                        completed=progress_data['processed']
                    )
                    
                    # Crear panel de métricas
                    metrics_text = f"""
[bold]Tiempos de Procesamiento:[/bold]
Promedio: {metrics['processing_times']['avg']:.2f}s
Mínimo: {metrics['processing_times']['min']:.2f}s
Máximo: {metrics['processing_times']['max']:.2f}s
Muestras: {metrics['processing_times']['count']}

[bold]Tasas de Procesamiento:[/bold]
"""
                    for worker_stat in metrics['worker_stats']:
                        metrics_text += f"{worker_stat['worker_id']}: {worker_stat['rate']:.2f} tareas/min ({worker_stat['success']}/{worker_stat['total']} exitosas)\n"
                    
                    metrics_panel = Panel(metrics_text, title="Métricas de Rendimiento")
                    
                    # Crear panel de errores
                    errors_text = "\n".join(errors) if errors else "No hay errores recientes"
                    errors_panel = Panel(errors_text, title="Errores Recientes")
                    
                    # ETA y tasa
                    eta_text = f"ETA: {progress_data['eta']} | Tasa: {progress_data['rate']:.2f} tareas/segundo"
                    
                    # Actualizar la visualización
                    live.update(
                        Panel(
                            stats_table,
                            title="Estado del Scraping",
                            subtitle=eta_text
                        )
                    )
                    
                    # Guardar datos en archivo si se solicita
                    if output_file:
                        with open(output_file, 'a') as f:
                            timestamp = datetime.now().isoformat()
                            data = {
                                'timestamp': timestamp,
                                'queue_stats': queue_stats,
                                'progress': progress_data,
                                'metrics': metrics,
                                'workers': [w['worker_id'] for w in workers]
                            }
                            f.write(json.dumps(data) + '\n')
                    
                    # Esperar para la próxima actualización
                    time.sleep(refresh_rate)
        
        except KeyboardInterrupt:
            print("\nMonitor detenido por el usuario")
        
        except Exception as e:
            logger.error(f"Error en el monitor: {str(e)}")
            import traceback
            traceback.print_exc()

def main():
    parser = argparse.ArgumentParser(description="Monitor de Scraping Distribuido")
    parser.add_argument(
        "--refresh-rate", 
        type=float, 
        default=5.0,
        help="Intervalo de actualización en segundos (default: 5.0)"
    )
    parser.add_argument(
        "--output", 
        type=str,
        default=None,
        help="Archivo de salida para métricas (JSON lines)"
    )
    
    args = parser.parse_args()
    
    monitor = ScrapingMonitor()
    monitor.run(refresh_rate=args.refresh_rate, output_file=args.output)

if __name__ == "__main__":
    main()