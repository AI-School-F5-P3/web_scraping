import argparse
import pandas as pd
import numpy as np
import logging
from task_manager import TaskManager
from database_supabase import SupabaseDatabaseManager

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_and_enqueue(file_path, batch_size=1000, reset_queues=False):
    """
    Carga datos desde un archivo CSV/Excel y los encola en Redis
    """
    # Inicializar managers
    task_manager = TaskManager()
    db = SupabaseDatabaseManager()
    
    # Resetear colas si se solicita
    if reset_queues:
        logger.warning("Resetting all queues as requested")
        task_manager.reset_queues()
    
    # Cargar archivo
    logger.info(f"Loading file: {file_path}")
    if file_path.endswith('.csv'):
        df = pd.read_csv(file_path, sep=';', encoding='utf-8')
    else:
        df = pd.read_excel(file_path)
    
    # Normalizar nombres de columnas
    df.columns = df.columns.str.strip().str.lower()
    
    # Limpiar datos
    df = df.replace(r'^\s*$', np.nan, regex=True)
    df = df.replace({np.nan: None})
    
    # Guardar en base de datos
    logger.info(f"Saving {len(df)} records to database")
    result = db.save_batch(df, check_duplicates=True)
    
    if result["status"] in ["success", "partial"]:
        logger.info(f"Saved {result.get('inserted', 0)} records to database")
    else:
        logger.error(f"Error saving to database: {result.get('message')}")
        return
    
    # Obtener empresas no procesadas
    logger.info("Getting companies to process")
    companies = db.execute_query(
        "SELECT cod_infotel, nif, razon_social, domicilio, cod_postal, nom_poblacion, nom_provincia, url " +
        "FROM sociedades WHERE processed = FALSE OR processed IS NULL",
        return_df=True
    )
    
    if companies is None or companies.empty:
        logger.warning("No companies to process")
        return
    
    # Convertir a lista de diccionarios
    companies_list = companies.to_dict('records')
    logger.info(f"Found {len(companies_list)} companies to process")
    
    # Encolar en lotes
    total_enqueued = 0
    for i in range(0, len(companies_list), batch_size):
        batch = companies_list[i:i+batch_size]
        enqueued = task_manager.enqueue_tasks(batch)
        total_enqueued += enqueued
        logger.info(f"Enqueued batch {i//batch_size + 1}/{(len(companies_list)-1)//batch_size + 1} ({enqueued} tasks)")
    
    # Mostrar estadísticas finales
    stats = task_manager.get_queue_stats()
    logger.info(f"Total enqueued: {total_enqueued}")
    logger.info(f"Queue stats: {stats}")

def main():
    parser = argparse.ArgumentParser(description="Load data and enqueue tasks")
    parser.add_argument(
        "file_path", 
        help="Path to CSV or Excel file with company data"
    )
    parser.add_argument(
        "--batch-size", 
        type=int, 
        default=1000,
        help="Batch size for enqueueing (default: 1000)"
    )
    parser.add_argument(
        "--reset", 
        action="store_true",
        help="Reset all queues before loading"
    )
    
    args = parser.parse_args()
    load_and_enqueue(args.file_path, args.batch_size, args.reset)

if __name__ == "__main__":
    main()