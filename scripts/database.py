# database.py

import psycopg2
import pandas as pd
from typing import Optional, List, Dict, Any
from psycopg2.extras import execute_values
from config import DB_CONFIG, HARDWARE_CONFIG
import platform
from datetime import datetime

class DatabaseManager:
    def __init__(self):
        """Inicializa la conexión a la base de datos"""
        self.connection = psycopg2.connect(**DB_CONFIG)
        self.connection.autocommit = True
        self._optimize_connection()

    def _optimize_connection(self):
        """Optimiza la conexión según el hardware disponible"""
        try:
            with self.connection.cursor() as cursor:
                ram_gb = int(HARDWARE_CONFIG['total_ram'].replace('GB',''))
                cursor.execute(f"SET work_mem = '{ram_gb//4}MB'")
                cursor.execute(f"SET maintenance_work_mem = '{ram_gb//4}MB'")
                cursor.execute(f"SET effective_cache_size = '{ram_gb*3//4}GB'")
                
                # Configuración específica según sistema operativo
                if platform.system().lower() == 'linux':
                    cursor.execute("SET effective_io_concurrency = 200")
                    cursor.execute("SET random_page_cost = 1.1")
                else:
                    cursor.execute("SET effective_io_concurrency = 0")
                    cursor.execute("SET random_page_cost = 4.0")
        except Exception as e:
            print(f"Error en optimización de conexión: {str(e)}")

    def _reconnect(self):
        """Reestablece la conexión con la base de datos"""
        try:
            self.connection.close()
        except:
            pass
        self.connection = psycopg2.connect(**DB_CONFIG)
        self.connection.autocommit = True
        self._optimize_connection()

    def get_batch(self, identifier: str) -> Optional[pd.DataFrame]:
        """
        Busca un lote por su ID o por identificador de usuario
        """
        try:
            query = """
            SELECT *
            FROM sociedades
            WHERE (lote_id = %s OR created_by = %s)
                AND deleted = FALSE
            ORDER BY cod_infotel
            """
            
            with self.connection.cursor() as cursor:
                cursor.execute(query, (identifier, identifier))
                columns = [desc[0] for desc in cursor.description]
                results = cursor.fetchall()
                
                if results:
                    return pd.DataFrame(results, columns=columns)
                return None
                
        except Exception as e:
            print(f"Error al buscar lote: {str(e)}")
            return None

    def save_batch(self, df: pd.DataFrame, batch_id: str, created_by: str) -> Dict[str, Any]:
        """
        Guarda un nuevo lote en la base de datos
        """
        try:
            # Normalizar nombres de columnas
            df = df.copy()
            df.columns = [col.lower() for col in df.columns]
            
            # Añadir columnas de control
            df['lote_id'] = batch_id
            df['created_by'] = created_by
            df['created_at'] = datetime.now()
            df['deleted'] = False
            
            # Columnas requeridas para la inserción
            columns = [
                'cod_infotel', 'nif', 'razon_social', 'domicilio', 
                'cod_postal', 'nom_poblacion', 'nom_provincia', 'url',
                'lote_id', 'created_by', 'created_at', 'deleted'
            ]
            
            # Insertar en chunks
            chunk_size = 1000
            total_inserted = 0
            errors = []
            
            for i in range(0, len(df), chunk_size):
                chunk = df.iloc[i:i + chunk_size]
                try:
                    values = [tuple(row) for row in chunk[columns].values]
                    insert_query = f"""
                    INSERT INTO sociedades ({', '.join(columns)}) 
                    VALUES %s
                    """
                    
                    with self.connection.cursor() as cursor:
                        execute_values(cursor, insert_query, values)
                        total_inserted += len(chunk)
                        
                except Exception as e:
                    errors.append(str(e))
                    self.connection.rollback()
            
            return {
                "status": "success" if total_inserted == len(df) else "error",
                "inserted": total_inserted,
                "total": len(df),
                "errors": errors
            }
            
        except Exception as e:
            return {
                "status": "error",
                "message": str(e)
            }

    def delete_batch(self, batch_id: str) -> Dict[str, Any]:
        """
        Elimina un lote completo
        """
        try:
            delete_query = """
            DELETE FROM sociedades 
            WHERE lote_id = %s
            RETURNING cod_infotel
            """
            
            with self.connection.cursor() as cursor:
                cursor.execute(delete_query, (batch_id,))
                deleted_rows = cursor.fetchall()
                self.connection.commit()
                
                return {
                    "status": "success",
                    "deleted": len(deleted_rows),
                    "batch_id": batch_id
                }
                
        except Exception as e:
            self.connection.rollback()
            return {
                "status": "error",
                "message": str(e)
            }

    def delete_records(self, cod_infotel_list: List[str], batch_id: str) -> Dict[str, Any]:
        """
        Elimina registros específicos de un lote
        """
        try:
            delete_query = """
            DELETE FROM sociedades 
            WHERE cod_infotel = ANY(%s)
                AND lote_id = %s
            """
            
            with self.connection.cursor() as cursor:
                cursor.execute(delete_query, (cod_infotel_list, batch_id))
                self.connection.commit()
                
                return {
                    "status": "success",
                    "deleted": cursor.rowcount,
                    "batch_id": batch_id
                }
                
        except Exception as e:
            self.connection.rollback()
            return {
                "status": "error",
                "message": str(e)
            }