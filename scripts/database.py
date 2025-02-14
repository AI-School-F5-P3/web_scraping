# database.py

import psycopg2
import pandas as pd
from typing import Optional, List, Dict, Any
from psycopg2.extras import execute_values
from config import DB_CONFIG, HARDWARE_CONFIG, TIMEOUT_CONFIG

class DatabaseManager:
    def __init__(self):
        self.connection = psycopg2.connect(**DB_CONFIG)
        self.connection.autocommit = True
        self._optimize_connection()

    def _optimize_connection(self):
        with self.connection.cursor() as cursor:
            ram_gb = int(HARDWARE_CONFIG['total_ram'].replace('GB',''))
            cursor.execute(f"SET work_mem = '{ram_gb//4}MB'")
            cursor.execute(f"SET maintenance_work_mem = '{ram_gb//4}MB'")
            cursor.execute(f"SET effective_cache_size = '{ram_gb*3//4}GB'")
            cursor.execute("SET effective_io_concurrency = 200")
            cursor.execute("SET random_page_cost = 1.1")
            cursor.execute("SET cpu_tuple_cost = 0.03")
            cursor.execute("SET cpu_index_tuple_cost = 0.01")

    def execute_query(self, query: str, params: tuple = None, return_df: bool = False) -> Optional[pd.DataFrame]:
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params or ())
                if return_df:
                    columns = [desc[0] for desc in cursor.description]
                    return pd.DataFrame(cursor.fetchall(), columns=columns)
                return cursor.fetchall()
        except Exception as e:
            self._handle_db_error(e, query)
            return None

    def _handle_db_error(self, error: Exception, query: str):
        error_msg = str(error)
        if "deadlock detected" in error_msg.lower():
            self.connection.rollback()
        elif "connection" in error_msg.lower():
            self._reconnect()
        raise error

    def _reconnect(self):
        try:
            self.connection.close()
        except:
            pass
        self.connection = psycopg2.connect(**DB_CONFIG)
        self.connection.autocommit = True
        self._optimize_connection()

    def batch_insert(self, df: pd.DataFrame, table: str, columns: List[str]) -> Dict[str, Any]:
        chunk_size = 1000
        total_inserted = 0
        errors = []

        try:
            df_chunks = [df[i:i + chunk_size] for i in range(0, len(df), chunk_size)]
            
            with self.connection.cursor() as cursor:
                for chunk in df_chunks:
                    try:
                        values = [tuple(row) for row in chunk[columns].values]
                        insert_query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES %s"
                        execute_values(cursor, insert_query, values)
                        total_inserted += len(chunk)
                    except Exception as e:
                        errors.append(str(e))
                        self.connection.rollback()

            return {
                "status": "success" if total_inserted == len(df) else "partial",
                "inserted": total_inserted,
                "total": len(df),
                "errors": errors
            }
        except Exception as e:
            return {"status": "error", "message": str(e), "errors": errors}

    def save_batch(self, df: pd.DataFrame, batch_id: str, created_by: str) -> Dict[str, Any]:
        insert_columns = [
            'cod_infotel', 'nif', 'razon_social', 'domicilio', 'cod_postal',
            'nom_poblacion', 'nom_provincia', 'url', 'lote_id', 'created_by'
        ]
        
        df = df.copy()
        df['lote_id'] = batch_id
        df['created_by'] = created_by
        
        return self.batch_insert(df, 'sociedades', insert_columns)

    def update_scraping_results(self, results: List[Dict[str, Any]], batch_id: str) -> Dict[str, Any]:
        try:
            update_query = """
            UPDATE sociedades 
            SET 
                url_exists = %(exists)s,
                url_limpia = %(clean_url)s,
                url_status = %(status)s,
                url_status_mensaje = %(status_message)s,
                telefono_1 = %(phone1)s,
                telefono_2 = %(phone2)s,
                telefono_3 = %(phone3)s,
                facebook = %(facebook)s,
                twitter = %(twitter)s,
                linkedin = %(linkedin)s,
                instagram = %(instagram)s,
                e_commerce = %(ecommerce)s,
                fecha_actualizacion = NOW()
            WHERE url = %(url)s AND lote_id = %(batch_id)s
            """
            
            with self.connection.cursor() as cursor:
                for result in results:
                    params = {
                        'exists': result.get('url_exists', False),
                        'clean_url': result.get('url_limpia'),
                        'status': result.get('url_status'),
                        'status_message': result.get('url_status_mensaje'),
                        'phone1': result.get('phones', [''])[0],
                        'phone2': result.get('phones', ['', ''])[1],
                        'phone3': result.get('phones', ['', '', ''])[2],
                        'facebook': result.get('social_media', {}).get('facebook'),
                        'twitter': result.get('social_media', {}).get('twitter'),
                        'linkedin': result.get('social_media', {}).get('linkedin'),
                        'instagram': result.get('social_media', {}).get('instagram'),
                        'ecommerce': result.get('is_ecommerce', False),
                        'url': result.get('url'),
                        'batch_id': batch_id
                    }
                    cursor.execute(update_query, params)
                    
            return {"status": "success", "updated": len(results)}
        except Exception as e:
            return {"status": "error", "message": str(e)}

    def get_urls_for_scraping(self, batch_id: str = None, limit: int = 100) -> pd.DataFrame:
        query = """
        SELECT id, cod_infotel, url
        FROM sociedades
        WHERE deleted = FALSE 
        AND url IS NOT NULL
        AND url_status IS NULL
        """
        
        if batch_id:
            query += " AND lote_id = %s"
            params = (batch_id,)
        else:
            params = None
            
        query += f" LIMIT {limit}"
        
        return self.execute_query(query, params, return_df=True)