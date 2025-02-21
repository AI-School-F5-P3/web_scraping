# database.py

import psycopg2
import pandas as pd
import numpy as np
from typing import Optional, List, Dict, Any
from psycopg2.extras import execute_values
from config import DB_CONFIG, HARDWARE_CONFIG, TIMEOUT_CONFIG
import platform
from db_validator import DataProcessor
class DatabaseManager:
    def __init__(self):
        self.connection = psycopg2.connect(**DB_CONFIG)
        self.connection.autocommit = True
        self.data_processor = DataProcessor()
        self._optimize_connection()
        self.create_table_if_not_exists()

    def _optimize_connection(self):
        with self.connection.cursor() as cursor:
            ram_gb = int(HARDWARE_CONFIG['total_ram'].replace('GB',''))
            cursor.execute(f"SET work_mem = '{ram_gb//4}MB'")
            cursor.execute(f"SET maintenance_work_mem = '{ram_gb//4}MB'")
            cursor.execute(f"SET effective_cache_size = '{ram_gb*3//4}GB'")
            
            system = platform.system()
            if system == "Darwin":
                cursor.execute("SET effective_io_concurrency = 0")
            else:
                try:
                    cursor.execute("SET effective_io_concurrency = 200")
                except Exception as e:
                    print(f"Warning: {e} (setting effective_io_concurrency on {system})")
                        
            cursor.execute("SET random_page_cost = 1.1")
            cursor.execute("SET cpu_tuple_cost = 0.03")
            cursor.execute("SET cpu_index_tuple_cost = 0.01")

    def execute_query(self, query: str, params: tuple = None, return_df: bool = False) -> Optional[pd.DataFrame]:
        """
        Executes a SQL query and optionally returns results as a DataFrame.
        
        Args:
            query (str): SQL query to execute
            params (tuple, optional): Query parameters. Defaults to None.
            return_df (bool, optional): Whether to return results as DataFrame. Defaults to False.
            
        Returns:
            Optional[pd.DataFrame]: Results as DataFrame if return_df is True, None otherwise
        """
        try:
            with self.connection.cursor() as cursor:
                # Ensure connection is alive
                if not self.connection.closed:
                    cursor.execute("SELECT 1")
                else:
                    self._reconnect()
                    
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                    
                if cursor.description:  # If the query returns results
                    columns = [desc[0] for desc in cursor.description]
                    results = cursor.fetchall()
                    
                    if return_df:
                        df = pd.DataFrame(results, columns=columns)
                        # Remove duplicates if present
                        if 'cod_infotel' in df.columns:
                            df = df.drop_duplicates(subset=['cod_infotel'])
                        return df
                    elif "count" in query.lower():
                        return results[0][0] if results else 0
                    else:
                        return results
                return None
        except Exception as e:
            print(f"Database error: {str(e)}")
            self._reconnect()
            return None

    def _handle_db_error(self, error: Exception, query: str) -> None:
        """
        Handles database errors in a more informative way.
        
        Args:
            error (Exception): The caught exception
            query (str): The query that caused the error
        """
        error_msg = f"Database error: {str(error)}\nQuery: {query}"
        print(error_msg)  # For logging
        raise type(error)(error_msg)  # Re-raise with more context

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
            # Convertir strings vacíos y espacios en blanco a None/NULL
            df = df.replace(r'^\s*$', None, regex=True)
            # Convertir NaN a None
            df = df.replace({np.nan: None})
            
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

    def save_batch(self, df: pd.DataFrame, check_duplicates: bool = False) -> Dict[str, Any]:
        # Limpiar strings vacíos y espacios en blanco antes del procesamiento
        df = df.replace(r'^\s*$', None, regex=True)
        df = df.replace({np.nan: None})
        
        # Asegurar limpieza de espacios en blanco antes de guardar
        df = self.data_processor.validator.clean_text_fields(df)
        df['nom_provincia'] = df['nom_provincia'].astype(str).str.strip()    
        
        # Procesar y validar datos
        df, errors = self.data_processor.process_dataframe(df)

        if errors:
            return {
                "status": "error",
                "message": "Errores de validación",
                "errors": errors
            }
            
        insert_columns = [
            'cod_infotel', 'nif', 'razon_social', 'domicilio', 'cod_postal',
            'nom_poblacion', 'nom_provincia', 'url', 'url_exists', 'url_limpia',
            'url_status'
        ]
        
        if check_duplicates:
            df = df.drop_duplicates(subset=['cod_infotel'])  # Remove duplicates based on cod_infotel
        
        return self.batch_insert(df, 'sociedades', insert_columns)

    def get_urls_for_scraping(self, limit: int = 10) -> pd.DataFrame:
        """Gets URLs that need to be scraped."""
        query = """
        SELECT cod_infotel, url 
        FROM sociedades 
        WHERE url IS NOT NULL 
        AND url != '' 
        LIMIT %s
        """
        return self.execute_query(query, params=(limit,), return_df=True)

    def get_record_count(self) -> int:
        """Gets total number of records in the sociedades table."""
        query = "SELECT COUNT(*) FROM sociedades"
        result = self.execute_query(query, return_df=True)
        return result.iloc[0, 0] if result is not None else 0

    def update_scraping_results(self, results: List[Dict[str, Any]]) -> Dict[str, Any]:
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
                youtube = %(youtube)s,
                e_commerce = %(ecommerce)s,
                fecha_actualizacion = NOW()
            WHERE cod_infotel = %(cod_infotel)s
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
                        'youtube': result.get('social_media', {}).get('youtube'),
                        'ecommerce': result.get('is_ecommerce', False),
                        'cod_infotel': result.get('cod_infotel')
                    }
                    cursor.execute(update_query, params)
                    
            return {"status": "success", "updated": len(results)}
        except Exception as e:
            return {"status": "error", "message": str(e)}

        
    def create_table_if_not_exists(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS sociedades (
            id SERIAL PRIMARY KEY,
            cod_infotel INTEGER NOT NULL,
            nif VARCHAR(11),
            razon_social VARCHAR(255),
            domicilio VARCHAR(255),
            cod_postal VARCHAR(5),
            nom_poblacion VARCHAR(100),
            nom_provincia VARCHAR(100),
            url VARCHAR(255),
            url_valida VARCHAR(255),
            url_exists BOOLEAN DEFAULT FALSE NOT NULL,
            url_limpia VARCHAR(255),
            url_status INTEGER,
            url_status_mensaje VARCHAR(255),
            telefono_1 VARCHAR(16),
            telefono_2 VARCHAR(16),
            telefono_3 VARCHAR(16),
            facebook VARCHAR(255),
            twitter VARCHAR(255),
            linkedin VARCHAR(255),
            instagram VARCHAR(255),
            youtube VARCHAR(255),
            e_commerce BOOLEAN DEFAULT FALSE NOT NULL,
            fecha_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            deleted BOOLEAN DEFAULT FALSE
        );
        """
        try:
            self.execute_query(create_table_query, return_df=False)
        except Exception as e:
            print(f"Error creating table: {e}")
            
    def reset_database(self):
        drop_query = "DROP TABLE IF EXISTS sociedades;"
        self.execute_query(drop_query, return_df=False)
        self.create_table_if_not_exists()
