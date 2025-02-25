import json
import psycopg2
import pandas as pd
import numpy as np
from typing import Optional, List, Dict, Any
from psycopg2.extras import execute_values
from supabase_config import SUPABASE_DB_CONFIG
import platform
from db_validator import DataProcessor

class SupabaseDatabaseManager:
    def __init__(self):
        self.connection = psycopg2.connect(**SUPABASE_DB_CONFIG)
        self.connection.autocommit = True
        self.data_processor = DataProcessor()
        self._optimize_connection()
        self.create_table_if_not_exists()

    def _optimize_connection(self):
        with self.connection.cursor() as cursor:
            # Configuraciones básicas de PostgreSQL
            cursor.execute("SET statement_timeout = '60s'")
            cursor.execute("SET lock_timeout = '10s'")
            cursor.execute("SET idle_in_transaction_session_timeout = '60s'")
            cursor.execute("SET client_encoding = 'UTF8'")
            
            # No necesitamos establecer optimizaciones agresivas en Supabase
            cursor.execute("SET random_page_cost = 1.1")
            cursor.execute("SET cpu_tuple_cost = 0.03")
            cursor.execute("SET cpu_index_tuple_cost = 0.01")

    def execute_query(self, query: str, params: tuple = None, return_df: bool = False) -> Optional[pd.DataFrame]:
        """
        Ejecuta una SQL query y opcionalmente retorna resultados como DataFrame.
        """
        try:
            with self.connection.cursor() as cursor:
                # Asegurar que la conexión está viva
                if not self.connection.closed:
                    cursor.execute("SELECT 1")
                else:
                    self._reconnect()
                    
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                    
                if cursor.description:  # Si la consulta retorna resultados
                    columns = [desc[0] for desc in cursor.description]
                    results = cursor.fetchall()
                    
                    if return_df:
                        df = pd.DataFrame(results, columns=columns)
                        # Eliminar duplicados si están presentes
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

    def _reconnect(self):
        try:
            self.connection.close()
        except:
            pass
        self.connection = psycopg2.connect(**SUPABASE_DB_CONFIG)
        self.connection.autocommit = True
        self._optimize_connection()

    # Resto de métodos idénticos a DatabaseManager original
    # ...
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
            processed BOOLEAN DEFAULT FALSE NOT NULL,  -- Nuevo campo
            fecha_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            deleted BOOLEAN DEFAULT FALSE
        );

        -- Crear índice único si no existe
        CREATE UNIQUE INDEX IF NOT EXISTS idx_sociedades_cod_infotel 
        ON sociedades(cod_infotel);
        """
        try:
            self.execute_query(create_table_query, return_df=False)
        except Exception as e:
            print(f"Error creating table: {e}")
            
    def update_scraping_results(self, results: List[Dict[str, Any]], worker_id: str = None) -> Dict[str, Any]:
        try:
            print("\nActualizando resultados en la BD...")
            update_query = """
            UPDATE sociedades 
            SET 
                url_exists = %(url_exists)s,
                url_valida = %(url_valida)s,
                url_limpia = %(url_limpia)s,
                url_status = %(url_status)s,
                url_status_mensaje = %(url_status_message)s,
                telefono_1 = %(telefono_1)s,
                telefono_2 = %(telefono_2)s,
                telefono_3 = %(telefono_3)s,
                facebook = %(facebook)s,
                twitter = %(twitter)s,
                linkedin = %(linkedin)s,
                instagram = %(instagram)s,
                youtube = %(youtube)s,
                e_commerce = %(e_commerce)s,
                worker_id = %(worker_id)s,
                processed = TRUE,
                fecha_actualizacion = NOW()
            WHERE cod_infotel = %(cod_infotel)s RETURNING cod_infotel
            """
            
            updated_companies = []
            with self.connection.cursor() as cursor:
                for result in results:
                    try:
                        # Preparar parámetros
                        params = {
                            'url_exists': result.get('url_exists', False),
                            'url_valida': result.get('url_valida', ''),
                            'url_limpia': result.get('url_limpia', ''),
                            'url_status': result.get('url_status', -1),
                            'url_status_message': result.get('url_status_mensaje', ''),
                            'telefono_1': result.get('phones', ['', '', ''])[0],
                            'telefono_2': result.get('phones', ['', '', ''])[1],
                            'telefono_3': result.get('phones', ['', '', ''])[2],
                            'facebook': result.get('social_media', {}).get('facebook', ''),
                            'twitter': result.get('social_media', {}).get('twitter', ''),
                            'linkedin': result.get('social_media', {}).get('linkedin', ''),
                            'instagram': result.get('social_media', {}).get('instagram', ''),
                            'youtube': result.get('social_media', {}).get('youtube', ''),
                            'e_commerce': result.get('is_ecommerce', False),
                            'worker_id': worker_id,
                            'cod_infotel': result.get('cod_infotel')
                        }

                        cursor.execute(update_query, params)

                        # Verificar si se actualizó la fila
                        if cursor.rowcount > 0:
                            self.connection.commit()
                            updated_companies.append(result.get('cod_infotel'))
                        else:
                            print(f"⚠️ No se actualizó la empresa {result.get('cod_infotel')}.")

                    except Exception as e:
                        print(f"❌ Error actualizando empresa {result.get('cod_infotel')}: {str(e)}")
                        self.connection.rollback()  # Evita afectar otras actualizaciones
                        continue
                
            return {"status": "success", "updated": len(updated_companies)}
            
        except Exception as e:
            self.connection.rollback()
            print(f"❌ Error en update_scraping_results: {str(e)}")
            return {"status": "error", "message": str(e)}