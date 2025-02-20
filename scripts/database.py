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
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params or ())
                if return_df and query.strip().lower().startswith("select"):
                    columns = [desc[0] for desc in cursor.description]
                    return pd.DataFrame(cursor.fetchall(), columns=columns)
                elif query.strip().lower().startswith("select"):
                    return cursor.fetchall()
                return None
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
        """
        Inserta un lote de registros en la base de datos, ignorando duplicados
        """
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
                        # Convertir los valores a una lista de tuplas, reemplazando string vacío por None
                        values = [
                            tuple(None if (isinstance(v, str) and v.strip() == '') else v 
                                  for v in row)
                            for row in chunk[columns].values
                        ]
                        
                        insert_query = f"""
                        INSERT INTO {table} ({', '.join(columns)})
                        VALUES %s
                        ON CONFLICT ON CONSTRAINT idx_sociedades_cod_infotel DO NOTHING
                        """
                        execute_values(cursor, insert_query, values)
                        total_inserted += cursor.rowcount
                    except Exception as e:
                        # Intentar con una sintaxis alternativa si la primera falla
                        try:
                            insert_query = f"""
                            INSERT INTO {table} ({', '.join(columns)})
                            VALUES %s
                            ON CONFLICT (cod_infotel) DO NOTHING
                            """
                            execute_values(cursor, insert_query, values)
                            total_inserted += cursor.rowcount
                        except Exception as e2:
                            errors.append(str(e2))
                            self.connection.rollback()

            return {
                "status": "success" if total_inserted > 0 else "no_changes",
                "inserted": total_inserted,
                "total": len(df),
                "errors": errors
            }
        except Exception as e:
            return {
                "status": "error",
                "message": str(e),
                "errors": errors
            }

    def save_batch(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Procesa y guarda un lote de registros evitando duplicados
        """
        try:
            # Limpiar strings vacíos y espacios en blanco antes del procesamiento
            df = df.replace(r'^\s*$', None, regex=True)
            df = df.replace({np.nan: None})
            
            # Procesar y validar datos
            processed_df, errors = self.data_processor.process_dataframe(df)
            
            if errors:
                return {
                    "status": "error",
                    "message": "Errores de validación",
                    "errors": errors
                }
            
            # Columnas para inserción
            insert_columns = [
                'cod_infotel', 'nif', 'razon_social', 'domicilio', 'cod_postal',
                'nom_poblacion', 'nom_provincia', 'url', 'url_exists', 'url_limpia',
                'url_status'
            ]
            
            # Obtener códigos existentes
            query = "SELECT cod_infotel FROM sociedades"
            existing_df = self.execute_query(query, return_df=True)
            
            if existing_df is not None and not existing_df.empty:
                existing_codes = set(existing_df['cod_infotel'].tolist())
                new_records = processed_df[~processed_df['cod_infotel'].isin(existing_codes)]
                
                if new_records.empty:
                    return {
                        "status": "success",
                        "inserted": 0,
                        "total": len(df),
                        "message": "No hay nuevos registros para insertar"
                    }
                
                return self.batch_insert(new_records, 'sociedades', insert_columns)
            else:
                return self.batch_insert(processed_df, 'sociedades', insert_columns)
                
        except Exception as e:
            return {
                "status": "error",
                "message": str(e),
                "inserted": 0,
                "total": len(df)
            }
                    
    def get_urls_for_scraping(self, batch_id: str = None, limit: int = 100) -> pd.DataFrame:
        query = """
        SELECT cod_infotel, url
        FROM sociedades
        WHERE deleted = FALSE 
        AND url IS NOT NULL
        AND url_status IS NULL
        """
            
        query += f" LIMIT {limit}"
        return self.execute_query(query, return_df=True)

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
        """Crea la tabla con índice único en cod_infotel"""
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
        
        -- Crear índice único si no existe
        CREATE UNIQUE INDEX IF NOT EXISTS idx_sociedades_cod_infotel 
        ON sociedades(cod_infotel);
        """
        try:
            self.execute_query(create_table_query, return_df=False)
        except Exception as e:
            print(f"Error creating table: {e}")
            
    def reset_database(self):
        """Elimina y recrea la tabla sociedades"""
        try:
            # Primero eliminamos la tabla si existe
            drop_query = "DROP TABLE IF EXISTS sociedades CASCADE;"
            self.execute_query(drop_query)
            
            # Luego recreamos la tabla con el índice único
            self.create_table_if_not_exists()
            
            return {
                "status": "success",
                "message": "Base de datos reiniciada exitosamente"
            }
        except Exception as e:
            return {
                "status": "error",
                "message": f"Error al reiniciar la base de datos: {str(e)}"
            }