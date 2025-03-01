import json
import os
import pandas as pd
import numpy as np
from typing import Optional, List, Dict, Any
from datetime import datetime
from supabase_config import SUPABASE_DB_CONFIG
from db_validator import DataProcessor
from supabase import create_client
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()
# Obtener variables de entorno
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

# Verificar si las variables esenciales están definidas
if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Variables de entorno SUPABASE_URL y SUPABASE_KEY deben estar definidas")

class SupabaseDatabaseManager:
    def __init__(self):
        self.supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        self.db_params = {"url": SUPABASE_URL, "key": SUPABASE_KEY}
        self.data_processor = DataProcessor()
        print("Conexión a Supabase establecida correctamente")
        
        # Verificar si la tabla existe
        self.create_table_if_not_exists()

    def execute_query(self, query: str, params: tuple = None, return_df: bool = False) -> Optional[pd.DataFrame]:
        """
        Ejecuta una consulta a través de la API de Supabase
        Nota: Este método emula el comportamiento de execute_query de la clase original,
        pero usa la API de Supabase en lugar de conexión directa PostgreSQL
        """
        try:
            print(f"Ejecutando consulta: {query}")
            
            # Para consultas SELECT
            if query.strip().upper().startswith("SELECT"):
                # Si es una consulta COUNT, usar función count de Supabase
                if "COUNT(*)" in query.upper():
                    # Extraer nombre de tabla
                    table_name = self._extract_table_name(query)
                    print(f"Consultando tabla: {table_name}")
                    
                    # Construir consulta con Supabase API
                    response = self.supabase.table(table_name).select('*', count='exact').execute()
                    
                    # Procesar resultado específicamente para COUNT
                    count = response.count if hasattr(response, 'count') else 0
                    
                    if return_df:
                        # Crear un DataFrame con las columnas esperadas en la consulta original
                        # Esto soluciona el problema del KeyError: 'total'
                        result_dict = {
                            'total': count,
                            'processed': 0,  # Valores predeterminados
                            'unprocessed': 0,
                            'with_url': 0,
                            'no_url_processed': 0
                        }
                        
                        # Intentar llenar con datos más precisos si es posible
                        if response.data:
                            # Contar registros procesados
                            processed = sum(1 for item in response.data if item.get('processed', False))
                            result_dict['processed'] = processed
                            result_dict['unprocessed'] = count - processed
                            
                            # Contar registros con URL
                            with_url = sum(1 for item in response.data if item.get('url_exists', False))
                            result_dict['with_url'] = with_url
                            
                            # Contar registros sin URL pero procesados
                            no_url_processed = sum(1 for item in response.data 
                                                if item.get('processed', False) and not item.get('url_exists', False))
                            result_dict['no_url_processed'] = no_url_processed
                        
                        df = pd.DataFrame([result_dict])
                        print(f"DataFrame creado con columnas: {df.columns.tolist()}")
                        return df
                    else:
                        return count
                else:
                    # Para otras consultas SELECT normales
                    table_name = self._extract_table_name(query)
                    response = self.supabase.table(table_name).select('*').execute()
                    
                    if return_df and response.data:
                        df = pd.DataFrame(response.data)
                        return df
                    else:
                        return response.data
            else:
                # Para otras consultas, por ahora retornamos None
                print(f"Advertencia: Consulta no compatible con API de Supabase: {query}")
                return None
                
        except Exception as e:
            print(f"Database error: {str(e)}")
            traceback.print_exc()
            return None
    
    def _extract_table_name(self, query: str) -> str:
        """
        Extrae el nombre de la tabla de una consulta SQL simple
        """
        # Buscar patrón "FROM table_name"
        import re
        match = re.search(r'FROM\s+(\w+)', query, re.IGNORECASE)
        if match:
            return match.group(1)
        return "sociedades"  # Default table name

    def _handle_db_error(self, error: Exception, query: str) -> None:
        """
        Handles database errors in a more informative way.
        """
        error_msg = f"Database error: {str(error)}\nQuery: {query}"
        print(error_msg)  # For logging
        raise type(error)(error_msg)  # Re-raise with more context

    def batch_insert(self, df: pd.DataFrame, table: str, columns: List[str]) -> Dict[str, Any]:
        chunk_size = 1000
        total_inserted = 0
        errors = []

        try:
            print(f"Attempting to insert {len(df)} records into {table} with columns: {columns}")
            
            # Convertir strings vacíos y espacios en blanco a None/NULL
            df = df.replace(r'^\s*$', None, regex=True)
            # Convertir NaN a None
            df = df.replace({np.nan: None})
            # Convertir cod_infotel a entero
            if 'cod_infotel' in df.columns:
                try:
                    # Primero, asegúrate de que no hay None o NaN
                    df['cod_infotel'] = df['cod_infotel'].fillna(0)
                    # Luego convierte a entero
                    df['cod_infotel'] = df['cod_infotel'].astype(float).astype(int)
                    print("cod_infotel convertido a entero correctamente")
                except Exception as e:
                    print(f"Error al convertir cod_infotel a entero: {e}")
                    # Buscar valores problemáticos
                    problematic = []
                    for idx, val in df['cod_infotel'].items():
                        try:
                            int(float(val))
                        except:
                            problematic.append((idx, val))
                    if problematic:
                        print(f"Valores problemáticos (idx, valor): {problematic[:10]}")
            # Dividir en lotes
            df_chunks = [df[i:i + chunk_size] for i in range(0, len(df), chunk_size)]
            
            for chunk_idx, chunk in enumerate(df_chunks):
                try:
                    print(f"Processing chunk {chunk_idx+1}/{len(df_chunks)} with {len(chunk)} records")
                    
                    # Preparar datos para inserción
                    records = []
                    for _, row in chunk[columns].iterrows():
                        record = {}
                        for col in columns:
                            record[col] = row[col]
                        records.append(record)
                    
                    print(f"Prepared {len(records)} records for insertion")
                    
                    # Insertar usando la API de Supabase
                    response = self.supabase.table(table).insert(records).execute()
                    
                    print(f"Supabase response type: {type(response)}")
                    print(f"Supabase response: {response}")
                    
                    if hasattr(response, 'data') and response.data:
                        print(f"Successfully inserted {len(records)} records")
                        total_inserted += len(records)
                    else:
                        error_msg = "No se insertaron registros (respuesta vacía)"
                        print(f"Error: {error_msg}")
                        errors.append(error_msg)
                except Exception as e:
                    print(f"Exception during chunk processing: {str(e)}")
                    errors.append(str(e))
            
            return {
                "status": "success" if total_inserted == len(df) else "partial",
                "inserted": total_inserted,
                "total": len(df),
                "errors": errors
            }
        except Exception as e:
            print(f"Exception in batch_insert: {str(e)}")
            return {"status": "error", "message": str(e), "errors": errors}

    def save_batch(self, df: pd.DataFrame, check_duplicates: bool = False) -> Dict[str, Any]:
        print(f"Original dataframe shape: {df.shape}")
        print(f"Columns in dataframe: {df.columns.tolist()}")
        
        # Limpiar strings vacíos y espacios en blanco antes del procesamiento
        df = df.replace(r'^\s*$', None, regex=True)
        df = df.replace({np.nan: None})
        
        # Asegurar limpieza de espacios en blanco antes de guardar
        df = self.data_processor.validator.clean_text_fields(df)
        df['nom_provincia'] = df['nom_provincia'].astype(str).str.strip()    
        
        # Procesar y validar datos
        df, errors = self.data_processor.process_dataframe(df)
        print(f"After processing, dataframe shape: {df.shape}")
        
        if errors:
            print(f"Found {len(errors)} validation errors. First 5: {errors[:5]}")
            return {
                "status": "error",
                "message": "Errores de validación",
                "errors": errors
            }
                
        insert_columns = [
        'cod_infotel', 'nif', 'razon_social', 'domicilio', 'cod_postal',
        'nom_poblacion', 'nom_provincia', 'url'
    ]
        
        # Asegúrate que cod_infotel es entero
        if 'cod_infotel' in df.columns:
            df['cod_infotel'] = df['cod_infotel'].fillna(0)
            df['cod_infotel'] = df['cod_infotel'].astype(float).astype(int)
        
        if check_duplicates:
            df = df.drop_duplicates(subset=['cod_infotel'])
        
        # Solo pasar las columnas originales
        return self.batch_insert(df, 'sociedades', insert_columns)
        
        

    def get_urls_for_scraping(self, limit: int = 10) -> pd.DataFrame:
        """Gets URLs that need to be scraped."""
        # Usando API de Supabase directamente
        response = self.supabase.table('sociedades') \
            .select('cod_infotel, url') \
            .not_is('url', 'null') \
            .neq('url', '') \
            .limit(limit) \
            .execute()
            
        if response.data:
            return pd.DataFrame(response.data)
        return pd.DataFrame()

    def get_record_count(self) -> int:
        """Gets total number of records in the sociedades table."""
        # Usando API de Supabase para contar
        response = self.supabase.table('sociedades').select('*', count='exact').execute()
        return response.count if hasattr(response, 'count') else 0
    
    def create_table_if_not_exists(self):
        # En Supabase, no podemos crear tablas a través de la API directamente
        # Esta función queda como placeholder
        print("Nota: La creación de tablas en Supabase debe realizarse desde la interfaz de Supabase SQL Editor")
        pass
            
    def update_scraping_results(self, results: List[Dict[str, Any]], worker_id: str = None) -> Dict[str, Any]:
        """Actualiza los resultados de scraping"""
        updated_companies = []
        
        for result in results:
            try:
                cod_infotel = result.get('cod_infotel')
                if not cod_infotel:
                    continue
                
                # Preparar datos para actualización
                update_data = {
                    'url_exists': result.get('url_exists', False),
                    'url_valida': result.get('url_valida', ''),
                    'url_limpia': result.get('url_limpia', ''),
                    'url_status': result.get('url_status', -1),
                    'url_status_mensaje': result.get('url_status_mensaje', ''),
                    'telefono_1': result.get('phones', ['', '', ''])[0] if result.get('phones') else '',
                    'telefono_2': result.get('phones', ['', '', ''])[1] if result.get('phones') and len(result.get('phones')) > 1 else '',
                    'telefono_3': result.get('phones', ['', '', ''])[2] if result.get('phones') and len(result.get('phones')) > 2 else '',
                    'facebook': result.get('social_media', {}).get('facebook', ''),
                    'twitter': result.get('social_media', {}).get('twitter', ''),
                    'linkedin': result.get('social_media', {}).get('linkedin', ''),
                    'instagram': result.get('social_media', {}).get('instagram', ''),
                    'youtube': result.get('social_media', {}).get('youtube', ''),
                    'e_commerce': result.get('is_ecommerce', False),
                    'worker_id': worker_id,
                    'processed': True
                }
                
                # Actualizar usando la API de Supabase
                response = self.supabase.table('sociedades').update(update_data).eq('cod_infotel', cod_infotel).execute()
                
                if response.data:
                    updated_companies.append(cod_infotel)
                    print(f"✅ Empresa {cod_infotel} actualizada")
                else:
                    print(f"⚠️ No se actualizó la empresa {cod_infotel}")
                    
            except Exception as e:
                print(f"❌ Error actualizando empresa {result.get('cod_infotel')}: {str(e)}")
                continue
        
        return {"status": "success", "updated": len(updated_companies)}