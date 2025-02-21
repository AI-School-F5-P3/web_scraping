# validators/data_validator.py

# validators/data_validator.py

import pandas as pd
import re
from urllib.parse import urlparse
import requests
from typing import Tuple

class DataValidator:
    @staticmethod
    def clean_text_fields(df: pd.DataFrame) -> pd.DataFrame:
        """
        Limpia espacios en blanco al inicio y final de todas las columnas de texto.
        """
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

        return df
        
    @staticmethod
    def validate_cod_infotel(df: pd.DataFrame) -> Tuple[bool, str]:
        """
        Valida que COD_INFOTEL sean valores únicos y no contenga nulos
        """
        # Verificar nulos
        null_count = df['cod_infotel'].isnull().sum()
        if null_count > 0:
            return False, f"Existen {null_count} valores nulos en COD_INFOTEL"
            
        # Verificar duplicados
        duplicates = df[df.duplicated(['cod_infotel'])]
        if not duplicates.empty:
            duplicate_values = duplicates['cod_infotel'].tolist()
            return False, f"Valores duplicados en COD_INFOTEL: {duplicate_values}"
            
        return True, "Validación de COD_INFOTEL correcta"

    @staticmethod
    def validate_and_clean_postal_code(df: pd.DataFrame) -> pd.DataFrame:
        """
        Asegura que los códigos postales tengan 5 dígitos
        """
        df['cod_postal'] = df['cod_postal'].astype(str)
        df['cod_postal'] = df['cod_postal'].apply(lambda x: x.zfill(5) if x.isdigit() else x)
        return df

    @staticmethod
    def validate_and_clean_urls(df: pd.DataFrame) -> pd.DataFrame:
        """
        Valida y limpia URLs, creando las columnas requeridas.
        URLs vacías o con solo espacios en blanco se convierten en None
        """
        def clean_url(url: str) -> str:
            """Extrae el dominio de una URL"""
            if pd.isna(url):
                return None
            # Limpiar espacios en blanco
            url = str(url).strip()
            if url == '':
                return None
            try:
                parsed = urlparse(url if url.startswith(('http://', 'https://')) else f'http://{url}')
                domain = parsed.netloc
                return domain if domain else None
            except:
                return None

        def check_url_status(url: str) -> int:
            """Verifica el estado de una URL"""
            if url is None:
                return None
            try:
                response = requests.head(
                    f'http://{url}' if not url.startswith(('http://', 'https://')) else url,
                    timeout=5,
                    allow_redirects=True
                )
                return response.status_code
            except requests.RequestException:
                return -1

        # Limpiar la columna URL original
        df['url'] = df['url'].apply(lambda x: None if pd.isna(x) or str(x).strip() == '' else str(x).strip())
        
        # Crear columna URL_EXISTS
        df['url_exists'] = df['url'].apply(lambda x: False if x is None else True)
        
        # Crear columna URL_LIMPIA
        df['url_limpia'] = df['url'].apply(clean_url)
        
        # Crear columna URL_STATUS
        df['url_status'] = df['url_limpia'].apply(check_url_status)
        
        return df

class DataProcessor:
    def __init__(self):
        self.validator = DataValidator()

    def process_dataframe(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, list]:
        """
        Procesa el DataFrame aplicando todas las validaciones y transformaciones
        """
        errors = []

        # Limpiar campos de texto
        df = self.validator.clean_text_fields(df)        
        
        # Validar COD_INFOTEL
        is_valid, message = self.validator.validate_cod_infotel(df)
        if not is_valid:
            errors.append(message)
            
        # Limpiar y validar códigos postales
        df = self.validator.validate_and_clean_postal_code(df)
        
        # Validar y limpiar URLs
        df = self.validator.validate_and_clean_urls(df)
        
        return df, errors