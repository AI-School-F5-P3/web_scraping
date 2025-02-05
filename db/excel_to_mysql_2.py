import os
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, text
import numpy as np
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

def find_excel_file(filename):
    """
    Busca el archivo Excel en directorios padre
    
    Parámetros:
    - filename: Nombre del archivo Excel a buscar
    
    Retorna:
    - Ruta completa del archivo si se encuentra
    - None si no se encuentra
    """
    current_dir = os.path.abspath(os.getcwd())
    
    # Lista de posibles ubicaciones relativas
    possible_paths = [
        os.path.join(current_dir, filename),
        os.path.join(current_dir, 'data', filename),
        os.path.join(current_dir, '..', 'data', filename),
        os.path.join(current_dir, '..', '..', 'data', filename),
        os.path.join(current_dir, 'WEB_SCRAPING', 'data', filename)
    ]
    
    for path in possible_paths:
        if os.path.exists(path):
            print(f"Archivo encontrado en: {path}")
            return path
    
    return None

def procesar_codigo_postal(cp):
    """
    Procesa el código postal:
    - Convierte a cadena
    - Rellena con ceros a la izquierda hasta 5 caracteres
    """
    if pd.isna(cp):
        return None
    
    # Convertir a cadena y rellenar con ceros a la izquierda
    return str(cp).zfill(5)

def process_excel_to_mysql(
    excel_filename='Muestra50K_Telefonos_20250204.xlsx', 
    db_name='empresas_db', 
    table_name='sociedades'
):
    """
    Procesa un archivo Excel y carga los datos en una base de datos MySQL
    con búsqueda de archivo flexible
    """
    try:
        # Buscar archivo Excel
        excel_path = find_excel_file(excel_filename)
        
        if not excel_path:
            raise FileNotFoundError(f"No se pudo encontrar el archivo: {excel_filename}")
        
        # Leer el archivo Excel
        df = pd.read_excel(excel_path)

        # Normalizar código postal
        df['COD_POSTAL'] = df['COD_POSTAL'].apply(procesar_codigo_postal) 

        # Añadir las nuevas columnas con valores predeterminados
        df['estado_pagina_web'] = 'No verificado'
        
        # Añadir tres columnas de teléfono
        df['telefono_1'] = np.nan
        df['telefono_2'] = np.nan
        df['telefono_3'] = np.nan
        
        # Añadir columnas de redes sociales (boolean)
        df['facebook'] = False
        df['twitter'] = False
        df['linkedin'] = False
        df['instagram'] = False
        df['youtube'] = False
        
        # Columna de comercio electrónico (boolean)
        df['comercio_electronico'] = False
        
        # Cargar credenciales desde variables de entorno
        username = os.getenv('MYSQL_USER', 'tu_usuario')
        password = os.getenv('MYSQL_PASSWORD', 'tu_contraseña')
        host = os.getenv('MYSQL_HOST', 'localhost')
        
        # Crear conexión al servidor MySQL
        engine = create_engine(f'mysql+pymysql://{username}:{password}@{host}/')
        
        # Crear base de datos si no existe
        with engine.connect() as connection:
            connection.execute(text(f'CREATE DATABASE IF NOT EXISTS {db_name}'))
            connection.execute(text(f'USE {db_name}'))
        
        # Reconectar a la base de datos específica
        engine = create_engine(f'mysql+pymysql://{username}:{password}@{host}/{db_name}')
        
        # Escribir DataFrame a MySQL
        df.to_sql(
            name=table_name, 
            con=engine, 
            if_exists='replace',  # Reemplaza la tabla si ya existe
            index=False,  # No incluir índice como columna
            dtype={
                # Definir tipos de datos específicos
                'COD_INFOTEL': sqlalchemy.types.Integer(),
                'estado_pagina_web': sqlalchemy.types.String(50),
                'telefono_1': sqlalchemy.types.String(20),
                'telefono_2': sqlalchemy.types.String(20),
                'telefono_3': sqlalchemy.types.String(20),
                'facebook': sqlalchemy.types.Boolean(),
                'twitter': sqlalchemy.types.Boolean(),
                'linkedin': sqlalchemy.types.Boolean(),
                'instagram': sqlalchemy.types.Boolean(),
                'youtube': sqlalchemy.types.Boolean(),
                'comercio_electronico': sqlalchemy.types.Boolean()
            }
        )
        
        # Establecer COD_INFOTEL como clave primaria
        with engine.connect() as connection:
            connection.execute(text(f'''
            ALTER TABLE {table_name}
            ADD PRIMARY KEY (COD_INFOTEL)
            '''))
        
        print(f"Datos procesados y guardados en la base de datos {db_name}, tabla {table_name}")
        
        # Verificar número de registros insertados
        with engine.connect() as connection:
            result = connection.execute(text(f'SELECT COUNT(*) FROM {table_name}'))
            print(f"Número total de registros: {result.scalar()}")
    
    except Exception as e:
        print(f"Error en el procesamiento: {e}")
        import traceback
        traceback.print_exc()  # Imprime el traceback completo para diagnóstico

# Uso del script con valores predeterminados
if __name__ == "__main__":
    process_excel_to_mysql()