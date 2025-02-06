import os
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, text
import numpy as np
from dotenv import load_dotenv
import phonenumbers

# Cargar variables de entorno
load_dotenv()

def normalizar_telefono(telefono):
    """
    Normaliza números de teléfono usando la librería phonenumbers
    - Convierte a formato internacional
    - Devuelve None si no es un número válido
    """
    if pd.isna(telefono):
        return None
    
    # Convertir a cadena
    telefono = str(telefono)
    
    try:
        # Intentar parsear el número de teléfono para España
        parsed_number = phonenumbers.parse(telefono, "ES")
        
        # Verificar si el número es válido
        if phonenumbers.is_valid_number(parsed_number):
            # Devolver en formato E.164 (con +)
            return phonenumbers.format_number(parsed_number, phonenumbers.PhoneNumberFormat.E164)
        
    except phonenumbers.phonenumberutil.NumberParseException:
        # Si hay error al parsear, intentar agregar prefijo de España
        try:
            # Intentar agregar +34 si no lo tiene
            if not telefono.startswith('+'):
                telefono = f'+34{telefono}'
            
            parsed_number = phonenumbers.parse(telefono, "ES")
            
            if phonenumbers.is_valid_number(parsed_number):
                return phonenumbers.format_number(parsed_number, phonenumbers.PhoneNumberFormat.E164)
        
        except:
            # Si sigue sin funcionar, devolver None
            return None
    
    # Si no se pudo normalizar
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
    con normalización de datos
    """
    try:
        # Buscar archivo Excel
        excel_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', excel_filename)
        
        # Leer el archivo Excel
        df = pd.read_excel(excel_path)
        
        # Normalizar código postal
        df['COD_POSTAL'] = df['COD_POSTAL'].apply(procesar_codigo_postal)
        
        # Normalizar teléfonos
        df['telefono_1'] = df['telefono_1'].apply(normalizar_telefono)
        df['telefono_2'] = df['telefono_2'].apply(normalizar_telefono)
        df['telefono_3'] = df['telefono_3'].apply(normalizar_telefono)
        
        # Columnas para redes sociales y comercio electrónico
        df['estado_pagina_web'] = 'No verificado'
        df['facebook'] = False
        df['twitter'] = False
        df['linkedin'] = False
        df['instagram'] = False
        df['comercio_electronico'] = False
        
        # Cargar credenciales
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
        
        # Definir tipos de datos para la tabla
        dtype_dict = {
            colname: sqlalchemy.types.VARCHAR(length=255) 
            for colname in df.columns if colname not in ['COD_INFOTEL', 'COD_POSTAL', 'telefono_1', 'telefono_2', 'telefono_3']
        }
        
        # Ajustes específicos de tipos
        dtype_dict.update({
            'COD_INFOTEL': sqlalchemy.types.Integer(),
            'COD_POSTAL': sqlalchemy.types.VARCHAR(5),
            'telefono_1': sqlalchemy.types.VARCHAR(15),
            'telefono_2': sqlalchemy.types.VARCHAR(15),
            'telefono_3': sqlalchemy.types.VARCHAR(15),
            'facebook': sqlalchemy.types.Boolean(),
            'twitter': sqlalchemy.types.Boolean(),
            'linkedin': sqlalchemy.types.Boolean(),
            'instagram': sqlalchemy.types.Boolean(),
            'comercio_electronico': sqlalchemy.types.Boolean()
        })
        
        # Escribir DataFrame a MySQL
        df.to_sql(
            name=table_name, 
            con=engine, 
            if_exists='replace',  # Reemplaza la tabla si ya existe
            index=False,  # No incluir índice como columna
            dtype=dtype_dict
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