import os
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine, Column, Integer, String, Boolean, text
from sqlalchemy.orm import sessionmaker, declarative_base
import numpy as np
from dotenv import load_dotenv

def validate_data(df):
    """
    Valida los datos del DataFrame antes de la inserción
    """
    # Convertir COD_INFOTEL a entero si no lo es ya
    df['COD_INFOTEL'] = pd.to_numeric(df['COD_INFOTEL'], errors='raise')
    
    # 1. Verificar valores únicos y nulos en COD_INFOTEL
    if df['COD_INFOTEL'].isnull().any():
        raise ValueError("Se encontraron valores nulos en la columna COD_INFOTEL")
    
    if df['COD_INFOTEL'].duplicated().any():
        raise ValueError("Se encontraron valores duplicados en la columna COD_INFOTEL")
        
    print("Validación de COD_INFOTEL completada: sin nulos y valores únicos")
    
    # 2. Normalizar COD_POSTAL
    df['COD_POSTAL'] = df['COD_POSTAL'].astype(str).str.zfill(5)
    print("Normalización de COD_POSTAL completada")
    
    # 3. Convertir strings vacíos a None/NULL
    string_columns = ['NIF', 'RAZON_SOCIAL', 'DOMICILIO', 'NOM_POBLACION', 'NOM_PROVINCIA', 'URL']
    for col in string_columns:
        df[col] = df[col].replace('', None)
        df[col] = df[col].where(pd.notnull(df[col]), None)
    
    return df

def find_excel_file(filename):
    """
    Busca el archivo Excel en directorios padre
    """
    current_dir = os.path.abspath(os.getcwd())
    
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

def create_database(engine, db_name):
    """
    Crea la base de datos si no existe
    """
    with engine.connect() as connection:
        connection.execute(text(f'CREATE DATABASE IF NOT EXISTS {db_name}'))
        connection.execute(text(f'USE {db_name}'))
    print(f"Base de datos '{db_name}' creada o verificada")

def prepare_dataframe(df):
    """
    Prepara el DataFrame añadiendo las nuevas columnas con valores por defecto
    """
    # Añadir nuevas columnas con valores por defecto
    new_columns = {
        'URL_EXISTS': False,
        'URL_VALIDO': None,
        'TELEFONO_1': None,
        'TELEFONO_2': None,
        'TELEFONO_3': None,
        'FACEBOOK': None,
        'TWITTER': None,
        'LINKEDIN': None,
        'INSTAGRAM': None,
        'YOUTUBE': None,
        'E_COMMERCE': False
    }
    
    for col, default_value in new_columns.items():
        if col not in df.columns:
            df[col] = default_value

    # Validar URLs con espacios en blanco
    df['URL'] = df['URL'].apply(lambda x: '' if pd.isna(x) or (isinstance(x, str) and x.strip() == '') else x)
    
    # Establecer URL_EXISTS basado en la columna URL 
    # Ahora considerará False si solo hay espacios en blanco
    df['URL_EXISTS'] = df['URL'].apply(lambda x: bool(x and x.strip()))

    # Asegurar que las columnas booleanas sean del tipo correcto
    df['URL_EXISTS'] = df['URL_EXISTS'].astype(bool)
    df['E_COMMERCE'] = df['E_COMMERCE'].astype(bool)
    
    # Convertir strings vacíos a None en las nuevas columnas
    string_columns = ['URL_VALIDO', 'TELEFONO_1', 'TELEFONO_2', 'TELEFONO_3', 
                    'FACEBOOK', 'TWITTER', 'LINKEDIN', 'INSTAGRAM', 'YOUTUBE']
    for col in string_columns:
        df[col] = df[col].replace('', None)
        df[col] = df[col].where(pd.notnull(df[col]), None)
            
    return df

def main():
    # Cargar variables de entorno
    load_dotenv(override=True)  # Añade el parámetro override=True

    # Obtener las credenciales directamente desde las variables de entorno
    username = os.getenv('MYSQL_USER')
    password = os.getenv('MYSQL_PASSWORD')
    host = os.getenv('MYSQL_HOST')
    db_name = os.getenv('MYSQL_DATABASE')  # Elimina el valor por defecto

    # Imprimir para depuración
    print(f"Nombre de base de datos cargado: {db_name}")

    # Verificar que todas las credenciales estén presentes
    if not all([username, password, host, db_name]):
        raise ValueError("Faltan credenciales en el archivo .env")
    
    # Crear conexión inicial al servidor MySQL
    engine = create_engine(f'mysql+pymysql://{username}:{password}@{host}/')

    # 3. Crear base de datos
    create_database(engine, db_name)

    # Reconectar a la base de datos específica
    engine = create_engine(f'mysql+pymysql://{username}:{password}@{host}/{db_name}')

    # Create a base class for declarative models
    Base = declarative_base()

    # 4. Define the SQLAlchemy model for the table with new columns
    class Sociedad(Base):
        __tablename__ = 'sociedades'
        
        # Original columns
        COD_INFOTEL = Column(Integer, primary_key=True)
        NIF = Column(String(11), nullable=True)
        RAZON_SOCIAL = Column(String(255), nullable=True)
        DOMICILIO = Column(String(255), nullable=True)
        COD_POSTAL = Column(String(5), nullable=True)
        NOM_POBLACION = Column(String(100), nullable=True)
        NOM_PROVINCIA = Column(String(100), nullable=True)
        URL = Column(String(255), nullable=True)
        
        # New columns
        URL_EXISTS = Column(Boolean, default=False, nullable=False)
        URL_VALIDO = Column(String(255), nullable=True)
        TELEFONO_1 = Column(String(16), nullable=True)
        TELEFONO_2 = Column(String(16), nullable=True)
        TELEFONO_3 = Column(String(16), nullable=True)
        FACEBOOK = Column(String(255), nullable=True)
        TWITTER = Column(String(255), nullable=True)
        LINKEDIN = Column(String(255), nullable=True)
        INSTAGRAM = Column(String(255), nullable=True)
        YOUTUBE = Column(String(255), nullable=True)
        E_COMMERCE = Column(Boolean, default=False, nullable=False)

    # Drop table if exists and create new one
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    print("Tabla 'sociedades' recreada con todas las columnas")

    # Buscar archivo Excel
    excel_filename = 'Muestra50K_Telefonos_20250204.xlsx'
    excel_path = find_excel_file(excel_filename)

    if not excel_path:
        raise FileNotFoundError(f"No se pudo encontrar el archivo: {excel_filename}")

    # Read Excel file
    print("Leyendo archivo Excel...")
    df = pd.read_excel(excel_path)
    print(f"Leídos {len(df)} registros del Excel")

    # Validar y normalizar datos
    df = validate_data(df)
    
    # Preparar DataFrame con nuevas columnas
    df = prepare_dataframe(df)

    # 5. Insert data
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Convert DataFrame to list of dictionaries
        records = df.to_dict('records')
        
        # Bulk insert
        session.bulk_insert_mappings(Sociedad, records)
        session.commit()
        print(f"Se insertaron exitosamente {len(records)} registros.")

    except Exception as e:
        session.rollback()
        print(f"Ocurrió un error durante la inserción: {e}")
        # Imprimir más detalles del error si están disponibles
        if hasattr(e, 'orig'):
            print(f"Error original: {e.orig}")

    finally:
        session.close()

    print("Proceso completado.")

if __name__ == "__main__":
    main()