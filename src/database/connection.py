import sqlalchemy as sa
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

def create_database(username, password, host, db_name):
    """
    Create database if it does not exist and return a new engine connected to it.
    """
    # Crear engine sin base de datos para verificar/crear la DB
    temp_engine = create_engine(f'mysql+pymysql://{username}:{password}@{host}/')
    
    with temp_engine.connect() as connection:
        connection.execute(text(f'CREATE DATABASE IF NOT EXISTS {db_name}'))
    
    print(f"Database '{db_name}' created or verified")

    # Crear un nuevo engine conectado a la base de datos recién creada
    return create_engine(f'mysql+pymysql://{username}:{password}@{host}/{db_name}')

def create_engine_connection(username, password, host, db_name):
    """
    Create and return a SQLAlchemy engine connection.
    """
    try:
        engine = create_engine(f'mysql+pymysql://{username}:{password}@{host}/{db_name}')
        print("Database engine created successfully")
        return engine
    except SQLAlchemyError as e:
        print(f"Error creating database engine: {e}")
        return None