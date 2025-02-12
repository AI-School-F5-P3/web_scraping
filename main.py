import os
from dotenv import load_dotenv
from src.database.connection import create_database, create_engine_connection
from src.database.models import create_table
from src.utils.data_validation import validate_and_prepare_dataframe
from src.utils.url_processing import process_urls

def main():
    # Load environment variables
    load_dotenv(override=True)

    # Get database credentials
    username = os.getenv('MYSQL_USER')
    password = os.getenv('MYSQL_PASSWORD')
    host = os.getenv('MYSQL_HOST')
    db_name = os.getenv('MYSQL_DATABASE')

    # Verify credentials
    if not all([username, password, host, db_name]):
        raise ValueError("Missing credentials in .env file")

    # Crear base de datos y obtener el engine conectado a ella
    engine = create_database(username, password, host, db_name)

    # Crear la tabla en la base de datos
    create_table(engine)

    # Process Excel file and insert data
    df = validate_and_prepare_dataframe('Muestra50K_Telefonos_20250114.xlsx')
    
    # Insert data and process URLs
    process_urls(engine, db_name, df)

if __name__ == "__main__":
    main()
