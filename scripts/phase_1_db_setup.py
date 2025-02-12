import os
from dotenv import load_dotenv
import psycopg2
import sys

# Cargar variables de entorno desde .env
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

def main():
    create_database_if_not_exists()
    create_sociedades_table()

def create_database_if_not_exists():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname="postgres",  # Usamos la BD postgres por defecto
            user=DB_USER,
            password=DB_PASSWORD
        )
        conn.autocommit = True
        cur = conn.cursor()

        # Verificar si existe la BD
        cur.execute(f"SELECT 1 FROM pg_database WHERE datname='{DB_NAME}';")
        exists = cur.fetchone()
        if not exists:
            cur.execute(f"CREATE DATABASE {DB_NAME};")
            print(f">>> Base de datos '{DB_NAME}' creada.")
        else:
            print(f">>> La base de datos '{DB_NAME}' ya existe.")

        cur.close()
        conn.close()
    except Exception as e:
        print("Error creando la BD:", e)
        sys.exit(1)

def create_sociedades_table():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        conn.autocommit = True
        cur = conn.cursor()

        create_table_sql = """
        CREATE TABLE IF NOT EXISTS sociedades (
            cod_infotel            BIGINT PRIMARY KEY,
            nif                    VARCHAR(20) NOT NULL,
            razon_social           VARCHAR(255),
            domicilio             VARCHAR(255),
            codigo_postal          VARCHAR(10),
            nom_poblacion          VARCHAR(100),
            nom_provincia          VARCHAR(100),
            url                    VARCHAR(1000),
            url_limpia             VARCHAR(300),
            estado_url             VARCHAR(20),
            url_valida             BOOLEAN,
            telefono               VARCHAR(50),
            email                  VARCHAR(255),
            facebook               VARCHAR(300),
            twitter                VARCHAR(300),
            instagram              VARCHAR(300),
            ecommerce              BOOLEAN,
            lote_id                VARCHAR(50),
            created_by             VARCHAR(100),
            deleted                BOOLEAN DEFAULT FALSE,
            fecha_creacion         TIMESTAMP DEFAULT NOW(),
            fecha_actualizacion    TIMESTAMP
        );
        """
        cur.execute(create_table_sql)
        print(">>> Tabla 'sociedades' creada o ya existente.")

        cur.close()
        conn.close()
    except Exception as e:
        print("Error creando la tabla 'sociedades':", e)
        sys.exit(1)

if __name__ == "__main__":
    main()
