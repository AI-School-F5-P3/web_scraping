import os
import sys
import random
import pandas as pd
import psycopg2
from dotenv import load_dotenv

# Cargar las variables de entorno desde .env
load_dotenv()

# Credenciales y parámetros de la BD
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "admin")
DB_NAME = os.getenv("DB_NAME", "web_scraping")

def main():
    """
    Fase 2: Ingesta de datos desde un CSV a la tabla 'sociedades'.
    1. Pide la ruta del CSV (o la define en el código).
    2. Genera un lote_id (ej. 10 dígitos aleatorios) y pide un 'created_by'.
    3. Lee el CSV con pandas y lo inserta en la BD (tabla 'sociedades').
    """
    # Ruta del CSV (puedes cambiarlo o leerlo desde input)
    csv_path = input("Ingresa la ruta del CSV a cargar: ")
    if not csv_path:
        print("No se proporcionó una ruta de CSV. Saliendo...")
        sys.exit(1)

    # Nombre de usuario (created_by)
    created_by = input("Ingresa tu nombre (para 'created_by'): ")
    if not created_by:
        created_by = "UnknownUser"

    # Generamos un lote_id de 10 dígitos (cero-padding si se desea)
    batch_id = f"{random.randint(1, 9999999999):010d}"

    print(f"\n>>> Cargando archivo '{csv_path}' con LOTE_ID={batch_id}, created_by='{created_by}'")

    # 1) Leer CSV con pandas
    try:
        df = pd.read_csv(csv_path, encoding='utf-8')
    except Exception as e:
        print(f"Error leyendo el CSV: {e}")
        sys.exit(1)

    # 2) Insertar en BD
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

        # Recorremos cada fila del DataFrame y hacemos INSERT
        rows_inserted = 0

        for idx, row in df.iterrows():
            # Ajustar el mapeo con las columnas REALES de tu CSV y tu tabla
            # Ejemplo asumiendo tu CSV trae EXACTAMENTE estas columnas:
            # cod_infotel, nif, razon_social, domicilio, codigo_postal,
            # nom_poblacion, nom_provincia, url, telefono, email, facebook, twitter, instagram, ecommerce
            # Otras columnas como url_limpia, estado_url se completan después (scraping).
            try:
                cod_infotel_val = int(row['cod_infotel']) if not pd.isna(row['cod_infotel']) else None
                nif_val = str(row['nif']) if not pd.isna(row['nif']) else None
                razon_social_val = str(row['razon_social']) if not pd.isna(row['razon_social']) else None
                domicilio_val = str(row['domicilio']) if not pd.isna(row['domicilio']) else None
                codigo_postal_val = str(row['codigo_postal']) if not pd.isna(row['codigo_postal']) else None
                nom_poblacion_val = str(row['nom_poblacion']) if not pd.isna(row['nom_poblacion']) else None
                nom_provincia_val = str(row['nom_provincia']) if not pd.isna(row['nom_provincia']) else None
                url_val = str(row['url']) if 'url' in row and not pd.isna(row['url']) else None
                telefono_val = str(row['telefono']) if 'telefono' in row and not pd.isna(row['telefono']) else None
                email_val = str(row['email']) if 'email' in row and not pd.isna(row['email']) else None
                facebook_val = str(row['facebook']) if 'facebook' in row and not pd.isna(row['facebook']) else None
                twitter_val = str(row['twitter']) if 'twitter' in row and not pd.isna(row['twitter']) else None
                instagram_val = str(row['instagram']) if 'instagram' in row and not pd.isna(row['instagram']) else None
                ecommerce_val = row['ecommerce'] if 'ecommerce' in row else None
                if pd.isna(ecommerce_val):
                    ecommerce_val = None

                # Sentencia SQL con ON CONFLICT (cod_infotel) DO NOTHING
                sql_insert = """
                INSERT INTO sociedades (
                    cod_infotel,
                    nif,
                    razon_social,
                    domicilio,
                    codigo_postal,
                    nom_poblacion,
                    nom_provincia,
                    url,
                    telefono,
                    email,
                    facebook,
                    twitter,
                    instagram,
                    ecommerce,
                    lote_id,
                    created_by,
                    fecha_creacion,
                    fecha_actualizacion
                )
                VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, NOW(), NOW()
                )
                ON CONFLICT (cod_infotel) DO NOTHING
                """

                values = (
                    cod_infotel_val,
                    nif_val,
                    razon_social_val,
                    domicilio_val,
                    codigo_postal_val,
                    nom_poblacion_val,
                    nom_provincia_val,
                    url_val,
                    telefono_val,
                    email_val,
                    facebook_val,
                    twitter_val,
                    instagram_val,
                    ecommerce_val,
                    batch_id,
                    created_by
                )

                cur.execute(sql_insert, values)
                # Si se insertó, rowcount puede no reflejarlo en psycopg2 con DO NOTHING,
                # así que contaremos manualmente.
                rows_inserted += 1

            except Exception as ex_row:
                # Si hay algún problema con la fila, la saltamos y continuamos.
                print(f"[WARN] Fila {idx} no insertada: {ex_row}")

        print(f"\n>>> Ingesta completada. Se procesaron {len(df)} filas. (Intentados inserts)")
        print(f">>> Filas insertadas (sin conflictos): {rows_inserted}")

        cur.close()
        conn.close()

    except Exception as e:
        print(f"Error conectando o insertando en la BD: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
