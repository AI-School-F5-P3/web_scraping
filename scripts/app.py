import os
import re
import random
import requests
import psycopg2
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from bs4 import BeautifulSoup

# ----------------------------------------------------------------
# 1) Cargar variables de entorno
# ----------------------------------------------------------------
load_dotenv()
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "admin")
DB_NAME = os.getenv("DB_NAME", "web_scraping")

# Si usas Ollama o similar:
OLLAMA_URL = "http://localhost:11434/api/generate"

# ----------------------------------------------------------------
# 2) Conexión y consultas a la BD
# ----------------------------------------------------------------
def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

def run_query(query, params=None):
    conn = get_connection()
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(query, params or ())
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def run_action(query, params=None):
    conn = get_connection()
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(query, params or ())
    cur.close()
    conn.close()

# ----------------------------------------------------------------
# 3) Funciones de procesamiento de archivos
# ----------------------------------------------------------------
def clean_numeric_columns(df, columns):
    """
    Limpia las columnas numéricas eliminando comas y puntos.
    
    Args:
        df (pd.DataFrame): DataFrame a limpiar
        columns (list): Lista de columnas a limpiar
    
    Returns:
        pd.DataFrame: DataFrame con las columnas limpias
    """
    for col in columns:
        if col in df.columns:
            # Convertimos a string primero para manejar cualquier tipo de dato
            df[col] = df[col].astype(str)
            # Eliminamos espacios, comas y puntos
            df[col] = df[col].str.strip().str.replace(',', '').str.replace('.', '')
            # Reemplazamos valores vacíos con None
            df[col] = df[col].replace('', None)
    return df

def process_uploaded_file(up_file):
    """
    Procesa el archivo subido aplicando las transformaciones necesarias.
    
    Args:
        up_file: Archivo subido a través de st.file_uploader
    
    Returns:
        pd.DataFrame: DataFrame procesado o None si hay error
    """
    try:
        ext = os.path.splitext(up_file.name)[1].lower()
        
        # Leemos el archivo según su extensión
        if ext == ".csv":
            df = pd.read_csv(up_file, dtype=str)
        elif ext == ".xlsx":
            df = pd.read_excel(up_file, dtype=str)
        else:
            st.warning(f"Formato no soportado: {ext}")
            return None
            
        # Convertimos nombres de columnas a minúsculas
        df.columns = df.columns.str.lower()
        
        # Limpiamos las columnas numéricas
        df = clean_numeric_columns(df, ['cod_infotel', 'codigo_postal'])
        
        return df
        
    except Exception as e:
        st.error(f"Error procesando archivo: {e}")
        return None

# ----------------------------------------------------------------
# 4) Funciones de Ingesta
# ----------------------------------------------------------------
def ingest_dataframe_to_db(df: pd.DataFrame, created_by="StreamlitUser"):
    """
    Inserta un DataFrame en la tabla 'sociedades'.
    Genera un lote_id aleatorio (batch).
    Uso de ON CONFLICT (cod_infotel) DO NOTHING para evitar duplicados.
    """
    batch_id = f"{random.randint(1,9999999999):010d}"
    st.info(f"[Ingesta] Lote={batch_id}, user={created_by}")

    conn = get_connection()
    conn.autocommit = True
    cur = conn.cursor()

    rows_inserted = 0

    for idx, row in df.iterrows():
        try:
            # Los valores ya vienen limpios del procesamiento previo
            cod_infotel_val = row.get("cod_infotel")
            codigo_postal_val = row.get("codigo_postal")

            # Procesamiento de otros campos
            nif_val = str(row.get("nif")) if pd.notna(row.get("nif")) else None
            razon_social_val = str(row.get("razon_social")) if pd.notna(row.get("razon_social")) else None
            domicilio_val = str(row.get("domicilio")) if pd.notna(row.get("domicilio")) else None
            nom_poblacion_val = str(row.get("nom_poblacion")) if pd.notna(row.get("nom_poblacion")) else None
            nom_provincia_val = str(row.get("nom_provincia")) if pd.notna(row.get("nom_provincia")) else None
            url_val = str(row.get("url")) if pd.notna(row.get("url")) else None
            telefono_val = str(row.get("telefono")) if pd.notna(row.get("telefono")) else None
            email_val = str(row.get("email")) if pd.notna(row.get("email")) else None
            facebook_val = str(row.get("facebook")) if pd.notna(row.get("facebook")) else None
            twitter_val = str(row.get("twitter")) if pd.notna(row.get("twitter")) else None
            instagram_val = str(row.get("instagram")) if pd.notna(row.get("instagram")) else None

            ecommerce_val = row.get("ecommerce")
            if pd.isna(ecommerce_val):
                ecommerce_val = None

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
            rows_inserted += 1

        except Exception as ex:
            st.error(f"[WARN] Fila {idx} no insertada: {ex}")

    cur.close()
    conn.close()
    st.success(f"[Ingesta] Filas={len(df)} / Insertadas={rows_inserted}.")

# ----------------------------------------------------------------
# 5) Scraping
# ----------------------------------------------------------------
def do_scraping(limit=1000):
    s_sql = f"""
    SELECT cod_infotel, url, url_limpia, estado_url, telefono, email
      FROM sociedades
     WHERE deleted=FALSE
       AND url IS NOT NULL
       AND (
         url_limpia IS NULL
         OR estado_url IS NULL
         OR telefono IS NULL
         OR email IS NULL
       )
     LIMIT {limit};
    """
    rows = run_query(s_sql)
    st.write(f"[Scraping] Se encontraron {len(rows)} registros para procesar")

    for cod_infotel_val, url_orig, url_limpia, estado_url, tel_db, email_db in rows:
        if not url_orig:
            continue

        if not url_limpia:
            url_limpia = clean_url(url_orig)

        if not estado_url:
            estado_url = get_http_status(url_limpia)

        new_tel = tel_db
        new_email = email_db
        new_fb, new_tw, new_ig = None, None, None

        if estado_url == "200":
            html_text = get_html(url_limpia)
            if html_text:
                emails, fb, tw, ig = extract_emails_and_socials(html_text)
                if (not new_email) and emails:
                    new_email = emails[0]

                new_fb, new_tw, new_ig = fb, tw, ig

                if not new_tel:
                    phone_found = extract_phone(html_text)
                    if phone_found:
                        new_tel = phone_found

        up_sql = """
        UPDATE sociedades
           SET url_limpia=%s,
               estado_url=%s,
               telefono=%s,
               email=%s,
               facebook=%s,
               twitter=%s,
               instagram=%s,
               fecha_actualizacion=NOW()
         WHERE cod_infotel=%s
        """
        run_action(up_sql, (
            url_limpia,
            estado_url,
            new_tel,
            new_email,
            new_fb,
            new_tw,
            new_ig,
            cod_infotel_val
        ))

    st.success("[Scraping] Completado.")

def clean_url(url: str) -> str:
    url = url.strip()
    url = url.replace("https://", "").replace("http://", "")
    return url.split("/")[0].lower()

def get_http_status(domain: str) -> str:
    if not domain.startswith("http"):
        domain = "http://" + domain
    try:
        resp = requests.head(domain, allow_redirects=True, timeout=5)
        return str(resp.status_code)
    except:
        return None

def get_html(domain: str) -> str:
    if not domain.startswith("http"):
        domain = "http://" + domain
    try:
        r = requests.get(domain, timeout=5)
        if r.status_code == 200:
            return r.text
    except:
        pass
    return ""

def extract_emails_and_socials(html: str):
    emails = re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", html)
    emails = list(set(emails))

    soup = BeautifulSoup(html, "html.parser")
    links = [a.get('href') for a in soup.find_all('a', href=True)]
    facebook = next((lnk for lnk in links if "facebook.com" in lnk), None)
    twitter = next((lnk for lnk in links if "twitter.com" in lnk), None)
    instagram = next((lnk for lnk in links if "instagram.com" in lnk), None)

    return (emails, facebook, twitter, instagram)

def extract_phone(html: str):
    pattern = r"(?:\+?\d{1,3}[ \-]?)?(?:\(\d{1,3}\)[ \-]?)?\d{7,12}"
    match = re.search(pattern, html)
    if match:
        return match.group(0)
    return None

# ----------------------------------------------------------------
# 6) Dashboard
# ----------------------------------------------------------------
def show_dashboard():
    st.subheader("Dashboard de Sociedades")
    r = run_query("""
    SELECT COUNT(*),
           COUNT(DISTINCT lote_id),
           COUNT(*) FILTER (WHERE deleted=TRUE)
      FROM sociedades
    """)
    if r:
        total, lotes, borrados = r[0]
        st.write(f"- Total registros: {total}")
        st.write(f"- Distintos lotes: {lotes}")
        st.write(f"- Marcados borrados: {borrados}")

    r2 = run_query("""
    SELECT nom_provincia, COUNT(*)
      FROM sociedades
     WHERE deleted=FALSE
     GROUP BY nom_provincia
    """)
    if r2:
        dfp = pd.DataFrame(r2, columns=["Provincia","Cantidad"])
        st.bar_chart(data=dfp, x="Provincia", y="Cantidad")

# ----------------------------------------------------------------
# 7) Chat heurístico
# ----------------------------------------------------------------
def interpret_user_prompt(prompt: str) -> str:
    p_lower = prompt.lower()
    if "scrapea" in p_lower:
        match = re.search(r"\d+", p_lower)
        limit = 100
        if match:
            limit = int(match.group(0))
        do_scraping(limit)
        return f"Se ejecutó scraping de {limit} registros."

    match_prov = re.search(r"provincia de (\w+)", p_lower)
    if match_prov:
        prov = match_prov.group(1)
        q = """
        SELECT cod_infotel, nif, razon_social, nom_provincia
          FROM sociedades
         WHERE nom_provincia ILIKE %s
           AND deleted=FALSE
         LIMIT 20
        """
        rows = run_query(q, (f"%{prov}%",))
        if rows:
            txt = f"Registros en provincia '{prov}':\n"
            for fila in rows:
                txt += f"- cod_infotel={fila[0]}, NIF={fila[1]}, RS={fila[2]}, PROV={fila[3]}\n"
            return txt
        else:
            return f"No hay registros para provincia '{prov}'."

    return generar_respuesta_ollama(prompt)

def generar_respuesta_ollama(prompt: str, model="deepseek-r1:8b") -> str:
    data = {
        "model": model,
        "prompt": prompt,
        "max_tokens": 150,
        "temperature": 0.7,
        "stream": False
    }
    try:
        resp = requests.post(OLLAMA_URL, json=data, timeout=10)
        if resp.status_code == 200:
            return resp.json().get('response', '')
        else:
            return f"[LLM Error {resp.status_code}]: {resp.text}"
    except Exception as e:
        return f"[Error conectando con LLM]: {e}"

# ----------------------------------------------------------------
# 8) App principal en Streamlit
# ----------------------------------------------------------------
def main():
    st.title("Aplicación: Ingesta + Scraping + Dashboard + Chat LLM (Heurístico)")

    # 1) Subir CSV/Excel
    st.header("1) Subir archivo e Ingestar")
    up_file = st.file_uploader("Sube un CSV/XLSX", type=["csv", "xlsx"])
    created_by = st.text_input("Usuario o lote:", value="StreamlitUser")

    if up_file:
        df_up = process_uploaded_file(up_file)
        
        if df_up is not None:
            st.write("Vista previa del DataFrame procesado:")
            st.dataframe(df_up)
            
            if st.button("Insertar en BD"):
                ingest_dataframe_to_db(df_up, created_by)

    st.write("---")

    # 2) Ver Dashboard
    st.header("2) Ver Dashboard")
    if st.button("Mostrar Dashboard"):
        show_dashboard()

    st.write("---")

    # 3) Scraping
    st.header("3) Scraping")
    limit_scrap = st.number_input("Cantidad registros a scrapear", 1, 999999, 100)
    if st.button("Ejecutar Scraping"):
        do_scraping(limit_scrap)

    st.write("---")

    # 4) Chat heurístico
    st.header("4) Chat / Prompt Heurístico")
    st.write("- Prueba con 'Scrapea 200' o 'provincia de barcelona' o una pregunta libre.")
    user_prompt = st.text_input("Pregunta/comando:")
    if st.button("Enviar Prompt"):
        if user_prompt.strip():
            resp = interpret_user_prompt(user_prompt)
            st.write("**Respuesta:**")
            st.info(resp)

if __name__ == "__main__":
    main()