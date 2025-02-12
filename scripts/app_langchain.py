import os
import re
import random
import requests
import psycopg2
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from bs4 import BeautifulSoup
from typing import Optional, List, Any, Dict

# Selenium
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# LangChain + langchain_community
from langchain.llms.base import LLM
from langchain.schema import LLMResult
from langchain.agents import Tool, AgentType, initialize_agent
from langchain_community.agent_toolkits.sql.toolkit import SQLDatabaseToolkit
from langchain_community.utilities import SQLDatabase

# ----------------------------------------------------------------
# 1) Config
# ----------------------------------------------------------------
load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "admin")
DB_NAME = os.getenv("DB_NAME", "web_scraping")

OLLAMA_URL = "http://localhost:11434/api/generate"

# ----------------------------------------------------------------
# 2) DB
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
# 3) LLM Wrapper (Ollama)
# ----------------------------------------------------------------
class OllamaLLM(LLM):
    model_name: str = "deepseek-r1:14b"
    max_tokens: int = 256
    temperature: float = 0.7

    @property
    def _llm_type(self) -> str:
        return "ollama_llm"

    def _call(self, prompt: str, stop: Optional[List[str]] = None, run_manager=None) -> str:
        data = {
            "model": self.model_name,
            "prompt": prompt,
            "max_tokens": self.max_tokens,
            "temperature": self.temperature,
            "stream": False
        }
        try:
            resp = requests.post(OLLAMA_URL, json=data, timeout=30)
            if resp.status_code == 200:
                return resp.json().get('response', '')
            else:
                return f"[Ollama Error {resp.status_code}]: {resp.text}"
        except Exception as e:
            return f"[Ollama Exception]: {e}"

    @property
    def identifying_params(self) -> Dict[str, Any]:
        return {
            "model_name": self.model_name,
            "max_tokens": self.max_tokens,
            "temperature": self.temperature
        }
    
# ----------------------------------------------------------------
# 4) Smart Query Builder
# ----------------------------------------------------------------
class SmartDBQuery:
    def __init__(self):
        self.column_mappings = {
            # Empresa ID
            'código de empresa': 'cod_infotel',
            'numero de empresa': 'cod_infotel',
            'id empresa': 'cod_infotel',
            'identificador': 'cod_infotel',
            
            # Información fiscal
            'nif': 'nif',
            'cif': 'nif',
            'número fiscal': 'nif',
            'identificación fiscal': 'nif',
            
            # Nombre empresa
            'nombre empresa': 'razon_social',
            'razón social': 'razon_social',
            'empresa': 'razon_social',
            
            # Dirección
            'dirección': 'domicilio',
            'calle': 'domicilio',
            'ubicación': 'domicilio',
            'domicilio': 'domicilio',
            
            # Código postal
            'cp': 'codigo_postal',
            'código postal': 'codigo_postal',
            'zona postal': 'codigo_postal',
            
            # Población
            'población': 'nom_poblacion',
            'ciudad': 'nom_poblacion',
            'localidad': 'nom_poblacion',
            
            # Provincia
            'provincia': 'nom_provincia',
        }

    def get_column_name(self, user_term):
        # Convertir a minúsculas para la búsqueda
        user_term = user_term.lower().strip()
        return self.column_mappings.get(user_term, user_term)

    def build_query(self, search_term, value, exact_match=False):
        column = self.get_column_name(search_term)
        
        # Construir la consulta base
        query = f"""
        SELECT cod_infotel, nif, razon_social, domicilio, 
               codigo_postal, nom_poblacion, nom_provincia
        FROM sociedades
        WHERE deleted = FALSE
        """
        
        # Añadir la condición de búsqueda
        if exact_match:
            query += f" AND {column} = %s"
        else:
            query += f" AND {column} ILIKE %s"
            value = f"%{value}%"
            
        query += " LIMIT 20"
        
        return query, (value,)

# ----------------------------------------------------------------
# 5) Procesamiento de archivos
# ----------------------------------------------------------------
def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia el DataFrame: convierte columnas a minúsculas y limpia SOLO cod_infotel
    """
    df = df.copy()
    
    # Convertir nombres de columnas a minúsculas
    df.columns = df.columns.str.lower()
    
    # SOLO limpiar cod_infotel, dejando el resto de campos intactos
    if 'cod_infotel' in df.columns:
        df['cod_infotel'] = df['cod_infotel'].astype(str).apply(
            lambda x: ''.join(c for c in x if c.isdigit()) if pd.notna(x) else None
        )
    
    return df

# ----------------------------------------------------------------
# 6) Ingesta CSV/Excel
# ----------------------------------------------------------------
def ingest_dataframe_to_db(df: pd.DataFrame, created_by="StreamlitUser"):
    """
    Inserta un DataFrame en la tabla 'sociedades'.
    Solo realiza la inserción básica sin scraping.
    """
    # Limpiar solo cod_infotel
    df = clean_dataframe(df)
    
    batch_id = f"{random.randint(1,9999999999):010d}"
    st.info(f"[Ingesta] Lote={batch_id}, user={created_by}")

    conn = get_connection()
    conn.autocommit = True
    cur = conn.cursor()
    rows_inserted = 0

    for idx, row in df.iterrows():
        try:
            # Solo procesar si cod_infotel es válido
            cod_infotel = row.get('cod_infotel')
            if pd.isna(cod_infotel):
                continue

            # Mantener valores originales sin procesamiento adicional
            values = (
                cod_infotel,
                row.get('nif'),
                row.get('razon_social'),
                row.get('domicilio'),
                row.get('codigo_postal'),  # Mantener valor original
                row.get('nom_poblacion'),
                row.get('nom_provincia'),
                row.get('url'),
                None,  # url_limpia
                None,  # estado_url
                None,  # url_valida
                row.get('telefono'),
                row.get('email'),
                None,  # facebook
                None,  # twitter
                None,  # instagram
                row.get('ecommerce'),
                batch_id,
                created_by
            )

            sql = """
            INSERT INTO sociedades (
                cod_infotel, nif, razon_social, domicilio, codigo_postal,
                nom_poblacion, nom_provincia, url, url_limpia, estado_url,
                url_valida, telefono, email, facebook, twitter,
                instagram, ecommerce, lote_id, created_by,
                fecha_creacion, fecha_actualizacion
            )
            VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                NOW(), NOW()
            )
            ON CONFLICT (cod_infotel) DO NOTHING
            """
            
            cur.execute(sql, values)
            rows_inserted += 1

        except Exception as ex:
            st.error(f"[WARN] Fila {idx} no insertada: {ex}")

    cur.close()
    conn.close()
    st.success(f"[Ingesta] Filas={len(df)} / Insertadas={rows_inserted}.")

# ----------------------------------------------------------------
# 7) Scraping (requests + Selenium)
# ----------------------------------------------------------------
def do_scraping(limit=1000):
    s_sql = f"""
    SELECT cod_infotel, nif, razon_social, domicilio, url, url_limpia, estado_url, telefono, email
      FROM sociedades
     WHERE deleted = FALSE
       AND (url IS NOT NULL)
       AND (url_limpia IS NULL OR estado_url IS NULL OR telefono IS NULL OR email IS NULL)
     LIMIT {limit};
    """
    rows = run_query(s_sql)
    st.write(f"[Scraping] Total registros={len(rows)}")

    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64) AppleWebKit/537.36")

    driver = webdriver.Chrome(options=chrome_options)

    for r in rows:
        cod_infotel, nif, razon, domicilio, url_orig, url_limpia, estado_url, tel_db, email_db = r
        if not url_orig:
            continue

        if not url_limpia:
            url_limpia = clean_url(url_orig)

        if not estado_url:
            estado_url = get_http_status(url_limpia)

        new_tel = tel_db
        new_email = email_db
        new_fb, new_tw, new_ig = None, None, None
        url_valida = False

        html_text = None
        if estado_url == "200":
            dynamic = detect_if_dynamic(url_limpia)
            if not dynamic:
                html_text = get_html_ua(url_limpia)
                if not html_text:
                    html_text = get_html_selenium(driver, url_limpia)
            else:
                html_text = get_html_selenium(driver, url_limpia)

        if html_text:
            emails, fb, tw, ig = extract_emails_and_socials(html_text)
            if not new_email and emails:
                new_email = emails[0]
            new_fb, new_tw, new_ig = fb, tw, ig

            if not new_tel:
                ph = extract_phone(html_text)
                if ph:
                    new_tel = ph

            checks = 0
            if nif and (nif.lower() in html_text.lower()):
                checks += 1
            if razon and (razon.lower() in html_text.lower()):
                checks += 1
            if domicilio and (domicilio.lower() in html_text.lower()):
                checks += 1
            url_valida = (checks > 0)

        up_sql = """
        UPDATE sociedades
           SET url_limpia=%s,
               estado_url=%s,
               telefono=%s,
               email=%s,
               facebook=%s,
               twitter=%s,
               instagram=%s,
               url_valida=%s,
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
            url_valida,
            cod_infotel
        ))

    driver.quit()
    st.success("[Scraping] Finalizado.")

def detect_if_dynamic(domain: str) -> bool:
    if not domain.startswith("http"):
        domain = "http://" + domain
    try:
        resp = requests.get(domain, timeout=5, headers={"User-Agent":"Mozilla/5.0"})
        if resp.status_code == 200:
            sc = resp.text.lower().count("<script")
            if sc > 10:
                return True
    except:
        pass
    return False

def clean_url(url: str) -> str:
    url = url.strip()
    url = url.replace("https://", "").replace("http://", "")
    return url.split("/")[0].lower()

def get_http_status(domain: str) -> str:
    if not domain.startswith("http"):
        domain = "http://" + domain
    try:
        r = requests.head(domain, allow_redirects=True, timeout=5, headers={"User-Agent":"Mozilla/5.0"})
        return str(r.status_code)
    except:
        return None

def get_html_ua(domain: str) -> str:
    if not domain.startswith("http"):
        domain = "http://" + domain
    try:
        resp = requests.get(domain, timeout=5, headers={"User-Agent":"Mozilla/5.0"})
        if resp.status_code == 200:
            return resp.text
    except:
        pass
    return ""

def get_html_selenium(driver, domain: str) -> str:
    if not domain.startswith("http"):
        domain = "http://" + domain
    try:
        driver.get(domain)
        return driver.page_source
    except:
        return ""

def extract_emails_and_socials(html: str):
    emails = re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", html)
    emails = list(set(emails))

    soup = BeautifulSoup(html, "html.parser")
    links = [a.get('href') for a in soup.find_all('a', href=True)]
    facebook = next((l for l in links if "facebook.com" in l), None)
    twitter = next((l for l in links if "twitter.com" in l), None)
    instagram = next((l for l in links if "instagram.com" in l), None)

    return (emails, facebook, twitter, instagram)

def extract_phone(html: str):
    pat = r"(?:\+?\d{1,3}[ \-]?)?(?:\(\d{1,3}\)[ \-]?)?\d{7,12}"
    m = re.search(pat, html)
    if m:
        return m.group(0)
    return None


# ----------------------------------------------------------------
# 8) Tools y Dashboard
# ----------------------------------------------------------------

def get_sql_database_toolkit(llm: LLM):
    """
    Crea y devuelve un toolkit para trabajar con la base de datos SQL
    """
    uri = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    db = SQLDatabase.from_uri(uri)
    toolkit = SQLDatabaseToolkit(db=db, llm=llm)
    return toolkit

def scraping_tool_func(limit_str: str) -> str:
    """
    Función para la herramienta de scraping
    """
    try:
        limit = int(limit_str)
    except:
        limit = 100
    do_scraping(limit=limit)
    return f"[ScrapingTool] Scrapeado {limit} registros."

# Definición de la herramienta de scraping
scraping_tool = Tool(
    name="ScrapingTool",
    func=scraping_tool_func,
    description="Herramienta para scraping en la tabla sociedades. input=numero (limit)."
)

def show_dashboard():
    """
    Muestra el dashboard con estadísticas de la base de datos
    """
    st.subheader("Dashboard - Estadísticas")
    
    # Consulta general de estadísticas
    stats_query = """
    SELECT 
        COUNT(*) as total,
        COUNT(DISTINCT lote_id) as lotes,
        COUNT(*) FILTER (WHERE deleted=TRUE) as borrados,
        COUNT(*) FILTER (WHERE url_valida=TRUE) as urls_validas,
        COUNT(DISTINCT nom_provincia) as provincias
    FROM sociedades;
    """
    
    try:
        stats = run_query(stats_query)[0]
        
        # Mostrar estadísticas generales
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Registros", f"{stats[0]:,}")
        with col2:
            st.metric("Total Lotes", f"{stats[1]:,}")
        with col3:
            st.metric("URLs Válidas", f"{stats[3]:,}")
            
        # Distribución por provincia
        prov_query = """
        SELECT nom_provincia, COUNT(*) as total
        FROM sociedades
        WHERE deleted = FALSE
        GROUP BY nom_provincia
        ORDER BY total DESC
        LIMIT 10;
        """
        
        prov_data = run_query(prov_query)
        if prov_data:
            st.subheader("Top 10 Provincias")
            df_prov = pd.DataFrame(prov_data, columns=["Provincia", "Total"])
            st.bar_chart(data=df_prov, x="Provincia", y="Total")
            
        # Estadísticas de datos web
        web_query = """
        SELECT 
            COUNT(*) FILTER (WHERE url IS NOT NULL) as con_web,
            COUNT(*) FILTER (WHERE email IS NOT NULL) as con_email,
            COUNT(*) FILTER (WHERE facebook IS NOT NULL) as con_facebook,
            COUNT(*) FILTER (WHERE twitter IS NOT NULL) as con_twitter,
            COUNT(*) FILTER (WHERE instagram IS NOT NULL) as con_instagram
        FROM sociedades
        WHERE deleted = FALSE;
        """
        
        web_stats = run_query(web_query)[0]
        st.subheader("Presencia Digital")
        col1, col2, col3, col4, col5 = st.columns(5)
        with col1:
            st.metric("Web", f"{web_stats[0]:,}")
        with col2:
            st.metric("Email", f"{web_stats[1]:,}")
        with col3:
            st.metric("Facebook", f"{web_stats[2]:,}")
        with col4:
            st.metric("Twitter", f"{web_stats[3]:,}")
        with col5:
            st.metric("Instagram", f"{web_stats[4]:,}")
            
    except Exception as e:
        st.error(f"Error cargando el dashboard: {str(e)}")


# ----------------------------------------------------------------
# 9) Procesamiento de lenguaje natural
# ----------------------------------------------------------------
def interpret_user_prompt(prompt: str) -> str:
    query_builder = SmartDBQuery()
    p_lower = prompt.lower()

    # Manejar comando de scraping
    if "scrapea" in p_lower:
        match = re.search(r"\d+", p_lower)
        limit = int(match.group(0)) if match else 100
        do_scraping(limit)
        return f"Se ejecutó scraping de {limit} registros."

    # Buscar patrones comunes en el prompt
    patterns = [
        (r"empresas? de (\w+)", "provincia"),
        (r"en (?:la |)provincia (?:de |)(\w+)", "provincia"),
        (r"(?:empresa|comercio|negocio).*?(?:calle|dirección|ubicad.) en ['\"](.+?)['\"]", "dirección"),
        (r"(?:empresa|comercio|negocio).*?código postal (\d+)", "código postal"),
        (r"(?:nif|cif)[:\s]+([A-Z0-9]+)", "nif"),
        (r"razón social[:\s]+['\"](.+?)['\"]", "razón social"),
        (r"(?:nombre|llamada)[:\s]+['\"](.+?)['\"]", "razón social"),
        (r"población[:\s]+['\"](.+?)['\"]", "población"),
    ]

    for pattern, search_type in patterns:
        match = re.search(pattern, p_lower)
        if match:
            value = match.group(1)
            query, params = query_builder.build_query(search_type, value)
            rows = run_query(query, params)
            
            if not rows:
                return f"No encontré resultados para {search_type}: {value}"
            
            response = f"Encontré los siguientes resultados para {search_type} '{value}':\n\n"
            for row in rows:
                response += f"- {row[2]} (NIF: {row[1]}, CP: {row[4]}, {row[5]}, {row[6]})\n"
            return response

    # Si no coincide con ningún patrón, usar LLM
    return generar_respuesta_ollama(prompt)

# ----------------------------------------------------------------
# 10) Generación de respuestas con Ollama
# ----------------------------------------------------------------
def generar_respuesta_ollama(prompt: str, model="deepseek-r1:14b") -> str:
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
# 11) Gestión de Lotes
# ----------------------------------------------------------------
def get_batch_data(batch_id: str):
    """
    Obtiene todos los datos de un lote específico
    """
    query = """
    SELECT *
    FROM sociedades
    WHERE lote_id = %s
    ORDER BY cod_infotel;
    """
    try:
        rows = run_query(query, (batch_id,))
        if not rows:
            return None
        
        # Obtener nombres de columnas
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT * FROM sociedades LIMIT 0")
        columns = [desc[0] for desc in cur.description]
        cur.close()
        conn.close()
        
        # Crear DataFrame
        return pd.DataFrame(rows, columns=columns)
    except Exception as e:
        st.error(f"Error obteniendo datos del lote: {e}")
        return None

def delete_batch(batch_id: str):
    """
    Elimina un lote completo de la base de datos
    """
    try:
        conn = get_connection()
        conn.autocommit = True
        cur = conn.cursor()
        
        # Primero contamos
        cur.execute("SELECT COUNT(*) FROM sociedades WHERE lote_id = %s", (batch_id,))
        count = cur.fetchone()[0]
        
        if count > 0:
            # Luego eliminamos
            cur.execute("DELETE FROM sociedades WHERE lote_id = %s", (batch_id,))
        
        cur.close()
        conn.close()
        return count
        
    except Exception as e:
        st.error(f"Error eliminando lote: {e}")
        return 0


def do_selective_scraping(df: pd.DataFrame, selected_rows: list):
    """
    Realiza scraping solo para las filas seleccionadas
    """
    if not selected_rows:
        return
        
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64) AppleWebKit/537.36")

    driver = webdriver.Chrome(options=chrome_options)
    
    for idx in selected_rows:
        row = df.iloc[idx]
        url_orig = row['url']
        if not url_orig or pd.isna(url_orig):
            continue
            
        url_limpia = clean_url(url_orig)
        estado_url = get_http_status(url_limpia)
        
        if estado_url == "200":
            html_text = get_html_selenium(driver, url_limpia)
            if html_text:
                emails, fb, tw, ig = extract_emails_and_socials(html_text)
                phone = extract_phone(html_text)
                
                update_query = """
                UPDATE sociedades
                SET url_limpia = %s,
                    estado_url = %s,
                    telefono = COALESCE(%s, telefono),
                    email = COALESCE(%s, email),
                    facebook = COALESCE(%s, facebook),
                    twitter = COALESCE(%s, twitter),
                    instagram = COALESCE(%s, instagram),
                    fecha_actualizacion = NOW()
                WHERE cod_infotel = %s
                """
                run_action(update_query, (
                    url_limpia,
                    estado_url,
                    phone,
                    emails[0] if emails else None,
                    fb,
                    tw,
                    ig,
                    row['cod_infotel']
                ))
                
    driver.quit()
    st.success("Scraping selectivo completado")

def show_batch_management():
    """
    Muestra la interfaz de gestión de lotes
    """
    st.sidebar.title("Modificación por Lote")
    batch_id = st.sidebar.text_input("Introducir lote:")
    
    # Inicializar estados en session_state
    if "delete_confirm" not in st.session_state:
        st.session_state.delete_confirm = False
    if "lote_eliminado" not in st.session_state:
        st.session_state.lote_eliminado = False
    
    if batch_id:
        df = get_batch_data(batch_id)
        if df is not None:
            st.session_state.lote_eliminado = False  # Resetear estado de eliminación
            st.subheader(f"Datos del Lote: {batch_id}")
            
            # Mostrar DataFrame con selección de filas
            st.write("Selecciona filas para scraping selectivo:")
            selected_rows = []
            
            # Crear tabla interactiva
            df_display = df.copy()
            df_display['Seleccionar'] = False
            edited_df = st.data_editor(
                df_display,
                hide_index=False,
                use_container_width=True,
                num_rows="fixed"
            )
            
            selected_rows = edited_df.index[edited_df['Seleccionar']].tolist()
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                if st.button("Hacer Scraping Selectivo") and selected_rows:
                    do_selective_scraping(df, selected_rows)
                    
            with col2:
                # Botón con estilo condicional basado en el estado de confirmación
                if not st.session_state.delete_confirm:
                    # Primer click - Botón normal
                    if st.button("Eliminar Lote", type="primary"):
                        st.session_state.delete_confirm = True
                        st.rerun()
                else:
                    # Segundo click - Botón rojo de confirmación
                    if st.button("Confirmar Eliminación", type="secondary", use_container_width=True):
                        with st.spinner("Eliminando lote..."):
                            deleted = delete_batch(batch_id)
                            if deleted > 0:
                                st.success(f"Se han eliminado {deleted} registros del lote {batch_id}")
                                st.session_state.delete_confirm = False
                                st.session_state.lote_eliminado = True  # Marcar como eliminado
                                st.rerun()
                            else:
                                st.error("No se encontraron registros para eliminar")
                    
                    # Botón para cancelar
                    if st.button("Cancelar", type="primary"):
                        st.session_state.delete_confirm = False
                        st.rerun()
                    
            with col3:
                if st.button("Refrescar Datos"):
                    st.session_state.delete_confirm = False
                    st.rerun()
            
            # Área de consulta en lenguaje natural
            st.subheader("Consultas sobre el Lote")
            query_text = st.text_area("Escribe tu consulta en lenguaje natural:", 
                                    placeholder="Ejemplo: muéstrame las empresas con email válido en este lote")
            
            if st.button("Ejecutar Consulta"):
                response = interpret_user_prompt(f"En el lote {batch_id}: {query_text}")
                st.write(response)
        else:
            if st.session_state.lote_eliminado:
                st.sidebar.success("Lote eliminado correctamente")
                st.session_state.lote_eliminado = False  # Resetear después de mostrar
            else:
                st.sidebar.warning("Lote no encontrado")


# ----------------------------------------------------------------
# 12) Streamlit con Memoria y Agente Conversacional
# ----------------------------------------------------------------
def main():
    # Mostrar gestión de lotes en el sidebar
    show_batch_management()
    
    # Contenido principal
    st.title("App con Agente Conversacional (CHAT) + Scraping")

    # Tabs para organizar la interfaz
    tab1, tab2, tab3, tab4 = st.tabs(["Chat", "Ingesta", "Dashboard", "Scraping"])

    with tab1:
        # Inicializar chat_history en session_state
        if "chat_history" not in st.session_state:
            st.session_state["chat_history"] = []

        llm_agent = OllamaLLM(model_name="deepseek-r1:14b", max_tokens=2000)

        sql_toolkit = get_sql_database_toolkit(llm_agent)
        sql_tools = sql_toolkit.get_tools()
        tools = sql_tools + [scraping_tool]

        # Configurar el agente con un prompt más específico
        agent = initialize_agent(
            tools=tools,
            llm=llm_agent,
            agent=AgentType.CHAT_CONVERSATIONAL_REACT_DESCRIPTION,
            agent_kwargs={
                "system_message": """Eres un asistente experto en consultar información de empresas.
                Entiendes diferentes formas de pedir la misma información:
                - Para empresas de una zona: "empresas de Madrid", "negocios en Barcelona"
                - Para búsqueda por dirección: "empresa en calle Mayor", "negocio ubicado en Gran Vía"
                - Para códigos postales: "empresa con CP 28001", "negocios en código postal 08001"
                - Para información fiscal: "empresa con NIF B12345678", "CIF de la empresa"
                - Para nombres de empresa: "busca la empresa llamada", "razón social"
                
                Usa las herramientas disponibles para buscar y devolver la información solicitada.
                Si no entiendes la consulta, pide aclaración."""
            },
            verbose=True
        )

        st.subheader("Chat con el Agente")
        user_text = st.text_input("Mensaje:")
        if st.button("Enviar"):
            if user_text.strip():
                st.session_state["chat_history"].append(("human", user_text))
                with st.spinner("Pensando..."):
                    response = interpret_user_prompt(user_text)
                st.session_state["chat_history"].append(("assistant", response))
                st.success(response)

        st.write("### Historial Conversacional")
        for rol, msg in st.session_state["chat_history"]:
            st.write(f"**{rol}**: {msg}")

    with tab2:
        st.subheader("Subir CSV/Excel e Ingestar")
        upfile = st.file_uploader("Archivo CSV/XLSX", type=["csv", "xlsx"])
        user_name = st.text_input("¿Quién ingesta?", "StreamlitUser")

        if upfile:
            ext = os.path.splitext(upfile.name)[1].lower()
            df_loaded = None
            try:
                if ext == ".csv":
                    df_loaded = pd.read_csv(upfile, dtype=str)
                elif ext == ".xlsx":
                    df_loaded = pd.read_excel(upfile, dtype=str)
                else:
                    st.warning("Formato no soportado.")
                    
                if df_loaded is not None:
                    df_cleaned = clean_dataframe(df_loaded)
                    st.write("DataFrame procesado:")
                    st.dataframe(df_cleaned)
                    
                    if st.button("Insertar en BD"):
                        ingest_dataframe_to_db(df_cleaned, user_name)
                        
            except Exception as e:
                st.error(f"Error leyendo archivo: {e}")
                st.exception(e)

    with tab3:
        st.subheader("Dashboard")
        if st.button("Actualizar Dashboard"):
            show_dashboard()

    with tab4:
        st.subheader("Scraping Manual")
        limit_scrap = st.number_input("Limite", 1, 999999, 100)
        if st.button("Ejecutar Scraping"):
            do_scraping(limit=limit_scrap)

if __name__ == "__main__":
    main()