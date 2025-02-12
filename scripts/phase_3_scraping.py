import os
import re
import sys
import requests
import psycopg2
import traceback
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# Parámetros de conexión a BD
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "admin")
DB_NAME = os.getenv("DB_NAME", "web_scraping")

def main():
    """
    Fase 3: Scraping y actualización de campos en la tabla 'sociedades'.
    1. Selecciona registros que carecen de cierta info (telefono, email, etc.),
       o cuyo 'url_limpia' o 'estado_url' sea NULL.
    2. Limpia la URL y comprueba estado (HEAD).
    3. Si 200, hace GET y extrae emails, redes sociales, etc.
    4. Actualiza la BD con los datos obtenidos.
    """
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

        print(">>> Fase 3 - Scraping iniciado.")

        # EJEMPLO de criterio: filas que no tienen telefono O email O url_limpia O estado_url
        # Ajústalo según tu escenario (puede que quieras filtrar por provincia, etc.)
        select_sql = """
        SELECT cod_infotel, url, url_limpia, estado_url, telefono, email
        FROM sociedades
        WHERE deleted = FALSE
          AND (
               telefono IS NULL
               OR email IS NULL
               OR url_limpia IS NULL
               OR estado_url IS NULL
              )
        LIMIT 100
        """
        # Pon un LIMIT para no procesar demasiados de golpe, o quítalo según tu preferencia.

        cur.execute(select_sql)
        rows = cur.fetchall()
        print(f">>> Registros a procesar: {len(rows)}")

        for row in rows:
            cod_infotel_val = row[0]
            url_original = row[1]
            url_limpia = row[2]
            estado_url = row[3]
            telefono_val = row[4]
            email_val = row[5]

            # 1) Si no hay URL, no podemos scrapear. Podrías intentar un "buscador" en otra fase.
            if not url_original:
                continue

            # 2) Limpieza de la URL (url_limpia) si está vacía
            if not url_limpia:
                url_limpia = clean_url(url_original)

            # 3) Verificar estado de la URL (HEAD request) si estado_url es NULL
            if not estado_url:
                estado_url = get_http_status(url_limpia)

            # 4) Si estado_url es 200, hacemos GET y extraemos datos (email, redes sociales, etc.)
            new_email = email_val
            new_telefono = telefono_val
            new_facebook = None
            new_twitter = None
            new_instagram = None

            if estado_url == "200":
                html_text = get_html(url_limpia)
                if html_text:
                    # Extraer email y redes
                    extracted_emails, fb, tw, ig = extract_emails_and_socials(html_text)
                    # Usar solo el primero si no teníamos email antes:
                    if not new_email and extracted_emails:
                        new_email = extracted_emails[0]
                    new_facebook = fb if fb else None
                    new_twitter = tw if tw else None
                    new_instagram = ig if ig else None

                    # Si no teníamos teléfono, opcionalmente podemos intentar encontrarlo en el HTML
                    if not new_telefono:
                        possible_phone = extract_phone(html_text)
                        if possible_phone:
                            new_telefono = possible_phone

            # 5) Actualizamos la BD
            update_sql = """
            UPDATE sociedades
               SET url_limpia = %s,
                   estado_url = %s,
                   telefono = %s,
                   email = %s,
                   facebook = %s,
                   twitter = %s,
                   instagram = %s,
                   fecha_actualizacion = NOW()
             WHERE cod_infotel = %s
            """
            update_values = (
                url_limpia,
                estado_url,
                new_telefono,
                new_email,
                new_facebook,
                new_twitter,
                new_instagram,
                cod_infotel_val
            )
            try:
                cur.execute(update_sql, update_values)
                print(f"    > cod_infotel={cod_infotel_val} actualizado.")
            except Exception as up_ex:
                print(f"[WARN] Error al actualizar cod_infotel={cod_infotel_val}: {up_ex}")
                traceback.print_exc()

        # FIN del bucle
        cur.close()
        conn.close()

        print(">>> Fase 3 finalizada. Scraping completado.")

    except Exception as e:
        print("Error en la fase 3 de scraping:", e)
        sys.exit(1)


def clean_url(url):
    """
    Limpia la URL removiendo protocolo http/https y rutas extra.
    'http://www.empresa.com/contacto' -> 'www.empresa.com'
    """
    url = url.strip()
    url = url.replace("https://", "").replace("http://", "")
    # Quitar ruta después del primer '/'
    parts = url.split("/")
    return parts[0].lower()


def get_http_status(domain):
    """
    Hace un HEAD a 'domain' (agregando 'http://' si no lo tiene).
    Retorna el código HTTP como string ('200', '404', etc.) o None en error.
    """
    if not domain:
        return None
    # Asegurar que tengamos un esquema
    if not domain.startswith("http"):
        domain = "http://" + domain
    try:
        resp = requests.head(domain, allow_redirects=True, timeout=5)
        return str(resp.status_code)
    except:
        return None


def get_html(domain):
    """
    Hace GET a domain y retorna el texto HTML, o None en caso de error.
    """
    if not domain.startswith("http"):
        domain = "http://" + domain
    try:
        resp = requests.get(domain, timeout=5)
        if resp.status_code == 200:
            return resp.text
    except:
        pass
    return None


def extract_emails_and_socials(html):
    """
    - Extrae todos los emails del HTML usando regex.
    - Extrae primera coincidencia de facebook, twitter, instagram en los enlaces.
    Retorna (list_of_emails, facebook, twitter, instagram).
    """
    # 1) Emails
    emails = re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", html)
    emails = list(set(emails))  # eliminar duplicados

    # 2) Redes
    soup = BeautifulSoup(html, "html.parser")
    links = [a.get('href') for a in soup.find_all('a', href=True)]
    facebook = next((l for l in links if "facebook.com" in l), None)
    twitter = next((l for l in links if "twitter.com" in l), None)
    instagram = next((l for l in links if "instagram.com" in l), None)

    return (emails, facebook, twitter, instagram)


def extract_phone(html):
    """
    Intenta encontrar un número de teléfono en el HTML.
    - Este regex es muy genérico. Ajústalo según tu país o formato.
    """
    # Ejemplo de patrón: +34 123 456 789 / 912345678 / etc.
    phone_pattern = r"(?:\+?\d{1,3}[ \-]?)?(?:\(\d{1,3}\)[ \-]?)?\d{7,12}"
    match = re.search(phone_pattern, html)
    if match:
        return match.group(0)
    return None


if __name__ == "__main__":
    main()
