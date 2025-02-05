import pandas as pd
import requests
import re
from urllib3.exceptions import InsecureRequestWarning
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import unicodedata
from bs4 import BeautifulSoup

# Desactivar advertencias de SSL
urllib3.disable_warnings(InsecureRequestWarning)

def remove_accents(text):
    """Elimina acentos y caracteres diacríticos del texto."""
    return ''.join(c for c in unicodedata.normalize('NFKD', text)
                  if not unicodedata.combining(c))

def clean_company_name(company_name):
    """Limpia y formatea el nombre de la empresa."""
    if not isinstance(company_name, str):
        return ""
    
    # Primero eliminar acentos
    name = remove_accents(company_name)
    
    # Convertir a minúsculas y eliminar espacios iniciales/finales
    name = name.lower().strip()
    
    # Eliminar caracteres especiales y espacios
    name = re.sub(r'[^\w\s-]', '', name)
    name = name.replace(' ', '-')
    
    # Eliminar SA o SL al final del nombre
    name = re.sub(r'(-sa|-s\.a\.|sa)$', '', name, flags=re.IGNORECASE)
    name = re.sub(r'(-sl|-s\.l\.|sl)$', '', name, flags=re.IGNORECASE)
    
    # Eliminar guiones al final
    name = name.rstrip('-')
    
    return name

def create_session():
    """Crea una sesión de requests con retry y timeouts configurados"""
    session = requests.Session()
    
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session

def get_page_content(url, session):
    """Obtiene el contenido de una página web."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
    }
    
    try:
        response = session.get(
            url, 
            timeout=(15, 30),
            verify=False,
            headers=headers
        )
        response.raise_for_status()
        return response.text
    except:
        return None

def verify_company_url_match(url, company_name, session):
    """Verifica si una URL corresponde realmente a la empresa."""
    content = get_page_content(url, session)
    if not content:
        return False
    
    try:
        # Preparar el nombre de la empresa para la búsqueda
        search_terms = clean_company_name(company_name).split('-')
        search_terms = [term for term in search_terms if len(term) > 2]  # Ignorar términos muy cortos
        
        # Analizar el contenido de la página
        soup = BeautifulSoup(content, 'html.parser')
        
        # Obtener texto relevante
        title = soup.title.string.lower() if soup.title else ''
        meta_description = soup.find('meta', {'name': 'description'})
        description = meta_description['content'].lower() if meta_description else ''
        
        # Buscar coincidencias
        text_to_search = f"{title} {description}".lower()
        
        # Contar cuántos términos de búsqueda aparecen
        matches = sum(1 for term in search_terms if term in text_to_search)
        
        # Consideramos que es una coincidencia si encontramos al menos el 50% de los términos
        return matches >= len(search_terms) * 0.5
        
    except:
        return False

def generate_possible_urls(company_name):
    """Genera y verifica posibles URLs para una empresa."""
    clean_name = clean_company_name(company_name)
    
    variations = [
        clean_name,
        f"www.{clean_name}",
        clean_name.replace('-', '')
    ]
    
    domains = ['.com', '.es', '.net', '.org', '.eu', '.com.es']
    
    session = create_session()
    valid_urls = []
    
    try:
        for variation in variations:
            for domain in domains:
                url = f"https://{variation}{domain}"
                if verify_company_url_match(url, company_name, session):
                    valid_urls.append(url)
    finally:
        session.close()
    
    return valid_urls

def process_excel(file_path, url_column='URL'):
    """Procesa un archivo Excel con nombres de empresas y URLs."""
    # Leer el Excel
    df = pd.read_excel(file_path)
    
    # Añadir columnas para los resultados
    df['URL_Válida'] = False
    df['URL_Sugerida'] = ''
    df['URL_Comentario'] = ''
    
    # Procesar cada fila
    for idx, row in df.iterrows():
        company_name = row['RAZON_SOCIAL']  # Ajusta el nombre de la columna según tu Excel
        current_url = row['URL'] if pd.notna(row[url_column]) else None
        
        if current_url:
            # Verificar URL existente
            session = create_session()
            try:
                is_valid = verify_company_url_match(current_url, company_name, session)
                df.at[idx, 'URL_Válida'] = is_valid
                df.at[idx, 'URL_Comentario'] = 'URL original válida' if is_valid else 'URL original no corresponde'
            finally:
                session.close()
        else:
            # Buscar URLs posibles
            possible_urls = generate_possible_urls(company_name)
            if possible_urls:
                df.at[idx, 'URL_Sugerida'] = possible_urls[0]
                df.at[idx, 'URL_Comentario'] = 'URL encontrada automáticamente'
            else:
                df.at[idx, 'URL_Comentario'] = 'No se encontró URL válida'
    
    # Guardar resultados
    output_file = file_path.replace('.xlsx', '_procesado.xlsx')
    df.to_excel(output_file, index=False)
    return output_file

# Ejemplo de uso
if __name__ == "__main__":
    file_path = input("Introduce la ruta del archivo Excel: ").strip('"')
    output_file = process_excel(file_path)
    print(f"\nProceso completado. Resultados guardados en: {output_file}")