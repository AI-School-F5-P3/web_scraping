import requests
import re
from urllib3.exceptions import InsecureRequestWarning
import urllib3
import unicodedata
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Desactivar advertencias de SSL para permitir verificación de URLs
urllib3.disable_warnings(InsecureRequestWarning)

def remove_accents(text):
    """Elimina acentos y caracteres diacríticos del texto."""
    return ''.join(c for c in unicodedata.normalize('NFKD', text)
                  if not unicodedata.combining(c))

def clean_company_name(company_name):
    """Limpia y formatea el nombre de la empresa."""
    # Primero eliminar acentos
    name = remove_accents(company_name)
    
    # Convertir a minúsculas y eliminar espacios iniciales/finales
    name = name.lower().strip()
    
    # Eliminar caracteres especiales y espacios
    name = re.sub(r'[^\w\s-]', '', name)
    name = re.sub(r'\s+', '', name)
    
    # Eliminar SA o SL al final del nombre
    name = re.sub(r'(-sa|-s\.a\.|sa)$', '', name, flags=re.IGNORECASE)
    name = re.sub(r'(-sl|-s\.l\.|sl)$', '', name, flags=re.IGNORECASE)
    
    # Eliminar guiones al final
    name = name.rstrip('-')
    
    return name

def create_session():
    """Crea una sesión de requests con retry y timeouts configurados"""
    session = requests.Session()
    
    # Configurar retry strategy
    retry_strategy = Retry(
        total=3,  # número total de intentos
        backoff_factor=1,  # tiempo entre reintentos
        status_forcelist=[429, 500, 502, 503, 504],  # códigos de error a reintentar
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session

def check_url_exists(url):
    """Verifica si una URL existe y es accesible."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
        'Connection': 'keep-alive',
    }
    
    session = create_session()
    
    try:
        # Separamos los timeouts: 15 segundos para conectar, 30 para leer
        response = session.get(
            url, 
            timeout=(15, 30),  # (connect timeout, read timeout)
            verify=False,
            allow_redirects=True,
            headers=headers
        )
        
        # Consideramos también redirecciones como válidas
        valid_status_codes = [200, 201, 301, 302, 307, 308]
        return response.status_code in valid_status_codes
        
    except requests.exceptions.Timeout:
        return True  # Asumimos que la página existe si hay timeout
    except requests.RequestException:
        return False
    finally:
        session.close()

def generate_possible_urls(company_name):
    """Genera y valida posibles URLs para una empresa."""
    clean_name = clean_company_name(company_name)
    
    # Lista de posibles variaciones
    variations = [
        clean_name,
        f"www.{clean_name}",
        clean_name.replace('-', '')
    ]
    
    # Lista de dominios comunes
    domains = [
        '.com',
        '.es',
        '.net',
        '.org',
        '.eu',
        '.com.es'
    ]
    
    # Generar todas las combinaciones posibles
    urls = []
    for variation in variations:
        for domain in domains:
            url = f"https://{variation}{domain}"
            exists = check_url_exists(url)
            urls.append({
                'url': url,
                'exists': exists
            })
    
    return urls

def print_results(urls):
    """Imprime los resultados de manera formateada."""
    print("\nResultados de la búsqueda de URLs:")
    print("-" * 60)
    
    # Separar URLs existentes y no existentes
    existing = [url for url in urls if url['exists']]
    non_existing = [url for url in urls if not url['exists']]
    
    if existing:
        print("\nURLs activas encontradas:")
        for url in existing:
            print(f"✓ {url['url']}")
    
    if non_existing:
        print("\nURLs no disponibles:")
        for url in non_existing:
            print(f"✗ {url['url']}")

# Ejemplo de uso
if __name__ == "__main__":
    company_name = input("Introduce el nombre de la empresa: ")
    urls = generate_possible_urls(company_name)
    print_results(urls)