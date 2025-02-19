import requests
import re
from urllib3.exceptions import InsecureRequestWarning
import urllib3
import unicodedata

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

def check_url_exists(url):
    """Verifica si una URL existe y es accesible."""
    try:
        # Primero intentamos con HEAD request
        response = requests.head(
            url, 
            timeout=5, 
            verify=False, 
            allow_redirects=True,
            headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        )
        if 200 <= response.status_code < 400:
            return True
            
        # Si HEAD falla, intentamos con GET
        response = requests.get(
            url, 
            timeout=5, 
            verify=False, 
            allow_redirects=True,
            headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        )
        return 200 <= response.status_code < 400
    except requests.RequestException:
        return False

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