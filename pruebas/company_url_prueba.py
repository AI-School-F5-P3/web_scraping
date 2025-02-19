import requests
import re
from urllib3.exceptions import InsecureRequestWarning
import urllib3
import unicodedata
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configurar logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Desactivar advertencias de SSL
urllib3.disable_warnings(InsecureRequestWarning)

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
        logger.debug(f"Intentando GET request para {url}")
        # Separamos los timeouts: 15 segundos para conectar, 30 para leer
        response = session.get(
            url, 
            timeout=(15, 30),  # (connect timeout, read timeout)
            verify=False,
            allow_redirects=True,
            headers=headers
        )
        
        logger.debug(f"Status code recibido: {response.status_code}")
        
        # Consideramos también redirecciones como válidas
        valid_status_codes = [200, 201, 301, 302, 307, 308]
        return response.status_code in valid_status_codes
        
    except requests.exceptions.Timeout as e:
        logger.debug(f"Timeout al acceder a {url}: {str(e)}")
        return True  # Asumimos que la página existe si hay timeout (mejor falso positivo que falso negativo)
    except requests.RequestException as e:
        logger.debug(f"Error al acceder a {url}: {str(e)}")
        return False
    finally:
        session.close()

# Código de prueba
if __name__ == "__main__":
    url = "https://www.elcorteingles.es"
    result = check_url_exists(url)
    print(f"\nResultado para {url}: {'✓' if result else '✗'}")