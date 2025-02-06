import pandas as pd
import requests
import re
from urllib3.exceptions import InsecureRequestWarning
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import unicodedata
from bs4 import BeautifulSoup
from tqdm import tqdm 
import os
import concurrent.futures
import sqlite3
from datetime import datetime, timedelta
import json
import whois
import dns.resolver
from functools import wraps
import time
import sys
import signal

# Desactivar advertencias de SSL
urllib3.disable_warnings(InsecureRequestWarning)
def signal_handler(signum, frame):
    print("\nPrograma interrumpido por el usuario. Guardando progreso...")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
class RateLimiter:
    def __init__(self, calls_per_minute=30):
        self.calls_per_minute = calls_per_minute
        self.calls = []
    
    def __call__(self, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            now = time.time()
            # Limpiar llamadas antiguas
            self.calls = [call for call in self.calls if call > now - 60]
            
            if len(self.calls) >= self.calls_per_minute:
                sleep_time = self.calls[0] - (now - 60)
                if sleep_time > 0:
                    time.sleep(sleep_time)
            
            self.calls.append(now)
            return func(*args, **kwargs)
        return wrapper

class URLCache:
    def __init__(self, db_path='url_cache.db'):
        self.db_path = db_path
        self.setup_database()
    
    def setup_database(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS url_cache (
                    company_name TEXT PRIMARY KEY,
                    urls TEXT,
                    verification_data TEXT,
                    last_updated TIMESTAMP
                )
            ''')
    
    def get(self, company_name):
        with sqlite3.connect(self.db_path) as conn:
            result = conn.execute(
                'SELECT urls, verification_data, last_updated FROM url_cache WHERE company_name = ?',
                (company_name,)
            ).fetchone()
            
            if result:
                urls, data, last_updated = result
                last_updated = datetime.fromisoformat(last_updated)
                if datetime.now() - last_updated < timedelta(days=30):
                    return json.loads(urls), json.loads(data)
        return None, None
    
    def set(self, company_name, urls, verification_data):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                '''
                INSERT OR REPLACE INTO url_cache 
                (company_name, urls, verification_data, last_updated)
                VALUES (?, ?, ?, ?)
                ''',
                (
                    company_name,
                    json.dumps(list(urls)),
                    json.dumps(verification_data),
                    datetime.now().isoformat()
                )
            )

def remove_accents(text):
    """Elimina acentos y caracteres diacríticos del texto."""
    return ''.join(c for c in unicodedata.normalize('NFKD', text)
                  if not unicodedata.combining(c))

def clean_company_name(company_name):
    """Limpia y formatea el nombre de la empresa."""
    if not isinstance(company_name, str):
        return ""
    
    name = remove_accents(company_name)
    name = name.lower().strip()
    name = re.sub(r'[^\w\s-]', '', name)
    name = name.replace(' ', '-')
    name = re.sub(r'(-sa|-s\.a\.|sa)$', '', name, flags=re.IGNORECASE)
    name = re.sub(r'(-sl|-s\.l\.|sl)$', '', name, flags=re.IGNORECASE)
    name = name.rstrip('-')
    
    return name

def create_session():
    """Crea una sesión de requests con retry y timeouts configurados"""
    session = requests.Session()
    
    retry_strategy = Retry(
        total=1,  # Reduced from 3 to 1
        backoff_factor=0.5,  # Reduced from 1 to 0.5
        status_forcelist=[429, 500, 502, 503, 504],
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session

@RateLimiter(calls_per_minute=30)
def get_page_content(url, session):
    """Obtiene el contenido de una página web con rate limiting."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        print(f"Intentando acceder a {url}...")
        response = session.get(
            url, 
            timeout=(10, 20),  # Increased timeouts
            verify=False,
            headers=headers
        )
        response.raise_for_status()
        print(f"Acceso exitoso a {url}")
        return response.text
    except Exception as e:
        print(f"Error accediendo a {url}: {str(e)}")
        return None

def verify_company_url(url, company_name, session):
    """Verificación mejorada de URLs de empresa."""
    content = get_page_content(url, session)
    if not content:
        return False, None
    
    try:
        soup = BeautifulSoup(content, 'html.parser')
        
        # Extraer información relevante
        data = {
            'title': soup.title.string.lower() if soup.title else '',
            'meta_description': soup.find('meta', {'name': 'description'})['content'].lower() 
                if soup.find('meta', {'name': 'description'}) else '',
            'h1': ' '.join([h1.text.lower() for h1 in soup.find_all('h1')]),
            'contact_page': bool(soup.find('a', string=re.compile(r'contacto|contact', re.I))),
        }
        
        # Extraer y limitar teléfonos
        phones = re.findall(r'(?:\+34|0034|34)?[\s-]?[6789]\d{8}', content)
        # Eliminar duplicados y limitar a 3
        phones = list(dict.fromkeys(phones))[:3]
        data['phones'] = phones
        
        # Extraer links de redes sociales
        social_links = {
            'facebook': '',
            'twitter': '',
            'instagram': '',
            'linkedin': '',
            'youtube': ''
        }
        
        # Buscar enlaces de redes sociales
        for link in soup.find_all('a', href=True):
            href = link['href'].lower()
            if 'facebook.com' in href:
                social_links['facebook'] = href
            elif 'twitter.com' in href or 'x.com' in href:
                social_links['twitter'] = href
            elif 'instagram.com' in href:
                social_links['instagram'] = href
            elif 'linkedin.com' in href:
                social_links['linkedin'] = href
            elif 'youtube.com' in href:
                social_links['youtube'] = href
        
        data['social_links'] = social_links
        
        # Calcular puntuación
        search_terms = clean_company_name(company_name).split('-')
        search_terms = [term for term in search_terms if len(term) > 2]
        
        score = 0
        text_to_search = f"{data['title']} {data['meta_description']} {data['h1']}"
        
        matches = sum(1 for term in search_terms if term in text_to_search)
        score += (matches / len(search_terms)) * 50
        
        if data['contact_page']:
            score += 15
        if data['phones']:
            score += 15
        score += len([x for x in social_links.values() if x]) * 5
        
        data['score'] = score
        return score >= 60, data
        
    except Exception as e:
        return False, None

def generate_possible_urls(company_name):
    """Generación mejorada de posibles URLs."""
    clean_name = clean_company_name(company_name)
    base_name = clean_name.replace('-', '')
    
    variations = [
        clean_name,
        base_name,
        f"grupo{base_name}",
        f"group{base_name}",
        f"{base_name}group",
        f"{base_name}grupo",
        f"{base_name}online",
        f"{base_name}web",
    ]
    
    domains = ['.es', '.com', '.net', '.org', '.eu', '.com.es']
    valid_domains = set()
    
    for variation in variations:
        for domain in domains:
            domain_to_check = f"{variation}{domain}"
            try:
                dns.resolver.resolve(domain_to_check, 'A')
                valid_domains.add(f"https://www.{domain_to_check}")
                valid_domains.add(f"https://{domain_to_check}")
            except:
                continue
    
    return list(valid_domains)

def verify_urls_parallel(urls, company_name):
    def verify_single_url(url):
        session = create_session()
        try:
            print(f"\nIniciando verificación de {url}")
            future = concurrent.futures.ThreadPoolExecutor().submit(
                verify_company_url, url, company_name, session
            )
            is_valid, data = future.result(timeout=8)  # Timeout individual por URL
            return url, is_valid, data
        except (concurrent.futures.TimeoutError, Exception) as e:
            print(f"Timeout o error en {url}")
            return url, False, None
        finally:
            session.close()

    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        futures = {executor.submit(verify_single_url, url): url for url in urls}
        done, _ = concurrent.futures.wait(
            futures, 
            timeout=10,  # Timeout global reducido
            return_when=concurrent.futures.FIRST_COMPLETED
        )
        
        for future in done:
            try:
                url, is_valid, data = future.result(timeout=1)
                if is_valid:
                    results[url] = data
            except Exception:
                continue

        executor._threads.clear()
        concurrent.futures.thread._threads_queues.clear()
    
    return results

def get_whois_info(domain):
    """Obtiene información WHOIS del dominio."""
    try:
        domain_info = whois.whois(domain)
        return {
            'registrant': domain_info.registrant_name,
            'org': domain_info.org,
            'creation_date': domain_info.creation_date,
            'expiration_date': domain_info.expiration_date
        }
    except:
        return None

def process_excel(file_path, url_column='URL'):
    """Procesa un archivo Excel con nombres de empresas y URLs."""
    _, ext = os.path.splitext(file_path)
    cache = URLCache()
    
    try:
        if ext.lower() == '.xls':
            df = pd.read_excel(file_path, engine='xlrd')
        else:
            df = pd.read_excel(file_path, engine='openpyxl')
    except Exception as e:
        print(f"Error al leer el archivo: {str(e)}")
        return None
    
    # Inicializar columnas
    df['URL_Válida'] = False
    df['URLs_Encontradas'] = ''
    df['Teléfono_1'] = ''
    df['Teléfono_2'] = ''
    df['Teléfono_3'] = ''
    df['Facebook'] = ''
    df['Twitter'] = ''
    df['Instagram'] = ''
    df['LinkedIn'] = ''
    df['YouTube'] = ''
    df['Info_Adicional'] = ''
    
    print("\nProcesando URLs...")
    for idx, row in tqdm(df.iterrows(), total=len(df), desc="Verificando empresas"):
        try:
            print(f"\nProcesando empresa {idx + 1}: {row['RAZON_SOCIAL']}")
            company_name = row['RAZON_SOCIAL']
            
            # Verificar caché
            cached_urls, cached_data = cache.get(company_name)
            if cached_urls:
                valid_urls = cached_urls
                verification_data = cached_data
            else:
                # Generar y verificar URLs
                possible_urls = generate_possible_urls(company_name)
                if row[url_column] and pd.notna(row[url_column]):
                    possible_urls.append(row[url_column])
                
                print(f"URLs a verificar: {possible_urls}")
                verification_data = verify_urls_parallel(possible_urls, company_name)
                valid_urls = list(verification_data.keys())
                
                # Actualizar caché
                if verification_data:
                    cache.set(company_name, valid_urls, verification_data)
        
            if verification_data:
                # Actualizar DataFrame
                df.at[idx, 'URL_Válida'] = True
                df.at[idx, 'URLs_Encontradas'] = ', '.join(valid_urls)
                
                # Recopilar todos los teléfonos y redes sociales de todas las URLs válidas
                all_phones = []
                social_links = {
                    'facebook': '',
                    'twitter': '',
                    'instagram': '',
                    'linkedin': '',
                    'youtube': ''
                }
                
                for data in verification_data.values():
                    if 'phones' in data:
                        all_phones.extend(data['phones'])
                    if 'social_links' in data:
                        for network, link in data['social_links'].items():
                            if link and not social_links[network]:  # Tomar el primer link válido encontrado
                                social_links[network] = link
                
                # Asignar teléfonos (limitados a 3)
                unique_phones = list(dict.fromkeys(all_phones))[:3]
                for i, phone in enumerate(unique_phones, 1):
                    df.at[idx, f'Teléfono_{i}'] = phone
                
                # Asignar redes sociales
                df.at[idx, 'Facebook'] = social_links['facebook']
                df.at[idx, 'Twitter'] = social_links['twitter']
                df.at[idx, 'Instagram'] = social_links['instagram']
                df.at[idx, 'LinkedIn'] = social_links['linkedin']
                df.at[idx, 'YouTube'] = social_links['youtube']
                
                # Intentar obtener información WHOIS
                try:
                    whois_info = get_whois_info(valid_urls[0].split('/')[2])
                    if whois_info:
                        df.at[idx, 'Info_Adicional'] = json.dumps(whois_info, default=str)
                except:
                    pass
        except KeyboardInterrupt:
            print("\nGuardando progreso antes de salir...")
            output_file = os.path.splitext(file_path)[0] + '_procesado3.xlsx'
            df.to_excel(output_file, index=False, engine='openpyxl')
            sys.exit(0)
        except Exception as e:
            print(f"Error procesando empresa {idx + 1}: {str(e)}")
            continue
    
    output_file = os.path.splitext(file_path)[0] + '_procesado3.xlsx'
    df.to_excel(output_file, index=False, engine='openpyxl')
    return output_file
    
    output_file = os.path.splitext(file_path)[0] + '_procesado3.xlsx'
    try:
        df.to_excel(output_file, index=False, engine='openpyxl')
        return output_file
    except Exception as e:
        print(f"Error al guardar el archivo: {str(e)}")
        return None

if __name__ == "__main__":
    file_path = input("Introduce la ruta del archivo Excel: ").strip('"')
    output_file = process_excel(file_path)
    print(f"\nProceso completado. Resultados guardados en: {output_file}")
