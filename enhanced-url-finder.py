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
    """Crea una sesión de requests con manejo de cookies y headers mejorados."""
    session = requests.Session()
    
    # Headers más completos para simular un navegador real
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'es-ES,es;q=0.8,en-US;q=0.5,en;q=0.3',
        'Accept-Encoding': 'gzip, deflate, br',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Cache-Control': 'max-age=0'
    }
    
    # Configurar cookies comunes de aceptación
    cookies = {
        'cookieconsent_status': 'allow',
        'cookies_accepted': 'true',
        'cookie_consent': 'accepted',
        'gdpr': 'accepted',
        'privacy_policy_accepted': 'true',
        'CookieConsent': 'true',
        'CONSENT_COOKIES': 'true',
        'euconsent-v2': 'accepted'
    }
    
    session.headers.update(headers)
    session.cookies.update(cookies)
    
    # Configurar retry strategy
    retry_strategy = Retry(
        total=2,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session

def detect_ecommerce(soup):
    """Detecta si una web tiene comercio electrónico."""
    # Lista de indicadores de ecommerce
    ecommerce_indicators = {
        'carrito_compra': [
            'carrito', 'cart', 'cesta', 'basket', 'shopping', 'comprar'
        ],
        'botones_compra': [
            'añadir al carrito', 'add to cart', 'comprar ahora', 'buy now',
            'realizar pedido', 'checkout', 'agregar al carrito', 'comprar'
        ]
        }
    
    score = 0
    evidence = []
    
    # Buscar en enlaces
    for link in soup.find_all('a', string=True):
        text = link.get_text().lower()
        href = link.get('href', '').lower()
        
        # Buscar indicadores en texto y href
        for category, indicators in ecommerce_indicators.items():
            for indicator in indicators:
                if indicator in text or indicator in href:
                    score += 1
                    evidence.append(f"Enlace encontrado: {text if text else href}")
                    break
    
    # Buscar formularios de compra
    forms = soup.find_all('form')
    for form in forms:
        action = form.get('action', '').lower()
        if any(term in action for term in ['cart', 'checkout', 'payment', 'compra', 'pago']):
            score += 2
            evidence.append(f"Formulario de compra encontrado: {action}")
    
    # Buscar elementos con clases/IDs típicos de ecommerce
    ecommerce_classes = ['cart', 'checkout', 'basket', 'shop', 'store', 'product', 'price']
    for class_name in ecommerce_classes:
        elements = soup.find_all(class_=re.compile(class_name))
        if elements:
            score += 1
            evidence.append(f"Elementos con clase '{class_name}' encontrados")
    
    # Buscar símbolos de moneda y precios
    price_pattern = r'(?:€|EUR)\s*\d+(?:[.,]\d{2})?|\d+(?:[.,]\d{2})?\s*(?:€|EUR)'
    text_content = soup.get_text()
    prices = re.findall(price_pattern, text_content, re.IGNORECASE)
    if prices:
        score += 2
        evidence.append(f"Precios encontrados: {len(prices)} ocurrencias")
    
    # Buscar meta tags relacionados con ecommerce
    meta_tags = soup.find_all('meta')
    for tag in meta_tags:
        content = tag.get('content', '').lower()
        name = tag.get('name', '').lower()
        if any(term in content or term in name for term in ['shop', 'store', 'product', 'ecommerce']):
            score += 1
            evidence.append(f"Meta tag de ecommerce encontrado: {name}")
    
    is_ecommerce = score >= 5
    return is_ecommerce, {
        'score': score,
        'evidence': evidence
    }

def verify_company_url(url, company_name, provincia, codigo_postal, session):
    """Verificación completa de URLs de empresa con manejo de cookies y extracción de datos."""
    try:
        if not url.startswith(('http://', 'https://')):
            url = f"https://{url}"
        
        print(f"\nIntentando acceder a {url}")
        
        # Primer intento - con cookies predefinidas
        response = session.get(
            url,
            timeout=(30, 60),
            verify=False,
            allow_redirects=True
        )
        
        # Si la respuesta es muy corta, podría ser una página de cookies
        content = response.text
        if len(content) < 5000:  # Umbral arbitrario para detectar páginas pequeñas
            print("Respuesta inicial muy corta, intentando con cookies adicionales...")
            
            # Intentar encontrar y extraer el token CSRF si existe
            soup = BeautifulSoup(content, 'html.parser')
            csrf_token = soup.find('input', {'name': ['csrf_token', '_csrf', 'CSRFToken']})
            if csrf_token:
                session.cookies.update({'csrf_token': csrf_token.get('value', '')})
            
            # Cookies específicas para sitios problemáticos conocidos
            if 'unilever' in url:
                specific_cookies = {
                    'OptanonAlertBoxClosed': '2024-02-13T12:00:00.000Z',
                    'OptanonConsent': 'isGpcEnabled=0&datestamp=2024-02-13T12:00:00&version=202309.1.0',
                    'euconsent-v2': 'CPykcQAPykcQAAGABCESC_CoAP_AAH_AAAAAJLNf_X__b2_r-_7_f_t0eY1P9_7__-0zjhfdl-8N3f_X_L8X42M7vF36tq4KuR4ku3bBIQdtHOncTUmx6olVrzPsbk2cr7NKJ7Pkmnsbe2dYGH9_n9_z_ZKZ7___f__7__________________________________________________________________',
                }
                session.cookies.update(specific_cookies)
            
            # Segundo intento con cookies actualizadas
            response = session.get(
                url,
                timeout=(30, 60),
                verify=False,
                allow_redirects=True
            )
            content = response.text
        
        if not content:
            print(f"No se pudo obtener contenido de {url}")
            return False, None
        
        soup = BeautifulSoup(content, 'html.parser')
        
        # Verificar si seguimos en una página de cookies
        cookie_indicators = [
            'cookie',
            'gdpr',
            'consent',
            'privacidad',
            'privacy',
            'aceptar',
            'accept'
        ]
        
        main_content = soup.find(['main', 'article', 'div'], class_=lambda x: x and 'content' in x.lower())
        if not main_content and all(indicator in content.lower() for indicator in cookie_indicators):
            print("Parece que seguimos en una página de cookies")
            return False, None
        
        # Extraer información básica
        data = {
            'title': soup.title.string.lower() if soup.title else '',
            'meta_description': soup.find('meta', {'name': 'description'})['content'].lower() 
                if soup.find('meta', {'name': 'description'}) else '',
            'h1': ' '.join([h1.text.lower() for h1 in soup.find_all('h1')]),
            'contact_page': bool(soup.find('a', string=re.compile(r'contacto|contact', re.I))),
            'all_text': soup.get_text().lower()
        }
        
        # Detectar ecommerce
        is_ecommerce, ecommerce_data = detect_ecommerce(soup)
        data['has_ecommerce'] = is_ecommerce
        data['ecommerce_data'] = ecommerce_data
        
        # Extraer teléfonos
        phones = set()
        
        def is_valid_spanish_phone(phone):
            clean_phone = re.sub(r'[^\d+]', '', phone)
            if clean_phone.startswith('+34'):
                clean_phone = clean_phone[3:]
            elif clean_phone.startswith('0034'):
                clean_phone = clean_phone[4:]
            elif clean_phone.startswith('34'):
                clean_phone = clean_phone[2:]
            return (len(clean_phone) == 9 and clean_phone[0] in '6789')
        
        def clean_and_format_phone(phone):
            clean_phone = re.sub(r'[^\d+]', '', phone)
            if not clean_phone:
                return None
            if clean_phone.startswith('+34'):
                clean_phone = clean_phone[3:]
            elif clean_phone.startswith('0034'):
                clean_phone = clean_phone[4:]
            elif clean_phone.startswith('34'):
                clean_phone = clean_phone[2:]
            if len(clean_phone) == 9 and clean_phone[0] in '6789':
                return f"+34{clean_phone}"
            return None
        
        # Buscar teléfonos en enlaces tel:
        tel_links = soup.find_all('a', href=re.compile(r'^tel:'))
        for link in tel_links:
            href = link.get('href', '')
            formatted_phone = clean_and_format_phone(href.replace('tel:', ''))
            if formatted_phone:
                phones.add(formatted_phone)
        
        # Buscar teléfonos en el texto
        phone_patterns = [
            r'(?:\+34|0034|34)?[\s-]?[6789]\d{2}[\s-]?(?:\d{2}[\s-]?){3}',
            r'[9]\d{2}[\s-]?\d{2}[\s-]?\d{2}[\s-]?\d{2}',
            r'(?:(?:Tel[eé]fono|Tel)[\s:.-]+)?(?:\+34|0034|34)?[\s-]?[6789]\d{2}[\s-]?(?:\d{2}[\s-]?){3}'
        ]
        
        for element in soup.find_all(['p', 'div', 'span', 'a']):
            if element.string:
                text = element.string.strip()
                for pattern in phone_patterns:
                    found_phones = re.finditer(pattern, text, re.IGNORECASE)
                    for match in found_phones:
                        phone = match.group()
                        formatted_phone = clean_and_format_phone(phone)
                        if formatted_phone:
                            phones.add(formatted_phone)
        
        # Buscar teléfonos en todo el texto
        full_text = soup.get_text()
        tel_sections = re.finditer(r'(?:Tel[eé]fono|Tel)[:\s.-]+([^<>\n]{1,50})', full_text, re.IGNORECASE)
        for section in tel_sections:
            text_after_tel = section.group(1)
            for pattern in phone_patterns:
                found_phones = re.finditer(pattern, text_after_tel)
                for match in found_phones:
                    phone = match.group()
                    formatted_phone = clean_and_format_phone(phone)
                    if formatted_phone:
                        phones.add(formatted_phone)
        
        data['phones'] = list(phones)[:3]
        
        # Extraer redes sociales
        social_links = {
            'facebook': '',
            'twitter': '',
            'instagram': '',
            'linkedin': '',
            'youtube': ''
        }
        
        social_patterns = {
            'facebook': r'facebook\.com/(?!sharer|share)([^/?&]+)',
            'twitter': r'twitter\.com/(?!share|intent)([^/?&]+)',
            'instagram': r'instagram\.com/([^/?&]+)',
            'linkedin': r'linkedin\.com/(?:company|in)/([^/?&]+)',
            'youtube': r'youtube\.com/(?:user|channel|c)/([^/?&]+)'
        }
        
        for link in soup.find_all('a', href=True):
            href = link['href'].lower()
            if 'sharer' in href or 'share?' in href or 'intent/tweet' in href:
                continue
            for network, pattern in social_patterns.items():
                if network in href:
                    match = re.search(pattern, href)
                    if match:
                        social_links[network] = href
        
        data['social_links'] = social_links
        
        # Calcular puntuación
        score = 0
        text_to_search = f"{data['title']} {data['meta_description']} {data['h1']} {data['all_text']}"
        
        # 1. Búsqueda de provincia
        provincia = provincia.lower() if isinstance(provincia, str) else ''
        if provincia and provincia in text_to_search:
            score += 40
            data['found_provincia'] = True
        else:
            data['found_provincia'] = False
        
        # 2. Búsqueda de código postal
        if isinstance(codigo_postal, (int, float)):
            codigo_postal = str(int(codigo_postal)).zfill(5)
        elif isinstance(codigo_postal, str):
            codigo_postal = codigo_postal.zfill(5)
        
        if codigo_postal and codigo_postal in text_to_search:
            score += 20
            data['found_codigo_postal'] = True
        else:
            data['found_codigo_postal'] = False
        
        # 3. Búsqueda del nombre de la empresa
        search_terms = clean_company_name(company_name).split('-')
        search_terms = [term for term in search_terms if len(term) > 2]
        if search_terms:
            matches = sum(1 for term in search_terms if term in text_to_search)
            score += (matches / len(search_terms)) * 20
        
        # 4. Elementos adicionales
        if data['contact_page']:
            score += 8
        if data['phones']:
            score += 7
        score += min(5, len([x for x in social_links.values() if x]))
        
        data['score'] = score
        data['score_breakdown'] = {
            'provincia_score': 40 if data['found_provincia'] else 0,
            'codigo_postal_score': 20 if data['found_codigo_postal'] else 0,
            'company_name_score': (matches / len(search_terms)) * 20 if search_terms else 0,
            'additional_elements_score': score - ((40 if data['found_provincia'] else 0) + 
                                               (20 if data['found_codigo_postal'] else 0) + 
                                               ((matches / len(search_terms)) * 20 if search_terms else 0))
        }
        
        # Se considera válida si alcanza al menos 30 puntos
        return score >= 30, data
        
    except Exception as e:
        print(f"Error en verify_company_url para {url}: {str(e)}")
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
    
    domains = ['.es', '.com', '.net', '.org', '.cat', '.eus', '.gal']
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

def verify_urls_parallel(urls, company_name, provincia, codigo_postal):
    def verify_single_url(url):
        session = create_session()
        try:
            print(f"\nIniciando verificación de {url}")
            future = concurrent.futures.ThreadPoolExecutor().submit(
                verify_company_url, url, company_name, provincia, codigo_postal, session
            )
            is_valid, data = future.result(timeout=8)  # Timeout individual por URL
            return url, is_valid, data
        except (concurrent.futures.TimeoutError, Exception) as e:
            print(f"Timeout o error en {url}: {str(e)}")
            return url, False, None
        finally:
            session.close()

    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        futures = {executor.submit(verify_single_url, url): url for url in urls}
        done, _ = concurrent.futures.wait(
            futures, 
            timeout=30,  # Timeout global
            return_when=concurrent.futures.ALL_COMPLETED
        )
        
        for future in done:
            try:
                url, is_valid, data = future.result(timeout=1)
                if is_valid:
                    results[url] = data
            except Exception as e:
                print(f"Error en future.result: {str(e)}")
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
    
    # Asegurarse de que el código postal tenga 5 dígitos
    df['COD_POSTAL'] = df['COD_POSTAL'].astype(str).apply(lambda x: x.zfill(5))
    
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
    df['Tiene_Ecommerce'] = False
    df['Ecommerce_Score'] = 0
    df['Ecommerce_Evidencia'] = ''
    df['Score_Total'] = 0
    df['Score_Provincia'] = 0
    df['Score_CodigoPostal'] = 0
    df['Score_NombreEmpresa'] = 0
    df['Score_ElementosAdicionales'] = 0
    
    print("\nProcesando URLs...")
    for idx, row in tqdm(df.iterrows(), total=len(df), desc="Verificando empresas"):
        try:
            print(f"\nProcesando empresa {idx + 1}: {row['RAZON_SOCIAL']}")
            company_name = row['RAZON_SOCIAL']
            provincia = row['NOM_PROVINCIA']
            codigo_postal = row['COD_POSTAL']
            provided_url = row[url_column]
            
            # Verificar caché primero
            cached_urls, cached_data = cache.get(company_name)
            if cached_urls:
                print("Usando datos en caché")
                valid_urls = cached_urls
                verification_data = cached_data
            else:
                verification_data = {}
                valid_urls = []
                
                # Primero intentar con la URL proporcionada si existe
                if provided_url and pd.notna(provided_url):
                    # Asegurar que la URL tenga el formato correcto
                    if not provided_url.startswith(('http://', 'https://')):
                        provided_url = f"https://{provided_url}"
                    
                    print(f"Intentando URL proporcionada: {provided_url}")
                    session = create_session()
                    is_valid, data = verify_company_url(provided_url, company_name, provincia, codigo_postal, session)
                    
                    if is_valid:
                        print("URL proporcionada válida")
                        verification_data[provided_url] = data
                        valid_urls.append(provided_url)
                    else:
                        print("URL proporcionada no válida, generando alternativas...")
                        # Solo si la URL proporcionada falla, intentamos generar alternativas
                        possible_urls = generate_possible_urls(company_name)
                        verification_data = verify_urls_parallel(possible_urls, company_name, provincia, codigo_postal)
                        valid_urls = list(verification_data.keys())
                
                # Actualizar caché si encontramos algo
                if verification_data:
                    cache.set(company_name, valid_urls, verification_data)
            
            # El resto del procesamiento sigue igual...
            if verification_data:
                # Actualizar DataFrame
                df.at[idx, 'URL_Válida'] = True
                df.at[idx, 'URLs_Encontradas'] = ', '.join(valid_urls)
                
                # Recopilar todos los teléfonos y redes sociales
                all_phones = []
                social_links = {
                    'facebook': '',
                    'twitter': '',
                    'instagram': '',
                    'linkedin': '',
                    'youtube': ''
                }
                
                # Variables para ecommerce y scores
                has_ecommerce = False
                max_ecommerce_score = 0
                all_evidence = []
                best_scores = {
                    'total': 0,
                    'provincia': 0,
                    'codigo_postal': 0,
                    'nombre_empresa': 0,
                    'elementos_adicionales': 0
                }
                
                for data in verification_data.values():
                    if data:  # Asegurarse de que data no es None
                        if 'phones' in data:
                            all_phones.extend(data['phones'])
                        if 'social_links' in data:
                            for network, link in data['social_links'].items():
                                if link and not social_links[network]:
                                    social_links[network] = link
                        if 'has_ecommerce' in data:
                            if data['has_ecommerce']:
                                has_ecommerce = True
                            if data['ecommerce_data']['score'] > max_ecommerce_score:
                                max_ecommerce_score = data['ecommerce_data']['score']
                            all_evidence.extend(data['ecommerce_data']['evidence'])
                        
                        # Actualizar mejores scores
                        if data.get('score', 0) > best_scores['total']:
                            best_scores['total'] = data.get('score', 0)
                            score_breakdown = data.get('score_breakdown', {})
                            best_scores['provincia'] = score_breakdown.get('provincia_score', 0)
                            best_scores['codigo_postal'] = score_breakdown.get('codigo_postal_score', 0)
                            best_scores['nombre_empresa'] = score_breakdown.get('company_name_score', 0)
                            best_scores['elementos_adicionales'] = score_breakdown.get('additional_elements_score', 0)
                
                # Asignar valores al DataFrame
                unique_phones = list(dict.fromkeys(all_phones))[:3]
                for i, phone in enumerate(unique_phones, 1):
                    df.at[idx, f'Teléfono_{i}'] = phone
                
                df.at[idx, 'Facebook'] = social_links['facebook']
                df.at[idx, 'Twitter'] = social_links['twitter']
                df.at[idx, 'Instagram'] = social_links['instagram']
                df.at[idx, 'LinkedIn'] = social_links['linkedin']
                df.at[idx, 'YouTube'] = social_links['youtube']
                
                df.at[idx, 'Tiene_Ecommerce'] = has_ecommerce
                df.at[idx, 'Ecommerce_Score'] = max_ecommerce_score
                df.at[idx, 'Ecommerce_Evidencia'] = '; '.join(all_evidence)
                
                df.at[idx, 'Score_Total'] = best_scores['total']
                df.at[idx, 'Score_Provincia'] = best_scores['provincia']
                df.at[idx, 'Score_CodigoPostal'] = best_scores['codigo_postal']
                df.at[idx, 'Score_NombreEmpresa'] = best_scores['nombre_empresa']
                df.at[idx, 'Score_ElementosAdicionales'] = best_scores['elementos_adicionales']
                
        except KeyboardInterrupt:
            print("\nGuardando progreso antes de salir...")
            output_file = os.path.splitext(file_path)[0] + '_procesado.xlsx'
            df.to_excel(output_file, index=False, engine='openpyxl')
            sys.exit(0)
        except Exception as e:
            print(f"Error procesando empresa {idx + 1}: {str(e)}")
            continue
    
    output_file = os.path.splitext(file_path)[0] + '_procesado.xlsx'
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
