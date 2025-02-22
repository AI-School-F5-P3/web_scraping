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
import socket
import threading

# Desactivar advertencias de SSL
urllib3.disable_warnings(InsecureRequestWarning)
def signal_handler(signum, frame):
    print("\nPrograma interrumpido por el usuario. Guardando progreso...")
    print("Cerrando hilos pendientes...")
    os._exit(1)  # Salida forzada del proceso

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
    
    # Patrones extendidos para eliminar diferentes variantes
    patterns = [
        r'(-sa|-s\.a\.|sa|sociedad-anonima|sociedad-anonyma)$',
        r'(-sl|-s\.l\.|sl|sociedad-limitada)$'
    ]
    
    for pattern in patterns:
        name = re.sub(pattern, '', name, flags=re.IGNORECASE)
    
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
    
def detect_ecommerce(soup):
    """Detecta si una web tiene comercio electrónico."""
    # Lista de indicadores de ecommerce
    ecommerce_indicators = {
        'carrito_compra': [
            'carrito', 'cart', 'cesta', 'basket', 'shopping', 'comprar'
        ],
        'botones_compra': [
            'añadir al carrito', 'add to cart', 'comprar ahora', 'buy now',
            'realizar pedido', 'checkout', 'agregar al carrito', 'comprar', 'tienda online'
        ],
        
        'elementos_tienda': [
            'tienda', 'shop', 'store', 'catálogo', 'catalog', 'productos', 'products'
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

def verify_company_url(url, company_name, session, provincia=None, codigo_postal=None, nif=None):
    """Verificación mejorada de URLs de empresa con datos del Excel."""
    
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
        
        # Detectar ecommerce
        is_ecommerce, ecommerce_data = detect_ecommerce(soup)
        data['has_ecommerce'] = is_ecommerce
        data['ecommerce_data'] = ecommerce_data
        
        # Mejorada la extracción de teléfonos
        phones = set()  # Usamos set para evitar duplicados automáticamente
        
        # 1. Buscar enlaces tipo tel:
        tel_links = soup.find_all('a', href=re.compile(r'^tel:'))
        for link in tel_links:
            href = link.get('href', '')
            phone = re.sub(r'[^\d+]', '', href.replace('tel:', ''))
            if phone.startswith('+'):
                phones.add(phone)
            elif phone.startswith('34'):
                phones.add(f"+{phone}")
            elif len(phone) == 9:  # Número español sin prefijo
                phones.add(f"+34{phone}")
        
        # 2. Buscar en el texto con patrón mejorado
        phone_pattern = r'(?:\+34|0034|34)?[\s-]?(?:[\s-]?\d){9}'
        
        # Buscar teléfonos en elementos de texto
        for element in soup.find_all(['p', 'div', 'span', 'a']):
            if element.string:
                found_phones = re.findall(phone_pattern, element.string)
                for phone in found_phones:
                    clean_phone = re.sub(r'[^\d]', '', phone)
                    if len(clean_phone) == 9:
                        phones.add(f"+34{clean_phone}")
                    elif len(clean_phone) > 9:
                        phones.add(f"+{clean_phone}")
        
        # 3. Buscar en atributos data-* que podrían contener teléfonos
        for element in soup.find_all(attrs=re.compile(r'^data-')):
            for attr_name, attr_value in element.attrs.items():
                if isinstance(attr_value, str):  # Asegurarse de que el valor es string
                    found_phones = re.findall(phone_pattern, attr_value)
                    for phone in found_phones:
                        clean_phone = re.sub(r'[^\d]', '', phone)
                        if len(clean_phone) == 9:
                            phones.add(f"+34{clean_phone}")
                        elif len(clean_phone) > 9:
                            phones.add(f"+{clean_phone}")
        
        # Convertir el set a lista y limitar a 3 teléfonos
        data['phones'] = list(phones)[:3]
        
        # Mejorar la extracción de links de redes sociales
        social_links = {
            'facebook': '',
            'twitter': '',
            'instagram': '',
            'linkedin': '',
            'youtube': ''
        }
        
        # Patrones mejorados para redes sociales
        social_patterns = {
            'facebook': r'facebook\.com/(?!sharer|share)([^/?&]+)',
            'twitter': r'twitter\.com/(?!share|intent)([^/?&]+)',
            'instagram': r'instagram\.com/([^/?&]+)',
            'linkedin': r'linkedin\.com/(?:company|in)/([^/?&]+)',
            'youtube': r'youtube\.com/(?:user|channel|c)/([^/?&]+)'
        }
        
        # Buscar enlaces de redes sociales
        for link in soup.find_all('a', href=True):
            href = link['href'].lower()
            
            # Ignorar links de compartir
            if 'sharer' in href or 'share?' in href or 'intent/tweet' in href:
                continue
                
            for network, pattern in social_patterns.items():
                if network in href:
                    match = re.search(pattern, href)
                    if match:
                        social_links[network] = href
        
        data['social_links'] = social_links
        
        # NUEVO: Verificación de datos del Excel en la página web
        # Texto completo de la página para buscar coincidencias
        full_text = soup.get_text().lower()
        
        # Inicializar flags para los datos del Excel
        provincia_found = False
        codigo_postal_found = False
        nif_found = False
        
        # 1. Verificar si la provincia del Excel aparece en la página
        if provincia and isinstance(provincia, str):
            provincia_lower = provincia.lower()
            # Normalizar provincia sin acentos para la búsqueda
            provincia_norm = remove_accents(provincia_lower)
            
            # Lista de variantes comunes de nombres de provincia
            provincia_variants = {
                'a coruña': ['a coruna', 'la coruña', 'la coruna', 'coruña', 'coruna'],
                'álava': ['alava', 'araba'],
                'alicante': ['alacant'],
                'asturias': ['principado de asturias', 'oviedo'],
                'baleares': ['illes balears', 'islas baleares', 'balears', 'mallorca'],
                'castellón': ['castellon', 'castello'],
                'guipúzcoa': ['guipuzcoa', 'gipuzkoa'],
                'gerona': ['girona'],
                'lérida': ['lleida', 'lerida'],
                'orense': ['ourense'],
                'vizcaya': ['bizkaia'],
            }
            
            # Preparar lista de búsqueda con la provincia y sus variantes
            search_terms = [provincia_lower, provincia_norm]
            
            # Añadir variantes si existen
            for key, variants in provincia_variants.items():
                if provincia_lower == key or provincia_lower in variants:
                    search_terms.extend(variants)
                    if provincia_lower != key:
                        search_terms.append(key)
            
            # Buscar cada variante en el texto
            for term in search_terms:
                if term in full_text:
                    provincia_found = True
                    data['provincia_found'] = provincia
                    print(f"Provincia '{provincia}' encontrada en la página (+15 puntos)")
                    break
        
        # 2. Verificar si el código postal del Excel aparece en la página
        if codigo_postal and (isinstance(codigo_postal, str) or isinstance(codigo_postal, int)):
            # Asegurar que el código postal tenga 5 dígitos
            cp_str = str(codigo_postal).strip()
            if len(cp_str) == 4:
                cp_str = '0' + cp_str
            
            # Buscar el código postal con diferentes formatos
            cp_patterns = [
                rf'\b{cp_str}\b',
                rf'CP\s*{cp_str}',
                rf'C\.P\.\s*{cp_str}',
                rf'Código\s+Postal\s*:?\s*{cp_str}',
            ]
            
            for pattern in cp_patterns:
                if re.search(pattern, full_text, re.IGNORECASE):
                    codigo_postal_found = True
                    data['codigo_postal_found'] = cp_str
                    print(f"Código postal '{cp_str}' encontrado en la página (+20 puntos)")
                    break
        
        # 3. Verificar si el NIF del Excel aparece en la página
        if nif and isinstance(nif, str):
            nif_clean = nif.upper().strip()
            # Patrones para NIF/CIF español
            nif_patterns = [
                rf'\b{nif_clean}\b',
                rf'NIF\s*:?\s*{nif_clean}',
                rf'CIF\s*:?\s*{nif_clean}',
                rf'N\.I\.F\.\s*:?\s*{nif_clean}',
                rf'C\.I\.F\.\s*:?\s*{nif_clean}',
            ]
            
            for pattern in nif_patterns:
                if re.search(pattern, full_text, re.IGNORECASE):
                    nif_found = True
                    data['nif_found'] = nif_clean
                    print(f"NIF '{nif_clean}' encontrado en la página (+40 puntos)")
                    break
        
        # MODIFICACIÓN: Verificación más flexible para marcas reconocidas
        company_name_lower = company_name.lower()
        text_to_search = f"{data['title']} {data['meta_description']} {data['h1']}"

        # Inicializar puntuación
        score = 0

        # Si la razón social coincide completamente, asignar 100 puntos
        total_words = company_name_lower.split()
        if company_name_lower in text_to_search.lower():
            score = 100
            print(f"Razón social '{company_name}' encontrada completamente en la página (100 puntos)")
        else:
            # Reducir puntuación eliminando palabras de atrás hacia adelante
            for i in range(len(total_words), 0, -1):
                partial_name = ' '.join(total_words[:i])
                if partial_name in text_to_search.lower():
                    score = (i / len(total_words)) * 100  # Puntuación proporcional
                    print(f"Coincidencia parcial encontrada: '{partial_name}' ({score:.2f} puntos)")
                    break

       # Si está en título o descripción meta, bonificación extra
        if company_name_lower in data['title'].lower() or company_name_lower in data['meta_description'].lower():
            score += 15  # Bonificación extra

        

        
        # Puntuación base
        if data['contact_page']:
            score += 15
        if data['phones']:
            score += 15
        score += len([x for x in social_links.values() if x]) * 5
        
        # ACTUALIZADO: Asignar puntuación adicional por datos del Excel encontrados
        if provincia_found:
            score += 15
        if codigo_postal_found:
            score += 20
        if nif_found:
            score += 100
        
        data['score'] = score
        print(f"Puntuación de verificación para {url}: {score}")
        return score >= 60, data
        
    except Exception as e:
        import traceback
        print(f"Error en verify_company_url: {str(e)}")
        print(traceback.format_exc())
        return False, None

def generate_possible_urls(company_name, excel_url=None, provincia=None):
    print(f"\nGenerando URLs para: {company_name}")
    print(f"Provincia: {provincia}")
    
    valid_domains = set()  # Usaremos este set para almacenar las URLs válidas
    
    # Step 1: Check Excel URL first if available
    if excel_url and isinstance(excel_url, str) and excel_url.strip():
        url = excel_url.strip()
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        return [url], True  # Return flag to indicate this is an Excel URL
    
    # Inner function for domain verification
    def verify_domain(url):
        domain = url.replace('https://', '').replace('http://', '')
        if domain.startswith('www.'):
            base_domain = domain[4:]
        else:
            base_domain = domain
            
        # Método 1: DNS resolver
        try:
            dns.resolver.resolve(base_domain, 'A')
            return True
        except:
            try:
                dns.resolver.resolve('www.' + base_domain, 'A')
                return True
            except:
                # Método 2: Socket como fallback
                try:
                    socket.gethostbyname(domain)
                    return True
                except socket.gaierror:
                    try:
                        socket.gethostbyname('www.' + base_domain)
                        return True
                    except:
                        return False
    
    # Step 2: Generate word combinations from clean name
    clean_name = clean_company_name(company_name)
    words = clean_name.split('-')
    print(f"Palabras limpias: {words}")
    
    # Determine domains based on provincia
    base_domains = ['.es', '.com']
    regional_domains = []

    if provincia:
        provincia_norm = remove_accents(str(provincia).strip().upper())
        print(f"Provincia normalizada para comparación: '{provincia_norm}'")
        
        if any(city in provincia_norm for city in ["BARCELONA", "TARRAGONA", "LERIDA", "LLEIDA", "GERONA", "GIRONA"]):
            regional_domains.append('.cat')
            print(f"✓ Dominio regional .cat añadido por provincia: {provincia}")
        
        if any(city in provincia_norm for city in ["LA CORUNA", "LUGO", "ORENSE", "PONTEVEDRA", "A CORUNA", "OURENSE"]):
            regional_domains.append('.gal')
            print(f"✓ Dominio regional .gal añadido por provincia: {provincia}")
        
        if any(city in provincia_norm for city in ["ALAVA", "VIZCAYA", "GUIPUZCOA", "ARABA", "BIZKAIA", "GIPUZKOA"]):
            regional_domains.append('.eus')
            print(f"✓ Dominio regional .eus añadido por provincia: {provincia}")

    domains = base_domains + regional_domains
    print(f"Dominios a comprobar: {domains}")
    
    # Generate all possible combinations
    word_combinations = set()  # Usamos set para evitar duplicados

    # Remove 'grupo' and 'group' if they exist in words
    words_without_prefix = [w for w in words if w not in ['grupo', 'group']]

    # Generate progressive combinations
    for i in range(len(words_without_prefix), 0, -1):
        base_combination = ''.join(words_without_prefix[:i])
        
        # Add base combination without prefix
        word_combinations.add(base_combination)
        
        # Add combinations with 'grupo' and 'group' prefixes
        word_combinations.add('grupo' + base_combination)
        
        
        # Add combinations with 'grupo' and 'group' suffixes
        
        word_combinations.add(base_combination + 'group')
        
        # If we have remaining words, try combinations with them
        if i < len(words_without_prefix):
            next_word = ''.join(words_without_prefix[i:i+1])
            if next_word:
                # Base combination with next word
                word_combinations.add(base_combination + next_word)
                
                # Prefix variations with next word
                word_combinations.add('grupo' + base_combination + next_word)
                
                
                # Suffix variations with next word
                
                word_combinations.add(base_combination + next_word + 'group')

    # Crear un segundo conjunto de variaciones eliminando la primera palabra
    if len(words) > 1 and words[0] not in ['grupo']:
        # Obtener palabras sin la primera
        words_without_first = words[1:]
        secondary_combinations = set()
        
        # Generar combinaciones progresivas con las palabras restantes
        for i in range(len(words_without_first), 0, -1):
            base_combination = ''.join(words_without_first[:i])
            
            # Añadir solo la combinación básica sin prefijos ni sufijos
            if base_combination:
                secondary_combinations.add(base_combination)
            
            # Si tenemos palabras restantes, probar combinaciones con ellas
            if i < len(words_without_first):
                next_word = ''.join(words_without_first[i:i+1])
                if next_word and base_combination:
                    # Solo combinación base con siguiente palabra
                    secondary_combinations.add(base_combination + next_word)
        
        # Añadir las combinaciones secundarias al conjunto principal
        word_combinations.update(secondary_combinations)
        print(f"Añadidas {len(secondary_combinations)} combinaciones adicionales sin la primera palabra '{words[0]}'")

    # Add original combinations if 'grupo' or 'group' was in the name
    if any(word in ['grupo', 'group'] for word in words):
        original_combination = ''.join(words)
        word_combinations.add(original_combination)
    
    print("Combinaciones generadas:")
    for comb in sorted(word_combinations):
        print(f"- {comb}")
    
    # Generate and verify URLs for each word combination
    for combination in word_combinations:
        for domain in domains:
            for url in [
                f"https://www.{combination}{domain}",
                f"https://{combination}{domain}"
            ]:
                if verify_domain(url):
                    valid_domains.add(url)
                    print(f"✓ Dominio válido encontrado: {url}")
                else:
                    print(f"✗ Dominio no válido: {url}")
    
    return list(valid_domains), False

def verify_urls_parallel(urls, company_name, provincia=None, codigo_postal=None, nif=None):
    def verify_single_url(url):
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
            
        session = create_session()
        try:
            print(f"\nIniciando verificación de {url}")
            result = verify_company_url(url, company_name, session, provincia, codigo_postal, nif)
            return url, *result
        except Exception as e:
            print(f"Error verificando {url}: {str(e)}")
            return url, False, None
        finally:
            session.close()

    results = {}
    active_threads = []
    
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            # Submit all tasks and keep track of futures
            future_to_url = {
                executor.submit(verify_single_url, url): url 
                for url in urls
            }
            
            # Process completed futures as they finish
            for future in concurrent.futures.as_completed(future_to_url, timeout=30):
                url = future_to_url[future]
                try:
                    url, is_valid, data = future.result(timeout=5)
                    if is_valid:
                        results[url] = data
                except concurrent.futures.TimeoutError:
                    print(f"Timeout al procesar {url}")
                except Exception as e:
                    print(f"Error al procesar {url}: {str(e)}")
                
                # Keep track of thread IDs
                for thread in threading.enumerate():
                    if thread != threading.main_thread():
                        active_threads.append(thread.ident)
                
    except (KeyboardInterrupt, Exception) as e:
        print(f"\nInterrupción o error detectado: {str(e)}")
    finally:
        # Clean up any remaining threads
        current_threads = threading.enumerate()
        main_thread = threading.main_thread()
        
        for thread in current_threads:
            if thread != main_thread:
                try:
                    print(f"Cerrando hilo: {thread.name}")
                    thread._stop()
                except Exception as e:
                    print(f"No se pudo cerrar el hilo {thread.name}: {str(e)}")
    
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

def process_excel(file_path, url_column='URL', provincia_column='NOM_PROVINCIA', cp_column='COD_POSTAL', nif_column='NIF'):
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
    
    # Initialize columns
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
    df['Provincia_En_Web'] = False
    df['CP_En_Web'] = False
    df['NIF_En_Web'] = False
    df['Score_URL'] = 0  # Nueva columna para guardar la puntuación
    
    print("\nProcesando URLs...")
    for idx, row in tqdm(df.iterrows(), total=len(df), desc="Verificando empresas"):
        try:
            print(f"\nProcesando empresa {idx + 1}: {row['RAZON_SOCIAL']}")
            company_name = row['RAZON_SOCIAL']
            excel_url = row[url_column] if pd.notna(row[url_column]) else None
            
            # Obtener datos adicionales del Excel
            provincia = row[provincia_column] if provincia_column in row and pd.notna(row[provincia_column]) else None
            
            codigo_postal = None
            if cp_column in row and pd.notna(row[cp_column]):
                cp_value = row[cp_column]
                if isinstance(cp_value, (int, float)):
                    codigo_postal = str(int(cp_value)).zfill(5)
                else:
                    codigo_postal = str(cp_value).strip().zfill(5)
            
            nif = row[nif_column] if nif_column in row and pd.notna(row[nif_column]) else None
            
            # Check cache first
            cached_urls, cached_data = cache.get(company_name)
            if cached_urls:
                valid_urls = cached_urls
                verification_data = cached_data
            else:
                verification_data = {}
                all_valid_urls_data = {}
                max_score = 0
                best_url_data = None
                
                # Generate URLs in batches, starting with Excel URL
                possible_urls, is_excel_url = generate_possible_urls(company_name, excel_url, provincia)
                
                if is_excel_url:
                    # Verify Excel URL first
                    try:
                        verification_results = verify_urls_parallel(
                            possible_urls, 
                            company_name, 
                            provincia, 
                            codigo_postal, 
                            nif
                        )
                        
                        # Process results
                        if verification_results:
                            best_url = max(
                                verification_results.items(),
                                key=lambda x: x[1].get('score', 0)
                            )[0]
                            
                            verification_data = {best_url: verification_results[best_url]}
                            valid_urls = [best_url]
                            
                            # Update cache
                            cache.set(company_name, valid_urls, verification_data)
                    except Exception as e:
                        print(f"Error en la verificación de URLs para {company_name}: {str(e)}")
                    continue
            
            # Update DataFrame with results from best URL
                if verification_data:
                    df.at[idx, 'URL_Válida'] = True
                    df.at[idx, 'URLs_Encontradas'] = ', '.join(valid_urls)
                            
                    # Get data from the best URL
                    best_data = list(verification_data.values())[0]
                            
                    # Update score
                    df.at[idx, 'Score_URL'] = best_data.get('score', 0)
                            
                    # Update phones
                    if 'phones' in best_data:
                        phones = best_data['phones'][:3]
                        for i, phone in enumerate(phones, 1):
                            df.at[idx, f'Teléfono_{i}'] = phone
                            
                            # Update social links
                    if 'social_links' in best_data:
                        social_links = best_data['social_links']
                        df.at[idx, 'Facebook'] = social_links.get('facebook', '')
                        df.at[idx, 'Twitter'] = social_links.get('twitter', '')
                        df.at[idx, 'Instagram'] = social_links.get('instagram', '')
                        df.at[idx, 'LinkedIn'] = social_links.get('linkedin', '')
                        df.at[idx, 'YouTube'] = social_links.get('youtube', '')
                            
                    # Update ecommerce data
                    if 'has_ecommerce' in best_data:
                        df.at[idx, 'Tiene_Ecommerce'] = best_data['has_ecommerce']
                        df.at[idx, 'Ecommerce_Score'] = best_data['ecommerce_data']['score']
                        df.at[idx, 'Ecommerce_Evidencia'] = '; '.join(best_data['ecommerce_data']['evidence'])
                    
                    # Update Excel data flags
                    df.at[idx, 'Provincia_En_Web'] = 'provincia_found' in best_data
                    df.at[idx, 'CP_En_Web'] = 'codigo_postal_found' in best_data
                    df.at[idx, 'NIF_En_Web'] = 'nif_found' in best_data
                
            
                    
        except KeyboardInterrupt:
            print("\nGuardando progreso antes de salir...")
            output_file = os.path.splitext(file_path)[0] + '_procesado_interrumpido.xlsx'
            df.to_excel(output_file, index=False, engine='openpyxl')
            print(f"Progreso guardado en {output_file}")
            # Forzar la salida después de guardar
            os._exit(0)
        except Exception as e:
            print(f"Error procesando empresa {idx + 1}: {str(e)}")
            continue
    
    output_file = os.path.splitext(file_path)[0] + '_procesado_21febb.xlsx'
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