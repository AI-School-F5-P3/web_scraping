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
        ],
        'precios': [
            '€', 'eur', 'euros', 'precio', 'price', 'pvp'
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

def verify_company_url(url, company_name, provincia, codigo_postal, session):
    """Verificación mejorada de URLs de empresa con énfasis en ubicación."""
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
            'all_text': soup.get_text().lower()  # Añadimos todo el texto para buscar ubicación
        }
        
        # Detectar ecommerce
        is_ecommerce, ecommerce_data = detect_ecommerce(soup)
        data['has_ecommerce'] = is_ecommerce
        data['ecommerce_data'] = ecommerce_data
        
        # Mejorada la extracción de teléfonos
        phones = set()  # Usamos set para evitar duplicados
        
        def is_valid_spanish_phone(phone):
            """Valida que un número sea un teléfono español válido."""
            # Limpiamos el número de cualquier carácter no numérico
            clean_phone = re.sub(r'[^\d+]', '', phone)
            
            # Si empieza con +34 o 0034, lo removemos para validar solo los 9 dígitos
            if clean_phone.startswith('+34'):
                clean_phone = clean_phone[3:]
            elif clean_phone.startswith('0034'):
                clean_phone = clean_phone[4:]
            elif clean_phone.startswith('34'):
                clean_phone = clean_phone[2:]
                
            # Validar que sea un número español válido:
            # - Debe tener exactamente 9 dígitos
            # - Debe empezar por 6, 7, 8 o 9
            return (len(clean_phone) == 9 and 
                   clean_phone[0] in '6789')
        
        def clean_and_format_phone(phone):
            """Limpia y formatea un número de teléfono."""
            clean_phone = re.sub(r'[^\d+]', '', phone)
            if not clean_phone:
                return None
                
            # Manejar diferentes formatos de prefijo
            if clean_phone.startswith('+34'):
                clean_phone = clean_phone[3:]
            elif clean_phone.startswith('0034'):
                clean_phone = clean_phone[4:]
            elif clean_phone.startswith('34'):
                clean_phone = clean_phone[2:]
                
            # Si el número tiene 9 dígitos y es válido, añadir prefijo
            if len(clean_phone) == 9 and clean_phone[0] in '6789':
                return f"+34{clean_phone}"
            return None
        
        # 1. Buscar enlaces tipo tel:
        tel_links = soup.find_all('a', href=re.compile(r'^tel:'))
        for link in tel_links:
            href = link.get('href', '')
            formatted_phone = clean_and_format_phone(href.replace('tel:', ''))
            if formatted_phone:
                phones.add(formatted_phone)
        
        # 2. Buscar en el texto con patrones mejorados
        # Varios patrones para diferentes formatos
        phone_patterns = [
            # Patrón para móviles y fijos con prefijo
            r'(?:\+34|0034|34)?[\s-]?[6789]\d{2}[\s-]?(?:\d{2}[\s-]?){3}',
            # Patrón específico para fijos con formato XXX XX XX XX
            r'[9]\d{2}[\s-]?\d{2}[\s-]?\d{2}[\s-]?\d{2}',
            # Patrón para números escritos con palabras "Teléfono" o "Tel" cerca
            r'(?:(?:Tel[eé]fono|Tel)[\s:.-]+)?(?:\+34|0034|34)?[\s-]?[6789]\d{2}[\s-]?(?:\d{2}[\s-]?){3}'
        ]
        
        # Buscar teléfonos en elementos de texto
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
        
        # 3. Buscar en todo el texto de la página para casos especiales
        full_text = soup.get_text()
        # Buscar específicamente después de la palabra "Teléfono" o "Tel"
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
        
        # Calcular puntuación con nuevos criterios
        score = 0
        text_to_search = f"{data['title']} {data['meta_description']} {data['h1']} {data['all_text']}"
        
        # 1. Búsqueda de provincia (40 puntos máximo)
        provincia = provincia.lower() if isinstance(provincia, str) else ''
        if provincia and provincia in text_to_search:
            score += 40
            data['found_provincia'] = True
        else:
            data['found_provincia'] = False
            
        # 2. Búsqueda de código postal (20 puntos máximo)
        # Asegurarnos de que el código postal tenga 5 dígitos
        if isinstance(codigo_postal, (int, float)):
            codigo_postal = str(int(codigo_postal)).zfill(5)
        elif isinstance(codigo_postal, str):
            codigo_postal = codigo_postal.zfill(5)
        
        if codigo_postal and codigo_postal in text_to_search:
            score += 20
            data['found_codigo_postal'] = True
        else:
            data['found_codigo_postal'] = False
        
        # 3. Búsqueda del nombre de la empresa (20 puntos máximo)
        search_terms = clean_company_name(company_name).split('-')
        search_terms = [term for term in search_terms if len(term) > 2]
        if search_terms:
            matches = sum(1 for term in search_terms if term in text_to_search)
            score += (matches / len(search_terms)) * 20
        
        # 4. Elementos adicionales (20 puntos máximo)
        if data['contact_page']:
            score += 8
        if data['phones']:
            score += 7
        score += min(5, len([x for x in social_links.values() if x])) # Máximo 5 puntos por redes sociales
        
        data['score'] = score
        data['score_breakdown'] = {
            'provincia_score': 40 if data['found_provincia'] else 0,
            'codigo_postal_score': 20 if data['found_codigo_postal'] else 0,
            'company_name_score': (matches / len(search_terms)) * 20 if search_terms else 0,
            'additional_elements_score': score - ((40 if data['found_provincia'] else 0) + 
                                               (20 if data['found_codigo_postal'] else 0) + 
                                               ((matches / len(search_terms)) * 20 if search_terms else 0))
        }
        
        # Se considera válida si alcanza al menos 60 puntos
        return score >= 30, data
        
    except Exception as e:
        print(f"Error en verify_company_url: {str(e)}")
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
            
            # Verificar caché
            cached_urls, cached_data = cache.get(company_name)
            if cached_urls:
                print("Usando datos en caché")
                valid_urls = cached_urls
                verification_data = cached_data
            else:
                # Generar y verificar URLs
                possible_urls = generate_possible_urls(company_name)
                if row[url_column] and pd.notna(row[url_column]):
                    possible_urls.append(row[url_column])
                
                print(f"URLs a verificar: {possible_urls}")
                verification_data = verify_urls_parallel(possible_urls, company_name, provincia, codigo_postal)
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
                
                # Variables para ecommerce
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
                
                # Asignar información de ecommerce
                df.at[idx, 'Tiene_Ecommerce'] = has_ecommerce
                df.at[idx, 'Ecommerce_Score'] = max_ecommerce_score
                df.at[idx, 'Ecommerce_Evidencia'] = '; '.join(all_evidence)
                
                # Asignar scores
                df.at[idx, 'Score_Total'] = best_scores['total']
                df.at[idx, 'Score_Provincia'] = best_scores['provincia']
                df.at[idx, 'Score_CodigoPostal'] = best_scores['codigo_postal']
                df.at[idx, 'Score_NombreEmpresa'] = best_scores['nombre_empresa']
                df.at[idx, 'Score_ElementosAdicionales'] = best_scores['elementos_adicionales']
                
                # Intentar obtener información WHOIS
                try:
                    whois_info = get_whois_info(valid_urls[0].split('/')[2])
                    if whois_info:
                        df.at[idx, 'Info_Adicional'] = json.dumps(whois_info, default=str)
                except:
                    pass
                    
        except KeyboardInterrupt:
            print("\nGuardando progreso antes de salir...")
            output_file = os.path.splitext(file_path)[0] + '_procesado_ubicacion.xlsx'
            df.to_excel(output_file, index=False, engine='openpyxl')
            sys.exit(0)
        except Exception as e:
            print(f"Error procesando empresa {idx + 1}: {str(e)}")
            continue
    
    output_file = os.path.splitext(file_path)[0] + '_procesado_ubicacion.xlsx'
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
