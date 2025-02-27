import concurrent
import traceback
from urllib.parse import urlparse
import requests
import re
from urllib3.exceptions import InsecureRequestWarning
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import unicodedata
from bs4 import BeautifulSoup
from datetime import datetime
import dns.resolver
from functools import wraps
import time
import socket
import json
import psycopg2
import logging
from typing import List, Dict, Any, Tuple, Set
from concurrent.futures import ThreadPoolExecutor
from config import DB_CONFIG, TIMEOUT_CONFIG

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Disable SSL warnings
urllib3.disable_warnings(InsecureRequestWarning)

class RateLimiter:
    def __init__(self, calls_per_minute=30):
        self.calls_per_minute = calls_per_minute
        self.calls = []
    
    def __call__(self, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            now = time.time()
            self.calls = [call for call in self.calls if call > now - 60]
            
            if len(self.calls) >= self.calls_per_minute:
                sleep_time = self.calls[0] - (now - 60)
                if sleep_time > 0:
                    time.sleep(sleep_time)
            
            self.calls.append(now)
            return func(*args, **kwargs)
        return wrapper

class WebScrapingService:
    def __init__(self, db_params: dict):
        """
        Inicializa el servicio de web scraping
        :param db_params: Parámetros de conexión a PostgreSQL
        """
        self.db_params = db_params
        try:
            # Conectar directamente a PostgreSQL
            self.connection = psycopg2.connect(**db_params)
            self.connection.autocommit = True
            logger.info("Conexión a la base de datos establecida correctamente")
        except Exception as e:
            logger.error(f"Error conectando a la base de datos: {str(e)}")
            self.connection = None
    
    def execute_query(self, query: str, params: tuple = None, return_df=False):
        """
        Ejecuta una consulta SQL y opcionalmente retorna los resultados como DataFrame
        """
        import pandas as pd
        try:
            if self.connection is None or self.connection.closed:
                self.connection = psycopg2.connect(**self.db_params)
                self.connection.autocommit = True
                
            with self.connection.cursor() as cursor:
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                    
                if cursor.description:  # Si la consulta retorna resultados
                    columns = [desc[0] for desc in cursor.description]
                    results = cursor.fetchall()
                    
                    if return_df:
                        df = pd.DataFrame(results, columns=columns)
                        return df
                    return results
                return None
        except Exception as e:
            logger.error(f"Error ejecutando consulta: {str(e)}")
            if self.connection and not self.connection.closed:
                self.connection.rollback()
            return None

    def get_companies_to_process(self, limit: int = 100) -> List[Dict]:
        try:
            print("\n=== Obteniendo empresas para procesar ===")
            query = """
                SELECT cod_infotel, nif, razon_social, domicilio, 
                    cod_postal, nom_poblacion, nom_provincia, url
                FROM sociedades 
                WHERE processed = FALSE OR processed IS NULL
                LIMIT %s
            """
            
            print(f"Ejecutando query con límite: {limit}")
            results = self.execute_query(query, params=(limit,), return_df=True)
            
            if results is not None and not results.empty:
                companies = results.to_dict('records')
                print(f"\nEmpresas encontradas: {len(companies)}")
                print("Primeras 5 empresas:")
                for company in companies[:5]:
                    url_display = company['url'] if company.get('url') else "Sin URL"
                    print(f"- {company['razon_social']}: {url_display}")
                return companies
            else:
                print("\n❌ No se encontraron empresas para procesar")
                print("Posibles razones:")
                print("1. Todas las empresas ya están procesadas (processed = TRUE)")
                print("2. La tabla está vacía")
                return []
                
        except Exception as e:
            print(f"\n❌ Error obteniendo empresas: {str(e)}")
            logger.error(f"Error obteniendo empresas: {e}")
            return []
                
        

    def process_company(self, company: Dict) -> Tuple[bool, Dict]:
        """
        Procesa una empresa individual siguiendo el flujo definido
        """
        print(f"\nProcesando empresa: {company['razon_social']}")

        try:
            # Verificar si la empresa tiene URL
            url = company.get('url')
            
            # Variable para almacenar si se encontró una URL válida
            url_encontrada = False
            
            # Si tiene URL, verificarla primero
            if url and url.strip():
                print(f"Verificando URL original: {url}")
                is_valid, data = self.verify_company_url(url, company)
                
                # Si la URL original es válida, devolver los datos
                if is_valid:
                    print(f"✅ URL original válida: {url}")
                    return True, data
                
                print("❌ URL original no válida.")
            else:
                print("ℹ️ La empresa no tiene URL. Generando alternativas...")
            
            # Generar URLs alternativas
            print("Generando URLs alternativas...")
            alternative_urls = self.generate_possible_urls(company['razon_social'], company.get('nom_provincia'))
            
            if alternative_urls:
                print(f"Se generaron {len(alternative_urls)} URLs alternativas")
                
                # Verificar URLs alternativas
                print("Verificando URLs alternativas...")
                url_results = self.verify_urls_parallel(alternative_urls, company)
                
                if url_results:
                    # Encontrar la mejor URL
                    best_url, best_data = self.choose_best_url(url_results)
                    print(f"✅ Mejor URL alternativa encontrada: {best_url}")
                    return True, best_data
                else:
                    print("❌ No se encontraron URLs alternativas válidas")
            else:
                print("❌ No se pudieron generar URLs alternativas")
            
            # Si llegamos aquí, no se encontró ninguna URL válida
            return False, {
                'cod_infotel': company['cod_infotel'],
                'url_exists': False,
                'url_status': -1,
                'url_status_mensaje': "No se encontró URL válida para esta empresa"
            }

        except Exception as e:
            print(f"\n❌ ERROR en process_company: {str(e)}")
            traceback.print_exc()
            return False, {
                'cod_infotel': company['cod_infotel'],
                'url_exists': False,
                'url_status': -1,
                'url_status_mensaje': str(e)
            }

    @staticmethod
    def clean_company_name(company_name: str) -> str:
        """Limpia y formatea el nombre de la empresa"""
        if not isinstance(company_name, str):
            return ""
        
        name = ''.join(c for c in unicodedata.normalize('NFKD', company_name)
                      if not unicodedata.combining(c))
        name = name.lower().strip()
        name = re.sub(r'[^\w\s-]', '', name)
        name = name.replace(' ', '-')
        
        patterns = [
            r'(-sa|-s\.a\.|sa|sociedad-anonima|sociedad-anonyma)$',
            r'(-sl|-s\.l\.|sl|sociedad-limitada)$'
        ]
        
        for pattern in patterns:
            name = re.sub(pattern, '', name, flags=re.IGNORECASE)
        
        return name.rstrip('-')

    def generate_possible_urls(self, company_name: str, provincia: str = None) -> Set[str]:
        """Genera posibles URLs basadas en el nombre de la empresa"""
        valid_domains = set()
        clean_name = self.clean_company_name(company_name)
        
        if not clean_name:
            return valid_domains
            
        words = clean_name.split('-')
        
        # Determinar dominios basados en provincia
        domains = ['.es', '.com', 'net', 'org']
        if provincia:
            provincia_norm = unicodedata.normalize('NFKD', str(provincia)).encode('ASCII', 'ignore').decode()
            if provincia_norm.upper() in ['BARCELONA', 'TARRAGONA', 'LERIDA', 'GIRONA', 'GERONA', 'LLEIDA']:
                domains.append('.cat')
            elif provincia_norm.upper() in ['LA CORUNA', 'LUGO', 'ORENSE', 'PONTEVEDRA', 'A CORUÑA', 'OURENSE']:
                domains.append('.gal')
            elif provincia_norm.upper() in ['ALAVA', 'VIZCAYA', 'GUIPUZCOA', 'ARABA', 'BIZKAIA', 'GIPUZKOA']:
                domains.append('.eus')

        # Generar combinaciones de nombres
        name_combinations = []
        
        # Nombre completo
        name_combinations.append(clean_name)
        
        # Primeras palabras si hay más de una
        if len(words) > 1:
            # Primera palabra
            name_combinations.append(words[0])
            
            # Dos primeras palabras
            if len(words) > 2:
                name_combinations.append('-'.join(words[:2]))
                
            # Tres primeras palabras
            if len(words) > 3:
                name_combinations.append('-'.join(words[:3]))
        
        # Generar las URLs combinando nombres y dominios
        for name in name_combinations:
            for domain in domains:
                for prefix in ['www.', '']:
                    url = f"https://{prefix}{name}{domain}"
                    if self.verify_domain(url):
                        valid_domains.add(url)
                        print(f"URL válida generada: {url}")
        
        return valid_domains

    @staticmethod
    def verify_domain(url: str) -> bool:
        """Verifica si un dominio existe"""
        try:
            domain = url.replace('https://', '').replace('http://', '')
            if domain.startswith('www.'):
                base_domain = domain[4:]
            else:
                base_domain = domain
                
            # Si no hay un punto en el dominio, no es un dominio válido
            if '.' not in base_domain:
                return False
                
            # Extraer solo el nombre de dominio sin la ruta
            base_domain = base_domain.split('/')[0]

            try:
                dns.resolver.resolve(base_domain, 'A')
                return True
            except:
                try:
                    socket.gethostbyname(domain)
                    return True
                except:
                    return False
        except Exception as e:
            print(f"Error verificando dominio {url}: {str(e)}")
            return False

    def verify_urls_parallel(self, urls: Set[str], company: Dict) -> Dict[str, Dict]:
        """
        Verifica múltiples URLs en paralelo y devuelve los resultados con puntuación
        """
        results = {}
        with ThreadPoolExecutor(max_workers=4) as executor:
            future_to_url = {
                executor.submit(self.verify_and_score_url, url, company): url 
                for url in urls
            }
            for future in concurrent.futures.as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    is_valid, data, score = future.result()
                    if is_valid:
                        # Guardar los datos junto con la puntuación
                        data['score'] = score
                        results[url] = data
                        print(f"URL válida: {url} (Puntuación: {score})")
                except Exception as e:
                    logger.error(f"Error verificando URL {url}: {e}")

        return results
    
    def verify_and_score_url(self, url: str, company: Dict) -> Tuple[bool, Dict, int]:
        """
        Verifica una URL y le asigna una puntuación
        """
        try:
            is_valid, data = self.verify_company_url(url, company)
            
            if is_valid:
                # Obtener contenido para puntuar
                session = requests.Session()
                content = self.get_page_content(url, session)
                
                if content:
                    soup = BeautifulSoup(content, 'html.parser')
                    score = self.score_website(url, soup, company)
                    data['score'] = score
                    return True, data, score
                    
            return False, {}, 0
                
        except Exception as e:
            print(f"Error en verify_and_score_url para {url}: {e}")
            return False, {}, 0
    
    def choose_best_url(self, url_results: Dict[str, Dict]) -> Tuple[str, Dict]:
        """
        Elige la mejor URL basada en puntuación
        """
        if not url_results:
            return None, {}
            
        # Encontrar la URL con la puntuación más alta
        best_url = None
        best_score = -1
        best_data = {}
        
        for url, data in url_results.items():
            score = data.get('score', 0)
            print(f"URL: {url} - Puntuación: {score}")
            
            if score > best_score:
                best_score = score
                best_url = url
                best_data = data
        
        print(f"Mejor URL seleccionada: {best_url} con puntuación {best_score}")
        return best_url, best_data

    def score_website(self, url: str, soup: BeautifulSoup, company: Dict) -> int:
        """
        Asigna una puntuación a un sitio web basado en su relevancia para la empresa
        """
        score = 0
        
        # Obtener el texto completo y limpiarlo
        full_text = soup.get_text().lower()
        
        # 1. Verificar si el nombre de la empresa aparece en el sitio
        if company.get('razon_social'):
            company_name = company['razon_social'].lower()
            clean_name = self.clean_company_name(company_name)
            words = clean_name.split('-')
            
            # Si el nombre completo aparece exactamente, alta puntuación
            if company_name in full_text:
                score += 10
            
            # Si aparecen partes significativas del nombre
            for word in words:
                if len(word) > 3 and word in full_text:
                    score += 2
        
        # 2. Verificar si la provincia aparece
        if company.get('nom_provincia'):
            provincia = company['nom_provincia'].lower()
            if provincia in full_text:
                score += 5
        
        # 3. Verificar si el código postal aparece
        if company.get('cod_postal'):
            cp = str(company['cod_postal']).strip()
            if cp in full_text:
                score += 7
        
        # 4. Verificar si el NIF/CIF aparece
        if company.get('nif'):
            nif = company['nif'].lower()
            if nif in full_text:
                score += 100  # Alta puntuación, muy específico
        
        # 5. Verificar si la dirección aparece
        if company.get('domicilio'):
            direccion = company['domicilio'].lower()
            if direccion in full_text:
                score += 10
            else:
                # Buscar partes de la dirección (número, calle, etc.)
                parts = direccion.split()
                for part in parts:
                    if len(part) > 3 and part in full_text:
                        score += 2
        
        # 6. Verificar si la población aparece
        if company.get('nom_poblacion'):
            poblacion = company['nom_poblacion'].lower()
            if poblacion in full_text:
                score += 5
        
        # 7. Verificar elementos típicos de un sitio corporativo
        corporate_terms = [
            'contacto', 'contact', 'quienes somos', 'about us', 'sobre nosotros',
            'política de privacidad', 'privacy policy', 'aviso legal', 'legal notice',
            'nuestros servicios', 'our services', 'productos', 'products'
        ]
        
        for term in corporate_terms:
            if term in full_text:
                score += 1
        
        # 8. Verificar si tiene secciones típicas en los menús
        for nav in soup.find_all(['nav', 'header']):
            nav_text = nav.get_text().lower()
            for term in corporate_terms:
                if term in nav_text:
                    score += 2  # Mayor peso en la navegación
        
        # 9. Verificar si tiene formulario de contacto
        contact_forms = soup.find_all('form')
        for form in contact_forms:
            if 'contact' in str(form) or 'contacto' in str(form):
                score += 3
        
        # 10. Verificar si tiene teléfonos
        phones = self.extract_phones(soup)
        if phones:
            score += len(phones) * 2
        
        # 11. Verificar si tiene redes sociales
        social_links = self.extract_social_links(soup)
        social_count = sum(1 for value in social_links.values() if value)
        score += social_count * 2
        
        # 12. Penalizar sitios que parecen directorios generales
        directory_terms = [
            'directorio de empresas', 'business directory',
            'listado de empresas', 'company listing',
            'todas las empresas', 'all companies'
        ]
        
        for term in directory_terms:
            if term in full_text:
                score -= 10
        # 13. Penalizar si es un dominio que está en venta
        domain_in_sale_terms = [
    
    "dominio en venta", "comprar este dominio", "este dominio está en venta",  
    "venta de dominio", "adquiere este dominio", "domain for sale", "buy this domain", "this domain is for sale",  
    "domain available", "this domain is available", "domain auction", "bid on this domain",  
    "purchase this domain"
]
        
        for term in domain_in_sale_terms:
            if term in full_text:
                score -= 100
        
        print(f"Puntuación para {url}: {score}")
        return score
    
    def verify_company_url(self, url: str, company: Dict) -> Tuple[bool, Dict]:
        """
        Verifica una URL específica y extrae información.
        Returns:
            Tuple[bool, Dict]: (éxito, datos extraídos)
        """
        print(f"\n{'='*50}")
        print(f"🚀 Iniciando verify_company_url para: {company['razon_social']}")
        print(f"🌍 URL original: {url}")

        session = requests.Session()

        try:
            # Estructura inicial de datos
            data = {
                'cod_infotel': company['cod_infotel'],
                'url_exists': False,
                'url_valida': None,
                'url_limpia': urlparse(url).netloc if url else None,
                'url_status': None,
                'url_status_mensaje': None,
                'phones': [],
                'social_media': {
                    'facebook': None,
                    'twitter': None,
                    'linkedin': None,
                    'instagram': None,
                    'youtube': None
                },
                'is_ecommerce': False
            }

            # Asegurar que la URL tenga protocolo
            if not url.startswith(('http://', 'https://')):
                url = 'https://' + url
            print(f"🔗 URL normalizada: {url}")

            # Verificar si la URL existe (DNS lookup)
            domain = urlparse(url).netloc
            base_domain = domain[4:] if domain.startswith('www.') else domain

            print(f"🔍 Verificando dominio: {base_domain}")
            try:
                dns.resolver.resolve(base_domain, 'A')
                print("✅ Dominio válido (DNS)")
                domain_exists = True
            except:
                try:
                    dns.resolver.resolve('www.' + base_domain, 'A')
                    print("✅ Dominio válido (DNS con www)")
                    domain_exists = True
                except:
                    print("❌ Dominio no válido")
                    domain_exists = False

            if not domain_exists:
                data.update({
                    'url_status': -1,
                    'url_status_mensaje': "Dominio no válido"
                })
                return False, data

            # Intentar obtener el contenido de la página
            print("📡 Intentando obtener contenido de la página...")
            content = self.get_page_content(url, session)

            if not content:
                print("❌ No se pudo obtener contenido")
                data.update({
                    'url_status': -1,
                    'url_status_mensaje': "No se pudo acceder a la URL"
                })
                return False, data

            print("✅ Contenido obtenido correctamente. URL válida!")

            # Procesar contenido HTML con BeautifulSoup
            soup = BeautifulSoup(content, 'html.parser')

            # Extraer información básica
            data.update({
                'url_exists': True,
                'url_valida': url,
                'url_status': 200,
                'url_status_mensaje': "URL válida y accesible"
            })

            # Extraer teléfonos
            phones = self.extract_phones(soup)
            print(f"📞 Teléfonos extraídos: {phones}")
            data['phones'] = phones

            # Extraer redes sociales
            social_links = self.extract_social_links(soup)
            print(f"📲 Redes sociales extraídas: {json.dumps(social_links, indent=2)}")
            data['social_media'].update(social_links)

            # Detectar e-commerce
            is_ecommerce, ecommerce_data = self.detect_ecommerce(soup)
            data['is_ecommerce'] = is_ecommerce  # Solo el booleano
            data['ecommerce_data'] = ecommerce_data  # Guarda detalles adicionales si los necesitas
            print(f"🛒 E-commerce detectado: {is_ecommerce}")

            return True, data

        except Exception as e:
            print(f"❌ ERROR en verify_company_url: {str(e)}")
            traceback.print_exc()
            data.update({
                'url_status': -1,
                'url_status_mensaje': str(e)
            })
            return False, data

        finally:
            session.close()

    @RateLimiter(calls_per_minute=30)
    def get_page_content(self, url: str, session: requests.Session) -> str:
        """Obtiene el contenido de una página web con rate limiting"""
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        try:
            print(f"Intentando acceder a {url}...")
            response = session.get(
                url, 
                timeout=(10, 20),
                verify=False,
                headers=headers
            )
            response.raise_for_status()
            print(f"Acceso exitoso a {url}")
            return response.text
        except Exception as e:
            print(f"Error accediendo a {url}: {str(e)}")
            return None
        
    def extract_phones(self, soup: BeautifulSoup) -> List[str]:
        """
        Extrae teléfonos de una página web usando BeautifulSoup
        """
        phones = set()  # Usamos set para evitar duplicados

        try:
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
                    if isinstance(attr_value, str):
                        found_phones = re.findall(phone_pattern, attr_value)
                        for phone in found_phones:
                            clean_phone = re.sub(r'[^\d]', '', phone)
                            if len(clean_phone) == 9:
                                phones.add(f"+34{clean_phone}")
                            elif len(clean_phone) > 9:
                                phones.add(f"+{clean_phone}")

            # Convertir el set a lista y limitar a 3 teléfonos
            return list(phones)[:3]

        except Exception as e:
            logger.error(f"Error extrayendo teléfonos: {e}")
            return []

    def extract_social_links(self, soup: BeautifulSoup) -> Dict[str, str]:
        """
        Extrae enlaces a redes sociales de una página web
        """
        try:
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

            return social_links

        except Exception as e:
            logger.error(f"Error extrayendo enlaces sociales: {e}")
            return {
                'facebook': '',
                'twitter': '',
                'instagram': '',
                'linkedin': '',
                'youtube': ''
            }
            
    def detect_ecommerce(self, soup: BeautifulSoup) -> Tuple[bool, Dict]:
        """Detecta si una web tiene comercio electrónico"""
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
            score += 0.5
            evidence.append(f"Precios encontrados: {len(prices)} ocurrencias")
        
        is_ecommerce = score >= 5
        return is_ecommerce, {
            'score': score,
            'evidence': evidence
        }

    def update_company_data(self, company_id: int, data: Dict) -> Dict[str, Any]:
        """Actualiza los datos de la empresa en la base de datos"""
        try:
            print(f"\nActualizando datos para empresa {company_id}")
            
            # Crear query de actualización
            update_query = """
            UPDATE sociedades 
            SET 
                url_exists = %s,
                url_valida = %s,
                url_limpia = %s,
                url_status = %s,
                url_status_mensaje = %s,
                telefono_1 = %s,
                telefono_2 = %s,
                telefono_3 = %s,
                facebook = %s,
                twitter = %s,
                linkedin = %s,
                instagram = %s,
                youtube = %s,
                e_commerce = %s,
                processed = TRUE,
                fecha_actualizacion = NOW()
            WHERE cod_infotel = %s
            """
            
            # Preparar los parámetros
            phones = data.get('phones', [])
            phones = phones + ['', '', '']  # Asegurar que hay al menos 3 elementos
            
            social_media = data.get('social_media', {})
            
            params = (
                data.get('url_exists', False),
                data.get('url_valida', ''),
                data.get('url_limpia', ''),
                data.get('url_status', -1),
                data.get('url_status_mensaje', ''),
                phones[0],
                phones[1],
                phones[2],
                social_media.get('facebook', ''),
                social_media.get('twitter', ''),
                social_media.get('linkedin', ''),
                social_media.get('instagram', ''),
                social_media.get('youtube', ''),
                data.get('is_ecommerce', False),
                company_id
            )
            
            # Ejecutar query
            with self.connection.cursor() as cursor:
                cursor.execute(update_query, params)
                
                if cursor.rowcount > 0:
                    self.connection.commit()
                    print(f"✅ Empresa {company_id} actualizada exitosamente")
                    return {
                        "status": "success",
                        "message": f"Empresa {company_id} actualizada exitosamente"
                    }
                else:
                    print(f"⚠️ No se actualizó la empresa {company_id}. Posible error de ID.")
                    return {
                        "status": "error",
                        "message": f"No se encontró la empresa con ID {company_id}"
                    }
                    
        except Exception as e:
            print(f"❌ Error actualizando empresa {company_id}: {str(e)}")
            traceback.print_exc()
            
            if self.connection and not self.connection.closed:
                self.connection.rollback()
                
            return {
                "status": "error",
                "message": str(e)
            }

    def process_batch(self, limit: int = 100) -> Dict[str, Any]:
        """Procesa un lote de empresas siguiendo el flujo completo"""
        companies = self.get_companies_to_process(limit)
        
        results = {
            'total': len(companies),
            'processed': 0,
            'successful': 0,
            'failed': 0,
            'details': []
        }
        
        for company in companies:
            try:
                print(f"\nProcesando empresa: {company['razon_social']} (ID: {company['cod_infotel']})")
                
                # 1. Verificar la URL original
                success, data = self.process_company(company)
                
                if success:
                    # Actualizar en base de datos
                    update_result = self.update_company_data(company['cod_infotel'], data)
                    
                    if update_result.get('status') == 'success':
                        results['successful'] += 1
                        detail = {
                            'cod_infotel': company['cod_infotel'],
                            'razon_social': company['razon_social'],
                            'success': True,
                            'url': data.get('url_valida', None),
                            'phones': len(data.get('phones', [])),
                            'social_networks': sum(1 for v in data.get('social_media', {}).values() if v),
                            'is_ecommerce': data.get('is_ecommerce', False)
                        }
                    else:
                        results['failed'] += 1
                        detail = {
                            'cod_infotel': company['cod_infotel'],
                            'razon_social': company['razon_social'],
                            'success': False,
                            'error': update_result.get('message', 'Error al actualizar en BD')
                        }
                else:
                    # Marcar como procesado pero sin éxito
                    empty_data = {
                        'cod_infotel': company['cod_infotel'],
                        'url_exists': False,
                        'url_status': -1,
                        'url_status_mensaje': data.get('url_status_mensaje', 'URL no válida')
                    }
                    self.update_company_data(company['cod_infotel'], empty_data)
                    
                    results['failed'] += 1
                    detail = {
                        'cod_infotel': company['cod_infotel'],
                        'razon_social': company['razon_social'],
                        'success': False,
                        'error': data.get('url_status_mensaje', 'URL no válida')
                    }
                
                results['details'].append(detail)
                
            except Exception as e:
                print(f"❌ Error procesando empresa {company['cod_infotel']}: {str(e)}")
                traceback.print_exc()
                
                results['failed'] += 1
                results['details'].append({
                    'cod_infotel': company['cod_infotel'],
                    'razon_social': company['razon_social'],
                    'success': False,
                    'error': str(e)
                })
                
            finally:
                results['processed'] += 1
                # Mostrar progreso
                print(f"Progreso: {results['processed']}/{results['total']}")
                
        return results

def main():
    # Usar la configuración de la base de datos desde config.py
    from config import DB_CONFIG
    
    scraper = WebScrapingService(DB_CONFIG)
    results = scraper.process_batch(limit=10)
    print(f"Resultados del procesamiento: {json.dumps(results, indent=2)}")

if __name__ == "__main__":
    main()