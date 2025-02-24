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
from config import DB_CONFIG
from database import DatabaseManager

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
        self.db = DatabaseManager()  # Usar DatabaseManager en lugar de conexión directa

    

    def get_companies_to_process(self, limit: int = 100) -> List[Dict]:
        try:
            print("\n=== Obteniendo empresas para procesar ===")
            query = """
                SELECT cod_infotel, nif, razon_social, domicilio, 
                    cod_postal, nom_poblacion, nom_provincia, url
                FROM sociedades 
                WHERE url IS NOT NULL 
                AND url != ''
                AND processed = FALSE
                LIMIT %s
            """
            
            print(f"Ejecutando query con límite: {limit}")
            results = self.db.execute_query(query, params=(limit,), return_df=True)
            
            if results is not None and not results.empty:
                companies = results.to_dict('records')
                print(f"\nEmpresas encontradas: {len(companies)}")
                print("Primeras 5 empresas:")
                for company in companies[:5]:
                    print(f"- {company['razon_social']}: {company['url']}")
                return companies
            else:
                print("\n❌ No se encontraron empresas para procesar")
                print("Posibles razones:")
                print("1. Todas las empresas ya están procesadas (processed = TRUE)")
                print("2. No hay empresas con URL válida")
                print("3. La tabla está vacía")
                return []
                
        except Exception as e:
            print(f"\n❌ Error obteniendo empresas: {str(e)}")
            logger.error(f"Error obteniendo empresas: {e}")
            return []

    def process_company(self, company: Dict) -> Tuple[bool, Dict]:
        """
        Procesa una empresa individual siguiendo el flujo definido
        """
        print("\n>>> PUNTO DE CONTROL 2: Iniciando process_company <<<")
        print(f"Procesando empresa: {company['razon_social']}")

        try:
            url = company.get('url')
            if not url:
                print("❌ URL no proporcionada")
                return False, {
                    'cod_infotel': company['cod_infotel'],
                    'url_exists': False,
                    'url_status': -1,
                    'url_status_mensaje': "URL no proporcionada"
                }

            print("\n>>> PUNTO DE CONTROL 3: Antes de verify_company_url <<<")
            try:
                is_valid, data = self.verify_company_url(url, company)
            except Exception as e:
                print(f"❌ EXCEPCIÓN en verify_company_url: {e}")
                traceback.print_exc()
                return False, {'cod_infotel': company['cod_infotel'], 'error': str(e)}

            print("\n>>> PUNTO DE CONTROL 4.1: Después de llamar a verify_company_url <<<")
            print(f"🔄 Resultado de verify_company_url -> is_valid: {is_valid}")
            print(f"🔄 Datos recibidos en process_company: {json.dumps(data, indent=2)}")

            if is_valid:
                print("\n>>> PUNTO DE CONTROL 5: Retornando datos válidos <<<")
                return True, data
            else:
                print("\n>>> PUNTO DE CONTROL 5: Retornando datos no válidos <<<")
                return False, data

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
        words = clean_name.split('-')
        
        # Determinar dominios basados en provincia
        domains = ['.es', '.com']
        if provincia:
            provincia_norm = unicodedata.normalize('NFKD', str(provincia)).encode('ASCII', 'ignore').decode()
            if provincia_norm in ['BARCELONA', 'TARRAGONA', 'LERIDA', 'GIRONA']:
                domains.append('.cat')
            elif provincia_norm in ['LA CORUNA', 'LUGO', 'ORENSE', 'PONTEVEDRA']:
                domains.append('.gal')
            elif provincia_norm in ['ALAVA', 'VIZCAYA', 'GUIPUZCOA']:
                domains.append('.eus')

        # Generar y verificar URLs
        for i in range(len(words), 0, -1):
            combination = ''.join(words[:i])
            for domain in domains:
                for prefix in ['www.', '']:
                    url = f"https://{prefix}{combination}{domain}"
                    if self.verify_domain(url):
                        valid_domains.add(url)

        return valid_domains

    @staticmethod
    def verify_domain(url: str) -> bool:
        """Verifica si un dominio existe"""
        domain = url.replace('https://', '').replace('http://', '')
        if domain.startswith('www.'):
            base_domain = domain[4:]
        else:
            base_domain = domain

        try:
            dns.resolver.resolve(base_domain, 'A')
            return True
        except:
            try:
                socket.gethostbyname(domain)
                return True
            except:
                return False

    def verify_urls_parallel(self, urls: Set[str], company: Dict) -> Dict[str, Dict]:
        """Verifica múltiples URLs en paralelo"""
        results = {}
        with ThreadPoolExecutor(max_workers=4) as executor:
            future_to_url = {
                executor.submit(self.verify_company_url, url, company): url 
                for url in urls
            }
            for future in concurrent.futures.as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    is_valid, data = future.result()
                    if is_valid:
                        results[url] = data
                except Exception as e:
                    logger.error(f"Error verificando URL {url}: {e}")

        return results
    
    def verify_company_data(self, soup: BeautifulSoup, company: Dict) -> Dict:
        """Verifica información de la empresa en la página"""
        data = {
            'provincia_en_web': False,
            'cp_en_web': False,
            'nif_en_web': False
        }
        
        # Obtener todo el texto de la página en minúsculas
        full_text = soup.get_text().lower()
        
        # Verificar provincia
        if company.get('nom_provincia'):
            provincia_lower = company['nom_provincia'].lower()
            # Normalizar el texto (quitar acentos)
            provincia_norm = ''.join(c for c in unicodedata.normalize('NFD', provincia_lower)
                                if unicodedata.category(c) != 'Mn')
            if provincia_lower in full_text or provincia_norm in full_text:
                data['provincia_en_web'] = True
        
        # Verificar código postal
        if company.get('cod_postal'):
            cp_str = str(company['cod_postal']).strip()
            if len(cp_str) == 4:
                cp_str = '0' + cp_str  # Asegurar 5 dígitos
            
            cp_patterns = [
                rf'\b{cp_str}\b',  # Código postal exacto
                rf'CP\s*{cp_str}',  # CP seguido del código
                rf'C\.P\.\s*{cp_str}'  # C.P. seguido del código
            ]
            
            for pattern in cp_patterns:
                if re.search(pattern, full_text, re.IGNORECASE):
                    data['cp_en_web'] = True
                    break
        
        # Verificar NIF
        if company.get('nif'):
            nif_clean = company['nif'].upper().strip()
            nif_patterns = [
                rf'\b{nif_clean}\b',  # NIF exacto
                rf'NIF\s*:?\s*{nif_clean}',  # NIF: seguido del número
                rf'CIF\s*:?\s*{nif_clean}'   # CIF: seguido del número
            ]
            
            for pattern in nif_patterns:
                if re.search(pattern, full_text, re.IGNORECASE):
                    data['nif_en_web'] = True
                    break
        
        return data
    
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

            # Log final de datos antes de retornar
            print("\n📤 Retornando desde verify_company_url:")
            print(json.dumps(data, indent=2))

            print("\n>>> PUNTO DE CONTROL 1: Saliendo de verify_company_url <<<")
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
            score += 2
            evidence.append(f"Precios encontrados: {len(prices)} ocurrencias")
        
        is_ecommerce = score >= 5
        return is_ecommerce, {
            'score': score,
            'evidence': evidence
        }

    def update_company_data(self, company_id: str, data: Dict) -> Dict[str, Any]:
        """Actualiza los datos de la empresa usando DatabaseManager"""
        try:
            print(f"\nActualizando datos para empresa {company_id}")
            print(f"Datos recibidos:")
            print(json.dumps(data, indent=2))

            # Preparar los datos para la BD
            result_data = [{
                'cod_infotel': company_id,
                'url_exists': data.get('url_exists', False),
                'url_valida': data.get('url_valida', ''),
                'url_limpia': data.get('url_limpia', ''),
                'status': data.get('url_status', -1),
                'status_message': data.get('url_status_mensaje', ''),
                'phones': data.get('phones', []),
                'social_media': {
                    'facebook': data.get('social_media', {}).get('facebook', ''),
                    'twitter': data.get('social_media', {}).get('twitter', ''),
                    'linkedin': data.get('social_media', {}).get('linkedin', ''),
                    'instagram': data.get('social_media', {}).get('instagram', ''),
                    'youtube': data.get('social_media', {}).get('youtube', '')
                },
                'is_ecommerce': data.get('is_ecommerce', False)
            }]

            print("\nDatos formateados para BD:")
            print(json.dumps(result_data, indent=2))

            # Intentar la actualización
            result = self.db.update_scraping_results(result_data)
            print(f"Resultado de la actualización: {result}")

            return result  # Retornar el resultado completo

        except Exception as e:
            print(f"❌ Error en update_company_data: {str(e)}")
            logger.error(f"Error actualizando empresa {company_id}: {e}")
            return {
                "status": "error",
                "message": str(e)
            }

    def process_batch(self, limit: int = 100) -> Dict[str, Any]:
        """Procesa un lote de empresas"""
        print("\n>>> PUNTO DE CONTROL 6: Iniciando process_batch <<<")
        companies = self.get_companies_to_process(limit)
        print(f"Empresas a procesar: {len(companies)}")

        results = {
            'total': len(companies),
            'processed': 0,
            'successful': 0,
            'failed': 0
        }

        for company in companies:
            try:
                print(f"\n>>> PUNTO DE CONTROL 6.1: Llamando process_company para {company['cod_infotel']} <<<")

                success, data = self.process_company(company)

                print("\n>>> PUNTO DE CONTROL 8: Resultado de process_company <<<")
                print(f"success: {success}")
                print(f"data: {json.dumps(data, indent=2)}")

                if success:
                    print("\n>>> PUNTO DE CONTROL 9: Intentando actualizar en BD <<<")
                    update_result = self.update_company_data(company['cod_infotel'], data)
                    print(f"Resultado actualización: {update_result}")

                    if update_result.get('status') == 'success':
                        results['successful'] += 1
                        print("✅ Actualización exitosa")
                    else:
                        results['failed'] += 1
                        print(f"❌ Error en actualización: {update_result.get('message')}")
                else:
                    results['failed'] += 1
                    print("❌ Procesamiento no exitoso")

            except Exception as e:
                print(f"\n❌ Error procesando empresa: {str(e)}")
                traceback.print_exc()
                results['failed'] += 1
            finally:
                results['processed'] += 1
                print(f"\nProgreso: {results['processed']}/{results['total']}")

        print("\n>>> PUNTO DE CONTROL 10: Resumen final <<<")
        print(json.dumps(results, indent=2))
        return results
    

def main():
    # Usar la configuración de la base de datos desde config.py
    db_params = {
        'dbname': DB_CONFIG['database'],
        'user': DB_CONFIG['user'],
        'password': DB_CONFIG['password'],
        'host': DB_CONFIG['host'],
        'port': DB_CONFIG['port']
    }
    
    scraper = WebScrapingService(db_params)
    results = scraper.process_batch(limit=100)
    print(f"Resultados del procesamiento: {json.dumps(results, indent=2)}")

if __name__ == "__main__":
    main()