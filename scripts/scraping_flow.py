import concurrent.futures
from urllib.parse import urlparse
import requests
import re
from urllib3.exceptions import InsecureRequestWarning
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import unicodedata
from bs4 import BeautifulSoup
import dns.resolver
from functools import wraps
import time
import socket
import json
import logging
from typing import List, Dict, Any, Tuple, Set
from concurrent.futures import ThreadPoolExecutor
from config import DB_CONFIG
from database import DatabaseManager
import streamlit as st
import os
import queue

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
        Initialize the web scraping service
        :param db_params: PostgreSQL connection parameters
        """
        self.db = DatabaseManager()
        self.optimize_performance()
        
    def optimize_performance(self):
        """Apply performance optimizations"""
        # Use a connection pool with more efficient connections
        adapter = HTTPAdapter(
            max_retries=Retry(
                total=1,
                backoff_factor=0.1,  # Reduced from 0.5
                status_forcelist=[429, 500, 502, 503, 504],
            ),
            pool_connections=20,  # Increased pool size
            pool_maxsize=50       # Increased max size
        )
        
        # Pre-create more sessions with the adapter
        self.session_pool = queue.Queue(maxsize=os.cpu_count() * 6)  # Increased from 4
        for _ in range(min(os.cpu_count() * 6, 30)):
            session = requests.Session()
            session.mount("http://", adapter)
            session.mount("https://", adapter)
            self.session_pool.put(session)
        
        # Set global socket timeout
        socket.setdefaulttimeout(3.0)  # Reduced from 5.0

    def get_companies_to_process(self, limit: int = 100) -> List[Dict]:
        """
        Gets companies to process in order, regardless of whether they have URLs or not.
        """
        try:
            # Validate limit to ensure it's a positive number
            limit = max(1, int(limit))
            
            # Get companies that haven't been processed yet, in insertion order
            query = """
                SELECT cod_infotel, nif, razon_social, domicilio, 
                    cod_postal, nom_poblacion, nom_provincia, url
                FROM sociedades 
                WHERE processed = FALSE
                ORDER BY id  -- Order by id to get them in insertion order
                LIMIT %s
            """
            
            results = self.db.execute_query(query, params=(limit,), return_df=True)
            companies = [] if results is None or results.empty else results.to_dict('records')
            
            print(f"\nEmpresas encontradas para procesar: {len(companies)}")
            print(f"- Con URL: {sum(1 for company in companies if company.get('url'))}")
            print(f"- Sin URL: {sum(1 for company in companies if not company.get('url'))}")
            
            # Preview some companies
            for i, company in enumerate(companies[:5]):
                url_status = f": {company['url']}" if company.get('url') else ": [Sin URL]"
                print(f"- {company['razon_social']}{url_status}")
                
            return companies
            
        except Exception as e:
            logger.error(f"Error obteniendo empresas: {e}")
            print(f"Error al obtener empresas: {e}")
            return []
            
    def process_company(self, company: Dict) -> Dict:
        """
        Unified method to process a company, handling both URL verification and data extraction
        """
        # Set a start time to track total processing time
        start_time = time.time()
        
        result = {
            'cod_infotel': company['cod_infotel'],
            'url_exists': False,
            'url_limpia': None,
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
            'is_ecommerce': False,
            'processing_time': 0
        }
        
        try:
            # Get or generate URL
            url = company.get('url')
            
            # Early termination if processing takes too long
            if time.time() - start_time > 20:  # 20-second overall timeout
                result.update({
                    'url_status': -1,
                    'url_status_mensaje': "Processing timeout exceeded"
                })
                return result
            
            # For companies without URLs, try to generate possible URLs
            if not url:
                possible_urls = self.generate_possible_urls(
                    company['razon_social'], 
                    company.get('nom_provincia')
                )
                
                for possible_url in possible_urls:
                    if self.verify_domain(possible_url):
                        url = possible_url
                        result['url_limpia'] = urlparse(url).netloc
                        result['url_status_mensaje'] = "URL generada automáticamente"
                        break
            
            if not url:
                result.update({
                    'url_status': -2,
                    'url_status_mensaje': "No se pudo generar una URL válida"
                })
                return result
            
            # Normalize URL
            if not url.startswith(('http://', 'https://')):
                url = 'https://' + url
            
            result['url_limpia'] = urlparse(url).netloc
            
            # Verify domain and get content
            if not self.verify_domain(url):
                result.update({
                    'url_status': -1,
                    'url_status_mensaje': "Dominio no válido"
                })
                return result
            
            session = self.get_session()
            content = self.get_page_content(url, session)
            self.return_session(session)
            
            if not content:
                result.update({
                    'url_status': -1,
                    'url_status_mensaje': "No se pudo acceder a la URL"
                })
                return result
            
            # Process content
            soup = BeautifulSoup(content, 'html.parser')
            result.update({
                'url_exists': True,
                'url_status': 200,
                'url_status_mensaje': "URL válida y accesible",
                'phones': self.extract_phones(soup),
                'social_media': self.extract_social_links(soup),
                'is_ecommerce': self.detect_ecommerce(soup)[0]
            })
            
            return result
        
            # Track and report processing time
            result['processing_time'] = time.time() - start_time
            return result
        
        except Exception as e:
            # Handle exceptions
            result['processing_time'] = time.time() - start_time
            return result

    def create_session(self):
        """Create a session with retry strategy"""
        session = requests.Session()
        retry_strategy = Retry(
            total=1,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    @staticmethod
    def clean_company_name(company_name: str) -> str:
        """Clean and format company name"""
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
        """Generate possible URLs based on company name"""
        valid_domains = set()
        clean_name = self.clean_company_name(company_name)
        words = clean_name.split('-')
        
        # Determine domains based on province
        domains = ['.es', '.com']
        if provincia:
            provincia_norm = unicodedata.normalize('NFKD', str(provincia)).encode('ASCII', 'ignore').decode()
            if provincia_norm in ['BARCELONA', 'TARRAGONA', 'LERIDA', 'GIRONA']:
                domains.append('.cat')
            elif provincia_norm in ['LA CORUNA', 'LUGO', 'ORENSE', 'PONTEVEDRA']:
                domains.append('.gal')
            elif provincia_norm in ['ALAVA', 'VIZCAYA', 'GUIPUZCOA']:
                domains.append('.eus')

        # Generate and verify URLs
        for i in range(len(words), 0, -1):
            combination = '-'.join(words[:i])
            for domain in domains:
                for prefix in ['www.', '']:
                    url = f"https://{prefix}{combination}{domain}"
                    if self.verify_domain(url):
                        valid_domains.add(url)

        return valid_domains

    @RateLimiter(calls_per_minute=30)
    def verify_domain(self, url: str) -> bool:
        """Verifies if a domain exists with timeout"""
        try:
            if not url.startswith(('http://', 'https://')):
                url = 'https://' + url
                    
            domain = urlparse(url).netloc
            if not domain:  # Handle empty domain
                return False
                
            base_domain = domain[4:] if domain.startswith('www.') else domain
            
            # Set a shorter timeout for DNS resolution
            dns_resolver = dns.resolver.Resolver()
            dns_resolver.timeout = 1.0
            dns_resolver.lifetime = 1.5
            
            # Fast check first - if it fails quickly, move on
            try:
                socket.setdefaulttimeout(1.0)  # Reduced from 2.0
                socket.gethostbyname(domain)
                return True
            except:
                # Try DNS resolver with short timeout
                try:
                    dns_resolver.resolve(base_domain, 'A')
                    return True
                except dns.resolver.NXDOMAIN:
                    try:
                        dns_resolver.resolve('www.' + base_domain, 'A')
                        return True
                    except:
                        return False
                except Exception:
                    return False
        except Exception:
            return False

    @RateLimiter(calls_per_minute=100)
    def get_page_content(self, url: str, session: requests.Session) -> str:
        """Gets page content with optimization for speed"""
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'es-ES,es;q=0.8,en-US;q=0.5,en;q=0.3',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive'
        }
        
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        
        try:
            # Use shorter timeouts: (connect_timeout, read_timeout)
            response = session.get(url, timeout=(3, 5), verify=False, headers=headers)
            response.raise_for_status()
            return response.text
        except Exception as e:
            logger.error(f"Error accediendo a {url}: {str(e)}")
            return None
        
    def extract_phones(self, soup: BeautifulSoup) -> List[str]:
        """Extract phone numbers from HTML content"""
        phones = set()
        # Pattern for Spanish phone numbers (fixed and mobile)
        phone_patterns = [
            r'(?:\+34|0034)?[\s-]?[6789]\d{8}',  # Mobile and fixed
            r'(?:\+34|0034)?[\s-]?[89]\d{2}[\s-]?\d{3}[\s-]?\d{3}',  # Format with spaces
        ]
        
        # Search in all page text
        text_content = soup.get_text()
        
        for pattern in phone_patterns:
            found_phones = re.findall(pattern, text_content)
            for phone in found_phones:
                # Clean number
                clean_phone = re.sub(r'[\s-]', '', phone)
                if not clean_phone.startswith('+34') and not clean_phone.startswith('0034'):
                    clean_phone = '+34' + clean_phone
                phones.add(clean_phone)
        
        return list(phones)[:3]  # Return maximum 3 phones

    @RateLimiter(calls_per_minute=20)
    def extract_social_links(self, soup: BeautifulSoup) -> Dict[str, str]:
        """Extract social media links"""
        social_links = {
            'facebook': None,
            'twitter': None,
            'linkedin': None,
            'instagram': None,
            'youtube': None
        }
        
        # Patterns for each social network
        social_domains = {
            'facebook.com': 'facebook',
            'twitter.com': 'twitter', 
            'linkedin.com': 'linkedin',
            'instagram.com': 'instagram',
            'youtube.com': 'youtube',
            'youtu.be': 'youtube'
        }
        
        # Only check first 100 links for social media
        link_count = 0
        for link in soup.find_all('a', href=True):
            if link_count > 100:  # Limit checking to first 100 links
                break
                
            href = link['href'].lower()
            for domain, network in social_domains.items():
                if domain in href and social_links[network] is None:
                    social_links[network] = 'https://' + href.split('//', 1)[-1]
                    
            link_count += 1
        
        return social_links

    def detect_ecommerce(self, soup: BeautifulSoup) -> Tuple[bool, Dict]:
        """Detect if a website has e-commerce capabilities"""
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
        
        # Search links
        for link in soup.find_all('a', string=True):
            text = link.get_text().lower()
            href = link.get('href', '').lower()
            
            for category, indicators in ecommerce_indicators.items():
                for indicator in indicators:
                    if indicator in text or indicator in href:
                        score += 1
                        evidence.append(f"Enlace encontrado: {text if text else href}")
                        break
        
        # Search for purchase forms
        forms = soup.find_all('form')
        for form in forms:
            action = form.get('action', '').lower()
            if any(term in action for term in ['cart', 'checkout', 'payment', 'compra', 'pago']):
                score += 2
                evidence.append(f"Formulario de compra encontrado: {action}")
        
        # Search for elements with typical ecommerce classes/IDs
        ecommerce_classes = ['cart', 'checkout', 'basket', 'shop', 'store', 'product', 'price']
        for class_name in ecommerce_classes:
            elements = soup.find_all(class_=re.compile(class_name))
            if elements:
                score += 1
                evidence.append(f"Elementos con clase '{class_name}' encontrados")
        
        # Search for currency symbols and prices
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

    def save_results(self, results: Dict) -> bool:
        """Save scraping results to database with better error handling"""
        try:
            # Ensure we have a valid cod_infotel
            if not results.get('cod_infotel'):
                logger.error("Error saving results: Missing cod_infotel")
                return False
            
            # Ensure social_media has the correct structure
            social_media = results.get('social_media', {})
            if not isinstance(social_media, dict):
                social_media = {
                    'facebook': None,
                    'twitter': None,
                    'linkedin': None,
                    'instagram': None,
                    'youtube': None
                }
            
            # Extract phones (max 3)
            phones = results.get('phones', [])
            phone1 = phones[0] if len(phones) > 0 else None
            phone2 = phones[1] if len(phones) > 1 else None
            phone3 = phones[2] if len(phones) > 2 else None
            
            # Format result for database update
            update_query = """
                UPDATE sociedades 
                SET processed = TRUE,
                    last_processed = NOW(),
                    url_limpia = %s,
                    url_exists = %s,
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
                    e_commerce = %s
                WHERE cod_infotel = %s
            """
            
            params = (
                results.get('url_limpia'),
                bool(results.get('url_exists', False)),
                results.get('url_status'),
                results.get('url_status_mensaje'),
                phone1,
                phone2,
                phone3,
                social_media.get('facebook'),
                social_media.get('twitter'),
                social_media.get('linkedin'),
                social_media.get('instagram'),
                social_media.get('youtube'),
                bool(results.get('is_ecommerce', False)),
                results['cod_infotel']
            )
            
            self.db.execute_query(update_query, params=params)
            return True
                
        except Exception as e:
            logger.error(f"Error saving results for {results.get('cod_infotel', 'unknown')}: {str(e)}")
            return False
    
    def get_session(self):
        """Get a session from the pool or create a new one"""
        try:
            return self.session_pool.get(block=False)
        except queue.Empty:
            return self.create_session()
    
    def return_session(self, session):
        """Return a session to the pool"""
        try:
            self.session_pool.put(session, block=False)
        except queue.Full:
            session.close()

    def process_batch(self, limit: int = 50, with_progress=False) -> Dict[str, Any]:
        """Process a batch of companies with progress reporting"""
        companies = self.get_companies_to_process(limit)
        results = {
            'total': len(companies),
            'processed': 0,
            'successful': 0,
            'failed': 0,
            'saved': 0,
            'save_failed': 0
        }

        # Create progress bar if using Streamlit
        progress_bar = None
        if with_progress and st:
            progress_bar = st.progress(0)
            status_text = st.empty()

        # Optimize the number of workers
        max_workers = min(os.cpu_count() * 6, 30)  # Increased from 2x to 4x
        
# Use smaller chunk size for better responsiveness
        chunk_size = max(1, min(10, len(companies) // 5))  # Process in smaller chunks
        
        for chunk_start in range(0, len(companies), chunk_size):
            chunk_end = min(chunk_start + chunk_size, len(companies))
            chunk = companies[chunk_start:chunk_end]
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit chunk of companies for processing
                future_to_company = {
                    executor.submit(self.process_company, company): company 
                    for company in chunk
                }
                
                # Process results as they complete
                for future in concurrent.futures.as_completed(future_to_company):
                    company = future_to_company[future]
                    try:
                        data = future.result(timeout=30)
                        if data.get('url_status') == 200:
                            results['successful'] += 1
                        else:
                            results['failed'] += 1
                        
                        # Save results and track success/failure
                        if self.save_results(data):
                            results['saved'] += 1
                        else:
                            results['save_failed'] += 1
                            logger.error(f"Failed to save results for company {company['cod_infotel']}")
                    except Exception as e:
                        logger.error(f"Error processing company {company['cod_infotel']}: {e}")
                        results['failed'] += 1
                    finally:
                        results['processed'] += 1
                        
                        # Update progress
                        if progress_bar:
                            progress_bar.progress(results['processed'] / results['total'])
                            status_text.text(f"Processed: {results['processed']}/{results['total']} | "
                                            f"Valid URLs: {results['successful']} | "
                                            f"Saved: {results['saved']}")

        # Final status update
        if progress_bar:
            progress_bar.progress(1.0)
            
        return results

def main():
    # Use database configuration from config.py
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