# scraper/manager.py
from typing import Dict, List, Generator
import pandas as pd
from database.connectors import MySQLConnector
from .website_finder import WebsiteFinder
from .website_scraper import WebsiteScraper
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import asyncio
from urllib.parse import urlparse
import json

class ScrapingManager:
    def __init__(self):
        self.finder = WebsiteFinder()
        self.scraper = WebsiteScraper()
        self.db = MySQLConnector()
        self.logger = logging.getLogger(__name__)
        
    async def validate_urls_async(self) -> Generator[Dict, None, None]:
        """Asynchronously validate and update existing URLs"""
        try:
            with self.db.get_session() as session:
                companies = session.execute("""
                    SELECT id, razon_social, website 
                    FROM empresas 
                    WHERE website IS NOT NULL AND url_valid = 0
                    LIMIT 50  # Process in batches
                """).fetchall()
                
                async def process_url(company):
                    try:
                        # Normalize URL
                        url = company.website
                        if not url.startswith(('http://', 'https://')):
                            url = f'https://{url}'
                            
                        # Validate URL format
                        parsed = urlparse(url)
                        if not all([parsed.scheme, parsed.netloc]):
                            raise ValueError("Invalid URL format")
                            
                        # Scrape and validate
                        result = await self.scraper.scrape_website_async(url)
                        
                        # Update database
                        with self.db.get_session() as session:
                            session.execute("""
                                UPDATE empresas 
                                SET url_valid = :valid,
                                    website = :url,
                                    telefonos = :phones,
                                    redes_sociales = :social,
                                    fecha_actualizacion = CURRENT_TIMESTAMP
                                WHERE id = :id
                            """, {
                                'valid': True,
                                'url': url,
                                'phones': ','.join(result.get('phones', [])),
                                'social': json.dumps(result.get('social_media', {})),
                                'id': company.id
                            })
                            session.commit()
                            
                        return {
                            'company_id': company.id,
                            'status': 'success',
                            'url': url
                        }
                        
                    except Exception as e:
                        self.logger.error(f"Error processing {company.razon_social}: {str(e)}")
                        return {
                            'company_id': company.id,
                            'status': 'error',
                            'error': str(e)
                        }
                
                # Process URLs concurrently with rate limiting
                tasks = [process_url(company) for company in companies]
                for result in asyncio.as_completed(tasks):
                    yield await result
                    
        except Exception as e:
            self.logger.error(f"Validation error: {str(e)}")
            yield {'status': 'error', 'error': str(e)}
    
    async def find_websites_async(self) -> Generator[Dict, None, None]:
        """Asynchronously find websites for companies without URLs"""
        try:
            with self.db.get_session() as session:
                companies = session.execute("""
                    SELECT id, razon_social 
                    FROM empresas 
                    WHERE website IS NULL OR website = ''
                    LIMIT 50  # Process in batches
                """).fetchall()
                
                async def search_website(company):
                    try:
                        result = await self.finder.search_company_website_async(company.razon_social)
                        
                        if result['url']:
                            # Update database with found URL
                            with self.db.get_session() as session:
                                session.execute("""
                                    UPDATE empresas 
                                    SET website = :url,
                                        confidence_score = :confidence,
                                        fecha_actualizacion = CURRENT_TIMESTAMP
                                    WHERE id = :id
                                """, {
                                    'url': result['url'],
                                    'confidence': int(result['confidence'] * 100),
                                    'id': company.id
                                })
                                session.commit()
                        
                        return {
                            'company_id': company.id,
                            'status': 'success' if result['url'] else 'not_found',
                            'url': result.get('url', '')
                        }
                        
                    except Exception as e:
                        self.logger.error(f"Error searching for {company.razon_social}: {str(e)}")
                        return {
                            'company_id': company.id,
                            'status': 'error',
                            'error': str(e)
                        }
                
                # Process searches concurrently with rate limiting
                tasks = [search_website(company) for company in companies]
                for result in asyncio.as_completed(tasks):
                    yield await result
                    
        except Exception as e:
            self.logger.error(f"Search error: {str(e)}")
            yield {'status': 'error', 'error': str(e)}