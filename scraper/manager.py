# scraper/manager.py
from typing import Dict, List
import pandas as pd
from database.connectors import MySQLConnector
from .website_finder import WebsiteFinder
from .website_scraper import WebsiteScraper
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

class ScrapingManager:
    def __init__(self):
        self.finder = WebsiteFinder()
        self.scraper = WebsiteScraper()
        self.db = MySQLConnector()
        self.logger = logging.getLogger(__name__)
        
    def validate_existing_urls(self):
        """Validate and update existing URLs"""
        with self.db.get_session() as session:
            companies = session.execute("""
                SELECT id, razon_social, website 
                FROM empresas 
                WHERE website IS NOT NULL AND url_valid = 0
            """).fetchall()
            
            results = []
            with ThreadPoolExecutor(max_workers=5) as executor:
                future_to_company = {
                    executor.submit(self.process_company, company): company 
                    for company in companies
                }
                
                for future in as_completed(future_to_company):
                    company = future_to_company[future]
                    try:
                        result = future.result()
                        results.append(result)
                    except Exception as e:
                        self.logger.error(f"Error processing {company.razon_social}: {str(e)}")
            
            return results
            
    def find_missing_websites(self):
        """Find websites for companies without URLs"""
        with self.db.get_session() as session:
            companies = session.execute("""
                SELECT id, razon_social 
                FROM empresas 
                WHERE website IS NULL OR website = ''
            """).fetchall()
            
            results = []
            with ThreadPoolExecutor(max_workers=5) as executor:
                future_to_company = {
                    executor.submit(self.process_company, company): company 
                    for company in companies
                }
                
                for future in as_completed(future_to_company):
                    company = future_to_company[future]
                    try:
                        result = future.result()
                        results.append(result)
                    except Exception as e:
                        self.logger.error(f"Error processing {company.razon_social}: {str(e)}")
            
            return results
            
    def process_company(self, company: Dict) -> Dict:
        """Process a single company"""
        try:
            # Find/validate website
            website_info = self.finder.search_company_website(
                company.razon_social, 
                company.website if hasattr(company, 'website') else None
            )
            
            if not website_info['url']:
                return {
                    'company_id': company.id,
                    'status': 'no_website_found'
                }
            
            # Scrape website data
            scrape_result = self.scraper.scrape_website(website_info['url'])
            
            # Update database
            with self.db.get_session() as session:
                session.execute("""
                    UPDATE empresas 
                    SET website = :url,
                        url_valid = :validated,
                        telefonos = :phones,
                        redes_sociales = :social,
                        ecommerce = :ecommerce,
                        confidence_score = :confidence
                    WHERE id = :company_id
                """, {
                    'url': website_info['url'],
                    'validated': website_info['validated'],
                    'phones': ','.join(scrape_result['phones']),
                    'social': json.dumps(scrape_result['social_media']),
                    'ecommerce': scrape_result['ecommerce'],
                    'confidence': int(website_info['confidence'] * 100),
                    'company_id': company.id
                })
                
            return {
                'company_id': company.id,
                'status': 'success',
                'website': website_info['url'],
                'data_found': scrape_result
            }
            
        except Exception as e:
            self.logger.error(f"Error in process_company: {str(e)}")
            return {
                'company_id': company.id,
                'status': 'error',
                'error': str(e)
            }