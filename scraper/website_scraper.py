# scraper/website_scraper.py
import requests
from bs4 import BeautifulSoup
import re
import json
from typing import Dict, List
import logging

class WebsiteScraper:
    def __init__(self):
        self.session = requests.Session()
        self.logger = logging.getLogger(__name__)
        
    def extract_phones(self, soup: BeautifulSoup) -> List[str]:
        """Extract phone numbers from website"""
        phones = set()
        phone_pattern = r'(?:\+34|0034|34)?[-\s]?[6789]\d{8}'
        
        # Search in text
        text = soup.get_text()
        phones.update(re.findall(phone_pattern, text))
        
        # Search in links
        for link in soup.find_all('a', href=True):
            if 'tel:' in link['href']:
                phone = re.sub(r'[^\d+]', '', link['href'])
                phones.add(phone)
                
        return list(phones)
        
    def extract_social_media(self, soup: BeautifulSoup) -> Dict[str, str]:
        """Extract social media links"""
        social_platforms = {
            'facebook': r'facebook\.com',
            'twitter': r'twitter\.com|x\.com',
            'instagram': r'instagram\.com',
            'linkedin': r'linkedin\.com'
        }
        
        social_links = {}
        for link in soup.find_all('a', href=True):
            href = link['href']
            for platform, pattern in social_platforms.items():
                if re.search(pattern, href, re.I):
                    social_links[platform] = href
                    
        return social_links
        
    def detect_ecommerce(self, soup: BeautifulSoup) -> bool:
        """Detect if website has ecommerce functionality"""
        ecommerce_indicators = [
            'carrito', 'cart', 'comprar', 'buy', 'tienda', 'shop',
            'producto', 'product', 'precio', 'price'
        ]
        
        text = soup.get_text().lower()
        return any(indicator in text for indicator in ecommerce_indicators)
        
    def scrape_website(self, url: str) -> Dict:
        """Scrape website for company information"""
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            return {
                'phones': self.extract_phones(soup),
                'social_media': self.extract_social_media(soup),
                'ecommerce': self.detect_ecommerce(soup),
                'success': True
            }
            
        except Exception as e:
            self.logger.error(f"Error scraping {url}: {str(e)}")
            return {
                'phones': [],
                'social_media': {},
                'ecommerce': False,
                'success': False,
                'error': str(e)
            }