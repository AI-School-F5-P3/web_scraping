# scraper/website_finder.py
import requests
from bs4 import BeautifulSoup
from googlesearch import search
import re
from urllib.parse import urlparse
import logging

class WebsiteFinder:
    def __init__(self):
        self.session = requests.Session()
        self.logger = logging.getLogger(__name__)
    
    def clean_url(self, url: str) -> str:
        """Clean URL to get base domain"""
        if not url:
            return ""
        try:
            parsed = urlparse(url)
            return f"{parsed.scheme}://{parsed.netloc}"
        except Exception:
            return ""
            
    def search_company_website(self, company_name: str, existing_url: str = None) -> dict:
        """Search for company website using Google"""
        try:
            search_query = f"{company_name} web oficial"
            potential_urls = []
            
            # Search first 5 results
            for url in search(search_query, num_results=5):
                clean_url = self.clean_url(url)
                if clean_url:
                    potential_urls.append(clean_url)
            
            if existing_url:
                clean_existing = self.clean_url(existing_url)
                if clean_existing in potential_urls:
                    return {
                        "url": clean_existing,
                        "validated": True,
                        "confidence": 0.9
                    }
            
            # Return best match if found
            if potential_urls:
                return {
                    "url": potential_urls[0],
                    "validated": False,
                    "confidence": 0.7
                }
                
            return {"url": "", "validated": False, "confidence": 0}
            
        except Exception as e:
            self.logger.error(f"Error searching website: {str(e)}")
            return {"url": "", "validated": False, "confidence": 0}