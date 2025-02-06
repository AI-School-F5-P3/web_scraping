# scraper/data_pipeline.py
import pandas as pd
from typing import Dict, List
import logging

class DataPipeline:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
    def transform_company_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw company data"""
        try:
            transformed = {
                'nif': self._normalize_nif(data.get('nif', '')),
                'razon_social': data.get('razon_social', '').strip().title(),
                'provincia': data.get('provincia', '').strip().title(),
                'website': self._normalize_url(data.get('website', '')),
                'direccion': data.get('direccion', '').strip(),
                'codigo_postal': self._normalize_postal_code(data.get('codigo_postal', '')),
                'confidence_score': self._calculate_confidence(data)
            }
            return transformed
        except Exception as e:
            self.logger.error(f"Transform error: {str(e)}")
            raise
            
    def _normalize_nif(self, nif: str) -> str:
        return nif.strip().upper()
        
    def _normalize_url(self, url: str) -> str:
        if not url.startswith(('http://', 'https://')):
            return f'https://{url}'
        return url
        
    def _normalize_postal_code(self, cp: str) -> str:
        return str(cp).zfill(5)
        
    def _calculate_confidence(self, data: Dict[str, Any]) -> float:
        # Add confidence score calculation logic
        return 100.0