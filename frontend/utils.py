# frontend/utils.py
import pandas as pd
import re
from typing import Dict, List, Any
import logging
from datetime import datetime
import hashlib

logger = logging.getLogger(__name__)

def sanitize_input(text: str) -> str:
    """Sanitize user input to prevent SQL injection"""
    return re.sub(r'[;\'\"\\]', '', text)

def generate_unique_id(data: Dict[str, Any]) -> str:
    """Generate unique ID for a company record"""
    content = f"{data.get('nif', '')}{data.get('razon_social', '')}{datetime.now().isoformat()}"
    return hashlib.md5(content.encode()).hexdigest()

def validate_company_data(data: Dict[str, Any]) -> tuple[bool, str]:
    """Validate company data before insertion"""
    required_fields = ['nif', 'razon_social', 'provincia']
    
    for field in required_fields:
        if not data.get(field):
            return False, f"Campo requerido faltante: {field}"
            
    if not re.match(r'^[A-Z0-9]{9}$', data.get('nif', '')):
        return False, "Formato de NIF inválido"
        
    return True, ""

def format_error_message(error: Exception) -> str:
    """Format error messages for user display"""
    return f"Error: {str(error)}" if Config.DEBUG else "Ha ocurrido un error. Por favor intente nuevamente."

class MetricsCalculator:
    @staticmethod
    def calculate_success_rate(total: int, successful: int) -> float:
        """Calculate success rate percentage"""
        return round((successful / total * 100) if total > 0 else 0, 2)

    @staticmethod
    def calculate_progress(current: int, total: int) -> float:
        """Calculate progress percentage"""
        return round((current / total * 100) if total > 0 else 0, 2)