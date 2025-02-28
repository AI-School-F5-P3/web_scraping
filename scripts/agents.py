# agents.py

from langchain.llms.base import LLM
import requests
import pandas as pd
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor
from config import GROQ_API_KEY, PROVINCIAS_ESPANA, HARDWARE_CONFIG
import re
import unicodedata
from dataclasses import dataclass, field
from enum import Enum
from thefuzz import fuzz, process

class CustomLLM(LLM):
    def __init__(self, model_name: str, provider: str = "groq"):
        super().__init__()
        self.model_name = model_name
        self.temperature = 0.2
        self.max_tokens = 2000
        self.provider = provider.lower()
        self.gpu_config = {
            "use_gpu": HARDWARE_CONFIG["gpu_enabled"],
            "gpu_layers": -1,
            "n_gpu_layers": 50
        }

    def _call(self, prompt: str, stop: List[str] = None) -> str:
        try:
            # Solo se usa Groq
            response = requests.post(
                "https://api.groq.cloud/generate",
                json={
                    "model": self.model_name,
                    "prompt": prompt,
                    "temperature": self.temperature,
                    "max_tokens": self.max_tokens,
                    "stream": False,
                    **self.gpu_config,
                    "api_key": GROQ_API_KEY
                },
                timeout=30
            )
            return response.json().get('response', '')
        except Exception as e:
            return f"Error: {str(e)}"
        
    def invoke(self, prompt: str, stop: List[str] = None) -> str:
        return self._call(prompt, stop)

    @property
    def _llm_type(self) -> str:
        return "custom_groq"
    
    class Config:
        extra = "allow"

class QueryType(Enum):
    COUNT = "count"
    TABLE = "table"
    AGGREGATE = "aggregate"
    NON_DB = "non_db"

@dataclass
class QueryContext:
    """Stores context about the query being processed"""
    query_type: QueryType = QueryType.TABLE
    has_url_filter: bool = False
    has_ecommerce_filter: bool = False
    has_social_filter: bool = False
    province: Optional[str] = None
    limit: int = 10
    specified_columns: List[str] = field(default_factory=list)

class DBAgent:
    ALLOWED_COLUMNS = [
        'cod_infotel', 'nif', 'razon_social', 'domicilio', 'cod_postal',
        'nom_poblacion', 'nom_provincia', 'url', 'e_commerce',
        'telefono_1', 'telefono_2', 'telefono_3', 'facebook', 'twitter', 'linkedin', 'youtube'
    ]

    # Keywords that indicate a query is not database-related
    NON_DB_KEYWORDS = [
        'tiempo', 'clima', 'temperatura', 'pronóstico',
        'hora', 'fecha', 'noticias', 'tráfico'
    ]

    # Social media related keywords
    SOCIAL_KEYWORDS = [
        'youtube', 'linkedin', 'facebook', 'twitter', 
        'instagram', 'tiktok', 'redes sociales'
    ]

    PROMPT = """You are a Senior SQL Architect specialized in business analytics. Your task is to generate precise SQL queries for the 'guardarail' database or to determine when a query is not database-related. 

INPUT: Natural language query
OUTPUT: A JSON object with the following structure:
{
    "query": "SQL query",
    "explanation": "Explanation of the query",
    "query_type": "count|table|aggregate|non_db",
    "error": "Error message if applicable"
}

IMPORTANT RULES:
1. If the query is not related to companies or the database (e.g., weather, news):
   - Set query_type: "non_db"
   - Set error: "This query is not related to the company database"
   
2. For proportion/percentage queries:
   - Use query_type: "aggregate"
   - Generate a WITH query to calculate percentages

3. For social media queries:
   - Indicate that social media information is not available currently.
   - Suggest consulting available data (URL, e-commerce).

Please answer in Spanish.

EJEMPLOS DE MANEJO DE CASOS ESPECIALES:

1. Consulta no relacionada:
Input: "¿Cuál es el tiempo en Madrid?"
Output: {
    "query": null,
    "explanation": "Esta consulta es sobre el clima y no sobre la base de datos de empresas",
    "query_type": "non_db",
    "error": "Esta consulta no está relacionada con la base de datos de empresas"
}

2. Consulta de proporción:
Input: "¿Qué proporción de empresas de Madrid tienen URL?"
Output: {
    "query": "WITH total AS (SELECT COUNT(*) AS total_count FROM sociedades WHERE nom_provincia ILIKE '%madrid%') SELECT COUNT(*) AS url_count, ROUND(COUNT(*) * 100.0 / total.total_count, 2) AS percentage FROM sociedades, total WHERE nom_provincia ILIKE '%madrid%' AND url IS NOT NULL AND LENGTH(TRIM(url)) > 0 AND (url ILIKE 'http%' OR url ILIKE 'www.%')",
    "explanation": "Cálculo del porcentaje de empresas en Madrid que tienen URL válida",
    "query_type": "aggregate"
}

3. Consulta de redes sociales:
Input: "Dame las empresas de Barcelona con YouTube"
Output: {
    "query": null,
    "explanation": "Actualmente no disponemos de información sobre redes sociales. Solo tenemos datos de URL y e-commerce",
    "query_type": "non_db",
    "error": "Información de redes sociales no disponible"
}
"""

    def __init__(self):
        self.llm = None

    def analyze_query(self, query: str) -> QueryContext:
        """Analyzes the natural language query to extract key information"""
        query_normalized = self.remove_accents(query.lower())
        ctx = QueryContext()

        # Check if query is non-database related
        if any(keyword in query_normalized for keyword in self.NON_DB_KEYWORDS):
            ctx.query_type = QueryType.NON_DB
            return ctx

        # Check if query involves social media
        if any(keyword in query_normalized for keyword in self.SOCIAL_KEYWORDS):
            ctx.query_type = QueryType.NON_DB
            return ctx

        # Detect query type
        if any(word in query_normalized for word in [
            "proporcion", "porcentaje", "ratio", "comparacion"
        ]):
            ctx.query_type = QueryType.AGGREGATE
        elif any(word in query_normalized for word in [
            "cuantas", "cuantos", "numero de", "total de", "cuenta"
        ]):
            ctx.query_type = QueryType.COUNT

        # Extract filters
        ctx.has_url_filter = any(term in query_normalized for term in [
            "con web", "tienen web", "con url", "tienen url"
        ])
        ctx.has_ecommerce_filter = any(term in query_normalized for term in [
            "e-commerce", "ecommerce", "tienda online"
        ])

        # Extract province using fuzzy matching
        ctx.province = self.extract_province_fuzzy(query_normalized)

        # Extract limit
        match = re.search(r'\b(\d+)\b', query)
        if match:
            ctx.limit = int(match.group(1))

        return ctx
    
    def extract_province_fuzzy(self, query_normalized: str) -> Optional[str]:
        """Extracts province from query using fuzzy matching"""
        # First try direct substring matching
        for province in PROVINCIAS_ESPANA:
            if self.remove_accents(province.lower()) in query_normalized:
                return province
        
        # If no direct match, try fuzzy matching with better tokenization
        # Extract potential location words (words that might be provinces)
        potential_locations = []
        words = query_normalized.split()
        
        # Single words
        for word in words:
            if len(word) > 3:  # Only consider words longer than 3 chars
                potential_locations.append(word)
        
        # Word pairs (for multi-word provinces like "Las Palmas")
        for i in range(len(words) - 1):
            if len(words[i]) > 2 and len(words[i+1]) > 2:
                potential_locations.append(f"{words[i]} {words[i+1]}")
        
        # Try fuzzy matching with all potential locations
        best_match = None
        best_score = 0
        
        for location in potential_locations:
            matches = process.extract(
                location, 
                [self.remove_accents(p.lower()) for p in PROVINCIAS_ESPANA], 
                scorer=fuzz.ratio,
                limit=1
            )
            if matches and matches[0][1] > 75 and matches[0][1] > best_score:  # Lower threshold to 75%
                best_score = matches[0][1]
                idx = [self.remove_accents(p.lower()) for p in PROVINCIAS_ESPANA].index(matches[0][0])
                best_match = PROVINCIAS_ESPANA[idx]
        
        return best_match

    def generate_query(self, natural_query: str) -> Dict[str, Any]:
        """Generates SQL query from natural language input"""
        try:
            ctx = self.analyze_query(natural_query)

            if ctx.query_type == QueryType.NON_DB:
                if any(keyword in natural_query.lower() for keyword in self.SOCIAL_KEYWORDS):
                    return {
                        "query": None,
                        "explanation": "Actualmente no disponemos de información sobre redes sociales. Solo tenemos datos de URL y e-commerce.",
                        "query_type": "non_db",
                        "error": "Información de redes sociales no disponible"
                    }
                return {
                    "query": None,
                    "explanation": "Esta consulta no está relacionada con la base de datos de empresas",
                    "query_type": "non_db",
                    "error": "Esta consulta no está relacionada con la base de datos de empresas"
                }

            # Add company extraction for company-specific queries
            company_name = self.extract_company_name(natural_query)
            if company_name:
                ctx.company_name = company_name

            if ctx.query_type == QueryType.AGGREGATE:
                return self.generate_aggregate_query(ctx)
            elif ctx.query_type == QueryType.COUNT:
                return self.generate_count_query(ctx)
            else:
                return self.generate_table_query(ctx)

        except Exception as e:
            import traceback
            trace = traceback.format_exc()
            print(f"Error generating query: {str(e)}\n{trace}")
            return {
                "query": None,
                "explanation": f"Error al procesar la consulta: {str(e)}",
                "error": str(e),
                "query_type": "error"
            }

    def extract_company_name(self, query: str) -> Optional[str]:
        """Extract company name from query using patterns"""
        patterns = [
            r'empresa\s+([A-Za-z0-9\s]+)',
            r'compañía\s+([A-Za-z0-9\s]+)',
            r'sociedad\s+([A-Za-z0-9\s]+)',
            r'información(?:\s+\w+){0,3}\s+de\s+([A-Za-z0-9\s]+)', 
            r'datos(?:\s+\w+){0,3}\s+de\s+([A-Za-z0-9\s]+)'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, query, re.IGNORECASE)
            if match:
                company_name = match.group(1).strip()
                # Remove common company suffixes for better matching
                company_name = re.sub(r'\b(S\.?A\.?|S\.?L\.?)$', '', company_name).strip()
                return company_name
        
        return None

    def generate_aggregate_query(self, ctx: QueryContext) -> Dict[str, Any]:
        """Generates aggregate query with percentages"""
        where_clauses = self.build_where_clauses(ctx)
        base_where = where_clauses.replace("WHERE ", "") if where_clauses else "TRUE"

        # Ensure we handle empty results better
        sql = f"""
        WITH total AS (
            SELECT COUNT(*) AS total_count 
            FROM sociedades 
            WHERE {base_where}
        )
        SELECT 
            COUNT(*) AS filtered_count,
            CASE 
                WHEN (SELECT total_count FROM total) > 0 
                THEN ROUND(COUNT(*) * 100.0 / (SELECT total_count FROM total), 2)
                ELSE 0
            END AS percentage
        FROM sociedades, total
        {where_clauses}
        """

        return {
            "query": sql.strip(),
            "explanation": self.generate_explanation(ctx),
            "query_type": "aggregate",
            "error": None
        }

    def generate_count_query(self, ctx: QueryContext) -> Dict[str, Any]:
        """Generates COUNT query based on context"""
        where_clauses = self.build_where_clauses(ctx)
        
        sql = f"""
        SELECT COUNT(*) AS total
        FROM sociedades
        {where_clauses}
        """

        return {
            "query": sql.strip(),
            "explanation": self.generate_explanation(ctx),
            "query_type": "count",
            "error": None
        }

    def generate_table_query(self, ctx: QueryContext) -> Dict[str, Any]:
        """Generates table query based on context"""
        where_clauses = self.build_where_clauses(ctx)
        
        columns = ctx.specified_columns or self.ALLOWED_COLUMNS
        columns_str = ", ".join(columns)
        
        # Special handling for company-specific queries
        if hasattr(ctx, 'company_name') and ctx.company_name:
            # Add company name filter
            company_clause = f"razon_social ILIKE '%{ctx.company_name}%'"
            if where_clauses:
                where_clauses += f" AND {company_clause}"
            else:
                where_clauses = f"WHERE {company_clause}"
        
        sql = f"""
        SELECT {columns_str}
        FROM sociedades
        {where_clauses}
        LIMIT {ctx.limit}
        """

        return {
            "query": sql.strip(),
            "explanation": self.generate_explanation(ctx),
            "query_type": "table",
            "error": None
        }

    @staticmethod
    def remove_accents(text: str) -> str:
        """Removes accents from text"""
        return ''.join(c for c in unicodedata.normalize('NFD', text)
                      if unicodedata.category(c) != 'Mn')

    @staticmethod
    def get_provinces() -> List[str]:
        """Returns list of Spanish provinces"""
        return [
            "Madrid", "Barcelona", "Valencia", "Sevilla", "Zaragoza",
            "Málaga", "Murcia", "Palma", "Las Palmas", "Bilbao",
            # Add more provinces as needed
        ]

    def build_where_clauses(self, ctx: QueryContext) -> str:
        """Builds WHERE clause based on query context"""
        clauses = []
        
        if ctx.province:
            clauses.append(f"nom_provincia ILIKE '%{ctx.province}%'")
            
        if ctx.has_url_filter:
            clauses.extend([
                "url IS NOT NULL",
                "LENGTH(TRIM(url)) > 0",
                "(url ILIKE 'http%' OR url ILIKE 'www.%')"
            ])
            
        if ctx.has_ecommerce_filter:
            clauses.append("e_commerce = true")
            
        if clauses:
            return "WHERE " + " AND ".join(clauses)
        return ""

    def generate_explanation(self, ctx: QueryContext) -> str:
        """Generates human-readable explanation of the query"""
        parts = []
        
        if ctx.query_type == QueryType.COUNT:
            parts.append("Conteo de empresas")
        elif ctx.query_type == QueryType.AGGREGATE:
            parts.append("Cálculo de proporción de empresas")
        else:
            parts.append("Listado de empresas")
            
        if ctx.province:
            parts.append(f"en {ctx.province}")
            
        if ctx.has_url_filter:
            parts.append("con URL válida")
            
        if ctx.has_ecommerce_filter:
            parts.append("con e-commerce")
            
        return " ".join(parts)