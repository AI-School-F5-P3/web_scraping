# agents.py

from langchain.agents import Tool, initialize_agent
from langchain.chains.conversation.memory import ConversationBufferMemory
from langchain.llms.base import LLM
from langchain_groq import ChatGroq
import requests
import pandas as pd
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor
from config import GROQ_API_KEY, PROVINCIAS_ESPANA, HARDWARE_CONFIG
import re
from scraping import ProWebScraper
import unicodedata

class CustomLLM(LLM):
    def __init__(self, model_name: str, provider: str = "groq"):
        super().__init__()
        self.model_name = model_name
        self.temperature = 0.7
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

class DBAgent:
    PROMPT = """Eres un Arquitecto SQL Senior especializado en análisis empresarial para la base de datos "guardarail" en España. Tu tarea es transformar consultas en lenguaje natural a SQL válido y preciso. Responde únicamente a preguntas relacionadas con la base de datos y sus columnas. Si la petición no está relacionada con la base de datos, responde: "Solo puedo ayudarte con información relacionada con la base de datos guardarail."

# Reglas:
1. **Columnas permitidas:**
   - cod_infotel, nif, razon_social, domicilio, cod_postal, nom_poblacion, nom_provincia, url, e_commerce, telefono_1, telefono_2, telefono_3.
2. **Seguridad:** Solo genera sentencias SELECT. Nunca INSERT, UPDATE o DELETE.
3. **Límites:**
   - Si la consulta no especifica un límite numérico y es de filas, añade 'LIMIT 10' por defecto.
   - Para consultas de agregación (conteo), utiliza COUNT(*) y no añadas LIMIT.
4. **Normalización y comparaciones:**
   - Todas las comparaciones de texto deben realizarse sin distinguir acentos ni mayúsculas (usa ILIKE en SQL).
   - Corrige errores ortográficos comunes en nombres de provincias (por ejemplo, 'barclona' → 'barcelona') eliminando acentos.
5. **Filtros adicionales:**
   - Si la consulta indica "con url" o "que tengan url", añade la condición: `AND url IS NOT NULL AND url <> ''`.
   - Si la consulta menciona "con e-commerce" o similar, añade: `AND e_commerce = true`.
6. **Formato SQL:**
   - Usa mayúsculas para todas las keywords SQL (SELECT, FROM, WHERE, etc.).
   - Emplea alias descriptivos, por ejemplo, 'total' para COUNT(*).
7. **Ambigüedad:**
   - Si la petición es ambigua, mal formulada o requiere suposiciones, responde en lenguaje natural solicitando más detalles: "¿Podrías especificar mejor la consulta? Por ejemplo, indica la provincia o el criterio adicional que deseas aplicar."
8. **Errores o columnas no admitidas:**
   - Si se mencionan columnas o funciones no disponibles, responde: "No puedo ayudarte con esa información. Las columnas disponibles son: cod_infotel, nif, razon_social, domicilio, cod_postal, nom_poblacion, nom_provincia, url, e_commerce, telefono_1, telefono_2, telefono_3."

# Ejemplos:

- **Consulta de filas:**
  - "Dame las 10 primeras empresas de Madrid" →
    ```sql
    SELECT cod_infotel, nif, razon_social, domicilio, cod_postal, nom_poblacion, nom_provincia, url, e_commerce, telefono_1, telefono_2, telefono_3
    FROM sociedades
    WHERE nom_provincia ILIKE '%madrid%'
    LIMIT 10;
    ```
  - "Dame las empresas de Ávila que tengan url" →
    ```sql
    SELECT cod_infotel, nif, razon_social, domicilio, cod_postal, nom_poblacion, nom_provincia, url, e_commerce, telefono_1, telefono_2, telefono_3
    FROM sociedades
    WHERE nom_provincia ILIKE '%avila%' AND url IS NOT NULL AND url <> ''
    LIMIT 10;
    ```
- **Consulta de agregación:**
  - "¿Cuántas empresas hay en Barcelona?" →
    ```sql
    SELECT COUNT(*) AS total
    FROM sociedades
    WHERE nom_provincia ILIKE '%barcelona%';
    ```
  - "¿Cuántas tiendas online hay en Valencia?" →
    ```sql
    SELECT COUNT(*) AS total
    FROM sociedades
    WHERE nom_provincia ILIKE '%valencia%' AND e_commerce = true;
    ```

# Si la consulta no es sobre la base de datos, responde:
"Solo puedo ayudarte con información relacionada con la base de datos guardarail."
"""

    def __init__(self):
        # Use Groq for querying the database
        self.llm = None

    def generate_query(self, natural_query: str) -> Dict[str, Any]:
        """Genera consulta SQL a partir de lenguaje natural."""
        # Normalizar la consulta quitando acentos y en minúsculas
        query_normalized = self.remove_accents(natural_query.lower())
        
        # Detectar si es consulta de conteo
        is_count = any(word in query_normalized for word in ["cuantas", "cuantos", "numero de", "total de", "cuenta"])
        
        # Extraer provincia si se menciona
        provinces = [p.lower() for p in PROVINCIAS_ESPANA]
        province = next((p for p in provinces if p in query_normalized), None)
        
        # Detectar si se pide filtrar por URL válida
        filter_url = "con url" in query_normalized or "que tengan url" in query_normalized
        
        # Construir cláusula WHERE
        where_clauses = []
        if province:
            where_clauses.append(f"nom_provincia ILIKE '%{province}%'")
        if filter_url:
            # Se agregan condiciones para que la url sea válida
            where_clauses.append("url IS NOT NULL")
            where_clauses.append("TRIM(url) <> ''")
            where_clauses.append("(url ILIKE 'http://%' OR url ILIKE 'https://%' OR url ILIKE 'www.%')")
        
        where_clause = ""
        if where_clauses:
            where_clause = "WHERE " + " AND ".join(where_clauses)
        
        if is_count:
            # Consulta de conteo no lleva LIMIT
            query = f"""
            SELECT COUNT(*) AS total
            FROM sociedades
            {where_clause};
            """
        else:
            # Extraer límite numérico si se menciona; de lo contrario, LIMIT 10
            # Aquí se puede hacer un análisis extra para detectar un número en la consulta
            # Por simplicidad, se usa LIMIT 10 si no se detecta
            limit = 10
            # Ejemplo: Si se detecta "10" en la consulta y no hay otra información, usar ese valor
            match = re.search(r'\b(\d+)\b', natural_query)
            if match:
                limit = int(match.group(1))
            query = f"""
            SELECT cod_infotel, nif, razon_social, domicilio, cod_postal, 
                nom_poblacion, nom_provincia, url 
            FROM sociedades
            {where_clause}
            LIMIT {limit};
            """
        
        return {
            "query": query.strip(),
            "explanation": f"Generated SQL query for: {natural_query}"
        }

    @staticmethod
    def remove_accents(text: str) -> str:
        """Remove accents from text."""
        return ''.join(c for c in unicodedata.normalize('NFD', text) 
                    if unicodedata.category(c) != 'Mn')
    
    def _determine_query_type(self, query: str) -> str:
        return "default"

class ScrapingAgent:
    PROMPT = """Eres un Ingeniero de Web Scraping Elite especializado en análisis empresarial. Tu tarea es la siguiente:
1. Si se dispone de una URL válida para una empresa:
   - Extrae información clave de la web, tales como:
     • Teléfonos (verificados).
     • Redes sociales (Facebook, Twitter, LinkedIn, Instagram, YouTube).
     • Indicador de presencia de e-commerce.
   - Valida la información extraída usando técnicas de scraping (HTML estático y/o dinámico) y documenta los pasos utilizados.
2. Si la empresa no tiene URL:
   - Sugiere una URL candidata basándote en la razón social y en posibles combinaciones de su nombre.
   - Una vez sugerida la URL, procede a extraer la misma información que en el caso anterior.
3. Aplica técnicas avanzadas de anti-detección:
   - Rotación de User-Agents, manejo de cookies y CAPTCHAs, delays aleatorios.
4. Devuelve un plan de scraping estructurado en formato JSON que contenga:
   - "strategy": "static" o "dynamic" (según la naturaleza de la web).
   - "steps": una lista de pasos detallados para el scraping.
   - "estimated_resources": un objeto con indicadores como "cpu_intensive", "gpu_needed" y "memory_required".
   
Utiliza un lenguaje claro, conciso y enfocado en la extracción de datos relevantes para análisis empresarial."""
    
    def __init__(self):
        self.llm = None
    
    def plan_scraping(self, url: str) -> dict:
        """
        Returns a more meaningful scraping plan with actual functionality.
        """
        if not url or pd.isna(url):
            # Implement URL discovery based on company name
            return {
                "strategy": "url_discovery",
                "steps": ["Buscar URL basada en nombre empresa"],
                "phones": [],
                "social_media": {},
                "is_ecommerce": False,
                "url_exists": False
            }
        
        try:
            # Use ProWebScraper for actual scraping
            scraper = ProWebScraper(use_proxies=False)
            result = scraper.scrape_url(url, {})
            
            # If URL exists but couldn't be accessed, increase the timeout
            if result and not result.get('url_exists', False):
                # Try again with increased timeout
                scraper.chrome_options.set_page_load_timeout(60)
                result = scraper.scrape_url(url, {})
            
            return {
                "strategy": "dynamic" if result.get('is_ecommerce') else "static",
                "steps": [
                    "Verificación de URL",
                    "Extracción de teléfonos",
                    "Búsqueda de redes sociales",
                    "Detección de e-commerce"
                ],
                "phones": result.get('phones', []),
                "social_media": result.get('social_media', {}),
                "is_ecommerce": result.get('is_ecommerce', False),
                "url_exists": result.get('url_exists', False)
            }
        except Exception as e:
            return {
                "strategy": "failed",
                "steps": [f"Error: {str(e)}"],
                "phones": [],
                "social_media": {},
                "is_ecommerce": False,
                "url_exists": False
            }
    
    def _estimate_resources(self, url: str) -> dict:
        return {
            "cpu_intensive": True if "javascript" in url.lower() else False,
            "gpu_needed": True if "javascript" in url.lower() else False,
            "memory_required": "high" if "javascript" in url.lower() else "low"
        }
