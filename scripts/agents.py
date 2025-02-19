# agents.py

from langchain.agents import Tool, initialize_agent
from langchain.chains.conversation.memory import ConversationBufferMemory
from langchain.llms.base import LLM
from langchain_groq import ChatGroq
import requests
import pandas as pd
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor
from config import LLM_MODELS, GROQ_API_KEY, OLLAMA_ENDPOINT, PROVINCIAS_ESPANA, HARDWARE_CONFIG
import re
from scraping import ProWebScraper
import unicodedata

class CustomLLM(LLM):
    def __init__(self, model_name: str, provider: str = "ollama"):
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
            if self.provider == "groq":
                response = requests.post(
                    # Using the Groq library default endpoint if not provided, or rely on the client.
                    # For our example, we assume the client library uses GROQ_API_KEY internally.
                    # If you need to call a specific endpoint, you could add that logic here.
                    # We'll assume the Groq library handles it.
                    # For illustration, we simply send the API key in the payload.
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
            else:
                response = requests.post(
                    OLLAMA_ENDPOINT,
                    json={
                        "model": self.model_name,
                        "prompt": prompt,
                        "temperature": self.temperature,
                        "max_tokens": self.max_tokens,
                        "stream": False,
                        **self.gpu_config
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
        return "custom_groq" if self.provider == "groq" else "custom_ollama"
    
    class Config:
        extra = "allow"

class DBAgent:
    PROMPT = """Eres un Arquitecto SQL Senior especializado en análisis empresarial español. Genera SQL válido o respuestas en lenguaje natural (español) según corresponda.
    
# Reglas:
1. **Columnas permitidas:**
   - cod_infotel, nif, razon_social, domicilio, cod_postal, nom_poblacion, nom_provincia, url, e_commerce, telefono_1, telefono_2, telefono_3.
2. **Seguridad:** Solo genera SELECT. Nunca INSERT, UPDATE o DELETE.
3. **Límites:** Si la consulta no especifica un límite, añade 'LIMIT 10' por defecto.
4. **Geografía:** Si la consulta menciona una ubicación sin especificar si es ciudad o provincia, asume que es provincia (nom_provincia) a menos que se indique lo contrario.
5. **Normalización:**
   - Corrige errores ortográficos en nombres de provincias (ej. 'barclona' → 'barcelona').
   - Elimina acentos para evitar fallos en ILIKE.
   - Usa ILIKE para todas las comparaciones de texto.
6. **Formato SQL:**
   - Usa mayúsculas para keywords SQL (SELECT, WHERE, etc.).
   - Usa alias descriptivos (ej. 'total' para COUNT(*)).
7. **Consultas ambiguas o erróneas:**
   - Si la petición es ambigua, está mal formulada o requiere suposiciones, responde en lenguaje natural solicitando clarificación.
   - Si la consulta menciona columnas, tablas o funciones no admitidas, responde: "No puedo ayudarte con esa información. Las columnas disponibles son: [listar_columnas]".
8. **Criterios de filtro:**
   - Si la consulta no especifica ningún criterio, omite la cláusula WHERE.
   - Si no hay suficientes datos para generar SQL, responde en español: "No encuentro datos para tu consulta. ¿Podrías reformularla?".
9. **Consulta de agregación:** Para consultas de conteo, utiliza COUNT(*) y no añade LIMIT.

# Ejemplos de consultas de filas:
- "Dame las 10 primeras empresas de Madrid" →
  ```sql
  SELECT cod_infotel, nif, razon_social, domicilio, cod_postal, nom_poblacion, nom_provincia, url, e_commerce, telefono_1, telefono_2, telefono_3 
  FROM sociedades 
  WHERE nom_provincia ILIKE '%madrid%'
  LIMIT 10;
  ```
- "Empresas en Málaga con e-commerce activo" →
  ```sql
  SELECT cod_infotel, nif, razon_social, domicilio, cod_postal, nom_poblacion, nom_provincia, url, e_commerce, telefono_1, telefono_2, telefono_3 
  FROM sociedades 
  WHERE nom_provincia ILIKE '%malaga%' AND e_commerce = true 
  LIMIT 10;
  ```

# Ejemplos de consultas agregadas:
- "¿Cuántas empresas hay en Barcelona?" →
  ```sql
  SELECT COUNT(*) AS total FROM sociedades WHERE nom_provincia ILIKE '%barcelona%';
  ```
- "¿Cuántas tiendas online hay en Valencia?" →
  ```sql
  SELECT COUNT(*) AS total FROM sociedades WHERE nom_provincia ILIKE '%valencia%' AND e_commerce = true;
  ```

# Respuestas en lenguaje natural:
- Si la petición es ambigua: "¿Podrías especificar la provincia o ciudad?".
- Si se piden datos no disponibles: "No tengo información sobre esa columna. Las columnas disponibles son: cod_infotel, nif, razon_social, domicilio, cod_postal, nom_poblacion, nom_provincia, url, e_commerce, telefono_1, telefono_2, telefono_3.".
- Si no se puede generar SQL: "No encuentro datos para tu consulta. ¿Podrías reformularla?".
"""

    def __init__(self):
        # Use Groq for querying the database
        self.llm = CustomLLM(LLM_MODELS["base_datos"], provider="groq")

    def generate_query(self, natural_query: str) -> Dict[str, Any]:
        """Generate SQL query from natural language."""
        # Normalize the query text
        query_normalized = self.remove_accents(natural_query.lower())
        
        # Check if it's a count query
        is_count = any(word in query_normalized for word in ["cuantas", "cuantos", "numero de", "total de", "cuenta"])
        
        # Extract province name if present
        provinces = [p.lower() for p in PROVINCIAS_ESPANA]
        province = next((p for p in provinces if p in query_normalized), None)
        
        if is_count and province:
            # Generate count query with province filter
            query = f"""
            SELECT COUNT(*) as total 
            FROM sociedades 
            WHERE LOWER(nom_provincia) LIKE '%{province}%';
            """
        elif province:
            # Generate regular query with province filter
            query = f"""
            SELECT cod_infotel, nif, razon_social, domicilio, cod_postal, 
                nom_poblacion, nom_provincia, url 
            FROM sociedades 
            WHERE LOWER(nom_provincia) LIKE '%{province}%'
            LIMIT {5 if '5' in natural_query else 10};
            """
        else:
            # Default query
            query = """
            SELECT cod_infotel, nif, razon_social, domicilio, cod_postal, 
                nom_poblacion, nom_provincia, url 
            FROM sociedades 
            LIMIT 10;
            """
        
        return {
            "query": query,
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
        self.llm = CustomLLM(LLM_MODELS["scraping"])
    
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
