# agents.py

from langchain.agents import Tool, initialize_agent
from langchain.chains.conversation.memory import ConversationBufferMemory
from langchain.llms.base import LLM
import requests
import pandas as pd
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor
from config import LLM_MODELS, OLLAMA_ENDPOINT, PROVINCIAS_ESPANA, HARDWARE_CONFIG
import re

class CustomLLM(LLM):
    def __init__(self, model_name: str):
        super().__init__()
        self.model_name = model_name
        self.temperature = 0.7
        self.max_tokens = 2000
        self.gpu_config = {
            "use_gpu": HARDWARE_CONFIG["gpu_enabled"],
            "gpu_layers": -1,
            "n_gpu_layers": 50
        }

    def _call(self, prompt: str, stop: List[str] = None) -> str:
        try:
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

    @property
    def _llm_type(self) -> str:
        return "custom_ollama"
    
    class Config:
        extra = "allow"

class DBAgent:
    PROMPT = """Eres un Arquitecto SQL Senior especializado en análisis empresarial español.
    
Usa solo las siguientes columnas en tus consultas:
- cod_infotel, nif, razon_social, domicilio, cod_postal, nom_poblacion, nom_provincia, url, e_commerce, telefono_1, telefono_2, telefono_3

Ejemplos para consultas de filas:
"Dame las 10 primeras empresas de Madrid" ->
SELECT cod_infotel, nif, razon_social, domicilio, cod_postal, nom_poblacion, nom_provincia, url, e_commerce, telefono_1, telefono_2, telefono_3 
FROM sociedades 
WHERE nom_provincia = 'Madrid' 
LIMIT 10;

Ejemplos para consultas agregadas:
"¿Cuántas empresas hay en Madrid?" ->
SELECT COUNT(*) AS total FROM sociedades WHERE nom_provincia = 'Madrid';

"¿Cuál es el porcentaje de empresas que tienen URL versus el total?" ->
SELECT 100.0 * SUM(CASE WHEN url IS NOT NULL AND TRIM(url) <> '' THEN 1 ELSE 0 END) / COUNT(*) AS porcentaje
FROM sociedades;
"""

    def __init__(self):
        self.llm = CustomLLM(LLM_MODELS["base_datos"])

    def generate_query(self, natural_query: str) -> Dict[str, Any]:
        full_prompt = f"{self.PROMPT}\nConsulta: {natural_query}\nGenera la consulta SQL:"
        response = self.llm(full_prompt)
        
        # Use regex to extract a SQL query starting with SELECT and ending with a semicolon.
        sql_match = re.search(r'SELECT.*?;', response, re.DOTALL | re.IGNORECASE)
        if sql_match:
            query = sql_match.group(0)
        else:
            # Fallback query using only the desired columns.
            query = ("SELECT cod_infotel, nif, razon_social, domicilio, cod_postal, "
                     "nom_poblacion, nom_provincia, url, e_commerce, telefono_1, telefono_2, telefono_3 "
                     "FROM sociedades LIMIT 10;")
            
        return {
            "query": query,
            "explanation": response
        }
    
    def _determine_query_type(self, query: str) -> str:
        return "default"

class ScrapingAgent:
    PROMPT = """Eres un Ingeniero de Web Scraping Elite especializado en análisis empresarial.

CAPACIDADES AVANZADAS:
1. Extracción Multi-Nivel:
   - HTML estático (BeautifulSoup)
   - JavaScript dinámico (Selenium)
   - Single Page Apps (SPA)

2. Detección Avanzada:
   - Validación de URLs empresariales
   - Extracción de contactos verificados
   - Identificación de redes sociales
   - Análisis de e-commerce

3. Anti-Detección:
   - Rotación de User-Agents
   - Gestión de cookies
   - Manejo de CAPTCHAs
   - Delays dinámicos

4. Optimización:
   - Uso de GPU para rendering
   - Procesamiento paralelo
   - Gestión de memoria eficiente

REGLAS DE SCRAPING:
1. Respetar robots.txt
2. Implementar delays aleatorios
3. Validar datos extraídos
4. Manejar timeouts y errores
5. Documentar problemas encontrados"""

    def __init__(self):
        self.llm = CustomLLM(LLM_MODELS["scraping"])

    def plan_scraping(self, url: str) -> Dict[str, Any]:
        full_prompt = f"{self.PROMPT}\nAnalizar URL: {url}\nGenerar plan de scraping:"
        analysis = self.llm(full_prompt)
        
        return {
            "strategy": "dynamic" if "javascript" in analysis.lower() else "static",
            "steps": analysis.split('\n'),
            "estimated_resources": self._estimate_resources(url)
        }

    def _estimate_resources(self, url: str) -> Dict[str, Any]:
        return {
            "cpu_intensive": True if "javascript" in url else False,
            "gpu_needed": True if "javascript" in url else False,
            "memory_required": "high" if "javascript" in url else "low"
        }
