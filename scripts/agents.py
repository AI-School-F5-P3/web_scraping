# agents.py

from langchain.agents import Tool, initialize_agent
from langchain.memory import ConversationBufferMemory
from langchain.llms.base import LLM
from langchain.callbacks.manager import CallbackManagerForLLMRun
from pydantic import Field, BaseModel
import requests
import pandas as pd
import re
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor
from config import LLM_MODELS, OLLAMA_ENDPOINT, PROVINCIAS_ESPANA, HARDWARE_CONFIG

class CustomLLM(LLM):
    model_name: str = Field(...)
    temperature: float =  Field(default=0.7)
    max_tokens: int = Field(default=2000)
    gpu_config: Dict[str, Any] = Field(default_factory=lambda: {
        "use_gpu": HARDWARE_CONFIG["gpu_enabled"],
        "gpu_layers": -1,
        "n_gpu_layers": 50
    })

    class Config:
        arbitrary_types_allowed = True

    def _call(self, prompt: str, stop: Optional[List[str]] = None, run_manager: Optional[CallbackManagerForLLMRun] = None) -> str:
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
            response.raise_for_status()  # Lanza excepción para códigos 4xx/5xx
            return response.json().get('response', '')
        except requests.exceptions.RequestException as e:
            return f"API Error: {str(e)}"
        except Exception as e:
            return f"Unexpected Error: {str(e)}"
        
    @property
    def _llm_type(self) -> str:
        return "custom_ollama"
    
    @property
    def _identifying_params(self) -> Dict[str, Any]:
        return {
            "model_name": self.model_name,
            "temperature": self.temperature,
            "max_tokens": self.max_tokens,
            "gpu_config": self.gpu_config
        }

class OrchestratorAgent:
    SYSTEM_PROMPT = f"""Eres un experto Director de Operaciones de nivel enterprise para análisis empresarial en España.

CONTEXTO Y ESPECIALIZACIONES:
1. Base de Datos Empresarial:
   - Gestión avanzada de registros empresariales
   - Análisis multi-provincial: {', '.join(PROVINCIAS_ESPANA)}
   - Validación y limpieza de datos empresariales

2. Web Scraping Especializado:
   - Análisis de presencia digital empresarial
   - Extracción de información de contacto verificada
   - Detección de actividad e-commerce

3. Análisis Empresarial:
   - Validación de URLs empresariales
   - Detección de redes sociales corporativas
   - Verificación de datos de contacto

REGLAS ESTRICTAS:
1. Enfoque Exclusivo: SOLO procesar consultas relacionadas con empresas españolas
2. Prioridad en Datos: Mantener precisión y verificación en todo momento
3. Optimización: Utilizar recursos GPU/CPU eficientemente
4. Coordinación: Distribuir tareas entre agentes según especialidad

EJEMPLOS DE INTERACCIÓN:
Usuario: "Analizar empresas de Madrid"
→ Coordinar DBAgent para consulta inicial
→ Distribuir ScrapingAgent para análisis web
→ Consolidar y verificar resultados

Usuario: "Verificar webs de Barcelona"
→ Priorizar análisis por lotes
→ Paralelizar scraping con recursos disponibles
→ Validar y actualizar estados URL"""

    def __init__(self):
        self.llm = CustomLLM(model_name=LLM_MODELS["orquestador"])
        self.memory = ConversationBufferMemory(memory_key="chat_history", return_messages=True)
        self.max_workers = HARDWARE_CONFIG["max_workers"]

    def process(self, user_input: str) -> Dict[str, Any]:
        if not self._validate_input(user_input):
            return {
                "response": "Solo proceso consultas relacionadas con empresas españolas.",
                "valid": False
            }

        full_prompt = f"{self.SYSTEM_PROMPT}\nUSER: {user_input}"
        response = self.llm(full_prompt)
        
        return {
            "response": response,
            "valid": True,
            "context": self.memory.load_memory_variables({})
        }

    def _validate_input(self, query: str) -> bool:
        business_terms = [
            'empresa', 'negocio', 'sociedad', 'comercio',
            'web', 'url', 'análisis', 'datos'
        ] + PROVINCIAS_ESPANA
        
        query_lower = query.lower()
        return any(term.lower() in query_lower for term in business_terms)

class DBAgent:
    PROMPT = """Eres un Arquitecto SQL Senior especializado en análisis empresarial español. 

COMPETENCIAS CORE:
1. Consultas Avanzadas:
   - CTEs recursivos y Window Functions
   - Análisis multi-tabla optimizado
   - Full-text search en español

2. Optimización Query:
   - Índices específicos para datos empresariales
   - Particionamiento por provincia/región
   - Gestión eficiente de batch processing

3. Validación de Datos:
   - Normalización de datos empresariales
   - Detección de duplicados inteligente
   - Control de integridad referencial

REGLAS DE GENERACIÓN SQL:
1. Priorizar índices y optimización
2. Usar CTEs para consultas complejas
3. Implementar paginación eficiente
4. Manejar errores y edge cases
5. Documentar queries generados

OUTPUT REQUERIDO:
1. Query SQL optimizado
2. Explicación de optimizaciones
3. Índices recomendados"""

    def __init__(self):
        self.llm = CustomLLM(model_name=LLM_MODELS["base_datos"])

    def generate_query(self, natural_query: str) -> Dict[str, Any]:
        try:
            response = self.llm(f"{self.PROMPT}\nConsulta: {natural_query}")
            query = self._extract_sql(response)
            return {
                "query": query,
                "explanation": response,
                "sql_type": self._determine_query_type(query)
            }
        except Exception as e:
            return {
                "query": query,
                "explanation": f"Error: {str(e)}",
                "sql_type": "ERROR"
            }
        

    def _extract_sql(self, response: str) -> str:
        # Buscar bloques de código entre ```
        code_blocks = re.findall(r'```sql\n(.*?)\n```', response, re.DOTALL)
        if code_blocks:
            return code_blocks[0].strip()
    
        # Buscar SELECT/INSERT/UPDATE explícitos
        sql_keywords = r'(SELECT|INSERT|UPDATE|DELETE|CREATE|ALTER|DROP)'
        matches = re.search(rf'{sql_keywords}.*?;', response, re.DOTALL | re.IGNORECASE)
        return matches.group(0) if matches else "SELECT * FROM sociedades LIMIT 10;"

    def _determine_query_type(self, query: str) -> str:
        query = query.lower()
        if "select" in query:
            return "SELECT"
        elif "update" in query:
            return "UPDATE"
        elif "insert" in query:
            return "INSERT"
        return "UNKNOWN"

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
        self.llm = CustomLLM(model_name=LLM_MODELS["scraping"])

    def plan_scraping(self, url: str) -> Dict[str, Any]:
        try:
            analysis = self.llm(f"{self.PROMPT}\nURL: {url}")
            return {
                "strategy": "dynamic" if "javascript" in analysis.lower() else "static",
                "steps": analysis.split('\n'),
                "estimated_resources": self._estimate_resources(url)
            }
        except Exception as e:
            return {
                "strategy": "ERROR",
                "steps": [f"Error: {str(e)}"],
                "estimated_resources": {}
            }

    def _estimate_resources(self, url: str) -> Dict[str, Any]:
        return {
            "cpu_intensive": True if "javascript" in url else False,
            "gpu_needed": True if "javascript" in url else False,
            "memory_required": "high" if "javascript" in url else "low"
        }