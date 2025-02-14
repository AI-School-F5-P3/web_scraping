# agents.py

from langchain.agents import Tool, initialize_agent
from langchain.chains.conversation.memory import ConversationBufferMemory
from langchain.llms.base import LLM
import requests
import pandas as pd
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor
from config import LLM_MODELS, OLLAMA_ENDPOINT, PROVINCIAS_ESPANA, HARDWARE_CONFIG

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
        self.llm = CustomLLM(LLM_MODELS["orquestador"])
        self.memory = ConversationBufferMemory()
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
        self.llm = CustomLLM(LLM_MODELS["base_datos"])

    def generate_query(self, natural_query: str) -> Dict[str, Any]:
        response = self.llm(f"{self.PROMPT}\nConsulta: {natural_query}")
        query = self._extract_sql(response)
        return {
            "query": query,
            "explanation": response,
            "sql_type": self._determine_query_type(query)
        }

    def _extract_sql(self, response: str) -> str:
        # Implementar extracción de SQL del texto
        # Esto dependerá del formato exacto de respuesta del LLM
        return ""

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
        self.llm = CustomLLM(LLM_MODELS["scraping"])

    def plan_scraping(self, url: str) -> Dict[str, Any]:
        analysis = self.llm(f"{self.PROMPT}\nURL: {url}")
        
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