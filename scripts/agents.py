# agents.py

import logging
from queue import Queue
import re
from threading import Thread
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from langchain.agents import Tool, initialize_agent
from langchain.chains.conversation.memory import ConversationBufferMemory
from langchain.llms.base import LLM
import requests
import pandas as pd
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor
from streamlit import json
from config import LLM_MODELS, OLLAMA_ENDPOINT, PROVINCIAS_ESPANA, HARDWARE_CONFIG
from database import DatabaseManager

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
    def process_batch(self, batch_id, max_urls=100, num_threads=5):
        """Process a batch of URLs from the database"""
        db_manager = DatabaseManager()
        scraper = ScrapingAgent()  # Using the optimized scraper
        
        # Get URLs for processing
        urls_df = db_manager.get_urls_for_scraping(batch_id, limit=max_urls)
        
        if urls_df is None or len(urls_df) == 0:
            return {"status": "error", "message": "No URLs to process"}
        
        # Setup multi-threading
        task_queue = Queue()
        results = []
        
        # Queue tasks
        for idx, row in urls_df.iterrows():
            task_queue.put((row['id'], row['cod_infotel'], row['url']))
        
        # Define worker function
        def worker():
            while not task_queue.empty():
                try:
                    id, cod_infotel, url = task_queue.get(timeout=1)
                    self.logger.info(f"Processing URL: {url} for company: {cod_infotel}")
                    
                    # Get scraping result
                    result = scraper.scrape_website(url)
                    result['url'] = url
                    results.append(result)
                    
                except Exception as e:
                    self.logger.error(f"Error processing URL: {e}")
                finally:
                    task_queue.task_done()
        
        # Start workers
        threads = []
        for _ in range(min(num_threads, urls_df.shape[0])):
            thread = Thread(target=worker)
            thread.daemon = True
            thread.start()
            threads.append(thread)
        
        # Wait for completion
        for thread in threads:
            thread.join()
        
        # Update database with results
        if results:
            update_result = db_manager.update_scraping_results(results, batch_id)
            return {
                "status": "success",
                "processed": len(results),
                "errors": len(urls_df) - len(results),
                "database_update": update_result
            }
        else:
            return {"status": "error", "message": "No results generated"}

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
        self.llm = CustomLLM(LLM_MODELS["base_datos"])

    def generate_query(self, natural_query: str) -> Dict[str, Any]:
        """
        Generates SQL query from natural language, with improved handling of aggregation queries.
        """
        full_prompt = f"{self.PROMPT}\nConsulta: {natural_query}\nGenera la consulta SQL:"
        response = self.llm.invoke(full_prompt)
        
        # Check if this is a counting/aggregation query
        is_count_query = any(word in natural_query.lower() for word in [
            "cuántas", "cuantas", "número de", "numero de", "total de", "cuenta"
        ])
        
        # Use regex to extract a SQL query
        sql_match = re.search(r'SELECT.*?;', response, re.DOTALL | re.IGNORECASE)
        
        if sql_match:
            query = sql_match.group(0)
            # For count queries, ensure we're using COUNT
            if is_count_query and "COUNT" not in query.upper():
                # Convert to COUNT query
                base_conditions = re.search(r'WHERE.*?(?:LIMIT|;|$)', query, re.DOTALL | re.IGNORECASE)
                conditions = base_conditions.group(0) if base_conditions else ";"
                if conditions.upper().endswith("LIMIT"):
                    conditions = conditions[:conditions.upper().find("LIMIT")] + ";"
                query = f"SELECT COUNT(*) as total FROM sociedades {conditions}"
        else:
            # Fallback query
            if is_count_query:
                query = "SELECT COUNT(*) as total FROM sociedades;"
            else:
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
    PROMPT = """Eres un Ingeniero de Web Scraping Elite especializado en análisis empresarial español.

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
        # Usa CustomLLM igual que las otras clases
        self.llm = CustomLLM(LLM_MODELS["scraping"])
        self.logger = logging.getLogger(__name__)
        self.logger.info("Initializing Scraping Agent with Langchain")
        
        # Crea herramientas para Langchain
        self.tools = [
            Tool(
                name="fetch_webpage",
                func=self.fetch_webpage,
                description="Fetches a webpage content given a URL"
            ),
            Tool(
                name="extract_data",
                func=self.extract_data,
                description="Extracts business data from HTML content"
            )
        ]
        
        # Inicializa el agente de Langchain
        self.memory = ConversationBufferMemory(return_messages=True)
        self.agent = initialize_agent(
            self.tools,
            self.llm,
            agent="conversational-react-description",
            memory=self.memory,
            verbose=HARDWARE_CONFIG.get("verbose", False)
        )

    def fetch_webpage(self, url):
        """Tool: Fetches a webpage and returns its content"""
        if not url or not isinstance(url, str) or not url.strip():
            return {'error': "Invalid URL"}
        
        url = url.strip()
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36',
                'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
            }
            response = requests.get(url, headers=headers, timeout=15)
            
            if response.status_code != 200:
                return {'error': f"Failed with status code: {response.status_code}"}
            
            return {'content': response.text, 'url': url}
            
        except requests.exceptions.Timeout:
            return {'error': "Timeout error"}
        except requests.exceptions.ConnectionError:
            return {'error': "Connection error"}
        except Exception as e:
            return {'error': f"Error: {str(e)}"}
    
    def extract_data(self, result):
        """Tool: Extracts business data from HTML content"""
        if 'error' in result:
            return {
                'url_exists': False, 
                'url_status': 'error', 
                'url_status_mensaje': result['error']
            }
        
        html_content = result['content']
        url = result['url']
        
        # Parse with BeautifulSoup to simplify the HTML
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Prepare a simplified version of the HTML for the LLM
        simple_html = str(soup.body) if soup.body else str(soup)
        simple_html = re.sub(r'<script.*?</script>', '', simple_html, flags=re.DOTALL)
        simple_html = re.sub(r'<style.*?</style>', '', simple_html, flags=re.DOTALL)
        simple_html = re.sub(r'<!--.*?-->', '', simple_html, flags=re.DOTALL)
        
        # Truncate if too long to fit in context window
        max_context = HARDWARE_CONFIG.get("max_context_length", 10000)
        if len(simple_html) > max_context:  
            simple_html = simple_html[:max_context] + "..."
        
        # Create prompt for the LLM
        prompt = f"""
{self.PROMPT}

Analyze this HTML from a Spanish company website to extract specific information.

URL: {url}

HTML (truncated): 
{simple_html}

TASK:
1. Extract up to 3 phone numbers if present (Spanish format).
2. Extract social media links for: LinkedIn, Facebook, Instagram, Twitter.
3. Determine if this is an e-commerce website (has online store/shopping functionality).

Output your findings ONLY in JSON format:
{{
  "phones": ["phone1", "phone2", "phone3"],
  "social_media": {{
    "facebook": "url or null",
    "twitter": "url or null",
    "linkedin": "url or null",
    "instagram": "url or null"
  }},
  "is_ecommerce": true/false,
  "reasoning": "Brief explanation of your e-commerce determination"
}}

For URLs that are relative, use {url} as the base.
"""

        # Use Langchain LLM for extraction
        llm_response = self.llm(prompt)
        
        # Process LLM response
        if llm_response:
            try:
                # Find the JSON block using regex
                json_match = re.search(r'\{[\s\S]*\}', llm_response)
                if json_match:
                    extracted_data = json.loads(json_match.group(0))
                    
                    # Add additional metadata
                    result = {
                        'url_exists': True,
                        'url_limpia': url,
                        'url_status': 'success',
                        'url_status_mensaje': 'Successfully scraped with LLM',
                        'phones': extracted_data.get('phones', []),
                        'social_media': extracted_data.get('social_media', {}),
                        'is_ecommerce': extracted_data.get('is_ecommerce', False)
                    }
                    
                    # Remove reasoning field 
                    if 'reasoning' in result:
                        del result['reasoning']
                    
                    return result
            except Exception as e:
                self.logger.warning(f"Failed to parse LLM response: {str(e)}")
        
        # Fallback to manual extraction if LLM fails
        self.logger.info(f"Falling back to manual extraction for {url}")
        return self.extract_manually(html_content, url)
    
    def extract_manually(self, html_content, url):
        """Extract information manually as a fallback method"""
        soup = BeautifulSoup(html_content, 'html.parser')
        base_url = urlparse(url).scheme + '://' + urlparse(url).netloc
        
        # Extract phones with regex
        phone_pattern = re.compile(r'(\+?\d{1,3}[-.\s]?)?(\d{2,4}[-.\s]?)(\d{2,4}[-.\s]?){1,3}\d{2,4}')
        phones = []
        phone_matches = phone_pattern.findall(html_content)
        for match in phone_matches:
            if isinstance(match, tuple):
                phone = ''.join(match).strip()
            else:
                phone = match.strip()
            phone = re.sub(r'[\s.-]', '', phone)
            if len(phone) >= 9:  # Basic validation for Spanish phone numbers
                phones.append(phone)
        phones = list(set(phones))[:3]  # Take up to 3 unique phones
        
        # Extract social media
        social_links = {
            'facebook': None,
            'twitter': None,
            'linkedin': None,
            'instagram': None
        }
        
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href'].lower()
            
            # Make URL absolute if needed
            if not href.startswith(('http://', 'https://')):
                href = urljoin(base_url, href)
            
            # Check for each social network
            if 'facebook.com' in href:
                social_links['facebook'] = href
            elif 'twitter.com' in href or 'x.com' in href:
                social_links['twitter'] = href
            elif 'linkedin.com' in href:
                social_links['linkedin'] = href
            elif 'instagram.com' in href:
                social_links['instagram'] = href
        
        # Basic e-commerce detection
        has_ecommerce = False
        ecommerce_indicators = [
            r'\b(tienda|shop|cart|carrito|comprar|buy|checkout|cesta|basket)\b',
            r'(payment|pago|paypal|stripe|redsys|visa|mastercard)',
            r'(producto|product|catalogo|catalogue|precio|price)'
        ]
        
        for pattern in ecommerce_indicators:
            if re.search(pattern, html_content, re.IGNORECASE):
                has_ecommerce = True
                break
        
        # Look for potential e-commerce platforms
        ecommerce_platforms = [
            'woocommerce', 'shopify', 'magento', 'prestashop', 'opencart',
            'commercetools', 'salesforce commerce', 'bigcommerce'
        ]
        for platform in ecommerce_platforms:
            if platform in html_content.lower():
                has_ecommerce = True
                break
        
        # Prepare cleaned url
        clean_url = url
        if clean_url.startswith('http://'):
            alternative = 'https://' + clean_url[7:]
        else:
            alternative = 'http://' + clean_url[8:]
            
        return {
            'url_exists': True,
            'url_limpia': clean_url,
            'url_status': 'success',
            'url_status_mensaje': 'Extracted manually',
            'phones': phones,
            'social_media': social_links,
            'is_ecommerce': has_ecommerce,
            'alternative_url': alternative
        }

    def scrape_website(self, url):
        """Main method to scrape website information using Langchain agent"""
        # Use the agent to orchestrate the scraping process
        fetched = self.fetch_webpage(url)
        if 'error' in fetched:
            return {
                'url_exists': False, 
                'url_status': 'error', 
                'url_status_mensaje': fetched['error']
            }
        return self.extract_data(fetched)
    
    def plan_scraping(self, url):
        """Maintain compatibility with your original ScrapingAgent interface"""
        # Let Langchain agent plan the scraping process
        prompt = f"""
        Create a scraping plan for the URL: {url}
        
        REQUIREMENTS:
        1. Determine if the site is static or dynamic
        2. Specify resources needed (memory, processing)
        3. List steps in the scraping process
        
        Format the output as a JSON object with these fields:
        - strategy: "static" or "dynamic"
        - steps: list of steps to execute
        - estimated_resources: dictionary with resource estimates
        """
        
        response = self.llm(prompt)
        
        # Try to parse as JSON
        try:
            json_match = re.search(r'\{[\s\S]*\}', response)
            if json_match:
                return json.loads(json_match.group(0))
        except:
            pass
        
        # Fallback to a default plan
        return {
            "strategy": "static",
            "steps": [
                "1. Fetch webpage content",
                "2. Extract phones and social media",
                "3. Analyze for e-commerce indicators"
            ],
            "estimated_resources": {
                "cpu_intensive": False,
                "gpu_needed": False,
                "memory_required": "low"
            }
        }
        
        
      

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