# rag_system.py

import requests
from bs4 import BeautifulSoup
import re
import json
import os
import unicodedata
import time  # Para limitar la frecuencia de requests a Google
from dataclasses import dataclass
from typing import Optional, Dict, Any
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.vectorstores import FAISS
from langchain.embeddings import HuggingFaceEmbeddings
from langchain.chains import RetrievalQA
from langchain.prompts import PromptTemplate
from config import GROQ_API_KEY

@dataclass
class CompanyFinancialInfo:
    name: str
    nif: Optional[str] = None
    sector: Optional[str] = None
    revenue: Optional[str] = None
    profit: Optional[str] = None
    employees: Optional[str] = None
    year: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "nif": self.nif,
            "sector": self.sector,
            "revenue": self.revenue,
            "profit": self.profit,
            "employees": self.employees,
            "year": self.year,
        }

class FinancialRAGSystem:
    """
    Sistema RAG para consultas financieras sobre empresas españolas.
    Este módulo extrae información pública (facturación, empleados, sector, 
    cotización en bolsa, etc.) de fuentes confiables mediante web scraping.
    
    La comunicación con la base de datos PostgreSQL (para consultas SQL) se gestiona 
    en otro módulo (agents.py), por lo que este sistema se enfoca únicamente en el 
    proceso RAG.
    """
    
    # Dominios de confianza para la búsqueda
    TRUSTED_DOMAINS = [
        "einforma.com",
        "axesor.es",
        "empresite.eleconomista.es",
        "expansion.com/empresas",
        "infocif.es"
    ]
    
    # Términos sensibles que se deben evitar en las respuestas
    BLACKLIST_TERMS = [
        "dni", "pasaporte", "cuenta bancaria", "tarjeta", "clave", "contraseña",
        "personal", "privado", "confidencial", "secreto"
    ]
    
    def __init__(self, groq_model: str, cache_dir: str = "./cache", embedding_model: str = "all-MiniLM-L6-v2"):
        """
        Inicializa el sistema RAG.
        
        Parámetros:
        - groq_model: Nombre del modelo Groq a utilizar (seleccionable en la interfaz).
        - cache_dir: Directorio para almacenar la caché y la base de datos vectorial.
        - embedding_model: Modelo de HuggingFace para generar embeddings.
        """
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)
        self.setup_vector_db()
        
        # Se utiliza HuggingFace para transformar el texto en embeddings,
        # y FAISS para indexarlos y permitir búsquedas semánticas.
        self.embeddings = HuggingFaceEmbeddings(
            model_name=embedding_model,
            model_kwargs={'device': 'cpu'}
        )
        
        # Inicializamos el LLM de Groq con el modelo seleccionado
        from langchain_groq import ChatGroq
        self.llm = ChatGroq(
            api_key=GROQ_API_KEY,
            model_name=groq_model
        )
        
        # Plantilla del prompt para el sistema RAG (enfocado en información financiera)
        self.rag_prompt = PromptTemplate(
            template="""
            Eres un asistente financiero para empresas españolas. Tu tarea es proveer información 
            precisa y respaldada sobre empresas basada en datos públicos disponibles, tales como 
            facturación, número de empleados, sector, cotización en bolsa, etc. 
            Utiliza únicamente el contexto proporcionado.
            
            Pregunta: {question}
            
            Respuesta:
            """,
            input_variables=["question", "context"]
        )
    
    def setup_vector_db(self):
        """Configura o carga la base de datos vectorial."""
        self.vectordb_path = os.path.join(self.cache_dir, "faiss_index")
        if os.path.exists(os.path.join(self.vectordb_path, "index.faiss")):
            self.vectordb = FAISS.load_local(self.vectordb_path, self.embeddings)
        else:
            self.vectordb = None
    
    def search_company_info(self, company_name: str) -> Dict[str, Any]:
        """
        Busca en línea información financiera pública de la empresa.
        Se utiliza una caché para evitar búsquedas repetitivas.
        """
        try:
            cache_file = os.path.join(self.cache_dir, f"{self.sanitize_filename(company_name)}.json")
            if os.path.exists(cache_file):
                with open(cache_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            
            # Normalize company name for better searching
            normalized_name = company_name.strip().lower()
            normalized_name = re.sub(r'\b(s\.a\.|s\.l\.|s\s*\.?\s*a\s*\.?|s\s*\.?\s*l\s*\.?)$', '', normalized_name).strip()
            
            company_info = self._search_online(normalized_name)
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(company_info.to_dict(), f, ensure_ascii=False, indent=2)
            return company_info.to_dict()
        except Exception as e:
            print(f"Error searching company info: {str(e)}")
            # Return minimal info to prevent errors
            return {"name": company_name, "error": str(e)}
    
    def _search_online(self, company_name: str) -> CompanyFinancialInfo:
        """
        Realiza una búsqueda en línea en dominios confiables para extraer información
        financiera pública. Se respeta un límite de requests (pausando 1 segundo entre cada búsqueda).
        """
        company_info = CompanyFinancialInfo(name=company_name)
        for domain in self.TRUSTED_DOMAINS:
            try:
                time.sleep(1)  # Respeta el límite de requests de Google
                search_url = f"https://www.google.com/search?q=site:{domain}+{company_name}+información+financiera"
                headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
                response = requests.get(search_url, headers=headers, timeout=10)
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    for result in soup.select('a'):
                        href = result.get('href', '')
                        if domain in href and 'google' not in href:
                            url_matches = re.findall(r'https?://[^\s&]+', href)
                            if not url_matches:
                                continue
                            url = url_matches[0]
                            if any(td in url for td in self.TRUSTED_DOMAINS):
                                page_content = self._fetch_page_safely(url)
                                if page_content:
                                    safe_content = self._sanitize_content(page_content)
                                    extracted_info = self._extract_financial_data(safe_content, company_name)
                                    # Actualizamos la información financiera si se encontró
                                    if extracted_info.nif:
                                        company_info.nif = extracted_info.nif
                                    if extracted_info.sector:
                                        company_info.sector = extracted_info.sector
                                    if extracted_info.revenue:
                                        company_info.revenue = extracted_info.revenue
                                    if extracted_info.profit:
                                        company_info.profit = extracted_info.profit
                                    if extracted_info.employees:
                                        company_info.employees = extracted_info.employees
                                    if extracted_info.year:
                                        company_info.year = extracted_info.year
                                    # Si se obtuvo información relevante, se detiene la búsqueda
                                    if company_info.revenue or company_info.profit or company_info.employees:
                                        break
            except Exception as e:
                print(f"Error buscando en {domain}: {str(e)}")
                continue
        return company_info
    
    def _fetch_page_safely(self, url: str) -> Optional[str]:
        """Descarga la página comprobando que el dominio sea confiable."""
        try:
            if not any(domain in url for domain in self.TRUSTED_DOMAINS):
                return None
            headers = {'User-Agent': 'Mozilla/5.0'}
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                return response.text
        except Exception as e:
            print(f"Error al obtener {url}: {str(e)}")
        return None
    
    def _sanitize_content(self, content: str) -> str:
        """Elimina información sensible del contenido descargado."""
        soup = BeautifulSoup(content, 'html.parser')
        text = soup.get_text(separator=' ', strip=True)
        for term in self.BLACKLIST_TERMS:
            pattern = re.compile(r'.{0,50}' + term + r'.{0,50}', re.IGNORECASE)
            text = pattern.sub('[INFORMACIÓN PROTEGIDA]', text)
        text = re.sub(r'\b\d{8}[A-Z]\b', '[ID PROTEGIDO]', text)
        text = re.sub(r'\b\d{9}\b', '[TELÉFONO]', text)
        text = re.sub(r'\S+@\S+\.\S+', '[EMAIL]', text)
        return text
    
    def _extract_financial_data(self, content: str, company_name: str) -> CompanyFinancialInfo:
        """
        Extrae información financiera pública de la empresa (facturación, beneficio,
        número de empleados, sector y año de ejercicio) a partir del contenido.
        """
        info = CompanyFinancialInfo(name=company_name)
        
        # Extraer NIF
        nif_pattern = re.compile(r'NIF:?\s*([A-Z0-9]{9})', re.IGNORECASE)
        nif_match = nif_pattern.search(content)
        if nif_match:
            info.nif = nif_match.group(1)
        
        # Extraer sector (o CNAE)
        sector_patterns = [
            re.compile(r'Sector:?\s*([^\n\.]+)', re.IGNORECASE),
            re.compile(r'CNAE:?\s*([^\n\.]+)', re.IGNORECASE)
        ]
        for pattern in sector_patterns:
            match = pattern.search(content)
            if match:
                info.sector = match.group(1).strip()
                break
        
        # Extraer facturación (ingresos)
        revenue_patterns = [
            re.compile(r'facturación:?\s*([\d\.,]+)\s*(?:€|EUR|euros|mil euros|millones)', re.IGNORECASE),
            re.compile(r'ingresos:?\s*([\d\.,]+)\s*(?:€|EUR|euros|mil euros|millones)', re.IGNORECASE)
        ]
        for pattern in revenue_patterns:
            match = pattern.search(content)
            if match:
                info.revenue = match.group(1).strip()
                year_pattern = re.compile(r'(?:en|de|del)?\s*(?:año|ejercicio)?\s*(20\d{2})')
                year_match = year_pattern.search(content[match.start()-50:match.start()+100])
                if year_match:
                    info.year = year_match.group(1)
                break
        
        # Extraer beneficio o resultado
        profit_patterns = [
            re.compile(r'resultado:?\s*([\d\.,\-]+)\s*(?:€|EUR|euros|mil euros|millones)', re.IGNORECASE),
            re.compile(r'beneficio:?\s*([\d\.,\-]+)\s*(?:€|EUR|euros|mil euros|millones)', re.IGNORECASE)
        ]
        for pattern in profit_patterns:
            match = pattern.search(content)
            if match:
                info.profit = match.group(1).strip()
                if not info.year:
                    year_pattern = re.compile(r'(?:en|de|del)?\s*(?:año|ejercicio)?\s*(20\d{2})')
                    year_match = year_pattern.search(content[match.start()-50:match.start()+100])
                    if year_match:
                        info.year = year_match.group(1)
                break
        
        # Extraer número de empleados
        employee_patterns = [
            re.compile(r'empleados:?\s*(\d+)', re.IGNORECASE),
            re.compile(r'trabajadores:?\s*(\d+)', re.IGNORECASE),
            re.compile(r'plantilla:?\s*(\d+)', re.IGNORECASE)
        ]
        for pattern in employee_patterns:
            match = pattern.search(content)
            if match:
                info.employees = match.group(1).strip()
                break
        
        return info
    
    def _add_to_vector_db(self, content: str, company_name: str, url: str):
        """
        Agrega el contenido obtenido a la base de datos vectorial para consultas RAG.
        Se utiliza un divisor de texto para fragmentar el contenido en chunks.
        """
        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=1000,
            chunk_overlap=200
        )
        chunks = text_splitter.split_text(content)
        texts_with_metadata = [
            {"content": chunk, "company": company_name, "source": url}
            for chunk in chunks
        ]
        if self.vectordb is None:
            self.vectordb = FAISS.from_texts(
                [t["content"] for t in texts_with_metadata],
                self.embeddings,
                metadatas=texts_with_metadata
            )
            self.vectordb.save_local(self.vectordb_path)
        else:
            self.vectordb.add_texts(
                [t["content"] for t in texts_with_metadata],
                metadatas=texts_with_metadata
            )
            self.vectordb.save_local(self.vectordb_path)
    
    def answer_financial_question(self, company_name: str, question: str) -> str:
        """
        Responde a una consulta financiera sobre la empresa utilizando el 
        enfoque Retrieval-Augmented Generation (RAG). Se filtra la información
        relevante de la base vectorial y se pasa al LLM.
        """
        if self.vectordb is None:
            return "No tengo información financiera disponible. Por favor, realiza primero una búsqueda de la empresa."
        
        retriever = self.vectordb.as_retriever(
            search_kwargs={"k": 3, "filter": {"company": company_name}}
        )
        qa_chain = RetrievalQA.from_chain_type(
            llm=self.llm,
            chain_type="stuff",
            retriever=retriever,
            chain_type_kwargs={"prompt": self.rag_prompt}
        )
        try:
            result = qa_chain.invoke({"question": question})
            answer = result.get('result', '')
            for term in self.BLACKLIST_TERMS:
                if term in answer.lower():
                    answer = answer.replace(term, "[INFORMACIÓN PROTEGIDA]")
            return answer
        except Exception as e:
            return f"No pude procesar esta consulta: {str(e)}"
    
    @staticmethod
    def sanitize_filename(filename: str) -> str:
        """Sanitiza el nombre para que sea seguro en el sistema de archivos."""
        filename = ''.join(c for c in unicodedata.normalize('NFD', filename)
                           if unicodedata.category(c) != 'Mn')
        return re.sub(r'[^\w\-_\. ]', '_', filename)
