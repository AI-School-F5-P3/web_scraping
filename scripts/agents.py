from langchain.llms.base import LLM
import requests
import re
import json
import os
import unicodedata
import pandas as pd
from typing import List, Dict, Any, Optional, Union
from enum import Enum
from thefuzz import fuzz, process
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.vectorstores import FAISS
from langchain.embeddings import HuggingFaceEmbeddings
from langchain.chains import RetrievalQA
from langchain.prompts import PromptTemplate
from langchain_groq import ChatGroq
from langchain.document_loaders import PyPDFLoader
from config import GROQ_API_KEY, PROVINCIAS_ESPANA, HARDWARE_CONFIG

class QueryType(Enum):
    COUNT = "count"
    TABLE = "table"
    AGGREGATE = "aggregate"
    NON_DB = "non_db"
    FINANCIAL = "financial"  # Query type for financial information from PDF

class QueryContext:
    """Stores context about the query being processed"""
    def __init__(self):
        self.query_type = QueryType.TABLE
        self.has_url_filter = False
        self.has_ecommerce_filter = False
        self.has_social_filter = False
        self.province = None
        self.limit = 10
        self.specified_columns = []

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

class UnifiedAgent:
    """
    Combined agent that handles both database queries and financial information retrieval from PDF.
    """
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
    
    # Financial information related keywords
    FINANCIAL_KEYWORDS = [
        'facturación', 'ingresos', 'beneficio', 'ganancia', 'resultado',
        'empleados', 'trabajadores', 'plantilla', 'sector', 'actividad',
        'financiera', 'financiero', 'económico', 'económica', 'finanzas',
        'balance', 'cuenta de resultados', 'rentabilidad', 'ventas',
        'informe', 'memoria', 'anual', 'trimestral'
    ]
    
    # Términos sensibles que se deben evitar en las respuestas
    BLACKLIST_TERMS = [
        "dni", "pasaporte", "cuenta bancaria", "tarjeta", "clave", "contraseña",
        "personal", "privado", "confidencial", "secreto"
    ]

    # SQL generation prompt
    SQL_PROMPT = """You are a Senior SQL Architect specialized in business analytics. Your task is to generate precise SQL queries for the 'guardarail' database or to determine when a query is not database-related. 

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

    # Financial RAG prompt template adaptado para cualquier empresa
    FINANCIAL_PROMPT_TEMPLATE = """
    Eres un asistente financiero especializado en la información del informe anual de {company_name}. 
    Tu tarea es proporcionar información precisa basada únicamente en el contenido del informe.
    
    Contexto: {context}
    
    Pregunta: {question}
    
    Respuesta: 
    """

    def __init__(self, groq_model: str = None, cache_dir: str = "./cache", embedding_model: str = "all-MiniLM-L6-v2", pdf_paths: dict = None):
        """
        Initializes the unified agent system with PDF-based RAG approach.
        
        Parameters:
        - groq_model: Name of the Groq model to use.
        - cache_dir: Directory for storing cache and vector database.
        - embedding_model: HuggingFace model for generating embeddings.
        - pdf_paths: Dictionary of company identifiers to PDF paths, defaults to just Repsol if None.
        """
        # Ensure groq_model has a default value
        if groq_model is None:
            groq_model = "llama3-70b-8192"  # Default model if none provided
            
        self.llm = CustomLLM(model_name=groq_model)
        self.groq_chat = ChatGroq(
            api_key=GROQ_API_KEY,
            model_name=groq_model
        )
        
        # Setup cache and vector database for financial info
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)
        
        # Initialize embeddings for vector search
        self.embeddings = HuggingFaceEmbeddings(
            model_name=embedding_model,
            model_kwargs={'device': 'cpu'}
        )
        
        # Default to just Repsol if no PDF paths provided
        if pdf_paths is None:
            pdf_paths = {"repsol": "informe-repsol.pdf"}
            
        self.pdf_paths = pdf_paths
        self.company_vectordbs = {}
        self.company_qa_chains = {}
        
        # Setup vector database for each company's financial information
        for company_id, pdf_path in pdf_paths.items():
            vectordb_path = os.path.join(self.cache_dir, f"faiss_index_{company_id}")
            
            # Initialize or load the vector database for this company
            if os.path.exists(os.path.join(vectordb_path, "index.faiss")):
                print(f"Loading existing vector database for {company_id}...")
                vectordb = FAISS.load_local(vectordb_path, self.embeddings)
            else:
                print(f"Creating new vector database for {company_id} from {pdf_path}...")
                if os.path.exists(pdf_path):
                    vectordb = self._create_vector_db_from_pdf(pdf_path, vectordb_path)
                else:
                    print(f"Warning: PDF file {pdf_path} not found. Skipping {company_id}")
                    continue
            
            self.company_vectordbs[company_id] = vectordb
            
            # Initialize prompt template for this company's financial RAG
            # Extract company name from identifier, defaulting to the ID if necessary
            company_name = company_id.capitalize()
            rag_prompt = PromptTemplate(
                template=self.FINANCIAL_PROMPT_TEMPLATE,
                input_variables=["question", "context", "company_name"]
            )
            
            # Create RAG chain for this company
            qa_chain = RetrievalQA.from_chain_type(
                llm=self.groq_chat,
                chain_type="stuff",
                retriever=vectordb.as_retriever(search_kwargs={"k": 4}),
                return_source_documents=True,
                chain_type_kwargs={"prompt": rag_prompt}
            )
            
            self.company_qa_chains[company_id] = qa_chain

    def _create_vector_db_from_pdf(self, pdf_path: str, vectordb_path: str) -> FAISS:
        """
        Creates a vector database from a PDF file.
        """
        try:
            # Load PDF
            loader = PyPDFLoader(pdf_path)
            documents = loader.load()
            
            # Split into chunks
            text_splitter = RecursiveCharacterTextSplitter(
                chunk_size=1000,
                chunk_overlap=100,
                separators=["\n\n", "\n", ".", " ", ""]
            )
            chunks = text_splitter.split_documents(documents)
            
            # Create vector store
            vectordb = FAISS.from_documents(chunks, self.embeddings)
            
            # Save vector store
            os.makedirs(vectordb_path, exist_ok=True)
            vectordb.save_local(vectordb_path)
            
            print(f"Created vector database with {len(chunks)} chunks from {pdf_path}")
            return vectordb
            
        except Exception as e:
            print(f"Error creating vector database: {str(e)}")
            raise

    def add_company_pdf(self, company_id: str, pdf_path: str, company_name: str = None):
        """
        Adds a new company's financial information to the system.
        
        Parameters:
        - company_id: Unique identifier for the company (e.g., "repsol", "telefonica")
        - pdf_path: Path to the PDF file containing the company's financial report
        - company_name: Display name of the company (defaults to capitalized company_id)
        """
        if not company_name:
            company_name = company_id.capitalize()
            
        if company_id in self.company_vectordbs:
            print(f"Company {company_id} already exists. Updating information...")
            
        vectordb_path = os.path.join(self.cache_dir, f"faiss_index_{company_id}")
        
        if os.path.exists(pdf_path):
            vectordb = self._create_vector_db_from_pdf(pdf_path, vectordb_path)
            self.company_vectordbs[company_id] = vectordb
            
            # Initialize prompt template for this company's financial RAG
            rag_prompt = PromptTemplate(
                template=self.FINANCIAL_PROMPT_TEMPLATE,
                input_variables=["question", "context", "company_name"]
            )
            
            # Create RAG chain for this company
            qa_chain = RetrievalQA.from_chain_type(
                llm=self.groq_chat,
                chain_type="stuff",
                retriever=vectordb.as_retriever(search_kwargs={"k": 4}),
                return_source_documents=True,
                chain_type_kwargs={"prompt": rag_prompt}
            )
            
            self.company_qa_chains[company_id] = qa_chain
            self.pdf_paths[company_id] = pdf_path
            
            print(f"Successfully added {company_name} ({company_id}) to the system")
        else:
            print(f"Error: PDF file {pdf_path} not found.")

    def process_query(self, query: str) -> Dict[str, Any]:
        """
        Main entry point for processing any user query.
        Determines if it's a database query or financial information request.
        """
        # Analyze the query first
        ctx = self.analyze_query(query)
        
        # Handle financial queries
        if ctx.query_type == QueryType.FINANCIAL:
            # Extract company name from query if possible
            company_id, company_name = self.extract_company_from_query(query)
            if company_id and company_id in self.company_qa_chains:
                return self.handle_financial_query(query, company_id, company_name)
            elif len(self.company_qa_chains) == 1:
                # If only one company is loaded, use that one
                company_id = next(iter(self.company_qa_chains))
                return self.handle_financial_query(query, company_id)
            elif "repsol" in self.company_qa_chains:
                # Fallback to Repsol if available
                return self.handle_financial_query(query, "repsol")
            else:
                # No company specified and multiple companies available
                return {
                    "query": None,
                    "explanation": "Por favor, especifica la empresa sobre la que quieres información financiera.",
                    "query_type": "financial",
                    "error": "Empresa no especificada"
                }
        
        # Handle database queries
        return self.generate_query(query)
    
    def extract_company_from_query(self, query: str) -> tuple:
        """
        Attempts to extract a company name from the query.
        Returns a tuple of (company_id, display_name) if found, or (None, None) if not.
        """
        query_lower = self.remove_accents(query.lower())
        
        # Check for company names in the query
        for company_id in self.company_qa_chains.keys():
            if company_id.lower() in query_lower:
                return company_id, company_id.capitalize()
        
        # Check if the query mentions a NIF, COD_INFOTEL, or RAZON_SOCIAL that we can use
        # This would require querying the database, which we'd implement here
        # For now, we'll just return None, None
        
        return None, None
        
    def analyze_query(self, query: str) -> QueryContext:
        """Analyzes the natural language query to extract key information"""
        query_normalized = self.remove_accents(query.lower())
        ctx = QueryContext()

        # Check if it's a financial query
        if any(keyword in query_normalized for keyword in self.FINANCIAL_KEYWORDS):
            # Look for company-specific keywords
            for company_id in self.company_qa_chains.keys():
                if company_id.lower() in query_normalized:
                    ctx.query_type = QueryType.FINANCIAL
                    return ctx
            
            # If any company is mentioned (even without specific financial keywords)
            for company_id in self.company_qa_chains.keys():
                if company_id.lower() in query_normalized:
                    ctx.query_type = QueryType.FINANCIAL
                    return ctx
            
            # If financial keywords are present without specific company
            # and we have at least one company loaded
            if len(self.company_qa_chains) > 0:
                ctx.query_type = QueryType.FINANCIAL
                return ctx

        # Check if query is non-database related
        if any(keyword in query_normalized for keyword in self.NON_DB_KEYWORDS):
            ctx.query_type = QueryType.NON_DB
            return ctx

        # Check if query involves social media
        if any(keyword in query_normalized for keyword in self.SOCIAL_KEYWORDS):
            ctx.has_social_filter = True

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
    
    def remove_accents(self, text: str) -> str:
        """Removes accents from a string"""
        return ''.join(
            c for c in unicodedata.normalize('NFD', text)
            if unicodedata.category(c) != 'Mn'
        )
    
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
                if ctx.has_social_filter:
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
                
            # Handle financial queries
            if ctx.query_type == QueryType.FINANCIAL:
                company_id, company_name = self.extract_company_from_query(natural_query)
                return self.handle_financial_query(natural_query, company_id, company_name)

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

    def build_where_clauses(self, ctx: QueryContext) -> str:
        """Builds WHERE clauses for SQL queries based on context"""
        clauses = []
        
        if ctx.province:
            clauses.append(f"nom_provincia ILIKE '%{ctx.province}%'")
            
        if ctx.has_url_filter:
            clauses.append("url IS NOT NULL AND LENGTH(TRIM(url)) > 0 AND (url ILIKE 'http%' OR url ILIKE 'www.%')")
            
        if ctx.has_ecommerce_filter:
            clauses.append("e_commerce IS NOT NULL AND e_commerce = TRUE")
        
        if clauses:
            return "WHERE " + " AND ".join(clauses)
        else:
            return ""
            
    def generate_explanation(self, ctx: QueryContext) -> str:
        """Generates a human-readable explanation of the query"""
        explanation = ""
        
        if ctx.query_type == QueryType.AGGREGATE:
            explanation = "Consulta de análisis de proporción"
        elif ctx.query_type == QueryType.COUNT:
            explanation = "Consulta de conteo"
        else:
            explanation = "Consulta de datos tabulares"
            
        if ctx.province:
            explanation += f" para empresas en {ctx.province}"
            
        filters = []
        if ctx.has_url_filter:
            filters.append("con sitio web")
        if ctx.has_ecommerce_filter:
            filters.append("con e-commerce")
            
        if filters:
            explanation += " " + " y ".join(filters)
            
        return explanation

    def handle_financial_query(self, query: str, company_id: str = None, company_name: str = None) -> Dict[str, Any]:
        """
        Handles queries about financial information from company PDF reports
        
        Parameters:
        - query: The user's query
        - company_id: The ID of the company to query (e.g., "repsol")
        - company_name: Display name of the company (defaults to capitalized company_id)
        """
        try:
            # If no company specified but we have multiple, let user know
            if not company_id and len(self.company_qa_chains) > 1:
                companies = ", ".join(self.company_qa_chains.keys())
                return {
                    "query": None,
                    "explanation": f"Por favor, especifica la empresa sobre la que deseas información. Tenemos informes de: {companies}",
                    "query_type": "financial",
                    "error": "Empresa no especificada"
                }
                
            # If no company specified but we have only one, use that
            if not company_id and len(self.company_qa_chains) == 1:
                company_id = next(iter(self.company_qa_chains))
                
            # If still no company or company not found, return error
            if not company_id or company_id not in self.company_qa_chains:
                available = ", ".join(self.company_qa_chains.keys()) if self.company_qa_chains else "ninguna empresa"
                return {
                    "query": None,
                    "explanation": f"No se encontró información para la empresa solicitada. Empresas disponibles: {available}",
                    "query_type": "financial",
                    "error": "Empresa no encontrada"
                }
                
            # Set display name if not provided
            if not company_name:
                company_name = company_id.capitalize()
                
            # Run the RAG query for the specified company
            qa_chain = self.company_qa_chains[company_id]
            
            # Include company name in the prompt variables
            response = qa_chain({"query": query, "company_name": company_name})
            
            # Extract the answer and source documents
            answer = response["result"]
            source_docs = response.get("source_documents", [])
            
            # Format the answer with source information
            formatted_answer = answer
            
            # Add page references if available
            if source_docs:
                page_refs = set()
                for doc in source_docs:
                    if hasattr(doc, "metadata") and "page" in doc.metadata:
                        page_refs.add(doc.metadata["page"])
                
                if page_refs:
                    formatted_answer += f"\n\nFuente: Informe {company_name}, páginas {', '.join(map(str, sorted(page_refs)))}"
            
            return {
                "query": None,
                "explanation": formatted_answer,
                "query_type": "financial",
                "error": None
            }
            
        except Exception as e:
            print(f"Error in financial query: {str(e)}")
            return {
                "query": None,
                "explanation": f"Error al procesar la consulta financiera: {str(e)}",
                "query_type": "financial",
                "error": str(e)
            }