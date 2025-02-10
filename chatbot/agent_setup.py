# chatbot/agent_setup.py
from typing import Dict, Any, Optional, Callable, List
from pydantic import BaseModel, Field
from crewai import Agent, Crew, Process, Task
from chatbot.query_executor import QueryExecutor
from config import Config
import streamlit as st
from chatbot.sql_generator import SQLGenerator
from datetime import datetime
from langchain_community.llms import Ollama  # Para modelos locales
from langchain_openai import ChatOpenAI     # Para OpenAI
from langchain.tools import tool
import time
import logging
logger = logging.getLogger(__name__)

def get_llm():
    """Get the appropriate LLM based on provider selection"""
    if st.session_state.llm_provider == "DeepSeek":
        # Use langchain's built-in Ollama wrapper instead of direct Ollama
        return Ollama(
            model="deepseek-r1",
            base_url=Config.OLLAMA_BASE_URL,
            temperature=0.7,
            stop=["\n"]  # Add stop sequence
        )
    else:  # OpenAI
        return ChatOpenAI(
            model="gpt-4",
            api_key=Config.OPENAI_API_KEY,
            temperature=0.7
        )

class DatabaseSearchOutput(BaseModel):
    results: str
    query: str
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())

class AnalysisOutput(BaseModel):
    analysis: str = Field(..., description="Analysis of the data")
    insights: List[str] = Field(default_factory=list, description="Key insights extracted")
    recommendations: List[str] = Field(default_factory=list, description="Recommendations based on analysis")

class DatabaseTool:
    def __init__(self):
        self.name = "database_search"
        self.description = "Searches the company database for relevant information based on the query"
        self.query_executor = QueryExecutor()
        self.sql_generator = SQLGenerator()
        
    @tool
    def database_search(self, query: str) -> str:
        """Execute database search and return results"""
        try:
            sql_query = self.sql_generator.generate_sql(query)
            if self.query_executor.validate_query(sql_query):
                df = self.query_executor.execute_query(sql_query)
                return df.to_string()
            return "Invalid query"
        except Exception as e:
            return f"Error: {str(e)}"
        
    def func(self, query: str) -> str:
        """Execute database search and return results"""
        try:
            # Convert natural language to SQL using SQLGenerator
            sql_query = self.sql_generator.generate_sql(query)
            
            # Validate the generated query
            if self.query_executor.validate_query(sql_query):
                df = self.query_executor.execute_query(sql_query)
                return df.to_string()
            return "Invalid query"
        except Exception as e:
            return f"Error executing query: {str(e)}"

class QueryResponse(BaseModel):
    description: str = Field(..., description="Query description")
    response: str = Field(..., description="Generated response")
    data: Optional[Dict[str, Any]] = Field(None, description="Additional data")

class ScrapingAgent:
    def __init__(self):
        self.llm = get_llm()
        self.db_tool = DatabaseTool()
        self.agents = self.create_agents()  # Crear agentes al inicializar
        self.tasks = []  # Tareas se crearán dinámicamente
        
    def create_agents(self) -> List[Agent]:
        retriever_agent = Agent(
            role="Database Information Retriever",
            goal="Retrieve accurate information from the company database",
            backstory="Expert at querying company information from databases",
            tools=[self.db_tool.database_search], 
            llm=self.llm,
            verbose=True
        )
        
        analyzer_agent = Agent(
            role="Data Analyzer",
            goal="Analyze and synthesize company information",
            backstory="Skilled at interpreting business data and providing insights",
            llm=self.llm,
            verbose=True
        )
        
        return [retriever_agent, analyzer_agent]
        
    def create_tasks(self, agents: List[Agent], query: str) -> List[Task]:
        retrieval_task = Task(
            description=f"Find relevant company information for: {query}",
            agent=agents[0],
            expected_output="A detailed response with the requested company information from the database",
            output_json=DatabaseSearchOutput
        )
        
        analysis_task = Task(
            description=f"Analyze and synthesize information for: {query}",
            agent=agents[1],
            expected_output="An analysis of the company information with insights and patterns",
            output_json=AnalysisOutput
        )
        
        return [retrieval_task, analysis_task]
        
    def process_query(self, query: str) -> Dict[str, Any]:
        try:
            # Validación inicial
            if not query or len(query.strip()) < 3:
                return {"success": False, "error": "Query too short"}

            # Crear tareas dinámicamente para cada query
            self.tasks = self.create_tasks(self.agents, query)
            
            # Configurar y ejecutar el Crew
            crew = Crew(
                agents=self.agents,
                tasks=self.tasks,
                process=Process.sequential,  # Proceso secuencial
                verbose=True
            )
            
            result = crew.kickoff()  # Ejecutar el flujo
            
            return {
                "success": True,
                "response": result
            }
            
        except Exception as e:
            logger.error(f"Error: {str(e)}")
            return {"success": False, "error": str(e)}