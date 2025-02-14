# chatbot/agent_setup.py
from typing import Dict, Any, Optional, List
from pydantic import BaseModel, Field
from crewai import Agent, Crew, Process, Task
from chatbot.query_executor import QueryExecutor
from chatbot.sql_generator import SQLGenerator
from config import Config
import logging
from datetime import datetime
from langchain_ollama import ChatOllama
from langchain.tools import Tool
import time
# Importamos el LLMManager que centraliza la elección del modelo
from chatbot.llm_manager import LLMManager

logger = logging.getLogger(__name__)

class DatabaseSearchOutput(BaseModel):
    results: str
    query: str
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())

class AnalysisOutput(BaseModel):
    analysis: str = Field(..., description="Analysis of the data")
    insights: List[str] = Field(default_factory=list, description="Key insights extracted")
    recommendations: List[str] = Field(default_factory=list, description="Recommendations based on analysis")

def get_llm(llm_provider: str):
    """
    Delegamos la selección del LLM a LLMManager para centralizar la lógica.
    """
    try:
        return LLMManager.get_llm(provider=llm_provider)
    except Exception as e:
        logger.error(f"Error al obtener el LLM: {str(e)}")
        raise

class DatabaseTool:
    def __init__(self):
        self.query_executor = QueryExecutor()
        self.sql_generator = SQLGenerator()
    
    def database_search(self, query: str) -> str:
        """Search company database with natural language query"""
        try:
            sql_query = self.sql_generator.generate_sql(query)
            if self.query_executor.validate_query(sql_query):
                df = self.query_executor.execute_query(sql_query)
                return df.to_string()
            return "Invalid query"
        except Exception as e:
            logger.error(f"Database search error: {str(e)}")
            return f"Error executing query: {str(e)}"

class ScrapingAgent:
    def __init__(self, llm_provider: str):
        self.llm = get_llm(llm_provider)
        self.db_tool = DatabaseTool()
        self.agents = self.create_agents()
        self.tasks = []
        
    def create_agents(self) -> List[Agent]:
        db_search_tool = Tool(
            name="database_search",
            func=self.db_tool.database_search,
            description="Search company database with natural language query"
        )
        
        # Explicitly set LLM for each agent
        retriever_agent = Agent(
            role="Database Information Retriever",
            goal="Retrieve accurate information from the company database",
            backstory="Expert at querying company information from databases",
            tools=[db_search_tool], 
            llm=self.llm,  # Use the configured LLM
            verbose=True
        )
        
        analyzer_agent = Agent(
            role="Data Analyzer",
            goal="Analyze and synthesize company information",
            backstory="Skilled at interpreting business data and providing insights",
            llm=self.llm,  # Use the configured LLM
            verbose=True
        )
        
        return [retriever_agent, analyzer_agent]
    
    def create_tasks(self, agents: List[Agent], query: str) -> List[Task]:
        retrieval_task = Task(
            description=f"Find relevant company information for: {query}",
            agent=agents[0],
            expected_output="string"  # Changed from DatabaseSearchOutput to "string"
        )
        
        analysis_task = Task(
            description=f"Analyze and synthesize information for: {query}",
            agent=agents[1],
            expected_output="string"  # Changed from AnalysisOutput to "string"
        )
        
        return [retrieval_task, analysis_task]
        
    def process_query(self, query: str) -> Dict[str, Any]:
        try:
            if not query or len(query.strip()) < 3:
                return {"success": False, "error": "Query too short"}

            self.tasks = self.create_tasks(self.agents, query)
            
            crew = Crew(
                agents=self.agents,
                tasks=self.tasks,
                process=Process.sequential,
                verbose=True
            )
            
            result = crew.kickoff()
            
            return {
                "success": True,
                "response": result
            }
            
        except Exception as e:
            logger.error(f"Error processing query: {str(e)}")
            return {"success": False, "error": str(e)}