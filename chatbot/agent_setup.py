# chatbot/agent_setup.py
from crewai import Agent, Crew, Process, Task, LLM
from crewai_tools import BaseTool 
from database.connectors import MySQLConnector
from typing import Dict, List
import pandas as pd

class DatabaseSearchTool:
    def __init__(self):
        self.connector = MySQLConnector()
    
    def _run(self, query: str) -> str:
        """Execute database search and return results"""
        with self.connector.get_session() as session:
            try:
                result = session.execute(query)
                df = pd.DataFrame(result.fetchall(), columns=result.keys())
                return df.to_string()
            except Exception as e:
                return f"Error executing query: {str(e)}"

class ScrapingAgent:
    def __init__(self):
        self.llm = LLM(model="ollama/deepseek-r1:7b", base_url="http://localhost:11434")
        self.db_tool = DatabaseSearchTool()
        
    def create_agents(self) -> List[Agent]:
        retriever_agent = Agent(
            role="Database Information Retriever",
            goal="Retrieve accurate information from the company database",
            backstory="Expert at querying company information from databases",
            tools=[self.db_tool],
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
            agent=agents[0]
        )
        
        analysis_task = Task(
            description=f"Analyze and synthesize information for: {query}",
            agent=agents[1]
        )
        
        return [retrieval_task, analysis_task]
        
    def process_query(self, query: str) -> str:
        agents = self.create_agents()
        tasks = self.create_tasks(agents, query)
        
        crew = Crew(
            agents=agents,
            tasks=tasks,
            process=Process.sequential,
            verbose=True
        )
        
        result = crew.kickoff(inputs={"query": query})
        return result.raw