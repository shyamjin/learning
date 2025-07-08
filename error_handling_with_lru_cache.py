import os
import logging
from typing import Optional
from cachetools import LRUCache, cached
from cachetools.keys import hashkey

from langchain_community.utilities import SQLDatabase
from langchain_openai import AzureChatOpenAI
from langchain_community.agent_toolkits import SQLDatabaseToolkit
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain.agents import AgentExecutor, create_openai_tools_agent

class LangChainSQLAgent:
    def __init__(self, conn_str: str, azure_config: dict):
        self.conn_str = conn_str
        self.azure_config = azure_config['azure_config']
        self._executor_cache = LRUCache(maxsize=8)  # Prompt → AgentExecutor
        
        # Core components
        self.db = None
        self.llm = None
        self.tools = None
        self._initialize_core_components()
        
        # Get stable identifiers for caching
        self.llm_id = f"{self.llm.deployment_name}-{id(self.llm)}"
        self.tools_id = str(id(tuple(self.tools)))

    def _initialize_core_components(self):
        self._setup_database()
        self._setup_llm()
        self._setup_tools()

    def _setup_database(self):
        self.db = SQLDatabase.from_uri(self.conn_str)
        print("✅ Database Dialect:", self.db.dialect)
        print("✅ Usable Tables:", self.db.get_usable_table_names())

    def _setup_llm(self):
        self.llm = AzureChatOpenAI(
            deployment_name=self.azure_config["DEPLOYMENT_NAME"],
            model=self.azure_config["MODEL_NAME"],
            openai_api_key=os.getenv("OPENAI_KEY", ""),
            azure_endpoint=self.azure_config["ENDPOINT_URL"],
            api_version=self.azure_config["API_VERSION"],
            temperature=0,
            max_retries=3,
            streaming=False
        )

    def _setup_tools(self):
        toolkit = SQLDatabaseToolkit(db=self.db, llm=self.llm)
        self.tools = toolkit.get_tools()
        print("✅ Tools Initialized:", self.tools)

    def create_agent_executor(self, system_prompt: str) -> AgentExecutor:
        """Create or retrieve cached AgentExecutor for a given prompt"""
        normalized_prompt = system_prompt.strip()

        if normalized_prompt in self._executor_cache:
            print("♻️ Reusing cached executor")
            return self._executor_cache[normalized_prompt]

        print("🆕 Creating new executor")
        prompt = ChatPromptTemplate.from_messages([
            ("system", normalized_prompt),
            ("human", "{input}"),
            MessagesPlaceholder(variable_name="agent_scratchpad")
        ])

        agent = create_openai_tools_agent(
            llm=self.llm,
            tools=self.tools,
            prompt=prompt
        )

        executor = AgentExecutor(
            agent=agent,
            tools=self.tools,
            verbose=False,
            handle_parsing_errors=True,
            early_stopping_method="generate",
            return_intermediate_steps=False,
            max_execution_time=30  # Add timeout
        )

        self._executor_cache[normalized_prompt] = executor
        return executor

    def prepare_inputs(self, query: str, top_k: int):
        return {
            "input": query,
            "dialect": self.db.dialect,
            "top_k": top_k,
            "agent_scratchpad": []
        }

    @cached(cache=LRUCache(maxsize=128), 
            key=lambda self, query, system_prompt, top_k: hashkey(
                query.strip().lower(), 
                system_prompt.strip(), 
                top_k,
                self.llm_id,
                self.tools_id
            ))
    def _execute_query(self, query: str, system_prompt: str, top_k: int) -> str:
        """Cached query execution with stable identifiers"""
        print("🔍 Executing query (not cached)")
        agent_executor = self.create_agent_executor(system_prompt)
        prepared_input = self.prepare_inputs(query, top_k)
        result = agent_executor.invoke(prepared_input)
        return result["output"]

    def run_query(self, query: str, system_prompt: str, top_k: int = 5, max_retries: int = 2) -> str:
        """Run the query with retry mechanism"""
        normalized_query = query.strip().lower()
        normalized_prompt = system_prompt.strip()
        
        for attempt in range(max_retries + 1):
            try:
                return self._execute_query(normalized_query, normalized_prompt, top_k)
            except Exception as e:
                logging.warning(f"⚠️ Attempt {attempt+1} failed: {str(e)}")
                if attempt >= max_retries - 1:
                    logging.error("❌ All retries exhausted")
                    raise
                
    def cache_info(self) -> dict:
        return {
            "success_cache": self._execute_query.cache_info(),
            "executor_cache": len(self._executor_cache)
        }

[A[A[B[B


