import os
import logging
from typing import Optional
from cachetools import LRUCache

from langchain_community.utilities import SQLDatabase
from langchain_openai import AzureChatOpenAI
from langchain_community.agent_toolkits import SQLDatabaseToolkit
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain.agents import AgentExecutor, create_openai_tools_agent


class LangChainSQLAgent:
    def __init__(self, conn_str: str, azure_config: dict):
        self.conn_str = conn_str
        self.azure_config = azure_config['azure_config']

        # Caches
        self._executor_cache = LRUCache(maxsize=8)              # Prompt → AgentExecutor
        self._success_cache_store = LRUCache(maxsize=128)       # (query, prompt, top_k) → result

        # Core components
        self.db = None
        self.llm = None
        self.tools = None

        self._initialize_core_components()

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
            return self._executor_cache[normalized_prompt]

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
            return_intermediate_steps=False
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

    def run_query(self, query: str, system_prompt: str, top_k: int = 5, max_retries: int = 2) -> str:
        """Run the query with retry + smart caching"""
        normalized_query = query.strip().lower()
        normalized_prompt = system_prompt.strip()
        cache_key = (normalized_query, normalized_prompt, top_k)

        # Return from success cache if available
        if cache_key in self._success_cache_store:
            logging.info("✅ Returning cached result.")
            return self._success_cache_store[cache_key]

        # Retry logic
        for attempt in range(max_retries + 1):
            try:
                result = self._execute_query(normalized_query, normalized_prompt, top_k)
                self._success_cache_store[cache_key] = result
                return result
            except Exception as e:
                logging.warning(f"⚠️ Attempt {attempt + 1} failed: {e}")
                if attempt == max_retries:
                    logging.error("❌ All retries exhausted.")
                    raise

    def _execute_query(self, query: str, system_prompt: str, top_k: int) -> str:
        """Execute query logic, raise on failure"""
        agent_executor = self.create_agent_executor(system_prompt)
        prepared_input = self.prepare_inputs(query, top_k)
        result = agent_executor.invoke(prepared_input)
        return result["output"]

    def cache_info(self) -> dict:
        return {
            "success_cache": len(self._success_cache_store),
            "executor_cache": len(self._executor_cache)
        }

