from functools import lru_cache
import os
from langchain_community.utilities import SQLDatabase
from langchain_openai import AzureChatOpenAI
from langchain_community.agent_toolkits import SQLDatabaseToolkit
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain.agents import AgentExecutor, create_openai_tools_agent

class LangChainSQLAgent:
    def __init__(self, conn_str, azure_config):
        self.conn_str = conn_str
        self.azure_config = azure_config['azure_config']
        self.db = None
        self.llm = None
        self.tools = None
        self._executor_cache = {}  # Cache for executors
        self._initialize_core_components()
        # Create a cached version of the run_query method
        self._cached_run_query = lru_cache(maxsize=128)(self._uncached_run_query)

    def _initialize_core_components(self):
        self._setup_database()
        self._setup_llm()
        self._setup_tools()

    def _setup_database(self):
        self.db = SQLDatabase.from_uri(self.conn_str)
        print("Database Dialect:", self.db.dialect)
        print("Usable Tables:", self.db.get_usable_table_names())

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
        print("Tools Initialized:", self.tools)

    def create_agent_executor(self, system_prompt):
        # Use normalized prompt as cache key
        normalized_prompt = system_prompt.strip()
        
        # Check if executor exists in cache
        if normalized_prompt in self._executor_cache:
            return self._executor_cache[normalized_prompt]
        
        # Create new executor if not cached
        prompt = ChatPromptTemplate.from_messages([
            ("system", system_prompt),
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
        
        # Cache the executor
        self._executor_cache[normalized_prompt] = executor
        return executor

    def prepare_inputs(self, query, top_k):
        return {
            "input": query,
            "dialect": self.db.dialect,
            "top_k": top_k,
            "agent_scratchpad": []
        }

    def run_query(self, query, system_prompt, top_k):
        """Public method with input normalization"""
        # Normalize inputs for consistent caching
        normalized_query = query.strip().lower()
        normalized_prompt = system_prompt.strip()
        return self._cached_run_query(normalized_query, normalized_prompt, top_k)
    
    def _uncached_run_query(self, query: str, system_prompt: str, top_k: int) -> str:
        """Actual query execution without caching"""
        agent_executor = self.create_agent_executor(system_prompt)
        prepared_input = self.prepare_inputs(query, top_k)
        result = agent_executor.invoke(prepared_input)
        return result["output"]
    
    def cache_info(self):
        """Get cache statistics"""
        return {
            "run_query": self._cached_run_query.cache_info(),
            "executors": len(self._executor_cache)
        }

