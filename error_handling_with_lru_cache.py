import os
import logging
import time
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
        self._executor_cache = LRUCache(maxsize=8)    # Prompt → AgentExecutor
        self._query_cache = LRUCache(maxsize=128)     # (query, prompt, top_k) → result
        
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
        logging.info(f"✅ Database Dialect: {self.db.dialect}")
        logging.info(f"✅ Usable Tables: {self.db.get_usable_table_names()}")

    def _setup_llm(self):
        self.llm = AzureChatOpenAI(
            deployment_name=self.azure_config["DEPLOYMENT_NAME"],
            model=self.azure_config["MODEL_NAME"],
            openai_api_key=os.getenv("OPENAI_KEY", ""),
            azure_endpoint=self.azure_config["ENDPOINT_URL"],
            api_version=self.azure_config["API_VERSION"],
            temperature=0,
            max_retries=1,  # Single attempt
            streaming=False
        )

    def _setup_tools(self):
        toolkit = SQLDatabaseToolkit(db=self.db, llm=self.llm)
        self.tools = toolkit.get_tools()
        logging.info(f"✅ Tools Initialized: {[t.name for t in self.tools]}")

    def create_agent_executor(self, system_prompt: str) -> AgentExecutor:
        """Create or retrieve cached AgentExecutor for a given prompt"""
        normalized_prompt = system_prompt.strip()

        if normalized_prompt in self._executor_cache:
            logging.debug("♻️ Reusing cached executor")
            return self._executor_cache[normalized_prompt]

        logging.debug("🆕 Creating new executor")
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
            max_execution_time=30  # Timeout after 30 seconds
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

    def _get_cache_key(self, query: str, system_prompt: str, top_k: int) -> tuple:
        """Create a stable cache key"""
        return (
            query.strip().lower(), 
            system_prompt.strip(), 
            top_k
        )

    def run_query(self, query: str, system_prompt: str, top_k: int = 5) -> str:
        """Execute a single query with caching"""
        # Create normalized inputs and cache key
        normalized_query = query.strip().lower()
        normalized_prompt = system_prompt.strip()
        cache_key = self._get_cache_key(normalized_query, normalized_prompt, top_k)
        
        # Return from cache if available
        if cache_key in self._query_cache:
            logging.info("♻️ Returning cached result")
            return self._query_cache[cache_key]
        
        logging.info("🔍 Executing new query")
        
        try:
            # Get executor (cached by prompt)
            agent_executor = self.create_agent_executor(normalized_prompt)
            
            # Prepare inputs
            prepared_input = self.prepare_inputs(normalized_query, top_k)
            
            # Execute query
            start_time = time.perf_counter()
            result = agent_executor.invoke(prepared_input)
            execution_time = time.perf_counter() - start_time
            logging.info(f"⏱️ Query executed in {execution_time:.2f} seconds")
            
            # Cache successful result
            self._query_cache[cache_key] = result["output"]
            return result["output"]
            
        except Exception as e:
            logging.error(f"❌ Query failed: {str(e)}")
            raise  # Propagate error to caller

    def cache_info(self) -> dict:
        return {
            "query_cache_size": len(self._query_cache),
            "executor_cache_size": len(self._executor_cache)
        }

    def clear_cache(self):
        """Clear all caches"""
        self._query_cache.clear()
        self._executor_cache.clear()
        logging.info("🧹 All caches cleared")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

    # Configuration - use environment variables in production
    conn_str = "postgresql+psycopg2://user:password@localhost:5432/mydb"
    azure_config = {
        "azure_config": {
            "DEPLOYMENT_NAME": "your-deployment",
            "MODEL_NAME": "gpt-4",
            "ENDPOINT_URL": "https://<your-endpoint>.openai.azure.com/",
            "API_VERSION": "2024-03-01-preview"
        }
    }

    agent = LangChainSQLAgent(conn_str, azure_config)

    SYSTEM_PROMPT = """
    You are a senior SQL database analyst. Your tasks:
    1. Identify ALL relevant tables, relationships, and constraints
    2. Show complete dependency chains (e.g., orders → customers → addresses)
    3. Never modify the database
    4. Limit results to {top_k} unless specified
    5. Verify query syntax before execution

    Database dialect: {dialect}
    """

    user_query = "Show all areas that have a forecast with temperature above 25"

    # First execution
    try:
        result = agent.run_query(user_query, SYSTEM_PROMPT, top_k=5)
        print("\n✅ Result:\n", result)
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
    
    # Cache status
    print("\nCache status:", agent.cache_info())
