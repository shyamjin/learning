from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional
import os
import logging
import time
from cachetools import LRUCache
from langchain_community.utilities import SQLDatabase
from langchain_openai import AzureChatOpenAI
from langchain_community.agent_toolkits import SQLDatabaseToolkit
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain.agents import AgentExecutor, create_openai_tools_agent

# ---------------------------
# GLOBAL CONFIG & CACHE INIT
# ---------------------------
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# Shared in-memory caches
EXECUTOR_CACHE = LRUCache(maxsize=8)     # prompt -> AgentExecutor
QUERY_CACHE = LRUCache(maxsize=128)      # (conn_str, query, prompt, top_k) -> output

# Azure OpenAI config (shared)
AZURE_CONFIG = {
    "azure_config": {
        "DEPLOYMENT_NAME": "your-deployment",
        "MODEL_NAME": "gpt-4",
        "ENDPOINT_URL": "https://<your-endpoint>.openai.azure.com/",
        "API_VERSION": "2024-03-01-preview"
    }
}


# ----------------------
# REQUEST SCHEMA
# ----------------------
class QueryRequest(BaseModel):
    db_type: str
    db_server: str
    db_name: str
    query: str
    prompt: str
    top_k: Optional[int] = 5


# ----------------------
# CORE AGENT CLASS
# ----------------------
class LangChainSQLAgent:
    def __init__(self, conn_str: str, azure_config: dict, executor_cache=None, query_cache=None):
        self.conn_str = conn_str
        self.azure_config = azure_config['azure_config']

        self._executor_cache = executor_cache or LRUCache(maxsize=8)
        self._query_cache = query_cache or LRUCache(maxsize=128)

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

    def _setup_llm(self):
        self.llm = AzureChatOpenAI(
            deployment_name=self.azure_config["DEPLOYMENT_NAME"],
            model=self.azure_config["MODEL_NAME"],
            openai_api_key=os.getenv("OPENAI_KEY", ""),
            azure_endpoint=self.azure_config["ENDPOINT_URL"],
            api_version=self.azure_config["API_VERSION"],
            temperature=0,
            max_retries=1,
            streaming=False
        )

    def _setup_tools(self):
        toolkit = SQLDatabaseToolkit(db=self.db, llm=self.llm)
        self.tools = toolkit.get_tools()

    def _get_cache_key(self, query: str, prompt: str, top_k: int) -> tuple:
        return (
            self.conn_str.strip(),
            query.strip().lower(),
            prompt.strip(),
            top_k
        )

    def prepare_inputs(self, query: str, top_k: int):
        return {
            "input": query,
            "dialect": self.db.dialect,
            "top_k": top_k,
            "agent_scratchpad": []
        }

    def create_executor(self, prompt: str) -> AgentExecutor:
        norm_prompt = prompt.strip()

        if norm_prompt in self._executor_cache:
            logging.info("♻️ Reusing cached executor")
            return self._executor_cache[norm_prompt]

        logging.info("🧠 Creating new executor")
        template = ChatPromptTemplate.from_messages([
            ("system", norm_prompt),
            ("human", "{input}"),
            MessagesPlaceholder(variable_name="agent_scratchpad")
        ])

        agent = create_openai_tools_agent(
            llm=self.llm,
            tools=self.tools,
            prompt=template
        )

        executor = AgentExecutor(
            agent=agent,
            tools=self.tools,
            verbose=False,
            handle_parsing_errors=True,
            early_stopping_method="generate",
            return_intermediate_steps=False,
            max_execution_time=30
        )

        self._executor_cache[norm_prompt] = executor
        return executor

    def run_query(self, query: str, system_prompt: str, top_k: int = 5) -> str:
        cache_key = self._get_cache_key(query, system_prompt, top_k)

        if cache_key in self._query_cache:
            logging.info("✅ Returning result from cache")
            return self._query_cache[cache_key]

        logging.info("🚀 Executing new query")
        try:
            executor = self.create_executor(system_prompt)
            inputs = self.prepare_inputs(query, top_k)
            start = time.perf_counter()
            result = executor.invoke(inputs)
            elapsed = time.perf_counter() - start
            logging.info(f"⏱️ Query executed in {elapsed:.2f}s")

            self._query_cache[cache_key] = result["output"]
            return result["output"]

        except Exception as e:
            logging.error(f"❌ Execution failed: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))


# ----------------------
# FASTAPI APP
# ----------------------
app = FastAPI()

@app.on_event("startup")
def clear_all_caches():
    EXECUTOR_CACHE.clear()
    QUERY_CACHE.clear()
    logging.info("🔄 All caches cleared on startup")

@app.post("/run")
def run_sql_agent(request: QueryRequest):
    conn_str = f"{request.db_type}://{request.db_server}/{request.db_name}"
    agent = LangChainSQLAgent(
        conn_str=conn_str,
        azure_config=AZURE_CONFIG,
        executor_cache=EXECUTOR_CACHE,
        query_cache=QUERY_CACHE
    )

    return {
        "result": agent.run_query(request.query, request.prompt, request.top_k),
        "cache_info": {
            "executor_cache_size": len(EXECUTOR_CACHE),
            "query_cache_size": len(QUERY_CACHE)
        }
    }

