from functools import lru_cache

class LangChainSQLAgent:

    def __init__(self, conn_str, azure_config):
        self.conn_str = conn_str
        self.azure_config = azure_config['azure_config']
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

    def prepare_inputs(self, query, top_k):
        return {
            "input": query,
            "dialect": self.db.dialect,
            "top_k": top_k,
            "agent_scratchpad": []
        }

    def run_query(self, query, system_prompt, top_k):
        query = query.strip().lower()
        executor = self._get_or_create_executor(system_prompt)
        prepared_input = self.prepare_inputs(query, top_k)
        result = executor.invoke(prepared_input)
        return result["output"]

    # ✅ LRU Caching for agent executor (avoiding `self`)
    @staticmethod
    @lru_cache(maxsize=128)
    def _cached_executor(system_prompt_key: str, llm_id: str, tools_id: str):
        """Internal static cache-safe function"""
        from langchain.prompts import ChatPromptTemplate, MessagesPlaceholder
        prompt = ChatPromptTemplate.from_messages([
            ("system", system_prompt_key),
            ("human", "{input}"),
            MessagesPlaceholder(variable_name="agent_scratchpad")
        ])
        agent = create_openai_tools_agent(llm=llm_id, tools=tools_id, prompt=prompt)
        return AgentExecutor(
            agent=agent,
            tools=tools_id,
            verbose=False,
            handle_parsing_errors=True,
            return_intermediate_steps=False,
            early_stopping_method="generate"
        )

    def _get_or_create_executor(self, system_prompt: str):
        """Wraps the cached executor with hashable identifiers"""
        system_prompt_key = system_prompt.strip()
        # We can use id() to identify toolset/llm (or any stable string)
        llm_id = str(id(self.llm))
        tools_id = str(id(tuple(self.tools)))  # Must be hashable
        return self._cached_executor(system_prompt_key, llm_id, tools_id)


