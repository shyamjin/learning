from langchain_openai import AzureChatOpenAI
from langchain.agents import create_react_agent, AgentExecutor
from langchain_core.prompts import ChatPromptTemplate
from langchain_community.tools.sql_database.tool import QuerySQLDataBaseTool, ListSQLDatabaseTool
from langchain_community.utilities import SQLDatabase

# Azure GPT-4.1 Setup
llm = AzureChatOpenAI(
    deployment_name="gpt-4",  # Azure deployment name
    model="gpt-4",
    openai_api_key="your-key",
    openai_api_base="https://YOUR_RESOURCE.openai.azure.com/",
    openai_api_version="2023-12-01-preview",
)

# Connect to your DB
db = SQLDatabase.from_uri("postgresql://user:pass@localhost:5432/yourdb")

# Define tools: list tables, run query
tools = [
    ListSQLDatabaseTool(db=db),        # Tool: lists tables
    QuerySQLDataBaseTool(db=db),       # Tool: runs SQL
]

# Custom system message / prompt
prompt = ChatPromptTemplate.from_messages([
    ("system", 
     "You are a highly intelligent data analyst agent. "
     "You are connected to a SQL database. "
     "Your job is to understand the schema, find relevant tables, and explain dependencies. "
     "Always return clear explanations before running any query."),
    ("human", "{input}")
])

# Create ReAct agent
agent = create_react_agent(llm=llm, tools=tools, prompt=prompt)

# Wrap with executor
agent_executor = AgentExecutor(agent=agent, tools=tools, verbose=True)
