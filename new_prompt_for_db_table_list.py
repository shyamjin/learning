import os
from getpass import getpass
from typing import List
from langchain_community.utilities import SQLDatabase
from langchain_openai import AzureChatOpenAI
from langchain_community.agent_toolkits import SQLDatabaseToolkit
from langchain.agents import AgentExecutor, create_openai_tools_agent
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder

# --- Azure OpenAI Setup ---
if not os.environ.get("AZURE_OPENAI_API_KEY"):
    os.environ["AZURE_OPENAI_API_KEY"] = getpass("Azure OpenAI API Key: ")
    
if not os.environ.get("AZURE_OPENAI_ENDPOINT"):
    os.environ["AZURE_OPENAI_ENDPOINT"] = getpass("Azure OpenAI Endpoint: ")

# Initialize GPT-4.1 model
llm = AzureChatOpenAI(
    deployment_name="gpt-4-1106",  # Replace with your Azure deployment name
    api_version="2024-05-01-preview",  # Latest stable version
    temperature=0,
    max_retries=3
)

# --- Database Connection with Enhanced Metadata ---
db = SQLDatabase.from_uri(
    "sqlite:///your_database.db",
    include_foreign_keys=True,  # Critical for relationship mapping
    view_support=True           # Includes views in schema analysis
)

# --- Create Toolkit ---
toolkit = SQLDatabaseToolkit(db=db, llm=llm)
tools = toolkit.get_tools()

# --- Focused Prompt for Critical Dependencies ---
SYSTEM_PROMPT = """
You are a SQL dependency analyzer. Your ONLY task is to identify CRITICAL dependencies for data generation.
When asked about a table, return ONLY a list of table names that are direct, required dependencies.

Rules:
1. Return ONLY comma-separated table names (no explanations, no schema, no extra text)
2. Include ONLY tables that MUST have data BEFORE the target table can be populated
3. EXCLUDE tables that:
   - Are connected but not required for data generation
   - Share keys but aren't dependencies
   - Are children of the target table
4. Format: table1, table2, table3

Example: 
Input: "What tables are critically dependent for generating orders data?"
Output: "customers, products"

Workflow:
1. Identify direct foreign key constraints pointing FROM the target table
2. Filter to only required parent tables
3. Return comma-separated list
""".strip()

# Create the prompt template
prompt = ChatPromptTemplate.from_messages([
    ("system", SYSTEM_PROMPT),
    ("human", "{input}"),
    MessagesPlaceholder(variable_name="agent_scratchpad")
])

# --- Build the Agent ---
agent = create_openai_tools_agent(
    llm=llm,
    tools=tools,
    prompt=prompt
)

# Create input processor
def prepare_inputs(input_dict):
    return {
        "input": input_dict["input"],
        "dialect": db.dialect,
        "top_k": 1,  # Minimize result size
        "agent_scratchpad": [] 
    }

agent_executor = AgentExecutor(
    agent=agent,
    tools=tools,
    verbose=False,  # Disable verbose output
    handle_parsing_errors=True,
    return_intermediate_steps=False  # Only final output
)

# --- Focused Dependency Extractor ---
def get_critical_dependencies(table_name: str) -> List[str]:
    """Get critical dependencies for a table as a clean list"""
    response = agent_executor.invoke({
        "input": f"What tables are critically dependent for generating {table_name} data? "
                 f"Return ONLY comma-separated table names."
    })
    
    # Extract and clean the table list
    output = response.get("output", "").strip()
    if not output:
        return []
    
    # Parse comma-separated list and clean
    tables = [t.strip() for t in output.split(",") if t.strip()]
    return tables

# --- Example Usage ---
if __name__ == "__main__":
    # Get critical dependencies for a table
    table_name = "orders"  # Replace with your table
    dependencies = get_critical_dependencies(table_name)
    
    # Print only the table list
    print(f"Critical dependencies for {table_name}:")
    for table in dependencies:
        print(f"- {table}")

