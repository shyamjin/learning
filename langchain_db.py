import os
from getpass import getpass
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

# --- Properly Structured Prompt with All Variables ---
SYSTEM_PROMPT = """
You are a senior SQL database analyst. Your tasks:
1. Identify ALL relevant tables, relationships, and constraints
2. Show complete dependency chains (e.g., orders → customers → addresses)
3. Never modify the database
4. Limit results to {top_k} unless specified
5. Verify query syntax before execution

Workflow for every query:
1. Analyze question -> Identify tables
2. Find relationships -> Map dependencies
3. Retrieve schema -> Construct query
4. Execute -> Return results

Database dialect: {dialect}
""".strip()

# Create the prompt template with ALL required variables
prompt = ChatPromptTemplate.from_messages([
    ("system", SYSTEM_PROMPT),
    ("human", "{input}"),
    MessagesPlaceholder(variable_name="agent_scratchpad")
])

# --- Build the Agent with Explicit Variable Binding ---
agent = create_openai_tools_agent(
    llm=llm,
    tools=tools,
    prompt=prompt
)

# Create input processor to ensure all variables are present
def prepare_inputs(input_dict):
    return {
        "input": input_dict["input"],
        "dialect": db.dialect,
        "top_k": 5,
        "agent_scratchpad": []  # Will be populated automatically
    }

agent_executor = AgentExecutor(
    agent=agent,
    tools=tools,
    verbose=True,
    handle_parsing_errors=True,
    return_intermediate_steps=True
)

# --- Wrapper Function for Execution ---
def run_agent_query(query):
    prepared_input = prepare_inputs({
        "input": query
    })
    return agent_executor.invoke(prepared_input)

# --- Example Usage ---
if __name__ == "__main__":
    try:
        # Test schema analysis
        print("Testing schema analysis...")
        result = run_agent_query(
            "Show schema for orders table and all dependent tables with relationships"
        )
        print("SCHEMA RESULT:", result["output"])
        
        # Test relationship-aware query
        print("\nTesting relationship query...")
        result = run_agent_query(
            "Find customers with more than 5 orders including their address details"
        )
        print("QUERY RESULT:", result["output"])
        
    except Exception as e:
        print(f"Execution failed: {str(e)}")
        print("Check these common issues:")
        print("1. Azure deployment name matches your portal")
        print("2. Database URI is correct")
        print("3. LangChain versions match (0.3.26/0.3.27/0.3.67)")
        print("4. Foreign keys are enabled in database")