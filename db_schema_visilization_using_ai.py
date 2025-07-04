from fastapi import FastAPI
from pydantic import BaseModel
from fastapi.responses import HTMLResponse
from langchain_community.utilities import SQLDatabase
from langchain.agents import create_react_agent, AgentExecutor
from langchain_community.agent_toolkits import SQLDatabaseToolkit
from langchain_community.chat_models import AzureChatOpenAI
import os

app = FastAPI()

# --- Replace with your Azure OpenAI credentials ---
AZURE_DEPLOYMENT = "gpt-4-1106"
AZURE_API_VERSION = "2024-05-01-preview"

class SchemaAnalysisRequest(BaseModel):
    db_type: str
    db_uri: str  # Full connection URI like sqlite:///my.db
    schema: str = None  # Optional

@app.post("/analyze-schema", response_class=HTMLResponse)
def analyze_schema(req: SchemaAnalysisRequest):
    # --- Step 1: Initialize DB and LLM ---
    db = SQLDatabase.from_uri(
        req.db_uri,
        include_foreign_keys=True,
        view_support=True,
    )

    llm = AzureChatOpenAI(
        deployment_name=AZURE_DEPLOYMENT,
        api_version=AZURE_API_VERSION,
        temperature=0,
        max_retries=2
    )

    # --- Step 2: Use LangChain SQL Toolkit ---
    toolkit = SQLDatabaseToolkit(db=db, llm=llm)
    tools = toolkit.get_tools()
    agent = create_react_agent(llm=llm, tools=tools)
    agent_executor = AgentExecutor(agent=agent, tools=tools, verbose=True)

    # --- Step 3: Run prompt to get relationships ---
    prompt = "List and explain all relationships between tables in the database. Be detailed and use markdown."
    response = agent_executor.invoke({"input": prompt})

    # --- Step 4: Return formatted response ---
    html_response = f"<html><body><h2>🧠 Inferred Table Relationships</h2><div>{response['output']}</div></body></html>"
    return HTMLResponse(content=html_response, status_code=200)

if st.button("🧠 Analyze Relationships with GPT"):
    with st.spinner("Analyzing..."):
        res = requests.post("http://localhost:8000/analyze-schema", json={"db_uri": db_uri, "db_type": db_type})
        if res.status_code == 200:
            components.html(res.text, height=650, scrolling=True)
        else:
            st.error("Failed to fetch relationship analysis.")
[A[A[A[A[A[A[A[A[A
[A
