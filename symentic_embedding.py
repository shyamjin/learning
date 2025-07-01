import os
import numpy as np
from sqlalchemy import create_engine, MetaData
import networkx as nx
import openai

# Set your Azure OpenAI configs
openai.api_type = "azure"
openai.api_base = os.getenv("AZURE_OPENAI_ENDPOINT")  # e.g. https://your-resource.openai.azure.com/
openai.api_version = "2023-05-15"
openai.api_key = os.getenv("AZURE_OPENAI_API_KEY")
embedding_model_name = "text-embedding-3-large"  # example embedding model

# --- Step 1: Connect and reflect DB schema ---

engine = create_engine("postgresql://user:password@host:port/dbname")
metadata = MetaData()
metadata.reflect(bind=engine)
table_names = list(metadata.tables.keys())

# Example: Optional table descriptions if available
table_descriptions = {
    "student": "Contains information about enrolled students, including id, name, department_id.",
    "department": "Information about academic departments with department_id and name.",
    "college": "Details of colleges with college_id and location."
    # Add more or extract from DB comments if possible
}

# --- Step 2: Build table metadata texts for embedding ---
def build_table_text(name):
    desc = table_descriptions.get(name, "")
    text = f"Table: {name}. Description: {desc}"
    return text

table_texts = {name: build_table_text(name) for name in table_names}

# --- Step 3: Embed all table texts ---
def get_embedding(text):
    response = openai.Embedding.create(
        engine=embedding_model_name,
        input=text
    )
    return response['data'][0]['embedding']

print("Embedding tables...")
table_embeddings = {}
for name, text in table_texts.items():
    table_embeddings[name] = get_embedding(text)

# --- Step 4: Build dependency graph ---
G = nx.DiGraph()
for table_name in table_names:
    G.add_node(table_name)
for table in metadata.tables.values():
    for fk in table.foreign_keys:
        parent = fk.column.table.name
        child = table.name
        G.add_edge(parent, child)

# --- Step 5: Define similarity function ---
def cosine_similarity(a, b):
    a = np.array(a)
    b = np.array(b)
    return np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b))

# --- Step 6: Map prompt to table ---
def map_prompt_to_table(prompt):
    prompt_embedding = get_embedding(prompt)
    similarities = {}
    for name, emb in table_embeddings.items():
        sim = cosine_similarity(prompt_embedding, emb)
        similarities[name] = sim
    best_match = max(similarities, key=similarities.get)
    return best_match, similarities[best_match]

# --- Step 7: Given prompt, find related tables ---
def get_related_tables_for_prompt(prompt):
    mapped_table, score = map_prompt_to_table(prompt)
    print(f"Prompt mapped to table: {mapped_table} (score: {score:.4f})")
    ancestors = nx.ancestors(G, mapped_table)
    related_tables = ancestors.union({mapped_table})
    return related_tables

# --- Usage example ---
user_prompt = "generate data for student"
related_tables = get_related_tables_for_prompt(user_prompt)
print("Related tables:", related_tables)

# --- Optional: Get generation order ---
subgraph = G.subgraph(related_tables)
generation_order = list(nx.topological_sort(subgraph))
print("Generation order:", generation_order)

