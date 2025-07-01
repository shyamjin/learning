import numpy as np
from sqlalchemy import create_engine, MetaData, select, and_
import openai

# --- Setup OpenAI ---
openai.api_key = "YOUR_OPENAI_API_KEY"  # replace with your key

def embed_texts(texts, model="text-embedding-3-large"):
    response = openai.Embedding.create(
        input=texts,
        model=model
    )
    return [item['embedding'] for item in response['data']]

def cosine_similarity(vec1, vec2):
    vec1 = np.array(vec1)
    vec2 = np.array(vec2)
    return np.dot(vec1, vec2) / (np.linalg.norm(vec1) * np.linalg.norm(vec2))

# --- DB setup ---
DB_URL = "postgresql://user:pass@localhost:5432/yourdb"  # Replace with your DB URL
engine = create_engine(DB_URL)
metadata_obj = MetaData()
metadata_obj.reflect(bind=engine)

# --- Prepare schema descriptions for embedding ---
# For demo, we manually create descriptions; you can automate this
table_summaries = {}
for table_name, table in metadata_obj.tables.items():
    cols = ", ".join([col.name for col in table.columns])
    desc = f"Table {table_name}: columns are {cols}."
    table_summaries[table_name] = desc

table_names = list(table_summaries.keys())
table_texts = list(table_summaries.values())

print("Embedding table schema summaries...")
table_embeddings = embed_texts(table_texts)

# --- Function to find top matching tables by prompt ---
def find_matching_tables(prompt, top_n=3):
    prompt_embedding = embed_texts([prompt])[0]
    similarities = [cosine_similarity(prompt_embedding, emb) for emb in table_embeddings]
    ranked_indices = np.argsort(similarities)[::-1][:top_n]
    return [(table_names[i], similarities[i]) for i in ranked_indices]

# --- Find referenced tables using SQLAlchemy foreign keys ---
def find_referenced_tables(table_name, metadata, visited=None):
    if visited is None:
        visited = set()
    if table_name in visited:
        return []
    visited.add(table_name)
    table = metadata.tables[table_name]
    referenced = []
    for fk in table.foreign_keys:
        ref_table = fk.column.table.name
        referenced.append(ref_table)
        referenced.extend(find_referenced_tables(ref_table, metadata, visited))
    return list(set(referenced))

# --- Fetch data dynamically applying simple filter example ---
def fetch_data(tables_to_fetch, metadata, engine, filters=None):
    # filters = dict of {table_name: {col: val}} or None
    results = {}
    for table_name in tables_to_fetch:
        table = metadata.tables[table_name]
        query = select(table)
        if filters and table_name in filters:
            conds = []
            for col, val in filters[table_name].items():
                if col in table.c:
                    conds.append(table.c[col] == val)
            if conds:
                query = query.where(and_(*conds))
        rows = engine.execute(query).fetchall()
        results[table_name] = [dict(row) for row in rows]
    return results

# --- Example Usage ---

user_prompt = "Generate data for activity where area would be from country US"

# Step 1: Find best matching tables for prompt
matched = find_matching_tables(user_prompt, top_n=2)
print("Matched tables by semantic similarity:")
for t, score in matched:
    print(f" - {t} (score: {score:.4f})")

main_tables = [t for t, _ in matched]

# Step 2: Find referenced tables recursively
all_tables = set(main_tables)
for mt in main_tables:
    refs = find_referenced_tables(mt, metadata_obj)
    all_tables.update(refs)

print("Tables to fetch including referenced tables:")
print(all_tables)

# Step 3: Prepare filters based on prompt (you can enhance this part with your own NLP or rules)
# For this example, manually add filter on 'area' table for country = 'US'
filters = {
    "area": {"country": "US"}
}

# Step 4: Fetch data from all tables dynamically
data = fetch_data(all_tables, metadata_obj, engine, filters)

print("\nFetched data sample:")
for table_name, rows in data.items():
    print(f"Table: {table_name}, rows fetched: {len(rows)}")
    for r in rows[:3]:
        print(r)

