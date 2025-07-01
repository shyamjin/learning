import numpy as np
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
import sqlite3
import re
from openai import OpenAI  # Requires openai package
import os

# Initialize OpenAI client (replace with your API key)
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# Database schema metadata with semantic descriptions
SCHEMA_METADATA = {
    "country": {
        "description": "Contains countries and their attributes",
        "columns": {
            "country_id": "Unique identifier for countries",
            "name": "Full name of the country",
            "code": "Country code abbreviation",
            "population": "Total population of the country"
        },
        "relationships": ["area"]
    },
    "area": {
        "description": "Geographical areas within countries",
        "columns": {
            "area_id": "Unique identifier for areas",
            "country_id": "Reference to the country this area belongs to",
            "name": "Name of the geographical area",
            "climate": "Climate type of the area"
        },
        "relationships": ["country", "activity"]
    },
    "activity": {
        "description": "Activities available in different areas",
        "columns": {
            "activity_id": "Unique identifier for activities",
            "area_id": "Reference to the area where this activity occurs",
            "name": "Name of the activity",
            "type": "Type of activity (e.g., sports, cultural)",
            "difficulty": "Difficulty level of the activity"
        },
        "relationships": ["area", "participant"]
    },
    "participant": {
        "description": "People participating in activities",
        "columns": {
            "participant_id": "Unique identifier for participants",
            "activity_id": "Reference to the activity",
            "name": "Full name of participant",
            "age": "Age of participant",
            "experience_level": "Participant's experience level"
        },
        "relationships": ["activity"]
    }
}

# Create in-memory database
conn = sqlite3.connect(':memory:')
cur = conn.cursor()

# Create tables
cur.execute("""
CREATE TABLE country (
    country_id INTEGER PRIMARY KEY,
    name TEXT,
    code TEXT,
    population INTEGER
)
""")

cur.execute("""
CREATE TABLE area (
    area_id INTEGER PRIMARY KEY,
    country_id INTEGER,
    name TEXT,
    climate TEXT,
    FOREIGN KEY(country_id) REFERENCES country(country_id)
)
""")

cur.execute("""
CREATE TABLE activity (
    activity_id INTEGER PRIMARY KEY,
    area_id INTEGER,
    name TEXT,
    type TEXT,
    difficulty TEXT,
    FOREIGN KEY(area_id) REFERENCES area(area_id)
)
""")

cur.execute("""
CREATE TABLE participant (
    participant_id INTEGER PRIMARY KEY,
    activity_id INTEGER,
    name TEXT,
    age INTEGER,
    experience_level TEXT,
    FOREIGN KEY(activity_id) REFERENCES activity(activity_id)
)
""")

# Populate with sample data
def populate_sample_data(conn):
    cur = conn.cursor()
    
    # Countries
    countries = [
        (1, 'United States', 'US', 331000000),
        (2, 'Canada', 'CA', 38000000),
        (3, 'Mexico', 'MX', 126000000)
    ]
    cur.executemany("INSERT INTO country VALUES (?, ?, ?, ?)", countries)
    
    # Areas
    areas = [
        (1, 1, 'New York', 'Temperate'),
        (2, 1, 'California', 'Mediterranean'),
        (3, 1, 'Texas', 'Subtropical'),
        (4, 2, 'Ontario', 'Continental'),
        (5, 3, 'Baja California', 'Arid')
    ]
    cur.executemany("INSERT INTO area VALUES (?, ?, ?, ?)", areas)
    
    # Activities
    activities = [
        (1, 1, 'Central Park Cycling', 'Sports', 'Beginner'),
        (2, 1, 'Broadway Show', 'Cultural', 'Easy'),
        (3, 2, 'Surfing Lessons', 'Sports', 'Intermediate'),
        (4, 3, 'Rodeo Experience', 'Cultural', 'Advanced'),
        (5, 4, 'Niagara Falls Tour', 'Sightseeing', 'Easy'),
        (6, 5, 'Desert Hiking', 'Adventure', 'Expert')
    ]
    cur.executemany("INSERT INTO activity VALUES (?, ?, ?, ?, ?)", activities)
    
    # Participants
    participants = [
        (1, 1, 'John Smith', 35, 'Intermediate'),
        (2, 1, 'Sarah Johnson', 28, 'Beginner'),
        (3, 3, 'Mike Thompson', 42, 'Advanced'),
        (4, 4, 'Emma Davis', 29, 'Expert'),
        (5, 5, 'David Wilson', 50, 'Beginner'),
        (6, 6, 'Lisa Chen', 33, 'Intermediate')
    ]
    cur.executemany("INSERT INTO participant VALUES (?, ?, ?, ?, ?)", participants)
    
    conn.commit()

populate_sample_data(conn)

# Generate embeddings for schema metadata
def generate_embeddings(text):
    """Generate embeddings using OpenAI API"""
    response = client.embeddings.create(
        input=text,
        model="text-embedding-3-small"
    )
    return response.data[0].embedding

# Create semantic index for tables and columns
def create_semantic_index():
    """Create embeddings for all table and column descriptions"""
    index = {
        "tables": {},
        "columns": {}
    }
    
    # Embed table descriptions
    for table, metadata in SCHEMA_METADATA.items():
        table_text = f"Table: {table}. Description: {metadata['description']}"
        index["tables"][table] = {
            "embedding": generate_embeddings(table_text),
            "metadata": metadata
        }
        
        # Embed column descriptions
        for col, desc in metadata["columns"].items():
            col_key = f"{table}.{col}"
            col_text = f"Table: {table}, Column: {col}. Description: {desc}"
            index["columns"][col_key] = {
                "embedding": generate_embeddings(col_text),
                "table": table,
                "column": col
            }
    
    return index

# Create the semantic index
semantic_index = create_semantic_index()

# Find best matching table for a prompt
def find_matching_table(prompt, top_n=3):
    """Find the most relevant table using semantic similarity"""
    prompt_embedding = generate_embeddings(prompt)
    
    similarities = []
    for table, data in semantic_index["tables"].items():
        sim = cosine_similarity([prompt_embedding], [data["embedding"]])[0][0]
        similarities.append((table, sim, data["metadata"]))
    
    # Sort by similarity score
    similarities.sort(key=lambda x: x[1], reverse=True)
    return similarities[:top_n]

# Find best matching column for a prompt
def find_matching_column(prompt, table=None, top_n=3):
    """Find the most relevant column using semantic similarity"""
    prompt_embedding = generate_embeddings(prompt)
    
    similarities = []
    for col_key, data in semantic_index["columns"].items():
        # If table is specified, only consider columns from that table
        if table and data["table"] != table:
            continue
            
        sim = cosine_similarity([prompt_embedding], [data["embedding"]])[0][0]
        similarities.append((col_key, sim, data))
    
    # Sort by similarity score
    similarities.sort(key=lambda x: x[1], reverse=True)
    return similarities[:top_n]

# Extract conditions from prompt
def parse_conditions(prompt):
    """Extract conditions from natural language prompt"""
    conditions = []
    
    # Pattern 1: "where country is US"
    match = re.search(r"where\s+(\w+)\s+is\s+['\"]?(\w+)['\"]?", prompt, re.IGNORECASE)
    if match:
        conditions.append({
            "column": match.group(1),
            "value": match.group(2),
            "operator": "="
        })
    
    # Pattern 2: "with population > 1000000"
    match = re.search(r"with\s+(\w+)\s*([<>]=?|!=?)\s*(\d+)", prompt, re.IGNORECASE)
    if match:
        conditions.append({
            "column": match.group(1),
            "operator": match.group(2),
            "value": int(match.group(3))
        })
    
    # Pattern 3: "for area in California"
    match = re.search(r"for\s+(\w+)\s+in\s+['\"]?([\w\s]+)['\"]?", prompt, re.IGNORECASE)
    if match:
        conditions.append({
            "column": match.group(1),
            "value": match.group(2),
            "operator": "="
        })
    
    return conditions

# Find related tables based on schema relationships
def find_related_tables(target_table, condition_tables):
    """Find all tables related to target and condition tables"""
    visited = set()
    queue = [target_table] + condition_tables
    related_tables = set()
    
    while queue:
        current = queue.pop(0)
        if current in visited:
            continue
            
        visited.add(current)
        related_tables.add(current)
        
        # Add relationships
        for rel_table in SCHEMA_METADATA[current]["relationships"]:
            if rel_table not in visited:
                queue.append(rel_table)
    
    return related_tables

# Generate SQL query based on prompt
def generate_query_from_prompt(prompt):
    """Convert natural language prompt to SQL query"""
    # Step 1: Find the best matching table
    table_matches = find_matching_table(prompt)
    target_table = table_matches[0][0] if table_matches else None
    
    if not target_table:
        raise ValueError("No matching table found for prompt")
    
    # Step 2: Extract conditions
    conditions = parse_conditions(prompt)
    condition_tables = set()
    
    # Step 3: Find columns for conditions
    for cond in conditions:
        col_matches = find_matching_column(cond["column"], table=target_table)
        if col_matches:
            best_col = col_matches[0][2]
            cond["resolved_table"] = best_col["table"]
            cond["resolved_column"] = best_col["column"]
            condition_tables.add(best_col["table"])
    
    # Step 4: Find all related tables
    related_tables = find_related_tables(target_table, list(condition_tables))
    
    # Step 5: Build SELECT clause
    select_columns = []
    for table in related_tables:
        for col in SCHEMA_METADATA[table]["columns"]:
            select_columns.append(f"{table}.{col} AS '{table}_{col}'")
    
    # Step 6: Build FROM and JOIN clauses
    from_clause = target_table
    join_clauses = []
    
    # Build joins based on relationships
    for table in related_tables:
        if table == target_table:
            continue
            
        # Find relationship path
        if table in SCHEMA_METADATA[target_table]["relationships"]:
            # Direct relationship
            join_clauses.append(
                f"JOIN {table} ON {target_table}.{table}_id = {table}.{table}_id"
            )
        else:
            # Indirect relationship - find common relationships
            for rel in SCHEMA_METADATA[target_table]["relationships"]:
                if table in SCHEMA_METADATA[rel]["relationships"]:
                    join_clauses.append(
                        f"JOIN {rel} ON {target_table}.{rel}_id = {rel}.{rel}_id"
                    )
                    join_clauses.append(
                        f"JOIN {table} ON {rel}.{table}_id = {table}.{table}_id"
                    )
                    break
    
    # Step 7: Build WHERE clause
    where_clauses = []
    for cond in conditions:
        if "resolved_table" in cond and "resolved_column" in cond:
            where_clauses.append(
                f"{cond['resolved_table']}.{cond['resolved_column']} {cond['operator']} '{cond['value']}'"
            )
    
    # Step 8: Construct full query
    query = f"SELECT {', '.join(select_columns)} FROM {from_clause}"
    if join_clauses:
        query += " " + " ".join(join_clauses)
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
    
    return query, related_tables

# Process user prompt
def process_prompt(prompt):
    """Main function to process user prompt and return data"""
    # Generate SQL query
    query, related_tables = generate_query_from_prompt(prompt)
    
    # Execute query
    result_df = pd.read_sql_query(query, conn)
    
    # Get reference data for all related tables
    reference_data = {}
    for table in related_tables:
        reference_data[table] = pd.read_sql_query(f"SELECT * FROM {table}", conn)
    
    return {
        "query": query,
        "result": result_df,
        "reference_tables": reference_data
    }

# Example usage
if __name__ == "__main__":
    # Example prompts
    prompts = [
        "generate data for activity where area is California",
        "show participants for activities in the US",
        "get areas with population > 10000000"
    ]
    
    for prompt in prompts:
        print(f"\n{'='*50}")
        print(f"Processing prompt: '{prompt}'")
        
        try:
            result = process_prompt(prompt)
            print("\nGenerated SQL Query:")
            print(result["query"])
            
            print("\nResult Data:")
            print(result["result"])
            
            print("\nReference Tables:")
            for table, data in result["reference_tables"].items():
                print(f"\n{table.upper()} table (first 3 rows):")
                print(data.head(3))
                
            # Save to Excel
            with pd.ExcelWriter(f"results_{prompt[:10]}.xlsx") as writer:
                result["result"].to_excel(writer, sheet_name="Results", index=False)
                for table, data in result["reference_tables"].items():
                    data.to_excel(writer, sheet_name=f"Ref_{table}", index=False)
            print("\nSaved results to Excel file")
            
        except Exception as e:
            print(f"Error processing prompt: {str(e)}")
