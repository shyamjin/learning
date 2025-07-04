# main.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict, List
import json
import os
import uvicorn
import pandas as pd
import random

DATA_FILE = "../app/data.json"

app = FastAPI()

class SchemaRequest(BaseModel):
    db_type: str
    db_name: str
    schema_name: str
    business_rules: List[str] = []

class DataGenRequest(BaseModel):
    num_records: int
    db_type: str
    db_name: str
    schema_name: str
    user_prompt: str
    business_rules: List[str] = []

def load_db_schemas():
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, "r") as f:
            return json.load(f)
    return {}

def save_db_schemas(data):
    with open(DATA_FILE, "w") as f:
        json.dump(data, f, indent=2)

# ------------- Routes -------------

@app.post("/register")
def register_schema(req: SchemaRequest):
    data = load_db_schemas()

    # Initialize db_type if not present
    if req.db_type not in data:
        data[req.db_type] = {}

    if req.db_name not in data[req.db_type]:
        data[req.db_type][req.db_name] = {}

    # Do not overwrite if already registered
    if req.schema_name not in data[req.db_type][req.db_name]:
        data[req.db_type][req.db_name][req.schema_name] = req.business_rules
    else:
        # Optional: append new rules (if not already present)
        existing_rules = set(data[req.db_type][req.db_name][req.schema_name])
        new_rules = set(req.business_rules)
        updated_rules = list(existing_rules.union(new_rules))
        data[req.db_type][req.db_name][req.schema_name] = updated_rules

    save_db_schemas(data)
    return {"message": f"Registered {req.db_type}.{req.db_name}.{req.schema_name}"}

@app.get("/schemas", response_model=None)
def get_schemas():
    return load_db_schemas()

@app.post("/generate-data")
def generate_data(req: DataGenRequest):
    df1 = pd.DataFrame({
        "id": list(range(1, req.num_records + 1)),
        "name": [f"Name{i}" for i in range(1, req.num_records + 1)],
        "db_type": req.db_type,
        "db": req.db_name,
        "schema": req.schema_name,
        "rule_info": "; ".join(req.business_rules) if req.business_rules else None
    })

    df2 = pd.DataFrame({
        "order_id": list(range(1001, 1001 + req.num_records)),
        "amount": [random.randint(100, 999) for _ in range(req.num_records)],
        "prompt_info": req.user_prompt,
        "db_type": req.db_type
    })

    # Convert DataFrames to JSON serializable dicts
    return {
        "Users": df1.to_dict(orient="records"),
        "Orders": df2.to_dict(orient="records")
    }


# --------- Run in PyCharm ---------
if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8080, reload=False)

