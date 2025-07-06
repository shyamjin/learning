# main.py
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
from typing import Dict, List, Optional
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

@app.post("/register")
def register_schema(req: SchemaRequest):
    data = load_db_schemas()

    if req.db_type not in data:
        data[req.db_type] = {}

    if req.db_name not in data[req.db_type]:
        data[req.db_type][req.db_name] = {}

    if req.schema_name not in data[req.db_type][req.db_name]:
        data[req.db_type][req.db_name][req.schema_name] = req.business_rules
    else:
        existing_rules = set(data[req.db_type][req.db_name][req.schema_name])
        new_rules = set(req.business_rules)
        updated_rules = list(existing_rules.union(new_rules))
        data[req.db_type][req.db_name][req.schema_name] = updated_rules

    save_db_schemas(data)
    return {"message": f"Registered {req.db_type}.{req.db_name}.{req.schema_name}"}

@app.get("/schemas")
def get_schemas():
    return load_db_schemas()

@app.get("/visualize-schema")
def get_mocked_relationship():
    # (Your existing full schema with relationships here)
    return {
        "tables": {
            "customers": {
                "columns": {
                    "customer_id": {"sdtype": "id"},
                    "name": {"sdtype": "categorical"},
                    "email": {"sdtype": "categorical"},
                    "country": {"sdtype": "categorical"}
                },
                "primary_key": "customer_id"
            },
            "orders": {
                "columns": {
                    "order_id": {"sdtype": "id"},
                    "customer_id": {"sdtype": "id"},
                    "employee_id": {"sdtype": "id"},
                    "shipper_id": {"sdtype": "id"},
                    "order_date": {"sdtype": "datetime"}
                },
                "primary_key": "order_id"
            },
            # ... other tables here ...
        },
        "relationships": [
            {"parent_table_name": "customers", "parent_primary_key": "customer_id", "child_table_name": "orders", "child_foreign_key": "customer_id"},
            {"parent_table_name": "employees", "parent_primary_key": "employee_id", "child_table_name": "orders", "child_foreign_key": "employee_id"},
            # ... other relationships here ...
        ]
    }

@app.post("/generate-data")
async def generate_data(request: Request):
    # Read raw JSON data
    payload = await request.json()
    # If no keys or empty payload, return full relationships + empty data
    if not payload or set(payload.keys()) == {""}:
        full_schema = get_mocked_relationship()
        return {
            "result_tables": {},  # empty data
            "relationships": full_schema
        }

    # Otherwise, parse into DataGenRequest model
    try:
        data_req = DataGenRequest(**payload)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid request payload: {e}")

    # Generate fake data for demonstration
    n = data_req.num_records

    df1 = pd.DataFrame({
        "id": list(range(1, n + 1)),
        "name": [f"Name{i}" for i in range(1, n + 1)],
        "db_type": data_req.db_type,
        "db": data_req.db_name,
        "schema": data_req.schema_name,
        "rule_info": "; ".join(data_req.business_rules) if data_req.business_rules else None
    })

    df2 = pd.DataFrame({
        "order_id": list(range(1001, 1001 + n)),
        "amount": [random.randint(100, 999) for _ in range(n)],
        "prompt_info": data_req.user_prompt,
        "db_type": data_req.db_type
    })

    # Filter relationships for only tables included in generated data
    generated_table_names = ["Users", "Orders"]
    full_schema = get_mocked_relationship()
    def filter_relationships(schema, table_names):
        filtered_tables = {k: v for k, v in schema["tables"].items() if k in [t.lower() for t in table_names]}
        filtered_relationships = [
            rel for rel in schema["relationships"]
            if rel["parent_table_name"] in filtered_tables and rel["child_table_name"] in filtered_tables
        ]
        return {"tables": filtered_tables, "relationships": filtered_relationships}

    filtered_schema = filter_relationships(full_schema, generated_table_names)

    return {
        "result_tables": {
            "Users": df1.to_dict(orient="records"),
            "Orders": df2.to_dict(orient="records")
        },
        "relationships": filtered_schema
    }

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8080, reload=False)
