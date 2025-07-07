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
@app.get("/visualize-schema")
def get_mocked_relationship():
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
        "products": {
            "columns": {
                "product_id": {"sdtype": "id"},
                "product_name": {"sdtype": "categorical"},
                "category_id": {"sdtype": "id"},
                "price": {"sdtype": "numerical"}
            },
            "primary_key": "product_id"
        },
        "categories": {
            "columns": {
                "category_id": {"sdtype": "id"},
                "category_name": {"sdtype": "categorical"}
            },
            "primary_key": "category_id"
        },
        "payments": {
            "columns": {
                "payment_id": {"sdtype": "id"},
                "order_id": {"sdtype": "id"},
                "payment_method": {"sdtype": "categorical"},
                "amount": {"sdtype": "numerical"},
                "payment_date": {"sdtype": "datetime"}
            },
            "primary_key": "payment_id"
        },
        "employees": {
            "columns": {
                "employee_id": {"sdtype": "id"},
                "name": {"sdtype": "categorical"},
                "department_id": {"sdtype": "id"}
            },
            "primary_key": "employee_id"
        },
        "departments": {
            "columns": {
                "department_id": {"sdtype": "id"},
                "department_name": {"sdtype": "categorical"}
            },
            "primary_key": "department_id"
        },
        "shippers": {
            "columns": {
                "shipper_id": {"sdtype": "id"},
                "shipper_name": {"sdtype": "categorical"},
                "contact_number": {"sdtype": "categorical"}
            },
            "primary_key": "shipper_id"
        },
        "order_items": {
            "columns": {
                "item_id": {"sdtype": "id"},
                "order_id": {"sdtype": "id"},
                "product_id": {"sdtype": "id"},
                "quantity": {"sdtype": "numerical"}
            },
            "primary_key": "item_id"
        },
        "reviews": {
            "columns": {
                "review_id": {"sdtype": "id"},
                "product_id": {"sdtype": "id"},
                "customer_id": {"sdtype": "id"},
                "rating": {"sdtype": "numerical"},
                "review_date": {"sdtype": "datetime"}
            },
            "primary_key": "review_id"
        }
    },
    "relationships": [
        {"parent_table_name": "customers", "parent_primary_key": "customer_id", "child_table_name": "orders", "child_foreign_key": "customer_id"},
        {"parent_table_name": "employees", "parent_primary_key": "employee_id", "child_table_name": "orders", "child_foreign_key": "employee_id"},
        {"parent_table_name": "shippers", "parent_primary_key": "shipper_id", "child_table_name": "orders", "child_foreign_key": "shipper_id"},
        {"parent_table_name": "orders", "parent_primary_key": "order_id", "child_table_name": "payments", "child_foreign_key": "order_id"},
        {"parent_table_name": "departments", "parent_primary_key": "department_id", "child_table_name": "employees", "child_foreign_key": "department_id"},
        {"parent_table_name": "categories", "parent_primary_key": "category_id", "child_table_name": "products", "child_foreign_key": "category_id"},
        {"parent_table_name": "orders", "parent_primary_key": "order_id", "child_table_name": "order_items", "child_foreign_key": "order_id"},
        {"parent_table_name": "products", "parent_primary_key": "product_id", "child_table_name": "order_items", "child_foreign_key": "product_id"},
        {"parent_table_name": "products", "parent_primary_key": "product_id", "child_table_name": "reviews", "child_foreign_key": "product_id"},
        {"parent_table_name": "customers", "parent_primary_key": "customer_id", "child_table_name": "reviews", "child_foreign_key": "customer_id"}
    ]
}



# --------- Run in PyCharm ---------
if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8080, reload=False)

