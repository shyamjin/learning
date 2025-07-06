class SchemaRequest(BaseModel):
    db_type: str
    db_server: str
    db_name: str
    schema_name: str
    business_rules: List[str] = []
------------
@app.post("/register")
def register_schema(req: SchemaRequest):
    data = load_db_schemas()

    # Add nested structure: db_type → db_server → db_name → schema
    if req.db_type not in data:
        data[req.db_type] = {}

    if req.db_server not in data[req.db_type]:
        data[req.db_type][req.db_server] = {}

    if req.db_name not in data[req.db_type][req.db_server]:
        data[req.db_type][req.db_server][req.db_name] = {}

    if req.schema_name not in data[req.db_type][req.db_server][req.db_name]:
        data[req.db_type][req.db_server][req.db_name][req.schema_name] = req.business_rules
    else:
        existing_rules = set(data[req.db_type][req.db_server][req.db_name][req.schema_name])
        new_rules = set(req.business_rules)
        updated_rules = list(existing_rules.union(new_rules))
        data[req.db_type][req.db_server][req.db_name][req.schema_name] = updated_rules

    save_db_schemas(data)
    return {"message": f"Registered {req.db_type}.{req.db_server}.{req.db_name}.{req.schema_name}"}
-----------------Register Form-------------------------
           db_type = st.selectbox("Database Type", ["PostgreSQL", "Oracle", "SQLite", "MySQL", "MSSQL"])
            db_server = st.text_input("Database Server")
            db_name = st.text_input("Database Name")
            schema_name = st.text_input("Schema Name")

----------------register_schema_api()-------------

def register_schema_api(db_type, db_server, db_name, schema_name, business_rules):
    try:
        payload = {
            "db_type": db_type,
            "db_server": db_server,
            "db_name": db_name,
            "schema_name": schema_name,
            "business_rules": business_rules
        }
        res = requests.post(f"{API_BASE_URL}/register", json=payload)
        res.raise_for_status()
        return res.json()["message"]
    except Exception as e:
        st.error(f"⚠️ Failed to register: {e}")
        return None
-------------register_schema_api-------------

msg = register_schema_api(db_type, db_server, db_name, schema_name, business_rules)


--------------output-----------------
{
  "PostgreSQL": {
    "server1": {
      "aa": {
        "aa": ["area should be in"]
      }
    },
    "server2": {
      "abc": {
        "axx": ["asfaf"]
      }
    }
  }
}
----------------request data type---------------
class DataGenRequest(BaseModel):
    num_records: int
    db_type: str
    db_server: str
    db_name: str
    schema_name: str
    user_prompt: str
    business_rules: List[str] = []
----------------- Generate data ---------------
@app.post("/generate-data")
def generate_data(req: DataGenRequest):
    df1 = pd.DataFrame({
        "id": list(range(1, req.num_records + 1)),
        "name": [f"Name{i}" for i in range(1, req.num_records + 1)],
        "db_type": req.db_type,
        "db_server": req.db_server,
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

    return {
        "Users": df1.to_dict(orient="records"),
        "Orders": df2.to_dict(orient="records")
    }
-------------------request form ---------------
with col_left:
    db_types = list(db_schemas.keys())
    selected_db_type = st.selectbox("Database Type", db_types)

    db_servers = list(db_schemas[selected_db_type].keys())
    selected_db_server = st.selectbox("Database Server", db_servers)

    db_names = list(db_schemas[selected_db_type][selected_db_server].keys())
    selected_db = st.selectbox("Database Name", db_names)

    schemas = db_schemas[selected_db_type][selected_db_server][selected_db]
    selected_schema = st.selectbox("Select Schema", schemas)

    rules = db_schemas[selected_db_type][selected_db_server][selected_db][selected_schema]
    st.text_area("Business Rules", "\n".join(rules), disabled=True, height=120)

    num_records = st.number_input("Number of Records to Generate", min_value=1, step=1)
    user_prompt = st.text_area("Enter prompt or instruction")
    submit_request = st.button("Submit Request")
---------------generate_dataframes()------------------
def generate_dataframes(num_records, selected_db_type, selected_db_server, selected_db, selected_schema, user_prompt, business_rules):
    payload = {
        "num_records": num_records,
        "db_type": selected_db_type,
        "db_server": selected_db_server,
        "db_name": selected_db,
        "schema_name": selected_schema,
        "user_prompt": user_prompt,
        "business_rules": business_rules
    }
    res = requests.post(f"{API_BASE_URL}/generate-data", json=payload)
    return res.json()
-----------------And update the call:-----------------------
result_tables = generate_dataframes(
    num_records, selected_db_type, selected_db_server,
    selected_db, selected_schema, user_prompt, rules
)
-------------
[Database Type]
  ↓
[Database Server]
  ↓
[Database Name]
  ↓
[Schema]



