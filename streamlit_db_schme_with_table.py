import hashlib
import time

import graphviz
import streamlit as st
import json
import os
import pandas as pd
from pyvis.network import Network
import streamlit.components.v1 as components

import requests

API_BASE_URL = "http://localhost:8080"  # Adjust if different



# Simulated function to return one or more dataframes
def generate_dataframes(num_records, selected_db_type, selected_db, selected_schema, user_prompt, business_rules):
    payload = {
        "num_records": num_records,
        "db_type": selected_db_type,
        "db_name": selected_db,
        "schema_name": selected_schema,
        "user_prompt": user_prompt,
        "business_rules": rules
    }
    res = requests.post(f"{API_BASE_URL}/generate-data", json=payload)
    return res.json()



# --------- Utility functions ---------
@st.cache_data(ttl=60)  # Cache for 60 seconds (adjust as needed)
def fetch_registered_schemas():
    try:
        res = requests.get(f"{API_BASE_URL}/schemas")
        res.raise_for_status()
        return res.json()
    except Exception as e:
        st.error(f"⚠️ Failed to load schemas: {e}")
        return {}

def register_schema_api(db_type, db_name, schema_name, business_rules):
    try:
        payload = {"db_name": db_name, "schema_name": schema_name, "db_type": db_type, "business_rules": business_rules}
        res = requests.post(f"{API_BASE_URL}/register", json=payload)
        res.raise_for_status()
        return res.json()["message"]
    except Exception as e:
        st.error(f"⚠️ Failed to register: {e}")
        return None



@st.cache_data(ttl=60)
def fetch_schema_visualization():
    try:
        res = requests.get(f"{API_BASE_URL}/visualize-schema")
        res.raise_for_status()
        return res.json()
    except Exception as e:
        st.error(f"⚠️ Visualization failed: {e}")
        return None

def generate_graphviz_er_diagram(metadata: dict):
    dot = graphviz.Digraph(engine="dot")
    # Color generator using hash
    def get_color(name: str):
        h = int(hashlib.md5(name.encode()).hexdigest(), 16)
        return f"#{h % 0xFFFFFF:06x}"

    for table_name, table_info in metadata["tables"].items():
        primary_key = table_info.get("primary_key", "")
        columns = table_info["columns"]

        # Format each column with sdtype
        column_lines = []
        for col, meta in columns.items():
            sdtype = meta.get("sdtype", "unknown")
            prefix = "🔑 " if col == primary_key else ""
            column_lines.append(f"{prefix}{col}: {sdtype}")

        fields = "\l".join(column_lines) + "\l"  # Left aligned record-style label

        dot.node(
            table_name,
            label=f"{{{table_name}|{fields}}}",
            shape="record",
            style="filled",
            fillcolor=get_color(table_name),
            fontcolor="white",
            color="black"
        )

    for rel in metadata["relationships"]:
        parent = rel["parent_table_name"]
        child = rel["child_table_name"]
        fk = rel["child_foreign_key"]
        pk = rel["parent_primary_key"]

        dot.edge(
            child,
            parent,
            label=f"{child}.{fk} ➜ {parent}.{pk}",
            color="orange",
            fontcolor="orange"
        )

    return dot

st.set_page_config(layout="wide")
# # --------- Session state setup ---------
# if "db_schemas" not in st.session_state:
#     st.session_state.db_schemas = load_db_schemas()
st.markdown("""
<style>
[data-testid="stVerticalBlockBorderWrapper"]{
        height: 100% !important;
        align-content: center;
}
</style>
""", unsafe_allow_html=True)

if "active_menu" not in st.session_state:
    st.session_state.active_menu = "Register"

# --------- Sidebar menu ---------
menu = st.sidebar.radio("Menu", ["Register", "Request"],
                        index=["Register", "Request"].index(st.session_state.active_menu))

# ----------------------
# Register Form
# ----------------------
if menu == "Register":
    st.title("Register Database")

    col1, col2 = st.columns([12,1])
    with col1:
        with st.form("register_form"):
            db_type = st.selectbox("Database Type", ["PostgreSQL", "Oracle", "SQLite", "MySQL", "MSSQL"])
            db_name = st.text_input("Database Name")
            schema_name = st.text_input("Schema Name")
            business_rules_text = st.text_area("Business Rules (one per line)")
            business_rules = [r.strip() for r in business_rules_text.strip().split("\n") if r.strip()]
            submitted = st.form_submit_button("Submit")

            if submitted:
                if db_name and schema_name:
                    msg = register_schema_api(db_type, db_name, schema_name, business_rules)
                    if msg:
                        # Show success toast before rerun
                        st.toast(f"✅ {msg}", icon="🎉")
                        st.cache_data.clear()  # Clear to refresh schema list
                        time.sleep(1.5)  # Give user a moment to see the toast
                        st.session_state.active_menu = "Request"
                        st.experimental_rerun()
                else:
                    st.error("❌ Please fill both fields.")

# ----------------------
# Request Form inside Container
# ----------------------
elif menu == "Request":
    st.title("Request Synthetic Data Generation")

    if "db_schemas" not in st.session_state:
        with st.spinner("Loading schemas..."):
            st.session_state.db_schemas = fetch_registered_schemas()

    db_schemas = st.session_state.db_schemas
    if not db_schemas:
        st.warning("No registered DBs. Please register first.")
    else:
        with st.container(border=True):
            col_left, col_right = st.columns([12, 12])

            with col_left:
                db_types = list(db_schemas.keys())
                selected_db_type = st.selectbox("Database Type", db_types)

                db_names = list(db_schemas[selected_db_type].keys())
                selected_db = st.selectbox("Database", db_names)

                schemas = db_schemas[selected_db_type][selected_db]
                selected_schema = st.selectbox("Select Schema", schemas)
                rules = db_schemas[selected_db_type][selected_db][selected_schema]
                st.text_area("Business Rules", "\n".join(rules), disabled=True, height=120)

                num_records = st.number_input("Number of Records to Generate", min_value=1, step=1)

                user_prompt = st.text_area("Enter prompt or instruction")
                submit_request = st.button("Submit Request")
            with col_right:
                vis_data = fetch_schema_visualization()
                if vis_data:
                    dot = generate_graphviz_er_diagram(vis_data)
                    with st.container():
                        st.graphviz_chart(dot, use_container_width=True)

        # Trigger request and store result in session state
        if submit_request:
            with st.spinner("🔄 Generating data... please wait..."):
                result_tables = generate_dataframes(num_records, selected_db_type, selected_db, selected_schema,
                                                    user_prompt, rules)
                st.session_state.result_tables = result_tables
                st.session_state.num_records = num_records
                st.session_state.prompt = user_prompt
                st.session_state.request_submitted = True

        # Show result if available in session
        if st.session_state.get("request_submitted") and st.session_state.get("result_tables"):
            result_tables = st.session_state.result_tables
            user_prompt = st.session_state.prompt
            num_records = st.session_state.num_records

            with st.container():
                st.success(f"✅ Request submitted for {num_records} records from `{selected_db}.{selected_schema}`")
                st.info(f"💬 Prompt:\n{user_prompt}")
                st.markdown("---")

                if len(result_tables) == 1:
                    table_name, records = list(result_tables.items())[0]
                    df = pd.DataFrame(records)
                    st.subheader("Generated Data")
                    st.dataframe(df, use_container_width=True)
                    if st.button(f"💾 Submit {table_name} to DB"):
                        try:
                            # save_df_to_db(df, table_name, engine)
                            st.success(f"✅ {table_name} saved to DB.")
                        except Exception as e:
                            st.error(f"❌ Failed: {e}")
                else:
                    tab_titles = list(result_tables.keys())
                    tabs = st.tabs(tab_titles)
                    selected_tables = []
                    for tab, title in zip(tabs, tab_titles):
                        with tab:
                            df = pd.DataFrame(result_tables[title])
                            st.subheader(f"{title} Table")
                            st.dataframe(df, use_container_width=True)
                            if st.checkbox(f"✅ Select {title} for submission", key=f"select_{title}"):
                                selected_tables.append((title, df))

                    st.markdown("---")
                    if selected_tables:
                        selected_names = [name for name, _ in selected_tables]
                        st.success(f"Tables selected: {', '.join(selected_names)}")
                        if st.button("💾 Submit Selected Tables to DB"):
                            for title, df in selected_tables:
                                try:
                                    # save_df_to_db(df, title, engine)
                                    st.success(f"✅ {df} saved to DB.")
                                except Exception as e:
                                    st.error(f"❌ Failed to save {title}: {e}")
                    else:
                        st.info("👉 Select at least one table to submit.")



