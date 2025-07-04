import time
import streamlit as st
import json
import os
import pandas as pd
import random
import requests
import streamlit.components.v1 as components

API_BASE_URL = "http://localhost:8000"

def fetch_registered_schemas():
    try:
        res = requests.get(f"{API_BASE_URL}/schemas")
        res.raise_for_status()
        return res.json()
    except Exception as e:
        st.error(f"⚠️ Failed to load schemas: {e}")
        return {}

def generate_dataframes(num_records, selected_db_type, selected_db, selected_schema, user_prompt, business_rules):
    payload = {
        "num_records": num_records,
        "db_type": selected_db_type,
        "db_name": selected_db,
        "schema_name": selected_schema,
        "user_prompt": user_prompt,
        "business_rules": business_rules
    }
    res = requests.post(f"{API_BASE_URL}/generate-data", json=payload)
    return res.json()

def build_uri(db_type, db_name):
    if db_type.lower() == "sqlite":
        return f"sqlite:///{db_name}.db"
    # Extend as needed for other DBs
    return f"postgresql://user:pass@localhost:5432/{db_name}"

st.set_page_config(layout="wide")

@st.cache_data(ttl=60)
def load_schema():
    return fetch_registered_schemas()

if "db_schemas" not in st.session_state:
    st.session_state.db_schemas = load_schema()

if "active_menu" not in st.session_state:
    st.session_state.active_menu = "Request"

menu = st.sidebar.radio("Menu", ["Request"], index=["Request"].index(st.session_state.active_menu))

if menu == "Request":
    st.title("Request Data Generation")

    db_schemas = st.session_state.db_schemas

    if not db_schemas:
        st.warning("No registered DBs. Please register first.")
    else:
        with st.container(border=True):
            col_left, col_right = st.columns([100, 4])

            with col_left:
                db_types = list(db_schemas.keys())
                selected_db_type = st.selectbox("Database Type", db_types, key="db_type")

                db_names = list(db_schemas[selected_db_type].keys())
                selected_db = st.selectbox("Database", db_names, key="db_name")

                schemas = list(db_schemas[selected_db_type][selected_db].keys())
                selected_schema = st.selectbox("Select Schema", schemas, key="schema")

                rules = db_schemas[selected_db_type][selected_db][selected_schema]
                st.text_area("Business Rules", "\n".join(rules), disabled=True, height=120)

                num_records = st.number_input("Number of Records to Generate", min_value=1, step=1)
                user_prompt = st.text_area("Enter prompt or instruction")
                submit_request = st.button("Submit Request")

            with col_right:
                st.info("📡 Relationship Diagram")
                selection_key = f"{selected_db_type}::{selected_db}::{selected_schema}"

                if st.session_state.get("last_selection") != selection_key or "relationship_html" not in st.session_state:
                    st.session_state.last_selection = selection_key
                    with st.spinner("🔄 Loading inferred relationships..."):
                        payload = {
                            "db_type": selected_db_type,
                            "db_uri": build_uri(selected_db_type, selected_db),
                            "schema": selected_schema
                        }
                        try:
                            res = requests.post(f"{API_BASE_URL}/visualize", json=payload)
                            if res.status_code == 200:
                                st.session_state.relationship_html = res.text
                            else:
                                st.error(f"Failed to load relationship diagram: {res.status_code}")
                        except Exception as e:
                            st.error(f"Error loading relationships: {e}")

                if "relationship_html" in st.session_state:
                    components.html(st.session_state.relationship_html, height=600, scrolling=True)

        if submit_request:
            with st.spinner("🔄 Generating data... please wait..."):
                time.sleep(2)
                result_tables = generate_dataframes(num_records, selected_db_type, selected_db, selected_schema, user_prompt, rules)

            with st.container():
                st.success(f"✅ Request submitted for {num_records} records from `{selected_db}.{selected_schema}`")
                st.info(f"💬 Prompt:\n{user_prompt}")
                st.markdown("---")

                if len(result_tables) == 1:
                    table_name, df = list(result_tables.items())[0]
                    st.subheader("Generated Data")
                    st.dataframe(df, use_container_width=True)
                    if st.button(f"💾 Submit {table_name} to DB"):
                        try:
                            st.success(f"✅ {table_name} saved to DB.")
                        except Exception as e:
                            st.error(f"❌ Failed: {e}")
                else:
                    tab_titles = list(result_tables.keys())
                    tabs = st.tabs(tab_titles)
                    for tab, title in zip(tabs, tab_titles):
                        with tab:
                            st.subheader(f"{title} Table")
                            st.dataframe(result_tables[title], use_container_width=True)
                            if st.button(f"💾 Submit {title} to DB", key=f"save_{title}"):
                                try:
                                    st.success(f"✅ {title} saved to DB.")
                                except Exception as e:
                                    st.error(f"❌ Failed to save {title}: {e}")


