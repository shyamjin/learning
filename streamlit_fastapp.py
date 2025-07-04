import time
import streamlit as st
import json
import os
import pandas as pd
import random

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



# def save_db_schemas(data):
#     with open(DATA_FILE, "w") as f:
#         json.dump(data, f, indent=2)

st.set_page_config(layout="wide")
# # --------- Session state setup ---------
# if "db_schemas" not in st.session_state:
#     st.session_state.db_schemas = load_db_schemas()

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
            col_left, col_right = st.columns([100, 4])

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
                st.empty()  # Reserved for preview/output/logs
                # OUTSIDE the form container — full width display
        if submit_request:
            with st.spinner("🔄 Generating data... please wait..."):
                time.sleep(3)
                result_tables = generate_dataframes(num_records, selected_db_type, selected_db, selected_schema, user_prompt, rules)


            with st.container():  # Full-width container
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
                    for tab, title in zip(tabs, tab_titles):
                        with tab:
                            df = pd.DataFrame(result_tables[title])
                            st.subheader(f"{title} Table")
                            st.dataframe(df, use_container_width=True)
                            if st.button(f"💾 Submit {title} to DB", key=f"save_{title}"):
                                try:
                                    # save_df_to_db(df, title, engine)
                                    st.success(f"✅ {title} saved to DB.")
                                except Exception as e:
                                    st.error(f"❌ Failed to save {title}: {e}")



