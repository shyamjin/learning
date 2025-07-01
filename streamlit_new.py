import time
import streamlit as st
import json
import os
import pandas as pd
import random

DATA_FILE = "data.json"

# Simulated function to return one or more dataframes
def generate_dataframes(num_records, selected_db, selected_schema, user_prompt):
    # Simulating multiple tables (e.g., users and orders)
    df1 = pd.DataFrame({
        "id": list(range(1, num_records + 1)),
        "name": [f"Name{i}" for i in range(1, num_records + 1)],
        "db": selected_db,
        "schema": selected_schema
    })

    df2 = pd.DataFrame({
        "order_id": list(range(1001, 1001 + num_records)),
        "amount": [random.randint(100, 999) for _ in range(num_records)],
        "prompt_info": user_prompt
    })

    return {
        "Users": df1,
        "Orders": df2
    }


# --------- Utility functions ---------
def load_db_schemas():
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, "r") as f:
            return json.load(f)
    return {}

def save_db_schemas(data):
    with open(DATA_FILE, "w") as f:
        json.dump(data, f, indent=2)

st.set_page_config(layout="wide")
# --------- Session state setup ---------
if "db_schemas" not in st.session_state:
    st.session_state.db_schemas = load_db_schemas()

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
            db_name = st.text_input("Database Name")
            schema_name = st.text_input("Schema Name")
            submitted = st.form_submit_button("Submit")

            if submitted:
                if db_name and schema_name:
                    db_schemas = st.session_state.db_schemas
                    if db_name in db_schemas:
                        db_schemas[db_name] = list(set(db_schemas[db_name] + [schema_name]))
                    else:
                        db_schemas[db_name] = [schema_name]

                    st.session_state.db_schemas = db_schemas
                    save_db_schemas(db_schemas)
                    st.success(f"Registered {db_name}.{schema_name}")
                    st.session_state.active_menu = "Request"
                    st.experimental_rerun()
                else:
                    st.error("Please fill both fields.")

# ----------------------
# Request Form inside Container
# ----------------------
elif menu == "Request":
    st.title("Request Data Generation")

    db_schemas = st.session_state.db_schemas

    if not db_schemas:
        st.warning("No registered DBs. Please register first.")
    else:
        with st.container(border=True):
            col_left, col_right = st.columns([100, 4])

            with col_left:
                db_options = list(db_schemas.keys())
                selected_db = st.selectbox("Select Database", db_options)

                schema_options = sorted(db_schemas[selected_db])
                selected_schema = st.selectbox("Select Schema", schema_options)

                num_records = st.number_input("Number of Records to Generate", min_value=1, step=1)

                user_prompt = st.text_area("Enter prompt or instruction")
                submit_request = st.button("Submit Request")
            with col_right:
                st.empty()  # Reserved for preview/output/logs
                # OUTSIDE the form container — full width display
        if submit_request:
            with st.spinner("🔄 Generating data... please wait..."):
                time.sleep(3)
                result_tables = generate_dataframes(num_records, selected_db, selected_schema, user_prompt)

            with st.container():  # Full-width container
                st.success(f"✅ Request submitted for {num_records} records from `{selected_db}.{selected_schema}`")
                st.info(f"💬 Prompt:\n{user_prompt}")
                st.markdown("---")
                if len(result_tables) == 1:
                    table_name, df = list(result_tables.items())[0]
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
                            st.subheader(f"{title} Table")
                            st.dataframe(result_tables[title], use_container_width=True)
                            if st.button(f"💾 Submit {title} to DB", key=f"save_{title}"):
                                try:
                                    # save_df_to_db(df, title, engine)
                                    st.success(f"✅ {title} saved to DB.")
                                except Exception as e:
                                    st.error(f"❌ Failed to save {title}: {e}")

