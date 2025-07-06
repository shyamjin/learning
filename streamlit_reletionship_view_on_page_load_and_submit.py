# Utility function to call generate-data API (handles both initial load & submit)
def generate_dataframes(num_records=None, selected_db_type=None, selected_db=None, selected_schema=None, user_prompt=None, business_rules=None):
    payload = {}
    if num_records is not None:
        payload = {
            "num_records": num_records,
            "db_type": selected_db_type,
            "db_name": selected_db,
            "schema_name": selected_schema,
            "user_prompt": user_prompt,
            "business_rules": business_rules,
        }
    res = requests.post(f"{API_BASE_URL}/generate-data", json=payload)
    res.raise_for_status()
    return res.json()


# ... inside your "Request" menu block

elif menu == "Request":
    st.title("Request Synthetic Data Generation")

    if "db_schemas" not in st.session_state:
        with st.spinner("Loading schemas..."):
            st.session_state.db_schemas = fetch_registered_schemas()

    db_schemas = st.session_state.db_schemas
    if not db_schemas:
        st.warning("No registered DBs. Please register first.")
    else:
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
            # Show ER diagram depending on session state (initial full or filtered after submit)
            if not st.session_state.get("request_submitted"):
                # Initial load: fetch full schema relationships + empty result_tables
                response = generate_dataframes()
                st.session_state.result_tables = response.get("result_tables", {})
                st.session_state.relationships = response.get("relationships")
                if st.session_state.relationships:
                    dot = generate_graphviz_er_diagram(st.session_state.relationships)
                    st.graphviz_chart(dot, use_container_width=True)
            else:
                # After submit: show filtered relationships from last submit
                if "relationships" in st.session_state:
                    dot = generate_graphviz_er_diagram(st.session_state.relationships)
                    st.graphviz_chart(dot, use_container_width=True)

        if submit_request:
            with st.spinner("🔄 Generating data... please wait..."):
                response = generate_dataframes(
                    num_records, selected_db_type, selected_db, selected_schema, user_prompt, rules
                )
                st.session_state.result_tables = response.get("result_tables", {})
                st.session_state.relationships = response.get("relationships", {})
                st.session_state.num_records = num_records
                st.session_state.prompt = user_prompt
                st.session_state.request_submitted = True
                st.experimental_rerun()

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
                    edited_df = st.data_editor(df, use_container_width=True, key=f"editor_{table_name}")
                    if st.button(f"💾 Submit {table_name} to DB"):
                        try:
                            # save_df_to_db(edited_df, table_name, engine)
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
                            edited_df = st.data_editor(df, use_container_width=True, key=f"editor_{title}")
                            if st.checkbox(f"✅ Select {title} for submission", key=f"select_{title}"):
                                selected_tables.append((title, edited_df))

                    st.markdown("---")
                    if selected_tables:
                        selected_names = [name for name, _ in selected_tables]
                        st.success(f"Tables selected: {', '.join(selected_names)}")
                        if st.button("💾 Submit Selected Tables to DB"):
                            for title, df in selected_tables:
                                try:
                                    # save_df_to_db(df, title, engine)
                                    st.success(f"✅ {title} saved to DB.")
                                except Exception as e:
                                    st.error(f"❌ Failed to save {title}: {e}")
                    else:
                        st.info("👉 Select at least one table to submit.")
