if selected_tables:
    selected_names = [name for name, _ in selected_tables]
    st.success(f"Tables selected: {', '.join(selected_names)}")

    if st.button("♻️ Resample Selected Tables using SDV"):
        try:
            with st.spinner("🔄 Resampling synthetic data..."):
                payload = {
                    "tables": {name: df.to_dict(orient="records") for name, df in selected_tables},
                    "num_records": num_records
                }
                res = requests.post(f"{API_BASE_URL}/resample-data", json=payload)
                res.raise_for_status()
                resampled_data = res.json().get("resampled_tables", {})

                # Store and render new synthetic data in tabs
                st.session_state.resampled_tables = resampled_data

        except Exception as e:
            st.error(f"❌ Resample failed: {e}")

--------------------
if "resampled_tables" in st.session_state:
    st.markdown("### ♻️ Resampled Synthetic Data")
    resampled_tables = st.session_state.resampled_tables
    resampled_tabs = st.tabs(list(resampled_tables.keys()))
    for tab, table_name in zip(resampled_tabs, resampled_tables):
        with tab:
            df = pd.DataFrame(resampled_tables[table_name])
            st.dataframe(df, use_container_width=True)


