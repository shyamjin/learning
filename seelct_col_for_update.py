from faker import Faker
fake = Faker()

def fakerize_column(col_name, dtype, n_rows):
    if pd.api.types.is_string_dtype(dtype):
        return [fake.word() for _ in range(n_rows)]
    elif pd.api.types.is_integer_dtype(dtype):
        return [fake.random_int(min=1000, max=9999) for _ in range(n_rows)]
    elif pd.api.types.is_float_dtype(dtype):
        return [round(fake.pyfloat(left_digits=2, right_digits=2, positive=True), 2) for _ in range(n_rows)]
    elif pd.api.types.is_bool_dtype(dtype):
        return [fake.boolean() for _ in range(n_rows)]
    else:
        return [f"UNK-{i}" for i in range(n_rows)]

# Inside your multi-tab UI
tab_titles = list(result_tables.keys())
tabs = st.tabs(tab_titles)
selected_tables = []

for tab, title in zip(tabs, tab_titles):
    with tab:
        df_key = f"modified_df_{title}"
        if df_key not in st.session_state:
            st.session_state[df_key] = pd.DataFrame(result_tables[title])

        st.write(f"### Preview: `{title}`")
        # Column selection for fakerization
        selected_cols = st.multiselect(
            f"🎭 Select columns in `{title}` to replace with Faker values",
            options=st.session_state[df_key].columns.tolist(),
            key=f"faker_cols_{title}"
        )

        if st.button(f"🎲 Apply Faker to selected columns in `{title}`"):
            df = st.session_state[df_key]
            for col in selected_cols:
                dtype = df[col].dtype
                df[col] = fakerize_column(col, dtype, len(df))
            st.success(f"✅ Faker applied to {', '.join(selected_cols)} in `{title}`")

        # Editable table
        edited_df = st.data_editor(st.session_state[df_key], use_container_width=True, key=f"editor_{title}")
        
        # Save edited df back
        st.session_state[df_key] = edited_df

        if st.checkbox(f"✅ Select `{title}` for submission", key=f"select_{title}"):
            selected_tables.append((title, edited_df))
