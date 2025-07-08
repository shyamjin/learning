import streamlit as st
import pandas as pd
from faker import Faker
import requests

fake = Faker()

API_ENDPOINT = "http://localhost:8080/submit_data"  # Replace with your real endpoint

# Sample data (can come from DB or CSV)
df = pd.DataFrame({
    'customer_id': [1, 2, 3],
    'customer_name': ['Alice', 'Bob', 'Charlie'],
    'email': ['alice@example.com', 'bob@example.com', 'charlie@example.com'],
    'score': [91.5, 78.2, 88.0],
    'is_active': [True, False, True]
})

# Keep the modified df in session state
if "modified_df" not in st.session_state:
    st.session_state.modified_df = df.copy()

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

st.title("🧪 In-place Faker + Submit")

# Step 1: Let user select columns to fakerize
columns = st.multiselect(
    "🔐 Select columns to replace with fake values (applied directly below)",
    options=st.session_state.modified_df.columns.tolist()
)

# Step 2: Apply Faker on button
if st.button("🎲 Apply Faker to Selected Columns"):
    df = st.session_state.modified_df
    for col in columns:
        df[col] = fakerize_column(col, df[col].dtype, len(df))
    st.success(f"✅ Faker applied to: {', '.join(columns)}")

# Step 3: Show editable dataframe
st.subheader("📋 Editable Table")
edited_df = st.data_editor(
    st.session_state.modified_df,
    use_container_width=True,
    key="editable_data"
)

# Step 4: Submit final version to backend
if st.button("🚀 Submit to Backend API"):
    try:
        payload = edited_df.to_dict(orient='records')
        response = requests.post(API_ENDPOINT, json={"data": payload})
        if response.status_code == 200:
            st.success("✅ Successfully submitted to backend!")
            st.json(response.json())
        else:
            st.error(f"❌ Submission failed: {response.status_code}")
            st.text(response.text)
    except Exception as e:
        st.error(f"🚨 Error during submission: {e}")


