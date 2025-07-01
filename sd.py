# pipeline_with_rules.py

from sqlalchemy import create_engine, MetaData, inspect
import pandas as pd
import os

from sdv.metadata import MultiTableMetadata
from sdv.multi_table import HMASynthesizer
from sdv.constraints import FixedCombinations, Inequality, CustomConstraint

# --- Step 1: Extract schema and data dynamically ---

def extract_schema_and_data(connection_str):
    engine = create_engine(connection_str)
    inspector = inspect(engine)
    metadata = MetaData()
    metadata.reflect(bind=engine)

    tables = {}
    with engine.connect() as conn:
        for table_name in inspector.get_table_names():
            df = pd.read_sql_table(table_name, conn)
            tables[table_name] = df
    return tables, metadata

# --- Step 2: Detect metadata and relationships with SDV ---

def generate_metadata(tables):
    metadata = MultiTableMetadata()
    metadata.detect(tables)
    return metadata

# --- Step 3: Define business rules as SDV constraints ---

def get_constraints():
    constraints = []

    # Example 1: department names limited to ["CS", "EC"]
    constraints.append(
        FixedCombinations(
            column_names=['name'],
            table_name='departments',
            # Only allow these values in 'name'
            allowed_combinations=[('CS',), ('EC',)]
        )
    )

    # Example 2: user age between 21 and 60
    constraints.append(
        Inequality(
            low_column='age',
            high_column='age',
            low_value=21,
            high_value=60,
            allow_equal=True,
            table_name='users'
        )
    )

    # You can add more custom constraints here...

    return constraints

# --- Step 4: Train synthesizer with constraints ---

def train_and_generate(tables, metadata, constraints, sample_size=1.0):
    synthesizer = HMASynthesizer(metadata, constraints=constraints)
    synthesizer.fit(tables)
    synthetic_data = synthesizer.sample(scale=sample_size)
    return synthetic_data

# --- Step 5: Export generated data ---

def export_to_csv(data: dict, folder: str):
    os.makedirs(folder, exist_ok=True)
    for table_name, df in data.items():
        df.to_csv(os.path.join(folder, f"{table_name}.csv"), index=False)

# --- Main pipeline runner ---

def run_pipeline_with_rules(connection_str, output_folder="output", sample_size=1.0):
    print("🔍 Extracting schema and data from DB...")
    tables, _ = extract_schema_and_data(connection_str)

    print("📐 Detecting metadata...")
    metadata = generate_metadata(tables)

    print("⚙️ Preparing constraints...")
    constraints = get_constraints()

    print("🧠 Training synthesizer with constraints...")
    synthetic = train_and_generate(tables, metadata, constraints, sample_size)

    print("💾 Exporting synthetic data to CSV...")
    export_to_csv(synthetic, output_folder)

    print("✅ Done!")

# --- Example usage ---

if __name__ == "__main__":
    # Change this connection string to your real DB or use sample SQLite DB
    conn_str = "sqlite:///sample.db"
    run_pipeline_with_rules(conn_str, output_folder="output_with_rules", sample_size=2)
