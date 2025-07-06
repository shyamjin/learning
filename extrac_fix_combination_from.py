from sqlalchemy import inspect
from langchain_community.utilities import SQLDatabase
from sdv.constraints.tabular import FixedCombinations

def extract_fixed_combinations_constraints(db: SQLDatabase, target_tables: list[str]) -> list:
    engine = db._engine
    inspector = inspect(engine)
    constraints = []

    existing_tables = inspector.get_table_names()

    for table_name in target_tables:
        if table_name not in existing_tables:
            continue  # or raise an error if preferred

        unique_constraints = inspector.get_unique_constraints(table_name)

        for uc in unique_constraints:
            column_names = uc.get("column_names", [])
            if len(column_names) > 1:
                constraint = FixedCombinations(
                    column_names=column_names,
                    table_name=table_name
                )
                constraints.append(constraint)

    return constraints

----------------------------
from sdv.multi_table import HMASynthesizer
from sdv.metadata import MultiTableMetadata
from your_module import extract_fixed_combinations_constraints  # <-- the function above

# Load metadata
metadata = MultiTableMetadata.load_from_dict(metadata_dict)

# Create synthesizer
synthesizer = HMASynthesizer(metadata)

# Extract constraints from PostgreSQL
db = SQLDatabase.from_uri("postgresql+psycopg2://user:password@localhost/dbname")
constraints = extract_fixed_combinations_constraints(db)

# Optionally add custom constraints
constraints.append(AgeGreaterThan20(table_name="customers"))

# Add constraints
synthesizer.add_constraints(constraints)

# Fit and sample
synthesizer.auto_assign_transformers(real_data)
synthesizer.fit(real_data)
synthetic_data = synthesizer.sample(scale=1.0)

