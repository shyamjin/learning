
from sdv.cag import ProgrammableConstraint, FixedCombinations


class AgeGreaterThan20(ProgrammableConstraint):
    def __init__(self, table_name):
        self.table_name = table_name
        self.column_name = 'age'
        self.min_age = 30

    def fit(self, data, metadata):
        # No fitting required for this constraint
        pass

    def transform(self, data):
        # No transformation needed before model training
        return data

    def get_updated_metadata(self, metadata):
        return metadata

    def reverse_transform(self, transformed_data):
        # Filter synthetic data to keep only rows where age > 20
        df = transformed_data[self.table_name]
        mask = df[self.column_name] > self.min_age
        transformed_data[self.table_name] = df[mask].reset_index(drop=True)
        return transformed_data

    def is_valid(self, data):
        # Check which rows satisfy the age > 20 condition
        df = data[self.table_name]
        is_valid_series = df[self.column_name] > self.min_age
        return {self.table_name: is_valid_series}




import pandas as pd
from sdv.multi_table import HMASynthesizer
from sdv.metadata import MultiTableMetadata

# Create sample data
customers_data = pd.DataFrame({
    'customer_id': [1, 2, 3, 4, 5],
    'country': ['USA', 'Canada', 'USA', 'Mexico', 'Canada'],
    'currency': ['USD', 'CAD', 'USD', 'MXN', 'CAD'],
    'age': [25, 40, 33, 28, 45]
})

orders_data = pd.DataFrame({
    'order_id': [101, 102, 103, 104],
    'customer_id': [1, 2, 4, 5],
    'amount': [100.50, 200.75, 50.25, 300.00],
    'date': ['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04']
})

# Create metadata
metadata_dict = {
    'tables': {
        'customers': {
            'columns': {
                'customer_id': {'sdtype': 'id'},
                'country': {'sdtype': 'categorical'},
                'currency': {'sdtype': 'categorical'},
                'age': {'sdtype': 'numerical'}
            },
            'primary_key': 'customer_id'
        },
        'orders': {
            'columns': {
                'order_id': {'sdtype': 'id'},
                'customer_id': {'sdtype': 'id'},
                'amount': {'sdtype': 'numerical'},
                'date': {'sdtype': 'datetime', 'datetime_format': '%Y-%m-%d'}
            },
            'primary_key': 'order_id'
        }
    },
    'relationships': [
        {
            'parent_table_name': 'customers',
            'parent_primary_key': 'customer_id',
            'child_table_name': 'orders',
            'child_foreign_key': 'customer_id'
        }
    ]
}

# Prepare real data dictionary
real_data = {
    'customers': customers_data,
    'orders': orders_data
}

# Define constraints
constraint_specs = [
    # Fixed country-currency combinations in customers table
    {
        "type": "FixedCombinations",
        "table": "customers",
        "params": {
            "column_names": ["country", "currency"]
        }
    },
    # Age range constraint in customers table
    {
        "type": "Range",
        "table": "customers",
        "params": {
            "column_name": "age",
            "min_value": 18,
            "max_value": 100
        }
    },
    # Positive order amounts in orders table
    {
        "type": "Range",
        "table": "orders",
        "params": {
            "column_name": "amount",
            "min_value": 0
        }
    }
]

# Load metadata
metadata = MultiTableMetadata.load_from_dict(metadata_dict)

# Create synthesizer
synthesizer = HMASynthesizer(metadata)

tabular_constraint = FixedCombinations( column_names=['country', 'currency'], table_name="customers")

# Add constraints to synthesizer
synthesizer.add_constraints(constraints=[AgeGreaterThan20(table_name='customers'), tabular_constraint])

# Continue with fitting process
synthesizer.auto_assign_transformers(real_data)
synthesizer.fit(real_data)

# Generate synthetic data
synthetic_data = synthesizer.sample(scale=1.0)

# Access results
synthetic_customers = synthetic_data['customers']
synthetic_transactions = synthetic_data['orders']

print("Synthetic Customers:")
print(synthetic_customers)
print("\nSynthetic orders:")
print(synthetic_transactions)

# Verify constraint
print("\nOriginal combinations:")
print(customers_data[['country', 'currency']].drop_duplicates())
print("\nSynthetic combinations:")
print(synthetic_customers[['country', 'currency']].drop_duplicates())