import pandas as pd
from sdv.multi_table import HMASynthesizer
from sdv.cag import FixedCombinations
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

# Create metadata using the dictionary approach
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

# Combine into a dictionary
# Prepare data dictionary
real_data = {
    'customers': customers_data,
    'orders': orders_data
}

# Create metadata from dictionary
metadata = MultiTableMetadata.load_from_dict(metadata_dict)

# Initialize synthesizer
synthesizer = HMASynthesizer(metadata)

# Create the constraint properly for multi-table
tabular_constraint = FixedCombinations( column_names=['country', 'currency'], table_name="customers")

# Add constraints
synthesizer.add_constraints(constraints=[tabular_constraint])



# Fit model
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

