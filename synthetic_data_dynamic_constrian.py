from sdv.multi_table import HMASynthesizer
import sdv.constraints as sdv_constraints
from sdv.metadata import MultiTableMetadata

import sdv.constraints as sdv_constraints
from sdv.constraints import Constraint
from sdv.multi_table import HMASynthesizer


def create_dynamic_constraints(constraint_specs, metadata):
    """Robust constraint initialization for SDV 1.23.0"""
    constraints_list = []
    
    # Constraint initialization strategies
    CONSTRUCTOR_PARAM_TYPES = [
        'FixedCombinations',
        'UniqueCombinations',
        'Positive',
        'Negative',
        'OneHotEncoding',
        'CustomConstraint'
    ]
    
    SET_PARAMETERS_TYPES = [
        'Inequality',
        'ScalarInequality',
        'Range',
        'FixedIncrements'
    ]
    
    for spec in constraint_specs:
        table_name = spec["table"]
        constraint_type = spec["type"]
        params = spec["params"]
        
        # Validate table exists
        if table_name not in metadata.tables:
            raise ValueError(f"Table '{table_name}' not found in metadata")
        
        # Get constraint class
        constraint_class = getattr(sdv_constraints, constraint_type, None)
        if not constraint_class:
            raise ValueError(f"Constraint type '{constraint_type}' not found in sdv.constraints")
        
        # Validate columns exist
        table_columns = list(metadata.tables[table_name].columns.keys())
        for key, value in params.items():
            if 'column' in key.lower():
                cols = [value] if isinstance(value, str) else value
                for col in cols:
                    if col not in table_columns:
                        raise ValueError(
                            f"Column '{col}' not in table '{table_name}'. "
                            f"Available columns: {table_columns}"
                        )
        
        # Initialize constraint using the correct strategy
        try:
            if constraint_type in CONSTRUCTOR_PARAM_TYPES:
                # Direct initialization with parameters
                constraint = constraint_class(**params)
                
            elif constraint_type in SET_PARAMETERS_TYPES:
                # Special initialization sequence
                constraint = constraint_class()
                constraint._set_parameters(**params)
                
                # Some constraints require additional setup
                if hasattr(constraint, '_fit'):
                    # Create dummy data for fitting
                    dummy_data = pd.DataFrame(columns=[params.get('column_name')] or 
                                            params.get('low_column_name', '') or 
                                            list(params.get('column_names', [])))
                    constraint._fit(dummy_data)
                    
            else:
                # Try both initialization strategies
                try:
                    constraint = constraint_class(**params)
                except TypeError:
                    constraint = constraint_class()
                    if hasattr(constraint, '_set_parameters'):
                        constraint._set_parameters(**params)
                    elif hasattr(constraint, '_fit'):
                        constraint._fit(None)
        
        except Exception as e:
            raise RuntimeError(
                f"Failed to initialize {constraint_type} constraint: {str(e)}\n"
                f"Parameters: {params}\n"
                f"Try different parameter names or constraint type"
            )
        
        constraints_list.append((table_name, constraint))
    
    return constraints_list

def fit_with_dynamic_constraints(metadata, tables, constraint_specs):
    """Fit synthesizer with dynamically provided constraints
    
    Args:
        metadata: SDV MultiTableMetadata
        tables: Dictionary of {table_name: DataFrame}
        constraint_specs: List of constraint dictionaries
        
    Returns:
        Fitted HMASynthesizer
    """
    synthesizer = HMASynthesizer(metadata)
    
    # Process constraints if provided
    constraints = create_dynamic_constraints(constraint_specs, metadata) if constraint_specs else None
    
    # Fit synthesizer
    synthesizer.fit(
        tables=tables,
        constraints=constraints
    )
    
    return synthesizer

# Define any constraints in generic format
user_constraints = [
    {
        "table": "users", 
        "type": "ScalarInequality",
        "params": {"column_name": "age", "relation": ">=", "value": 18}
    },
    {
        "table": "orders",
        "type": "FixedCombinations",
        "params": {"column_names": ["product_id", "discount_group"]}
    },
    {
        "table": "payments",
        "type": "Inequality",
        "params": {"low_column_name": "created_at", "high_column_name": "completed_at"}
    }
]

# Initialize metadata (assuming you have this)
metadata = MultiTableMetadata.load_from_json('your_metadata.json')

# Load your real data
tables = {
    'users': users_df,
    'orders': orders_df,
    'payments': payments_df
}

# Fit synthesizer with dynamic constraints
synthesizer = fit_with_dynamic_constraints(
    metadata=metadata,
    tables=tables,
    constraint_specs=user_constraints
)

# Generate synthetic data
synthetic_data = synthesizer.sample(scale=1.0)


# Minimal working example
from sdv.metadata import MultiTableMetadata
from sdv.constraints import ScalarInequality

metadata = MultiTableMetadata()
metadata.add_table(name='test', data=pd.DataFrame(columns=['age']))

constraint = ScalarInequality(column_name='age', relation='>=', value=18)
synthesizer = HMASynthesizer(metadata)
synthesizer.add_constraints([('test', constraint)])
synthesizer.fit(tables={'test': pd.DataFrame(columns=['age'])})