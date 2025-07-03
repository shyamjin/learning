from sdv.multi_table import HMASynthesizer
import sdv.constraints as sdv_constraints
from sdv.metadata import MultiTableMetadata

def create_dynamic_constraints(constraint_specs, metadata):
    """Dynamically create SDV constraint objects from generic specifications
    
    Args:
        constraint_specs: List of constraint dictionaries
        metadata: SDV MultiTableMetadata object
        
    Returns:
        Dictionary of {table_name: [constraint_objects]}
    """
    constraints_dict = {}
    
    for spec in constraint_specs:
        table_name = spec["table"]
        constraint_type = spec["type"]
        params = spec["params"]
        
        # Validate table exists
        if table_name not in metadata.tables:
            raise ValueError(f"Table '{table_name}' not found in metadata")
        
        # Dynamically get constraint class
        constraint_class = getattr(sdv_constraints, constraint_type, None)
        if not constraint_class:
            raise ValueError(f"Constraint type '{constraint_type}' not found in sdv.constraints")
        
        # Validate columns exist in table
        table_columns = set(metadata.tables[table_name].columns.keys())
        for key, value in params.items():
            # Handle column references (single or multiple)
            if 'column' in key.lower():
                if isinstance(value, str):
                    if value not in table_columns:
                        raise ValueError(f"Column '{value}' not found in table '{table_name}'")
                elif isinstance(value, list):
                    for col in value:
                        if col not in table_columns:
                            raise ValueError(f"Column '{col}' not found in table '{table_name}'")
        
        # Instantiate constraint
        try:
            constraint = constraint_class(**params)
        except TypeError as e:
            raise ValueError(f"Invalid parameters for {constraint_type}: {str(e)}")
        
        # Add to constraints dictionary
        if table_name not in constraints_dict:
            constraints_dict[table_name] = []
        constraints_dict[table_name].append(constraint)
    
    return constraints_dict

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