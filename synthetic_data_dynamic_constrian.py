from sdv.multi_table import HMASynthesizer
import sdv.constraints as sdv_constraints
from sdv.metadata import MultiTableMetadata

import sdv.constraints as sdv_constraints
from sdv.constraints import Constraint
from sdv.multi_table import HMASynthesizer



def create_dynamic_constraints(constraint_specs, metadata):
    """Properly initializes SDV constraints for version 1.23.0"""
    constraints_list = []
    
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
        
        # SPECIAL HANDLING FOR SPECIFIC CONSTRAINT TYPES
        try:
            # For constraints that require constructor parameters
            if constraint_type in ["FixedCombinations", "UniqueCombinations"]:
                constraint = constraint_class(**params)
            
            # For constraints that need special initialization
            elif constraint_type in ["Inequality", "ScalarInequality", "Range"]:
                constraint = constraint_class()
                constraint._set_parameters(**params)
            
            # For other constraints
            else:
                try:
                    # First try with parameters in constructor
                    constraint = constraint_class(**params)
                except TypeError:
                    # Fallback to parameter setting
                    constraint = constraint_class()
                    constraint._set_parameters(**params)
        except Exception as e:
            raise RuntimeError(
                f"Failed to initialize {constraint_type} constraint: {str(e)}\n"
                f"Parameters: {params}"
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