from sdv.multi_table import HMASynthesizer
import sdv.constraints as sdv_constraints
from sdv.metadata import MultiTableMetadata

import sdv.constraints as sdv_constraints
from sdv.constraints import Constraint
from sdv.multi_table import HMASynthesizer

def create_dynamic_constraints(constraint_specs, metadata):
    """Create properly initialized SDV constraints for 1.23.0"""
    constraints_list = []
    
    for spec in constraint_specs:
        table_name = spec["table"]
        constraint_type = spec["type"]
        params = spec["params"]
        
        # 1. Validate table exists
        if table_name not in metadata.tables:
            raise ValueError(f"Table '{table_name}' not found in metadata")
        
        # 2. Get constraint class
        try:
            constraint_class = getattr(sdv_constraints, constraint_type)
            if not issubclass(constraint_class, Constraint):
                raise TypeError(f"{constraint_type} is not a valid SDV constraint")
        except AttributeError:
            raise ValueError(f"Constraint type '{constraint_type}' not found in sdv.constraints")
        
        # 3. Validate columns
        table_columns = list(metadata.tables[table_name].columns.keys())
        for param_name, param_value in params.items():
            if 'column' in param_name.lower():
                if isinstance(param_value, str):
                    if param_value not in table_columns:
                        raise ValueError(f"Column '{param_value}' not in table '{table_name}'. Available: {table_columns}")
                elif isinstance(param_value, list):
                    for col in param_value:
                        if col not in table_columns:
                            raise ValueError(f"Column '{col}' not in table '{table_name}'. Available: {table_columns}")
        
        # 4. Create and initialize constraint PROPERLY
        try:
            # SPECIAL HANDLING FOR SDV 1.23.0
            constraint = constraint_class()
            
            # Set parameters using SDV's internal method
            if hasattr(constraint, '_set_parameters'):
                constraint._set_parameters(**params)
            else:
                # Fallback for some constraint types
                for key, value in params.items():
                    setattr(constraint, key, value)
                
                # Required initialization
                if hasattr(constraint, '_fit'):
                    constraint._fit(None)
        except Exception as e:
            raise RuntimeError(f"Failed to initialize {constraint_type}: {str(e)}")
        
        # 5. Add to constraints list
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