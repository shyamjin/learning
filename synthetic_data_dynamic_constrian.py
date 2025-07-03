from sdv.multi_table import HMASynthesizer
import sdv.constraints as sdv_constraints
from sdv.metadata import MultiTableMetadata

import sdv.constraints as sdv_constraints
from sdv.constraints import Constraint
from sdv.multi_table import HMASynthesizer


import pandas as pd
from sdv import constraints as sdv_constraints

def create_dynamic_constraints(constraint_specs, metadata):
    """Create properly initialized SDV constraints for version 1.23.0"""
    constraints_list = []
    
    # Types of constraints that accept params directly in constructor
    CONSTRUCTOR_PARAM_TYPES = [
        'UniqueCombinations',
        'Positive',
        'Negative',
        'OneHotEncoding',
        'CustomConstraint'
    ]
    
    # Types that require special _set_parameters + _fit calls
    SET_PARAMETERS_TYPES = [
        'Inequality',
        'ScalarInequality',
        'Range',
        'FixedIncrements'
    ]
    
    # Multi-table constraints requiring table_name as first positional argument
    MULTI_TABLE_CONSTRAINTS = [
        'FixedCombinations',
    ]
    
    for spec in constraint_specs:
        table_name = spec["table"]
        constraint_type = spec["type"]
        params = spec.get("params", {})
        
        if table_name not in metadata.tables:
            raise ValueError(f"Table '{table_name}' not found in metadata")
        
        constraint_class = getattr(sdv_constraints, constraint_type, None)
        if not constraint_class:
            raise ValueError(f"Constraint type '{constraint_type}' not found in sdv.constraints")
        
        # Validate columns exist in the table schema
        table_columns = list(metadata.tables[table_name].columns.keys())
        for key, value in params.items():
            if 'column' in key.lower():
                cols = [value] if isinstance(value, str) else value
                for col in cols:
                    if col and col not in table_columns:
                        raise ValueError(
                            f"Column '{col}' not in table '{table_name}'. Available columns: {table_columns}"
                        )
        
        # Initialize constraint based on its category
        if constraint_type in MULTI_TABLE_CONSTRAINTS:
            # Pass table_name explicitly as first positional argument
            constraint = constraint_class(table_name, **params)
        
        elif constraint_type in CONSTRUCTOR_PARAM_TYPES:
            constraint = constraint_class(**params)
        
        elif constraint_type in SET_PARAMETERS_TYPES:
            constraint = constraint_class()
            constraint._set_parameters(**params)
            
            # Prepare dummy data for _fit call if needed
            dummy_cols = []
            # Collect relevant columns for dummy dataframe
            for key in ['column_name', 'column_names', 'low_column_name', 'high_column_name']:
                val = params.get(key)
                if val:
                    if isinstance(val, list):
                        dummy_cols.extend(val)
                    else:
                        dummy_cols.append(val)
            dummy_cols = list(set(dummy_cols))  # unique column names
            dummy_df = pd.DataFrame(columns=dummy_cols)
            
            if hasattr(constraint, '_fit'):
                constraint._fit(dummy_df)
        
        else:
            # Last resort fallback: try constructor, then _set_parameters and _fit
            try:
                constraint = constraint_class(**params)
            except TypeError:
                constraint = constraint_class()
                if hasattr(constraint, '_set_parameters'):
                    constraint._set_parameters(**params)
                if hasattr(constraint, '_fit'):
                    dummy_cols = []
                    for key in ['column_name', 'column_names', 'low_column_name', 'high_column_name']:
                        val = params.get(key)
                        if val:
                            if isinstance(val, list):
                                dummy_cols.extend(val)
                            else:
                                dummy_cols.append(val)
                    dummy_cols = list(set(dummy_cols))
                    dummy_df = pd.DataFrame(columns=dummy_cols)
                    constraint._fit(dummy_df)
        
        constraints_list.append(constraint)
    
    return constraints_list


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

-------------------------------------------------
import sdv.constraints as sdv_constraints

def create_dynamic_constraints(constraint_specs, metadata):
    constraints_list = []
    
    for spec in constraint_specs:
        table_name = spec["table"]
        constraint_type = spec["type"]
        params = spec["params"]
        
        # 1. Validate table exists
        if table_name not in metadata.tables:
            raise ValueError(f"Table '{table_name}' not found in metadata")
        
        # 2. Get constraint class
        constraint_class = getattr(sdv_constraints, constraint_type, None)
        if not constraint_class:
            raise ValueError(f"Constraint type '{constraint_type}' not found")
        
        # 3. Validate columns
        table_columns = list(metadata.tables[table_name].columns.keys())
        for key, value in params.items():
            if 'column' in key:
                cols = [value] if isinstance(value, str) else value
                for col in cols:
                    if col not in table_columns:
                        raise ValueError(f"Column '{col}' not in table '{table_name}'")
        
        # 4. Create constraint with try/except
        try:
            # Try direct initialization
            constraint = constraint_class(**params)
        except TypeError as e:
            # Handle special initialization
            if constraint_type == "FixedCombinations":
                # FixedCombinations requires column_names parameter
                constraint = constraint_class(column_names=params["column_names"])
            else:
                raise ValueError(f"Constraint creation failed: {str(e)}")
        
        # 5. Add to list
        constraints_list.append((table_name, constraint))
    
    return constraints_list

# Create synthesizer
synthesizer = HMASynthesizer(metadata)

# Generate constraints
constraints_list = create_dynamic_constraints([
    {
        "table": "users",
        "type": "ScalarInequality",
        "params": {"column_name": "age", "relation": ">=", "value": 18}
    },
    {
        "table": "products",
        "type": "FixedCombinations",
        "params": {"column_names": ["category", "price"]}
    }
], metadata)

# Add constraints
synthesizer.add_constraints(constraints_list)

# Fit with real data
synthesizer.fit(tables=tables)

# Generate synthetic data
synthetic_data = synthesizer.sample(scale=1.0)