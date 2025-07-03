import openai
from typing import List, Dict
import json

# Initialize Azure OpenAI client
client = openai.AzureOpenAI(
    api_key="YOUR_API_KEY",
    api_version="2023-12-01-preview",
    azure_endpoint="https://YOUR_RESOURCE.openai.azure.com/"
)

def generate_constraint_config(prompt: str) -> List[Dict]:
    """Convert natural language constraints to SDV constraint config using Azure OpenAI"""
    system_prompt = """
    You are a constraint specification generator. Convert user requirements into JSON format for SDV (Synthetic Data Vault) constraints. Use this structure:
    [
      {
        "table": "table_name",
        "type": "ConstraintType",
        "params": {"param1": value1, "param2": value2}
      }
    ]
    
    Supported constraint types:
    - ScalarInequality: {"column_name": str, "relation": ">", ">=", "<", "<=", "value": num}
    - Inequality: {"low_column_name": str, "high_column_name": str}
    - FixedCombinations: {"column_names": [str]}
    - UniqueCombinations: {"columns": [str]}
    
    Example input: "For customer table: Age should be at least 18"
    Example output: [{"table": "customer", "type": "ScalarInequality", "params": {"column_name": "age", "relation": ">=", "value": 18}}]
    """
    
    response = client.chat.completions.create(
        model="gpt-4",  # Use your deployment name
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt}
        ],
        temperature=0.1,
        max_tokens=500
    )
    
    # Extract and parse JSON from response
    try:
        content = response.choices[0].message.content
        json_start = content.find('[')
        json_end = content.rfind(']') + 1
        json_str = content[json_start:json_end]
        return json.loads(json_str)
    except (json.JSONDecodeError, IndexError) as e:
        raise ValueError(f"Failed to parse constraints: {e}\nAI Response: {content}")

# Example usage
prompt = """
For customer table:
1. Age should be at least 18
2. Preserve combinations of state and membership tier
3. Ensure signup date before first purchase date
"""

constraint_config = generate_constraint_config(prompt)
print("Generated Constraint Config:")
print(json.dumps(constraint_config, indent=2))
"""
[
  {
    "table": "customer",
    "type": "ScalarInequality",
    "params": {
      "column_name": "age",
      "relation": ">=",
      "value": 18
    }
  },
  {
    "table": "customer",
    "type": "FixedCombinations",
    "params": {
      "column_names": ["state", "membership_tier"]
    }
  },
  {
    "table": "customer",
    "type": "Inequality",
    "params": {
      "low_column_name": "signup_date",
      "high_column_name": "first_purchase_date"
    }
  }
]
"""
from sdv.multi_table import HMASynthesizer

def generate_synthetic_data(metadata, tables, constraint_prompt):
    # Generate constraint config from prompt
    constraint_config = generate_constraint_config(constraint_prompt)
    
    # Create synthesizer
    synthesizer = HMASynthesizer(metadata)
    
    # Fit with constraints
    synthesizer.fit(
        tables=tables,
        constraints=constraint_config
    )
    
    return synthesizer.sample(scale=1.0)

# Example usage
synthetic_data = generate_synthetic_data(
    metadata=my_metadata,
    tables=my_tables,
    constraint_prompt="""
    For users table:
    - Income should be at least 50000
    - Ensure login_count < subscription_years*50
    
    For orders table:
    - Preserve combinations of product_category and discount_bracket
    """
)
"""
Extandable
# Add custom constraint types
SUPPORTED_CONSTRAINTS = {
    "Positive": {"column_name": str},
    "Negative": {"column_name": str},
    "Range": {"column_name": str, "low_value": num, "high_value": num}
}
"""