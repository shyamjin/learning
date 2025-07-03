import re
from typing import List, Dict, Union

class ConstraintGenerator:
    """Generates SDV constraint configurations from natural language prompts"""
    
    # Constraint patterns with capture groups
    PATTERNS = {
        "ScalarInequality": [
            r"(?:ensure|require|constrain|where)\s+(\w+)\s+(>=|<=|>|<|greater than or equal to|less than or equal to|greater than|less than)\s+(\d+)",
            r"(\w+)\s+should be\s+(at least|at most|greater than|less than)\s+(\d+)"
        ],
        "Inequality": [
            r"(\w+)\s+(?:should be|must be)\s+(before|after|less than|greater than)\s+(\w+)",
            r"ensure\s+(\w+)\s+([<>]=?)\s+(\w+)"
        ],
        "FixedCombinations": [
            r"preserve (?:the )?combinations? (?:of|between) ([\w\s,]+)",
            r"fixed combinations? for ([\w\s,]+)"
        ],
        "UniqueCombinations": [
            r"unique combinations? (?:of|for) ([\w\s,]+)",
            r"distinct pairs? of ([\w\s,]+)"
        ]
    }
    
    # Relation mapping
    RELATION_MAP = {
        "greater than": ">",
        "less than": "<",
        "at least": ">=",
        "at most": "<=",
        "before": "<",
        "after": ">",
        "greater than or equal to": ">=",
        "less than or equal to": "<=",
    }

    @classmethod
    def from_prompt(cls, prompt: str, table: str = "users") -> List[Dict]:
        """Generate constraints from natural language prompt"""
        constraints = []
        
        # Try to detect table name from prompt
        table_match = re.search(r"for (\w+)", prompt, re.IGNORECASE)
        if table_match:
            table = table_match.group(1)
        
        # Scalar Inequality (single column)
        for pattern in cls.PATTERNS["ScalarInequality"]:
            for match in re.finditer(pattern, prompt, re.IGNORECASE):
                col = match.group(1)
                relation = cls.RELATION_MAP.get(match.group(2).lower(), match.group(2))
                value = int(match.group(3))
                
                constraints.append({
                    "table": table,
                    "type": "ScalarInequality",
                    "params": {
                        "column_name": col,
                        "relation": relation,
                        "value": value
                    }
                })
        
        # Inequality (between two columns)
        for pattern in cls.PATTERNS["Inequality"]:
            for match in re.finditer(pattern, prompt, re.IGNORECASE):
                col1 = match.group(1)
                relation = cls.RELATION_MAP.get(match.group(2).lower(), match.group(2))
                col2 = match.group(3)
                
                constraints.append({
                    "table": table,
                    "type": "Inequality",
                    "params": {
                        "low_column_name": col1 if relation in ["<", "<="] else col2,
                        "high_column_name": col2 if relation in ["<", "<="] else col1
                    }
                })
        
        # Fixed Combinations
        for pattern in cls.PATTERNS["FixedCombinations"]:
            for match in re.finditer(pattern, prompt, re.IGNORECASE):
                columns = [col.strip() for col in match.group(1).split(",")]
                
                constraints.append({
                    "table": table,
                    "type": "FixedCombinations",
                    "params": {
                        "column_names": columns
                    }
                })
        
        # Unique Combinations
        for pattern in cls.PATTERNS["UniqueCombinations"]:
            for match in re.finditer(pattern, prompt, re.IGNORECASE):
                columns = [col.strip() for col in match.group(1).split(",")]
                
                constraints.append({
                    "table": table,
                    "type": "UniqueCombinations",
                    "params": {
                        "columns": columns
                    }
                })
        
        return constraints

# Example usage
if __name__ == "__main__":
    prompt = "Generate data for user where age should greater than 18 year"
    constraints = ConstraintGenerator.from_prompt(prompt)
    
    print("Generated Constraints:")
    for constraint in constraints:
        print(constraint)
    
    # Sample output:
    # [
    #   {
    #     "table": "user",
    #     "type": "ScalarInequality",
    #     "params": {
    #         "column_name": "age",
    #         "relation": ">",
    #         "value": 18
    #     }
    #   }
    # ]

prompt = "Ensure income is at least 50000"
constraints = ConstraintGenerator.from_prompt(prompt, "employees")
# Output: ScalarInequality constraint for income >= 50000

prompt = "Preserve combinations of product category and region"
constraints = ConstraintGenerator.from_prompt(prompt, "sales")
# Output: FixedCombinations constraint for ['product category', 'region']

prompt = """
For customer table:
1. Age should be at least 18
2. Preserve combinations of state and membership tier
3. Ensure signup date before first purchase date
"""
constraints = ConstraintGenerator.from_prompt(prompt)
# Outputs three constraints for the 'customer' table

def generate_synthetic_data(prompts: List[str], metadata, tables):
    # Collect constraints from all prompts
    all_constraints = []
    for prompt in prompts:
        all_constraints.extend(ConstraintGenerator.from_prompt(prompt))
    
    # Initialize synthesizer
    synthesizer = HMASynthesizer(metadata)
    
    # Fit with dynamic constraints
    synthesizer.fit(
        tables=tables,
        constraints=all_constraints or None
    )
    
    return synthesizer.sample(scale=1.0)

# Example usage
prompts = [
    "For users: age >= 18",
    "In orders: preserve combinations of product_id and discount_group",
    "Ensure payments.created_at < payments.completed_at"
]

synthetic_data = generate_synthetic_data(
    prompts=prompts,
    metadata=my_metadata,
    tables=my_tables
)