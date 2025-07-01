from sqlalchemy import create_engine, MetaData
from graphviz import Digraph

# Connect to your database
engine = create_engine("postgresql://user:password@host:port/dbname")

# Reflect schema
metadata = MetaData()
metadata.reflect(bind=engine)

# Create Graphviz graph
dot = Digraph(comment='Table Relationships', format='png')  # or use 'pdf'/'svg'
dot.attr(rankdir='LR', fontsize='10', size='8')

# Add table nodes
for table_name in metadata.tables:
    dot.node(table_name)

# Add foreign key edges
for table in metadata.tables.values():
    for fk in table.foreign_keys:
        parent = fk.column.table.name
        child = table.name
        dot.edge(parent, child)

# Render and view
output_path = "table_relationships"
dot.render(output_path, view=True)  # opens the generated PNG or PDF

