import streamlit as st
from sqlalchemy import create_engine, MetaData
from pyvis.network import Network
import networkx as nx
import tempfile
import os

# --- DB connection ---
engine = create_engine("postgresql://user:password@localhost:5432/yourdbname")  # Update this
metadata = MetaData()
metadata.reflect(bind=engine)

# --- Build dependency graph ---
G = nx.DiGraph()

# Add all table nodes
for table_name in metadata.tables:
    G.add_node(table_name)

# Add FK edges
for table in metadata.tables.values():
    for fk in table.foreign_keys:
        source = fk.column.table.name  # referenced table (parent)
        target = table.name            # referencing table (child)
        G.add_edge(source, target)

# --- Build PyVis Network ---
highlight = st.text_input("Highlight table", "student")

net = Network(height="700px", width="100%", directed=True)
for node in G.nodes:
    net.add_node(node, label=node,
                 color='red' if node == highlight else 'skyblue',
                 shape='box')

for source, target in G.edges:
    net.add_edge(source, target)

# --- Render as HTML ---
with tempfile.NamedTemporaryFile(delete=False, suffix=".html") as tmp_file:
    net.show(tmp_file.name)
    html_content = open(tmp_file.name, 'r', encoding='utf-8').read()

st.title("📊 Table Relationship Graph (Foreign Keys)")
st.components.v1.html(html_content, height=750, scrolling=True)

# Clean up temp file
os.unlink(tmp_file.name)
