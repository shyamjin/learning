import streamlit as st
from sqlalchemy import create_engine, MetaData
import networkx as nx
from pyvis.network import Network
import tempfile
import os

# --- DB connection ---
engine = create_engine("postgresql://user:password@host:port/dbname")  # update this
metadata = MetaData()
metadata.reflect(bind=engine)

# --- Build dependency graph ---
G = nx.DiGraph()
for table_name in metadata.tables:
    G.add_node(table_name)
for table in metadata.tables.values():
    for fk in table.foreign_keys:
        G.add_edge(fk.column.table.name, table.name)

# --- PyVis network setup ---
net = Network(height="700px", width="100%", directed=True, notebook=False)
for node in G.nodes:
    net.add_node(node, label=node, color='red' if node == 'student' else 'skyblue')

for source, target in G.edges:
    net.add_edge(source, target)

# --- Generate interactive HTML ---
with tempfile.NamedTemporaryFile(delete=False, suffix=".html") as tmp_file:
    net.show(tmp_file.name)
    HtmlFile = open(tmp_file.name, 'r', encoding='utf-8')
    source_code = HtmlFile.read()

# --- Display in Streamlit ---
st.title("Database Table Relationship Graph (FKs)")
st.components.v1.html(source_code, height=750, scrolling=True)

# Cleanup temp file
HtmlFile.close()
os.unlink(tmp_file.name)
