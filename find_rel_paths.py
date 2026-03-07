import pandas as pd
import networkx as nx

# Load MRREL.parquet
mrrel = pd.read_parquet("input/MRREL.parquet")

# Filter only for REL = 'RB'
rb_edges = mrrel[mrrel['REL'] == 'RB']

# We only care about CUI1 -> CUI2
edges = rb_edges[['CUI1', 'CUI2']].drop_duplicates()
G = nx.DiGraph()
G.add_edges_from(edges.values)

pairs = [
    ("C0030827", "C0689921"),
    ("C0038897", "C1814855"),
    ("C0020971", "C0041582")
]

for source, target in pairs:
    try:
        # shortest_path by number of edges
        path = nx.shortest_path(G, source=target, target=source)
        print(f"Path from {source} to {target}: {' -> '.join(path)}")
    except nx.NetworkXNoPath:
        print(f"No path found from {source} to {target}")