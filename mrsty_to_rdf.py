import pandas as pd
from pathlib import Path

PREFIXES = """@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix umls_semantic: <https://uts.nlm.nih.gov/uts/umls/semantic-network/>

"""

def ensure_output_dir(path):
    Path(path).mkdir(parents=True, exist_ok=True)

def write_semtype_rdf(df, output_file):
    """Emit RDF triples for each semantic type."""
    buffer = []

    for _, row in df.iterrows():
        tui = row["TUI"]
        label = row["STY"]
        safe_label = label.replace('\\', '\\\\').replace('"', '\\"')
        buffer.append(f"umls_semantic:{tui} rdfs:label \"{safe_label}\"@en .\n")

    with open(output_file, "w", encoding="utf-8") as f:
        f.write(PREFIXES)
        f.writelines(buffer)

    print(f"Wrote semantic types RDF to {output_file}")


if __name__ == "__main__":
    MRSTY_FILE = "input/MRSTY.parquet"
    OUTPUT_FILE = "umls_rdf/semantic_types.ttl"

    ensure_output_dir("umls_rdf")

    # Read only TUI and STY, drop duplicates
    df = pd.read_parquet(MRSTY_FILE, columns=["TUI","STY"])
    df = df.drop_duplicates()

    write_semtype_rdf(df, OUTPUT_FILE)