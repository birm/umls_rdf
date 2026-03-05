import pandas as pd
from pathlib import Path

PREFIXES = """@prefix umls: <http://bioportal.bioontology.org/ontologies/umls/> .
@prefix umls_concept: <https://uts.nlm.nih.gov/uts/umls/concept/> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .

"""


def ensure_output_dir(path):
    Path(path).mkdir(parents=True, exist_ok=True)


def row_to_ttl(row):
    """
    Convert MRREL row into Turtle triple.
    Only include if SUPPRESS == 'N'
    """
    if row.get("SUPPRESS") != "N":
        return None

    cui1 = row["CUI1"]
    rel = row["REL"]
    cui2 = row["CUI2"]
    rela = None
    if row.get("RELA", None) is not None:
        rela = f"{row['SAB']}_{row['RELA']}"

    rel_row = f"umls_concept:{cui1} umls:{rel} umls_concept:{cui2} .\n"
    rela_row = ""
    if rela:
        rela_row = f"umls_concept:{cui1} umls:{rela} umls_concept:{cui2} .\n"

    return rel_row + rela_row 


def write_batch(triples, batch_number):
    filename = Path(OUTPUT_DIR) / f"mrrel_batch_{batch_number}.ttl"
    with open(filename, "w", encoding="utf-8") as f:
        f.write(PREFIXES)
        f.writelines(triples)
    print(f"Wrote batch {batch_number} with {len(triples)} triples.")


def convert_rels_to_rdf(INPUT_PQ, OUTPUT_DIR, BATCH_SIZE):
    ensure_output_dir(OUTPUT_DIR)

    df = pd.read_parquet(
        INPUT_PQ,
        engine="pyarrow",
        columns=["CUI1", "REL", "CUI2", "RELA", "SAB", "SUPPRESS"]
    )

    batch_number = 1
    triple_buffer = []

    for _, row in df.iterrows():
        ttl_line = row_to_ttl(row)
        if ttl_line:
            triple_buffer.append(ttl_line)

        if len(triple_buffer) >= BATCH_SIZE:
            write_batch(triple_buffer, batch_number)
            triple_buffer = []
            batch_number += 1

    if triple_buffer:
        write_batch(triple_buffer, batch_number)

    print("Done.")


if __name__ == "__main__":
    INPUT_PQ = "input/MRREL.parquet"
    OUTPUT_DIR = "umls_rdf/rels"
    BATCH_SIZE = 10000
    convert_rels_to_rdf(INPUT_PQ, OUTPUT_DIR, BATCH_SIZE)