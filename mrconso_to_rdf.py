import pandas as pd
from pathlib import Path
from urllib.parse import quote

# --------------------------
# RDF Prefixes
# --------------------------
PREFIXES = """@prefix umls: <http://bioportal.bioontology.org/ontologies/umls/> .
@prefix umls_concept: <https://uts.nlm.nih.gov/uts/concept/> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .

"""

# --------------------------
# SAB → Vocabulary mapping
# --------------------------
SAB_TO_VOCAB = {
    "SNOMEDCT_US": "snomed",
    "SCTSPA": "snomed",
    "LNC": "loinc",
    "RXNORM": "rxnorm",
    "ICD10CM": "icd10cm",
    "ICD9CM": "icd9cm",
    "ICD10PCS": "icd10pcs",
    "ICD10": "icd10",
    "HCPCS": "hcpcs",
    "ATC": "atc",
    "MTHSPL": "spl",
    "SOP": "sopt"
}

# --------------------------
# Vocabulary → external URI prefixes
# --------------------------
reference_uri_prefixes = {
    "snomed": "http://snomed.info/id/",
    "loinc": "http://loinc.org/rdf/",
    "rxnorm": "http://purl.bioontology.org/ontology/RXNORM/",
    "icd10cm": "http://purl.bioontology.org/ontology/ICD10CM/",
    "icd9cm": "http://purl.bioontology.org/ontology/ICD9CM/",
    "icd10pcs": "http://purl.bioontology.org/ontology/ICD10PCS/",
    "icd10": "http://purl.bioontology.org/ontology/ICD10/",
    "atc": "http://purl.bioontology.org/ontology/ATC/",
    "hcpcs": "http://purl.bioontology.org/ontology/HCPCS/",
    "spl": "http://terminology.hl7.org/CodeSystem/v3-extTmp-spl/",
    "sopt": "https://nahdo.org/sopt/"
}

# languages
LAT_TO_LANG = {
    "ENG": "en",
    "FRE": "fr",
    "GER": "de",
    "ITA": "it",
    "SPA": "es",
    "POR": "pt",
    "DUT": "nl",
    "SWE": "sv",
    "NOR": "no",
    "DAN": "da",
    "FIN": "fi",
    "HEB": "he",
    "CHI": "zh",
    "JPN": "ja",
    "KOR": "ko",
    "RUS": "ru",
    "CZE": "cs",
    "POL": "pl",
    "HUN": "hu",
    "TUR": "tr",
    "LIT": "lt",
    "LAV": "lv",
    "EST": "et",
    "SCR": "sh",
    "GRE": "el",
    "ISL": "is",
    "ARA": "ar",
    "BAQ": "eu"
}

# --------------------------
# Helpers
# --------------------------
def ensure_output_dir(path):
    Path(path).mkdir(parents=True, exist_ok=True)

def flush_concept(cui, labels, code_pairs, output_buffer):
    """Flush a concept block to buffer with labels and codes."""
    if not cui:
        return

    subject = f"umls_concept:{cui} a umls:Concept ;"
    output_buffer.append(f"{subject}\n")

    # Labels (already deduplicated via set)
    for lbl, lang in sorted(labels):
        safe_lbl = lbl.replace('\\', '\\\\').replace('"', '\\"')
        output_buffer.append(f'    rdfs:label "{safe_lbl}"@{lang} ;\n')

    # Codes
    unique_codes = sorted(set(code_pairs))
    sameas_uris = set()  # deduplicate owl:sameAs
    for sab, code in unique_codes:
        safe_code = quote(str(code))
        vocab = SAB_TO_VOCAB.get(sab)

        # Skip MTH* codes for owl:sameAs (internal Metathesaurus codes)
        if code.startswith("MTH"):
            output_buffer.append(f'    umls:sourceCode "{sab}:{safe_code}" ;\n')
            continue

        if code.lower() == "nocode":
            continue

        if vocab:
            uri_prefix = reference_uri_prefixes.get(vocab)
            if uri_prefix:
                uri = f"{uri_prefix}{safe_code}"
                if uri not in sameas_uris:
                    output_buffer.append(f'    owl:sameAs <{uri}> ;\n')
                    sameas_uris.add(uri)
                continue

        # fallback combined representation
        output_buffer.append(f'    umls:sourceCode "{sab}:{safe_code}" ;\n')

    # replace final semicolon with period
    if output_buffer[-1].strip().endswith(";"):
        output_buffer[-1] = output_buffer[-1].rstrip(" ;\n") + " .\n\n"

def write_batch(buffer, batch_number, output_dir):
    filename = Path(output_dir) / f"mrconso_batch_{batch_number}.ttl"
    with open(filename, "w", encoding="utf-8") as f:
        f.write(PREFIXES)
        f.writelines(buffer)
    print(f"Wrote batch {batch_number}.")

# --------------------------
# Main conversion function
# --------------------------
def convert_parquet_to_rdf(input_pq, output_dir, batch_size):
    ensure_output_dir(output_dir)

    # Read only needed columns
    df = pd.read_parquet(
        input_pq,
        engine="pyarrow",
        columns=["CUI", "LAT", "CODE", "SAB", "STR", "SUPPRESS"]
    )

    # Only keep non-suppressed rows
    df = df[df["SUPPRESS"] == "N"]

    # Sort lexically by CUI
    df = df.sort_values("CUI")

    current_cui = None
    labels = set()
    codes = []

    batch_number = 1
    concept_count = 0
    buffer = []

    for _, row in df.iterrows():
        cui = row["CUI"]

        # Detect CUI boundary
        if current_cui and cui != current_cui:
            flush_concept(current_cui, labels, codes, buffer)
            concept_count += 1

            if concept_count >= batch_size:
                write_batch(buffer, batch_number, output_dir)
                buffer = []
                batch_number += 1
                concept_count = 0

            labels = set()
            codes = []

        current_cui = cui

        lat = row.get("LAT")
        lang = LAT_TO_LANG.get(lat, "und")  # default "und" = undefined

        if row.get("STR"):
            labels.add((row["STR"], lang))

        sab = row.get("SAB")
        code = row.get("CODE")
        if sab and code:
            codes.append((sab, code))

    # Flush last concept
    flush_concept(current_cui, labels, codes, buffer)

    if buffer:
        write_batch(buffer, batch_number, output_dir)

    print("Done.")

# --------------------------
# Script entry point
# --------------------------
if __name__ == "__main__":
    INPUT_PQ = "input/MRCONSO.parquet"
    OUTPUT_DIR = "umls_rdf/concepts"
    BATCH_SIZE = 10000  # number of CUIs per file

    convert_parquet_to_rdf(INPUT_PQ, OUTPUT_DIR, BATCH_SIZE)