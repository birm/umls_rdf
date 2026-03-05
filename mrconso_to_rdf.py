import pandas as pd
from pathlib import Path
from urllib.parse import quote
from collections import defaultdict

# --------------------------
# RDF Prefixes
# --------------------------
PREFIXES = """@prefix umls: <http://bioportal.bioontology.org/ontologies/umls/> .
@prefix umls_concept: <https://uts.nlm.nih.gov/uts/umls/concept/> .
@prefix umls_semantic: <https://uts.nlm.nih.gov/uts/umls/semantic-network/> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .


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

# main language for label vs altlabel
MAIN_LANG = "en"
ONLY_MAIN_LANG = True

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

def flush_concept(cui, labels, ttys, code_pairs, output_buffer, cui_to_tui):
    """Flush a concept block to buffer with preferred label, altLabels, codes, and TUIs."""
    if not cui:
        return

    output_buffer.append(f"umls_concept:{cui} a umls:Concept ;\n")

    # Preferred label
    preferred = None
    alt_labels = set()
    first_in_main_lang = None

    # First pass: find PT in MAIN_LANG and collect altLabels
    for (lbl, lang), tty in zip(labels, ttys):
        if tty == "PT" and lang == MAIN_LANG and not preferred:
            preferred = (lbl, lang)
        else:
            alt_labels.add((lbl, lang))
        if lang == MAIN_LANG and not first_in_main_lang:
            first_in_main_lang = (lbl, lang)

    # Fallback if no PT in MAIN_LANG
    if not preferred:
        if first_in_main_lang:
            preferred = first_in_main_lang
            alt_labels.discard(first_in_main_lang)
        else:
            # pick any label
            preferred = labels[0]
            alt_labels.discard(labels[0])


    if preferred:
        safe_lbl = preferred[0].replace('\\', '\\\\').replace('"', '\\"')
        output_buffer.append(f'    rdfs:label "{safe_lbl}"@{preferred[1]} ;\n')

    for lbl, lang in sorted(alt_labels):
        if lang == MAIN_LANG or not ONLY_MAIN_LANG:
            safe_lbl = lbl.replace('\\', '\\\\').replace('"', '\\"')
            output_buffer.append(f'    skos:altLabel "{safe_lbl}"@{lang} ;\n')

    # Codes (keep your existing logic)
    unique_codes = sorted(set(code_pairs))
    sameas_uris = set()
    for sab, code in unique_codes:
        safe_code = quote(str(code))
        vocab = SAB_TO_VOCAB.get(sab)

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

        output_buffer.append(f'    umls:sourceCode "{sab}:{safe_code}" ;\n')

    # Semantic type triples
    for tui in cui_to_tui.get(cui, []):
        output_buffer.append(f'    umls:semanticType umls_semantic:{tui} ;\n')

    # Replace last semicolon with period
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
def convert_parquet_to_rdf(input_pq, sty_pq, output_dir, batch_size):
    ensure_output_dir(output_dir)

    # --------------------------
    # Load CONSO
    # --------------------------
    df = pd.read_parquet(
        input_pq,
        engine="pyarrow",
        columns=["CUI", "LAT", "CODE", "SAB", "STR", "TTY", "SUPPRESS"]
    )

    # Only non-suppressed rows
    df = df[df["SUPPRESS"] == "N"]
    df = df.sort_values("CUI")

    # --------------------------
    # Load semantic types
    # --------------------------
    stys = pd.read_parquet(sty_pq, engine="pyarrow", columns=['CUI', 'TUI'])
    stys = stys.drop_duplicates()

    # Map CUI -> list of TUIs
    cui_to_tuis = defaultdict(list)
    for _, row in stys.iterrows():
        cui_to_tuis[row['CUI']].append(row['TUI'])

    # --------------------------
    # Iterate over concepts
    # --------------------------
    current_cui = None
    labels = []
    ttys = []
    codes = []

    batch_number = 1
    concept_count = 0
    buffer = []

    for _, row in df.iterrows():
        cui = row["CUI"]

        # New CUI boundary → flush previous concept
        if current_cui and cui != current_cui:
            flush_concept(current_cui, labels, ttys, codes, buffer, cui_to_tuis)
            concept_count += 1

            # Batch write
            if concept_count >= batch_size:
                write_batch(buffer, batch_number, output_dir)
                buffer = []
                batch_number += 1
                concept_count = 0

            # Reset accumulators
            labels = []
            ttys = []
            codes = []

        current_cui = cui

        # Label and TTY
        lat = row.get("LAT")
        lang = LAT_TO_LANG.get(lat, "und")
        if row.get("STR"):
            labels.append((row["STR"], lang))
            ttys.append(row.get("TTY"))

        # Codes
        sab = row.get("SAB")
        code = row.get("CODE")
        if sab and code:
            codes.append((sab, code))

    # Flush last concept
    if current_cui:
        flush_concept(current_cui, labels, ttys, codes, buffer, cui_to_tuis)

    # Write remaining buffer
    if buffer:
        write_batch(buffer, batch_number, output_dir)

    print("Done.")

# --------------------------
# Script entry point
# --------------------------
if __name__ == "__main__":
    INPUT_PQ = "input/MRCONSO.parquet"
    STY_PQ = "input/MRSTY.parquet"
    OUTPUT_DIR = "umls_rdf/concepts"
    BATCH_SIZE = 10000  # number of CUIs per file

    convert_parquet_to_rdf(INPUT_PQ, STY_PQ, OUTPUT_DIR, BATCH_SIZE)