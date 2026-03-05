#!/usr/bin/env python3

import sys
import requests
import csv
from collections import Counter
import os

SUMMARY_FILE = "analysis_summary.csv"
write_header = False

DEBUG = False

SPARQL_ENDPOINT = "http://localhost:7020"

PREFIXES = """
PREFIX omop: <http://purl.org/ohdsi/>
PREFIX omop_concept: <http://purl.org/ohdsi/Concept/>
PREFIX umls: <http://bioportal.bioontology.org/ontologies/umls/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
"""

LIMIT_SAMPLE = 1000

QUERIES = {

"descendants_only": """
omop_concept:%ROOT%(^omop:isAncestryChildOf)+ ?c .
?c a omop:Concept .
""",

"single_hop_mapping_descendants": """
omop_concept:%ROOT% (omop:strongMapping | omop:weakMapping)? ?mapped .
?mapped (^omop:isAncestryChildOf)+ ?c .
?c a omop:Concept .
""",

"partial_synonym_descendants": """
omop_concept:%ROOT% (omop:strongMapping | ^omop:isAncestryChildOf)+ ?c .
?c a omop:Concept .
""",

"unconstrained_mapping_descendants": """
omop_concept:%ROOT% (omop:weakMapping | omop:strongMapping | ^omop:isAncestryChildOf)+ ?c .
?c a omop:Concept .
""",

"sameas_roundtrip": """
omop_concept:%ROOT% (owl:sameAs | ^owl:sameAs)+ ?c .
?c a omop:Concept .
""",

"descendants_then_sameas_roundtrip": """
?co omop:isAncestryChildOf+ omop_concept:%ROOT% .
?co owl:sameAs ?external .
?c owl:sameAs ?external .
?c a omop:Concept .
""",

"omop_descendants_and_sameas_expansion": """
?c (omop:isAncestryChildOf | omop:strongMapping | owl:sameAs | ^owl:sameAs)+ omop_concept:%ROOT% .
?c a omop:Concept .
""",

"sameas_expansion_and_umls_descendents": """
?c (umls:RB | owl:sameAs | ^owl:sameAs)+
omop_concept:%ROOT% .
?c a omop:Concept .
""",

"dual_expansion":"""
?c (omop:isAncestryChildOf | omop:strongMapping | owl:sameAs | ^owl:sameAs | umls:SY | ^umls:SY | umls:RB)+ omop_concept:%ROOT% .
?c a omop:Concept .
"""
}

def write_summary_row(root, strategy, total_count, sample_size, class_dist):
    global write_header

    row = {
        "root": root,
        "strategy": strategy,
        "total_count": total_count,
        "sample_size": sample_size,
        "class_distribution": "; ".join(f"{k}:{v}" for k, v in sorted(class_dist.items(), key=lambda x: -x[1]))
    }

    # Append mode
    mode = "a"
    with open(SUMMARY_FILE, mode, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=row.keys())
        if not write_header:
            writer.writeheader()
            write_header = True
        writer.writerow(row)

def run_sparql(query):
    if DEBUG:
        print("---")
        print(query)
        print("---")

    r = requests.post(
        SPARQL_ENDPOINT,
        data={"query": query},
        headers={"Accept": "application/sparql-results+json"},
        timeout=600
    )

    r.raise_for_status()

    try:
        return r.json()["results"]["bindings"]
    except Exception:
        print("Failed to parse JSON. Response snippet:")
        print(r.text[:2000])
        raise


def run_count(where_block):

    query = PREFIXES + f"""
SELECT (COUNT(DISTINCT ?c) AS ?count)
WHERE {{
{where_block}
}}
"""

    results = run_sparql(query)

    return int(results[0]["count"]["value"])


def run_sample(where_block):

    query = PREFIXES + f"""
SELECT DISTINCT ?c ?label ?class
WHERE {{
{where_block}

OPTIONAL {{
  ?c rdfs:label ?label .
}}

OPTIONAL {{
  ?c omop:hasConceptClass ?class .
}}
}}
LIMIT {LIMIT_SAMPLE}
"""

    return run_sparql(query)


def analyze_sample(results):

    class_counts = Counter()
    ids = []
    labels = {}
    classes = {}

    for r in results:

        cid = r["c"]["value"].split("/")[-1]
        ids.append(cid)

        if "label" in r:
            labels[cid] = r["label"]["value"]

        if "class" in r:
            cclass = r["class"]["value"].split("/")[-1]
            classes[cid] = cclass
            class_counts[cclass] += 1

    return ids, labels, classes, class_counts


def save_sample(root, strategy, ids, labels, classes):

    filename = f"analysis_samples/O{root}/{strategy}_sample.csv"
    os.makedirs(f'analysis_samples/O{root}', exist_ok=True)

    with open(filename, "w", newline="") as f:

        writer = csv.writer(f)
        writer.writerow(["concept_id", "label", "concept_class"])

        for cid in ids:

            writer.writerow([
                cid,
                labels.get(cid, ""),
                classes.get(cid, "")
            ])

    print("Saved:", filename)


def benchmark(root):

    print("Root concept:", root)
    print()

    for name, where in QUERIES.items():
        try:

            print("="*60)
            print(name)

            where_block = where.replace("%ROOT%", root)

            print("Running count...")
            total = run_count(where_block)

            print("Total concepts:", total)

            print("Fetching sample...")
            sample = run_sample(where_block)

            ids, labels, classes, class_dist = analyze_sample(sample)

            print("Sample size:", len(ids))

            print("\nConcept class distribution (sample):")
            for c, n in class_dist.most_common():
                print(f"  {c}: {n}")

            save_sample(root, name, ids, labels, classes)
            write_summary_row(root, name, total, len(ids), class_dist)

            print()
        except BaseException as e:
            print("failed", e)


if __name__ == "__main__":

    if len(sys.argv) != 2:
        print("Usage: python benchmark_expansion.py ROOT_CONCEPT_ID")
        sys.exit(1)

    root = sys.argv[1]

    benchmark(root)