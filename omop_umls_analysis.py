#!/usr/bin/env python3

import sys
import requests
import csv
from collections import Counter
import os
from text_baseline import get_lexical_baseline

SUMMARY_FILE = "analysis_summary.csv"
WRITTEN_HEADER = False

DEBUG = False

SPARQL_ENDPOINT = "http://localhost:7020"

PREFIXES = """
PREFIX omop: <http://purl.org/ohdsi/>
PREFIX omop_concept: <http://purl.org/ohdsi/Concept/>
PREFIX umls: <http://bioportal.bioontology.org/ontologies/umls/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
"""

QUERIES = {

# 1. Pure OMOP hierarchy
"descendants_only": """
omop_concept:%ROOT% (^omop:isAncestryChildOf)* ?c .
?c a omop:Concept .
""",

# 2. OMOP hierarchy plus OMOP synonym mappings
"partial_synonym_descendants": """
omop_concept:%ROOT% (omop:weakMapping | omop:strongMapping )? ?co .
?co ( ^omop:isAncestryChildOf )* ?c1 .
?c1 (omop:weakMapping | omop:strongMapping )? ?c .
?c a omop:Concept .
""",

# 3. Allow hierarchy traversal and equivalence during expansion
"hierarchy_and_sameas": """
omop_concept:%ROOT% ( ^omop:isAncestryChildOf)* ?co .
?co owl:sameAs ?ext .
?umls owl:sameAs ?ext .
?umls owl:sameAs ?ext2 .
?c owl:sameAs ?ext2 .
?c a omop:Concept .
""",

# 4. Expand through equivalence and UMLS broader relations
"sameas_expansion_and_umls_rb": """
omop_concept:%ROOT% (owl:sameAs) ?ext .
?umls_parent (owl:sameAs) ?ext .
?umls_child (umls:RB )*  ?umls_parent .
?umls_child owl:sameAs ?ext2 .
?c owl:sameAs ?ext2 .
?c a omop:Concept .
?c rdfs:label ?label .
""",

# 5. Same as above but allow PAR hierarchy
"sameas_expansion_and_umls_rb_or_par": """
omop_concept:%ROOT% (owl:sameAs) ?ext .
?umls_parent (owl:sameAs) ?ext .
?umls_child (umls:RB | umls:PAR)*  ?umls_parent .
?umls_child owl:sameAs ?ext2 .
?c owl:sameAs ?ext2 .
?c a omop:Concept .
?c rdfs:label ?label .
""",

# 6. Swap between and use either descendents
#
#"dual_semantic_expansion": """
#?c (
#    ^omop:isAncestryChildOf
#  | owl:sameAs
#  | ^owl:sameAs
#  | umls:RB
#)* omop_concept:%ROOT% .
#?c a omop:Concept .
#"""
}

def write_summary_row(root, strategy, total_count, class_dist):
    global WRITTEN_HEADER

    row = {
        "root": root,
        "strategy": strategy,
        "total_count": total_count,
        "class_distribution": "; ".join(f"{k}:{v}" for k, v in sorted(class_dist.items(), key=lambda x: -x[1]))
    }

    # Append mode
    mode = "a"
    with open(SUMMARY_FILE, mode, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=row.keys())
        if not WRITTEN_HEADER:
            writer.writeheader()
            WRITTEN_HEADER = True
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


def run_results(where_block):

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


def save_results(root, strategy, ids, labels, classes):

    filename = f"analysis_results/O{root}/{strategy}.csv"
    os.makedirs(f'analysis_results/O{root}', exist_ok=True)

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


def benchmark(root, name):

    print("Root concept:", root)
    print()

    lex_ids = get_lexical_baseline('omop_concepts.db', name)
    write_summary_row(root, "lexical_baseline",  len(lex_ids), {})
    save_results(root, "lexical_baseline", lex_ids, {}, {})
    
    for name, where in QUERIES.items():
        try:

            print("="*60)
            print(name)

            where_block = where.replace("%ROOT%", root)

            print("Running count...")
            total = run_count(where_block)

            print("Total concepts:", total)

            print("Fetching sample...")
            sample = run_results(where_block)

            ids, labels, classes, class_dist = analyze_sample(sample)

            print("Sample size:", len(ids))

            print("\nConcept class distribution (sample):")
            for c, n in class_dist.most_common():
                print(f"  {c}: {n}")

            save_results(root, name, ids, labels, classes)
            write_summary_row(root, name, total, class_dist)

            print()
        except BaseException as e:
            print("failed", e)


if __name__ == "__main__":

    if len(sys.argv) != 3:
        print("Usage: python benchmark_expansion.py ROOT_CONCEPT_ID NAME")
        sys.exit(1)

    root = sys.argv[1]
    name = sys.argv[2]

    benchmark(root, name)