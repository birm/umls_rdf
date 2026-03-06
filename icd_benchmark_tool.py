import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON

ENDPOINT_URL = "http://localhost:7020"
sparql = SPARQLWrapper(ENDPOINT_URL)

PREFIXES = """
PREFIX omop: <http://purl.org/ohdsi/>
PREFIX omop_concept: <http://purl.org/ohdsi/Concept/>
PREFIX umls_concept: <https://uts.nlm.nih.gov/uts/umls/concept/>
PREFIX umls: <http://bioportal.bioontology.org/ontologies/umls/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX icd10: <http://purl.bioontology.org/ontology/ICD10/>
PREFIX omop_vocab: <http://purl.org/ohdsi/Vocabulary/>
"""

# The purely lexical baseline query
BASELINE_QUERY = """
SELECT DISTINCT ?c ?label ?code
WHERE {
    VALUES ?icd_vocabs { omop_vocab:Icd10cm omop_vocab:Icd10 }
    ?c omop:hasVocabulary ?icd_vocabs .
    ?c omop:hasConceptCode ?code .
    FILTER(STRSTARTS(STR(?code), "%PREFIX%"))
    ?c rdfs:label ?label .
}
"""

QUERIES = {
"omop_expand": """
SELECT DISTINCT ?c ?label ?code
WHERE {
VALUES ?icd_vocabs { omop_vocab:Icd10cm omop_vocab:Icd10 }
VALUES ?icd_root { %ROOT_URI% }
# Bridge ICD Source to OMOP Standard Concept
?omop_start owl:sameAs ?icd_root .
?c0 (omop:weakMapping | omop:strongMapping )? ?omop_start .
# Expand down the Standard Hierarchy (e.g., SNOMED)
?c0 (^omop:isAncestryChildOf)* ?c1 .
# Map back to ICD Source codes
?c1 (omop:weakMapping | omop:strongMapping )? ?c .
?c a omop:Concept ;
omop:hasVocabulary ?icd_vocabs ;
omop:hasConceptCode ?code ;
rdfs:label ?label .
}
""",

"umls_expand": """
SELECT DISTINCT ?c ?label ?code
WHERE {
VALUES ?icd_vocabs { omop_vocab:Icd10cm omop_vocab:Icd10 }
VALUES ?icd_root { %ROOT_URI% }

?umls_start owl:sameAs ?icd_root .
?umls_start a umls:Concept .
?umls_expanded (umls:RB)* ?umls_start .
?umls_expanded owl:sameAs ?ext_bridge .
?c owl:sameAs ?ext_bridge .
?c a omop:Concept ;
omop:hasVocabulary ?icd_vocabs ;
omop:hasConceptCode ?code ;
rdfs:label ?label .
}
""",

"dual_expand": """
SELECT DISTINCT ?c ?label ?code
WHERE {
VALUES ?icd_vocabs { omop_vocab:Icd10cm omop_vocab:Icd10 }
VALUES ?icd_root { %ROOT_URI% }
?start_node owl:sameAs ?icd_root .
?c0 (omop:strongMapping | omop:weakMapping)? ?start_node .

?c0 (umls:RN | ^omop:isAncestryChild)* ?expanded .
# Final projection back to ICD
?expanded (owl:sameAs / ^owl:sameAs)? ?omop_base .
?omop_base (omop:weakMapping | omop:strongMapping )? ?c .
?c a omop:Concept .
?c omop:hasVocabulary ?icd_vocabs .
?c omop:hasConceptCode ?code .
?c rdfs:label ?label .
}
"""
} 

def run_query(query_text):
    sparql.setQuery(PREFIXES + query_text)
    sparql.setReturnFormat(JSON)
    try:
        results = sparql.query().convert()
        return results["results"]["bindings"]
    except Exception as e:
        print(f"Error running query: {e}")
        return []

def get_baseline(prefix):
    print(f"Fetching Baseline for prefix: {prefix}...")
    query = BASELINE_QUERY.replace("%PREFIX%", prefix)
    results = run_query(query)
    # Return dictionary mapping concept URI to its ICD code for easy diffs
    return {r['c']['value']: r['code']['value'] for r in results}

def benchmark_icd_codes(test_cases):
    all_results = []
    
    for uri, prefix in test_cases.items():
        print(f"\n--- Benchmarking Root: {uri} (Prefix: {prefix}) ---")
        
        # 1. Get Control Group (Baseline)
        baseline_data = get_baseline(prefix)
        baseline_set = set(baseline_data.keys())
        print(f"Baseline Count (Starts with '{prefix}'): {len(baseline_set)}")
        
        # 2. Run Expansions
        for strategy_name, query_template in QUERIES.items():
            query_text = query_template.replace("%ROOT_URI%", uri)
            results = run_query(query_text)
            
            expansion_data = {r['c']['value']: r['code']['value'] for r in results}
            expansion_set = set(expansion_data.keys())
            
            # 3. Calculate Deltas
            gained_uris = expansion_set - baseline_set
            lost_uris = baseline_set - expansion_set
            
            # Get the actual ICD codes for the sample output
            gained_codes = [expansion_data[u] for u in gained_uris]
            lost_codes = [baseline_data[u] for u in lost_uris]
            
            all_results.append({
                "icd_root": uri,
                "strategy": strategy_name,
                "baseline_count": len(baseline_set),
                "expansion_count": len(expansion_set),
                "gained_count": len(gained_codes),
                "lost_count": len(lost_codes),
                "sample_gained_codes": gained_codes[:3], # Top 3 gained
                "sample_lost_codes": lost_codes[:3]      # Top 3 lost
            })
            
    return pd.DataFrame(all_results)

if __name__ == "__main__":
    # Dictionary of Root URI : Lexical Prefix
    test_cases = {
        # 1. The Anchor (Circulatory / Trauma Overlap)
        "icd10:I64": "I64",       # Stroke, unspecified
        
        # 2. The Systemic Web (Endocrine -> Eye/Kidney/Neuro)
        "icd10:E11": "E11",       # Type 2 diabetes mellitus
        
        # 3. The Autoimmune Discovery (Endocrine)
        "icd10:E10.9": "E10.9",   # Type 1 diabetes without complications (Will it find LADA?)
        
        # 4. The Psychiatric Drift (Mental Health)
        "icd10:F32": "F32",       # Major depressive disorder (Does it drift into bipolar or anxiety?)
        
        # 5. The Anatomical Specificity (Neoplasms)
        "icd10:C50": "C50",       # Malignant neoplasm of breast (Does it pull in generic metastases?)
        
        # 6. The Syndrome Boundary (Respiratory)
        "icd10:J45": "J45",       # Asthma (Does it appropriately pull in COPD or inappropriately pull in generic coughs?)
        
        # 7. The Autoimmune Cascade (Musculoskeletal)
        "icd10:M06.9": "M06.9",   # Rheumatoid arthritis, unspecified (Should pull in specific joint involvements)
        
        # 8. The Pathogen Web (Infectious Disease)
        "icd10:A41.9": "A41.9",   # Sepsis, unspecified organism (Will it expand to specific bacterial sepses?)
        
        # 9. The Tightly Bounded Acute Event (Digestive)
        "icd10:K35": "K35",       # Acute appendicitis (Control case: should have very little cross-chapter drift)
        
        # 10. The Cross-Chapter Classic (Nervous System vs. Mental Health)
        "icd10:G30.9": "G30.9"    # Alzheimer's disease, unspecified (Should correctly discover F00/F01 Dementia codes)
    }
    
    df_metrics = benchmark_icd_codes(test_cases)

    print("\n--- Benchmark Results ---")
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    print(df_metrics[['icd_root', 'strategy', 'expansion_count', 'gained_count', 'lost_count', 'sample_gained_codes', 'sample_lost_codes']])
    df_metrics.to_csv("./icd_10_benchmarks.csv", index=False)