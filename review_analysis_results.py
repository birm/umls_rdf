#!/usr/bin/env python3
import csv, os, random
from collections import defaultdict

STRATEGIES = [
    "lexical_baseline",
    "descendants_only",
    "partial_synonym_descendants",
    "hierarchy_and_sameas",
    "sameas_expansion_and_umls_rb",
    "sameas_expansion_and_umls_rb_or_par",
    "dual_semantic_expansion"
]

BASE_DIR = "analysis_results"

def load_sets(root):
    sets, labels = {}, {}
    for s in STRATEGIES:
        path = f"{BASE_DIR}/O{root}/{s}.csv"
        ids = set()
        if os.path.exists(path):
            with open(path) as f:
                reader = csv.DictReader(f)
                for row in reader:
                    cid = row["concept_id"]
                    ids.add(cid)
                    if row.get("label"):
                        labels[cid] = row["label"]
        sets[s] = ids
    return sets, labels

def make_dirs(root):
    save_dir = f"./analysis_results/O{root}"
    os.makedirs(save_dir, exist_ok=True)
    return save_dir

def uniqueness_report(sets, labels, save_dir):
    """Report unique and missing concepts per strategy."""
    report_file = os.path.join(save_dir,"strategy_uniqueness.csv")
    with open(report_file,"w",newline="") as f:
        writer = csv.DictWriter(f,fieldnames=[
            "strategy",
            "unique_count",
            "unique_example1_cid",
            "unique_example1_label",
            "unique_example2_cid",
            "unique_example2_label",
            "missing_from_baseline_count",
            "missing_example1_cid",
            "missing_example1_label",
            "missing_example2_cid",
            "missing_example2_label"
        ])
        writer.writeheader()
        
        baseline = sets["lexical_baseline"]
        
        for s, s_ids in sets.items():
            # Unique concepts in this strategy
            unique_ids = s_ids - set().union(*(v for k,v in sets.items() if k != s))
            unique_list = list(unique_ids)  # convert set to list
            unique_examples = random.sample(unique_list, min(2, len(unique_list)))
            while len(unique_examples) < 2:
                unique_examples.append(None)

            # Missing compared to baseline
            missing_from_baseline = baseline - s_ids
            missing_list = list(missing_from_baseline)  # convert set to list
            missing_examples = random.sample(missing_list, min(2, len(missing_list)))
            while len(missing_examples) < 2:
                missing_examples.append(None)

            # Write row
            writer.writerow({
                "strategy": s,
                "unique_count": len(unique_ids),
                "unique_example1_cid": unique_examples[0] or "",
                "unique_example1_label": labels.get(unique_examples[0], "") if unique_examples[0] else "",
                "unique_example2_cid": unique_examples[1] or "",
                "unique_example2_label": labels.get(unique_examples[1], "") if unique_examples[1] else "",
                "missing_from_baseline_count": len(missing_from_baseline),
                "missing_example1_cid": missing_examples[0] or "",
                "missing_example1_label": labels.get(missing_examples[0], "") if missing_examples[0] else "",
                "missing_example2_cid": missing_examples[1] or "",
                "missing_example2_label": labels.get(missing_examples[1], "") if missing_examples[1] else "",
            })

    print("Saved uniqueness/missing report:", report_file)

def analyze(root):
    sets, labels = load_sets(root)
    save_dir = make_dirs(root)
    uniqueness_report(sets, labels, save_dir)

if __name__=="__main__":
    import sys
    if len(sys.argv)<2:
        print("Usage: python review_analysis_results.py ROOT_ID")
        exit(1)
    analyze(sys.argv[1])