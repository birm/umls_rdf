import pandas as pd
from pathlib import Path

# Root folder
root = Path("analysis_results")

# Query strategy files in order
strategy_files = [
    "lexical_baseline.csv",
    "descendants_only.csv",
    "hierarchy_and_sameas.csv",
    "sameas_expansion_and_umls_rb.csv",
    "dual_semantic_expansion.csv"
]

# Number of example differences to show
n_examples = 5

# Helper function to get label (cid) for a set of concept_ids
def format_examples(df, concept_ids, n=n_examples):
    examples = []
    df_indexed = df.set_index('concept_id')
    for cid in list(concept_ids)[:n]:
        label = df_indexed.at[cid, 'label'] if cid in df_indexed.index else ''
        examples.append(f"  - {label} ({cid})")
    return examples

# Iterate over each subdirectory in analysis_results
for subdir in root.iterdir():
    if subdir.is_dir():
        root = subdir.name[1:]
        report_lines = [f"# Comparison Report for {root}\n"]
        prev_df = None
        
        # Load and compare each strategy file
        for idx, fname in enumerate(strategy_files):
            fpath = subdir / fname
            if not fpath.exists():
                print(f"File not found: {fpath}")
                continue
            
            df = pd.read_csv(fpath, dtype=str).fillna('')  # fill NaN with empty string
            df['concept_id'] = df['concept_id'].astype(str)
            df_set = set(df['concept_id'])

            if idx == 0:
                # Lexical baseline
                report_lines.append(f"## {fname} (Lexical Baseline)")
                empty_labels = df[df['label']=='']
                report_lines.append(f"- Total concepts: {len(df)}")
                report_lines.append(f"- Concepts without labels: {len(empty_labels)}\n")
            else:
                prev_set = set(prev_df['concept_id'])
                
                missing = prev_set - df_set
                added = df_set - prev_set
                
                # Compare to lexical baseline
                lexical_df = pd.read_csv(subdir / "lexical_baseline.csv", dtype=str).fillna('')
                lexical_df['concept_id'] = lexical_df['concept_id'].astype(str)
                lexical_set = set(lexical_df['concept_id'])
                missing_vs_lexical = lexical_set - df_set
                added_vs_lexical = df_set - lexical_set

                report_lines.append(f"## {fname}")
                report_lines.append(f"- Total concepts: {len(df)}")
                
                report_lines.append(f"- Missing compared to previous ({strategy_files[idx-1]}): {len(missing)}")
                report_lines.extend(format_examples(prev_df, missing))
                
                report_lines.append(f"- Added compared to previous ({strategy_files[idx-1]}): {len(added)}")
                report_lines.extend(format_examples(df, added))
                
                report_lines.append(f"- Missing compared to lexical_baseline: {len(missing_vs_lexical)}")
                report_lines.extend(format_examples(lexical_df, missing_vs_lexical))
                
                report_lines.append(f"- Added compared to lexical_baseline: {len(added_vs_lexical)}")
                report_lines.extend(format_examples(df, added_vs_lexical))
                
                report_lines.append("")  # extra newline

            prev_df = df

        # Write the markdown report
        report_path = Path("./analysis_reports/" + f"{root}_comparison_report.md")
        report_path.write_text("\n".join(report_lines))
        print(f"Report written to {report_path}")