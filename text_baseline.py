import sqlite3
import re
import unicodedata

def normalize(text: str) -> str:
    if not text: return ""
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii").lower()
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9 ]+", " ", text)).strip()

def get_lexical_baseline(db_file, root_name, domain=None):
    """
    Returns a set of concept_ids. 
    Allows skipping words by using token-based matching instead of phrase matching.
    """
    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    
    # 1. Clean and Normalize
    raw_norm = normalize(root_name)
    
    # 2. Prepare the Greedy FTS Query
    # Remove any FTS5 control characters that might cause errors
    clean_tokens = re.sub(r'[^a-z0-9 ]+', '', raw_norm).split()
    
    # 'heart attack' becomes 'heart AND attack', allowing for skipped words
    fts_greedy = " AND ".join(clean_tokens)
    
    # 3. Use a stable SQL structure
    # We search for the exact phrase first, then the greedy tokens.
    sql = """
        SELECT concept_id FROM concept_fts WHERE name = ?
        UNION
        SELECT concept_id FROM concept_fts WHERE concept_fts MATCH ?
    """
    params = [raw_norm, fts_greedy]

    try:
        c.execute(sql, params)
        # Using a set to ensure unique IDs from the UNION
        return {str(row[0]) for row in c.fetchall()}
    except sqlite3.OperationalError as e:
        # If 'AND' logic fails (e.g. empty query), return empty set
        print(f"FTS Search failed for '{root_name}': {e}")
        return set()
    finally:
        conn.close()