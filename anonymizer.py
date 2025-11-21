import re

def normalize_query(query: str):
    q = re.sub(r"\s+", " ", query.strip())

    # ONLY match variable names before a label: var:Label
    vars_found = re.findall(r"\b([a-z][a-zA-Z0-9_]*)\s*:(?=[A-Z])", q)

    mapping = {var: f"v{i+1}" for i, var in enumerate(vars_found)}

    # Replace ONLY whole variable names
    for old, new in mapping.items():
        q = re.sub(rf"\b{old}\b", new, q)

    # Remove aliases
    q = re.sub(r"AS\s+\w+", "", q)

    return q
