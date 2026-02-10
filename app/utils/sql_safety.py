import re
from fastapi import HTTPException

SAFE_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

def _validate_ident(name: str, what: str) -> str:
    if not SAFE_IDENT.match(name):
        raise HTTPException(status_code=400, detail=f"Invalid {what}: {name}")
    return name

def _is_query_safe(sql: str) -> bool:
    # Normalize for checking
    sql_clean = sql.strip()
    
    # 1. Deny-list of destructive keywords (matched as whole words)
    #    We use regex \bKEYWORD\b to avoid matching "update_date" or "insert_id"
    unsafe_keywords = [
        "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "TRUNCATE", 
        "CREATE", "GRANT", "REVOKE", "COPY", "CALL", "DO", "EXEC", "EXECUTE"
    ]
    pattern = r"\b(" + "|".join(unsafe_keywords) + r")\b"
    
    if re.search(pattern, sql_clean, re.IGNORECASE):
        return False

    # 2. Disallow multiple statements (semicolon check)
    #    We allow a single trailing semicolon, but not multiple statements.
    #    Naive check: if ; appears before the end (ignoring whitespace).
    #    (This is imperfect without a parser but catches simple "SELECT ...; DROP ..." attacks)
    if ";" in sql_clean[:-1]:
        return False
        
    return True
