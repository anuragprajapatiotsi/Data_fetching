from typing import List, Dict, Any
from app.repositories import semantic_repository, metadata_repository

# In-memory caches
_semantic_cache: Dict[tuple, List[Dict[str, Any]]] = {}
_semantic_types_cache: List[Dict[str, str]] = []

async def get_all_semantic_types() -> List[Dict[str, str]]:
    global _semantic_types_cache
    if not _semantic_types_cache:
        _semantic_types_cache = await semantic_repository.get_all_semantic_types()
    return _semantic_types_cache

async def save_semantic_mapping(schema: str, table: str, column: str, sm_code: str):
    await semantic_repository.upsert_semantic_mapping(schema, table, column, sm_code)
    # Invalidate cache for this table
    if (schema, table) in _semantic_cache:
        del _semantic_cache[(schema, table)]

async def save_bulk_semantic_mappings(mappings: List[Dict[str, str]]) -> Dict[str, Any]:
    # 1. Transform to list of dicts for repo
    # mappings input is expected to be list of dicts with keys: schema, table, column, sm_code
    
    # 2. Call repo
    await semantic_repository.upsert_bulk_semantic_mappings(mappings)
    
    # 3. Invalidate cache
    # Find all unique (schema, table) pairs
    affected_tables = set()
    for m in mappings:
        affected_tables.add((m["schema"], m["table"]))
        
    for key in affected_tables:
        if key in _semantic_cache:
            del _semantic_cache[key]
            
    return {
        "updated": len(mappings), # Approximate, since we don't distinguish insert vs update
        "inserted": 0,
        "failed": 0,
        "message": "Bulk semantic mapping applied successfully"
    }

def _map_label_to_ui_type(label: str) -> str:
    l = label.lower()
    if "timestamp" in l: return "datetime"
    if "date" in l: return "date"
    if any(x in l for x in ["integer", "number", "decimal", "currency", "percentage"]): return "number"
    if "boolean" in l: return "boolean"
    if "string" in l: return "string"
    return "string"

def map_semantic_to_ui_type(sm_code: str) -> str:
    # This helper might need the list of types to be loaded. 
    # For now, we rely on the caller or use the internal cache if available.
    # If the cache is empty, we default to string or need to fetch.
    # Since this is synchronous helper, we can't await. 
    # We'll assume types are loaded or we map based on known codes if needed, 
    # but the robust way is looking up the label.
    # Given the constraint, we'll iterate the cache if available.
    if _semantic_types_cache:
        for t in _semantic_types_cache:
            if t["code"] == sm_code:
                return _map_label_to_ui_type(t["label"])
    return "string" # Default

def _get_filter_type(ui_type: str) -> str:
    if ui_type in ["date", "datetime", "number"]:
        return "range"
    if ui_type == "string":
        return "contains"
    return "equals"

async def get_merged_columns(schema: str, table: str) -> List[Dict[str, Any]]:
    # Check cache
    if (schema, table) in _semantic_cache:
        return _semantic_cache[(schema, table)]

    # 1. Get DB columns (real columns)
    # db_cols is [{"key": "col_name", "type": "ui_type"}] 
    # metadata_repository returns ui aligned types (number, string, date, boolean)
    try:
        db_cols = await metadata_repository._get_table_columns_with_types(schema, table)
    except Exception:
        # If table not found or error, re-raise or return empty
        db_cols = []

    # 2. Get Semantic Mappings
    mappings = await semantic_repository.get_semantic_mappings(schema, table)
    
    # 3. Get Semantic Types to resolve labels
    # Ensure cache is populated
    global _semantic_types_cache
    if not _semantic_types_cache:
        _semantic_types_cache = await semantic_repository.get_all_semantic_types()
    
    code_to_label = {t["code"]: t["label"] for t in _semantic_types_cache}
    
    merged = []
    for col in db_cols:
        col_name = col["key"]
        initial_type = col["type"] # This is already a UI type from metadata_repo
        
        final_type = initial_type
        source = "database"
        
        if col_name in mappings:
            sm_code = mappings[col_name]
            label = code_to_label.get(sm_code)
            if label:
                ui_type = _map_label_to_ui_type(label)
                # Override if valid
                if ui_type:
                    final_type = ui_type
                    source = "semantic"
        
        merged.append({
            "column": col_name,
            "semanticType": final_type,
            "filterType": _get_filter_type(final_type),
            "source": source
        })
        
    # Update cache
    _semantic_cache[(schema, table)] = merged
    return merged

async def get_semantic_column_types(type_filter: str | None = None) -> List[Dict[str, str]]:
    return await semantic_repository.get_semantic_column_types(type_filter)
