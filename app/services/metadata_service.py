from typing import Any, Optional
import json
from pathlib import Path
from fastapi import HTTPException
from sqlalchemy import text

from app.repositories import metadata_repository, query_repository
from app.utils.cache import _columns_cache

COLUMNS_PATH = Path("./columns.json")

def load_columns_dynamic(db_columns: list[str], auto_generate: bool = True) -> list[dict[str, Any]]:
    if not COLUMNS_PATH.exists():
        return [{"key": c, "label": c, "type": "string", "enableSorting": True} for c in db_columns]

    mtime = COLUMNS_PATH.stat().st_mtime
    if _columns_cache.mtime != mtime:
        raw = json.loads(COLUMNS_PATH.read_text())
        _columns_cache.value = raw
        _columns_cache.mtime = mtime

    columns = _columns_cache.value or []

    existing = {c["key"] for c in columns}
    for col in db_columns:
        if col not in existing:
            columns.append({"key": col, "label": col, "type": "string", "enableSorting": True})

    return columns


def _cast_value(raw: Any, col_type: str) -> Any:
    if raw is None:
        return None
    if col_type == "number":
        return raw
    if col_type == "boolean":
        return bool(raw)
    if col_type in {"date", "datetime"}:
        return raw.isoformat() if hasattr(raw, "isoformat") else str(raw)
    return str(raw)


async def get_table_details(
    schema: str, 
    table: str, 
    limit: int, 
    offset: int, 
    sort_by: Optional[str], 
    sort_dir: str, 
    filters: Optional[str], 
    auto_generate_schema: bool
):
    # Columns + types from DB
    db_cols_with_types = await metadata_repository._get_table_columns_with_types(schema, table)
    db_cols = [c["key"] for c in db_cols_with_types]
    type_map = {c["key"]: c["type"] for c in db_cols_with_types}

    columns = load_columns_dynamic(db_cols, auto_generate_schema)
    for col in columns:
        col["type"] = type_map.get(col["key"], col.get("type", "string"))

    col_map = {c["key"]: c for c in columns}

    # -------- Sorting --------
    if sort_by:
        if sort_by not in db_cols:
            raise HTTPException(status_code=400, detail=f"Unknown sort_by: {sort_by}")
        if not col_map.get(sort_by, {}).get("enableSorting", True):
            raise HTTPException(status_code=400, detail=f"Sorting disabled for: {sort_by}")

    # -------- Filtering --------
    where_clauses = []
    bind_params = {}

    if filters:
        try:
            filter_list = json.loads(filters)
            if not isinstance(filter_list, list):
                raise ValueError
        except ValueError:
            raise HTTPException(status_code=400, detail="Filters must be a JSON array")

        for idx, f in enumerate(filter_list):
            field = f.get("field")
            op = f.get("op")
            value = f.get("value")

            if not field or not op or value in {None, ""}:
                continue

            if field not in db_cols:
                raise HTTPException(status_code=400, detail=f"Invalid filter field: {field}")

            col_type = col_map[field]["type"]

            allowed_ops = {
                "string": {"eq", "contains", "starts_with", "ends_with"},
                "number": {"eq", "gt", "gte", "lt", "lte"},
                "boolean": {"eq"},
                "date": {"eq", "gt", "gte", "lt", "lte"},
                "datetime": {"eq", "gt", "gte", "lt", "lte"},
            }

            if op not in allowed_ops.get(col_type, {"eq"}):
                raise HTTPException(
                    status_code=400,
                    detail=f"Operator '{op}' not valid for {col_type}",
                )

            p = f"p{idx}"

            if op == "eq":
                where_clauses.append(f'"{field}" = :{p}')
                bind_params[p] = value
            elif op in {"gt", "gte", "lt", "lte"}:
                sym = {"gt": ">", "gte": ">=", "lt": "<", "lte": "<="}[op]
                where_clauses.append(f'"{field}" {sym} :{p}')
                bind_params[p] = value
            elif op == "contains":
                where_clauses.append(f'"{field}" ILIKE :{p}')
                bind_params[p] = f"%{value}%"
            elif op == "starts_with":
                where_clauses.append(f'"{field}" ILIKE :{p}')
                bind_params[p] = f"{value}%"
            elif op == "ends_with":
                where_clauses.append(f'"{field}" ILIKE :{p}')
                bind_params[p] = f"%{value}"

    where_sql = f" WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    order_sql = f' ORDER BY "{sort_by}" {sort_dir.upper()}' if sort_by else ""

    sql_rows = text(
        f'SELECT * FROM "{schema}"."{table}"{where_sql}{order_sql} LIMIT :limit OFFSET :offset'
    )
    sql_count = text(f'SELECT COUNT(*) FROM "{schema}"."{table}"{where_sql}')

    # Execute
    total = await query_repository.execute_count_query(sql_count, bind_params)
    
    rows_res = await query_repository.execute_data_query(
        sql_rows, 
        {**bind_params, "limit": limit, "offset": offset}
    )
    rows = rows_res.mappings().all()

    data = []
    for r in rows:
        row = {}
        for k, v in r.items():
            row[k] = _cast_value(v, col_map[k]["type"])
        data.append(row)

    return {
        "columns": columns,
        "data": data,
        "meta": {
            "total": total,
            "limit": limit,
            "offset": offset,
            "table": f"{schema}.{table}",
        },
    }

async def get_pg_schemas():
    return await metadata_repository._get_pg_schemas()

async def get_pg_tables(schema: str):
    return await metadata_repository._get_pg_tables(schema)

async def get_pg_views(schema: str):
    return await metadata_repository._get_pg_views(schema)

async def get_pg_matviews(schema: str):
    return await metadata_repository._get_pg_matviews(schema)

async def get_pg_indexes(schema: str):
    return await metadata_repository._get_pg_indexes(schema)

async def get_pg_sequences(schema: str):
    return await metadata_repository._get_pg_sequences(schema)

async def get_pg_datatypes(schema: str):
    return await metadata_repository._get_pg_datatypes(schema)

async def get_pg_functions(schema: str):
    return await metadata_repository._get_pg_functions(schema)

async def get_pg_columns(schema: str, table: str):
    return await metadata_repository._get_pg_columns(schema, table)
