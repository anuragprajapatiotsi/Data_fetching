from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Optional
import uuid

from fastapi import FastAPI, HTTPException, Query, Body
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine


app = FastAPI(title="Postgres Table API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DATABASE_URL = "postgresql+asyncpg://postgres:Rahul0905%40%23@localhost:5432/postgres"

engine: AsyncEngine = create_async_engine(DATABASE_URL, pool_pre_ping=True)

COLUMNS_PATH = Path("./columns.json")

ColumnType = Literal["string", "number", "boolean", "date", "datetime"]
SortDir = Literal["asc", "desc"]

SAFE_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# ---------------- Global State ----------------

QUERY_PIDS: dict[str, int] = {}


# ---------------- Cache ----------------

@dataclass
class FileCache:
    mtime: float = -1.0
    value: Any = None


_columns_cache = FileCache()


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


# ---------------- DB helpers ----------------

async def _get_table_columns_with_types(schema: str, table: str) -> list[dict[str, str]]:
    sql = text("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = :schema
          AND table_name = :table
        ORDER BY ordinal_position
    """)
    async with engine.connect() as conn:
        res = await conn.execute(sql, {"schema": schema, "table": table})
        rows = res.fetchall()

    if not rows:
        raise HTTPException(status_code=404, detail="Table not found")

    def map_type(db_type: str) -> str:
        t = db_type.lower()
        if t in {"integer", "bigint", "smallint", "numeric", "real", "double precision"}:
            return "number"
        if t == "boolean":
            return "boolean"
        if t == "date":
            return "date"
        if t.startswith("timestamp"):
            return "datetime"
        return "string"

    return [{"key": r[0], "type": map_type(r[1])} for r in rows]


async def _get_all_tables(schema: str) -> list[str]:
    sql = text("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = :schema
        ORDER BY table_name
    """)
    async with engine.connect() as conn:
        res = await conn.execute(sql, {"schema": schema})
        rows = res.fetchall()
    return [r[0] for r in rows]


async def _get_schemas_and_tables() -> dict[str, list[str]]:
    sql = text("""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
        ORDER BY table_schema, table_name
    """)
    async with engine.connect() as conn:
        res = await conn.execute(sql)
        rows = res.fetchall()

    result = {}
    for schema, table in rows:
        if schema not in result:
            result[schema] = []
        result[schema].append(table)
    
    return result


async def _get_pg_schemas() -> list[str]:
    # Exclude system schemas usually hidden in DBeaver/pgAdmin unless enabled
    sql = text("""
        SELECT nspname
        FROM pg_catalog.pg_namespace
        WHERE nspname !~ '^pg_'
          AND nspname <> 'information_schema'
        ORDER BY nspname
    """)
    async with engine.connect() as conn:
        res = await conn.execute(sql)
        rows = res.fetchall()
    return [r[0] for r in rows]


    return [r[0] for r in rows]


async def _get_pg_tables(schema: str) -> list[str]:
    sql = text("""
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = :schema
    """)
    async with engine.connect() as conn:
        res = await conn.execute(sql, {"schema": schema})
        return [r[0] for r in res.fetchall()]


async def _get_pg_views(schema: str) -> list[str]:
    sql = text("""
        SELECT viewname
        FROM pg_views
        WHERE schemaname = :schema
    """)
    async with engine.connect() as conn:
        res = await conn.execute(sql, {"schema": schema})
        return [r[0] for r in res.fetchall()]


async def _get_pg_matviews(schema: str) -> list[str]:
    sql = text("""
        SELECT matviewname
        FROM pg_matviews
        WHERE schemaname = :schema
    """)
    async with engine.connect() as conn:
        res = await conn.execute(sql, {"schema": schema})
        return [r[0] for r in res.fetchall()]


async def _get_pg_indexes(schema: str) -> list[dict[str, str]]:
    sql = text("""
        SELECT indexname, tablename
        FROM pg_indexes
        WHERE schemaname = :schema
    """)
    async with engine.connect() as conn:
        res = await conn.execute(sql, {"schema": schema})
        return [{"name": r[0], "table": r[1]} for r in res.fetchall()]


async def _get_pg_sequences(schema: str) -> list[str]:
    sql = text("""
        SELECT c.relname AS sequence_name
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind = 'S'
          AND n.nspname = :schema
    """)
    async with engine.connect() as conn:
        res = await conn.execute(sql, {"schema": schema})
        return [r[0] for r in res.fetchall()]


async def _get_pg_datatypes(schema: str) -> list[str]:
    sql = text("""
        SELECT typname
        FROM pg_catalog.pg_type t
        JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
        WHERE n.nspname = :schema
    """)
    async with engine.connect() as conn:
        res = await conn.execute(sql, {"schema": schema})
        return [r[0] for r in res.fetchall()]


async def _get_pg_functions(schema: str) -> list[str]:
    sql = text("""
        SELECT proname
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = :schema
    """)
    async with engine.connect() as conn:
        res = await conn.execute(sql, {"schema": schema})
        return [r[0] for r in res.fetchall()]


async def _get_pg_columns(schema: str, table_name: str) -> list[dict[str, Any]]:
    # Resolve table OID first (or join, but two steps is cleaner for safety check)
    # We can join pg_class + pg_namespace + pg_attribute + pg_type
    sql = text("""
        SELECT 
            a.attname,
            format_type(a.atttypid, a.atttypmod) as format_type,
            a.attnotnull,
            a.attnum
        FROM pg_catalog.pg_attribute a
        JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = :schema
          AND c.relname = :table
          AND a.attnum > 0 
          AND NOT a.attisdropped
        ORDER BY a.attnum
    """)
    async with engine.connect() as conn:
        res = await conn.execute(sql, {"schema": schema, "table": table_name})
        rows = res.fetchall()
        
    return [
        {
            "name": r[0],
            "type": r[1],
            "nullable": not r[2],
            "position": r[3]
        }
        for r in rows
    ]


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



# ---------------- API ----------------

class QueryRequest(BaseModel):
    query: str
    limit: int = 10
    offset: int = 0
    query_id: Optional[str] = None

class CancelRequest(BaseModel):
    query_id: str

@app.post("/query/cancel")
async def cancel_query(request: CancelRequest):
    query_id = request.query_id
    pid = QUERY_PIDS.get(query_id)
    
    if not pid:
         raise HTTPException(status_code=404, detail="Query ID not found or query already completed")
         
    try:
        # Use a new connection to cancel
        sql = text("SELECT pg_cancel_backend(:pid)")
        async with engine.connect() as conn:
            await conn.execute(sql, {"pid": pid})
            
        return {"cancelled": True, "pid": pid}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/query")
async def execute_query(request: QueryRequest):
    original_sql = request.query.strip()
    limit = request.limit
    offset = request.offset
    query_id = request.query_id or str(uuid.uuid4())

    # 1. Safety Check
    if not _is_query_safe(original_sql):
        raise HTTPException(
            status_code=400, 
            detail="Query contains restricted keywords (e.g. INSERT, UPDATE, DROP) or multiple statements."
        )
    
    # 2. Wrap query to enforce row limit and pagination
    #    Remove trailing semicolon if present to avoid syntax error in subquery
    inner_sql = original_sql.rstrip(";")
    
    #    We select * from the user's query with LIMIT and OFFSET
    wrapped_sql = text(f"""
        SELECT * FROM (
            {inner_sql}
        ) AS limited_query
        LIMIT :limit OFFSET :offset
    """)
    
    #    Count query to get total rows
    count_sql = text(f"""
        SELECT COUNT(*) FROM (
            {inner_sql}
        ) AS count_query
    """)
    
    try:
        async with engine.connect() as conn:
            # 2.5 Query Tracking
            # Get current PID
            pid_res = await conn.execute(text("SELECT pg_backend_pid()"))
            pid = pid_res.scalar_one()
            
            QUERY_PIDS[query_id] = pid
            
            try:
                # Execute count query
                total_rows_res = await conn.execute(count_sql)
                total_rows = total_rows_res.scalar_one()

                # Execute data query
                result = await conn.execute(wrapped_sql, {"limit": limit, "offset": offset})
                
                # 3. Safe Fetch
                #    We fetch up to 'limit' rows.
                rows = result.fetchmany(limit)
                
                #    Get columns
                keys = list(result.keys())
                
                data = []
                for r in rows:
                    # Convert RowMapping/Row to dict, casting values
                    row_dict = {}
                    mapping = r._mapping if hasattr(r, '_mapping') else r
                    
                    for k, v in mapping.items():
                        row_dict[k] = str(v) if v is not None else None
                    data.append(row_dict)
                
                row_count = len(data)
                # has_more is true if we haven't reached the total count yet
                has_more = (offset + row_count) < total_rows
                
                return {
                    "columns": [{"key": k, "label": k, "type": "string"} for k in keys],
                    "data": data,
                    "row_count": row_count,
                    "total_rows": total_rows,
                    "has_more": has_more,
                    "query_id": query_id,
                    "error": None
                }
            except SQLAlchemyError as e:
                # Catch specific cancellation error if possible, but generic handling is fine
                # Postgres error code for query cancelled is 57014 (query_canceled)
                # But here we just re-raise or handle.
                # If wrapped in DBAPIError, orig.pgcode might be available.
                raise e
            finally:
                # Clean up PID
                QUERY_PIDS.pop(query_id, None)
            
    except SQLAlchemyError as e:
        # Catch SQL syntax errors from the user's query
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/tables")
async def get_tables(schema: str = Query("public")):
    _validate_ident(schema, "schema")
    tables = await _get_all_tables(schema)
    return tables


@app.get("/schemas")
async def get_schemas():
    return await _get_schemas_and_tables()

@app.get("/table")
async def get_table(
    table: str = Query(...),
    schema: str = Query("public"),
    limit: int = Query(50, ge=1, le=5000),
    offset: int = Query(0, ge=0),
    sort_by: Optional[str] = Query(None),
    sort_dir: SortDir = Query("asc"),
    filters: Optional[str] = Query(None),
    auto_generate_schema: bool = Query(True),
):
    schema = _validate_ident(schema, "schema")
    table = _validate_ident(table, "table")

    # Columns + types from DB
    db_cols_with_types = await _get_table_columns_with_types(schema, table)
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

    async with engine.connect() as conn:
        total = (await conn.execute(sql_count, bind_params)).scalar_one()
        rows = (
            await conn.execute(
                sql_rows,
                {**bind_params, "limit": limit, "offset": offset},
            )
        ).mappings().all()

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


# ---------------- Metadata API (pg_catalog) ----------------

@app.get("/metadata/schemas")
async def get_metadata_schemas():
    return await _get_pg_schemas()


@app.get("/metadata/schemas/{schema}/tables")
async def get_metadata_tables(schema: str):
    _validate_ident(schema, "schema")
    return await _get_pg_tables(schema)


@app.get("/metadata/schemas/{schema}/views")
async def get_metadata_views(schema: str):
    _validate_ident(schema, "schema")
    return await _get_pg_views(schema)


@app.get("/metadata/schemas/{schema}/matviews")
async def get_metadata_matviews(schema: str):
    _validate_ident(schema, "schema")
    return await _get_pg_matviews(schema)


@app.get("/metadata/schemas/{schema}/indexes")
async def get_metadata_indexes(schema: str):
    _validate_ident(schema, "schema")
    return await _get_pg_indexes(schema)


@app.get("/metadata/schemas/{schema}/sequences")
async def get_metadata_sequences(schema: str):
    _validate_ident(schema, "schema")
    return await _get_pg_sequences(schema)


@app.get("/metadata/schemas/{schema}/datatypes")
async def get_metadata_datatypes(schema: str):
    _validate_ident(schema, "schema")
    return await _get_pg_datatypes(schema)


@app.get("/metadata/schemas/{schema}/functions")
async def get_metadata_functions(schema: str):
    _validate_ident(schema, "schema")
    return await _get_pg_functions(schema)


@app.get("/metadata/schemas/{schema}/columns")
async def get_metadata_columns(
    schema: str, 
    table: str = Query(..., description="Table or View name")
):
    _validate_ident(schema, "schema")
    _validate_ident(table, "table")
    return await _get_pg_columns(schema, table)
