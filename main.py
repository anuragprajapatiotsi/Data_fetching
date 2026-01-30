from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Optional

from fastapi import FastAPI, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine


app = FastAPI(title="Postgres Table API")

# Example: postgresql+asyncpg://user:password@localhost:5432/mydb
DATABASE_URL = "postgresql+asyncpg://postgres:Rahul0905%40%23@localhost:5432/postgres"

engine: AsyncEngine = create_async_engine(
    DATABASE_URL,
    pool_pre_ping=True,
)

COLUMNS_PATH = Path("./columns.json")

ColumnType = Literal["string", "number", "boolean", "date", "datetime"]
SortDir = Literal["asc", "desc"]

SAFE_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


# ---------- reload-if-changed cache for columns.json ----------

@dataclass
class FileCache:
    mtime: float = -1.0
    value: Any = None


_columns_cache = FileCache()


def _dedupe_columns(columns: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for c in columns:
        k = c.get("key")
        if isinstance(k, str) and k and k not in seen:
            seen.add(k)
            out.append(c)
    return out


def load_columns_dynamic(db_columns: list[str], *, auto_generate: bool = True) -> list[dict[str, Any]]:
    """
    Load columns.json (with mtime cache). If missing/invalid and auto_generate=True,
    generate schema using DB column names.
    """
    if not COLUMNS_PATH.exists():
        if auto_generate:
            return [{"key": c, "label": c, "type": "string", "enableSorting": True} for c in db_columns]
        return []

    try:
        mtime = COLUMNS_PATH.stat().st_mtime
        if _columns_cache.mtime != mtime:
            raw = json.loads(COLUMNS_PATH.read_text(encoding="utf-8"))
            if not isinstance(raw, list):
                raise ValueError("columns.json must be a JSON array")
            _columns_cache.value = _dedupe_columns(raw)
            _columns_cache.mtime = mtime

        columns: list[dict[str, Any]] = _columns_cache.value or []

        if auto_generate:
            existing = {c.get("key") for c in columns}
            for col in db_columns:
                if col not in existing:
                    columns.append({"key": col, "label": col, "type": "string", "enableSorting": True})

        return columns
    except Exception:
        if auto_generate:
            return [{"key": c, "label": c, "type": "string", "enableSorting": True} for c in db_columns]
        raise


def _cast_value(raw: Any, col_type: str) -> Any:
    if raw is None:
        return None

    t = (col_type or "string").lower()

    # DB types usually come out correctly already, but we keep logic for consistency.
    if t == "string":
        return str(raw)

    if t == "number":
        # If numeric already, return it; else try conversions
        if isinstance(raw, (int, float)):
            return raw
        s = str(raw).strip()
        try:
            return int(s)
        except ValueError:
            try:
                return float(s)
            except ValueError:
                return s

    if t == "boolean":
        if isinstance(raw, bool):
            return raw
        s = str(raw).strip().lower()
        if s in {"true", "1", "yes", "y"}:
            return True
        if s in {"false", "0", "no", "n"}:
            return False
        return s

    # date/datetime: keep as ISO string
    if t in {"date", "datetime"}:
        try:
            return raw.isoformat()  # works for date/datetime objects
        except Exception:
            return str(raw)

    return raw


def _validate_ident(name: str, what: str) -> str:
    if not SAFE_IDENT.match(name):
        raise HTTPException(status_code=400, detail=f"Invalid {what}: {name}")
    return name


async def _get_table_columns(schema: str, table: str) -> list[str]:
    """
    Returns column names in table order using information_schema.
    """
    sql = text(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = :schema AND table_name = :table
        ORDER BY ordinal_position
        """
    )
    async with engine.connect() as conn:
        res = await conn.execute(sql, {"schema": schema, "table": table})
        cols = [r[0] for r in res.fetchall()]
    if not cols:
        raise HTTPException(status_code=404, detail=f"Table not found or has no columns: {schema}.{table}")
    return cols


@app.get("/table")
async def get_table(
    table: str = Query(..., description="DB table name (e.g. matches)"),
    schema: str = Query("public", description="DB schema (default public)"),
    limit: int = Query(50, ge=1, le=5000),
    offset: int = Query(0, ge=0),
    sort_by: Optional[str] = Query(None),
    sort_dir: SortDir = Query("asc"),
    auto_generate_schema: bool = Query(True),
):
    schema = _validate_ident(schema, "schema")
    table = _validate_ident(table, "table")

    # Discover DB columns dynamically (so table schema can change)
    db_cols = await _get_table_columns(schema, table)

    if sort_by:
        if sort_by not in db_cols:
            raise HTTPException(status_code=400, detail=f"Unknown sort_by: {sort_by}")
        _validate_ident(sort_by, "sort_by")

    # Load columns.json dynamically (add missing DB cols if enabled)
    columns = load_columns_dynamic(db_cols, auto_generate=auto_generate_schema)
    col_map = {c["key"]: c for c in columns if isinstance(c, dict) and "key" in c}

    # Respect enableSorting if present
    if sort_by:
        schema_entry = col_map.get(sort_by)
        if schema_entry is not None and not bool(schema_entry.get("enableSorting", True)):
            raise HTTPException(status_code=400, detail=f"Sorting disabled for: {sort_by}")

    # Build safe SQL (identifiers validated; values parameterized)
    order_clause = ""
    if sort_by:
        order_clause = f' ORDER BY "{sort_by}" {sort_dir.upper()} '

    sql_rows = text(
        f'SELECT * FROM "{schema}"."{table}"'
        + order_clause
        + " LIMIT :limit OFFSET :offset"
    )

    sql_count = text(f'SELECT COUNT(*) FROM "{schema}"."{table}"')

    async with engine.connect() as conn:
        total = (await conn.execute(sql_count)).scalar_one()
        res = await conn.execute(sql_rows, {"limit": limit, "offset": offset})
        rows = res.mappings().all()  # list[RowMapping]

    # Cast based on schema types
    data: list[dict[str, Any]] = []
    for r in rows:
        obj: dict[str, Any] = {}
        for k in r.keys():
            schema_entry = col_map.get(k, {})
            obj[k] = _cast_value(r[k], str(schema_entry.get("type", "string")))
        data.append(obj)

    return {
        "columns": columns,
        "data": data,
        "meta": {"total": total, "limit": limit, "offset": offset, "table": f"{schema}.{table}"},
    }
