from sqlalchemy import text
from fastapi import HTTPException
from app.core.database import engine

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


async def _get_pg_columns(schema: str, table_name: str) -> list[dict[str, str]]:
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
