from sqlalchemy import text
from app.core.database import engine

async def get_all_semantic_types() -> list[dict[str, str]]:
    sql = text("""
        SELECT sm_column_code as code, referencedimension as label
        FROM _prebuilt_sys._prebuilt_semantic_modeling_column_types
    """)
    async with engine.connect() as conn:
        res = await conn.execute(sql)
        rows = res.fetchall()
    return [{"code": r.code, "label": r.label} for r in rows]

async def get_semantic_mappings(schema: str, table: str) -> dict[str, str]:
    sql = text("""
        SELECT column_name, sm_column_code
        FROM _prebuilt_sys.semantic_column_mapping
        WHERE schema_name = :schema AND table_name = :table
    """)
    async with engine.connect() as conn:
        res = await conn.execute(sql, {"schema": schema, "table": table})
        rows = res.fetchall()
    return {r.column_name: r.sm_column_code for r in rows}

async def upsert_semantic_mapping(schema: str, table: str, column: str, sm_code: str):
    sql = text("""
        INSERT INTO _prebuilt_sys.semantic_column_mapping (schema_name, table_name, column_name, sm_column_code)
        VALUES (:schema, :table, :column, :sm_code)
        ON CONFLICT (schema_name, table_name, column_name)
        DO UPDATE SET sm_column_code = EXCLUDED.sm_column_code
    """)
    async with engine.begin() as conn:
        await conn.execute(sql, {"schema": schema, "table": table, "column": column, "sm_code": sm_code})

async def upsert_bulk_semantic_mappings(mappings: list[dict]):
    sql = text("""
        INSERT INTO _prebuilt_sys.semantic_column_mapping (schema_name, table_name, column_name, sm_column_code)
        VALUES (:schema, :table, :column, :sm_code)
        ON CONFLICT (schema_name, table_name, column_name)
        DO UPDATE SET sm_column_code = EXCLUDED.sm_column_code
    """)
    # Using transaction to ensure all succeed or all fail
    async with engine.begin() as conn:
        for m in mappings:
            await conn.execute(sql, {
                "schema": m["schema"],
                "table": m["table"],
                "column": m["column"],
                "sm_code": m["sm_code"]
            })
        # engine.begin() acts as a context manager that commits on exit


