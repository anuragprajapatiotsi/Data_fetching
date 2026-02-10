from fastapi import APIRouter
from app.services import metadata_service

router = APIRouter()

@router.get("/metadata/schemas")
async def get_metadata_schemas():
    return await metadata_service.get_pg_schemas()

@router.get("/metadata/schemas/{schema}/tables")
async def get_metadata_tables(schema: str):
    # Validation logic was: _validate_ident(schema, "schema")
    # We should add that. Let's import from utils.
    from app.utils.sql_safety import _validate_ident
    _validate_ident(schema, "schema")
    return await metadata_service.get_pg_tables(schema)

@router.get("/metadata/schemas/{schema}/views")
async def get_metadata_views(schema: str):
    from app.utils.sql_safety import _validate_ident
    _validate_ident(schema, "schema")
    return await metadata_service.get_pg_views(schema)

@router.get("/metadata/schemas/{schema}/matviews")
async def get_metadata_matviews(schema: str):
    from app.utils.sql_safety import _validate_ident
    _validate_ident(schema, "schema")
    return await metadata_service.get_pg_matviews(schema)

@router.get("/metadata/schemas/{schema}/indexes")
async def get_metadata_indexes(schema: str):
    from app.utils.sql_safety import _validate_ident
    _validate_ident(schema, "schema")
    return await metadata_service.get_pg_indexes(schema)

@router.get("/metadata/schemas/{schema}/sequences")
async def get_metadata_sequences(schema: str):
    from app.utils.sql_safety import _validate_ident
    _validate_ident(schema, "schema")
    return await metadata_service.get_pg_sequences(schema)

@router.get("/metadata/schemas/{schema}/datatypes")
async def get_metadata_datatypes(schema: str):
    from app.utils.sql_safety import _validate_ident
    _validate_ident(schema, "schema")
    return await metadata_service.get_pg_datatypes(schema)

@router.get("/metadata/schemas/{schema}/functions")
async def get_metadata_functions(schema: str):
    from app.utils.sql_safety import _validate_ident
    _validate_ident(schema, "schema")
    return await metadata_service.get_pg_functions(schema)

@router.get("/metadata/schemas/{schema}/columns")
async def get_metadata_columns(schema: str, table: str):
    from app.utils.sql_safety import _validate_ident
    _validate_ident(schema, "schema")
    _validate_ident(table, "table")
    return await metadata_service.get_pg_columns(schema, table)
