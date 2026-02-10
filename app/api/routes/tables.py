from typing import Literal, Optional
from fastapi import APIRouter, Query
from app.services import metadata_service
from app.utils.sql_safety import _validate_ident

# We need SortDir definition or just use str
SortDir = Literal["asc", "desc"]

router = APIRouter()

@router.get("/tables")
async def get_tables(schema: str = Query("public")):
    _validate_ident(schema, "schema")
    # This was calling _get_all_tables
    return await metadata_service.metadata_repository._get_all_tables(schema)

@router.get("/schemas")
async def get_schemas():
    return await metadata_service.metadata_repository._get_schemas_and_tables()

@router.get("/table")
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
    _validate_ident(schema, "schema")
    _validate_ident(table, "table")
    
    return await metadata_service.get_table_details(
        schema=schema,
        table=table,
        limit=limit,
        offset=offset,
        sort_by=sort_by,
        sort_dir=sort_dir,
        filters=filters,
        auto_generate_schema=auto_generate_schema
    )
