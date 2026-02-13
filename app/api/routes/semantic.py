from fastapi import APIRouter, Body
from pydantic import BaseModel, Field
from typing import List
from app.services import semantic_service

router = APIRouter(prefix="/semantic", tags=["Semantic"])

class SemanticTypeResponse(BaseModel):
    code: str
    label: str
    

class SemanticMappingRequest(BaseModel):
    schema_name: str = Field(..., alias="schema")
    table_name: str = Field(..., alias="table")
    column_name: str = Field(..., alias="column")
    sm_code: str

@router.get("/types", response_model=List[SemanticTypeResponse])
async def get_semantic_types():
    return await semantic_service.get_all_semantic_types()

@router.post("/mapping")
async def create_semantic_mapping(request: SemanticMappingRequest):
    await semantic_service.save_semantic_mapping(
        request.schema_name, 
        request.table_name, 
        request.column_name, 
        request.sm_code
    )
    return {"status": "success"}

class BulkSemanticMappingRequest(BaseModel):
    mappings: List[SemanticMappingRequest]

@router.post("/mapping/bulk")
async def create_bulk_semantic_mapping(request: BulkSemanticMappingRequest):
    # Convert Pydantic models to list of dicts for service
    data = [
        {
            "schema": m.schema_name,
            "table": m.table_name,
            "column": m.column_name,
            "sm_code": m.sm_code
        }
        for m in request.mappings
    ]
    return await semantic_service.save_bulk_semantic_mappings(data)

@router.get("/columns")
async def get_semantic_columns(schema: str, table: str):
    return await semantic_service.get_merged_columns(schema, table)

modeling_router = APIRouter(prefix="/semantic-modeling", tags=["Semantic Modeling"])

class SemanticColumnTypeResponse(BaseModel):
    code: str
    type: str
    category: str
    name: str

@modeling_router.get("/column-types", response_model=List[SemanticColumnTypeResponse])
async def get_semantic_column_types(type: str | None = None):
    return await semantic_service.get_semantic_column_types(type)
