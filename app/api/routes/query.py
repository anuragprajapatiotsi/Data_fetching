from fastapi import APIRouter, Body
from app.models.schemas import QueryRequest, CancelRequest
from app.services import query_service, cancel_service

router = APIRouter()

@router.post("/query")
async def execute_query(request: QueryRequest):
    return await query_service.execute_query_logic(
        query=request.query,
        limit=request.limit,
        offset=request.offset,
        client_query_id=request.query_id
    )

@router.post("/query/cancel")
async def cancel_query(request: CancelRequest):
    pid = await cancel_service.cancel_query_by_id(request.query_id)
    if not pid:
         # raise HTTPException(status_code=404, detail="Query ID not found or query already completed")
         # The service returns None if not found. We should raise http exception here or in service.
         # For consistency with original main.py, let's raise it.
         from fastapi import HTTPException
         raise HTTPException(status_code=404, detail="Query ID not found or query already completed")
         
    return {"cancelled": True, "pid": pid}
