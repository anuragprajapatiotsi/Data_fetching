from typing import List
from uuid import UUID
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.database import get_db
from app.schemas.dataset import DatasetCreate, DatasetResponse
from app.services import dataset_service

router = APIRouter(prefix="/datasets", tags=["datasets"])

@router.post("/", response_model=DatasetResponse, status_code=status.HTTP_201_CREATED)
async def create_dataset(
    dataset: DatasetCreate,
    db: AsyncSession = Depends(get_db)
):
    return await dataset_service.create_dataset(db, dataset)

@router.get("/", response_model=List[DatasetResponse])
async def get_datasets(
    db: AsyncSession = Depends(get_db)
):
    return await dataset_service.get_all_datasets(db)

@router.get("/{dataset_id}", response_model=DatasetResponse)
async def get_dataset(
    dataset_id: UUID,
    db: AsyncSession = Depends(get_db)
):
    dataset = await dataset_service.get_dataset(db, dataset_id)
    if not dataset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Dataset not found"
        )
    return dataset

@router.delete("/{dataset_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_dataset(
    dataset_id: UUID,
    db: AsyncSession = Depends(get_db)
):
    success = await dataset_service.delete_dataset(db, dataset_id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Dataset not found"
        )

# Dataset Table Endpoints

from app.schemas.dataset_table import DatasetTableCreate, DatasetTableResponse, DatasetTableUpdate

@router.post("/{dataset_id}/tables", response_model=DatasetTableResponse, status_code=status.HTTP_201_CREATED)
async def add_dataset_table(
    dataset_id: UUID,
    table_data: DatasetTableCreate,
    db: AsyncSession = Depends(get_db)
):
    # Verify dataset exists first
    dataset = await dataset_service.get_dataset(db, dataset_id)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
        
    return await dataset_service.add_table(db, dataset_id, table_data)

@router.get("/{dataset_id}/tables", response_model=List[DatasetTableResponse])
async def get_dataset_tables(
    dataset_id: UUID,
    db: AsyncSession = Depends(get_db)
):
    dataset = await dataset_service.get_dataset(db, dataset_id)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    return await dataset_service.get_tables(db, dataset_id)

@router.delete("/{dataset_id}/tables/{table_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_dataset_table(
    dataset_id: UUID,
    table_id: UUID,
    db: AsyncSession = Depends(get_db)
):
    success = await dataset_service.delete_table(db, dataset_id, table_id)
    if not success:
        raise HTTPException(status_code=404, detail="Table not found in dataset")

@router.patch("/{dataset_id}/tables/{table_id}", response_model=DatasetTableResponse)
async def update_dataset_table_position(
    dataset_id: UUID,
    table_id: UUID,
    update_data: DatasetTableUpdate,
    db: AsyncSession = Depends(get_db)
):
    updated_table = await dataset_service.update_table_position(db, dataset_id, table_id, update_data)
    if not updated_table:
        raise HTTPException(status_code=404, detail="Table not found in dataset")
    return updated_table

# Dataset Join Endpoints

from app.schemas.dataset_join import DatasetJoinCreate, DatasetJoinResponse

@router.post("/{dataset_id}/joins", response_model=DatasetJoinResponse, status_code=status.HTTP_201_CREATED)
async def add_dataset_join(
    dataset_id: UUID,
    join_data: DatasetJoinCreate,
    db: AsyncSession = Depends(get_db)
):
    dataset = await dataset_service.get_dataset(db, dataset_id)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    new_join = await dataset_service.add_join(db, dataset_id, join_data)
    if not new_join:
        raise HTTPException(
            status_code=400, 
            detail="Invalid join. Tables must belong to the dataset or join already exists."
        )
    return new_join

@router.get("/{dataset_id}/joins", response_model=List[DatasetJoinResponse])
async def get_dataset_joins(
    dataset_id: UUID,
    db: AsyncSession = Depends(get_db)
):
    dataset = await dataset_service.get_dataset(db, dataset_id)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    return await dataset_service.get_joins(db, dataset_id)

@router.delete("/{dataset_id}/joins/{join_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_dataset_join(
    dataset_id: UUID,
    join_id: UUID,
    db: AsyncSession = Depends(get_db)
):
    success = await dataset_service.delete_join(db, dataset_id, join_id)
    if not success:
        raise HTTPException(status_code=404, detail="Join not found in dataset")

from app.schemas.dataset_column import DatasetColumnCreate, DatasetColumnResponse

@router.put("/{dataset_id}/tables/{table_id}/columns/{column_name}", response_model=DatasetColumnResponse)
async def save_dataset_column(
    dataset_id: UUID,
    table_id: UUID,
    column_name: str,
    column_data: DatasetColumnCreate,
    db: AsyncSession = Depends(get_db)
):
    # Ensure column_name in path matches body or override it
    column_data.column_name = column_name 
    
    saved_column = await dataset_service.save_column_metadata(db, dataset_id, table_id, column_data)
    if not saved_column:
        raise HTTPException(status_code=404, detail="Table not found in dataset")
    if not saved_column:
        raise HTTPException(status_code=404, detail="Table not found in dataset")
    return saved_column

from app.schemas.dataset_column import DatasetColumnCreateRequest

@router.post("/{dataset_id}/columns", response_model=DatasetColumnResponse)
async def create_dataset_column(
    dataset_id: UUID,
    column_request: DatasetColumnCreateRequest,
    db: AsyncSession = Depends(get_db)
):
    # Use dataset_table_id directly
    table_id = column_request.dataset_table_id
    
    # Base payload (DatasetColumnCreate)
    col_data = DatasetColumnCreate(**column_request.model_dump(exclude={"dataset_table_id"}))
    
    saved_column = await dataset_service.save_column_metadata(db, dataset_id, table_id, col_data)
    if not saved_column:
        raise HTTPException(status_code=404, detail="Table not found in dataset")
    return saved_column


@router.get("/{dataset_id}/columns", response_model=List[DatasetColumnResponse])
async def get_dataset_columns(
    dataset_id: UUID,
    db: AsyncSession = Depends(get_db)
):
    dataset = await dataset_service.get_dataset(db, dataset_id)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    return await dataset_service.get_dataset_columns(db, dataset_id)

@router.get("/{dataset_id}/preview")
async def preview_dataset(
    dataset_id: UUID,
    db: AsyncSession = Depends(get_db)
):
    dataset = await dataset_service.get_dataset(db, dataset_id)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    result = await dataset_service.get_dataset_preview(db, dataset_id)
    
    if result and "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
        
    return result





