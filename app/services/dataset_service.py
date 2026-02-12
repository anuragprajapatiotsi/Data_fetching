from typing import List, Optional
from uuid import UUID
from sqlalchemy.ext.asyncio import AsyncSession
from app.repositories import dataset_repository
from app.schemas.dataset import DatasetCreate
from app.models.dataset import Dataset, DatasetTable, DatasetJoin
from app.schemas.dataset_table import DatasetTableCreate, DatasetTableUpdate
from app.schemas.dataset_join import DatasetJoinCreate

async def create_dataset(session: AsyncSession, dataset: DatasetCreate) -> Dataset:
    return await dataset_repository.create_dataset(session, dataset)

async def get_dataset(session: AsyncSession, dataset_id: UUID) -> Optional[Dataset]:
    return await dataset_repository.get_dataset(session, dataset_id)

async def get_all_datasets(session: AsyncSession) -> List[Dataset]:
    return await dataset_repository.get_all_datasets(session)

async def delete_dataset(session: AsyncSession, dataset_id: UUID) -> bool:
    return await dataset_repository.delete_dataset(session, dataset_id)

async def add_table(session: AsyncSession, dataset_id: UUID, table_data: DatasetTableCreate) -> DatasetTable:
    return await dataset_repository.add_table(session, dataset_id, table_data)

async def get_tables(session: AsyncSession, dataset_id: UUID) -> List[DatasetTable]:
    return await dataset_repository.get_tables(session, dataset_id)

async def delete_table(session: AsyncSession, dataset_id: UUID, table_id: UUID) -> bool:
    return await dataset_repository.delete_table(session, dataset_id, table_id)

async def add_join(session: AsyncSession, dataset_id: UUID, join_data: DatasetJoinCreate) -> DatasetJoin:
    return await dataset_repository.add_join(session, dataset_id, join_data)

async def get_joins(session: AsyncSession, dataset_id: UUID) -> List[DatasetJoin]:
    return await dataset_repository.get_joins(session, dataset_id)

async def delete_join(session: AsyncSession, dataset_id: UUID, join_id: UUID) -> bool:
    return await dataset_repository.delete_join(session, dataset_id, join_id)

async def update_table_position(
    session: AsyncSession, 
    dataset_id: UUID, 
    table_id: UUID, 
    update_data: DatasetTableUpdate
) -> Optional[DatasetTable]:
    return await dataset_repository.update_table_position(session, dataset_id, table_id, update_data)
