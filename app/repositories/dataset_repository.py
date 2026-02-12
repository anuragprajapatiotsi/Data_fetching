from typing import List, Optional
from uuid import UUID
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from app.models.dataset import Dataset, DatasetTable, DatasetJoin
from app.schemas.dataset import DatasetCreate
from app.schemas.dataset_table import DatasetTableCreate, DatasetTableUpdate
from app.schemas.dataset_join import DatasetJoinCreate

async def create_dataset(session: AsyncSession, dataset_in: DatasetCreate) -> Dataset:
    db_dataset = Dataset(**dataset_in.model_dump())
    session.add(db_dataset)
    await session.commit()
    await session.refresh(db_dataset)
    return db_dataset

async def get_dataset(session: AsyncSession, dataset_id: UUID) -> Optional[Dataset]:
    result = await session.execute(select(Dataset).where(Dataset.id == dataset_id))
    return result.scalars().first()

async def get_all_datasets(session: AsyncSession) -> List[Dataset]:
    result = await session.execute(select(Dataset))
    return result.scalars().all()

async def delete_dataset(session: AsyncSession, dataset_id: UUID) -> bool:
    db_dataset = await get_dataset(session, dataset_id)
    if db_dataset:
        await session.delete(db_dataset)
        await session.commit()
        return True
    return False

async def add_table(session: AsyncSession, dataset_id: UUID, table_data: DatasetTableCreate) -> DatasetTable:
    db_table = DatasetTable(dataset_id=dataset_id, **table_data.model_dump())
    session.add(db_table)
    await session.commit()
    await session.refresh(db_table)
    return db_table

async def get_tables(session: AsyncSession, dataset_id: UUID) -> List[DatasetTable]:
    result = await session.execute(select(DatasetTable).where(DatasetTable.dataset_id == dataset_id))
    return result.scalars().all()

async def delete_table(session: AsyncSession, dataset_id: UUID, table_id: UUID) -> bool:
    result = await session.execute(
        select(DatasetTable).where(
            DatasetTable.id == table_id,
            DatasetTable.dataset_id == dataset_id
        )
    )
    db_table = result.scalars().first()
    if db_table:
        await session.delete(db_table)
        await session.commit()
        return True
    return False

async def add_join(session: AsyncSession, dataset_id: UUID, join_data: DatasetJoinCreate) -> DatasetJoin:
    db_join = DatasetJoin(dataset_id=dataset_id, **join_data.model_dump())
    session.add(db_join)
    await session.commit()
    await session.refresh(db_join)
    return db_join

async def get_joins(session: AsyncSession, dataset_id: UUID) -> List[DatasetJoin]:
    result = await session.execute(select(DatasetJoin).where(DatasetJoin.dataset_id == dataset_id))
    return result.scalars().all()

async def delete_join(session: AsyncSession, dataset_id: UUID, join_id: UUID) -> bool:
    result = await session.execute(
        select(DatasetJoin).where(
            DatasetJoin.id == join_id,
            DatasetJoin.dataset_id == dataset_id
        )
    )
    db_join = result.scalars().first()
    if db_join:
        await session.delete(db_join)
        await session.commit()
        return True
    return False

async def update_table_position(
    session: AsyncSession, 
    dataset_id: UUID, 
    table_id: UUID, 
    update_data: DatasetTableUpdate
) -> Optional[DatasetTable]:
    result = await session.execute(
        select(DatasetTable).where(
            DatasetTable.id == table_id,
            DatasetTable.dataset_id == dataset_id
        )
    )
    db_table = result.scalars().first()
    
    if not db_table:
        return None
        
    if update_data.position_x is not None:
        db_table.position_x = update_data.position_x
    if update_data.position_y is not None:
        db_table.position_y = update_data.position_y
        
    await session.commit()
    await session.refresh(db_table)
    return db_table
