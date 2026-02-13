import asyncio
from sqlalchemy import text
from app.core.database import engine
from app.models.base import Base
# Import models to ensure they are registered for create_all
from app.models.dataset import Dataset, DatasetTable, DatasetJoin, DatasetColumn

async def fix_schema():
    print("Dropping dataset_joins table...")
    async with engine.begin() as conn:
        await conn.execute(text("DROP TABLE IF EXISTS dataset_joins CASCADE"))
    
    print("Recreating tables...")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    
    print("Schema fixed.")

if __name__ == "__main__":
    asyncio.run(fix_schema())
