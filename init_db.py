import asyncio
from app.core.database import engine
from app.models.base import Base
# Import all models to ensure they are registered
from app.models.dataset import Dataset, DatasetTable, DatasetJoin, DatasetColumn

async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    print("Database tables created successfully.")

if __name__ == "__main__":
    asyncio.run(init_db())
