import asyncio
from sqlalchemy import text
from app.core.database import engine

async def inspect():
    async with engine.connect() as conn:
        # Try to select * limit 0 to get description or columns
        try:
            result = await conn.execute(text("SELECT * FROM _prebuilt_sys._prebuilt_semantic_modeling_column_types LIMIT 1"))
            print("Columns:", result.keys())
            row = result.fetchone()
            if row:
                print("First row:", row._mapping)
        except Exception as e:
            print("Error:", e)

if __name__ == "__main__":
    asyncio.run(inspect())
