import asyncio
import sys
import os
from sqlalchemy import text

sys.path.append(os.getcwd())
from app.core.database import engine

async def main():
    async with engine.connect() as conn:
        print("Checking _prebuilt_sys.semantic_column_mapping data...")
        try:
            res = await conn.execute(text("""
                SELECT * 
                FROM _prebuilt_sys.semantic_column_mapping
            """))
            rows = res.fetchall()
            for r in rows:
                print(r)
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
