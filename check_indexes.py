import asyncio
import sys
import os
from sqlalchemy import text

sys.path.append(os.getcwd())
from app.core.database import engine

async def main():
    async with engine.connect() as conn:
        print("Checking indexes on _prebuilt_sys.semantic_column_mapping...")
        try:
            res = await conn.execute(text("""
                SELECT indexname, indexdef 
                FROM pg_indexes 
                WHERE schemaname = '_prebuilt_sys' 
                  AND tablename = 'semantic_column_mapping'
            """))
            rows = res.fetchall()
            for r in rows:
                print(f"Index: {r[0]}")
                print(f"Def: {r[1]}")
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
