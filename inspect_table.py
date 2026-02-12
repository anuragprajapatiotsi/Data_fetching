import asyncio
import sys
import os
from sqlalchemy import text

sys.path.append(os.getcwd())
from app.core.database import engine

async def main():
    async with engine.connect() as conn:
        print("Checking _prebuilt_sys._prebuilt_semantic_modeling_column_types columns...")
        try:
            res = await conn.execute(text("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_schema = '_prebuilt_sys' 
                  AND table_name = '_prebuilt_semantic_modeling_column_types'
            """))
            rows = res.fetchall()
            if rows:
                for r in rows:
                    print(f"Column: {r[0]}, Type: {r[1]}")
            else:
                print("Table not found or no columns.")
                
            # Also check semantic_column_mapping table
            print("\nChecking _prebuilt_sys.semantic_column_mapping columns...")
            res = await conn.execute(text("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_schema = '_prebuilt_sys' 
                  AND table_name = 'semantic_column_mapping'
            """))
            rows = res.fetchall()
            if rows:
                for r in rows:
                    print(f"Column: {r[0]}, Type: {r[1]}")
            else:
                print("Table not found or no columns.")
                
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
