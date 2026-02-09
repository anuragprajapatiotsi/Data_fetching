import asyncio
import sys
import os

# Add current directory to path so we can import main
sys.path.append(os.getcwd())

from main import _get_all_schemas, _get_raw_columns, _get_all_tables, engine

async def test():
    try:
        print("Testing schemas...")
        schemas = await _get_all_schemas()
        print(f"Schemas: {schemas}")
        
        print("\nTesting tables (public)...")
        tables = await _get_all_tables("public")
        print(f"Tables: {tables}")
        
        if tables:
            t = tables[0]
            print(f"\nTesting columns for 'public.{t}'...")
            cols = await _get_raw_columns("public", t)
            print(f"Columns (first 1): {cols[:1]}") 
        else:
            print("\nNo tables found in public schema to test columns.")
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        await engine.dispose()

if __name__ == "__main__":
    asyncio.run(test())
