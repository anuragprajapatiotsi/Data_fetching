import asyncio
import sys
import os

# Add current directory to path
sys.path.append(os.getcwd())

from main import (
    _get_pg_schemas, 
    _get_pg_tables,
    _get_pg_views,
    _get_pg_matviews,
    _get_pg_indexes,
    _get_pg_sequences,
    _get_pg_datatypes,
    _get_pg_functions, 
    _get_pg_columns,
    engine
)

async def test():
    try:
        print("# Testing Refined Metadata (Correct Queries)")
        
        # 1. Schemas
        print("\n[1] Schemas:")
        schemas = await _get_pg_schemas()
        print(f"    {schemas}")
        
        if not schemas:
            print("No schemas found, aborting.")
            return

        target_schema = "public"
        if target_schema not in schemas:
            target_schema = schemas[0]
            
        print(f"\n# Testing objects in schema '{target_schema}'")

        # 2. Tables
        tables = await _get_pg_tables(target_schema)
        print(f"\n[2] Tables: {len(tables)} found")
        if tables:
            print(f"    First 3: {tables[:3]}")
            
        # 3. Views
        views = await _get_pg_views(target_schema)
        print(f"\n[3] Views: {len(views)} found")
        if views:
            print(f"    First 3: {views[:3]}")

        # 4. Materialized Views
        matviews = await _get_pg_matviews(target_schema)
        print(f"\n[4] Mat Views: {len(matviews)} found")
        if matviews:
            print(f"    First 3: {matviews[:3]}")

        # 5. Indexes
        indexes = await _get_pg_indexes(target_schema)
        print(f"\n[5] Indexes: {len(indexes)} found")
        if indexes:
            print(f"    First 3: {indexes[:3]}")

        # 6. Sequences
        seqs = await _get_pg_sequences(target_schema)
        print(f"\n[6] Sequences: {len(seqs)} found")
        if seqs:
            print(f"    First 3: {seqs[:3]}")
            
        # 7. Functions
        funcs = await _get_pg_functions(target_schema)
        print(f"\n[7] Functions: {len(funcs)} found")
        if funcs:
            print(f"    First 3: {funcs[:3]}")

        # 8. Data Types
        types = await _get_pg_datatypes(target_schema)
        print(f"\n[8] Data Types: {len(types)} found")
        if types:
            print(f"    First 3: {types[:3]}")
            
        # 9. Columns (if a table exists)
        if tables:
            t = tables[0]
            print(f"\n[9] Columns for table '{t}':")
            cols = await _get_pg_columns(target_schema, t)
            for c in cols[:3]:
                print(f"    - {c['name']}: {c['type']} (Nullable: {c['nullable']})")
                
    except Exception as e:
        print(f"ERROR: {e}")
    finally:
        await engine.dispose()

if __name__ == "__main__":
    asyncio.run(test())
