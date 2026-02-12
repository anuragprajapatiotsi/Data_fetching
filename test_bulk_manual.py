import asyncio
import sys
import os

# Add project root to path
sys.path.append(os.getcwd())

from app.services import semantic_service
from app.api.routes import semantic
from app.repositories import metadata_repository

async def main():
    print("--- Testing Bulk Semantic API ---")

    # 1. Identify Target Table
    try:
        tables_map = await metadata_repository._get_schemas_and_tables()
        if 'public' in tables_map and tables_map['public']:
            table_name = tables_map['public'][0]
            schema_name = 'public'
            print(f"Target: {schema_name}.{table_name}")
            
            # Get columns to map
            cols = await metadata_repository._get_table_columns_with_types(schema_name, table_name)
            if len(cols) >= 2:
                col1 = cols[0]['key']
                col2 = cols[1]['key']
                print(f"Mapping columns: {col1}, {col2}")
                
                # 2. Persist bulk mapping
                mappings = [
                    {"schema": schema_name, "table": table_name, "column": col1, "sm_code": "DIM_024"}, # Date
                    {"schema": schema_name, "table": table_name, "column": col2, "sm_code": "DIM_023"}  # Integer
                ]
                
                print(f"\nSending bulk request with {len(mappings)} mappings...")
                result = await semantic_service.save_bulk_semantic_mappings(mappings)
                print(f"Result: {result}")
                
                # 3. Verify
                print("\nVerifying Merged Columns...")
                merged = await semantic_service.get_merged_columns(schema_name, table_name)
                
                c1 = next((c for c in merged if c["column"] == col1), None)
                c2 = next((c for c in merged if c["column"] == col2), None)
                
                success = True
                if c1 and c1["semanticType"] == "date":
                    print(f"SUCCESS: {col1} -> date")
                else:
                    print(f"FAILURE: {col1} -> {c1.get('semanticType') if c1 else 'None'}")
                    success = False

                if c2 and c2["semanticType"] == "number": # DIM_023 is Integer -> number
                    print(f"SUCCESS: {col2} -> number")
                else:
                    print(f"FAILURE: {col2} -> {c2.get('semanticType') if c2 else 'None'}")
                    success = False
                    
                if success:
                    print("\nBULK UPDATE TEST PASSED")
                else:
                    print("\nBULK UPDATE TEST FAILED")
            else:
                print("Not enough columns to test bulk mapping.")
        else:
            print("No public tables found.")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
