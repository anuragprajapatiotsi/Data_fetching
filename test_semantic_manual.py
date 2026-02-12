import asyncio
import sys
import os

# Add project root to path
sys.path.append(os.getcwd())

from app.services import semantic_service
from app.repositories import semantic_repository, metadata_repository
from app.core.database import engine

async def main():
    print("--- Testing Semantic Service ---")
    
    # 1. Get Types
    print("\n1. Fetching Semantic Types...")
    try:
        types = await semantic_service.get_all_semantic_types()
        print(f"Found {len(types)} types.")
        print(types[:3])
    except Exception as e:
        print(f"Error fetching types: {e}")
        # If this fails, tables might not exist.
        return

    # 2. List tables to find a target
    print("\n2. Finding a target table...")
    try:
        tables_map = await metadata_repository._get_schemas_and_tables()
        if 'public' in tables_map and tables_map['public']:
            table_name = tables_map['public'][0]
            schema_name = 'public'
            print(f"Target: {schema_name}.{table_name}")
            
            # Get columns
            cols = await metadata_repository._get_table_columns_with_types(schema_name, table_name)
            if cols:
                col_name = cols[0]['key']
                current_type = cols[0]['type']
                print(f"Target Column: {col_name} (Current Type: {current_type})")
                
                # 3. Create Mapping
                print(f"\n3. Mapping {col_name} to DIM_024 (Date)...")
                # Assuming DIM_024 exists and is Date.
                await semantic_service.save_semantic_mapping(schema_name, table_name, col_name, "DIM_024")
                print("Mapping saved.")
                
                # 4. Verify Merged Columns
                print("\n4. Verifying Merged Columns...")
                merged = await semantic_service.get_merged_columns(schema_name, table_name)
                
                target_col = next((c for c in merged if c["column"] == col_name), None)
                if target_col:
                    print(f"Merged Column: {target_col}")
                    if target_col["semanticType"] == "date":
                        print("SUCCESS: Semantic type is 'date'.")
                    else:
                        print(f"FAILURE: Semantic type is '{target_col['semanticType']}'. Expected 'date'.")
                else:
                    print(f"Column {col_name} not found in merged results.")
            else:
                print(f"No columns in {table_name}")
        else:
            print("No tables in public schema.")
            
    except Exception as e:
        print(f"Error during test: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
