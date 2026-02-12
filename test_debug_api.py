import asyncio
import sys
import os

# Add project root to path
sys.path.append(os.getcwd())

from app.services import semantic_service
from app.repositories import semantic_repository

async def main():
    print("--- Debugging Semantic Service ---")
    
    schema = 'public'
    table = 'ipl_matches'
    
    # 1. Check if mapping exists in DB
    print("\n1. Checking DB Mappings...")
    mappings = await semantic_repository.get_semantic_mappings(schema, table)
    print(f"DB Mappings: {mappings}")
    
    # 2. Check Service Merged Columns (Cache hit?)
    print("\n2. Checking Merged Columns from Service...")
    merged = await semantic_service.get_merged_columns(schema, table)
    
    # Find mapped columns
    for col in merged:
        if col['column'] in mappings:
            print(f"Column: {col['column']}, Type: {col['semanticType']}, Source: {col['source']}")
            
    # 3. Force Invalidate Cache and Retry
    print("\n3. Force Invalidating Cache...")
    if (schema, table) in semantic_service._semantic_cache:
        del semantic_service._semantic_cache[(schema, table)]
        print("Cache deleted.")
    else:
        print("Cache was empty.")
        
    print("\n4. Retry Merged Columns...")
    merged_fresh = await semantic_service.get_merged_columns(schema, table)
    for col in merged_fresh:
        if col['column'] in mappings:
            print(f"Column: {col['column']}, Type: {col['semanticType']}, Source: {col['source']}")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
