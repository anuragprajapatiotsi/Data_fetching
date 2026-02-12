import asyncio
import sys
import os

# Add project root to path
sys.path.append(os.getcwd())

from app.services import semantic_service

async def main():
    print("--- Debugging Mapping Logic ---")
    
    # 1. Check loaded types
    types = await semantic_service.get_all_semantic_types()
    print(f"Loaded {len(types)} Types")
    # print(types)
    
    # 2. Check map logic
    test_codes = ["DIM_024", "DIM_023", "DIM_014", "DIM_001"]
    for code in test_codes:
        ui_type = semantic_service.map_semantic_to_ui_type(code)
        # We need to access the internal mapping logic to see WHAT label it finds
        label = next((t['label'] for t in types if t['code'] == code), "NOT FOUND")
        print(f"Code: {code} -> Label: {label} -> UI Type: {ui_type}")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
