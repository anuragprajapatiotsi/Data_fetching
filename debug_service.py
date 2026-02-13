import asyncio
from app.core.database import get_db
from app.services import dataset_service

async def debug():
    print("Testing dataset service...")
    try:
        async for session in get_db():
            print("Session acquired.")
            try:
                datasets = await dataset_service.get_all_datasets(session)
                print(f"Successfully fetched {len(datasets)} datasets.")
                for d in datasets:
                    print(f"Dataset: {d.name}")
                    for t in d.tables:
                        print(f"  Table: {t.table_name}")
                        for c in t.columns:
                            print(f"    Column: {c.column_name}")
            except Exception as e:
                import traceback
                traceback.print_exc()
            break
    except Exception as e:
        print(f"Failed to get DB session: {e}")

if __name__ == "__main__":
    asyncio.run(debug())
