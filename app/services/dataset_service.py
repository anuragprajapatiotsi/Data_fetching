from typing import List, Optional
from uuid import UUID
from sqlalchemy.ext.asyncio import AsyncSession
from app.repositories import dataset_repository
from app.schemas.dataset import DatasetCreate
from app.models.dataset import Dataset, DatasetTable, DatasetJoin
from app.schemas.dataset_table import DatasetTableCreate, DatasetTableUpdate
from app.schemas.dataset_join import DatasetJoinCreate

async def create_dataset(session: AsyncSession, dataset: DatasetCreate) -> Dataset:
    return await dataset_repository.create_dataset(session, dataset)

async def get_dataset(session: AsyncSession, dataset_id: UUID) -> Optional[Dataset]:
    return await dataset_repository.get_dataset(session, dataset_id)

async def get_all_datasets(session: AsyncSession) -> List[Dataset]:
    return await dataset_repository.get_all_datasets(session)

async def delete_dataset(session: AsyncSession, dataset_id: UUID) -> bool:
    return await dataset_repository.delete_dataset(session, dataset_id)

async def add_table(session: AsyncSession, dataset_id: UUID, table_data: DatasetTableCreate) -> DatasetTable:
    return await dataset_repository.add_table(session, dataset_id, table_data)

async def get_tables(session: AsyncSession, dataset_id: UUID) -> List[DatasetTable]:
    return await dataset_repository.get_tables(session, dataset_id)

async def delete_table(session: AsyncSession, dataset_id: UUID, table_id: UUID) -> bool:
    return await dataset_repository.delete_table(session, dataset_id, table_id)

async def get_table_by_name(session: AsyncSession, dataset_id: UUID, table_name: str) -> Optional[DatasetTable]:
    return await dataset_repository.get_table_by_name(session, dataset_id, table_name)


async def add_join(session: AsyncSession, dataset_id: UUID, join_data: DatasetJoinCreate) -> Optional[DatasetJoin]:
    return await dataset_repository.add_join(session, dataset_id, join_data)

async def get_joins(session: AsyncSession, dataset_id: UUID) -> List[DatasetJoin]:
    return await dataset_repository.get_joins(session, dataset_id)

async def delete_join(session: AsyncSession, dataset_id: UUID, join_id: UUID) -> bool:
    return await dataset_repository.delete_join(session, dataset_id, join_id)

async def update_table_position(
    session: AsyncSession, 
    dataset_id: UUID, 
    table_id: UUID, 
    update_data: DatasetTableUpdate
) -> Optional[DatasetTable]:
    return await dataset_repository.update_table_position(session, dataset_id, table_id, update_data)
    return await dataset_repository.update_table_position(session, dataset_id, table_id, update_data)

from app.models.dataset import DatasetColumn
from app.schemas.dataset_column import DatasetColumnCreate

async def save_column_metadata(
    session: AsyncSession, 
    dataset_id: UUID, 
    table_id: UUID, 
    column_data: DatasetColumnCreate
) -> Optional[DatasetColumn]:
    return await dataset_repository.upsert_dataset_column(session, dataset_id, table_id, column_data)

async def get_dataset_columns(session: AsyncSession, dataset_id: UUID) -> List[DatasetColumn]:
    return await dataset_repository.get_dataset_columns(session, dataset_id)

from sqlalchemy import text

async def get_dataset_preview(session: AsyncSession, dataset_id: UUID):
    # 1. Load Metadata
    tables = await dataset_repository.get_tables(session, dataset_id)
    joins = await dataset_repository.get_joins(session, dataset_id)
    columns = await dataset_repository.get_dataset_columns(session, dataset_id)

    if not tables:
         return None # Or raise specific error in route
    
    # Filter only configured columns
    # (The requirement says "If no columns are configured -> return 400", handled in route or here)
    if not columns:
        return {"error": "Dataset has no selected columns"}

    # 2. Determine Base Table
    base_table = tables[0]
    # Use alias if present, else table_name as alias (or generate one, but user said "if alias exists use it")
    # For simplicity if alias is None, let's use formatting 
    # But wait, "If alias exists use it, otherwise generate one automatically."
    # Let's verify if tables have aliases.
    
    # Helper to get alias
    def get_alias(t):
        return t.alias if t.alias else t.table_name

    base_alias = get_alias(base_table)
    
    # Map reference IDs to aliases for easy lookup
    # joins use dataset_table_id, so we need to map dataset_table_id -> alias
    table_map = {t.id: t for t in tables}
    
    # 3. Build Query
    # FROM
    query_from = f"{base_table.table_name} {base_alias}"
    
    # JOINs
    query_joins = ""
    for join in joins:
        # Get left and right table info
        left_t = table_map.get(join.left_dataset_table_id)
        right_t = table_map.get(join.right_dataset_table_id)
        
        if not left_t or not right_t:
             continue # specific error handling?

        left_alias = get_alias(left_t)
        right_alias = get_alias(right_t)
        
        # Join type
        j_type = join.join_type.upper() # LEFT, RIGHT, INNER
        
        query_joins += f"\n{j_type} JOIN {right_t.table_name} {right_alias} ON {left_alias}.{join.left_column} = {right_alias}.{join.right_column}"

    # SELECT & GROUP BY
    select_clauses = []
    group_by_clauses = []
    
    for col in columns:
        # Find which table this column belongs to
        t = table_map.get(col.dataset_table_id)
        if not t:
            continue
        
        t_alias = get_alias(t)
        col_expr = f"{t_alias}.{col.column_name}"
        display = f'"{col.display_name}"' if col.display_name else f'"{col.column_name}"'
        
        if col.role == "Dimension":
            select_clauses.append(f"{col_expr} AS {display}")
            group_by_clauses.append(col_expr) # Group by the expression, not alias (Postgres supports alias in group by but safer to use expr)
        elif col.role == "Indicator":
            # Default aggregation SUM
            select_clauses.append(f"SUM({col_expr}) AS {display}")
            
    if not select_clauses:
         return {"error": "No valid columns to select"}
         
    query_select = "SELECT " + ", ".join(select_clauses)
    
    final_sql = f"{query_select}\nFROM {query_from}{query_joins}"
    
    if group_by_clauses:
        final_sql += "\nGROUP BY " + ", ".join(group_by_clauses)
        
    print(f"Generated SQL:\n{final_sql}") # For debug
    
    # 5. Execute
    try:
        result = await session.execute(text(final_sql))
        rows = result.fetchall()
        # Convert rows to list of lists/dicts
        # keys() returns the columns
        keys = list(result.keys())
        data = [list(row) for row in rows]
        
        return {"columns": keys, "rows": data}
    except Exception as e:
        print(f"SQL Execution failed: {e}")
        raise e
