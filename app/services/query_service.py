import uuid
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from fastapi import HTTPException
from app.core.database import engine
from app.repositories import query_repository
from app.services import cancel_service
from app.utils.sql_safety import _is_query_safe

async def execute_query_logic(query: str, limit: int, offset: int, client_query_id: str | None):
    original_sql = query.strip()
    query_id = client_query_id or str(uuid.uuid4())

    # 1. Safety Check
    if not _is_query_safe(original_sql):
        raise HTTPException(
            status_code=400, 
            detail="Query contains restricted keywords (e.g. INSERT, UPDATE, DROP) or multiple statements."
        )
    
    # 2. Wrap query
    inner_sql = original_sql.rstrip(";")
    
    wrapped_sql = text(f"""
        SELECT * FROM (
            {inner_sql}
        ) AS limited_query
        LIMIT :limit OFFSET :offset
    """)
    
    count_sql = text(f"""
        SELECT COUNT(*) FROM (
            {inner_sql}
        ) AS count_query
    """)
    
    try:
        async with engine.connect() as conn:
            # 2.5 Query Tracking
            pid = await query_repository.get_backend_pid(conn)
            cancel_service.register_pid(query_id, pid)
            
            try:
                # Execute count query
                total_rows = await query_repository.execute_count_query(count_sql, {}) # params handled in execute/logic? No, params empty for count usually unless bound
                # Wait, count_sql above wraps inner_sql which might have params? No, user query is raw string here.
                # Actually, the original code executed count_sql on conn.
                # Here we are using query_repository helper which opens NEW connection. This is WRONG if we want to track PID on the same connection?
                # The requirement says "Before running the user SQL, execute SELECT pg_backend_pid()".
                # If we use `query_repository.execute_count_query`, it opens a NEW connection.
                # So we must execute on the SAME connection `conn` we just opened.
                
                # REFACTOR NOTE: The repository pattern suggests passing the connection if transaction/session logic is needed.
                # Or we just do it here for now since `execute_query_logic` is the service layer orchestrating it.
                
                # Execute count (on same conn to be safe? actually count doesn't need same conn, but main query does for PID tracking)
                # Actually PID is for the connection running the query.
                # If we run count on one conn and data on another, distinct PIDs.
                # We want to cancel the DATA query.
                
                # Let's run count first. 
                # Note: `execute_count_query` in repo opens its own connection. 
                # That's fine for count.
                total_rows = await query_repository.execute_count_query(count_sql, {})
                
                # Now the data query.
                # We need PID of the connection that runs data query.
                # So we must get PID and run query on SAME connection.
                
                # Execute data query
                result = await conn.execute(wrapped_sql, {"limit": limit, "offset": offset})
                
                # 3. Safe Fetch
                rows = result.fetchmany(limit)
                keys = list(result.keys())
                
                data = []
                for r in rows:
                    row_dict = {}
                    mapping = r._mapping if hasattr(r, '_mapping') else r
                    for k, v in mapping.items():
                        row_dict[k] = str(v) if v is not None else None
                    data.append(row_dict)
                
                row_count = len(data)
                has_more = (offset + row_count) < total_rows
                
                return {
                    "columns": [{"key": k, "label": k, "type": "string"} for k in keys],
                    "data": data,
                    "row_count": row_count,
                    "total_rows": total_rows,
                    "has_more": has_more,
                    "query_id": query_id,
                    "error": None
                }
            except SQLAlchemyError as e:
                raise e
            finally:
                cancel_service.unregister_pid(query_id)
            
    except SQLAlchemyError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
