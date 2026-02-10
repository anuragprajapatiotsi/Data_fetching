from sqlalchemy import text
from app.core.database import engine

async def execute_count_query(sql, params):
    async with engine.connect() as conn:
        total_rows_res = await conn.execute(sql, params)
        return total_rows_res.scalar_one()

async def execute_data_query(sql, params):
    async with engine.connect() as conn:
        return await conn.execute(sql, params)

async def get_backend_pid(conn):
    pid_res = await conn.execute(text("SELECT pg_backend_pid()"))
    return pid_res.scalar_one()
    
async def cancel_backend_pid(pid: int):
     sql = text("SELECT pg_cancel_backend(:pid)")
     async with engine.connect() as conn:
        await conn.execute(sql, {"pid": pid})
