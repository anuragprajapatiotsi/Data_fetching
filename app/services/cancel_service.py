from app.repositories import query_repository

QUERY_PIDS: dict[str, int] = {}

def register_pid(query_id: str, pid: int):
    QUERY_PIDS[query_id] = pid

def unregister_pid(query_id: str):
    QUERY_PIDS.pop(query_id, None)

def get_pid(query_id: str) -> int | None:
    return QUERY_PIDS.get(query_id)

async def cancel_query_by_id(query_id: str) -> int:
    pid = get_pid(query_id)
    if not pid:
        return None
    
    await query_repository.cancel_backend_pid(pid)
    return pid
