from typing import Optional, Any
from pydantic import BaseModel

class QueryRequest(BaseModel):
    query: str
    limit: int = 10
    offset: int = 0
    query_id: Optional[str] = None

class CancelRequest(BaseModel):
    query_id: str
