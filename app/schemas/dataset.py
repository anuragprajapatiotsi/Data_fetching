from typing import Optional
from datetime import datetime
from uuid import UUID
from pydantic import BaseModel

class DatasetBase(BaseModel):
    name: str
    description: Optional[str] = None
    created_by: Optional[str] = None

class DatasetCreate(DatasetBase):
    pass

class DatasetResponse(DatasetBase):
    id: UUID
    created_at: datetime

    class Config:
        from_attributes = True
