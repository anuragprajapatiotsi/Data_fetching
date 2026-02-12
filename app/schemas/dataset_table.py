from typing import Optional
from uuid import UUID
from pydantic import BaseModel

class DatasetTableBase(BaseModel):
    table_name: str
    alias: Optional[str] = None
    position_x: float = 0.0
    position_y: float = 0.0

class DatasetTableCreate(DatasetTableBase):
    pass

class DatasetTableUpdate(BaseModel):
    position_x: Optional[float] = None
    position_y: Optional[float] = None

class DatasetTableResponse(DatasetTableBase):
    id: UUID
    dataset_id: UUID

    class Config:
        from_attributes = True
