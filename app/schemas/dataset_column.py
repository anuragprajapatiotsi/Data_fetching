from typing import Optional
from uuid import UUID
from pydantic import BaseModel

class DatasetColumnBase(BaseModel):
    column_name: str
    role: Optional[str] = None
    definition_code: Optional[str] = None
    display_name: Optional[str] = None


class DatasetColumnCreate(DatasetColumnBase):
    pass


class DatasetColumnCreateRequest(DatasetColumnCreate):
    table_id: Optional[UUID] = None
    table_name: Optional[str] = None



class DatasetColumnUpdate(BaseModel):
    role: Optional[str] = None
    definition_code: Optional[str] = None
    display_name: Optional[str] = None

class DatasetColumnResponse(DatasetColumnBase):
    id: UUID
    dataset_table_id: UUID

    class Config:
        from_attributes = True
