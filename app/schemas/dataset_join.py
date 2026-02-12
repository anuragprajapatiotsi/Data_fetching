from typing import Literal
from uuid import UUID
from pydantic import BaseModel

class DatasetJoinBase(BaseModel):
    left_table: str
    left_column: str
    right_table: str
    right_column: str
    join_type: Literal["inner", "left", "right"]

class DatasetJoinCreate(DatasetJoinBase):
    pass

class DatasetJoinResponse(DatasetJoinBase):
    id: UUID
    dataset_id: UUID

    class Config:
        from_attributes = True
