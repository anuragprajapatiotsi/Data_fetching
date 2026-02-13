from typing import Literal
from uuid import UUID
from pydantic import BaseModel


class DatasetJoinBase(BaseModel):
    left_dataset_table_id: UUID
    left_column: str
    right_dataset_table_id: UUID
    right_column: str
    join_type: Literal["inner", "left", "right"]


class DatasetJoinCreate(DatasetJoinBase):
    pass

class DatasetJoinResponse(DatasetJoinBase):
    id: UUID
    dataset_id: UUID

    class Config:
        from_attributes = True
