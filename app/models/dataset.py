import uuid
from sqlalchemy import Column, String, Text, DateTime, func, ForeignKey, Float
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import UUID
from app.models.base import Base

class Dataset(Base):
    __tablename__ = "datasets"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String, nullable=False)
    description = Column(Text, nullable=True)
    created_by = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    tables = relationship("DatasetTable", back_populates="dataset", cascade="all, delete-orphan", lazy="selectin")
    joins = relationship("DatasetJoin", back_populates="dataset", cascade="all, delete-orphan", lazy="selectin")

class DatasetJoin(Base):
    __tablename__ = "dataset_joins"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    dataset_id = Column(UUID(as_uuid=True), ForeignKey("datasets.id", ondelete="CASCADE"), nullable=False)
    left_table = Column(String, nullable=False)
    left_column = Column(String, nullable=False)
    right_table = Column(String, nullable=False)
    right_column = Column(String, nullable=False)
    join_type = Column(String, nullable=False) # 'inner', 'left', 'right'

    dataset = relationship("Dataset", back_populates="joins")

class DatasetTable(Base):
    __tablename__ = "dataset_tables"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    dataset_id = Column(UUID(as_uuid=True), ForeignKey("datasets.id", ondelete="CASCADE"), nullable=False)
    table_name = Column(String, nullable=False)
    alias = Column(String, nullable=True)
    position_x = Column(Float, default=0.0)
    position_y = Column(Float, default=0.0)

    dataset = relationship("Dataset", back_populates="tables")
