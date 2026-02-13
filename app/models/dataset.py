# import uuid
# from sqlalchemy import Column, String, Text, DateTime, func, ForeignKey, Float
# from sqlalchemy.orm import relationship
# from sqlalchemy.dialects.postgresql import UUID
# from app.models.base import Base

# class Dataset(Base):
#     __tablename__ = "datasets"

#     id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
#     name = Column(String, nullable=False)
#     description = Column(Text, nullable=True)
#     created_by = Column(String, nullable=True)
#     created_at = Column(DateTime(timezone=True), server_default=func.now())

#     tables = relationship("DatasetTable", back_populates="dataset", cascade="all, delete-orphan", lazy="selectin")
#     joins = relationship("DatasetJoin", back_populates="dataset", cascade="all, delete-orphan", lazy="selectin")

# class DatasetJoin(Base):
#     __tablename__ = "dataset_joins"

#     id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
#     dataset_id = Column(UUID(as_uuid=True), ForeignKey("datasets.id", ondelete="CASCADE"), nullable=False)
#     left_table = Column(String, nullable=False)
#     left_column = Column(String, nullable=False)
#     right_table = Column(String, nullable=False)
#     right_column = Column(String, nullable=False)
#     join_type = Column(String, nullable=False) # 'inner', 'left', 'right'

#     dataset = relationship("Dataset", back_populates="joins")

# class DatasetTable(Base):
#     __tablename__ = "dataset_tables"

#     id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
#     dataset_id = Column(UUID(as_uuid=True), ForeignKey("datasets.id", ondelete="CASCADE"), nullable=False)
#     table_name = Column(String, nullable=False)
#     alias = Column(String, nullable=True)
#     position_x = Column(Float, default=0.0)
#     position_y = Column(Float, default=0.0)


#     dataset = relationship("Dataset", back_populates="tables")
#     columns = relationship("DatasetColumn", back_populates="table", cascade="all, delete-orphan", lazy="selectin")

# class DatasetColumn(Base):
#     __tablename__ = "dataset_columns"

#     id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
#     dataset_table_id = Column(UUID(as_uuid=True), ForeignKey("dataset_tables.id", ondelete="CASCADE"), nullable=False)
#     column_name = Column(String, nullable=False)
#     role = Column(String, nullable=True) # Dimension | Indicator
#     definition_code = Column(String, nullable=True)
#     display_name = Column(String, nullable=True)

#     table = relationship("DatasetTable", back_populates="columns")



import uuid
from sqlalchemy import (
    Column,
    String,
    Text,
    DateTime,
    func,
    ForeignKey,
    Float,
    UniqueConstraint
)
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import UUID
from app.models.base import Base


# =========================================================
# DATASET (Logical Dataset Container)
# =========================================================
class Dataset(Base):
    __tablename__ = "datasets"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String, nullable=False)
    description = Column(Text, nullable=True)
    created_by = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Canvas tables (nodes)
    tables = relationship(
        "DatasetTable",
        back_populates="dataset",
        cascade="all, delete-orphan",
        lazy="selectin"
    )

    # Join relationships (edges)
    joins = relationship(
        "DatasetJoin",
        back_populates="dataset",
        cascade="all, delete-orphan",
        lazy="selectin"
    )


# =========================================================
# DATASET TABLE (Canvas Node)
# Represents a table dragged onto canvas
# =========================================================
class DatasetTable(Base):
    __tablename__ = "dataset_tables"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    dataset_id = Column(
        UUID(as_uuid=True),
        ForeignKey("datasets.id", ondelete="CASCADE"),
        nullable=False
    )

    # Physical DB table name
    table_name = Column(String, nullable=False)

    # Alias used in SQL generation (important for joins & self joins)
    alias = Column(String, nullable=True)

    # Canvas position
    position_x = Column(Float, default=0.0)
    position_y = Column(Float, default=0.0)

    dataset = relationship("Dataset", back_populates="tables")

    # Semantic column mappings
    columns = relationship(
        "DatasetColumn",
        back_populates="table",
        cascade="all, delete-orphan",
        lazy="selectin"
    )


# =========================================================
# DATASET COLUMN (Semantic Column Mapping)
# Maps DB column -> Business meaning
# =========================================================
class DatasetColumn(Base):
    __tablename__ = "dataset_columns"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    # VERY IMPORTANT: column belongs to a dataset node, NOT a DB table
    dataset_table_id = Column(
        UUID(as_uuid=True),
        ForeignKey("dataset_tables.id", ondelete="CASCADE"),
        nullable=False
    )

    # Physical column name
    column_name = Column(String, nullable=False)

    # Business role
    # "Dimension" or "Indicator"
    role = Column(String, nullable=True)

    # DIM_001, IND_001 etc
    definition_code = Column(String, nullable=True)

    # User-friendly name shown in Data tab
    display_name = Column(String, nullable=True)

    table = relationship("DatasetTable", back_populates="columns")

    # Prevent duplicate mappings
    __table_args__ = (
        UniqueConstraint(
            "dataset_table_id",
            "column_name",
            name="uq_dataset_column"
        ),
    )


# =========================================================
# DATASET JOIN (Relationship between TWO dataset nodes)
# This is the MOST IMPORTANT model
# =========================================================
class DatasetJoin(Base):
    __tablename__ = "dataset_joins"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    dataset_id = Column(
        UUID(as_uuid=True),
        ForeignKey("datasets.id", ondelete="CASCADE"),
        nullable=False
    )

    # LEFT SIDE (node + column)
    left_dataset_table_id = Column(
        UUID(as_uuid=True),
        ForeignKey("dataset_tables.id", ondelete="CASCADE"),
        nullable=False
    )
    left_column = Column(String, nullable=False)

    # RIGHT SIDE (node + column)
    right_dataset_table_id = Column(
        UUID(as_uuid=True),
        ForeignKey("dataset_tables.id", ondelete="CASCADE"),
        nullable=False
    )
    right_column = Column(String, nullable=False)

    # inner | left | right
    join_type = Column(String, nullable=False)

    dataset = relationship("Dataset", back_populates="joins")

    # These relationships are CRITICAL for SQL generation
    left_table = relationship(
        "DatasetTable",
        foreign_keys=[left_dataset_table_id]
    )

    right_table = relationship(
        "DatasetTable",
        foreign_keys=[right_dataset_table_id]
    )
