from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api.routes import query, metadata, tables, semantic, datasets

app = FastAPI(title="Postgres Table API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(query.router)
app.include_router(metadata.router)
app.include_router(tables.router)
app.include_router(semantic.router)
app.include_router(semantic.modeling_router)
app.include_router(datasets.router)

from app.core.database import engine
from app.models.base import Base
# Make sure models are imported so they are registered with Base metadata
from app.models.dataset import Dataset 

@app.on_event("startup")
async def init_tables():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
