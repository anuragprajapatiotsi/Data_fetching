DATABASE_URL = "postgresql+asyncpg://postgres:Rahul0905%40%23@localhost:5432/postgres"

engine: AsyncEngine = create_async_engine(DATABASE_URL, pool_pre_ping=True)
