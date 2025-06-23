import asyncpg
from settings import PG_DSN

pg_pool = None  # type: asyncpg.pool.Pool | None

async def init_db():
    global pg_pool
    pg_pool = await asyncpg.create_pool(dsn=PG_DSN, min_size=2, max_size=10)

async def close_db():
    if pg_pool is not None:
        await pg_pool.close()

# TODO: add concrete insert helpers (`insert_open_interest`, `insert_trade_agg_1s`, etc.).
