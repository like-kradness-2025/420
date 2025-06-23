"""Consumes Redis streams and flushes to PostgreSQL every FLUSH_SEC seconds."""
import asyncio, time
from utils.redis_client import init_redis, redis_client, close_redis
from utils.db import init_db, close_db, pg_pool
from utils.agg import add_trade, flush_buckets
from settings import FLUSH_SEC

STREAMS = {"orderflow:oi": ">", "orderflow:trade": ">"}
GROUP   = "db-writer"
CONSUMER= "writer-1"

async def ensure_groups():
    for stream in STREAMS:
        try:
            await redis_client.xgroup_create(stream, GROUP, id="$", mkstream=True)
        except Exception:
            pass  # group exists

async def process_messages():
    last_flush = time.time()
    while True:
        msgs = await redis_client.xreadgroup(GROUP, CONSUMER, STREAMS, count=500, block=2000)
        for stream, entries in msgs:
            for msg_id, data in entries:
                if stream.endswith(":trade"):
                    add_trade(data["ex"], data["sym"], data["side"], float(data["px"]), float(data["qty"]), int(data["ts"]))
                elif stream.endswith(":oi"):
                    # TODO: buffer OI if needed, else insert directly later
                    pass
                await redis_client.xack(stream, GROUP, msg_id)

        if time.time() - last_flush >= FLUSH_SEC:
            await flush_to_db()
            last_flush = time.time()

async def flush_to_db():
    """Insert aggregated trade + OI rows into Postgres."""
    async with pg_pool.acquire() as conn:
        async with conn.transaction():
            # insert trades
            for row in flush_buckets():
                await conn.execute(
                    """
                    INSERT INTO trade_agg_1s (ts_1s,exchange,symbol,side,volume,vwap,trades,min_price,max_price)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                    ON CONFLICT DO NOTHING
                    """,
                    row["ts"], row["exchange"], row["symbol"], row["side"],
                    row["volume"], row["vwap"], row["trades"], row["min_price"], row["max_price"],
                )
            # TODO: insert open_interest_history rows (buffer separately)

async def main():
    await init_redis(); await init_db(); await ensure_groups()
    try:
        await process_messages()
    finally:
        await close_db(); await close_redis()

if __name__ == "__main__":
    asyncio.run(main())
