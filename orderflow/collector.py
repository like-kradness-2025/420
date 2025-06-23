"""Collects order-book / trade WS and OI REST and publishes to Redis."""
import asyncio, json, aiohttp, websockets
from settings import WATCH_TARGETS, POLL_INTERVAL_SEC, REDIS_STREAM_MAXLEN
from utils.redis_client import init_redis, close_redis, redis_client
from utils.agg import add_trade

async def publish_stream(key:str, data:dict):
    await redis_client.xadd(key, data, maxlen=REDIS_STREAM_MAXLEN, approximate=True)

async def collect_trades():
    """Connect to WS for trades (placeholder)."""
    # TODO: implement real WS handlers per exchange
    pass

async def poll_open_interest():
    """10-second REST pollers for Binance/Bybit OI."""
    # TODO: loop through WATCH_TARGETS and poll REST endpoints
    pass

async def main():
    await init_redis()
    try:
        await asyncio.gather(
            collect_trades(),
            poll_open_interest(),
        )
    finally:
        await close_redis()

if __name__ == "__main__":
    asyncio.run(main())
