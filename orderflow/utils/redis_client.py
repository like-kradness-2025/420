import redis.asyncio as aioredis
from settings import REDIS_URL

redis_client = None  # type: aioredis.Redis | None

async def init_redis():
    """Initialise global async Redis connection."""
    global redis_client
    redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)

async def close_redis():
    """Close Redis connection gently."""
    if redis_client is not None:
        await redis_client.close()
