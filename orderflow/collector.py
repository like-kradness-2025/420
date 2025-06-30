"""Collects order-book / trade WS and OI REST and publishes to Redis."""
import asyncio, json, aiohttp, websockets
from settings import WATCH_TARGETS, POLL_INTERVAL_SEC, REDIS_STREAM_MAXLEN
from utils.redis_client import init_redis, close_redis, redis_client
from utils.agg import add_trade

async def publish_stream(key:str, data:dict):
    await redis_client.xadd(key, data, maxlen=REDIS_STREAM_MAXLEN, approximate=True)

async def collect_trades():
    """Subscribe to trade streams from Binance and Bybit."""
    async def binance_ws(symbol: str):
        url = f"wss://stream.binance.com:9443/ws/{symbol.lower()}@trade"
        async with websockets.connect(url) as ws:
            async for msg in ws:
                t = json.loads(msg)
                await publish_stream(
                    "orderflow:trade",
                    {
                        "ex": "binance",
                        "sym": symbol,
                        "side": "sell" if t.get("m") else "buy",
                        "px": t["p"],
                        "qty": t["q"],
                        "ts": t["T"],
                    },
                )

    async def bybit_ws(symbol: str):
        url = "wss://stream.bybit.com/v5/public/linear"
        async with websockets.connect(url) as ws:
            sub = {"op": "subscribe", "args": [f"publicTrade.{symbol}"]}
            await ws.send(json.dumps(sub))
            async for msg in ws:
                data = json.loads(msg)
                if data.get("topic", "").startswith("publicTrade"):
                    for tr in data["data"]:
                        await publish_stream(
                            "orderflow:trade",
                            {
                                "ex": "bybit",
                                "sym": symbol,
                                "side": tr["S"].lower(),
                                "px": tr["p"],
                                "qty": tr["v"],
                                "ts": tr["T"],
                            },
                        )

    tasks = []
    for ex, _, sym in WATCH_TARGETS:
        if ex == "binance":
            tasks.append(asyncio.create_task(binance_ws(sym)))
        elif ex == "bybit":
            tasks.append(asyncio.create_task(bybit_ws(sym)))
    await asyncio.gather(*tasks)

async def poll_open_interest():
    """10-second REST pollers for Binance/Bybit OI."""
    async with aiohttp.ClientSession() as session:
        while True:
            for ex, _, sym in WATCH_TARGETS:
                if ex == "binance":
                    url = "https://fapi.binance.com/futures/data/openInterestHist"
                    params = {"symbol": sym, "period": "5m", "limit": 1}
                    async with session.get(url, params=params) as r:
                        j = await r.json()
                        if j:
                            row = j[-1]
                            await publish_stream(
                                "orderflow:oi",
                                {
                                    "ex": ex,
                                    "sym": sym,
                                    "oi": row["sumOpenInterest"],
                                    "ts": row["timestamp"],
                                },
                            )
                elif ex == "bybit":
                    url = "https://api.bybit.com/v5/market/open-interest"
                    params = {"category": "linear", "symbol": sym}
                    async with session.get(url, params=params) as r:
                        j = await r.json()
                        res = j.get("result", {}).get("list")
                        if res:
                            it = res[0]
                            await publish_stream(
                                "orderflow:oi",
                                {
                                    "ex": ex,
                                    "sym": sym,
                                    "oi": it["openInterest"],
                                    "ts": it["timestamp"],
                                },
                            )
            await asyncio.sleep(POLL_INTERVAL_SEC)

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
