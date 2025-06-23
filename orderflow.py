import asyncio, os, json, math, time, logging
from dataclasses import dataclass
from typing import Dict, List

import websockets
import redis.asyncio as aioredis
import aiosqlite

@dataclass
class Config:
    redis_url: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    sqlite_path: str = os.getenv("SQLITE_PATH", "orderflow.sqlite")
    bucket_size: float = float(os.getenv("BUCKET_SIZE", "10"))
    trim_size: int = int(os.getenv("REDIS_TRIM", "3600"))

CFG = Config()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# --- Redis helpers ---
async def push_and_trim(r: aioredis.Redis, key: str, value: Dict, trim: int = None):
    await r.rpush(key, json.dumps(value))
    if trim:
        await r.ltrim(key, -trim, -1)

# --- Bucketization ---
def bucketize(order_side: List[List[str]], bucket: float, is_bid: bool) -> Dict[str, float]:
    d: Dict[str, float] = {}
    for price_str, qty_str in order_side:
        price = float(price_str)
        qty = float(qty_str)
        if is_bid:
            b = math.floor(price / bucket) * bucket
        else:
            b = math.ceil(price / bucket) * bucket
        d[str(b)] = d.get(str(b), 0.0) + qty
    return d

# --- Aggregators ---
class TradeAggregator:
    def __init__(self, exchange: str, symbol: str, redis: aioredis.Redis):
        self.exchange = exchange
        self.symbol = symbol
        self.redis = redis
        self.cur_ts = None
        self.buffer: List[Dict] = []

    async def add(self, price: float, qty: float, side: str):
        ts = int(time.time())
        if self.cur_ts is None:
            self.cur_ts = ts
        if ts != self.cur_ts:
            await self.flush()
            self.cur_ts = ts
        self.buffer.append({"price": price, "qty": qty, "side": side})

    async def flush(self):
        if not self.buffer:
            return
        prices = [t["price"] for t in self.buffer]
        total_qty = sum(t["qty"] for t in self.buffer)
        buy_qty = sum(t["qty"] for t in self.buffer if t["side"] == "buy")
        sell_qty = total_qty - buy_qty
        data = {
            "timestamp_unix": self.cur_ts,
            "exchange": self.exchange,
            "symbol": self.symbol,
            "open_price": self.buffer[0]["price"],
            "high_price": max(prices),
            "low_price": min(prices),
            "close_price": self.buffer[-1]["price"],
            "total_quantity": total_qty,
            "buy_quantity": buy_qty,
            "sell_quantity": sell_qty,
            "number_of_trades": len(self.buffer),
        }
        key = f"trades:{self.exchange}:{self.symbol}"
        await push_and_trim(self.redis, key, data, CFG.trim_size)
        self.buffer.clear()

class OIAggregator:
    def __init__(self, exchange: str, symbol: str, redis: aioredis.Redis):
        self.exchange = exchange
        self.symbol = symbol
        self.redis = redis
        self.cur_ts = None
        self.last_oi = None

    async def update(self, oi: float):
        ts = int(time.time())
        if ts != self.cur_ts:
            if self.cur_ts is not None and self.last_oi is not None:
                await self.flush()
            self.cur_ts = ts
        self.last_oi = oi

    async def flush(self):
        data = {
            "timestamp_unix": self.cur_ts,
            "exchange": self.exchange,
            "symbol": self.symbol,
            "open_interest": float(self.last_oi),
        }
        key = f"oi:{self.exchange}:{self.symbol}"
        await push_and_trim(self.redis, key, data, CFG.trim_size)

class BookAggregator:
    def __init__(self, exchange: str, symbol: str, redis: aioredis.Redis):
        self.exchange = exchange
        self.symbol = symbol
        self.redis = redis
        self.last_update = 0

    async def update(self, bids: List[List[str]], asks: List[List[str]]):
        ts = int(time.time())
        if ts != self.last_update:
            self.last_update = ts
            mid = (float(bids[0][0]) + float(asks[0][0])) / 2.0
            bids_b = bucketize(bids, CFG.bucket_size, True)
            asks_b = bucketize(asks, CFG.bucket_size, False)
            data = {
                "timestamp_unix": ts,
                "exchange": self.exchange,
                "symbol": self.symbol,
                "mid_price": mid,
                "bids_json": json.dumps(bids_b),
                "asks_json": json.dumps(asks_b),
            }
            key_hist = f"book:{self.exchange}:{self.symbol}"
            await push_and_trim(self.redis, key_hist, data, CFG.trim_size)
            key_latest = f"book:{self.exchange}:{self.symbol}:latest"
            await self.redis.set(key_latest, json.dumps(data))

# --- Exchange connections ---
async def run_binance(symbol: str, redis: aioredis.Redis):
    stream = f"{symbol.lower()}@aggTrade/{symbol.lower()}@openInterest@1s/{symbol.lower()}@depth@100ms"
    url = f"wss://fstream.binance.com/stream?streams={stream}"
    trade_aggr = TradeAggregator("binance", symbol, redis)
    oi_aggr = OIAggregator("binance", symbol, redis)
    book_aggr = BookAggregator("binance", symbol, redis)
    async for ws in websockets.connect(url, ping_interval=20):
        try:
            async for msg in ws:
                obj = json.loads(msg)
                data = obj.get("data", {})
                stream = obj.get("stream", "")
                if stream.endswith("@aggTrade"):
                    price = float(data["p"])
                    qty = float(data["q"])
                    side = "sell" if data["m"] else "buy"
                    await trade_aggr.add(price, qty, side)
                elif stream.endswith("@openInterest"):
                    await oi_aggr.update(float(data["openInterest"]))
                elif stream.endswith("@depth@100ms"):
                    bids = data.get("b", [])
                    asks = data.get("a", [])
                    if bids and asks:
                        await book_aggr.update(bids, asks)
        except Exception as e:
            logging.error("binance ws error %s", e)
            await asyncio.sleep(5)

async def run_bybit(symbol: str, redis: aioredis.Redis):
    url = "wss://stream.bybit.com/v5/public/linear"
    subs = [
        {"op": "subscribe", "args": [f"publicTrade.{symbol}", f"tickers.{symbol}", f"orderbook.50.{symbol}"]}
    ]
    trade_aggr = TradeAggregator("bybit", symbol, redis)
    oi_aggr = OIAggregator("bybit", symbol, redis)
    book_aggr = BookAggregator("bybit", symbol, redis)
    async for ws in websockets.connect(url, ping_interval=20):
        try:
            await ws.send(json.dumps(subs[0]))
            async for msg in ws:
                obj = json.loads(msg)
                if obj.get("topic", "").startswith("publicTrade"):
                    for t in obj.get("data", []):
                        price = float(t["p"])
                        qty = float(t["v"])
                        side = "buy" if t["S"] == "Buy" else "sell"
                        await trade_aggr.add(price, qty, side)
                elif obj.get("topic", "").startswith("tickers"):
                    data = obj.get("data", {})
                    if isinstance(data, list):
                        data = data[0]
                    if data and "openInterest" in data:
                        await oi_aggr.update(float(data["openInterest"]))
                elif obj.get("topic", "").startswith("orderbook"):
                    data = obj.get("data", {})
                    bids = data.get("b", [])
                    asks = data.get("a", [])
                    if bids and asks:
                        await book_aggr.update(bids, asks)
        except Exception as e:
            logging.error("bybit ws error %s", e)
            await asyncio.sleep(5)

# --- DB writer ---
CREATE_AGG_TBL = """CREATE TABLE IF NOT EXISTS aggregated_trade_history (
    timestamp_unix INTEGER, exchange TEXT, symbol TEXT,
    open_price REAL, high_price REAL, low_price REAL, close_price REAL,
    total_quantity REAL, buy_quantity REAL, sell_quantity REAL,
    number_of_trades INTEGER,
    PRIMARY KEY(timestamp_unix, exchange, symbol))"""

CREATE_OI_TBL = """CREATE TABLE IF NOT EXISTS open_interest_history (
    timestamp_unix INTEGER, exchange TEXT, symbol TEXT, open_interest REAL,
    PRIMARY KEY(timestamp_unix, exchange, symbol))"""

CREATE_BOOK_TBL = """CREATE TABLE IF NOT EXISTS order_book_history (
    timestamp_unix INTEGER, exchange TEXT, symbol TEXT, mid_price REAL,
    bids_json TEXT, asks_json TEXT,
    PRIMARY KEY(timestamp_unix, exchange, symbol))"""

async def db_writer(redis: aioredis.Redis):
    async with aiosqlite.connect(CFG.sqlite_path) as db:
        await db.execute(CREATE_AGG_TBL)
        await db.execute(CREATE_OI_TBL)
        await db.execute(CREATE_BOOK_TBL)
        await db.commit()
        while True:
            await asyncio.sleep(60)
            for key, table, cols in [
                ("trades", "aggregated_trade_history", 11),
                ("oi", "open_interest_history", 4),
                ("book", "order_book_history", 6),
            ]:
                keys = await redis.keys(f"{key}:*:*" )
                for k in keys:
                    data = await redis.lrange(k, 0, -1)
                    if data:
                        await redis.delete(k)
                        rows = [json.loads(x) for x in data]
                        placeholders = ",".join(["?"]*cols)
                        columns = rows[0].keys()
                        await db.executemany(
                            f"INSERT OR IGNORE INTO {table} ({','.join(columns)}) VALUES ({placeholders})",
                            [tuple(r[c] for c in columns) for r in rows],
                        )
            await db.commit()

async def main():
    redis = aioredis.from_url(CFG.redis_url)
    tasks = [
        run_binance("btcusdt", redis),
        run_binance("btcusd_perp", redis),
        run_bybit("BTCUSDT", redis),
        run_bybit("BTCUSD", redis),
        db_writer(redis),
    ]
    await asyncio.gather(*[asyncio.create_task(t) for t in tasks])

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
