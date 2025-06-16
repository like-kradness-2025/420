import logging
import time
from typing import Dict, List
import ccxt  # type: ignore
import pandas as pd

from database import save_ohlcv, load_ohlcv


def setup_exchange(name: str, api_key: str = "", api_secret: str = "") -> ccxt.Exchange:
    cls = getattr(ccxt, name)
    ex = cls({"enableRateLimit": True})
    if name == "binance" and api_key and "PLACEHOLDER" not in api_key:
        ex.apiKey = api_key
        ex.secret = api_secret
    ex.load_markets()
    return ex


def parse_tf(tf: str) -> int:
    units = {"m": 60_000, "h": 3_600_000, "d": 86_400_000}
    try:
        val, suf = int(tf[:-1]), tf[-1]
        return val * units[suf]
    except Exception:
        return 300_000


def fetch_and_cache_ohlcv(con, ex, sym, since_ms, until_ms, tf, limit):
    tf_ms = parse_tf(tf)
    local = load_ohlcv(con, ex.id, sym, since_ms, until_ms)
    want = pd.date_range(
        pd.to_datetime(since_ms, unit="ms", utc=True),
        pd.to_datetime(until_ms, unit="ms", utc=True),
        freq=pd.Timedelta(milliseconds=tf_ms),
    )
    have = set(local["timestamp"]) if not local.empty else set()
    miss = [t for t in want if t not in have]
    if miss:
        logging.info("[%s] %s missing %d – fetching", ex.id, sym, len(miss))
        blocks, cur = [], []
        for t in miss:
            if not cur or (t - cur[-1]).total_seconds() * 1000 == tf_ms:
                cur.append(t)
            else:
                blocks.append(cur)
                cur = [t]
        if cur:
            blocks.append(cur)
        for blk in blocks:
            cursor = int(blk[0].value // 1e6)
            until_blk = int(blk[-1].value // 1e6)
            while cursor <= until_blk:
                try:
                    raw = ex.fetch_ohlcv(sym, timeframe=tf, since=cursor, limit=limit)
                    if not raw:
                        break
                    df = pd.DataFrame(
                        raw, columns=["timestamp", "open", "high", "low", "close", "volume"]
                    )
                    save_ohlcv(con, ex.id, sym, df)
                    cursor = int(df["timestamp"].max()) + tf_ms
                    time.sleep(max(ex.rateLimit / 1000, 0.5))
                except Exception as e:
                    logging.error("fetch error %s", e)
                    break
    return load_ohlcv(con, ex.id, sym, since_ms, until_ms)


def fetch_and_cache_all(con, ex, syms: List[str], since_ms, until_ms, tf, limit):
    for s in syms:
        fetch_and_cache_ohlcv(con, ex, s, since_ms, until_ms, tf, limit)
