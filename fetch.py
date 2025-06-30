import requests
import sqlite3
import time
import datetime as dt

API_URL = "https://api.exchange.coinbase.com"
PAIR = "BTC-USD"

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS ohlcv(
    ts INTEGER PRIMARY KEY,
    open REAL, high REAL, low REAL, close REAL, volume REAL
)"""

PURGE_DAYS = 31


def _init_db(db_path: str) -> sqlite3.Connection:
    con = sqlite3.connect(db_path)
    con.execute(CREATE_TABLE)
    con.commit()
    return con


def _fetch_chunk(pair: str, start: int, end: int) -> list[list]:
    params = {
        "granularity": 60,
        "start": dt.datetime.utcfromtimestamp(start).isoformat(),
        "end": dt.datetime.utcfromtimestamp(end).isoformat(),
    }
    url = f"{API_URL}/products/{pair}/candles"
    r = requests.get(url, params=params, timeout=10)
    r.raise_for_status()
    return r.json()


def sync_ohlcv(
    db_path: str, pair: str = PAIR, days: int = 2, history_days: int = PURGE_DAYS
) -> None:
    """最終 ts を DB から取得し Coinbase API から 1 分足を取得する"""
    con = _init_db(db_path)
    cur = con.execute("SELECT MAX(ts) FROM ohlcv")
    last = cur.fetchone()[0]
    if last is not None:
        start = last // 1000 + 60
    else:
        start = int(time.time()) - days * 86400
    end = int(time.time())
    while start < end:
        chunk_end = min(start + 300 * 60, end)
        data = _fetch_chunk(pair, start, chunk_end)
        for candle in data:
            ts = int(candle[0]) * 1000
            low, high, open_, close, vol = candle[1:6]
            con.execute(
                "INSERT OR IGNORE INTO ohlcv(ts, open, high, low, close, volume) "
                "VALUES(?,?,?,?,?,?)",
                (ts, open_, high, low, close, vol),
            )
        con.commit()
        start = chunk_end + 60
        time.sleep(0.34)  # rate limit ~3 req/sec
    # purge old records
    limit_ts = int(time.time() * 1000) - history_days * 86_400_000
    con.execute("DELETE FROM ohlcv WHERE ts<?", (limit_ts,))
    con.commit()
    con.close()


if __name__ == "__main__":
    sync_ohlcv("tpo_cva.db")
