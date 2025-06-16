import sqlite3
from pathlib import Path
import pandas as pd


def ensure_db(path: Path) -> sqlite3.Connection:
    """Create database and table if needed, returning connection."""
    path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(path))
    con.execute(
        """CREATE TABLE IF NOT EXISTS ohlcv (
            exchange TEXT, symbol TEXT, timestamp INTEGER,
            open REAL, high REAL, low REAL, close REAL, volume REAL,
            PRIMARY KEY(exchange, symbol, timestamp)
        )"""
    )
    con.commit()
    return con


def save_ohlcv(con: sqlite3.Connection, ex: str, sym: str, df: pd.DataFrame) -> None:
    if df.empty:
        return
    df = df.copy()
    df["exchange"] = ex
    df["symbol"] = sym
    df = df[
        ["exchange", "symbol", "timestamp", "open", "high", "low", "close", "volume"]
    ]
    df.to_sql("ohlcv", con, if_exists="append", index=False, method="multi")


def load_ohlcv(
    con: sqlite3.Connection, ex: str, sym: str, since: int, until: int
) -> pd.DataFrame:
    sql = (
        "SELECT timestamp,open,high,low,close,volume FROM ohlcv "
        "WHERE exchange=? AND symbol=? AND timestamp BETWEEN ? AND ? ORDER BY timestamp"
    )
    df = pd.read_sql(sql, con, params=(ex, sym, since, until))
    if not df.empty:
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    return df
