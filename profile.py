import sqlite3
import math
import pandas as pd
from collections import defaultdict

BIN_SIZE_USD = 20
TPO_SIZE_MIN = 30

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS daily_va(
    session_date TEXT PRIMARY KEY,
    val REAL, vah REAL, poc REAL,
    bin_size REAL DEFAULT 20.0
)"""


def _init_db(path: str) -> sqlite3.Connection:
    con = sqlite3.connect(path)
    con.execute(CREATE_TABLE)
    con.commit()
    return con


def _load_day(con: sqlite3.Connection, date: str) -> pd.DataFrame:
    start_ts = int(pd.Timestamp(date).timestamp()) * 1000
    end_ts = start_ts + 86_400_000
    return pd.read_sql(
        "SELECT * FROM ohlcv WHERE ts>=? AND ts<?",
        con,
        params=(start_ts, end_ts),
    )


def _calc_va(df: pd.DataFrame, bin_size: int) -> tuple[float, float, float]:
    if df.empty:
        raise ValueError("empty dataframe")
    df["bin"] = (df["close"] // bin_size).astype(int)
    df["interval"] = (df["ts"] // (TPO_SIZE_MIN * 60_000)).astype(int)
    counts = defaultdict(int)
    for _, row in df.iterrows():
        counts[(row["bin"], row["interval"])] = 1
    tally = defaultdict(int)
    for b, _ in counts:
        tally[b] += 1
    total = sum(tally.values())
    sorted_bins = sorted(tally.items(), key=lambda x: x[1], reverse=True)
    acc = 0
    va_bins = []
    for b, c in sorted_bins:
        acc += c
        va_bins.append(b)
        if acc / total >= 0.7:
            break
    poc = sorted_bins[0][0] * bin_size
    val = min(va_bins) * bin_size
    vah = (max(va_bins) + 1) * bin_size
    return val, vah, poc


def build_daily_va(db_path: str, bin_size: int = BIN_SIZE_USD) -> None:
    con = _init_db(db_path)
    dates = [r[0] for r in con.execute("SELECT DISTINCT date(ts/1000,'unixepoch') FROM ohlcv").fetchall()]
    done = {r[0] for r in con.execute("SELECT session_date FROM daily_va").fetchall()}
    for d in dates:
        if d in done:
            continue
        df = _load_day(con, d)
        if df.empty:
            continue
        val, vah, poc = _calc_va(df, bin_size)
        con.execute(
            "INSERT OR REPLACE INTO daily_va(session_date,val,vah,poc,bin_size) VALUES(?,?,?,?,?)",
            (d, val, vah, poc, bin_size),
        )
        con.commit()
    con.close()


if __name__ == "__main__":
    build_daily_va("tpo_cva.db")
