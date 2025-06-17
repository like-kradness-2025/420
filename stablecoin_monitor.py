#!/usr/bin/env python3
"""Monitor stablecoin prices and 24h volume and notify Discord."""

from __future__ import annotations

import time
import sqlite3
from pathlib import Path
from typing import Dict, List

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
import requests
import yaml

CONFIG_FILE = Path("config.yaml")
STABLECOINS = ["USDT", "USDC", "FDUSD", "TUSD", "DAI"]


def load_config() -> Dict[str, str]:
    if not CONFIG_FILE.exists():
        raise FileNotFoundError(f"Missing config file: {CONFIG_FILE}")
    with CONFIG_FILE.open() as fh:
        cfg = yaml.safe_load(fh)
    return cfg


def init_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(path)
    con.execute(
        """CREATE TABLE IF NOT EXISTS history (
            ts INTEGER,
            coin TEXT,
            price REAL,
            volume REAL,
            PRIMARY KEY(ts, coin)
        )"""
    )
    con.commit()
    return con


def prune_db(con: sqlite3.Connection, max_age: int) -> None:
    threshold = int(time.time()) - max_age
    con.execute("DELETE FROM history WHERE ts < ?", (threshold,))
    con.commit()


def fetch_cmc(api_key: str, symbols: List[str]) -> Dict[str, Dict[str, float]]:
    url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest"
    headers = {"X-CMC_PRO_API_KEY": api_key}
    params = {"symbol": ",".join(symbols)}
    resp = requests.get(url, headers=headers, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()["data"]
    results = {}
    for sym in symbols:
        info = data[sym]
        quote = info["quote"]["USD"]
        results[sym] = {"price": quote["price"], "volume": quote["volume_24h"]}
    return results


def save_snapshot(con: sqlite3.Connection, snapshot: Dict[str, Dict[str, float]]) -> None:
    now = int(time.time())
    rows = [(now, c, d["price"], d["volume"]) for c, d in snapshot.items()]
    con.executemany(
        "INSERT OR REPLACE INTO history(ts, coin, price, volume) VALUES(?, ?, ?, ?)",
        rows,
    )
    con.commit()


def load_week(con: sqlite3.Connection) -> pd.DataFrame:
    one_week = int(time.time()) - 7 * 86400
    df = pd.read_sql("SELECT * FROM history WHERE ts >= ?", con, params=(one_week,))
    if not df.empty:
        df["timestamp"] = pd.to_datetime(df["ts"], unit="s", utc=True)
    return df


def make_charts(df: pd.DataFrame, out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8))
    for coin, grp in df.groupby("coin"):
        ax1.plot(grp["timestamp"], grp["price"], label=coin)
        ax2.plot(grp["timestamp"], grp["volume"], label=coin)
    ax1.set_title("Price (USD)")
    ax1.legend()
    ax2.set_title("24h Volume (USD)")
    ax2.legend()
    fig.autofmt_xdate()
    path = out_dir / "stablecoins.png"
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)
    return path


def discord_notify(webhook: str, image_path: Path, text: str) -> None:
    with image_path.open("rb") as fh:
        files = {"file": (image_path.name, fh, "image/png")}
        resp = requests.post(webhook, data={"content": text}, files=files, timeout=10)
    resp.raise_for_status()


def main() -> None:
    cfg = load_config()
    con = init_db(Path(cfg.get("db_path", "stablecoin_history.db")))
    api_key = cfg["coinmarketcap_key"]
    webhook = cfg.get("discord_webhook", "")
    interval = int(cfg.get("interval_sec", 300))

    while True:
        try:
            snap = fetch_cmc(api_key, STABLECOINS)
            save_snapshot(con, snap)
            prune_db(con, 7 * 86400)
            df = load_week(con)
            if not df.empty:
                chart = make_charts(df, Path(cfg.get("chart_dir", "charts")))
                latest_lines = [f"{c}: {d['price']:.4f} USD, vol {d['volume']:.0f}" for c, d in snap.items()]
                if webhook:
                    discord_notify(webhook, chart, "\n".join(latest_lines))
        except Exception as e:
            if webhook:
                try:
                    requests.post(webhook, json={"content": f"Error: {e}"}, timeout=10)
                except Exception:
                    pass
        time.sleep(interval)


if __name__ == "__main__":
    main()
