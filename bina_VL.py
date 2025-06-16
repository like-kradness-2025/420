#!/usr/bin/env python3
"""Fetch OHLCV data from multiple exchanges and create volume charts."""
import logging
import os
import time
import datetime as dt
from typing import Dict, List


from config import load_config, DB_FILE, CHART_DIR
from database import ensure_db, load_ohlcv
from exchange_utils import setup_exchange, fetch_and_cache_all, parse_tf
from chart import generate_chart
from notify import discord_notify

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)sZ - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    force=True,
)
logging.Formatter.converter = time.gmtime


def ensure_runtime_config():
    cfg = load_config()
    timeframe = cfg.get("DEFAULT", "TIMEFRAME")
    tf_ms = parse_tf(timeframe)
    days = cfg.getint("DEFAULT", "CHART_PERIOD_DAYS")
    update = cfg.getint("DEFAULT", "UPDATE_INTERVAL_SECONDS")
    limit = cfg.getint("DEFAULT", "FETCH_LIMIT")
    api_key = os.getenv("BINANCE_API_KEY", cfg.get("DEFAULT", "API_KEY"))
    api_secret = os.getenv("BINANCE_API_SECRET", cfg.get("DEFAULT", "API_SECRET"))
    webhook = os.getenv("DISCORD_WEBHOOK_URL", cfg.get("DEFAULT", "DISCORD_WEBHOOK_URL"))
    return timeframe, tf_ms, days, update, limit, api_key, api_secret, webhook


def main() -> None:
    (
        timeframe,
        tf_ms,
        days,
        update,
        limit,
        api_key,
        api_secret,
        webhook,
    ) = ensure_runtime_config()

    con = ensure_db(DB_FILE)

    # exchanges
    binance = setup_exchange("binance", api_key, api_secret)
    coinbase = setup_exchange("coinbasepro") if hasattr(
        __import__("ccxt"), "coinbasepro"
    ) else setup_exchange("coinbase")
    bitstamp = setup_exchange("bitstamp")
    kraken = setup_exchange("kraken")

    pair_map: Dict[str, List[str]] = {
        "binance": ["BTC/USDT", "BTC/FDUSD", "BTC/USDC", "BTC/EUR"],
        "coinbasepro": ["BTC/USD"],
        "coinbase": ["BTC/USD"],
        "bitstamp": ["BTC/USD", "BTC/EUR", "BTC/GBP", "BTC/USDT"],
        "kraken": ["BTC/USD", "BTC/EUR", "BTC/GBP", "BTC/USDT"],
    }
    ex_obj = {
        "binance": binance,
        "coinbasepro": coinbase,
        "coinbase": coinbase,
        "bitstamp": bitstamp,
        "kraken": kraken,
    }

    def repr_sym(lst: List[str]) -> str:
        for p in ("BTC/USDT", "BTC/USD", lst[0]):
            if p in lst:
                return p
        return lst[0]

    while True:
        t0 = time.time()
        now = int(time.time() * 1000)
        since = now - days * 86_400_000
        for ex_id, syms in pair_map.items():
            ex = ex_obj[ex_id]
            if ex is None:
                continue
            fetch_and_cache_all(con, ex, syms, since, now, timeframe, limit)
            price_sym = repr_sym(syms)
            price_df = load_ohlcv(con, ex_id, price_sym, since, now)
            vol_map = {
                s: load_ohlcv(con, ex_id, s, since, now)[["timestamp", "volume"]]
                for s in syms
            }
            img = CHART_DIR / f"{ex_id}_btc_volume.png"
            generate_chart(ex_id, price_df, price_sym, vol_map, tf_ms, img)
            if webhook:
                discord_notify(
                    webhook,
                    img,
                    f"{ex_id.capitalize()} BTC update {dt.datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}",
                )
        sl = max(update - (time.time() - t0), 0)
        logging.info("sleep %.1fs", sl)
        time.sleep(sl)


if __name__ == "__main__":
    main()
