"""Centralised constants & env vars."""
import os

# --- Redis ----------------------------------------------------
REDIS_URL            = os.getenv("REDIS_URL", "redis://redis:6379/0")
REDIS_STREAM_MAXLEN  = int(os.getenv("REDIS_STREAM_MAXLEN", 10_000))

# --- PostgreSQL ----------------------------------------------
PG_DSN = os.getenv("PG_DSN", "postgresql://postgres:postgres@postgres:5432/orderflow")

# --- Poller / Writer intervals -------------------------------
POLL_INTERVAL_SEC = 10        # OI REST polling interval
FLUSH_SEC         = 30        # DB flush cadence
AGG_BUCKET_SEC    = 1         # Trade aggregation bucket size

# --- Watchlist -----------------------------------------------
WATCH_TARGETS = [  # (exchange, market_type, symbol)
    ("binance", "Futures", "BTCUSDT"),
    ("binance", "Futures", "BTCUSD"),
    ("bybit",   "Futures", "BTCUSDT"),
    ("bybit",   "Futures", "BTCUSD"),
]
