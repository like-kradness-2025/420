import os
import configparser
from pathlib import Path

CONFIG_FILE = Path("config.ini")
DB_FILE = Path(os.getenv("OHLCV_DB_PATH", Path.home()/".cache/bina_VL/ohlcv.db"))
CHART_DIR = Path(os.getenv("CHART_SAVE_PATH", Path.home()/"bina_VL_charts"))

DEFAULTS = {
    "API_KEY": "YOUR_BINANCE_API_KEY_PLACEHOLDER",
    "API_SECRET": "YOUR_BINANCE_API_SECRET_PLACEHOLDER",
    "DISCORD_WEBHOOK_URL": "",
    "TIMEFRAME": "5m",
    "CHART_PERIOD_DAYS": "1",
    "UPDATE_INTERVAL_SECONDS": "300",
    "FETCH_LIMIT": "1000",
}


def load_config() -> configparser.ConfigParser:
    """Ensure config file exists and return a loaded ConfigParser."""
    if not CONFIG_FILE.exists():
        CONFIG_FILE.write_text("\n".join(f"{k}={v}" for k, v in DEFAULTS.items()))
    cfg = configparser.ConfigParser()
    cfg.read(CONFIG_FILE)
    for k, v in DEFAULTS.items():
        cfg["DEFAULT"].setdefault(k, v)
    return cfg
