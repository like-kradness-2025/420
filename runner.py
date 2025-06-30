import argparse
import yaml
from fetch import sync_ohlcv
from profile import build_daily_va
from cva import update_cva
from plot import render_png
from discord import post_image

CONFIG_FILE = "config.yaml"


def load_config(path: str) -> dict:
    with open(path) as fh:
        return yaml.safe_load(fh)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=7, help="days to plot")
    args = parser.parse_args()
    cfg = load_config(CONFIG_FILE)
    db_path = cfg.get("tpo_db_path", "tpo_cva.db")
    sync_ohlcv(db_path, cfg.get("pair", "BTC-USD"))
    build_daily_va(db_path, cfg.get("bin_size", 20))
    update_cva(db_path, cfg.get("overlap_threshold", 0.5), cfg.get("max_cva_days", 7))
    img = render_png(db_path, args.days, cfg.get("bin_size", 20))
    post_image(img, cfg.get("tpo_webhook_url", ""))


if __name__ == "__main__":
    main()
