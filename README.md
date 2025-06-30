# Crypto Monitoring Tools

This repository contains two small Python projects:

1. **Stablecoin Monitor** – tracks major stablecoins via the CoinMarketCap API.
2. **BTC-USD TPO/CVA Bot** – builds Market Profile statistics from Coinbase 1m data and posts charts to Discord.

## Stablecoin Monitor

The original script `stablecoin_monitor.py` records USDT, USDC, FDUSD, TUSD and DAI prices and 24h volume every five minutes. Data is stored in SQLite and a weekly chart is produced. If `discord_webhook` is configured, the latest snapshot is posted automatically.

### Usage

```bash
pip install pandas matplotlib requests pyyaml
python stablecoin_monitor.py
```

Configuration keys for this script live in `config.yaml` (see comments inside).

## BTC-USD TPO/CVA Bot

This bot fetches Coinbase BTC-USD candles, calculates daily value areas and composite value areas, then plots the last few days and posts a PNG to Discord.

Run manually with:

```bash
python runner.py --days 7
```

Configuration options in `config.yaml` include:

- `pair` – trading pair (default `BTC-USD`)
- `bin_size` – price bin in USD for profiles
- `max_cva_days` – maximum days merged into one composite profile
- `overlap_threshold` – required VA overlap to extend a composite
- `tpo_db_path` – SQLite file used by the bot
- `tpo_webhook_url` – Discord Webhook to receive charts

The bot keeps roughly one month of OHLCV history.

## License

MIT License. See `LICENSE` for details.
