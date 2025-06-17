# Stablecoin Monitor

This project provides a Python script to track major stablecoins using the CoinMarketCap API. It stores recent data in SQLite, generates weekly charts, and optionally sends updates to Discord.

## Features

- Monitors USDT, USDC, FDUSD, TUSD and DAI
- Records price and 24h volume every five minutes
- Keeps one week of history in a SQLite database
- Generates charts using matplotlib
- Sends the latest chart and numbers to a Discord webhook

## Setup

1. Install Python 3.11 or newer.
2. Install required packages:
   ```sh
   pip install pandas matplotlib requests pyyaml
   ```
3. Copy `config.yaml` and edit your CoinMarketCap API key and Discord webhook URL.
4. Run the script:
   ```sh
   python stablecoin_monitor.py
   ```

The script will continue running, fetching new data every five minutes.

## Configuration

Configuration is stored in `config.yaml`:

- `coinmarketcap_key`: your API key from CoinMarketCap
- `discord_webhook`: webhook URL to post updates (leave blank to disable)
- `interval_sec`: fetch interval in seconds (default 300)
- `chart_dir`: directory to save chart images
- `db_path`: SQLite database file

## Notes

Network access may be required to contact the CoinMarketCap API and Discord. If running in a restricted environment, ensure those domains are allowed.
