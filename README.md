# BTC Volume Monitor

This utility fetches 1-minute BTC OHLCV data from multiple exchanges and posts a stacked volume chart with average price to Discord.

## Setup

```bash
pip install -r requirements.txt
cp config.yml config.yml
# edit config.yml with your Discord webhook URL
python btc_volume_monitor.py
```

Set `RUN_TESTS=1` to run built-in tests.
