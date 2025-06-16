#!/usr/bin/env python3
"""BTC Volume Monitor: fetch OHLCV data from multiple exchanges and plot volume stack plus average price.
"""
import sys
import os
import subprocess
import sqlite3
import time
import datetime as dt
from typing import List, Dict, Any

# check dependencies
REQUIRED_PKGS = {
    'ccxt': 'ccxt',
    'pandas': 'pandas',
    'matplotlib': 'matplotlib',
    'requests': 'requests',
    'yaml': 'PyYAML',
}

def _ensure_packages() -> None:
    for mod, pkg in REQUIRED_PKGS.items():
        try:
            __import__(mod)
        except Exception:
            try:
                subprocess.check_call([sys.executable, '-m', 'pip', 'install', pkg])
            except Exception as e:
                print(f'Warning: failed to install {pkg}: {e}', file=sys.stderr)

_ensure_packages()

import ccxt  # type: ignore
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import requests
import yaml

DEFAULTS: Dict[str, Any] = {
    'discord_webhook_url': '',
    'db_path': 'btc_volume.db',
    'chart_lookback_minutes': 1440,
    'timeframe': '1m',
    'exchanges': {
        'binance': {
            'id': 'binance',
            'pairs': ['BTC/FDUSD', 'BTC/USDT', 'BTC/USDC'],
        },
        'coinbase': {
            'id': 'coinbase',
            'pairs': ['BTC/USD', 'BTC/USDC', 'BTC/USDT'],
        },
        'kraken': {
            'id': 'kraken',
            'pairs': ['XBT/USD', 'XBT/EUR', 'XBT/USDT'],
        },
        'bitstamp': {
            'id': 'bitstamp',
            'pairs': ['BTC/USD', 'BTC/EUR', 'BTC/USDT'],
        },
    },
    'chart_path': 'chart.png',
}

CONFIG_FILE = 'config.yml'

def load_config() -> Dict[str, Any]:
    data: Dict[str, Any] = {}
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, 'r') as fh:
            data = yaml.safe_load(fh) or {}
    # environment override
    env_map = {
        'discord_webhook_url': os.getenv('DISCORD_WEBHOOK_URL'),
        'db_path': os.getenv('DB_PATH'),
        'chart_lookback_minutes': os.getenv('CHART_LOOKBACK_MINUTES'),
        'timeframe': os.getenv('TIMEFRAME'),
        'chart_path': os.getenv('CHART_PATH'),
    }
    for k, v in env_map.items():
        if v is not None:
            data[k] = v
    # merge defaults
    cfg = DEFAULTS.copy()
    cfg.update(data)
    if not cfg.get('discord_webhook_url'):
        print('discord_webhook_url not set', file=sys.stderr)
        sys.exit(1)
    cfg['chart_lookback_minutes'] = int(cfg.get('chart_lookback_minutes'))
    return cfg

def init_db(path: str) -> sqlite3.Connection:
    con = sqlite3.connect(path)
    con.execute('PRAGMA journal_mode=WAL;')
    con.execute(
        'CREATE TABLE IF NOT EXISTS ohlcv ('
        'exch TEXT, ts INTEGER, o REAL, h REAL, l REAL, c REAL, v REAL, '
        'PRIMARY KEY(exch, ts))')
    return con

def timeframe_ms(tf: str) -> int:
    unit = {'m': 60000, 'h': 3600000, 'd': 86400000}
    try:
        val, suf = int(tf[:-1]), tf[-1]
        return val * unit[suf]
    except Exception:
        return 60000

def fetch_pair(ex, pair: str, tf: str, since_ms: int, until_ms: int) -> List[List[Any]]:
    data: List[List[Any]] = []
    cursor = since_ms
    tfms = timeframe_ms(tf)
    while cursor <= until_ms:
        try:
            chunk = ex.fetch_ohlcv(pair, timeframe=tf, since=cursor, limit=1000)
        except Exception as e:
            time.sleep(1)
            continue
        if not chunk:
            break
        data.extend([x for x in chunk if x[0] <= until_ms])
        cursor = chunk[-1][0] + tfms
        if len(chunk) < 1000:
            break
    return data

def fetch_exchange(ex_id: str, info: Dict[str, Any], since: int, until: int) -> pd.DataFrame:
    ex_class = getattr(ccxt, info['id'])
    ex = ex_class({'enableRateLimit': True})
    pairs = info['pairs']
    agg: Dict[int, Dict[str, Any]] = {}
    for p in pairs:
        rows = fetch_pair(ex, p, cfg['timeframe'], since, until)
        for ts, o, h, l, c, v in rows:
            a = agg.setdefault(ts, {'o':0,'h':0,'l':0,'c':0,'v':0,'cnt':0})
            a['o'] += o
            a['h'] += h
            a['l'] += l
            a['c'] += c
            a['v'] += v
            a['cnt'] += 1
    records = [
        (ts, val['o']/val['cnt'], val['h']/val['cnt'], val['l']/val['cnt'], val['c']/val['cnt'], val['v'])
        for ts, val in sorted(agg.items())
    ]
    return pd.DataFrame(records, columns=['ts','o','h','l','c','v'])

def upsert_df(con: sqlite3.Connection, exch: str, df: pd.DataFrame) -> None:
    if df.empty:
        return
    df = df.copy()
    df.insert(0, 'exch', exch)
    cur = con.cursor()
    for _, row in df.iterrows():
        cur.execute(
            'INSERT OR REPLACE INTO ohlcv (exch, ts, o, h, l, c, v) VALUES (?, ?, ?, ?, ?, ?, ?)',
            (row['exch'], int(row['ts']), row['o'], row['h'], row['l'], row['c'], row['v'])
        )
    con.commit()

def load_range(con: sqlite3.Connection, start_ms: int, end_ms: int) -> pd.DataFrame:
    df = pd.read_sql(
        'SELECT * FROM ohlcv WHERE ts BETWEEN ? AND ? ORDER BY ts',
        con, params=(start_ms, end_ms)
    )
    return df

def make_chart(df: pd.DataFrame, out_path: str) -> None:
    if df.empty:
        print('no data for chart', file=sys.stderr)
        return
    df['dt'] = pd.to_datetime(df['ts'], unit='ms', utc=True)
    vol = df.pivot_table(index='dt', columns='exch', values='v', fill_value=0)
    price = df.groupby('dt')['c'].mean()
    bar_w = timeframe_ms(cfg['timeframe']) / 86400000 * 0.8
    fig, (ax1, ax2) = plt.subplots(2,1, sharex=True, figsize=(10,6), gridspec_kw={'height_ratios':[1,2]})
    ax2.plot(price.index, price.values, color='blue', label='Avg Close')
    ax2.set_ylabel('Price')
    ax2.legend(loc='upper left')
    bottom = [0]*len(vol)
    for col in vol.columns:
        ax1.bar(vol.index, vol[col], bottom=bottom, label=col, width=bar_w)
        bottom = [b+v for b,v in zip(bottom, vol[col])]
    ax1.set_ylabel('Volume')
    ax1.legend(loc='upper left')
    fig.suptitle('BTC Volume & Avg Price — ' + price.index[-1].strftime('%Y-%m-%d %H:%M UTC'))
    fig.tight_layout()
    fig.savefig(out_path, dpi=120)
    plt.close(fig)

def send_discord(webhook: str, path: str) -> None:
    with open(path, 'rb') as fh:
        r = requests.post(webhook, files={'file': ('chart.png', fh, 'image/png')})
    if r.status_code >= 300:
        raise RuntimeError(f'Discord error {r.status_code}: {r.text}')

def get_latest_ts(con: sqlite3.Connection, exch: str) -> int:
    cur = con.execute('SELECT MAX(ts) FROM ohlcv WHERE exch=?', (exch,))
    row = cur.fetchone()
    return row[0] if row and row[0] else 0

if os.getenv('RUN_TESTS') == '1':
    import unittest
    class Tests(unittest.TestCase):
        def test_timeframe_ms(self):
            self.assertEqual(timeframe_ms('1m'), 60000)
            self.assertEqual(timeframe_ms('5m'), 300000)
    unittest.main(exit=False)
    sys.exit(0)

cfg = load_config()
con = init_db(cfg['db_path'])

now_ms = int(time.time()*1000)
lookback_ms = cfg['chart_lookback_minutes']*60*1000

for ex_name, info in cfg['exchanges'].items():
    latest = get_latest_ts(con, ex_name)
    since = latest + timeframe_ms(cfg['timeframe']) if latest else now_ms - lookback_ms
    df = fetch_exchange(ex_name, info, since, now_ms)
    upsert_df(con, ex_name, df)

start = now_ms - lookback_ms
df_all = load_range(con, start, now_ms)
make_chart(df_all, cfg['chart_path'])

send_discord(cfg['discord_webhook_url'], cfg['chart_path'])
