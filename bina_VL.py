#!/usr/bin/env python3
"""
取引所別チャート（価格＋出来高スタック）。
出来高カラーは **オリジナル版のシンボル配色** に合わせました。
"""
import logging, os, time, datetime as dt
from pathlib import Path
import sqlite3, configparser, requests
from typing import Dict, List
import ccxt  # type: ignore
import pandas as pd, numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt, matplotlib.dates as mdates

###############################################################################
# CONFIG
###############################################################################
CONFIG_FILE = Path("config.ini")
DB_FILE     = Path(os.getenv("OHLCV_DB_PATH", Path.home()/".cache/bina_VL/ohlcv.db"))
CHART_DIR   = Path(os.getenv("CHART_SAVE_PATH",  Path.home()/"bina_VL_charts"))
DEFAULTS    = {
    "API_KEY": "YOUR_BINANCE_API_KEY_PLACEHOLDER",
    "API_SECRET": "YOUR_BINANCE_API_SECRET_PLACEHOLDER",
    "DISCORD_WEBHOOK_URL": "",
    "TIMEFRAME": "5m",
    "CHART_PERIOD_DAYS": "1",
    "UPDATE_INTERVAL_SECONDS": "300",
    "FETCH_LIMIT": "1000",
}
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)sZ - %(levelname)s - %(message)s", datefmt="%Y-%m-%dT%H:%M:%S", force=True)
logging.Formatter.converter = time.gmtime

###############################################################################
# DB helpers
###############################################################################

def ensure_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(path))
    con.execute("""CREATE TABLE IF NOT EXISTS ohlcv (
        exchange TEXT, symbol TEXT, timestamp INTEGER,
        open REAL, high REAL, low REAL, close REAL, volume REAL,
        PRIMARY KEY(exchange,symbol,timestamp))""")
    con.commit(); return con

def save_ohlcv(con: sqlite3.Connection, ex: str, sym: str, df: pd.DataFrame):
    if df.empty: return
    df=df.copy(); df["exchange"]=ex; df["symbol"]=sym
    df=df[["exchange","symbol","timestamp","open","high","low","close","volume"]]
    df.to_sql("ohlcv", con, if_exists="append", index=False, method="multi")

def load_ohlcv(con: sqlite3.Connection, ex: str, sym: str, since: int, until: int) -> pd.DataFrame:
    sql="SELECT timestamp,open,high,low,close,volume FROM ohlcv WHERE exchange=? AND symbol=? AND timestamp BETWEEN ? AND ? ORDER BY timestamp"
    df=pd.read_sql(sql, con, params=(ex,sym,since,until))
    if not df.empty:
        df["timestamp"]=pd.to_datetime(df["timestamp"],unit="ms",utc=True)
    return df

###############################################################################
# EXCHANGE util
###############################################################################

def setup_exchange(name:str, api_key:str="", api_secret:str="")->ccxt.Exchange:
    cls=getattr(ccxt,name); ex=cls({"enableRateLimit":True})
    if name=="binance" and api_key and "PLACEHOLDER" not in api_key:
        ex.apiKey=api_key; ex.secret=api_secret
    ex.load_markets(); return ex

def parse_tf(tf:str)->int:
    units={"m":60_000,"h":3_600_000,"d":86_400_000}
    try: val, suf=int(tf[:-1]), tf[-1]; return val*units[suf]
    except Exception: return 300_000

###############################################################################
# FETCH with cache
###############################################################################

def fetch_and_cache_ohlcv(con, ex, sym, since_ms, until_ms, tf, limit):
    tf_ms=parse_tf(tf)
    local=load_ohlcv(con, ex.id, sym, since_ms, until_ms)
    want=pd.date_range(pd.to_datetime(since_ms,unit="ms",utc=True), pd.to_datetime(until_ms,unit="ms",utc=True), freq=pd.Timedelta(milliseconds=tf_ms))
    have=set(local["timestamp"]) if not local.empty else set()
    miss=[t for t in want if t not in have]
    if miss:
        logging.info("[%s] %s missing %d – fetching", ex.id, sym, len(miss))
        blocks, cur=[],[]
        for t in miss:
            if not cur or (t-cur[-1]).total_seconds()*1000==tf_ms: cur.append(t)
            else: blocks.append(cur); cur=[t]
        if cur: blocks.append(cur)
        for blk in blocks:
            cursor=int(blk[0].value//1e6); until_blk=int(blk[-1].value//1e6)
            while cursor<=until_blk:
                try:
                    raw=ex.fetch_ohlcv(sym,timeframe=tf,since=cursor,limit=limit)
                    if not raw: break
                    df=pd.DataFrame(raw,columns=["timestamp","open","high","low","close","volume"])
                    save_ohlcv(con,ex.id,sym,df)
                    cursor=int(df["timestamp"].max())+tf_ms
                    time.sleep(max(ex.rateLimit/1000,0.5))
                except Exception as e:
                    logging.error("fetch error %s",e); break
    return load_ohlcv(con, ex.id, sym, since_ms, until_ms)

def fetch_and_cache_all(con, ex, syms, since_ms, until_ms, tf, limit):
    for s in syms: fetch_and_cache_ohlcv(con, ex, s, since_ms, until_ms, tf, limit)

###############################################################################
# COLOR mapping (original)
###############################################################################
SYMBOL_COLOR = {
    # Binance
    "BTC/USDT": "#368DFF",  # blue
    "BTC/FDUSD": "#39B54A",  # green
    "BTC/USDC": "#FFA500",  # orange
    "BTC/EUR":  "#FFC847",  # yellow
    # Coinbase & Bitstamp / Kraken shared colors
    "BTC/USD":  "#0077B6",  # deep blue
    "BTC/EUR_bitstamp": "#F7B801",
    "BTC/GBP_bitstamp": "#D7263D",
    "BTC/USDT_bitstamp": "#00BFA5",
    "BTC/EUR_kraken": "#F7B801",
    "BTC/GBP_kraken": "#D7263D",
    "BTC/USDT_kraken": "#00BFA5",
}
DEFAULT_VOL_COLOR="#888888"

###############################################################################
# CHARTING
###############################################################################
plt.style.use("dark_background")

def get_color(ex_id:str, sym:str)->str:
    key=f"{sym}_{ex_id}" if f"{sym}_{ex_id}" in SYMBOL_COLOR else sym
    return SYMBOL_COLOR.get(key, DEFAULT_VOL_COLOR)

def generate_chart(ex_id:str, price_df:pd.DataFrame, price_sym:str, vol_map:Dict[str,pd.DataFrame], tf_ms:int, out:Path):
    if price_df.empty:
        logging.warning("%s no price", ex_id); return
    bar_w=(tf_ms/86_400_000)*0.9
    fig,(axp,axv)=plt.subplots(2,1,figsize=(14,6),sharex=True,gridspec_kw={"height_ratios":[2,1],"hspace":0.15})
    fig.patch.set_facecolor("#000000"); fig.suptitle(f"{ex_id.capitalize()} – {price_sym} Price & Volume",color="w",fontsize=18)
    axp.plot(price_df["timestamp"],price_df["close"],lw=1.5,color="#47aaff",label=price_sym)
    axp.grid(ls=":",alpha=0.3); axp.tick_params(axis="y",colors="#ccc"); axp.yaxis.tick_right(); axp.set_ylabel("Price",color="#ccc"); axp.legend(loc="upper left")
    bottom=np.zeros(len(price_df))
    for sym,vdf in vol_map.items():
        ser=vdf.set_index("timestamp")["volume"].reindex(price_df["timestamp"]).fillna(0)
        axv.bar(price_df["timestamp"],ser,bottom=bottom,width=bar_w,color=get_color(ex_id,sym),label=sym)
        bottom+=ser.values
    axv.set_ylabel("Volume",color="#ccc"); axv.grid(ls=":",alpha=0.3); axv.tick_params(axis="y",colors="#ccc"); axv.yaxis.tick_right(); axv.legend(loc="upper left",fontsize=9,ncol=2)
    loc=mdates.AutoDateLocator(minticks=5,maxticks=10); axv.xaxis.set_major_locator(loc); axv.xaxis.set_major_formatter(mdates.ConciseDateFormatter(loc)); fig.autofmt_xdate()
    fig.tight_layout(rect=[0,0,1,0.95]); out.parent.mkdir(parents=True,exist_ok=True); fig.savefig(out,dpi=150,bbox_inches="tight",facecolor=fig.get_facecolor()); plt.close(fig); logging.info("saved %s",out)

###############################################################################
# DISCORD
###############################################################################

def discord_notify(webhook:str, path:Path, msg:str=""):
    if not webhook or not path.exists(): return
    try:
        with path.open("rb") as fh:
            r=requests.post(webhook,data={"content":msg},files={"file":(path.name,fh,"image/png")}); r.raise_for_status(); logging.info("discord ok")
    except Exception as e: logging.error("discord fail %s",e)

###############################################################################
# MAIN
###############################################################################

def ensure_config():
    if not CONFIG_FILE.exists():
        cp=configparser.ConfigParser(); cp["DEFAULT"]=DEFAULTS; CONFIG_FILE.write_text("\n".join(f"{k}={v}" for k,v in DEFAULTS.items()))
    cfg=configparser.ConfigParser(); cfg.read(CONFIG_FILE); [cfg["DEFAULT"].setdefault(k,v) for k,v in DEFAULTS.items()]; return cfg
cfg=ensure_config(); TIMEFRAME=cfg.get("DEFAULT","TIMEFRAME"); TF_MS=parse_tf(TIMEFRAME); DAYS=cfg.getint("DEFAULT","CHART_PERIOD_DAYS"); UPDATE=cfg.getint("DEFAULT","UPDATE_INTERVAL_SECONDS"); LIMIT=cfg.getint("DEFAULT","FETCH_LIMIT")
API_KEY=os.getenv("BINANCE_API_KEY",cfg.get("DEFAULT","API_KEY")); API_SECRET=os.getenv("BINANCE_API_SECRET",cfg.get("DEFAULT","API_SECRET")); WEBHOOK=os.getenv("DISCORD_WEBHOOK_URL",cfg.get("DEFAULT","DISCORD_WEBHOOK_URL"))
con=ensure_db(DB_FILE)
# exchanges
binance=setup_exchange("binance",API_KEY,API_SECRET); coinbase=setup_exchange("coinbasepro") if hasattr(ccxt,"coinbasepro") else setup_exchange("coinbase"); bitstamp=setup_exchange("bitstamp"); kraken=setup_exchange("kraken")
PAIR_MAP={
    "binance":["BTC/USDT","BTC/FDUSD","BTC/USDC","BTC/EUR"],
    "coinbasepro":["BTC/USD"],
    "coinbase":["BTC/USD"],
    "bitstamp":["BTC/USD","BTC/EUR","BTC/GBP","BTC/USDT"],
    "kraken":["BTC/USD","BTC/EUR","BTC/GBP","BTC/USDT"],
}
EX_OBJ={"binance":binance,"coinbasepro":coinbase,"coinbase":coinbase,"bitstamp":bitstamp,"kraken":kraken}

def repr_sym(lst):
    for p in ("BTC/USDT","BTC/USD",lst[0]):
        if p in lst: return p
    return lst[0]

while True:
    t0=time.time(); now=int(time.time()*1000); since=now-DAYS*86_400_000
    for ex_id,syms in PAIR_MAP.items():
        ex=EX_OBJ[ex_id];
        if ex is None: continue
        fetch_and_cache_all(con,ex,syms,since,now,TIMEFRAME,LIMIT)
        price_sym=repr_sym(syms); price_df=load_ohlcv(con,ex_id,price_sym,since,now)
        vol_map={s:load_ohlcv(con,ex_id,s,since,now)[["timestamp","volume"]] for s in syms}
        img=CHART_DIR/f"{ex_id}_btc_volume.png"; generate_chart(ex_id,price_df,price_sym,vol_map,TF_MS,img)
        if WEBHOOK: discord_notify(WEBHOOK,img,f"{ex_id.capitalize()} BTC update {dt.datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    sl=max(UPDATE-(time.time()-t0),0); logging.info("sleep %.1fs",sl); time.sleep(sl)

