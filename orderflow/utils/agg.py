"""Trade aggregation to 1-second buckets."""
from collections import defaultdict
from datetime import datetime, timezone
from settings import AGG_BUCKET_SEC

# bucket_key => [vol, px_qty_sum, trades, min_px, max_px]
trade_buckets = defaultdict(lambda: [0.0, 0.0, 0, float("inf"), 0.0])

def add_trade(exchange:str, symbol:str, side:str, price:float, qty:float, ts_ms:int):
    bucket_start = (ts_ms // 1000 // AGG_BUCKET_SEC) * AGG_BUCKET_SEC
    k = (exchange, symbol, side, bucket_start)
    bucket = trade_buckets[k]
    bucket[0] += qty
    bucket[1] += price * qty
    bucket[2] += 1
    bucket[3] = min(bucket[3], price)
    bucket[4] = max(bucket[4], price)

def flush_buckets():
    """Yield and then clear current buckets."""
    for (ex, sym, side, sec), (vol, pxqty, n, mn, mx) in list(trade_buckets.items()):
        if vol == 0:
            continue
        yield {
            "ts": datetime.fromtimestamp(sec, tz=timezone.utc),
            "exchange": ex,
            "symbol": sym,
            "side": side,
            "volume": vol,
            "vwap": pxqty / vol,
            "trades": n,
            "min_price": mn,
            "max_price": mx,
        }
        del trade_buckets[(ex, sym, side, sec)]
