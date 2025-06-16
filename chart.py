import logging
from pathlib import Path
from typing import Dict

import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

SYMBOL_COLOR = {
    "BTC/USDT": "#368DFF",
    "BTC/FDUSD": "#39B54A",
    "BTC/USDC": "#FFA500",
    "BTC/EUR": "#FFC847",
    "BTC/USD": "#0077B6",
    "BTC/EUR_bitstamp": "#F7B801",
    "BTC/GBP_bitstamp": "#D7263D",
    "BTC/USDT_bitstamp": "#00BFA5",
    "BTC/EUR_kraken": "#F7B801",
    "BTC/GBP_kraken": "#D7263D",
    "BTC/USDT_kraken": "#00BFA5",
}
DEFAULT_VOL_COLOR = "#888888"
plt.style.use("dark_background")


def get_color(ex_id: str, sym: str) -> str:
    key = f"{sym}_{ex_id}" if f"{sym}_{ex_id}" in SYMBOL_COLOR else sym
    return SYMBOL_COLOR.get(key, DEFAULT_VOL_COLOR)


def generate_chart(
    ex_id: str,
    price_df: pd.DataFrame,
    price_sym: str,
    vol_map: Dict[str, pd.DataFrame],
    tf_ms: int,
    out: Path,
) -> None:
    if price_df.empty:
        logging.warning("%s no price", ex_id)
        return
    bar_w = (tf_ms / 86_400_000) * 0.9
    fig, (axp, axv) = plt.subplots(
        2, 1, figsize=(14, 6), sharex=True, gridspec_kw={"height_ratios": [2, 1], "hspace": 0.15}
    )
    fig.patch.set_facecolor("#000000")
    fig.suptitle(f"{ex_id.capitalize()} – {price_sym} Price & Volume", color="w", fontsize=18)
    axp.plot(price_df["timestamp"], price_df["close"], lw=1.5, color="#47aaff", label=price_sym)
    axp.grid(ls=":", alpha=0.3)
    axp.tick_params(axis="y", colors="#ccc")
    axp.yaxis.tick_right()
    axp.set_ylabel("Price", color="#ccc")
    axp.legend(loc="upper left")
    bottom = pd.Series(0, index=price_df.index)
    for sym, vdf in vol_map.items():
        ser = vdf.set_index("timestamp")["volume"].reindex(price_df["timestamp"]).fillna(0)
        axv.bar(price_df["timestamp"], ser, bottom=bottom, width=bar_w, color=get_color(ex_id, sym), label=sym)
        bottom += ser
    axv.set_ylabel("Volume", color="#ccc")
    axv.grid(ls=":", alpha=0.3)
    axv.tick_params(axis="y", colors="#ccc")
    axv.yaxis.tick_right()
    axv.legend(loc="upper left", fontsize=9, ncol=2)
    loc = mdates.AutoDateLocator(minticks=5, maxticks=10)
    axv.xaxis.set_major_locator(loc)
    axv.xaxis.set_major_formatter(mdates.ConciseDateFormatter(loc))
    fig.autofmt_xdate()
    fig.tight_layout(rect=[0, 0, 1, 0.95])
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    logging.info("saved %s", out)
