import sqlite3
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

CREATE_PLOT = "tpo_plot.png"


def distribute_uniform(row: pd.Series, bin_size: int, hist: dict[str, float]) -> None:
    bins = np.arange(
        np.floor(row.low / bin_size) * bin_size,
        np.ceil(row.high / bin_size) * bin_size + bin_size,
        bin_size,
    )
    vpb = row.volume / max(len(bins) - 1, 1)
    for b in bins[:-1]:
        hist[b] = hist.get(b, 0.0) + vpb


def render_png(db_path: str, days: int = 7, bin_size: int = 20, out_path: str = CREATE_PLOT) -> str:
    con = sqlite3.connect(db_path)
    start = pd.Timestamp.utcnow().normalize() - pd.Timedelta(days=days-1)
    df = pd.read_sql(
        "SELECT * FROM ohlcv WHERE ts>=?",
        con,
        params=(int(start.timestamp()*1000),),
    )
    va = pd.read_sql(
        "SELECT * FROM daily_va ORDER BY session_date ASC", con
    )
    cva = pd.read_sql("SELECT * FROM cva", con)
    con.close()
    if df.empty or va.empty:
        return out_path
    df["timestamp"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
    fig, axes = plt.subplots(days, 1, figsize=(19.2, 10.8), sharex=False)
    if days == 1:
        axes = [axes]
    for i, day in enumerate(sorted(df["timestamp"].dt.date.unique())[-days:]):
        ax = axes[i]
        ddf = df[df["timestamp"].dt.date == day]
        ax.plot(ddf["timestamp"], ddf["close"], color="black", linewidth=0.5)
        hist: dict[str, float] = {}
        for _, row in ddf.iterrows():
            distribute_uniform(row, bin_size, hist)
        if hist:
            series = pd.Series(hist).sort_index()
            series /= series.max()
            ax.barh(series.index, series.values * bin_size, height=bin_size, color="gray", alpha=0.3, align="edge")
        vd = va[va["session_date"] == str(day)]
        if not vd.empty:
            vrow = vd.iloc[0]
            ax.axhspan(vrow["val"], vrow["vah"], color="#4da6ff", alpha=0.25)
            ax.axhline(vrow["poc"], color="green", linestyle="-")
        for _, crow in cva.iterrows():
            if crow["start_date"] <= str(day) <= crow["end_date"]:
                ax.axhspan(crow["val"], crow["vah"], color="none", edgecolor="red", linewidth=2)
        ax.set_title(str(day))
    fig.tight_layout()
    fig.savefig(out_path)
    plt.close(fig)
    return out_path


if __name__ == "__main__":
    render_png("tpo_cva.db")
