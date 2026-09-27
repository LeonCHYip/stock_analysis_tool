"""Event study: forward SOXL / MU returns after SOXL daily drops of various sizes.

Compares event-day forward returns to (a) all days in the same window and
(b) a random-day bootstrap of equal size, and reports a de-clustered version
(first trigger in 10 trading days) because drops arrive in bursts.
"""
import numpy as np
import pandas as pd
from common import HERE, START, END, HORIZONS, THRESHOLDS, load, daily_ret, fwd_ret, fwd_min, fwd_max

rng = np.random.default_rng(7)
close, opn = load()
r = daily_ret(close)
OUT = HERE / "out"
OUT.mkdir(exist_ok=True)


def declustered(mask, gap=10):
    idx = np.flatnonzero(mask.values)
    keep, last = [], -10**9
    for i in idx:
        if i - last > gap:
            keep.append(i)
        last = i  # a burst stays one episode while triggers keep coming
    m = pd.Series(False, index=mask.index)
    m.iloc[keep] = True
    return m


def boot_p(pool, n, obs_mean, iters=10000):
    """Share of random equal-size day samples whose mean <= observed mean."""
    pool = pool.dropna().values
    if n == 0 or len(pool) == 0:
        return np.nan
    sims = rng.choice(pool, size=(iters, n), replace=True).mean(axis=1)
    return (sims <= obs_mean).mean()


def study(lo, hi, label):
    win = (close.index >= lo) & (close.index <= hi)
    rows = []
    for tkr in ["SOXL", "MU"]:
        s = close[tkr]
        fr = {h: fwd_ret(s, h) for h in HORIZONS}
        for th in THRESHOLDS:
            for kind in ["all", "declustered"]:
                m = (r["SOXL"] <= th) & win
                if kind == "declustered":
                    m = declustered(m)
                for h in HORIZONS:
                    ev = fr[h][m].dropna()
                    base = fr[h][win].dropna()
                    rows.append(dict(
                        window=label, asset=tkr, threshold=th, sample=kind, h=h,
                        n=len(ev), mean=ev.mean(), median=ev.median(), hit=(ev > 0).mean() * 100,
                        base_mean=base.mean(), base_median=base.median(), base_hit=(base > 0).mean() * 100,
                        excess_mean=ev.mean() - base.mean(),
                        excess_median=ev.median() - base.median(),
                        boot_pct=boot_p(base, len(ev), ev.mean()),
                    ))
    return pd.DataFrame(rows)


res = pd.concat([
    study(START, END, "3y (2023-09 → 2026-09)"),
    study(pd.Timestamp("2010-03-11"), START - pd.Timedelta(days=1), "pre-window (2010 → 2023-09)"),
])
res.to_csv(OUT / "event_study.csv", index=False)

# --- event-level table for the -5% and worse set (context joined later) ---
win = (close.index >= START) & (close.index <= END)
ev = pd.DataFrame(index=close.index[(r["SOXL"] <= -5) & win])
for t in ["SOXL", "MU", "SOXX", "NVDA", "QQQ", "VIX"]:
    ev[f"{t}_1d"] = r[t]
ev["VIX_close"] = close["VIX"]
for t in ["SOXL", "MU"]:
    s = close[t]
    ev[f"{t}_gap_next_open"] = (opn[t].shift(-1) / s - 1) * 100
    for h in HORIZONS:
        ev[f"{t}_f{h}"] = fwd_ret(s, h)
    for h in [5, 21]:
        ev[f"{t}_mae{h}"] = fwd_min(s, h)
        ev[f"{t}_mfe{h}"] = fwd_max(s, h)
s = close["SOXL"]
ev["SOXL_vs_sma200"] = (s / s.rolling(200).mean() - 1) * 100
ev["SOXL_vs_sma50"] = (s / s.rolling(50).mean() - 1) * 100
ev["SOXL_dd_252"] = (s / s.rolling(252).max() - 1) * 100
ev["SOXL_ret_20d_prior"] = (s / s.shift(20) - 1) * 100
ev["SOXL_vol20"] = r["SOXL"].rolling(20).std().shift(1) * np.sqrt(252)
ev["SOXL_z"] = r["SOXL"] / r["SOXL"].rolling(60).std().shift(1)
prior10 = (r["SOXL"] <= -10).rolling(10).sum().shift(1)
ev["prior_10pct_drops_10d"] = prior10
ev.index.name = "date"
ev.round(3).to_csv(OUT / "events_5pct.csv")

pd.set_option("display.width", 250, "display.max_columns", 40)
show = res[(res["sample"] == "all") & res.h.isin([1, 5, 21, 63])]
print(show.pivot_table(index=["window", "asset", "threshold"], columns="h",
                       values=["n", "mean", "excess_mean", "median", "hit", "boot_pct"]).round(2).to_string())
print("\nDECLUSTERED")
show = res[(res["sample"] == "declustered") & res.h.isin([1, 5, 21, 63])]
print(show.pivot_table(index=["window", "asset", "threshold"], columns="h",
                       values=["n", "mean", "excess_mean", "hit", "boot_pct"]).round(2).to_string())
