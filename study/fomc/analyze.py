"""FOMC event study for SOXL and MU, 2016 → 2026 with a 3-year focus.

For every decision: returns into the meeting, on the day, in the 2pm announcement
window (hourly bars, Oct 2023+), and 1-21 days after; the 2-year Treasury move as the
market's read of the surprise; macro backdrop (CPI, core PCE, unemployment, real rate);
technical setup on the eve (RSI, 50/200-day, prior-month return, volatility, drawdown);
and MU / NVDA earnings proximity. Compares against every trading day in the same window.
"""
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).parent
OUT = HERE / "out"
OUT.mkdir(exist_ok=True)
START10 = pd.Timestamp("2016-01-01")
START3 = pd.Timestamp("2023-09-15")
END = pd.Timestamp("2026-09-14")
rng = np.random.default_rng(3)

px = pd.read_csv(HERE / "prices_daily.csv", parse_dates=["date"])
close = px.pivot(index="date", columns="ticker", values="close")
opn = px.pivot(index="date", columns="ticker", values="open")
keep = close["MU"].notna()  # VIX/DXY print on some exchange holidays
close, opn = close[keep].copy(), opn[keep].copy()
for c in ["VIX", "DXY", "TNX", "IRX"]:
    close[c] = close[c].ffill()
idx = close.index
r = close.pct_change(fill_method=None) * 100
fred = pd.read_csv(HERE / "fred.csv", parse_dates=["date"]).pivot(index="date", columns="series", values="value")
meet = pd.read_csv(HERE / "meetings.csv", parse_dates=["announce", "trade_date"])
labels = pd.read_csv(HERE / "labels.csv", parse_dates=["date"])

WINDOWS = {"pre10": (-11, -1), "pre5": (-6, -1), "day_before": (-2, -1), "fomc_day": (-1, 0),
           "post1": (0, 1), "post5": (0, 5), "post10": (0, 10), "post21": (0, 21)}
ASSETS = ["SOXL", "MU", "SOXX", "QQQ", "TLT"]


def spear(a, b):
    """Spearman rho without scipy: Pearson on ranks."""
    return a.rank().corr(b.rank())


def rsi(c, n=14):
    d = c.diff()
    up = d.clip(lower=0).ewm(alpha=1 / n, adjust=False).mean()
    dn = (-d.clip(upper=0)).ewm(alpha=1 / n, adjust=False).mean()
    return 100 - 100 / (1 + up / dn)


TECH = {}
for t in ["SOXL", "MU"]:
    c = close[t]
    TECH[t] = pd.DataFrame({
        "rsi14": rsi(c), "vs50": (c / c.rolling(50).mean() - 1) * 100, "vs200": (c / c.rolling(200).mean() - 1) * 100,
        "ret20": (c / c.shift(20) - 1) * 100, "vol20": r[t].rolling(20).std() * np.sqrt(252),
        "dd252": (c / c.rolling(252).max() - 1) * 100})

# window returns for every day (baseline) — value indexed by the "T" day
WR = {t: {w: (close[t].shift(-b) / close[t].shift(-a) - 1) * 100 for w, (a, b) in WINDOWS.items()} for t in ASSETS}
GAP = {t: (opn[t] / close[t].shift(1) - 1) * 100 for t in ["SOXL", "MU"]}
INTRA = {t: (close[t] / opn[t] - 1) * 100 for t in ["SOXL", "MU"]}

# earnings reaction days (AMC -> next session)
E = pd.read_csv(HERE / "earnings_dates.csv")
E["ts"] = pd.to_datetime(E.ts, utc=True).dt.tz_convert("America/New_York")
E["d"] = E.ts.dt.tz_localize(None).dt.normalize()
E = E[E.d >= idx[0]].reset_index(drop=True)
pos = idx.searchsorted(E.d)
pos = np.where((E.ts.dt.hour >= 12).values & (idx[np.minimum(pos, len(idx) - 1)] == E.d.values), pos + 1, pos)
E["react_pos"] = pos
REACT = {t: np.sort(E[(E.ticker == t)].react_pos.values) for t in ["MU", "NVDA"]}


def fred_change(sid, T):
    s = fred[sid].dropna()
    if T not in s.index:
        return np.nan
    return (s[T] - s[s.index < T].iloc[-1]) * 100


def fred_lag(sid, T, lag):
    return fred[sid].dropna()[lambda s: s.index <= T - pd.Timedelta(days=lag)]


def yoy(sid, T, lag):
    s = fred_lag(sid, T, lag)
    return (s.iloc[-1] / s.iloc[-13] - 1) * 100 if len(s) >= 13 else np.nan  # FRED pull starts 2015


rows, paths = [], []
for m in meet.itertuples():
    T = m.trade_date
    p = idx.get_loc(T)
    d = dict(date=T, announce=m.announce, scheduled=m.scheduled, sep=m.sep, decision=m.decision, move_bp=m.move_bp,
             upper_after=m.upper_after, phase=m.phase)
    for t in ASSETS:
        for w in WINDOWS:
            d[f"{t}_{w}"] = WR[t][w].iloc[p]
    for t in ["SOXL", "MU"]:
        d[f"{t}_gap"], d[f"{t}_intraday"] = GAP[t].iloc[p], INTRA[t].iloc[p]
        for k, v in TECH[t].iloc[p - 1].items():
            d[f"{t}_{k}"] = v
    d["d2y_bp"], d["d10y_bp"], d["d3m_bp"] = fred_change("DGS2", T), fred_change("DGS10", T), fred_change("DGS3MO", T)
    d["dxy_day"] = r["DXY"].iloc[p]
    d["vix_prev"] = close["VIX"].iloc[p - 1]
    d["cpi_yoy"], d["core_cpi_yoy"] = yoy("CPIAUCSL", T, 45), yoy("CPILFESL", T, 45)
    d["core_pce_yoy"] = yoy("PCEPILFE", T, 60)
    u = fred_lag("UNRATE", T, 35)
    d["unrate"], d["unrate_3m_chg"] = u.iloc[-1], u.iloc[-1] - u.iloc[-4]
    d["real_rate"] = m.upper_before - d["core_cpi_yoy"]
    d["slope_10y2y"] = fred["T10Y2Y"].dropna()[lambda s: s.index < T].iloc[-1]
    for t in ["MU", "NVDA"]:
        rp = REACT[t]
        j = rp[np.argmin(np.abs(rp - p))]
        d[f"{t}_earn_gap_days"] = int(j - p)
    for t in ["SOXL", "MU", "QQQ"]:
        base = close[t].iloc[p - 1]
        for k in range(-10, 22):
            if 0 <= p + k < len(idx):
                paths.append(dict(date=T, asset=t, k=k, ret=(close[t].iloc[p + k] / base - 1) * 100))
    rows.append(d)

F = pd.DataFrame(rows)
F["tone_mkt"] = np.select([F.d2y_bp >= 4, F.d2y_bp <= -4], ["hawkish", "dovish"], "neutral")
F.loc[F.d2y_bp.isna(), "tone_mkt"] = np.nan
F["window3y"] = F.date >= START3
F["soxl_above200"] = F.SOXL_vs200 > 0
F["soxl_pre10_up"] = F.SOXL_pre10 > 0
F["vix_gt20"] = F.vix_prev > 20
F["mu_earn_near"] = F.MU_earn_gap_days.abs() <= 5
F = F.merge(labels.rename(columns={"date": "date"}), on="date", how="left")

# --- intraday (hourly bars, Oct 2023+): 13:30 price vs close ---
H = pd.read_csv(HERE / "prices_hourly.csv", parse_dates=["ts"])
H["day"] = H.ts.dt.normalize()
H["hm"] = H.ts.dt.strftime("%H:%M")
intr = []
hdays = sorted(H.day.unique())
for t in ["SOXL", "MU", "QQQ", "SHY"]:
    h = H[H.ticker == t]
    pre = h[h.hm == "12:30"].set_index("day").close          # price at 13:30, before the 14:00 statement
    cl = h[h.hm == "15:30"].set_index("day").close           # 16:00 close
    op = h[h.hm == "09:30"].set_index("day").open
    df = pd.DataFrame({"pre": pre, "close": cl, "open": op}).dropna()
    df["prev_close"] = df.close.shift(1)
    df["asset"] = t
    intr.append(df)
I = pd.concat(intr).reset_index().rename(columns={"index": "day"})
I["morning"] = (I.pre / I.prev_close - 1) * 100
I["reaction"] = (I.close / I.pre - 1) * 100
fdates = set(F.date)
I["fomc"] = I.day.isin(fdates)
Iw = I.pivot_table(index="day", columns="asset", values=["morning", "reaction"])
Iw.columns = [f"{a}_{b}" for b, a in [(c[0], c[1]) for c in Iw.columns]]
F = F.merge(Iw, left_on="date", right_index=True, how="left")
intraday_base = I[~I.fomc].groupby("asset").agg(abs_reaction_med=("reaction", lambda x: x.abs().median()),
                                                reaction_mean=("reaction", "mean"))
intraday_fomc = I[I.fomc].groupby("asset").agg(abs_reaction_med=("reaction", lambda x: x.abs().median()),
                                               reaction_mean=("reaction", "mean"), n=("reaction", "size"))

F.to_csv(OUT / "meeting_features.csv", index=False)
pd.DataFrame(paths).to_csv(OUT / "paths.csv", index=False)

# --- baseline + drift / bootstrap ---
WIN = {"10y": START10, "3y": START3}
sched = F[F.scheduled]
drift = []
for wl, ws in WIN.items():
    ev = sched[sched.date >= ws]
    days = idx[(idx >= ws) & (idx <= END)]
    for t in ["SOXL", "MU", "QQQ"]:
        absday = r[t].loc[days].abs().median()
        for w in WINDOWS:
            x = ev[f"{t}_{w}"].dropna()
            pool = WR[t][w].loc[days].dropna().values
            sims = rng.choice(pool, size=(10000, len(x))).mean(axis=1)
            drift.append(dict(window=wl, asset=t, measure=w, n=len(x), mean=x.mean(), median=x.median(),
                              hit=(x > 0).mean() * 100, base_mean=pool.mean(), base_median=np.median(pool),
                              base_hit=(pool > 0).mean() * 100, boot_pct=(sims <= x.mean()).mean() * 100))
        x = ev[f"{t}_fomc_day"].dropna()
        drift.append(dict(window=wl, asset=t, measure="abs_day_vs_normal", n=len(x),
                          mean=x.abs().median() / absday, median=np.nan, hit=np.nan, base_mean=1,
                          base_median=np.nan, base_hit=np.nan, boot_pct=np.nan))
        y = ev[[f"{t}_fomc_day", f"{t}_post1", f"{t}_post5"]].dropna()
        drift.append(dict(window=wl, asset=t, measure="corr_day_vs_next1", n=len(y),
                          mean=spear(y.iloc[:, 0], y.iloc[:, 1]), median=np.nan, hit=np.nan,
                          base_mean=np.nan, base_median=np.nan, base_hit=np.nan, boot_pct=np.nan))
        drift.append(dict(window=wl, asset=t, measure="corr_day_vs_next5", n=len(y),
                          mean=spear(y.iloc[:, 0], y.iloc[:, 2]), median=np.nan, hit=np.nan,
                          base_mean=np.nan, base_median=np.nan, base_hit=np.nan, boot_pct=np.nan))
D = pd.DataFrame(drift)
D.to_csv(OUT / "drift.csv", index=False)

# --- group stats ---
OUTC = ["pre5", "fomc_day", "post1", "post5", "post21"]


def grp(df, col, wl):
    res = []
    for k, g in ([("All meetings", df)] if col is None else df.groupby(col, dropna=True)):
        d = dict(window=wl, factor=col or "all", level=str(k), n=len(g))
        for t in ["SOXL", "MU"]:
            for w in OUTC:
                x = g[f"{t}_{w}"].dropna()
                d[f"{t}_{w}_mean"], d[f"{t}_{w}_med"] = x.mean(), x.median()
                d[f"{t}_{w}_hit"] = (x > 0).mean() * 100 if len(x) else np.nan
        res.append(d)
    return res


G = []
for wl, ws in WIN.items():
    ev = sched[sched.date >= ws]
    for col in [None, "decision", "phase", "sep", "tone_mkt", "soxl_above200", "soxl_pre10_up", "vix_gt20", "mu_earn_near"]:
        G += grp(ev, col, wl)
G += grp(sched[sched.window3y], "tone_hand", "3y")
G += grp(F[~F.scheduled], None, "emergency")
G = pd.DataFrame(G)
G.to_csv(OUT / "group_stats.csv", index=False)

# --- factor correlations (Spearman) ---
FACT = ["d2y_bp", "d10y_bp", "dxy_day", "move_bp", "SOXL_pre10", "SOXL_rsi14", "SOXL_vs200", "SOXL_vol20", "SOXL_dd252",
        "MU_pre10", "MU_rsi14", "MU_vs200", "vix_prev", "core_cpi_yoy", "core_pce_yoy", "real_rate", "unrate_3m_chg",
        "slope_10y2y", "upper_after"]
C = []
for wl, ws in WIN.items():
    ev = sched[sched.date >= ws]
    for y in ["SOXL_fomc_day", "SOXL_post5", "SOXL_post21", "MU_fomc_day", "MU_post5", "MU_post21"]:
        for f in FACT:
            z = ev[[f, y]].dropna()
            C.append(dict(window=wl, outcome=y, factor=f, n=len(z), rho=spear(z[f], z[y])))
C = pd.DataFrame(C)
C.to_csv(OUT / "factor_corr.csv", index=False)
pd.concat({"fomc": intraday_fomc, "other_days": intraday_base}).to_csv(OUT / "intraday_summary.csv")

pd.set_option("display.width", 250, "display.max_columns", 60, "display.max_rows", 400)
print(D.round(2).to_string(index=False))
print(intraday_fomc.round(2), "\n", intraday_base.round(2))
g = G[G.factor != "all"][["window", "factor", "level", "n", "SOXL_fomc_day_mean", "SOXL_post5_mean", "SOXL_post5_hit",
                          "SOXL_post21_med", "MU_fomc_day_mean", "MU_post5_mean", "MU_post21_med", "MU_post21_hit"]]
print(G[G.factor == "all"].iloc[:, :12].round(2).to_string(index=False))
print(g.round(1).to_string(index=False))
cc = C[C.n >= 20].copy()
cc["abs"] = cc.rho.abs()
print(cc.sort_values("abs", ascending=False).groupby("window").head(14).round(2).to_string(index=False))
t3 = F[F.window3y][["date", "decision", "move_bp", "sep", "d2y_bp", "tone_mkt", "tone_hand", "SOXL_pre5", "SOXL_morning",
                    "SOXL_reaction", "SOXL_fomc_day", "SOXL_post1", "SOXL_post5", "SOXL_post21", "MU_fomc_day", "MU_post5",
                    "MU_post21", "SOXL_vs200", "MU_earn_gap_days"]]
print(t3.round(1).to_string(index=False))
print(F[~F.scheduled][["date", "move_bp", "SOXL_fomc_day", "SOXL_post5", "SOXL_post21", "MU_post21"]].round(1))
