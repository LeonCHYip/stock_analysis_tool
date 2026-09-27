"""Condition the SOXL <= -10% events on context: trend, clustering, VIX, breadth,
MU-specific weakness, same-day earnings reactions and (if present) the
hand-labelled news catalyst. Also the risk view (what happens next) and a
calendar-year split of the rule vs buy & hold.
"""
import numpy as np
import pandas as pd
from common import HERE, START, END, load, daily_ret, fwd_ret, fwd_min
from rules import simulate, metrics

close, opn = load()
r = daily_ret(close)
idx = close.index
OUT = HERE / "out"
TH = -10
PRE = (pd.Timestamp("2010-03-12"), START - pd.Timedelta(days=1))

s = close["SOXL"]
F = pd.DataFrame(index=idx)
for t in ["SOXL", "MU"]:
    for h in [1, 5, 21, 63]:
        F[f"{t}_f{h}"] = fwd_ret(close[t], h)
    F[f"{t}_mae21"] = fwd_min(close[t], 21)
    F[f"{t}_gap"] = (opn[t].shift(-1) / close[t] - 1) * 100
    F[f"{t}_intraday_next"] = (close[t].shift(-1) / opn[t].shift(-1) - 1) * 100
for t in ["SOXL", "MU", "SOXX", "NVDA", "QQQ", "VIX"]:
    F[f"{t}_1d"] = r[t]
trig = r["SOXL"] <= TH
F["trig"] = trig
F["above200"] = s > s.rolling(200).mean()
F["first_in_cluster"] = trig & (trig.astype(int).rolling(10).sum().shift(1) == 0)
F["vix_jump20"] = r["VIX"] >= 20
F["broad_qqq_le_-2"] = r["QQQ"] <= -2
F["mu_worse_than_soxx"] = r["MU"] < r["SOXX"]
F["dd252"] = (s / s.rolling(252).max() - 1) * 100
F["dd_bucket"] = pd.cut(F.dd252, [-101, -40, -20, 0.1], labels=["DD >40%", "DD 20-40%", "DD <20%"])
F["prior20_up"] = s / s.shift(20) > 1
F["another_trig_5d"] = pd.concat([trig.shift(-k) for k in range(1, 6)], axis=1).fillna(False).any(axis=1)
F["SOXL_fvol21"] = r["SOXL"][::-1].rolling(21).std()[::-1].shift(-1) * np.sqrt(252)

# --- same-day earnings reactions (3y only; DB earnings coverage starts 2023) ---
E = pd.read_csv(HERE / "semi_earnings.csv", parse_dates=["earnings_date"])
pos = idx.searchsorted(E.earnings_date)  # first trading day >= report date
pos = np.where(E.earnings_time == "AMC", pos + (idx[np.minimum(pos, len(idx) - 1)] == E.earnings_date), pos)
E = E[pos < len(idx)].copy()
E["react_day"] = idx[pos[pos < len(idx)]]
E["beat"] = (E.eps_sur > 0) & (E.rev_sur > 0)
E["tag"] = E.apply(lambda x: f"{x.ticker}({'beat' if x.beat else 'miss'},{x.one_day_change:+.0f}%)", axis=1)
eg = E.groupby("react_day").agg(earn_names=("tag", " ".join),
                                earn_beat_selloff=("beat", lambda b: bool((b & (E.loc[b.index, "one_day_change"] < -5)).any())))
F = F.join(eg)
F["earn_linked"] = F.earn_names.notna()

CAT = HERE / "catalysts.csv"
if CAT.exists():
    c = pd.read_csv(CAT, parse_dates=["date"]).set_index("date")
    F = F.join(c)

ev3 = F[trig & (idx >= START) & (idx <= END)].copy()
evp = F[trig & (idx >= PRE[0]) & (idx <= PRE[1])].copy()
ev3.index.name = "date"
ev3.round(3).to_csv(OUT / "events_10pct_context.csv")

VALS = ["SOXL_f1", "SOXL_f5", "SOXL_f21", "SOXL_f63", "MU_f5", "MU_f21", "MU_f63", "SOXL_mae21"]


def bucket(ev, col, window):
    rows = []
    for k, g in ev.groupby(col, observed=True):
        d = dict(window=window, factor=col, level=str(k), n=len(g))
        for v in VALS:
            d[f"{v}_mean"] = g[v].mean()
            d[f"{v}_med"] = g[v].median()
        d["SOXL_hit21"] = (g.SOXL_f21.dropna() > 0).mean() * 100
        d["MU_hit21"] = (g.MU_f21.dropna() > 0).mean() * 100
        rows.append(d)
    return rows


rows = []
for col in ["above200", "first_in_cluster", "vix_jump20", "broad_qqq_le_-2", "mu_worse_than_soxx", "dd_bucket", "prior20_up"]:
    rows += bucket(ev3, col, "3y") + bucket(evp, col, "2010-2023")
rows += bucket(ev3, "earn_linked", "3y") + bucket(ev3, "earn_beat_selloff", "3y")
if "category" in ev3:
    rows += bucket(ev3, "category", "3y")
B = pd.DataFrame(rows)
B.to_csv(OUT / "buckets.csv", index=False)

# --- risk view: event days vs every day ---
risk = []
for wname, (lo, hi) in {"3y": (START, END), "2010-2023": PRE}.items():
    w = F[(idx >= lo) & (idx <= hi)]
    for lbl, g in [("all days", w), ("SOXL <= -10% days", w[w.trig])]:
        risk.append(dict(window=wname, sample=lbl, n=len(g),
                         p_another_trig_5d=g.another_trig_5d.mean() * 100,
                         p_soxl_f21_le_m20=(g.SOXL_f21.dropna() <= -20).mean() * 100,
                         p_soxl_f21_ge_p20=(g.SOXL_f21.dropna() >= 20).mean() * 100,
                         soxl_mae21_mean=g.SOXL_mae21.mean(), mu_mae21_mean=g.MU_mae21.mean(),
                         soxl_fwd_vol21=g.SOXL_fvol21.mean(),
                         soxl_next_gap_mean=g.SOXL_gap.mean(), soxl_next_gap_med=g.SOXL_gap.median(),
                         soxl_next_intraday_mean=g.SOXL_intraday_next.mean(),
                         mu_next_gap_mean=g.MU_gap.mean(), mu_next_intraday_mean=g.MU_intraday_next.mean(),
                         soxl_f1_mean=g.SOXL_f1.mean(), mu_f1_mean=g.MU_f1.mean()))
R = pd.DataFrame(risk)
R.to_csv(OUT / "risk.csv", index=False)

# --- calendar-year split, full history ---
lo = pd.Timestamp("2010-03-12")
yr = []
for asset in ["SOXL", "MU"]:
    bh = r[asset].loc[lo:END] / 100
    bh.iloc[0] = 0
    variants = {"buy&hold": bh}
    for n in [5, 21]:
        variants[f"-10% out {n}d"] = simulate(asset, trig, "ndays", "close", lo, END, n)[0]
    for name, ser in variants.items():
        g = ser.groupby(ser.index.year)
        for y, x in g:
            eq = (1 + x).cumprod()
            yr.append(dict(asset=asset, variant=name, year=y, ret=(eq.iloc[-1] - 1) * 100,
                           maxdd=(eq / eq.cummax() - 1).min() * 100,
                           triggers=int(trig.loc[x.index].sum())))
Y = pd.DataFrame(yr)
Y.to_csv(OUT / "by_year.csv", index=False)

pd.set_option("display.width", 260, "display.max_columns", 60, "display.max_rows", 300)
show = ["window", "factor", "level", "n", "SOXL_f1_mean", "SOXL_f5_mean", "SOXL_f21_mean", "SOXL_f21_med", "SOXL_hit21",
        "SOXL_f63_mean", "MU_f5_mean", "MU_f21_mean", "MU_f21_med", "MU_hit21", "SOXL_mae21_mean"]
print(B[show].round(1).to_string(index=False))
print()
print(R.round(2).T.to_string())
print()
print(Y.pivot_table(index="year", columns=["asset", "variant"], values="ret").round(0).to_string())
print(Y[Y.variant == "buy&hold"].pivot_table(index="year", columns="asset", values="triggers").to_string())
print()
print(ev3[["SOXL_1d", "MU_1d", "QQQ_1d", "above200", "first_in_cluster", "earn_names", "SOXL_f5", "SOXL_f21", "MU_f21"]].round(1).to_string())
