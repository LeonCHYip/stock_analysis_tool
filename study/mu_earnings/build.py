"""MU earnings study: beat history x pre/post price action x market & technical context.

Earnings fundamentals sourced from investing.com consensus (EPS/revenue forecast vs
actual); FQ2'24 estimates from the Mar-2024 release coverage. All MU prints are AMC
(16:00 ET), so day T is the last pre-news close and T+1 carries the reaction.
"""
import pandas as pd, numpy as np

PX = pd.read_csv("prices.csv", parse_dates=["date"])
IDX = ["SOXX", "SOXL", "QQQ", "SPY"]

# fq, report_date, eps_est, eps_act, rev_est($B), rev_act($B)
EVENTS = [
    ("FQ2'24", "2024-03-20", -0.24,  0.42,  5.351,  5.824),
    ("FQ3'24", "2024-06-26",  0.48,  0.62,  6.660,  6.811),
    ("FQ4'24", "2024-09-25",  1.11,  1.18,  7.650,  7.750),
    ("FQ1'25", "2024-12-18",  1.73,  1.79,  8.680,  8.709),
    ("FQ2'25", "2025-03-20",  1.44,  1.56,  7.910,  8.053),
    ("FQ3'25", "2025-06-25",  1.59,  1.91,  8.840,  9.301),
    ("FQ4'25", "2025-09-23",  2.77,  3.03, 11.110, 11.315),
    ("FQ1'26", "2025-12-17",  3.94,  4.78, 12.830, 13.643),
    ("FQ2'26", "2026-03-18",  8.79, 12.20, 19.190, 23.860),
    ("FQ3'26", "2026-06-24", 20.49, 25.11, 35.690, 41.456),
]

def series(tkr):
    s = PX[PX.ticker == tkr].sort_values("date").reset_index(drop=True)
    return s

def rsi(close, n=14):
    d = close.diff()
    up = d.clip(lower=0).ewm(alpha=1/n, adjust=False).mean()
    dn = (-d.clip(upper=0)).ewm(alpha=1/n, adjust=False).mean()
    return 100 - 100 / (1 + up / dn)

def atr_pct(df, n=14):
    pc = df.close.shift()
    tr = pd.concat([df.high - df.low, (df.high - pc).abs(), (df.low - pc).abs()], axis=1).max(axis=1)
    return tr.ewm(alpha=1/n, adjust=False).mean() / df.close * 100

D = {}
for t in ["MU"] + IDX:
    s = series(t)
    s["rsi14"] = rsi(s.close)
    s["sma20"] = s.close.rolling(20).mean()
    s["sma50"] = s.close.rolling(50).mean()
    s["sma200"] = s.close.rolling(200).mean()
    s["atrp"] = atr_pct(s)
    s["vol20"] = s.close.pct_change().rolling(20).std() * np.sqrt(252) * 100
    s["hi252"] = s.close.rolling(252).max()
    s["advol20"] = s.volume.rolling(20).mean()
    D[t] = s

def ret(s, i0, i1):
    if i0 < 0 or i1 >= len(s) or i0 >= len(s):
        return np.nan
    return (s.close.iloc[i1] / s.close.iloc[i0] - 1) * 100

rows = []
for fq, dstr, eps_e, eps_a, rev_e, rev_a in EVENTS:
    d = pd.Timestamp(dstr)
    mu = D["MU"]
    i = mu.index[mu.date == d]
    if len(i) == 0:
        print("missing date", dstr); continue
    i = int(i[0])
    r = {"fq": fq, "date": dstr,
         "eps_est": eps_e, "eps_act": eps_a,
         "eps_surp_pct": (eps_a - eps_e) / abs(eps_e) * 100 if eps_e else np.nan,
         "rev_est": rev_e, "rev_act": rev_a,
         "rev_surp_pct": (rev_a - rev_e) / rev_e * 100,
         "eps_beat": int(eps_a > eps_e), "rev_beat": int(rev_a > rev_e)}

    # --- MU price action around the print (T = report day close = last pre-news px)
    r["px_T"] = mu.close.iloc[i]
    for k, n in [("pre_5d", 5), ("pre_10d", 10), ("pre_21d", 21), ("pre_63d", 63)]:
        r["mu_" + k] = ret(mu, i - n, i)
    r["gap_pct"] = (mu.open.iloc[i+1] / mu.close.iloc[i] - 1) * 100 if i + 1 < len(mu) else np.nan
    r["react_1d"] = ret(mu, i, i + 1)
    for k, n in [("post_3d", 3), ("post_5d", 5), ("post_10d", 10), ("post_21d", 21)]:
        r["mu_" + k] = ret(mu, i + 1, i + n + 1) if i + n + 1 < len(mu) else np.nan
    r["mu_full_21d"] = ret(mu, i, i + 21) if i + 21 < len(mu) else np.nan

    # --- MU technical state going into the print
    r["rsi14"] = mu.rsi14.iloc[i]
    r["vs_sma50"] = (mu.close.iloc[i] / mu.sma50.iloc[i] - 1) * 100
    r["vs_sma200"] = (mu.close.iloc[i] / mu.sma200.iloc[i] - 1) * 100
    r["vs_52wh"] = (mu.close.iloc[i] / mu.hi252.iloc[i] - 1) * 100
    r["atrp"] = mu.atrp.iloc[i]
    r["vol20"] = mu.vol20.iloc[i]
    r["ma_stack"] = int(mu.close.iloc[i] > mu.sma50.iloc[i] > mu.sma200.iloc[i])

    # --- market / sector context
    for t in IDX:
        s = D[t]
        j = int(s.index[s.date == d][0])
        r[f"{t}_pre_21d"] = ret(s, j - 21, j)
        r[f"{t}_pre_63d"] = ret(s, j - 63, j)
        r[f"{t}_post_1d"] = ret(s, j, j + 1)
        r[f"{t}_post_5d"] = ret(s, j + 1, j + 6) if j + 6 < len(s) else np.nan
        r[f"{t}_post_21d"] = ret(s, j + 1, j + 22) if j + 22 < len(s) else np.nan
        if t in ("SOXX", "QQQ"):
            r[f"{t}_above50"] = int(s.close.iloc[j] > s.sma50.iloc[j])
            r[f"{t}_above200"] = int(s.close.iloc[j] > s.sma200.iloc[j])
    # relative strength / excess
    r["rs_pre_63d"] = r["mu_pre_63d"] - r["SOXX_pre_63d"]
    r["rs_pre_21d"] = r["mu_pre_21d"] - r["SOXX_pre_21d"]
    r["excess_1d"] = r["react_1d"] - r["SOXX_post_1d"]
    r["excess_21d"] = r["mu_post_21d"] - r["SOXX_post_21d"]
    rows.append(r)

df = pd.DataFrame(rows)
df.to_csv("mu_earnings_study.csv", index=False)
pd.set_option("display.width", 250, "display.max_columns", 100)
print(df.round(2).to_string(index=False))
