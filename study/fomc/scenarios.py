"""Scenario inputs for the 16 Sep 2026 FOMC decision.

1. Today's setup (as of the 14 Sep close) on the same factors as every past meeting.
2. Analog tables: every hike since 2016; meetings entered after a >=10% SOXL slide; meetings
   with SOXL below its 200-day average.
3. Reaction by market-read tone (2-year yield move) overall, for hikes, and for weak setups.
"""
import json
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).parent
OUT = HERE / "out"
F = pd.read_csv(OUT / "meeting_features.csv", parse_dates=["date"])
S = F[F.scheduled].copy()
px = pd.read_csv(HERE / "prices_daily.csv", parse_dates=["date"])
close = px.pivot(index="date", columns="ticker", values="close")
close = close[close["MU"].notna()]
r = close.pct_change(fill_method=None) * 100
fred = pd.read_csv(HERE / "fred.csv", parse_dates=["date"]).pivot(index="date", columns="series", values="value")


def rsi(c, n=14):
    d = c.diff()
    up = d.clip(lower=0).ewm(alpha=1 / n, adjust=False).mean()
    dn = (-d.clip(upper=0)).ewm(alpha=1 / n, adjust=False).mean()
    return 100 - 100 / (1 + up / dn)


# --- current setup: last close stands in for T-1 (15 Sep not yet closed when built) ---
last = close.index[-1]
cur = {"as_of": str(last.date())}
for t in ["SOXL", "MU"]:
    c = close[t]
    cur[f"{t}_close"] = c.iloc[-1]
    cur[f"{t}_pre10"] = (c.iloc[-1] / c.iloc[-11] - 1) * 100
    cur[f"{t}_pre5"] = (c.iloc[-1] / c.iloc[-6] - 1) * 100
    cur[f"{t}_rsi14"] = rsi(c).iloc[-1]
    cur[f"{t}_vs50"] = (c.iloc[-1] / c.rolling(50).mean().iloc[-1] - 1) * 100
    cur[f"{t}_vs200"] = (c.iloc[-1] / c.rolling(200).mean().iloc[-1] - 1) * 100
    cur[f"{t}_dd252"] = (c.iloc[-1] / c.rolling(252).max().iloc[-1] - 1) * 100
    cur[f"{t}_vol20"] = r[t].iloc[-20:].std() * np.sqrt(252)
cur["vix"] = close["VIX"].ffill().iloc[-1]
cur["dgs2"] = fred["DGS2"].dropna().iloc[-1]
cur["dgs2_date"] = str(fred["DGS2"].dropna().index[-1].date())
cur["dgs2_chg_20d_bp"] = (fred["DGS2"].dropna().iloc[-1] - fred["DGS2"].dropna().iloc[-21]) * 100
cur["dgs10"] = fred["DGS10"].dropna().iloc[-1]
cpi = fred["CPIAUCSL"].dropna()
core = fred["CPILFESL"].dropna()
pce = fred["PCEPILFE"].dropna()
cur["cpi_yoy"] = (cpi.iloc[-1] / cpi.iloc[-13] - 1) * 100
cur["core_cpi_yoy"] = (core.iloc[-1] / core.iloc[-13] - 1) * 100
cur["core_pce_yoy"] = (pce.iloc[-1] / pce.iloc[-13] - 1) * 100
u = fred["UNRATE"].dropna()
cur["unrate"], cur["unrate_3m_chg"] = u.iloc[-1], u.iloc[-1] - u.iloc[-4]
cur["upper"] = fred["DFEDTARU"].dropna().iloc[-1]
cur["real_rate"] = cur["upper"] - cur["core_cpi_yoy"]
# where today's setup ranks among the 84 scheduled meetings
for k in ["SOXL_pre10", "SOXL_rsi14", "SOXL_vs200", "SOXL_dd252", "SOXL_vol20", "MU_pre10", "MU_vs200"]:
    cur[f"{k}_pctile"] = (S[k].dropna() < cur[k]).mean() * 100
(OUT / "current_state.json").write_text(json.dumps({k: (round(v, 3) if isinstance(v, float) else v) for k, v in cur.items()}, indent=2))

COLS = ["date", "decision", "move_bp", "sep", "phase", "d2y_bp", "tone_mkt", "SOXL_pre10", "SOXL_vs200", "SOXL_dd252",
        "SOXL_fomc_day", "SOXL_post1", "SOXL_post5", "SOXL_post21", "MU_fomc_day", "MU_post5", "MU_post21"]
hikes = S[S.decision == "hike"][COLS]
hikes.to_csv(OUT / "analog_hikes.csv", index=False)
slide = S[S.SOXL_pre10 <= -10][COLS]
slide.to_csv(OUT / "analog_selloff.csv", index=False)


def summ(g, label):
    d = dict(bucket=label, n=len(g))
    for t in ["SOXL", "MU"]:
        for w in ["fomc_day", "post1", "post5", "post21"]:
            x = g[f"{t}_{w}"].dropna()
            d[f"{t}_{w}_med"] = x.median() if len(x) else np.nan
            d[f"{t}_{w}_mean"] = x.mean() if len(x) else np.nan
            d[f"{t}_{w}_hit"] = (x > 0).mean() * 100 if len(x) else np.nan
            d[f"{t}_{w}_p10"] = x.quantile(0.1) if len(x) >= 5 else np.nan
            d[f"{t}_{w}_p90"] = x.quantile(0.9) if len(x) >= 5 else np.nan
    return d


rows = [summ(S, "All meetings")]
for tone in ["hawkish", "neutral", "dovish"]:
    rows.append(summ(S[S.tone_mkt == tone], f"All · {tone}"))
H = S[S.decision == "hike"]
rows.append(summ(H, "Hikes"))
for tone in ["hawkish", "neutral", "dovish"]:
    rows.append(summ(H[H.tone_mkt == tone], f"Hikes · {tone}"))
W = S[S.SOXL_pre10 <= -10]
rows.append(summ(W, "SOXL down 10%+ into meeting"))
for tone in ["hawkish", "neutral", "dovish"]:
    rows.append(summ(W[W.tone_mkt == tone], f"Slide · {tone}"))
B = S[S.SOXL_vs200 < 0]
rows.append(summ(B, "SOXL below 200-day"))
for tone in ["hawkish", "neutral", "dovish"]:
    rows.append(summ(B[B.tone_mkt == tone], f"Below 200 · {tone}"))
T = pd.DataFrame(rows)
T.to_csv(OUT / "scenario_buckets.csv", index=False)

pd.set_option("display.width", 250, "display.max_columns", 40)
print(json.dumps({k: (round(v, 2) if isinstance(v, float) else v) for k, v in cur.items()}, indent=1))
print(hikes.round(1).to_string(index=False))
print(slide.round(1).to_string(index=False))
print(T[["bucket", "n", "SOXL_fomc_day_med", "SOXL_post1_med", "SOXL_post5_med", "SOXL_post5_hit", "SOXL_post21_med",
         "SOXL_post21_hit", "SOXL_post21_p10", "SOXL_post21_p90", "MU_fomc_day_med", "MU_post5_med", "MU_post21_med",
         "MU_post21_hit"]].round(1).to_string(index=False))
