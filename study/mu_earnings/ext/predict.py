"""Predict MU's post-earnings path for the upcoming print from the latest technical data.

    uv run python predict.py            # refresh prices, predict off the latest close
    uv run python predict.py --no-fetch # reuse prices_long.csv

The models' inputs are day-T (report-day close) values. Run before T, the script also shows
how the call changes if MU moves X% over the sessions still to come, by appending synthetic
bars on a straight-line path to the scenario close. Re-run after the report-day close for the
real read.
"""
import sys, subprocess, numpy as np, pandas as pd
from features import load_prices, feature_table, HERE
from model import MODELS, OVX, fit_predict

NEXT_PRINT = pd.Timestamp("2026-09-30")          # AMC -> day T is this session's close

if "--no-fetch" not in sys.argv:
    subprocess.run([sys.executable, f"{HERE}/fetch.py"], check=True, capture_output=True)
subprocess.run([sys.executable, f"{HERE}/panel.py"], check=True, capture_output=True, cwd=HERE)

panel = pd.read_csv(f"{HERE}/panel.csv", parse_dates=["date"])
P = load_prices()
last = P["MU"].index[-1]
bdays = pd.bdate_range(last, NEXT_PRINT)[1:]    # sessions still to come up to and including T
prev_react = panel.react_1d.iloc[-1]


def features_at_T(move_pct):
    """Feature row at T assuming MU moves move_pct% (straight line) over the remaining sessions."""
    Q = {k: v.copy() for k, v in P.items()}
    if len(bdays):
        mu = Q["MU"]; c0 = mu.close.iloc[-1]; v0 = mu.volume.iloc[-20:].mean()
        path = c0 * (1 + move_pct / 100) ** (np.arange(1, len(bdays) + 1) / len(bdays))
        add = pd.DataFrame({"open": path, "high": path, "low": path, "close": path,
                            "volume": v0, "ticker": "MU"}, index=bdays)
        Q["MU"] = pd.concat([mu, add])
        for t in Q:                              # context tickers: hold flat over the gap
            if t != "MU":
                s = Q[t]; row = s.iloc[[-1]]
                Q[t] = pd.concat([s, pd.DataFrame(np.repeat(row.values, len(bdays), 0),
                                                  columns=s.columns, index=bdays)]).astype(s.dtypes)
    x = feature_table(Q).iloc[[-1]].copy()
    x["prev_react"] = prev_react
    return x


def predict(x):
    out = {}
    for name, m in MODELS.items():
        hist = panel[panel.date >= m["start"]]
        p, beta, sd, xt = fit_predict(hist, x, m["feats"], m["target"])
        out[name] = (p[0], sd, xt)
    return out


pd.set_option("display.width", 200)
print(f"latest bar {last.date()}   print {NEXT_PRINT.date()} AMC   sessions to T: {len(bdays)}   "
      f"prev print reaction {prev_react:+.1f}%")

x0 = features_at_T(0.0)
reg = panel[panel.date >= "2014-01-01"]
print("\n-- overextension inputs at T (flat-path scenario) vs 2014+ print history")
for f in OVX + ["rsi14", "ret_21d", "ret_63d", "vs_sma50", "sox_ret_63d", "atrp"]:
    v = x0[f].iloc[0]
    print(f"   {f:12s} {v:8.2f}   pctile {np.mean(reg[f] < v)*100:5.0f}   2014+ median {reg[f].median():7.2f}")

res = predict(x0)
lab = {"A": "reaction day (T->T+1)", "B": "drift after reaction (T+1->T+6)", "C": "hold through (T->T+5)"}
print("\n-- model read (flat path to T)")
for k, (p, sd, xt) in res.items():
    ovx = f"   ovx z {xt['ovx'].iloc[0]:+.2f}" if "ovx" in xt else ""
    print(f"   {k} {lab[k]:32s} {p:+6.2f}%   ±1sd {sd:5.1f}%{ovx}")

print("\n-- scenario: MU cumulative move from last close to T close")
rows = []
for mv in (-10, -6, -3, 0, 3, 6, 10):
    r = predict(features_at_T(mv))
    rows.append({"move_to_T": mv, "A_react": r["A"][0], "B_drift5": r["B"][0], "C_full5": r["C"][0],
                 "ovx_z": r["B"][2]["ovx"].iloc[0]})
print(pd.DataFrame(rows).round(2).to_string(index=False))

# nearest 2014+ analogues on the two stable signals
reg = reg.copy()
reg["ovx"] = ((reg[OVX] - reg[OVX].mean()) / reg[OVX].std()).mean(1)
x_ovx = (((x0[OVX] - reg[OVX].mean()) / reg[OVX].std()).mean(1)).iloc[0]
z = lambda s, v: (s - v) / s.std()
reg["dist"] = np.hypot(z(reg.ovx, x_ovx), z(reg.prev_react, prev_react))
print("\n-- closest 2014+ analogues (overextension + previous reaction)")
print(reg.nsmallest(6, "dist")[["date", "prev_react", "ovx", "ret_5d", "react_1d", "post_5d", "full_5d", "post_21d"]]
      .round(2).to_string(index=False))
