"""Backtest the 'sell everything when SOXL closes <= X%' rule on SOXL and MU.

Exit is either at the trigger-day close (MOC order, idealised) or the next
open (realistic if you only see the close). Re-entry variants: fixed N trading
days after the last trigger, SOXL reclaiming its 10-day SMA, or SOXL closing
back above the exit-day close. Cash earns a flat 4%/yr. A random-exit placebo
(same number of exits, same N) tells us whether the rule beats luck.
"""
import numpy as np
import pandas as pd
from common import HERE, START, END, load, daily_ret
from rules import simulate, metrics, close, opn, r, CASH_D

OUT = HERE / "out"
rng = np.random.default_rng(11)

WINDOWS = {"3y": (START, END), "2010-2023": (pd.Timestamp("2010-03-12"), START)}
rows, equity = [], {}
for wname, (lo, hi) in WINDOWS.items():
    for asset in ["SOXL", "MU"]:
        bh = r[asset].loc[(close.index >= lo) & (close.index <= hi)].copy()
        bh.iloc[0] = 0
        rows.append(dict(window=wname, asset=asset, trigger="buy&hold", reentry="-", exec="-",
                         exits=0, time_in=100, **metrics(bh)))
        if wname == "3y":
            equity[(asset, "Buy & hold")] = (1 + bh).cumprod()
        trigs = {f"ret<={t}%": r["SOXL"] * 100 <= t for t in [-5, -6, -7, -8, -9, -10, -12, -15, -20]}
        z = r["SOXL"] / r["SOXL"].rolling(60).std().shift(1)
        trigs.update({f"z<={t}": z <= t for t in [-2.0, -2.5, -3.0]})
        for tname, trig in trigs.items():
            variants = [("ndays", n) for n in [1, 3, 5, 10, 21]] + [("sma10", 0), ("above_exit", 0)]
            for reentry, n in variants:
                for ex in ["close", "open"]:
                    s, exits, tin = simulate(asset, trig, reentry, ex, lo, hi, n)
                    lbl = f"{n}d" if reentry == "ndays" else reentry
                    rows.append(dict(window=wname, asset=asset, trigger=tname, reentry=lbl, exec=ex,
                                     exits=exits, time_in=tin * 100, **metrics(s)))
                    if wname == "3y" and tname == "ret<=-10%" and ex == "close" and lbl in ("5d", "sma10", "above_exit"):
                        equity[(asset, f"Sell at -10%, re-enter {lbl}")] = (1 + s).cumprod()

bt = pd.DataFrame(rows)
bt.to_csv(OUT / "backtest.csv", index=False)
eqdf = pd.DataFrame({f"{a}|{k}": v for (a, k), v in equity.items()})
eqdf.to_csv(OUT / "equity_3y.csv")

# --- placebo: random exit days, same count, same N-day re-entry, close execution ---
pl = []
for wname, (lo, hi) in WINDOWS.items():
    idx = close.index[(close.index >= lo) & (close.index <= hi)]
    real_trig = (r["SOXL"] * 100 <= -10)
    k = int(real_trig.loc[idx].sum())
    for asset in ["SOXL", "MU"]:
        for n in [1, 5, 21]:
            real = metrics(simulate(asset, real_trig, "ndays", "close", lo, hi, n)[0])
            sims = []
            for _ in range(500):
                fake = pd.Series(False, index=close.index)
                fake.loc[rng.choice(idx[1:-1], size=k, replace=False)] = True
                sims.append(metrics(simulate(asset, fake, "ndays", "close", lo, hi, n)[0]))
            sims = pd.DataFrame(sims)
            pl.append(dict(window=wname, asset=asset, reentry=f"{n}d", n_trig=k,
                           rule_cagr=real["cagr"], placebo_cagr_med=sims.cagr.median(),
                           rule_cagr_pctile=(sims.cagr < real["cagr"]).mean() * 100,
                           rule_maxdd=real["maxdd"], placebo_maxdd_med=sims.maxdd.median(),
                           rule_maxdd_pctile=(sims.maxdd < real["maxdd"]).mean() * 100,
                           rule_sharpe=real["sharpe"], placebo_sharpe_med=sims.sharpe.median(),
                           rule_sharpe_pctile=(sims.sharpe < real["sharpe"]).mean() * 100))
pld = pd.DataFrame(pl)
pld.to_csv(OUT / "placebo.csv", index=False)

pd.set_option("display.width", 250, "display.max_columns", 40, "display.max_rows", 500)
cols = ["asset", "trigger", "reentry", "exec", "exits", "time_in", "total", "cagr", "maxdd", "vol", "sharpe", "calmar"]
for w in WINDOWS:
    d = bt[(bt.window == w) & ((bt.trigger.isin(["buy&hold", "ret<=-10%"])) | ((bt.reentry == "5d") & (bt["exec"] == "close")))]
    print(f"\n=== {w} ===")
    print(d[cols].round(2).to_string(index=False))
print("\n=== PLACEBO (-10%, close exec) ===")
print(pld.round(2).to_string(index=False))
