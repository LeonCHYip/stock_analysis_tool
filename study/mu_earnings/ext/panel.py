"""Build the ~100-print MU earnings panel (Dec 2001 -> Jun 2026): features at T, outcomes after.

Outcome definitions match the original 10-print study so the two can be compared:
  react_1d   T -> T+1 close         (the reaction day)
  gap        T close -> T+1 open
  post_5d    T+1 -> T+6             (drift AFTER the reaction day - the -0.72 target)
  post_21d   T+1 -> T+22
  full_5d    T -> T+5               (reaction + drift, what a holder through the print gets)
  *_ex       same, minus the sector (SOXX/SMH) over the same window
"""
import numpy as np, pandas as pd
from features import load_prices, feature_table, HERE

ED = pd.read_csv(f"{HERE}/../../fomc/earnings_dates.csv")
ED = ED[ED.ticker == "MU"].copy()
ED["ts"] = pd.to_datetime(ED.ts.str[:19])
ED = ED.sort_values("ts").reset_index(drop=True)

P = load_prices()
X = feature_table(P)
mu = P["MU"]
sec = P["SOXX"].close.reindex(mu.index).fillna(P["SMH"].close.reindex(mu.index))
dates = mu.index


def fwd(series, i0, i1):
    if i1 >= len(series) or i0 < 0:
        return np.nan
    return (series.iloc[i1] / series.iloc[i0] - 1) * 100


rows = []
for _, e in ED.iterrows():
    d = e.ts.normalize()
    if d > dates[-1]:
        continue                                   # upcoming print - handled by predict.py
    i = dates.searchsorted(d)
    if i >= len(dates):
        continue
    hour = e.ts.hour
    if hour < 9:                                   # BMO: news hits the open of day d
        T = i - 1 if dates[i] == d else i - 1
    elif hour == 12:                               # Yahoo placeholder time: pick the bigger move
        a = abs(fwd(mu.close, i - 1, i)); b = abs(fwd(mu.close, i, i + 1))
        T = i if b >= a else i - 1
    else:                                          # AMC (16:00-18:00)
        T = i if dates[i] == d else i - 1
    c = mu.close
    r = {"date": dates[T].date(), "report_ts": e.ts, "eps_est": e.eps_est, "eps_act": e.eps_act,
         "eps_surp": e.surprise_pct,
         "gap": (mu.open.iloc[T + 1] / c.iloc[T] - 1) * 100 if T + 1 < len(c) else np.nan,
         "react_1d": fwd(c, T, T + 1), "post_5d": fwd(c, T + 1, T + 6),
         "post_21d": fwd(c, T + 1, T + 22), "full_5d": fwd(c, T, T + 5),
         "sec_react": fwd(sec, T, T + 1), "sec_post_5d": fwd(sec, T + 1, T + 6),
         "sec_post_21d": fwd(sec, T + 1, T + 22)}
    r["react_ex"] = r["react_1d"] - r["sec_react"]
    r["post_5d_ex"] = r["post_5d"] - r["sec_post_5d"]
    r["post_21d_ex"] = r["post_21d"] - r["sec_post_21d"]
    r.update(X.iloc[T].to_dict())
    r["react_atr"] = r["react_1d"] / r["atrp"]              # reaction in ATR units
    r["abs_react_atr"] = abs(r["react_atr"])
    rows.append(r)

df = pd.DataFrame(rows)
# memory of the previous print
df["prev_react"] = df.react_1d.shift()
df["prev_post_21d"] = df.post_21d.shift()
df["ret_since_prev"] = [np.nan] + [
    fwd(mu.close, dates.get_loc(pd.Timestamp(a)) + 1, dates.get_loc(pd.Timestamp(b)))
    for a, b in zip(df.date[:-1], df.date[1:])]
df.to_csv(f"{HERE}/panel.csv", index=False)

# sanity: for AMC prints the reaction day should dwarf day T itself
tday = [fwd(mu.close, dates.get_loc(pd.Timestamp(d)) - 1, dates.get_loc(pd.Timestamp(d))) for d in df.date]
share = np.mean(np.abs(df.react_1d) > np.abs(tday))
print(f"events={len(df)}  {df.date.min()} -> {df.date.max()}   |react| > |day-T move| in {share:.0%}")
print(df[["date", "react_1d", "post_5d", "post_21d", "ret_5d", "rsi14", "atrp"]].tail(12).round(2).to_string(index=False))
