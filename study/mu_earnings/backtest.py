"""Does trading the print beat holding through it? 2024-03-20 -> 2026-09-11.

Signal timing is honest: guidance is known at ~4:15pm ET on report day T, so every
rule that uses it trades at the T+1 OPEN, never the T close.
"""
import pandas as pd, numpy as np
P = pd.read_csv("prices.csv", parse_dates=["date"])
S = pd.read_csv("mu_earnings_study.csv")

mu = P[P.ticker=="MU"].sort_values("date").set_index("date")
START = pd.Timestamp("2024-03-20")
mu = mu[mu.index >= START].copy()
n = len(mu)
idx = {d:i for i,d in enumerate(mu.index)}

# report-day positions and their guidance verdict
ev = [(idx[pd.Timestamp(r.date)], r.guide_surp_pct) for r in S.itertuples() if pd.Timestamp(r.date) in idx]

o, c = mu.open.values, mu.close.values
r_cc = np.zeros(n); r_cc[1:] = c[1:]/c[:-1] - 1          # close-to-close
r_co = np.zeros(n); r_co[1:] = o[1:]/c[:-1] - 1          # prev close -> open (the gap)
r_oc = o.copy(); r_oc = c/o - 1                          # open -> close, same day

def stats(daily, name, trades=None):
    eq = np.cumprod(1+daily)
    yrs = (mu.index[-1]-mu.index[0]).days/365.25
    cagr = (eq[-1]**(1/yrs)-1)*100
    vol  = np.std(daily)*np.sqrt(252)*100
    dd   = ((eq/np.maximum.accumulate(eq))-1).min()*100
    t = f"{trades:>3}" if trades is not None else "  —"
    return dict(name=name, mult=eq[-1], cagr=cagr, vol=vol, rv=cagr/vol if vol else np.nan, dd=dd, trades=t), eq

R = []

# A ── buy & hold
R.append(stats(r_cc, "A. Buy & hold MU"))

# B ── hold, but flat across every earnings reaction (out T close -> T+1 close)
d = r_cc.copy()
for i,_ in ev:
    if i+1 < n: d[i+1] = 0.0
R.append(stats(d, "B. Hold, sit out the 10 reaction days", trades=len(ev)*2))

# C ── long only the 21 sessions after an above-consensus guide (enter T+1 open)
d = np.zeros(n); tr=0
for i,g in ev:
    if g >= 2 and i+1 < n:
        tr += 2
        d[i+1] = r_oc[i+1]                                   # enter at open, hold to close
        for k in range(i+2, min(i+22, n)): d[k] = r_cc[k]
R.append(stats(d, "C. Long 21d only after an above-street guide", trades=tr))

# D ── hold always, EXCEPT flat 21 sessions after an at/below-street guide
d = r_cc.copy(); tr=0
for i,g in ev:
    if g < 2 and i+1 < n:
        tr += 2
        d[i+1] = 0.0
        for k in range(i+2, min(i+22, n)): d[k] = 0.0
R.append(stats(d, "D. Hold, but exit 21d on a bad guide", trades=tr))

# E ── vol-targeted buy & hold, no leverage (the literal 'filter out the vol')
rv20 = pd.Series(r_cc).rolling(20).std().shift(1)*np.sqrt(252)
for tgt,cap,lab in [(0.25,1.0,"E. Vol-target 25%, no leverage"),(0.25,2.0,"F. Vol-target 25%, up to 2x")]:
    w = np.clip((tgt/rv20).fillna(0).values, 0, cap)
    R.append(stats(w*r_cc, lab))

# benchmarks
for t in ["SOXX","QQQ","SPY"]:
    b = P[P.ticker==t].sort_values("date").set_index("date")
    b = b[b.index>=START]
    R.append(stats(np.r_[0, b.close.values[1:]/b.close.values[:-1]-1], f"·  {t} buy & hold"))

print(f"{'strategy':<44}{'x money':>9}{'CAGR':>9}{'vol':>8}{'ret/vol':>9}{'maxDD':>9}{'trades':>8}")
print("-"*96)
for s,_ in R:
    print(f"{s['name']:<44}{s['mult']:>8.2f}x{s['cagr']:>8.1f}%{s['vol']:>7.1f}%{s['rv']:>9.2f}{s['dd']:>8.1f}%{s['trades']:>8}")

print("\nWhat the 10 reaction days alone contributed:")
react = np.prod([1+r_cc[i+1] for i,_ in ev if i+1<n])
print(f"  compounding just the 10 next-day moves: {react:.3f}x  ({(react-1)*100:+.1f}%)")
print(f"  buy & hold total:                       {np.prod(1+r_cc):.2f}x")
print(f"  so the events were {(react-1)*100:+.1f}% of a {(np.prod(1+r_cc)-1)*100:+.0f}% total move")

print("\n" + "="*96)
print("Trend filters — 'filter out the vol' in its practical form (signal at T-1 close, trade at T open)")
print("="*96)
extra=[]
mu_c = pd.Series(c, index=mu.index)
sox = P[P.ticker=="SOXX"].sort_values("date").set_index("date")
sox_full = sox.close
for lab, sig in [
    ("G. Hold MU only when MU > its 200-day", (mu_c > mu_c.rolling(200, min_periods=60).mean()).shift(1)),
    ("H. Hold MU only when MU > its 50-day",  (mu_c > mu_c.rolling(50,  min_periods=20).mean()).shift(1)),
    ("I. Hold MU only when SOXX > its 50-day",(sox_full > sox_full.rolling(50).mean()).reindex(mu.index).shift(1)),
]:
    w = sig.fillna(False).astype(float).values
    st,_ = stats(w*r_cc, lab, trades=int(np.abs(np.diff(np.r_[0,w])).sum()))
    extra.append(st)
print(f"{'strategy':<44}{'x money':>9}{'CAGR':>9}{'vol':>8}{'ret/vol':>9}{'maxDD':>9}{'trades':>8}")
print("-"*96)
for s in extra:
    print(f"{s['name']:<44}{s['mult']:>8.2f}x{s['cagr']:>8.1f}%{s['vol']:>7.1f}%{s['rv']:>9.2f}{s['dd']:>8.1f}%{s['trades']:>8}")
print(f"\n(reference)  A. Buy & hold MU                    {R[0][0]['mult']:>8.2f}x{R[0][0]['cagr']:>8.1f}%{R[0][0]['vol']:>7.1f}%{R[0][0]['rv']:>9.2f}{R[0][0]['dd']:>8.1f}%")
