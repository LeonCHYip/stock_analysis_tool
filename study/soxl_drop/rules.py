"""Rule engine for the SOXL drop backtest: simulate() and metrics()."""
import numpy as np
import pandas as pd
from common import HERE, START, END, load, daily_ret

close, opn = load()
r = daily_ret(close) / 100
CASH_D = 1.04 ** (1 / 252) - 1


def simulate(asset, trig, reentry, exec_="close", lo=START, hi=END, n_days=5):
    """trig: bool Series on the full index. Returns daily strategy returns within [lo, hi]."""
    idx = close.index[(close.index >= lo) & (close.index <= hi)]
    px, soxl = close[asset].loc[idx], close["SOXL"].loc[idx]
    ra = r[asset].loc[idx].values
    gap = (opn[asset].loc[idx].values / px.shift(1).values) - 1
    sma10 = close["SOXL"].rolling(10).mean().loc[idx].values
    tg = trig.loc[idx].values
    out = np.zeros(len(idx))
    invested, last_trig, exit_px, exits, days_in = True, -1, np.nan, 0, 0
    for i in range(len(idx)):
        if i == 0:
            out[i] = 0.0  # start at the first close
        elif invested:
            out[i] = ra[i]
            days_in += 1
        else:
            # exited at close of a prior day; if exec is next-open we still eat day-after gap
            out[i] = gap[i] if (exec_ == "open" and i == last_trig + 1) else CASH_D
            if exec_ == "open" and i == last_trig + 1:
                days_in += 1
        # decide state at this close (acts from next day)
        if tg[i]:
            if invested:
                exits += 1
                exit_px = soxl.values[i]
            invested, last_trig = False, i
        elif not invested:
            if reentry == "ndays" and i - last_trig >= n_days:
                invested = True
            elif reentry == "sma10" and soxl.values[i] > sma10[i]:
                invested = True
            elif reentry == "above_exit" and soxl.values[i] > exit_px:
                invested = True
    s = pd.Series(out, index=idx)
    return s, exits, days_in / (len(idx) - 1)


def metrics(s):
    eq = (1 + s).cumprod()
    yrs = (len(s) - 1) / 252
    dd = (eq / eq.cummax() - 1).min() * 100
    vol = s.std() * np.sqrt(252) * 100
    sharpe = (s.mean() - CASH_D) / s.std() * np.sqrt(252)
    cagr = (eq.iloc[-1] ** (1 / yrs) - 1) * 100
    return dict(total=(eq.iloc[-1] - 1) * 100, cagr=cagr, maxdd=dd, vol=vol, sharpe=sharpe,
                calmar=cagr / abs(dd))


