"""Technical feature engine for the extended MU earnings study.

Everything is computed from daily bars only, so the same function serves both the
historical panel (features at each print's day-T close) and the live prediction.
Day T = last close before the news: the report day for AMC prints, the prior day for BMO.
"""
import numpy as np, pandas as pd

HERE = __file__.rsplit("/", 1)[0]


def load_prices(path=f"{HERE}/prices_long.csv"):
    px = pd.read_csv(path, parse_dates=["date"])
    return {t: g.sort_values("date").set_index("date") for t, g in px.groupby("ticker")}


def _rsi(c, n):
    d = c.diff()
    up = d.clip(lower=0).ewm(alpha=1 / n, adjust=False).mean()
    dn = (-d.clip(upper=0)).ewm(alpha=1 / n, adjust=False).mean()
    return 100 - 100 / (1 + up / dn)


def _atr(s, n=14):
    pc = s.close.shift()
    tr = pd.concat([s.high - s.low, (s.high - pc).abs(), (s.low - pc).abs()], axis=1).max(axis=1)
    return tr.ewm(alpha=1 / n, adjust=False).mean()


def _adx(s, n=14):
    up, dn = s.high.diff(), -s.low.diff()
    pdm = np.where((up > dn) & (up > 0), up, 0.0)
    ndm = np.where((dn > up) & (dn > 0), dn, 0.0)
    atr = _atr(s, n)
    pdi = 100 * pd.Series(pdm, s.index).ewm(alpha=1 / n, adjust=False).mean() / atr
    ndi = 100 * pd.Series(ndm, s.index).ewm(alpha=1 / n, adjust=False).mean() / atr
    dx = 100 * (pdi - ndi).abs() / (pdi + ndi)
    return dx.ewm(alpha=1 / n, adjust=False).mean(), pdi - ndi


def indicator_frame(s):
    """Per-bar indicator table for one ticker (all columns known at that bar's close)."""
    c, v = s.close, s.volume.replace(0, np.nan)
    r = c.pct_change()
    f = pd.DataFrame(index=s.index)
    for n in (1, 3, 5, 10, 21, 63, 126, 252):
        f[f"ret_{n}d"] = (c / c.shift(n) - 1) * 100
    atr = _atr(s)
    f["atrp"] = atr / c * 100
    f["atr_ratio"] = atr / _atr(s, 63)                    # short vs long ATR: vol expanding?
    for n in (10, 20, 60):
        f[f"rvol{n}"] = r.rolling(n).std() * np.sqrt(252) * 100
    f["rvol_ratio"] = f.rvol10 / f.rvol60
    # vol-normalised run-ins: how many "normal days" of movement the run-in represents
    f["ret_5d_z"] = f.ret_5d / (r.rolling(60).std() * 100 * np.sqrt(5))
    f["ret_21d_z"] = f.ret_21d / (r.rolling(60).std() * 100 * np.sqrt(21))
    for n in (2, 5, 14):
        f[f"rsi{n}"] = _rsi(c, n)
    lo14, hi14 = s.low.rolling(14).min(), s.high.rolling(14).max()
    f["stoch14"] = (c - lo14) / (hi14 - lo14) * 100
    m20, sd20 = c.rolling(20).mean(), c.rolling(20).std()
    f["bb_pctb"] = (c - (m20 - 2 * sd20)) / (4 * sd20)
    f["bb_width"] = 4 * sd20 / m20 * 100
    f["bb_width_rank"] = f.bb_width.rolling(252).rank(pct=True)   # squeeze vs own year
    smas = {n: c.rolling(n).mean() for n in (10, 20, 50, 100, 200)}
    for n, m in smas.items():
        f[f"vs_sma{n}"] = (c / m - 1) * 100
    f["sma50_slope"] = (smas[50] / smas[50].shift(20) - 1) * 100
    f["sma200_slope"] = (smas[200] / smas[200].shift(20) - 1) * 100
    order = [smas[n] for n in (10, 20, 50, 100, 200)]
    f["ma_stack"] = sum((order[k] > order[k + 1]).astype(int) for k in range(4))   # 0..4
    ema12, ema26 = c.ewm(span=12, adjust=False).mean(), c.ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    f["macd_hist_p"] = (macd - macd.ewm(span=9, adjust=False).mean()) / c * 100
    f["ppo"] = macd / ema26 * 100
    f["adx"], f["di_spread"] = _adx(s)
    hi252, lo252 = c.rolling(252).max(), c.rolling(252).min()
    f["vs_52wh"] = (c / hi252 - 1) * 100
    f["vs_52wl"] = (c / lo252 - 1) * 100
    f["days_since_52wh"] = c.rolling(252).apply(lambda a: len(a) - 1 - np.argmax(a), raw=True)
    # up/down streak ending at the bar
    sign = np.sign(r).fillna(0)
    grp = (sign != sign.shift()).cumsum()
    f["streak"] = sign.groupby(grp).cumsum()
    f["up_days_10"] = (r > 0).rolling(10).sum()
    # volume
    f["vol_5_50"] = v.rolling(5).mean() / v.rolling(50).mean()
    f["vol_21_63"] = v.rolling(21).mean() / v.shift(21).rolling(63).mean()
    obv = (np.sign(r).fillna(0) * v).cumsum()
    f["obv_21_z"] = (obv - obv.shift(21)) / v.rolling(63).mean() / 21     # net signed vol, avg-days
    mfm = ((c - s.low) - (s.high - c)) / (s.high - s.low).replace(0, np.nan)
    f["cmf20"] = (mfm * v).rolling(20).sum() / v.rolling(20).sum()
    upv = v.where(r > 0, 0).rolling(10).sum()
    f["upvol_share10"] = upv / v.rolling(10).sum()
    gap = (s.open / c.shift() - 1) * 100
    f["gaps_abs_21"] = gap.abs().rolling(21).mean()
    return f


CTX_COLS = ["ret_5d", "ret_21d", "ret_63d", "rsi14", "vs_sma50", "vs_sma200", "vs_52wh", "rvol20"]


def feature_table(P):
    """Joined per-date feature table: MU indicators + sector/market context + relative strength."""
    mu = indicator_frame(P["MU"])
    # sector proxy: SOXX where it exists, SMH before its July-2001 launch
    sox = indicator_frame(P["SOXX"]).reindex(mu.index)
    smh = indicator_frame(P["SMH"]).reindex(mu.index)
    sec = sox.where(sox.ret_63d.notna(), smh)
    qqq = indicator_frame(P["QQQ"]).reindex(mu.index)
    spy = indicator_frame(P["SPY"]).reindex(mu.index)
    vix = P["VIX"].close.reindex(mu.index)
    X = mu.copy()
    for col in CTX_COLS:
        X[f"sox_{col}"] = sec[col]
        X[f"qqq_{col}"] = qqq[col]
    X["spy_ret_21d"] = spy.ret_21d
    X["spy_vs_sma200"] = spy.vs_sma200
    for n in (5, 21, 63):
        X[f"rs_{n}d"] = mu[f"ret_{n}d"] - sec[f"ret_{n}d"]          # MU minus sector
    X["vix"] = vix
    X["vix_chg_5d"] = (vix / vix.shift(5) - 1) * 100
    X["vix_vs_50"] = (vix / vix.rolling(50).mean() - 1) * 100
    return X
