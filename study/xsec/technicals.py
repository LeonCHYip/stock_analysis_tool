"""Technical / price-action features, computed from `price_history`.

`tech_indicators` only goes back to February 2026, so every technical measure
the study uses for 2024 and 2025 is recomputed here from raw daily bars. Work
is done on wide (date x ticker) matrices: a rolling mean over 2,300 tickers is
one vectorised call on a matrix rather than 2,300 grouped passes.

Two price series are carried:

* `px_total` — `adj_close`, with splits *and* dividends removed. Returns are
  measured on this and nothing else.
* `px_split` — `close` on the current share basis, so splits are out but
  dividends are still in. This is the series that pairs with reported EPS, and
  it is what the P/E work uses.

On the share basis: `price_history.close` was checked against NVDA's and
AVGO's 10:1 splits and is *already* back-adjusted for splits (yfinance
back-adjusts `Close` even under `auto_adjust=False`; only the dividend
adjustment is withheld). The `cumsplit` calculation below therefore returns
1.0 throughout the current sample. It is kept as a guard rather than removed:
if a future refetch ever stores a genuinely unadjusted close, it detects the
divergence and restates, instead of a 10:1 split silently entering the study
as a 90% loss.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from .config import MIN_WINDOW_DAYS, Regime

# An unadjusted split shows up as a large one-day divergence between the raw
# and adjusted close. Dividends move the two apart by well under 30%, so this
# band separates the two without needing a corporate-action feed.
_SPLIT_LO, _SPLIT_HI = 0.77, 1.3

# A window return is only comparable if the ticker actually traded through the
# window; below this share of the window's sessions the name is dropped.
MIN_COVERAGE = 0.80


def build_matrices(px: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Pivot the long price frame into the wide matrices everything else uses."""
    close = px.pivot(index="date", columns="ticker", values="close").sort_index()
    adj = px.pivot(index="date", columns="ticker", values="adj_close").sort_index()
    vol = px.pivot(index="date", columns="ticker", values="volume").sort_index()

    # adj_close is null on ~2% of rows; the raw close is the right stand-in,
    # since on those rows there is no adjustment information to apply.
    adj = adj.where(adj.notna(), close)

    ratio = (close.shift(1) / close) / (adj.shift(1) / adj)
    ratio = ratio.replace([np.inf, -np.inf], np.nan)
    is_split = (ratio > _SPLIT_HI) | (ratio < _SPLIT_LO)
    split = pd.DataFrame(1.0, index=close.index, columns=close.columns)
    split = split.mask(is_split.fillna(False), ratio)

    # cumsplit(d) = product of every split taking effect strictly after d, so
    # close(d) / cumsplit(d) is on today's share basis and cumsplit is 1.0 at
    # the end of the sample.
    cumsplit = split[::-1].cumprod()[::-1].shift(-1).fillna(1.0)

    return {
        "close": close,
        "adj_close": adj,
        "volume": vol,
        "cumsplit": cumsplit,
        "px_total": adj,
        "px_split": close / cumsplit,
    }


# ── Daily feature matrices ────────────────────────────────────────────────────

def _rsi(px: pd.DataFrame, period: int = 14) -> pd.DataFrame:
    """Wilder RSI, computed column-wise on the wide matrix."""
    delta = px.diff()
    gain = delta.clip(lower=0.0)
    loss = (-delta).clip(lower=0.0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0.0, np.nan)
    rsi = 100 - 100 / (1 + rs)
    # A window with no down days is RSI 100 by definition, not undefined.
    return rsi.where(avg_loss.ne(0) | avg_gain.isna(), 100.0)


def daily_features(mats: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """Per-day technical state, as one wide matrix per feature.

    These are the "technicals" the study tries to explain, and — measured at a
    window's open — the controls it conditions on.
    """
    px = mats["px_total"]
    vol = mats["volume"]
    rets = px.pct_change(fill_method=None)

    smas = {n: px.rolling(n, min_periods=int(n * 0.8)).mean() for n in
            (10, 20, 50, 150, 200)}

    # How many of the four MA inequalities of the T3 indicator hold (0-4).
    ma_align = sum(
        (smas[a] > smas[b]).where(smas[a].notna() & smas[b].notna())
        for a, b in ((10, 20), (20, 50), (50, 150), (150, 200))
    )

    high_252 = px.rolling(252, min_periods=120).max()
    avg_vol_21 = vol.rolling(21, min_periods=15).mean()
    avg_vol_63 = vol.rolling(63, min_periods=45).mean()

    return {
        "ret_1d": rets,
        "px_over_sma50": px / smas[50] - 1.0,
        "px_over_sma200": px / smas[200] - 1.0,
        "ma_align": ma_align.astype("float64"),
        "rsi14": _rsi(px),
        "vol_63d_ann": rets.rolling(63, min_periods=45).std() * np.sqrt(252),
        "dist_52w_high": px / high_252 - 1.0,
        # 12-1 momentum: the standard construction, skipping the most recent
        # month so short-term reversal does not contaminate the signal.
        "mom_12_1": px.shift(21) / px.shift(252) - 1.0,
        "mom_63d": px / px.shift(63) - 1.0,
        "vol_trend": avg_vol_21 / avg_vol_63 - 1.0,
    }


# ── Window aggregates ─────────────────────────────────────────────────────────

def _max_drawdown(w: pd.DataFrame) -> pd.Series:
    """Worst peak-to-trough fall inside the window, per ticker."""
    running = w.cummax()
    return (w / running - 1.0).min()


def window_stats(mats: dict[str, pd.DataFrame], window: Regime) -> pd.DataFrame:
    """Per-ticker price outcome over one window.

    Returns total return, worst drawdown, realised vol and the share of the
    window's sessions the name actually traded.
    """
    px = mats["px_total"]
    sl = px.loc[str(window.start): str(window.end)]
    if sl.empty:
        return pd.DataFrame()

    n_sessions = len(sl)
    obs = sl.notna().sum()
    # First and last *traded* price per ticker, ignoring leading/trailing gaps.
    first_px = sl.bfill().iloc[0]
    last_px = sl.ffill().iloc[-1]

    rets = sl.pct_change(fill_method=None)
    out = pd.DataFrame(
        {
            "n_days": obs,
            "coverage": obs / n_sessions,
            "ret_pct": (last_px / first_px - 1.0) * 100.0,
            "max_dd_pct": _max_drawdown(sl.ffill()) * 100.0,
            "ann_vol_pct": rets.std() * np.sqrt(252) * 100.0,
        }
    )
    out.index.name = "ticker"
    keep = (out["n_days"] >= MIN_WINDOW_DAYS) & (out["coverage"] >= MIN_COVERAGE)
    return out[keep].reset_index().assign(window=window.name)


def features_asof(feats: dict[str, pd.DataFrame], when: str,
                  names: list[str] | None = None) -> pd.DataFrame:
    """Technical state on the last session at or before `when`.

    Used to build the ex-ante controls for a window: everything here is known
    before the window's first return, so including it cannot leak the outcome.
    """
    names = names or list(feats)
    recs = {}
    for name in names:
        m = feats[name]
        sl = m.loc[: str(when)]
        recs[name] = sl.iloc[-1] if len(sl) else pd.Series(dtype="float64")
    out = pd.DataFrame(recs)
    out.index.name = "ticker"
    return out.reset_index()


def forward_returns(mats: dict[str, pd.DataFrame], events: pd.DataFrame,
                    horizons: tuple[int, ...]) -> pd.DataFrame:
    """Forward total returns from each event's anchor session.

    `events` needs `ticker` and `anchor_date`. The anchor is the session whose
    close already contains the announcement reaction, so a forward return
    measures drift *after* the jump rather than the jump itself.
    """
    px = mats["px_total"]
    idx = px.index.to_numpy()

    ev = events.copy()
    # Map each anchor onto its trading-session position. A date that is not a
    # session (a holiday, or an after-close report on a Friday) takes the next
    # session, which is the first close that can contain the reaction.
    anchors = pd.DatetimeIndex(ev["anchor_date"]).to_numpy()
    ev["_pos"] = np.searchsorted(idx, anchors, side="left")

    valid = (ev["_pos"] < len(idx)) & ev["ticker"].isin(px.columns)
    ev = ev[valid].copy()
    ev["_pos"] = ev["_pos"].astype(int)
    col = px.columns.get_indexer(ev["ticker"])
    values = px.to_numpy()

    base = values[ev["_pos"].to_numpy(), col]
    # A zero or missing anchor close cannot anchor a return.
    base = np.where(np.isfinite(base) & (base > 0), base, np.nan)
    ev["anchor_px"] = base
    for h in horizons:
        fwd_pos = np.minimum(ev["_pos"].to_numpy() + h, len(idx) - 1)
        # A horizon running past the end of the sample is not a short return,
        # it is an unobserved one.
        past_end = ev["_pos"].to_numpy() + h > len(idx) - 1
        fwd = values[fwd_pos, col]
        with np.errstate(divide="ignore", invalid="ignore"):
            r = (fwd / base - 1.0) * 100.0
        r[past_end] = np.nan
        ev[f"fwd_{h}d_pct"] = r
    return ev.drop(columns=["_pos"])
