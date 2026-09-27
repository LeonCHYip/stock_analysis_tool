"""Panel construction: prices, earnings, and the derived valuation series.

Two share-basis conventions matter here and are easy to get wrong:

* `price_history.close` is the **raw, unadjusted** close (the fetchers call
  `yf.download(..., auto_adjust=False)`), while `adj_close` is adjusted for
  both splits and dividends.
* Finviz `eps_act` is **as reported at the time**, so it is on the share basis
  that was in force on the report date.

NVDA and AVGO both split 10:1 inside the study window, so a trailing-EPS sum
that mixes pre- and post-split quarters is meaningless. Everything valuation
related is therefore restated onto the *current* share basis: prices are
divided by the cumulative split factor still to come as of that date, and each
quarter's EPS is divided by the factor as of its report date.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from .regimes import Regime

# A split shows up as a large divergence between the raw and adjusted close on
# a single day. Dividends move the two apart by well under 30%, so this
# threshold separates the two cleanly without needing a corporate-action feed.
_SPLIT_LO, _SPLIT_HI = 0.77, 1.3

# R2_SHOCK spans only four trading sessions, so the per-regime minimum has to
# be lower than a week.
MIN_REGIME_DAYS = 3


# ── Prices ─────────────────────────────────────────────────────────────────────

def load_prices(con, tickers: list[str], start: str, end: str) -> pd.DataFrame:
    """Daily OHLCV for `tickers` between `start` and `end` (inclusive)."""
    if not tickers:
        return pd.DataFrame(
            columns=["ticker", "date", "close", "adj_close", "volume"]
        )
    df = con.execute(
        """
        SELECT ticker, date, close, adj_close, volume
        FROM price_history
        WHERE ticker IN (SELECT * FROM UNNEST(?))
          AND date BETWEEN ? AND ?
        ORDER BY ticker, date
        """,
        [tickers, start, end],
    ).df()
    df["date"] = pd.to_datetime(df["date"]).astype("datetime64[ns]")
    return df


def add_share_basis(px: pd.DataFrame) -> pd.DataFrame:
    """Add `cumsplit` and the two price series the study uses.

    cumsplit(d) = product of every split ratio taking effect *after* d, so a
    price on the current share basis is `close(d) / cumsplit(d)` and cumsplit
    is 1.0 at the end of the sample.

    Adds:
      cumsplit  — cumulative future split factor
      px_split  — close restated to the current share basis (keeps dividends
                  in the price, so it pairs correctly with as-reported EPS)
      px_total  — total-return price (adj_close, falling back to close)
    """
    out = []
    for ticker, g in px.groupby("ticker", sort=False):
        g = g.sort_values("date").copy()

        prev_c = g["close"].shift(1)
        prev_a = g["adj_close"].shift(1)
        with np.errstate(divide="ignore", invalid="ignore"):
            ratio = (prev_c / g["close"]) / (prev_a / g["adj_close"])
        ratio = ratio.replace([np.inf, -np.inf], np.nan)

        split = pd.Series(1.0, index=g.index)
        is_split = (ratio > _SPLIT_HI) | (ratio < _SPLIT_LO)
        split[is_split.fillna(False)] = ratio[is_split.fillna(False)]

        # product of splits strictly after each date
        rev_cum = split[::-1].cumprod()[::-1]
        g["cumsplit"] = rev_cum.shift(-1).fillna(1.0)

        g["px_split"] = g["close"] / g["cumsplit"]
        g["px_total"] = g["adj_close"].where(g["adj_close"].notna(), g["close"])
        out.append(g)

    return pd.concat(out, ignore_index=True) if out else px.assign(
        cumsplit=1.0, px_split=np.nan, px_total=np.nan
    )


def cumsplit_asof(px: pd.DataFrame, when: pd.DataFrame) -> np.ndarray:
    """cumsplit for each (ticker, date) row of `when`, as of that date.

    Returns an array in `when`'s own row order. `merge_asof` requires its left
    frame sorted by the join key, which is not the order the caller holds its
    rows in, so the original position is carried through and restored -- a
    plain positional assignment of the merged result would scatter each
    ticker's split factors onto other tickers' rows.

    Uses `merge_asof` rather than a per-row lookup so this stays linear in the
    number of price rows instead of rescanning the price frame per report.
    """
    left = when[["ticker", "date"]].copy()
    left["_ord"] = np.arange(len(left))
    left = left.sort_values("date")
    right = px.sort_values("date")[["ticker", "date", "cumsplit"]]

    merged = pd.merge_asof(
        left, right, on="date", by="ticker", direction="backward"
    )
    # A report older than the first price row has no backward match. Falling
    # back to 1.0 there would silently leave pre-split EPS unrestated, so use
    # the oldest cumsplit known for that ticker instead.
    oldest = px.sort_values("date").groupby("ticker")["cumsplit"].first()
    merged["cumsplit"] = (
        merged["cumsplit"].fillna(merged["ticker"].map(oldest)).fillna(1.0)
    )
    return merged.sort_values("_ord")["cumsplit"].to_numpy()


# ── Earnings ───────────────────────────────────────────────────────────────────

def load_earnings(con, tickers: list[str], start: str, end: str) -> pd.DataFrame:
    """Finviz earnings rows for `tickers` with report date in the window."""
    if not tickers:
        return pd.DataFrame()
    df = con.execute(
        """
        SELECT ticker, earnings_date, earnings_time,
               eps_est, eps_act, eps_sur,
               eps_gaap_est, eps_gaap_act, eps_gaap_sur,
               rev_est_m, rev_act_m, rev_sur,
               one_day_change
        FROM earnings_history
        WHERE ticker IN (SELECT * FROM UNNEST(?))
          AND earnings_date BETWEEN ? AND ?
        ORDER BY ticker, earnings_date
        """,
        [tickers, start, end],
    ).df()
    df["earnings_date"] = pd.to_datetime(df["earnings_date"]).astype("datetime64[ns]")
    return df


def _yoy(cur: pd.Series, prior: pd.Series) -> pd.Series:
    """Percent change, undefined where the prior base is non-positive.

    Growth off a negative or zero base is not interpretable, so it is dropped
    rather than reported as a misleading number.
    """
    base = prior.where(prior > 0)
    return (cur - base) / base * 100.0


def add_yoy(earn: pd.DataFrame) -> pd.DataFrame:
    """Add `rev_yoy` / `eps_yoy` as lag-4 growth of the reported actuals.

    The stored `q_rev_yoy` / `q_eps_yoy` columns are only populated for recent
    rows, so YoY is recomputed here from the backfilled actuals. A lag-4 pair
    is only accepted when the two reports are 300–430 days apart, which guards
    against gaps and reporting-schedule changes.
    """
    out = []
    for _, g in earn.groupby("ticker", sort=False):
        g = g.sort_values("earnings_date").copy()
        gap = (g["earnings_date"] - g["earnings_date"].shift(4)).dt.days
        valid = gap.between(300, 430)

        # Revenue is a company-level total and so is split-neutral; EPS is
        # per share, and must use the restated series or a split reads as a
        # ~90% earnings collapse.
        eps = g["eps_cur"] if "eps_cur" in g.columns else g["eps_act"]
        g["rev_yoy"] = _yoy(g["rev_act_m"], g["rev_act_m"].shift(4)).where(valid)
        g["eps_yoy"] = _yoy(eps, eps.shift(4)).where(valid)
        out.append(g)
    return pd.concat(out, ignore_index=True) if out else earn


def add_ttm_eps(earn: pd.DataFrame, px: pd.DataFrame,
                eps_col: str = "eps_act") -> pd.DataFrame:
    """Add `eps_cur` (current share basis) and `ttm_eps` (trailing 4 quarters).

    Each quarter's as-reported EPS is divided by the split factor outstanding
    as of its report date, so the four quarters in a TTM sum stay comparable
    across a split.
    """
    if earn.empty:
        return earn

    e = earn.sort_values(["ticker", "earnings_date"]).copy()
    e["date"] = e["earnings_date"]
    e["cumsplit_at_report"] = cumsplit_asof(px, e)
    e = e.drop(columns=["date"])
    e["eps_cur"] = e[eps_col] / e["cumsplit_at_report"]
    e["ttm_eps"] = (
        e.groupby("ticker")["eps_cur"]
        .transform(lambda s: s.rolling(4, min_periods=4).sum())
    )
    return e


def enrich_earnings(earn: pd.DataFrame, px: pd.DataFrame,
                    eps_col: str = "eps_act") -> pd.DataFrame:
    """Add the restated-EPS, TTM and YoY columns in the one order that works.

    `add_yoy` needs the split-restated `eps_cur` that `add_ttm_eps` creates, so
    calling them the other way round silently reports a split as an earnings
    collapse. Prefer this over calling the two directly.

    `eps_col` selects the earnings basis: "eps_act" is the non-GAAP figure the
    estimates and surprises are stated on (so it keeps the whole study on one
    basis), while "eps_gaap_act" reproduces the GAAP P/E that data providers
    quote. Levels differ a lot for stocks with large addbacks; the regime
    *changes* the study relies on do not.
    """
    return add_yoy(add_ttm_eps(earn, px, eps_col=eps_col))


# ── Derived valuation ──────────────────────────────────────────────────────────

def trailing_pe(px: pd.DataFrame, earn: pd.DataFrame) -> pd.DataFrame:
    """Point-in-time trailing P/E: `px_split` / most recent published TTM EPS.

    `merge_asof` attaches, for every trading day, the newest TTM EPS whose
    report date is on or before that day — so nothing uses a figure before it
    was public. Rows where TTM EPS is non-positive get a null P/E, since a P/E
    on negative earnings is not meaningful.
    """
    have = earn.dropna(subset=["ttm_eps"])[["ticker", "earnings_date", "ttm_eps"]]
    if have.empty or px.empty:
        return px.assign(ttm_eps=np.nan, pe=np.nan)

    left  = px.sort_values("date")
    right = have.sort_values("earnings_date")

    merged = pd.merge_asof(
        left,
        right,
        left_on="date",
        right_on="earnings_date",
        by="ticker",
        direction="backward",
        # Reports are quarterly, so a TTM figure more than ~2 quarters old
        # means the company stopped reporting. Without this a lapsed or
        # delisted ticker would carry a stale P/E forward indefinitely.
        tolerance=pd.Timedelta("200D"),
    )
    merged["pe"] = merged["px_split"] / merged["ttm_eps"].where(
        merged["ttm_eps"] > 0
    )
    return merged


# ── Regime slicing ─────────────────────────────────────────────────────────────

def _max_drawdown(series: pd.Series) -> float:
    running = series.cummax()
    return float(((series / running) - 1.0).min() * 100.0)


def regime_price_stats(px: pd.DataFrame, regime: Regime) -> pd.DataFrame:
    """Per-ticker total return, max drawdown and annualised vol in a regime."""
    w = px[
        (px["date"] >= pd.Timestamp(regime.start))
        & (px["date"] <= pd.Timestamp(regime.end))
    ]
    recs = []
    for ticker, g in w.groupby("ticker", sort=False):
        g = g.sort_values("date")
        s = g["px_total"].dropna()
        if len(s) < MIN_REGIME_DAYS:
            continue
        rets = s.pct_change().dropna()
        recs.append(
            {
                "ticker":     ticker,
                "regime":     regime.name,
                "n_days":     len(s),
                "ret_pct":    (s.iloc[-1] / s.iloc[0] - 1.0) * 100.0,
                "max_dd_pct": _max_drawdown(s),
                "ann_vol_pct": rets.std() * np.sqrt(252) * 100.0,
            }
        )
    return pd.DataFrame(recs)


def regime_pe_change(pe_px: pd.DataFrame, regime: Regime) -> pd.DataFrame:
    """Decompose a regime's price move into EPS growth and multiple change.

    In logs, price = P/E x TTM EPS, so the price move splits exactly into a
    re-rating term and an earnings term. That separation is the point of the
    whole valuation exercise: it says whether a fall was earnings going down
    or the market paying less for the same earnings.
    """
    w = pe_px[
        (pe_px["date"] >= pd.Timestamp(regime.start))
        & (pe_px["date"] <= pd.Timestamp(regime.end))
    ]
    recs = []
    for ticker, g in w.groupby("ticker", sort=False):
        g = g.sort_values("date").dropna(subset=["pe", "ttm_eps"])
        if len(g) < MIN_REGIME_DAYS:
            continue
        first, last = g.iloc[0], g.iloc[-1]
        if first["pe"] <= 0 or last["pe"] <= 0:
            continue
        recs.append(
            {
                "ticker":       ticker,
                "regime":       regime.name,
                "pe_start":     first["pe"],
                "pe_end":       last["pe"],
                "pe_chg_pct":   (last["pe"] / first["pe"] - 1.0) * 100.0,
                "ttm_eps_start": first["ttm_eps"],
                "ttm_eps_end":   last["ttm_eps"],
                "eps_growth_pct": (last["ttm_eps"] / first["ttm_eps"] - 1.0) * 100.0,
                "px_chg_pct":   (last["px_split"] / first["px_split"] - 1.0) * 100.0,
            }
        )
    return pd.DataFrame(recs)


def earnings_in_regime(earn: pd.DataFrame, regime: Regime) -> pd.DataFrame:
    """Reports whose earnings date falls inside the regime window."""
    return earn[
        (earn["earnings_date"] >= pd.Timestamp(regime.start))
        & (earn["earnings_date"] <= pd.Timestamp(regime.end))
    ].copy()
