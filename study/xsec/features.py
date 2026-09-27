"""Fundamental features derived from `earnings_history`.

Two things about the stored table drive the design here:

* `eps_sur` / `rev_sur` are populated back to 2023, so *surprise* is read
  straight off the table.
* `q_eps_yoy` / `q_rev_yoy` are only populated for rows fetched from 2026
  onward — they are empty across the whole study window — so *growth* is
  recomputed from the reported actuals as lag-4 changes.

On the share basis: Finviz restates EPS history onto the current basis rather
than leaving it as reported at the time — checked against NVDA (2023-02-22
`eps_act` 0.088, i.e. the post-10:1 figure, against $0.88 as reported) and
AVGO. The split restatement below is consequently a no-op on today's data. It
is kept because a trailing-four-quarter sum that mixes share bases is
meaningless, and the failure would be silent: a 10:1 split would read as a 90%
earnings collapse rather than as an error.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

# A lag-4 pair is only accepted when the two reports are roughly a year apart,
# which guards against gaps in the scrape and against fiscal-calendar changes.
_YOY_MIN_DAYS, _YOY_MAX_DAYS = 300, 430

# Quarters of history behind the standardised surprise. Eight is the usual
# choice: long enough for a stable dispersion estimate, short enough to track
# a company whose earnings volatility changes.
_SUE_WINDOW = 8


def _asof_matrix_lookup(mat: pd.DataFrame, tickers: pd.Series,
                        dates: pd.Series) -> np.ndarray:
    """Value of a wide (date x ticker) matrix as of each (ticker, date) pair.

    Returns an array in the caller's row order. Rows whose date precedes the
    matrix are given the oldest value for that ticker rather than a default of
    1.0 — for a split factor, defaulting would silently leave pre-split EPS
    unrestated.
    """
    idx = mat.index.to_numpy()
    want = pd.DatetimeIndex(dates).to_numpy()
    # Last session at or before each date; -1 means the date predates the matrix.
    row = np.searchsorted(idx, want, side="right") - 1

    col = mat.columns.get_indexer(tickers)
    values = mat.to_numpy()

    out = np.full(len(tickers), np.nan)
    ok = (row >= 0) & (col >= 0)
    out[ok] = values[row[ok], col[ok]]

    # Reports predating the price series: fall back to that ticker's oldest
    # known value.
    oldest = mat.bfill().iloc[0]
    fallback = tickers.map(oldest).to_numpy(dtype="float64")
    return np.where(np.isnan(out), fallback, out)


def _yoy(cur: pd.Series, prior: pd.Series) -> pd.Series:
    """Percent change, left undefined where the prior base is non-positive.

    Growth measured off a negative or zero base is not interpretable as a
    growth rate, so it is dropped rather than reported as a large number with
    an arbitrary sign.
    """
    base = prior.where(prior > 0)
    return (cur - base) / base * 100.0


def build(earn: pd.DataFrame, mats: dict[str, pd.DataFrame],
          eps_col: str = "eps_act") -> pd.DataFrame:
    """One row per report with every fundamental feature the study uses.

    `eps_col` chooses the earnings basis: `eps_act` is the non-GAAP figure the
    estimates and surprises are stated on (which keeps growth and surprise on
    one basis), `eps_gaap_act` reproduces the GAAP series data providers quote.
    """
    if earn.empty:
        return earn

    e = earn.sort_values(["ticker", "earnings_date"]).copy()

    e["cumsplit_at_report"] = _asof_matrix_lookup(
        mats["cumsplit"], e["ticker"], e["earnings_date"]
    )
    e["eps_cur"] = e[eps_col] / e["cumsplit_at_report"]
    e["eps_est_cur"] = e["eps_est"] / e["cumsplit_at_report"]

    g = e.groupby("ticker", sort=False)

    # Trailing twelve months, on the restated share basis.
    e["ttm_eps"] = g["eps_cur"].transform(
        lambda s: s.rolling(4, min_periods=4).sum()
    )
    # Revenue is a company-level total, so it is split-neutral as reported.
    e["ttm_rev"] = g["rev_act_m"].transform(
        lambda s: s.rolling(4, min_periods=4).sum()
    )

    gap = g["earnings_date"].transform(lambda s: (s - s.shift(4)).dt.days)
    valid = gap.between(_YOY_MIN_DAYS, _YOY_MAX_DAYS)

    e["rev_yoy"] = _yoy(e["rev_act_m"], g["rev_act_m"].shift(4)).where(valid)
    e["eps_yoy"] = _yoy(e["eps_cur"], g["eps_cur"].shift(4)).where(valid)
    e["ttm_rev_yoy"] = _yoy(e["ttm_rev"], g["ttm_rev"].shift(4)).where(valid)
    e["ttm_eps_yoy"] = _yoy(e["ttm_eps"], g["ttm_eps"].shift(4)).where(valid)

    # Acceleration: is growth itself getting better or worse? The rotation
    # story is largely about second derivatives, not levels.
    e["rev_yoy_accel"] = e["rev_yoy"] - g["rev_yoy"].shift(1)
    e["eps_yoy_accel"] = e["eps_yoy"] - g["eps_yoy"].shift(1)

    # Standardised unexpected earnings. The percentage surprise on the table
    # explodes when the estimate is near zero; scaling the raw miss by the
    # company's own history of misses is the standard fix and makes the number
    # comparable across the cross-section.
    e["eps_gap"] = e["eps_cur"] - e["eps_est_cur"]
    e["sue"] = e["eps_gap"] / g["eps_gap"].transform(
        lambda s: s.shift(1).rolling(_SUE_WINDOW, min_periods=4).std()
    ).replace(0.0, np.nan)

    e["eps_beat"] = (e["eps_sur"] > 0).where(e["eps_sur"].notna())
    e["rev_beat"] = (e["rev_sur"] > 0).where(e["rev_sur"].notna())
    both = e["eps_beat"].astype("boolean") & e["rev_beat"].astype("boolean")
    e["double_beat"] = both.astype("Float64").astype("float64")
    e["beat_streak"] = g["eps_beat"].transform(_streak)

    # The session whose close already contains the reaction: the report day
    # for a before-open release, the next session for an after-close one.
    e["anchor_date"] = e["earnings_date"] + pd.to_timedelta(
        (e["earnings_time"].fillna("BMO") == "AMC").astype(int), unit="D"
    )
    return e


def _streak(s: pd.Series) -> pd.Series:
    """Count of consecutive prior-and-current True values, per ticker."""
    b = s.astype("boolean").fillna(False).astype(int)
    grp = (b == 0).cumsum()
    return b.groupby(grp).cumsum()


# Everything a regression may use as an explanatory variable.
FUND_METRICS = [
    "eps_sur", "rev_sur", "sue",
    "rev_yoy", "eps_yoy", "ttm_rev_yoy", "ttm_eps_yoy",
    "rev_yoy_accel", "eps_yoy_accel", "beat_streak",
]

# The compact set used where a wide table would be unreadable.
CORE_METRICS = ["sue", "eps_sur", "rev_sur", "rev_yoy", "eps_yoy"]


def per_ticker_in_window(feat: pd.DataFrame, start: str, end: str,
                         metrics: list[str] | None = None) -> pd.DataFrame:
    """Median of each metric across the reports a ticker filed in a window.

    A median rather than the last report: a stock that reported twice inside a
    window was judged on both, and the median is not dragged around by one
    outlier quarter.
    """
    metrics = metrics or FUND_METRICS
    w = feat[
        (feat["earnings_date"] >= pd.Timestamp(start))
        & (feat["earnings_date"] <= pd.Timestamp(end))
    ]
    if w.empty:
        return pd.DataFrame(columns=["ticker", *metrics, "n_reports"])
    out = w.groupby("ticker")[metrics].median()
    out["n_reports"] = w.groupby("ticker").size()
    return out.reset_index()


def latest_before(feat: pd.DataFrame, when: str,
                  metrics: list[str] | None = None,
                  max_age_days: int = 120) -> pd.DataFrame:
    """Each ticker's most recent report strictly before `when`.

    This is the ex-ante view: only information the market already had when the
    window opened. `max_age_days` drops stocks whose last report is too stale
    to describe the business — roughly one missed quarter.
    """
    metrics = metrics or FUND_METRICS
    cutoff = pd.Timestamp(when)
    w = feat[feat["earnings_date"] < cutoff]
    if w.empty:
        return pd.DataFrame(columns=["ticker", *metrics, "report_age_days"])
    last = w.sort_values("earnings_date").groupby("ticker").tail(1).copy()
    last["report_age_days"] = (cutoff - last["earnings_date"]).dt.days
    last = last[last["report_age_days"] <= max_age_days]
    return last[["ticker", *metrics, "report_age_days"]].reset_index(drop=True)
