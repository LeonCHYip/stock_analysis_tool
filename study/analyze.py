"""The five analyses.

Everything is reported per (group, regime) so the semiconductor pass and the
later sector passes produce identically shaped tables.

Surprise percentages have very fat tails — a company earning a cent against a
zero estimate produces a four-digit "surprise" — so aggregates use medians and
the regressions winsorise their inputs. `scipy` is not a dependency of this
project, so the Spearman correlation comes from pandas and the t-statistics
are computed directly.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from . import panel
from .regimes import REGIMES, Regime

FUND_METRICS = ["eps_sur", "rev_sur", "rev_yoy", "eps_yoy"]


def winsorise(s: pd.Series, lo: float = 0.01, hi: float = 0.99) -> pd.Series:
    s = pd.to_numeric(s, errors="coerce")
    if s.notna().sum() < 5:
        return s
    return s.clip(s.quantile(lo), s.quantile(hi))


def _ols(x: pd.Series, y: pd.Series) -> dict:
    """Slope of y on x with its t-statistic. NaN-safe, no scipy."""
    d = pd.DataFrame({"x": x, "y": y}).dropna()
    n = len(d)
    if n < 5 or d["x"].std() == 0:
        return {"n": n, "slope": np.nan, "intercept": np.nan,
                "r": np.nan, "t_stat": np.nan}
    slope, intercept = np.polyfit(d["x"], d["y"], 1)
    fitted = slope * d["x"] + intercept
    resid  = d["y"] - fitted
    # standard error of the slope
    ss_x = ((d["x"] - d["x"].mean()) ** 2).sum()
    se   = np.sqrt((resid**2).sum() / (n - 2) / ss_x) if ss_x > 0 else np.nan
    return {
        "n": n,
        "slope": float(slope),
        "intercept": float(intercept),
        "r": float(d["x"].corr(d["y"])),
        "t_stat": float(slope / se) if se and se > 0 else np.nan,
    }


def _spearman(x: pd.Series, y: pd.Series) -> dict:
    d = pd.DataFrame({"x": x, "y": y}).dropna()
    n = len(d)
    if n < 5:
        return {"n": n, "rho": np.nan, "t_stat": np.nan}
    # pandas delegates method="spearman" to scipy, which this project does
    # not depend on. Spearman is just Pearson on the ranks.
    rho = d["x"].rank().corr(d["y"].rank())
    if pd.isna(rho) or abs(rho) >= 1:
        return {"n": n, "rho": float(rho) if pd.notna(rho) else np.nan,
                "t_stat": np.nan}
    t = rho * np.sqrt((n - 2) / (1 - rho**2))
    return {"n": n, "rho": float(rho), "t_stat": float(t)}


# ── 1. Return decomposition ────────────────────────────────────────────────────

def returns_by_group(
    px_by_group: dict[str, pd.DataFrame]
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Per-ticker regime stats, and the group-level aggregate."""
    per_ticker, agg = [], []
    for group, px in px_by_group.items():
        for reg in REGIMES:
            st = panel.regime_price_stats(px, reg)
            if st.empty:
                continue
            st["group"] = group
            per_ticker.append(st)
            agg.append(
                {
                    "group": group,
                    "regime": reg.name,
                    "label": reg.label,
                    "n_stocks": len(st),
                    "median_ret_pct": st["ret_pct"].median(),
                    "mean_ret_pct": st["ret_pct"].mean(),
                    "pct_positive": (st["ret_pct"] > 0).mean() * 100.0,
                    "median_max_dd_pct": st["max_dd_pct"].median(),
                    "median_ann_vol_pct": st["ann_vol_pct"].median(),
                }
            )
    pt = pd.concat(per_ticker, ignore_index=True) if per_ticker else pd.DataFrame()
    return pt, pd.DataFrame(agg)


def return_spreads(agg: pd.DataFrame, focus: str) -> pd.DataFrame:
    """`focus` group's median return minus every other group's, per regime."""
    recs = []
    for reg in REGIMES:
        r = agg[agg["regime"] == reg.name].set_index("group")["median_ret_pct"]
        if focus not in r.index:
            continue
        for other in r.index:
            if other == focus:
                continue
            recs.append(
                {
                    "regime": reg.name,
                    "label": reg.label,
                    "focus": focus,
                    "vs": other,
                    "focus_ret_pct": r[focus],
                    "other_ret_pct": r[other],
                    "spread_pct": r[focus] - r[other],
                }
            )
    return pd.DataFrame(recs)


# ── 2. Fundamental delivery ────────────────────────────────────────────────────

def delivery_by_group(earn_by_group: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Beat rates and growth of reported actuals, per group per regime."""
    recs = []
    for group, earn in earn_by_group.items():
        for reg in REGIMES:
            w = panel.earnings_in_regime(earn, reg)
            if w.empty:
                continue
            recs.append(
                {
                    "group": group,
                    "regime": reg.name,
                    "label": reg.label,
                    "n_reports": len(w),
                    "n_stocks": w["ticker"].nunique(),
                    "eps_beat_rate_pct": (w["eps_sur"] > 0).mean() * 100.0,
                    "rev_beat_rate_pct": (w["rev_sur"] > 0).mean() * 100.0,
                    "median_eps_sur_pct": w["eps_sur"].median(),
                    "median_rev_sur_pct": w["rev_sur"].median(),
                    "median_rev_yoy_pct": w["rev_yoy"].median(),
                    "median_eps_yoy_pct": w["eps_yoy"].median(),
                    "median_1d_reaction_pct": w["one_day_change"].median(),
                }
            )
    return pd.DataFrame(recs)


# ── 3. Did fundamentals explain cross-sectional returns? ───────────────────────

def cross_section(
    px_by_group: dict[str, pd.DataFrame],
    earn_by_group: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """Rank correlation of regime return against each fundamental metric.

    Per regime each stock is reduced to one observation: its return over the
    window, and the median of each fundamental metric across the reports it
    filed inside that window.
    """
    recs = []
    for group in px_by_group:
        px, earn = px_by_group[group], earn_by_group[group]
        for reg in REGIMES:
            rets = panel.regime_price_stats(px, reg)
            w = panel.earnings_in_regime(earn, reg)
            if rets.empty or w.empty:
                continue
            fund = w.groupby("ticker")[FUND_METRICS].median()
            d = rets.set_index("ticker")[["ret_pct"]].join(fund, how="inner")
            for metric in FUND_METRICS:
                res = _spearman(winsorise(d[metric]), d["ret_pct"])
                recs.append(
                    {
                        "group": group,
                        "regime": reg.name,
                        "label": reg.label,
                        "metric": metric,
                        **res,
                    }
                )
    return pd.DataFrame(recs)


# ── 4. Earnings-day reaction ───────────────────────────────────────────────────

def reaction_slopes(earn_by_group: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """How much a point of surprise moved the stock on the day it reported.

    A slope that collapses toward zero means beats stopped being paid for —
    the clearest single sign that the market had stopped trading the sector on
    its fundamentals.
    """
    recs = []
    for group, earn in earn_by_group.items():
        for reg in REGIMES:
            w = panel.earnings_in_regime(earn, reg)
            if w.empty:
                continue
            for metric in ["eps_sur", "rev_sur"]:
                res = _ols(winsorise(w[metric]), winsorise(w["one_day_change"]))
                recs.append(
                    {
                        "group": group,
                        "regime": reg.name,
                        "label": reg.label,
                        "metric": metric,
                        **res,
                    }
                )
    return pd.DataFrame(recs)


# ── 5. Growth vs multiple ──────────────────────────────────────────────────────

def pe_decomposition(
    pe_by_group: dict[str, pd.DataFrame]
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Per-ticker and group-median split of price moves into P/E and EPS."""
    per_ticker, agg = [], []
    for group, pe_px in pe_by_group.items():
        for reg in REGIMES:
            d = panel.regime_pe_change(pe_px, reg)
            if d.empty:
                continue
            d["group"] = group
            per_ticker.append(d)
            agg.append(
                {
                    "group": group,
                    "regime": reg.name,
                    "label": reg.label,
                    "n_stocks": len(d),
                    "median_pe_start": d["pe_start"].median(),
                    "median_pe_end": d["pe_end"].median(),
                    "median_pe_chg_pct": d["pe_chg_pct"].median(),
                    "median_eps_growth_pct": d["eps_growth_pct"].median(),
                    "median_px_chg_pct": d["px_chg_pct"].median(),
                }
            )
    pt = pd.concat(per_ticker, ignore_index=True) if per_ticker else pd.DataFrame()
    return pt, pd.DataFrame(agg)
