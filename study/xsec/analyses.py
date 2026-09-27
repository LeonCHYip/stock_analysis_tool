"""The analyses. Each returns a tidy DataFrame that `run.py` writes to CSV.

The through-line: take the price outcome the user calls "technicals", and ask
how much of its cross-sectional variation the earnings record explains, window
by window and sector by sector.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from . import features, stats, technicals
from .config import (
    ALL_WINDOWS,
    DRIFT_HORIZONS,
    MIN_GROUP_OBS,
    MIN_XSEC_OBS,
    STUDY_START,
)

# Regressors for the multivariate specifications, in two blocks.
#
# `eps_sur` is left out of both deliberately: it carries the same information
# as `sue` on a scale that explodes when the estimate is near zero.
#
# The split into core and full is a data-coverage decision, not a modelling
# one. A YoY figure is a lag-4 comparison and an acceleration is lag-5, so the
# full block needs a year more earnings history behind a window than the core
# block does. Reporting both keeps the earliest window estimable on the core
# block even where the full one has no sample.
FUND_CORE = ["sue", "rev_sur", "rev_yoy", "eps_yoy"]
FUND_FULL = FUND_CORE + ["rev_yoy_accel", "beat_streak"]
FUND_REGRESSORS = FUND_FULL

# Ex-ante technical / size controls, all measured at the window's open.
CONTROLS = ["log_mcap", "mom_12_1", "vol_63d_ann", "px_over_sma200"]

# The price-action outcomes the study tries to explain. Return is the headline,
# but "technical performance" is not only direction: drawdown and realised
# volatility are the risk side of the same question, and a fundamental record
# can explain one without explaining the others.
OUTCOMES = ["ret_pct", "max_dd_pct", "ann_vol_pct"]


# ── 1. What actually happened: returns by group and window ────────────────────

def group_returns(win_stats: dict[str, pd.DataFrame],
                  tags: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Per-ticker window outcomes, and the group medians drawn from them."""
    per_ticker, agg = [], []
    for w in ALL_WINDOWS:
        st = win_stats.get(w.name, pd.DataFrame())
        if st.empty:
            continue
        st = st.merge(tags[["ticker", "group", "sector", "market_cap"]], on="ticker",
                      how="left")
        st["label"] = w.label
        per_ticker.append(st)

        for group, g in st.groupby("group"):
            if len(g) < MIN_GROUP_OBS:
                continue
            agg.append(
                {
                    "window": w.name,
                    "label": w.label,
                    "group": group,
                    "n_stocks": len(g),
                    "median_ret_pct": g["ret_pct"].median(),
                    "mean_ret_pct": g["ret_pct"].mean(),
                    "pct_positive": (g["ret_pct"] > 0).mean() * 100.0,
                    "median_max_dd_pct": g["max_dd_pct"].median(),
                    "median_ann_vol_pct": g["ann_vol_pct"].median(),
                }
            )
        agg.append(
            {
                "window": w.name, "label": w.label, "group": "ALL",
                "n_stocks": len(st),
                "median_ret_pct": st["ret_pct"].median(),
                "mean_ret_pct": st["ret_pct"].mean(),
                "pct_positive": (st["ret_pct"] > 0).mean() * 100.0,
                "median_max_dd_pct": st["max_dd_pct"].median(),
                "median_ann_vol_pct": st["ann_vol_pct"].median(),
            }
        )

    pt = pd.concat(per_ticker, ignore_index=True) if per_ticker else pd.DataFrame()
    ag = pd.DataFrame(agg)
    if not ag.empty:
        # Excess over the equal-weighted universe median. There is no index in
        # `price_history`, and for a cross-sectional study the universe median
        # is the more relevant benchmark anyway.
        base = ag[ag["group"] == "ALL"].set_index("window")["median_ret_pct"]
        ag["excess_vs_universe_pct"] = ag["median_ret_pct"] - ag["window"].map(base)
    return pt, ag


# ── 2. What the companies delivered ───────────────────────────────────────────

def group_delivery(feat: pd.DataFrame, tags: pd.DataFrame) -> pd.DataFrame:
    """Reported growth, beat rates and announcement reactions, per group/window.

    Read against `group_returns`, this is the crux of the question: a group can
    show strong delivery here and a poor return there, and that gap is exactly
    what a rotation is.
    """
    f = feat.merge(tags[["ticker", "group"]], on="ticker", how="left")
    recs = []
    for w in ALL_WINDOWS:
        win = f[
            (f["earnings_date"] >= pd.Timestamp(w.start))
            & (f["earnings_date"] <= pd.Timestamp(w.end))
        ]
        if win.empty:
            continue
        for group, g in list(win.groupby("group")) + [("ALL", win)]:
            if len(g) < MIN_GROUP_OBS:
                continue
            recs.append(
                {
                    "window": w.name, "label": w.label, "group": group,
                    "n_reports": len(g), "n_stocks": g["ticker"].nunique(),
                    "eps_beat_rate_pct": g["eps_beat"].mean() * 100.0,
                    "rev_beat_rate_pct": g["rev_beat"].mean() * 100.0,
                    "double_beat_rate_pct": g["double_beat"].mean() * 100.0,
                    "median_eps_sur_pct": g["eps_sur"].median(),
                    "median_rev_sur_pct": g["rev_sur"].median(),
                    "median_sue": g["sue"].median(),
                    "median_rev_yoy_pct": g["rev_yoy"].median(),
                    "median_eps_yoy_pct": g["eps_yoy"].median(),
                    "median_rev_yoy_accel_pp": g["rev_yoy_accel"].median(),
                    "median_1d_reaction_pct": g["one_day_change"].median(),
                }
            )
    return pd.DataFrame(recs)


# ── 3. How much of the cross-section do fundamentals explain? ─────────────────

def _design(rets: pd.DataFrame, tags: pd.DataFrame, fund: pd.DataFrame,
            ctrl: pd.DataFrame) -> pd.DataFrame:
    """Join outcome, sector, fundamentals and controls into one design frame."""
    d = (
        rets[["ticker", *OUTCOMES]]
        .merge(tags[["ticker", "group", "market_cap"]], on="ticker", how="left")
        .merge(fund, on="ticker", how="left")
        .merge(ctrl, on="ticker", how="left")
    )
    d["log_mcap"] = np.log(d["market_cap"].where(d["market_cap"] > 0))
    for c in FUND_REGRESSORS + CONTROLS:
        if c in d.columns:
            d[f"z_{c}"] = stats.zscore(d[c])
    return d


def explanatory_power(win_stats: dict[str, pd.DataFrame], feats_daily,
                      feat: pd.DataFrame,
                      tags: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Nested regressions of window return on sector, controls, fundamentals.

    Two information sets are run for every window:

    * `exante`   — the last report filed *before* the window opened, so the
                   regression only uses what the market already knew. This is
                   the honest predictive test.
    * `contemp`  — the reports filed *during* the window. This cannot forecast
                   anything, but it answers a different and equally live
                   question: did prices move with the news as it landed?

    The number that matters is the incremental R^2 of the fundamental block
    over sector and controls — the share of cross-sectional return variation
    that the earnings record explains and sector membership does not.
    """
    fit_rows, coef_rows = [], []

    for w in ALL_WINDOWS:
        rets = win_stats.get(w.name, pd.DataFrame())
        if rets.empty:
            continue
        ctrl = technicals.features_asof(
            feats_daily, w.start, ["mom_12_1", "vol_63d_ann", "px_over_sma200"]
        )
        views = {
            "exante": features.latest_before(feat, w.start, FUND_REGRESSORS),
            "contemp": features.per_ticker_in_window(feat, w.start, w.end,
                                                     FUND_REGRESSORS),
        }
        for basis, fund in views.items():
            if fund.empty:
                continue
            d = _design(rets, tags, fund, ctrl)
            zc = [f"z_{c}" for c in CONTROLS]
            sec = stats.dummies(d["group"], "sec")
            blocks = {
                "core": [f"z_{c}" for c in FUND_CORE],
                "full": [f"z_{c}" for c in FUND_FULL],
            }

            for outcome in OUTCOMES:
                y = stats.winsorise(d[outcome])
                specs = {
                    "controls": d[zc],
                    "sector": sec,
                    "sector+controls": pd.concat([sec, d[zc]], axis=1),
                }
                for block, zf in blocks.items():
                    specs[f"fund_{block}"] = d[zf]
                    specs[f"sector+controls+fund_{block}"] = pd.concat(
                        [sec, d[zc], d[zf]], axis=1
                    )

                fitted = {}
                for spec_name, X in specs.items():
                    res = stats.ols(y, X)
                    fitted[spec_name] = res
                    if res["n"] < MIN_XSEC_OBS:
                        continue
                    fit_rows.append(
                        {
                            "window": w.name, "label": w.label, "basis": basis,
                            "outcome": outcome, "spec": spec_name, "n": res["n"],
                            "r2": res["r2"], "adj_r2": res["adj_r2"],
                        }
                    )
                    if (spec_name == "sector+controls+fund_core"
                            and outcome == "ret_pct"
                            and not res["coefs"].empty):
                        c = res["coefs"].copy()
                        c.insert(0, "basis", basis)
                        c.insert(0, "label", w.label)
                        c.insert(0, "window", w.name)
                        coef_rows.append(c)

                # The incremental R^2 is only meaningful against a base fitted
                # on the same rows, so it is recomputed on the fundamental
                # block's own sample rather than differenced across samples.
                for block, zf in blocks.items():
                    full = fitted.get(f"sector+controls+fund_{block}", {})
                    if not np.isfinite(full.get("r2", np.nan)):
                        continue
                    same_rows = pd.concat([sec, d[zc], d[zf]], axis=1).dropna().index
                    base = stats.ols(y.loc[same_rows],
                                     pd.concat([sec, d[zc]], axis=1).loc[same_rows])
                    if not np.isfinite(base.get("r2", np.nan)):
                        continue
                    fit_rows.append(
                        {
                            "window": w.name, "label": w.label, "basis": basis,
                            "outcome": outcome,
                            "spec": f"INCREMENTAL_fund_{block}", "n": full["n"],
                            "r2": full["r2"] - base["r2"],
                            "adj_r2": full["adj_r2"] - base["adj_r2"],
                        }
                    )

    fits = pd.DataFrame(fit_rows)
    coefs = pd.concat(coef_rows, ignore_index=True) if coef_rows else pd.DataFrame()
    return fits, coefs


# ── 4. Univariate rank correlations, overall and within sector ────────────────

def rank_correlations(win_stats: dict[str, pd.DataFrame], feat: pd.DataFrame,
                      tags: pd.DataFrame) -> pd.DataFrame:
    """Spearman rho of window return against each fundamental metric.

    Reported for the whole universe and within each group, because a
    correlation that only exists across sectors is a sector bet, not a
    fundamental one.
    """
    recs = []
    for w in ALL_WINDOWS:
        rets = win_stats.get(w.name, pd.DataFrame())
        if rets.empty:
            continue
        for basis, fund in (
            ("exante", features.latest_before(feat, w.start)),
            ("contemp", features.per_ticker_in_window(feat, w.start, w.end)),
        ):
            if fund.empty:
                continue
            d = (
                rets[["ticker", "ret_pct"]]
                .merge(fund, on="ticker", how="inner")
                .merge(tags[["ticker", "group"]], on="ticker", how="left")
            )
            for group, g in list(d.groupby("group")) + [("ALL", d)]:
                if len(g) < MIN_XSEC_OBS:
                    continue
                for metric in features.FUND_METRICS:
                    res = stats.spearman(g[metric], g["ret_pct"])
                    recs.append(
                        {
                            "window": w.name, "label": w.label, "basis": basis,
                            "group": group, "metric": metric, **res,
                        }
                    )
    return pd.DataFrame(recs)


# ── 5. Was a beat paid for? Announcement reaction and drift ───────────────────

def reaction_and_drift(events: pd.DataFrame, tags: pd.DataFrame) -> pd.DataFrame:
    """Slope of the announcement-day move on the surprise, per group/window.

    A slope near zero means the market stopped paying for beats — the single
    clearest signature of a sector being traded on something other than its
    own numbers.
    """
    ev = events.merge(tags[["ticker", "group"]], on="ticker", how="left")
    recs = []
    for w in ALL_WINDOWS:
        win = ev[
            (ev["earnings_date"] >= pd.Timestamp(w.start))
            & (ev["earnings_date"] <= pd.Timestamp(w.end))
        ]
        if win.empty:
            continue
        for group, g in list(win.groupby("group")) + [("ALL", win)]:
            if len(g) < MIN_GROUP_OBS * 2:
                continue
            for signal in ("sue", "eps_sur", "rev_sur"):
                for outcome in ["one_day_change"] + [
                    f"fwd_{h}d_pct" for h in DRIFT_HORIZONS
                ]:
                    if outcome not in g.columns:
                        continue
                    X = pd.DataFrame({signal: stats.zscore(g[signal])})
                    res = stats.ols(stats.winsorise(g[outcome]), X)
                    if res["coefs"].empty:
                        continue
                    row = res["coefs"].set_index("term")
                    if signal not in row.index:
                        continue
                    recs.append(
                        {
                            "window": w.name, "label": w.label, "group": group,
                            "signal": signal, "outcome": outcome, "n": res["n"],
                            "slope_per_sd": row.loc[signal, "coef"],
                            "t_stat": row.loc[signal, "t_stat"],
                            "p_value": row.loc[signal, "p_value"],
                            "r2": res["r2"],
                        }
                    )
    return pd.DataFrame(recs)


def drift_portfolios(events: pd.DataFrame, tags: pd.DataFrame) -> pd.DataFrame:
    """Quintile sorts on surprise, with the top-minus-bottom spread.

    The economically legible version of the regression above: what a portfolio
    of the biggest beats returned against the biggest misses, over each
    horizon, in each window.
    """
    ev = events.merge(tags[["ticker", "group"]], on="ticker", how="left")
    outcomes = ["one_day_change"] + [f"fwd_{h}d_pct" for h in DRIFT_HORIZONS]
    outcomes = [c for c in outcomes if c in ev.columns]
    frames = []
    for w in ALL_WINDOWS:
        win = ev[
            (ev["earnings_date"] >= pd.Timestamp(w.start))
            & (ev["earnings_date"] <= pd.Timestamp(w.end))
        ]
        if len(win) < 100:
            continue
        for signal in ("sue", "rev_sur"):
            for group_col in (None, "group"):
                q = stats.quantile_sort(win, signal, outcomes, q=5,
                                        group_col=group_col)
                if q.empty:
                    continue
                q.insert(0, "label", w.label)
                q.insert(0, "window", w.name)
                q.insert(2, "scope", "by_group" if group_col else "universe")
                frames.append(q)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


# ── 6. Time variation: quarter-by-quarter cross-sectional slopes ──────────────

def fama_macbeth_quarterly(events: pd.DataFrame, tags: pd.DataFrame,
                           horizon: int = 63) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Cross-sectional regression per reporting quarter, then averaged.

    Each quarter, forward return after the report is regressed on that
    quarter's fundamentals across the whole universe (with sector dummies, so
    the estimate is a within-sector one). The per-quarter slope series shows
    *when* the market was paying for fundamentals; the Fama-MacBeth average
    says whether it did on the whole.
    """
    ev = events.merge(tags[["ticker", "group", "market_cap"]], on="ticker",
                      how="left")
    y_col = f"fwd_{horizon}d_pct"
    if y_col not in ev.columns:
        return pd.DataFrame(), pd.DataFrame()

    ev = ev[ev["earnings_date"] >= pd.Timestamp(STUDY_START)].copy()
    ev["quarter"] = ev["earnings_date"].dt.to_period("Q").astype(str)
    ev["log_mcap"] = np.log(ev["market_cap"].where(ev["market_cap"] > 0))

    per_q = []
    for quarter, g in ev.groupby("quarter"):
        if len(g) < MIN_XSEC_OBS * 2:
            continue
        X = pd.concat(
            [
                stats.dummies(g["group"], "sec"),
                pd.DataFrame(
                    {f"z_{c}": stats.zscore(g[c]) for c in
                     FUND_REGRESSORS + ["log_mcap"] if c in g.columns},
                    index=g.index,
                ),
            ],
            axis=1,
        )
        res = stats.ols(stats.winsorise(g[y_col]), X)
        if res["coefs"].empty:
            continue
        c = res["coefs"]
        c = c[c["term"].str.startswith("z_")].copy()
        c["quarter"] = quarter
        c["n"] = res["n"]
        c["r2"] = res["r2"]
        per_q.append(c)

    if not per_q:
        return pd.DataFrame(), pd.DataFrame()
    slopes = pd.concat(per_q, ignore_index=True)
    slopes = slopes[["quarter", "term", "coef", "se", "t_stat", "n", "r2"]]
    return slopes, stats.fama_macbeth(slopes)


# ── 7. Growth versus multiple ─────────────────────────────────────────────────

def _pe_asof(mats, feat: pd.DataFrame, when: str,
             max_stale_days: int = 200) -> pd.DataFrame:
    """Trailing P/E per ticker on one date, using only published figures.

    `px_split` (split-restated, dividends retained) over the newest TTM EPS
    whose report date is on or before `when`. A TTM figure older than roughly
    two quarters means the company stopped reporting, so it is dropped rather
    than carried forward into a stale multiple.
    """
    px = mats["px_split"]
    sl = px.loc[: str(when)]
    if sl.empty:
        return pd.DataFrame(columns=["ticker", "px_split", "ttm_eps", "pe"])
    last_px = sl.ffill().iloc[-1].rename("px_split")

    cutoff = pd.Timestamp(when)
    have = feat.dropna(subset=["ttm_eps"])
    have = have[have["earnings_date"] <= cutoff]
    if have.empty:
        return pd.DataFrame(columns=["ticker", "px_split", "ttm_eps", "pe"])
    last = have.sort_values("earnings_date").groupby("ticker").tail(1)
    last = last[(cutoff - last["earnings_date"]).dt.days <= max_stale_days]

    d = last[["ticker", "ttm_eps"]].merge(
        last_px.rename_axis("ticker").reset_index(), on="ticker", how="inner"
    )
    d["pe"] = d["px_split"] / d["ttm_eps"].where(d["ttm_eps"] > 0)
    return d


def pe_decomposition(mats, feat: pd.DataFrame,
                     tags: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split each window's price move into earnings growth and re-rating.

    Price = P/E x TTM EPS, so in logs the move separates exactly into the two.
    This is what distinguishes "the business got worse" from "the market
    decided to pay less for the same business" — the difference at the heart
    of the 2024H2 semiconductor question.
    """
    per_ticker, agg = [], []
    for w in ALL_WINDOWS:
        a = _pe_asof(mats, feat, w.start)
        b = _pe_asof(mats, feat, w.end)
        if a.empty or b.empty:
            continue
        d = a.merge(b, on="ticker", suffixes=("_start", "_end"))
        d = d[(d["pe_start"] > 0) & (d["pe_end"] > 0)]
        if d.empty:
            continue
        d["pe_chg_pct"] = (d["pe_end"] / d["pe_start"] - 1.0) * 100.0
        d["eps_growth_pct"] = (d["ttm_eps_end"] / d["ttm_eps_start"] - 1.0) * 100.0
        d["px_chg_pct"] = (d["px_split_end"] / d["px_split_start"] - 1.0) * 100.0
        d = d.merge(tags[["ticker", "group"]], on="ticker", how="left")
        d["window"], d["label"] = w.name, w.label
        per_ticker.append(d)

        for group, g in list(d.groupby("group")) + [("ALL", d)]:
            if len(g) < MIN_GROUP_OBS:
                continue
            agg.append(
                {
                    "window": w.name, "label": w.label, "group": group,
                    "n_stocks": len(g),
                    "median_pe_start": g["pe_start"].median(),
                    "median_pe_end": g["pe_end"].median(),
                    "median_pe_chg_pct": g["pe_chg_pct"].median(),
                    "median_eps_growth_pct": g["eps_growth_pct"].median(),
                    "median_px_chg_pct": g["px_chg_pct"].median(),
                }
            )
    pt = pd.concat(per_ticker, ignore_index=True) if per_ticker else pd.DataFrame()
    return pt, pd.DataFrame(agg)


# ── 8. Daily group index, for the rotation chart ──────────────────────────────

def group_index(mats, tags: pd.DataFrame, start: str = STUDY_START) -> pd.DataFrame:
    """Equal-weighted (median-of-rebased) daily index per group, base 100."""
    px = mats["px_total"].loc[str(start):]
    if px.empty:
        return pd.DataFrame()
    rebased = px / px.bfill().iloc[0] * 100.0
    mapping = tags.set_index("ticker")["group"]
    groups = pd.Series(px.columns.map(mapping), index=px.columns)

    frames = []
    for group, cols in groups.groupby(groups):
        sub = rebased[cols.index]
        if sub.shape[1] < MIN_GROUP_OBS:
            continue
        frames.append(
            sub.median(axis=1).rename("index_level").reset_index().assign(group=group)
        )
    frames.append(
        rebased.median(axis=1).rename("index_level").reset_index().assign(group="ALL")
    )
    return pd.concat(frames, ignore_index=True)


# ── 9. The rotation question at sector level ──────────────────────────────────

def sector_level_link(ret_group: pd.DataFrame,
                      delivery: pd.DataFrame) -> pd.DataFrame:
    """Across sectors, did the ones that delivered more also return more?

    The cross-sectional regressions above ask the question stock by stock. A
    rotation, though, is a statement about sectors: money left one and went to
    another. So the same question is asked again with the sector as the unit of
    observation — thirteen points per window, which is few, but it is the level
    the claim is actually made at.
    """
    d = ret_group.merge(delivery, on=["window", "label", "group"], how="inner")
    d = d[d["group"] != "ALL"]
    metrics = [
        "median_rev_yoy_pct", "median_eps_yoy_pct", "median_rev_yoy_accel_pp",
        "eps_beat_rate_pct", "rev_beat_rate_pct", "median_sue",
    ]
    recs = []
    for (window, label), g in d.groupby(["window", "label"]):
        for metric in metrics:
            res = stats.spearman(g[metric], g["median_ret_pct"])
            recs.append(
                {"window": window, "label": label, "metric": metric,
                 "n_sectors": res["n"], "rho": res["rho"],
                 "t_stat": res["t_stat"], "p_value": res["p_value"]}
            )
    return pd.DataFrame(recs)


# ── 10. The single table the write-up leans on ────────────────────────────────

def delivery_vs_price(ret_group: pd.DataFrame, delivery: pd.DataFrame,
                      pe_group: pd.DataFrame) -> pd.DataFrame:
    """Return, delivery and re-rating side by side, per group and window.

    Everything needed to say "this sector grew and was marked down anyway" in
    one row.
    """
    keys = ["window", "label", "group"]
    d = (
        ret_group[keys + ["n_stocks", "median_ret_pct", "excess_vs_universe_pct"]]
        .merge(
            delivery[keys + ["n_reports", "eps_beat_rate_pct", "rev_beat_rate_pct",
                             "median_rev_yoy_pct", "median_eps_yoy_pct",
                             "median_rev_yoy_accel_pp", "median_1d_reaction_pct"]],
            on=keys, how="left",
        )
        .merge(
            pe_group[keys + ["median_pe_start", "median_pe_end", "median_pe_chg_pct",
                             "median_eps_growth_pct"]],
            on=keys, how="left",
        )
    )
    return d.sort_values(["window", "median_ret_pct"], ascending=[True, False])
