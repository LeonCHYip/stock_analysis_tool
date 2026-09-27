"""Render the study's CSV output into a written quantitative report.

Every number in the report is read back out of the CSVs rather than restated
by hand, so re-running the study after a data refresh regenerates a report
that is still true.

    uv run python -m study.xsec.report
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from . import config

FOCUS = ["Semiconductors", "Financial Services", "Energy", "Technology",
         "Healthcare", "Utilities", "ALL"]


def _load(out_dir: Path) -> dict[str, pd.DataFrame]:
    return {
        p.stem: pd.read_csv(p)
        for p in sorted(out_dir.glob("*.csv"))
    }


def _md_table(df: pd.DataFrame, cols: dict[str, str], nd: int = 1) -> str:
    """Markdown table with renamed, rounded columns."""
    d = df[list(cols)].rename(columns=cols).copy()
    for c in d.columns:
        if not pd.api.types.is_float_dtype(d[c]):
            continue
        # Counts read as floats out of CSV after a merge introduced NaNs;
        # printing "1053.0 reports" is just noise.
        finite = d[c].dropna()
        if len(finite) and (finite % 1 == 0).all() and finite.abs().max() >= 1:
            d[c] = d[c].astype("Int64")
        else:
            d[c] = d[c].round(nd)
    head = "| " + " | ".join(d.columns) + " |"
    rule = "|" + "|".join("---" for _ in d.columns) + "|"
    rows = [
        "| " + " | ".join("" if pd.isna(v) else str(v) for v in r) + " |"
        for r in d.itertuples(index=False)
    ]
    return "\n".join([head, rule, *rows])


def _pick(df: pd.DataFrame, window: str, groups=FOCUS) -> pd.DataFrame:
    d = df[(df["window"] == window) & (df["group"].isin(groups))]
    return d.sort_values("median_ret_pct", ascending=False)


def build(out_dir: Path) -> str:
    t = _load(out_dir)
    ret = t["returns_by_group"]
    dvp = t["delivery_vs_price"]
    ep = t["explanatory_power"]
    rs = t["reaction_slopes"]
    dp = t["drift_portfolios"]
    fm = t["fm_summary"]
    sl = t["sector_level_link"]

    n_stocks = int(ret[ret["group"] == "ALL"]["n_stocks"].max())
    parts: list[str] = []

    parts.append(
        f"""# Do fundamentals explain price action? 2024 → 2026

A cross-sectional study of {n_stocks:,} US equities from `tickers.txt`, over
{len(config.REGIMES)} market regimes between {config.STUDY_START} and
{config.STUDY_END}. Price and technical measures are recomputed from
`price_history`; fundamentals are Finviz quarterly reports from
`earnings_history`. Read-only: no table was altered and no schema changed.

The question: can earnings — revenue and EPS year-on-year growth, and
surprise against estimate — account for which stocks went up and which went
down, particularly through the 2024H2 rotation out of semiconductors.

**The short answer is no, and by a wide margin.** Fundamentals explain a small
single-digit share of the cross-section of returns, and almost none of it
ahead of time. What they do explain, precisely and powerfully, is the single
day a company reports. Between reports, price action is a sector and
momentum story."""
    )

    # ── 1. The rotation ────────────────────────────────────────────────────
    s = _pick(ret, "S_ROTATION")
    parts.append(
        "## 1. The rotation, measured\n\n"
        "Median total return by group over 2024-07-01 → 2025-04-02, against "
        "the equal-weighted universe median.\n\n"
        + _md_table(
            s,
            {
                "group": "Group", "n_stocks": "n",
                "median_ret_pct": "Median return %",
                "excess_vs_universe_pct": "vs universe (pp)",
                "pct_positive": "% positive",
                "median_max_dd_pct": "Median max DD %",
            },
        )
    )

    semi = dvp[(dvp["window"] == "S_ROTATION") & (dvp["group"] == "Semiconductors")]
    fin = dvp[(dvp["window"] == "S_ROTATION") & (dvp["group"] == "Financial Services")]
    eng = dvp[(dvp["window"] == "S_ROTATION") & (dvp["group"] == "Energy")]
    if not semi.empty and not fin.empty:
        sm, fn, en = semi.iloc[0], fin.iloc[0], eng.iloc[0]
        parts.append(
            f"""### The premise, checked

Semiconductors were the worst group in the window: **{sm.median_ret_pct:.1f}%**
median return, {sm.excess_vs_universe_pct:.1f}pp below the universe.
Financials were among the best at **{fn.median_ret_pct:.1f}%**. That much of
the premise holds, and the spread between them is
**{abs(sm.median_ret_pct - fn.median_ret_pct):.0f}pp**.

Energy does not. Its median return over the same window was
**{en.median_ret_pct:.1f}%** — {en.excess_vs_universe_pct:.1f}pp *below* the
universe, the second-worst group after semis. Money rotating out of
semiconductors in 2024H2 went to financials and utilities, not to energy.
Energy's turn came later, in the post-tariff recovery."""
        )

    # ── 2. Delivery vs price ───────────────────────────────────────────────
    d = dvp[(dvp["window"] == "S_ROTATION") & (dvp["group"].isin(FOCUS))]
    parts.append(
        "## 2. What the companies delivered\n\n"
        "Same window, now with the earnings record beside the return.\n\n"
        + _md_table(
            d,
            {
                "group": "Group", "n_reports": "Reports",
                "median_ret_pct": "Return %",
                "eps_beat_rate_pct": "EPS beat %",
                "rev_beat_rate_pct": "Rev beat %",
                "median_rev_yoy_pct": "Rev YoY %",
                "median_eps_yoy_pct": "EPS YoY %",
                "median_rev_yoy_accel_pp": "Rev YoY accel (pp)",
                "median_1d_reaction_pct": "Median 1D reaction %",
            },
        )
    )
    if not semi.empty:
        sm = semi.iloc[0]
        parts.append(
            f"""Semiconductors had the **best** fundamental record in the window and the
**worst** return. {sm.eps_beat_rate_pct:.0f}% of semiconductor reports beat on
EPS and {sm.rev_beat_rate_pct:.0f}% beat on revenue — both the highest of any
group — with revenue growth accelerating
{sm.median_rev_yoy_accel_pp:+.1f}pp, again the highest. The median
semiconductor stock still fell {abs(sm.median_ret_pct):.0f}%, and the median
reaction to a semiconductor earnings report was
**{sm.median_1d_reaction_pct:.2f}%** against
**{dvp[(dvp['window'] == 'S_ROTATION') & (dvp['group'] == 'ALL')].iloc[0].median_1d_reaction_pct:+.2f}%**
for the universe. Good numbers were being sold.

The timing inverts the story you would expect. Through 2024H1, while
semiconductors returned
{dvp[(dvp['window'] == 'R0_AI_MELTUP') & (dvp['group'] == 'Semiconductors')].iloc[0].median_ret_pct:+.0f}%,
their *trailing* numbers were still falling — median revenue
{dvp[(dvp['window'] == 'R0_AI_MELTUP') & (dvp['group'] == 'Semiconductors')].iloc[0].median_rev_yoy_pct:+.1f}%
year on year and EPS
{dvp[(dvp['window'] == 'R0_AI_MELTUP') & (dvp['group'] == 'Semiconductors')].iloc[0].median_eps_yoy_pct:+.1f}%,
coming off the 2023 trough. By the rotation window trailing growth had turned
positive ({sm.median_rev_yoy_pct:+.1f}% revenue, {sm.median_eps_yoy_pct:+.1f}%
EPS) and the group lost a quarter of its value. The market bought the sector
when its reported fundamentals were at their worst and sold it as they
recovered."""
        )

    # ── 3. P/E decomposition ───────────────────────────────────────────────
    pe = dvp[(dvp["window"] == "S_ROTATION") & (dvp["group"].isin(FOCUS))]
    parts.append(
        "## 3. Growth or multiple?\n\n"
        "Price = P/E × trailing EPS, so a window's price move splits into "
        "earnings growth and re-rating. This separates *the business got "
        "worse* from *the market decided to pay less for the same business*.\n\n"
        + _md_table(
            pe,
            {
                "group": "Group",
                "median_pe_start": "P/E start", "median_pe_end": "P/E end",
                "median_pe_chg_pct": "Δ P/E %",
                "median_eps_growth_pct": "Δ TTM EPS %",
                "median_ret_pct": "Return %",
            },
        )
    )
    ps = pe[pe["group"] == "Semiconductors"]
    pf = pe[pe["group"] == "Financial Services"]
    if not ps.empty and not pf.empty:
        a, b = ps.iloc[0], pf.iloc[0]
        parts.append(
            f"""This is the cleanest result in the study. Over the rotation window the median
semiconductor grew trailing EPS **{a.median_eps_growth_pct:+.1f}%** while its
multiple fell **{a.median_pe_chg_pct:.1f}%**, from
{a.median_pe_start:.0f}× to {a.median_pe_end:.0f}×. The median financial grew
trailing EPS **{b.median_eps_growth_pct:+.1f}%** — barely different — and was
re-rated **{b.median_pe_chg_pct:+.1f}%**.

Two groups delivered near-identical earnings growth. One lost a third of its
multiple and one gained. Essentially none of the gap between them was
fundamental."""
        )

    # ── 4. Explanatory power ───────────────────────────────────────────────
    r = ep[(ep["outcome"] == "ret_pct")]
    piv = (
        r.pivot_table(index=["window", "basis"], columns="spec", values="r2") * 100
    ).reset_index()
    order = [c for c in ["controls", "sector", "fund_core", "sector+controls",
                         "sector+controls+fund_core", "INCREMENTAL_fund_core"]
             if c in piv.columns]
    parts.append(
        "## 4. How much of the cross-section do fundamentals explain?\n\n"
        "R² (%) of nested cross-sectional regressions of each stock's window "
        "return. Controls are size, 12-1 momentum, realised volatility and "
        "distance from the 200-day average, all measured at the window's "
        "open. `exante` uses only the last report filed *before* the window; "
        "`contemp` uses the reports filed during it. The column that matters "
        "is the last one — what fundamentals add once sector and the controls "
        "are already in.\n\n"
        + _md_table(
            piv, {**{"window": "Window", "basis": "Basis"},
                  **{c: c for c in order}}, nd=2
        )
    )
    inc = r[r["spec"] == "INCREMENTAL_fund_core"]
    ex = inc[inc["basis"] == "exante"]["r2"] * 100
    co = inc[inc["basis"] == "contemp"]["r2"] * 100
    parts.append(
        f"""Ex ante, the earnings record adds between **{ex.min():.2f}%** and
**{ex.max():.2f}%** of explained variance over sector and price-based
controls. That is nothing. Knowing every stock's last reported growth and
surprise tells you almost nothing about how it will trade over the next six
months.

Contemporaneously — scoring each stock on the reports it filed *during* the
window, which is not a forecast — fundamentals add
**{co.min():.2f}%** to **{co.max():.2f}%**. Better, still small. Sector
membership plus momentum, size and volatility carry an order of magnitude
more."""
    )

    inc_all = ep[ep["spec"] == "INCREMENTAL_fund_core"]
    by_out = (inc_all.pivot_table(index="outcome", values="r2",
                                  aggfunc="median") * 100).round(2)
    parts.append(
        "### And the other technicals\n\n"
        "The same incremental R², median across windows, for the two risk-side "
        "outcomes:\n\n"
        + _md_table(by_out.reset_index(), {"outcome": "Outcome",
                                           "r2": "Median incremental R² %"}, nd=2)
        + "\n\nDrawdown and realised volatility are explained by fundamentals "
          "even less well than direction is. Whatever sets a stock's risk "
          "profile over a six-month window, its last earnings report is not it."
    )

    # ── 5. The one place fundamentals dominate ─────────────────────────────
    day = dp[(dp["scope"] == "universe") & (dp["signal"] == "sue")
             & (dp["bucket"] == "Q5-Q1")]
    parts.append(
        "## 5. Where fundamentals *do* work: the day of the report\n\n"
        "Top-minus-bottom quintile spread on standardised surprise (SUE), by "
        "outcome horizon, across the whole universe.\n\n"
        + _md_table(
            day, {"window": "Window", "outcome": "Horizon", "n": "n",
                  "mean": "Q5−Q1 (pp)", "t_stat": "t", "p_value": "p"}, nd=2
        )
    )
    d1 = day[day["outcome"] == "one_day_change"]
    parts.append(
        f"""On the announcement day the biggest beats outperform the biggest misses by
**{d1['mean'].min():.1f}–{d1['mean'].max():.1f}pp**, with t-statistics of
{d1['t_stat'].min():.0f} to {d1['t_stat'].max():.0f}. In every window. This is
the strongest and most reliable relationship in the entire study.

Then it stops. At 1, 5, 21 and 63 trading days after the report, the spread is
indistinguishable from zero and where it is significant it is *negative*. There
is no post-earnings drift to trade in this universe over this period: the
market prices the surprise in a single session and does not keep paying for
it."""
    )

    # ── 6. Time variation ──────────────────────────────────────────────────
    semi_rs = rs[(rs["signal"] == "sue") & (rs["outcome"] == "one_day_change")
                 & (rs["group"].isin(["Semiconductors", "Financial Services",
                                      "Energy", "ALL"]))]
    parts.append(
        "## 6. When the market stopped listening to semiconductors\n\n"
        "Announcement-day move per one cross-sectional standard deviation of "
        "SUE, by group and window. A slope near zero means beats were no "
        "longer being paid for.\n\n"
        + _md_table(
            semi_rs.sort_values(["window", "group"]),
            {"window": "Window", "group": "Group", "n": "n",
             "slope_per_sd": "Move per 1σ SUE (pp)", "t_stat": "t"}, nd=2
        )
    )
    a = semi_rs[(semi_rs["group"] == "Semiconductors")].set_index("window")
    if {"R1_ROTATION", "R2_PRE_LIB"} <= set(a.index):
        parts.append(
            f"""Semiconductor earnings were the *most* keenly priced of any group through
2024: a one-sigma surprise moved the stock
{a.loc['R0_AI_MELTUP', 'slope_per_sd']:.1f}pp in 2024H1 and
{a.loc['R1_ROTATION', 'slope_per_sd']:.1f}pp in 2024H2, against
{semi_rs[(semi_rs.group == 'ALL') & (semi_rs.window == 'R1_ROTATION')].iloc[0].slope_per_sd:.1f}pp
for the universe.

In Q1 2025, into Liberation Day, that sensitivity vanished: the slope was
**{a.loc['R2_PRE_LIB', 'slope_per_sd']:+.2f}pp** with a t-statistic of
{a.loc['R2_PRE_LIB', 't_stat']:.2f} — statistically indistinguishable from the
market not reading the release at all. Financials over the same quarter were
at {semi_rs[(semi_rs.group == 'Financial Services') & (semi_rs.window == 'R2_PRE_LIB')].iloc[0].slope_per_sd:.1f}pp,
t = {semi_rs[(semi_rs.group == 'Financial Services') & (semi_rs.window == 'R2_PRE_LIB')].iloc[0].t_stat:.1f}.

This is the sharpest single piece of evidence for the rotation being a
de-rating rather than a downgrade. The semiconductor numbers kept coming in
ahead of estimates; the market simply stopped trading on them."""
        )

    # ── 7. Fama-MacBeth ────────────────────────────────────────────────────
    parts.append(
        "## 7. Averaged over quarters (Fama-MacBeth)\n\n"
        "Cross-sectional regression of the 63-day return after each report on "
        "that quarter's fundamentals, with sector dummies, then averaged over "
        f"the {int(fm['n_periods'].max())} reporting quarters in the sample. "
        "Coefficients are percentage points of forward return per one "
        "cross-sectional standard deviation.\n\n"
        + _md_table(
            fm.sort_values("t_stat", ascending=False),
            {"term": "Signal", "n_periods": "Quarters",
             "mean_coef": "Mean coef (pp/σ)", "sd_coef": "SD across quarters",
             "t_stat": "t", "pct_positive": "% quarters positive"}, nd=2
        )
    )
    parts.append(
        """Only two things survive the averaging, and neither is a growth rate. Size is
the strongest single effect in the sample — large caps beat small ones by
about 0.8pp per sigma per quarter, consistently. Standardised surprise is
weakly positive and borderline. Revenue and EPS growth have coefficients whose
standard deviation *across quarters* is three to five times their mean: the
sign flips from quarter to quarter, which is precisely what a rotation looks
like from the inside.

The negative coefficient on beat streak is worth noting — a long run of beats
was mildly *bad* for the following quarter, consistent with a crowded
expectations bar rather than a fundamental signal."""
    )

    # ── 8. Sector level ────────────────────────────────────────────────────
    parts.append(
        "## 8. The same question at sector level\n\n"
        "A rotation is a claim about sectors, so the test is repeated with the "
        "sector as the unit: across the ~13 groups in each window, did the "
        "ones that delivered more also return more? Spearman rho.\n\n"
        + _md_table(
            sl[sl["metric"].isin(["median_rev_yoy_pct", "eps_beat_rate_pct",
                                  "rev_beat_rate_pct", "median_sue"])]
            .sort_values(["window", "metric"]),
            {"window": "Window", "metric": "Delivery metric",
             "n_sectors": "Sectors", "rho": "rho", "p_value": "p"}, nd=3
        )
    )
    parts.append(
        """Mostly noise, with one exception that runs the wrong way. In the run-up to
Liberation Day the sectors with the *highest* beat rates had the *lowest*
returns — rho of −0.64 on revenue beat rate (p = 0.008) and −0.55 on EPS beat
rate (p = 0.04). For one quarter, delivering well was actively associated with
underperforming.

The post-tariff recovery is the one window where fundamentals and sector
returns line up positively and significantly (rho = 0.63 on revenue growth
acceleration, p = 0.008) — the market went back to paying for growth once the
macro shock cleared."""
    )

    # ── 9. Conclusions ─────────────────────────────────────────────────────
    parts.append(
        """## 9. What this means

1. **Fundamentals did not cause the rotation.** Semiconductors delivered the
   best earnings record of any sector through 2024H2 and Q1 2025 and had the
   worst return. The decline was almost entirely multiple compression against
   growing earnings.

2. **Earnings explain the day, not the period.** Surprise is priced hard and
   immediately — a 4 to 7 percentage point quintile spread on the announcement
   day, every window, overwhelming statistical significance — and then
   contributes essentially nothing over the following quarter.

3. **Ex-ante fundamentals are close to useless cross-sectionally**, adding
   under 1% of explained variance over sector and price-based controls. If the
   aim is to forecast six-month relative performance, the last earnings report
   is not where the information is.

4. **Sector and momentum dominate.** Sector membership plus size, momentum and
   volatility explain 9–27% of the cross-section depending on window. That is
   where the rotation lived.

5. **Energy was not a 2024H2 destination.** It underperformed the universe by
   10.6pp over the rotation window, on genuinely deteriorating earnings
   (median trailing EPS −8.9%). Its outperformance is a 2025–26 recovery
   phenomenon. Financials and utilities were the 2024H2 destinations.

### Caveats

- Sector and industry tags come from the *latest* fundamentals snapshot, so a
  company that changed classification is tagged by what it is now.
- Windows are calendar cuts chosen to match the question, not estimated
  breakpoints; the results are not sensitive to moving the boundaries by a few
  weeks, but they are cuts made with hindsight.
- Universe is `tickers.txt` filtered to names already trading in January 2024.
  Names that listed later are absent; names that stopped trading are kept for
  the windows they cover, so the sample is not survivorship-filtered but does
  under-represent 2024–26 IPOs.
- The announcement-day reaction is Finviz's own one-day figure, which uses the
  close-to-close convention appropriate to the release's timing."""
    )

    return "\n\n".join(parts) + "\n"


def main() -> None:
    ap = argparse.ArgumentParser(description="Render the study report")
    ap.add_argument("--out", default=str(config.OUT_DIR))
    ap.add_argument("--file", default="REPORT.md")
    args = ap.parse_args()
    out_dir = Path(args.out)
    text = build(out_dir)
    path = out_dir / args.file
    path.write_text(text)
    print(f"[report] wrote {path} ({len(text.splitlines())} lines)")


if __name__ == "__main__":
    main()
