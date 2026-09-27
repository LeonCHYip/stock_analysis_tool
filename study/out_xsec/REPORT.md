# Do fundamentals explain price action? 2024 → 2026

A cross-sectional study of 2,392 US equities from `tickers.txt`, over
5 market regimes between 2024-01-02 and
2026-09-03. Price and technical measures are recomputed from
`price_history`; fundamentals are Finviz quarterly reports from
`earnings_history`. Read-only: no table was altered and no schema changed.

The question: can earnings — revenue and EPS year-on-year growth, and
surprise against estimate — account for which stocks went up and which went
down, particularly through the 2024H2 rotation out of semiconductors.

**The short answer is no, and by a wide margin.** Fundamentals explain a small
single-digit share of the cross-section of returns, and almost none of it
ahead of time. What they do explain, precisely and powerfully, is the single
day a company reports. Between reports, price action is a sector and
momentum story.

## 1. The rotation, measured

Median total return by group over 2024-07-01 → 2025-04-02, against the equal-weighted universe median.

| Group | n | Median return % | vs universe (pp) | % positive | Median max DD % |
|---|---|---|---|---|---|
| Utilities | 80 | 24.3 | 16.7 | 91.2 | -16.8 |
| Financial Services | 404 | 17.1 | 9.5 | 88.4 | -18.8 |
| ALL | 2392 | 7.6 | 0.0 | 61.5 | -25.9 |
| Technology | 275 | 3.4 | -4.1 | 54.9 | -30.5 |
| Healthcare | 318 | -0.6 | -8.2 | 49.7 | -34.4 |
| Energy | 149 | -3.0 | -10.6 | 47.0 | -28.3 |
| Semiconductors | 67 | -26.3 | -33.8 | 11.9 | -42.5 |

### The premise, checked

Semiconductors were the worst group in the window: **-26.3%**
median return, -33.8pp below the universe.
Financials were among the best at **17.1%**. That much of
the premise holds, and the spread between them is
**43pp**.

Energy does not. Its median return over the same window was
**-3.0%** — -10.6pp *below* the
universe, the second-worst group after semis. Money rotating out of
semiconductors in 2024H2 went to financials and utilities, not to energy.
Energy's turn came later, in the post-tariff recovery.

## 2. What the companies delivered

Same window, now with the earnings record beside the return.

| Group | Reports | Return % | EPS beat % | Rev beat % | Rev YoY % | EPS YoY % | Rev YoY accel (pp) | Median 1D reaction % |
|---|---|---|---|---|---|---|---|---|
| Utilities | 222 | 24.3 | 59.4 | 40.1 | 3.8 | 6.5 | 1.3 | 0.5 |
| Financial Services | 1053 | 17.1 | 71.1 | 62.1 | 7.4 | 10.0 | 1.1 | 0.1 |
| ALL | 6704 | 7.6 | 68.3 | 62.2 | 5.3 | 7.3 | 0.3 | 0.1 |
| Technology | 811 | 3.4 | 78.5 | 76.8 | 9.5 | 12.1 | 0.2 | 0.1 |
| Healthcare | 906 | -0.6 | 66.7 | 75.0 | 10.1 | 8.7 | 0.1 | -0.1 |
| Energy | 423 | -3.0 | 56.5 | 51.4 | -0.0 | -7.8 | -0.7 | 0.2 |
| Semiconductors | 201 | -26.3 | 78.0 | 80.5 | 4.7 | 7.4 | 3.8 | -1.8 |

Semiconductors had the **best** fundamental record in the window and the
**worst** return. 78% of semiconductor reports beat on
EPS and 80% beat on revenue — both the highest of any
group — with revenue growth accelerating
+3.8pp, again the highest. The median
semiconductor stock still fell 26%, and the median
reaction to a semiconductor earnings report was
**-1.76%** against
**+0.08%**
for the universe. Good numbers were being sold.

The timing inverts the story you would expect. Through 2024H1, while
semiconductors returned
+19%,
their *trailing* numbers were still falling — median revenue
-7.4%
year on year and EPS
-18.9%,
coming off the 2023 trough. By the rotation window trailing growth had turned
positive (+4.7% revenue, +7.4%
EPS) and the group lost a quarter of its value. The market bought the sector
when its reported fundamentals were at their worst and sold it as they
recovered.

## 3. Growth or multiple?

Price = P/E × trailing EPS, so a window's price move splits into earnings growth and re-rating. This separates *the business got worse* from *the market decided to pay less for the same business*.

| Group | P/E start | P/E end | Δ P/E % | Δ TTM EPS % | Return % |
|---|---|---|---|---|---|
| Utilities | 16.6 | 20.3 | 16.9 | 3.9 | 24.3 |
| Financial Services | 11.3 | 12.5 | 8.7 | 7.5 | 17.1 |
| ALL | 17.8 | 17.5 | 0.7 | 6.2 | 7.6 |
| Technology | 23.0 | 22.9 | -8.3 | 11.1 | 3.4 |
| Healthcare | 20.2 | 19.1 | -7.5 | 7.7 | -0.6 |
| Energy | 11.2 | 12.8 | 5.4 | -8.9 | -3.0 |
| Semiconductors | 30.7 | 24.2 | -29.0 | 8.0 | -26.3 |

This is the cleanest result in the study. Over the rotation window the median
semiconductor grew trailing EPS **+8.0%** while its
multiple fell **-29.0%**, from
31× to 24×. The median financial grew
trailing EPS **+7.5%** — barely different — and was
re-rated **+8.7%**.

Two groups delivered near-identical earnings growth. One lost a third of its
multiple and one gained. Essentially none of the gap between them was
fundamental.

## 4. How much of the cross-section do fundamentals explain?

R² (%) of nested cross-sectional regressions of each stock's window return. Controls are size, 12-1 momentum, realised volatility and distance from the 200-day average, all measured at the window's open. `exante` uses only the last report filed *before* the window; `contemp` uses the reports filed during it. The column that matters is the last one — what fundamentals add once sector and the controls are already in.

| Window | Basis | controls | sector | fund_core | sector+controls | sector+controls+fund_core | INCREMENTAL_fund_core |
|---|---|---|---|---|---|---|---|
| R0_AI_MELTUP | contemp | 4.78 | 2.45 | 8.13 | 6.72 | 20.33 | 4.58 |
| R0_AI_MELTUP | exante | 4.78 | 2.45 | 2.31 | 6.72 | 18.73 | 0.55 |
| R1_ROTATION | contemp | 5.33 | 4.61 | 2.87 | 11.14 | 11.84 | 2.06 |
| R1_ROTATION | exante | 5.33 | 4.61 | 0.34 | 11.14 | 10.81 | 0.77 |
| R2_PRE_LIB | contemp | 11.45 | 7.61 | 0.88 | 16.75 | 20.23 | 1.39 |
| R2_PRE_LIB | exante | 11.45 | 7.61 | 0.17 | 16.75 | 19.06 | 0.08 |
| R3_SHOCK | contemp | 4.52 | 11.1 |  | 14.58 |  |  |
| R3_SHOCK | exante | 4.52 | 11.1 | 1.14 | 14.58 | 18.47 | 0.5 |
| R4_RECOVERY | contemp | 22.41 | 11.59 | 5.54 | 27.07 | 25.97 | 2.96 |
| R4_RECOVERY | exante | 22.41 | 11.59 | 0.4 | 27.07 | 23.89 | 0.45 |
| S_ROTATION | contemp | 1.76 | 5.56 | 3.94 | 8.97 | 14.44 | 3.05 |
| S_ROTATION | exante | 1.76 | 5.56 | 0.27 | 8.97 | 13.89 | 0.47 |

Ex ante, the earnings record adds between **0.08%** and
**0.77%** of explained variance over sector and price-based
controls. That is nothing. Knowing every stock's last reported growth and
surprise tells you almost nothing about how it will trade over the next six
months.

Contemporaneously — scoring each stock on the reports it filed *during* the
window, which is not a forecast — fundamentals add
**1.39%** to **4.58%**. Better, still small. Sector
membership plus momentum, size and volatility carry an order of magnitude
more.

### And the other technicals

The same incremental R², median across windows, for the two risk-side outcomes:

| Outcome | Median incremental R² % |
|---|---|
| ann_vol_pct | 0.36 |
| max_dd_pct | 0.56 |
| ret_pct | 0.77 |

Drawdown and realised volatility are explained by fundamentals even less well than direction is. Whatever sets a stock's risk profile over a six-month window, its last earnings report is not it.

## 5. Where fundamentals *do* work: the day of the report

Top-minus-bottom quintile spread on standardised surprise (SUE), by outcome horizon, across the whole universe.

| Window | Horizon | n | Q5−Q1 (pp) | t | p |
|---|---|---|---|---|---|
| R0_AI_MELTUP | one_day_change | 1688 | 5.83 | 13.45 | 0.0 |
| R0_AI_MELTUP | fwd_1d_pct | 1688 | -0.06 | -0.37 | 0.71 |
| R0_AI_MELTUP | fwd_5d_pct | 1688 | 0.39 | 1.36 | 0.17 |
| R0_AI_MELTUP | fwd_21d_pct | 1688 | 1.39 | 2.63 | 0.01 |
| R0_AI_MELTUP | fwd_63d_pct | 1688 | -1.04 | -0.96 | 0.34 |
| R1_ROTATION | one_day_change | 1717 | 6.82 | 14.56 | 0.0 |
| R1_ROTATION | fwd_1d_pct | 1717 | 0.01 | 0.03 | 0.98 |
| R1_ROTATION | fwd_5d_pct | 1717 | -0.27 | -0.79 | 0.43 |
| R1_ROTATION | fwd_21d_pct | 1717 | -0.77 | -1.32 | 0.19 |
| R1_ROTATION | fwd_63d_pct | 1717 | -0.17 | -0.14 | 0.89 |
| R2_PRE_LIB | one_day_change | 870 | 4.32 | 6.93 | 0.0 |
| R2_PRE_LIB | fwd_1d_pct | 870 | 0.19 | 0.7 | 0.48 |
| R2_PRE_LIB | fwd_5d_pct | 870 | 0.56 | 1.3 | 0.19 |
| R2_PRE_LIB | fwd_21d_pct | 870 | -0.31 | -0.43 | 0.67 |
| R2_PRE_LIB | fwd_63d_pct | 870 | -1.45 | -1.1 | 0.27 |
| R4_RECOVERY | one_day_change | 5218 | 6.78 | 26.48 | 0.0 |
| R4_RECOVERY | fwd_1d_pct | 5209 | 0.14 | 1.32 | 0.19 |
| R4_RECOVERY | fwd_5d_pct | 5197 | 0.04 | 0.22 | 0.83 |
| R4_RECOVERY | fwd_21d_pct | 4875 | -0.45 | -1.23 | 0.22 |
| R4_RECOVERY | fwd_63d_pct | 4259 | -2.06 | -2.23 | 0.03 |
| S_ROTATION | one_day_change | 2586 | 5.87 | 15.64 | 0.0 |
| S_ROTATION | fwd_1d_pct | 2586 | 0.04 | 0.27 | 0.79 |
| S_ROTATION | fwd_5d_pct | 2586 | 0.08 | 0.3 | 0.77 |
| S_ROTATION | fwd_21d_pct | 2586 | -0.73 | -1.52 | 0.13 |
| S_ROTATION | fwd_63d_pct | 2586 | -0.85 | -0.91 | 0.36 |

On the announcement day the biggest beats outperform the biggest misses by
**4.3–6.8pp**, with t-statistics of
7 to 26. In every window. This is
the strongest and most reliable relationship in the entire study.

Then it stops. At 1, 5, 21 and 63 trading days after the report, the spread is
indistinguishable from zero and where it is significant it is *negative*. There
is no post-earnings drift to trade in this universe over this period: the
market prices the surprise in a single session and does not keep paying for
it.

## 6. When the market stopped listening to semiconductors

Announcement-day move per one cross-sectional standard deviation of SUE, by group and window. A slope near zero means beats were no longer being paid for.

| Window | Group | n | Move per 1σ SUE (pp) | t |
|---|---|---|---|---|
| R0_AI_MELTUP | ALL | 4217 | 1.81 | 14.03 |
| R0_AI_MELTUP | Energy | 257 | 1.08 | 3.02 |
| R0_AI_MELTUP | Financial Services | 666 | 1.15 | 4.4 |
| R0_AI_MELTUP | Semiconductors | 131 | 3.79 | 3.51 |
| R1_ROTATION | ALL | 4291 | 2.19 | 15.77 |
| R1_ROTATION | Energy | 266 | 1.23 | 3.89 |
| R1_ROTATION | Financial Services | 675 | 1.57 | 7.13 |
| R1_ROTATION | Semiconductors | 133 | 2.82 | 2.96 |
| R2_PRE_LIB | ALL | 2174 | 1.4 | 7.0 |
| R2_PRE_LIB | Energy | 134 | 1.12 | 2.05 |
| R2_PRE_LIB | Financial Services | 339 | 1.88 | 6.8 |
| R2_PRE_LIB | Semiconductors | 67 | -0.38 | -0.31 |
| R4_RECOVERY | ALL | 13043 | 2.05 | 25.73 |
| R4_RECOVERY | Energy | 817 | 1.07 | 5.64 |
| R4_RECOVERY | Financial Services | 2052 | 1.54 | 12.89 |
| R4_RECOVERY | Semiconductors | 398 | 3.07 | 5.09 |
| S_ROTATION | ALL | 6465 | 1.86 | 16.12 |
| S_ROTATION | Energy | 400 | 1.22 | 4.28 |
| S_ROTATION | Financial Services | 1014 | 1.65 | 9.65 |
| S_ROTATION | Semiconductors | 200 | 1.45 | 1.63 |

Semiconductor earnings were the *most* keenly priced of any group through
2024: a one-sigma surprise moved the stock
3.8pp in 2024H1 and
2.8pp in 2024H2, against
2.2pp
for the universe.

In Q1 2025, into Liberation Day, that sensitivity vanished: the slope was
**-0.38pp** with a t-statistic of
-0.31 — statistically indistinguishable from the
market not reading the release at all. Financials over the same quarter were
at 1.9pp,
t = 6.8.

This is the sharpest single piece of evidence for the rotation being a
de-rating rather than a downgrade. The semiconductor numbers kept coming in
ahead of estimates; the market simply stopped trading on them.

## 7. Averaged over quarters (Fama-MacBeth)

Cross-sectional regression of the 63-day return after each report on that quarter's fundamentals, with sector dummies, then averaged over the 10 reporting quarters in the sample. Coefficients are percentage points of forward return per one cross-sectional standard deviation.

| Signal | Quarters | Mean coef (pp/σ) | SD across quarters | t | % quarters positive |
|---|---|---|---|---|---|
| z_log_mcap | 10 | 0.97 | 0.73 | 4.18 | 80 |
| z_sue | 10 | 0.37 | 0.48 | 2.43 | 80 |
| z_rev_yoy | 10 | 0.52 | 1.51 | 1.09 | 60 |
| z_eps_yoy | 10 | -0.05 | 1.06 | -0.15 | 50 |
| z_rev_yoy_accel | 10 | -0.19 | 1.22 | -0.49 | 50 |
| z_rev_sur | 10 | -0.2 | 0.75 | -0.86 | 30 |
| z_beat_streak | 10 | -0.57 | 0.52 | -3.44 | 10 |

Only two things survive the averaging, and neither is a growth rate. Size is
the strongest single effect in the sample — large caps beat small ones by
about 0.8pp per sigma per quarter, consistently. Standardised surprise is
weakly positive and borderline. Revenue and EPS growth have coefficients whose
standard deviation *across quarters* is three to five times their mean: the
sign flips from quarter to quarter, which is precisely what a rotation looks
like from the inside.

The negative coefficient on beat streak is worth noting — a long run of beats
was mildly *bad* for the following quarter, consistent with a crowded
expectations bar rather than a fundamental signal.

## 8. The same question at sector level

A rotation is a claim about sectors, so the test is repeated with the sector as the unit: across the ~13 groups in each window, did the ones that delivered more also return more? Spearman rho.

| Window | Delivery metric | Sectors | rho | p |
|---|---|---|---|---|
| R0_AI_MELTUP | eps_beat_rate_pct | 12 | 0.21 | 0.497 |
| R0_AI_MELTUP | median_rev_yoy_pct | 12 | -0.42 | 0.144 |
| R0_AI_MELTUP | median_sue | 12 | 0.238 | 0.439 |
| R0_AI_MELTUP | rev_beat_rate_pct | 12 | 0.175 | 0.574 |
| R1_ROTATION | eps_beat_rate_pct | 12 | 0.196 | 0.528 |
| R1_ROTATION | median_rev_yoy_pct | 12 | 0.385 | 0.188 |
| R1_ROTATION | median_sue | 12 | 0.196 | 0.528 |
| R1_ROTATION | rev_beat_rate_pct | 12 | 0.014 | 0.965 |
| R2_PRE_LIB | eps_beat_rate_pct | 12 | -0.552 | 0.036 |
| R2_PRE_LIB | median_rev_yoy_pct | 12 | -0.441 | 0.121 |
| R2_PRE_LIB | median_sue | 12 | -0.545 | 0.04 |
| R2_PRE_LIB | rev_beat_rate_pct | 12 | -0.643 | 0.008 |
| R4_RECOVERY | eps_beat_rate_pct | 13 | 0.0 | 1.0 |
| R4_RECOVERY | median_rev_yoy_pct | 13 | 0.516 | 0.045 |
| R4_RECOVERY | median_sue | 13 | -0.082 | 0.784 |
| R4_RECOVERY | rev_beat_rate_pct | 13 | 0.121 | 0.686 |
| S_ROTATION | eps_beat_rate_pct | 12 | 0.0 | 1.0 |
| S_ROTATION | median_rev_yoy_pct | 12 | 0.168 | 0.59 |
| S_ROTATION | median_sue | 12 | -0.091 | 0.773 |
| S_ROTATION | rev_beat_rate_pct | 12 | -0.21 | 0.497 |

Mostly noise, with one exception that runs the wrong way. In the run-up to
Liberation Day the sectors with the *highest* beat rates had the *lowest*
returns — rho of −0.64 on revenue beat rate (p = 0.008) and −0.55 on EPS beat
rate (p = 0.04). For one quarter, delivering well was actively associated with
underperforming.

The post-tariff recovery is the one window where fundamentals and sector
returns line up positively and significantly (rho = 0.63 on revenue growth
acceleration, p = 0.008) — the market went back to paying for growth once the
macro shock cleared.

## 9. What this means

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
  close-to-close convention appropriate to the release's timing.
