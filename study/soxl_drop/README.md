# SOXL −10% sell-rule study

Question: does SOXL closing down 10% or more (or another threshold) say anything
about future SOXL and MU returns, and does "sell everything on that close" beat
holding? Primary window 14 Sep 2023 → 14 Sep 2026 (54 triggers), with
2010 → Sep 2023 (109 triggers) as an out-of-sample check.

Read-only with respect to the app: prices come from Yahoo (`fetch.py`), and the
DuckDB is only read (`earnings_pull.py`, falling back to a temp copy if
Streamlit holds the lock).

## Run order

```bash
cd study/soxl_drop
uv run python fetch.py          # prices.csv (Yahoo, auto-adjusted, 2010 → today)
uv run python earnings_pull.py  # semi_earnings.csv from earnings_history (read-only)
uv run python event_study.py    # out/event_study.csv, out/events_5pct.csv
uv run python backtest.py       # out/backtest.csv, out/placebo.csv, out/equity_3y.csv
uv run python context.py        # out/events_10pct_context.csv, buckets, risk, by_year
uv run python report.py         # soxl_drop_study.html
```

`catalysts.csv` is hand-labelled (news category, one-line cause, confidence)
for every ≤ −10% day in the 3-year window; `context.py` joins it if present.

| File | Role |
|------|------|
| `common.py` | Loader, window dates, forward-return helpers |
| `rules.py` | Backtest engine: `simulate()` and `metrics()` |
| `event_study.py` | Forward 1/2/3/5/10/21/63d returns after drops vs all days, bootstrap, declustered sample |
| `backtest.py` | Threshold × re-entry × execution sweep, random-exit placebo |
| `context.py` | Conditioning (200DMA, clustering, breadth, VIX, earnings, catalyst), risk view, calendar years |
| `report.py` + `report_template.html` | Self-contained HTML report |

## Findings (data through 14 Sep 2026 close)

- **No downside continuation.** After a ≤ −10% SOXL close, SOXL averaged +1.8%
  the next day (all days +0.5%), +6.7% over 5 days (+2.4%) and +13.9% over 21
  days (+10.8%). MU: +1.0% next day, 21 days in line with its baseline. The
  2010–2023 sample says the same (+1.7% next day).
- **Selling at the trigger close is worse than random.** Sell-and-rebuy-next-day
  ranks at the 13th percentile of 500 random-exit runs for SOXL in 2023–26 and
  the 0.4th percentile in 2010–23.
- **Holding beat the rule in 2023–26** for every re-entry variant at −10%.
  MU: 137% CAGR holding vs 80% (out 5 days) vs 21% (out 21 days).
  SOXL: 69% vs 40% vs 0.7%.
- **The rule is bear-market insurance.** In 2010–23, standing aside 21 days cut
  SOXL max drawdown from −90% to −58% at similar CAGR (32% vs 30%), driven by
  2011, 2020 and 2022. It cost heavily in 2019, 2023, 2025 and 2026.
- **What a trigger does predict is volatility.** 52% of triggers see another
  within 5 days (28% for any day), and forward 21-day vol runs 135% vs 109%.
- **Context splits don't survive out of sample.** 200DMA, clustering,
  broad-vs-semis-only, VIX and prior-month trend all pick a different "better"
  side in 2023–26 than in 2010–23. Beat-and-still-sold earnings drops had a
  weak median month (−2.2%, n = 19), but that's 3-year data only.

Caveats: overlapping windows (about 21 independent episodes in 3 years), an
extraordinary memory bull market in the primary window, no costs or taxes,
and hand-labelled catalysts.
