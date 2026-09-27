# Cross-sectional fundamentals study (2024 → 2026)

Does the earnings record — revenue and EPS year-on-year growth, and surprise
against estimate — explain how US stocks actually traded from 2024 through the
tariff shock?

Covers every ticker in `tickers.txt` with price history reaching back to
January 2024 (2,392 of 2,530). Separate from the app: it opens DuckDB with
`read_only=True`, adds no rows to any table and alters no schema.

## Run it

```bash
# the study — writes CSVs to study/out_xsec/
uv run python -m study.xsec.run

# the written report
uv run python -m study.xsec.report     # → study/out_xsec/REPORT.md
uv run python -m study.xsec.publish    # → study/out_xsec/report.html
```

DuckDB allows one process at a time, so **close the Streamlit app first**, or
point the study at a file copy:

```bash
cp stock_analysis_v2.duckdb /tmp/snap.duckdb
STUDY_DB=/tmp/snap.duckdb uv run python -m study.xsec.run
```

## The 2022 earnings backfill

`earnings_history` starts on 2023-01-03. Year-on-year growth is a lag-4
comparison, so the earliest report carrying a YoY figure would be one filed in
2024 — leaving the 2024H1 window with no ex-ante fundamentals at all. Calendar
2022 is scraped from Finviz to close that gap.

```bash
# 1. scrape to a staging file — no database access, ~25 min
uv run python -m study.xsec.backfill_2022 --scrape

# 2. optional: insert the staged rows into earnings_history
#    (needs the write lock, so close Streamlit first)
uv run python -m study.xsec.backfill_2022 --load --dry-run   # count first
uv run python -m study.xsec.backfill_2022 --load
```

Step 2 is optional. `run.py` reads the staging file directly, so the study is
complete without ever writing to the database. The load only ever INSERTs rows
whose `(ticker, earnings_date)` is absent — it adds no columns and never
modifies an existing row — and after loading, a rerun produces an identical
panel because the database wins on any key it already holds.

## Modules

| Module | Role |
|---|---|
| `config.py` | Regime windows, universe path, thresholds. All tuning lives here. |
| `data.py` | Read-only loaders for `price_history`, `earnings_history`, `fundamentals`, plus the staging-file merge. |
| `technicals.py` | Price/technical features recomputed from daily bars on wide (date × ticker) matrices. `tech_indicators` only reaches back to Feb 2026, so nothing in the study can use it. |
| `features.py` | Fundamental features: lag-4 YoY, growth acceleration, standardised surprise (SUE), TTM sums, beat streaks. |
| `stats.py` | OLS with HC1 robust errors, Spearman, Fama-MacBeth, quantile sorts — numpy only, since the project has neither scipy nor statsmodels. |
| `analyses.py` | The ten analyses, each returning a tidy frame. |
| `charts.py` | Inline-SVG chart builders for the HTML report. |
| `run.py` / `report.py` / `publish.py` | CLI entry points. |

## Output

| File | Contents |
|---|---|
| `returns_by_group.csv` / `_ticker` | Return, drawdown, volatility per window |
| `delivery_by_group.csv` | Beat rates, growth, announcement reactions |
| `delivery_vs_price.csv` | Return, delivery and re-rating in one row per group/window |
| `explanatory_power.csv` | Nested-regression R², incl. the incremental R² of the fundamental block |
| `regression_coefficients.csv` | Coefficients, HC1 t-stats, p-values |
| `rank_correlations.csv` | Spearman rho of return vs each metric, overall and within sector |
| `reaction_slopes.csv` | Announcement-day and drift slopes per unit of surprise |
| `drift_portfolios.csv` | Surprise-quintile portfolios with Q5−Q1 spreads |
| `fm_quarterly_slopes.csv` / `fm_summary.csv` | Quarter-by-quarter slopes and their Fama-MacBeth average |
| `pe_decomp_by_group.csv` / `_ticker` | Price move split into EPS growth and multiple change |
| `sector_level_link.csv` | Delivery vs return with the sector as the unit |
| `group_index.csv` | Daily rebased median index per group |

## Two data facts worth knowing

- **`q_rev_yoy` / `q_eps_yoy` are unusable for this period.** The columns exist
  but are only populated for rows fetched from 2026 (6,832 of 62,000). All
  growth in the study is recomputed as a lag-4 change off the reported actuals.
- **Both prices and Finviz EPS arrive already split-restated.** Checked against
  NVDA and AVGO's 10:1 splits. The restatement code in `technicals` and
  `features` is therefore a no-op guard, kept because the failure it prevents
  would otherwise be silent.
