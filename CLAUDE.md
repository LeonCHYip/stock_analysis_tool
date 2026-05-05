# CLAUDE.md — Stock Analysis Tool

## Project Purpose

Personal stock screening and analysis tool. Fetches price, technical, and fundamental data for a large watchlist of US equities, scores each stock across 10 standardised indicators, and presents the results in a Streamlit dashboard with multiple views and drill-down tabs.

---

## How to Run

```bash
# Start the Streamlit UI (primary interface)
uv run streamlit run app.py

# CLI scan (legacy — runs a single batch and prints to terminal)
uv run python main.py --tickers AAPL,MSFT,NVDA

# Fetch daily earnings from Finviz (run manually or on a schedule)
uv run python earnings_fetcher.py --daily --lookback 30
```

Environment: Python 3.12+, managed by **uv**. Run `uv sync` if dependencies are missing.

---

## Architecture Overview

```
app.py  (Streamlit UI)
   ├── storage.py         ← DuckDB (primary data store)
   ├── technical_fetcher.py   ← Extended tech indicators → DuckDB
   ├── fundamental_fetcher.py ← Yahoo Finance HTTP APIs → DuckDB
   ├── earnings_fetcher.py    ← Finviz scraper → DuckDB
   ├── indicators.py          ← 10-indicator scoring engine
   ├── trigger_engine.py      ← Custom trigger-based column computation
   ├── peers_fetcher.py       ← Yahoo peer/competitor valuations
   ├── ai_analyzer.py         ← Google Gemini AI deep-dive analysis
   ├── column_catalog.py      ← Authoritative UI column reference
   └── market_calendar.py     ← NYSE trading calendar utilities

main.py         ← Legacy CLI entry point (uses data_fetcher.py + db.py)
data_fetcher.py ← Original yfinance technical+fundamental fetch (legacy)
db.py           ← Legacy SQLite schema (migrated to DuckDB, Feb 2026)
migrate.py      ← DISABLED (migration already done)
reporter.py     ← CLI console table renderer (used by main.py)
vpn_switcher.py ← Mullvad VPN rotation helper for bulk scan batches
config.py       ← Loads .env; exposes FMP_API_KEY (currently unused)
```

---

## Key Files

| File | Role |
|------|------|
| `app.py` | Streamlit UI — very large; contains all tab rendering, column group definitions (`VALUE_COL_GROUPS`), and user preference persistence (`user_prefs.json`) |
| `storage.py` | DuckDB persistence layer. All active tables live here. Public API intentionally mirrors `db.py` to ease the migration. |
| `technical_fetcher.py` | Downloads 3 years of OHLCV via `yf.download`, computes the full extended indicator set (RSI, MACD, BBands, ATR, ADX, Stochastic, EMA, Donchian, CMF, A/D, realised vol, max drawdown, gaps, rolling streaks), and stores to `tech_indicators`. Sets `is_finalized` based on whether NYSE has closed today. |
| `fundamental_fetcher.py` | Two HTTP calls per ticker: (1) Yahoo `quoteSummary` for market cap, forward PE, P/B, margins, sector, next earnings date, insider activity; (2) Yahoo `v8/timeseries` for GAAP quarterly/annual EPS and revenue history. Stores to `fundamentals`. |
| `earnings_fetcher.py` | Scrapes Finviz `/calendar/earnings` per trading day. Stores EPS estimates/actuals/surprises and 1D price reactions to `earnings_history`. Also computes extended post-earnings metrics (5D px/vol, rolling averages) from `price_history`. |
| `indicators.py` | Pure scoring logic — no I/O. Takes `tech` + `fund` + `peer_data` dicts, returns `{T1..F6: {pass: "PASS/PARTIAL/FAIL/NA", detail: {...}}}`. |
| `trigger_engine.py` | User-defined trigger conditions (e.g., "Daily Px% > 5") applied against `price_history`. Returns per-ticker price/volume returns between trigger start and end dates. |
| `peers_fetcher.py` | Yahoo Finance `recommendationsbysymbol` endpoint → peer forward PE and P/B. In-memory cache per process. |
| `ai_analyzer.py` | Calls Gemini 2.5 Flash with a structured prompt. Requires `GEMINI_API_KEY` or `GOOGLE_API_KEY` in `.env`. |
| `column_catalog.py` | Authoritative column reference used to render the "Column Reference" tab in the UI. **Must be kept in sync** whenever columns are added/renamed in `app.py`. |
| `market_calendar.py` | NYSE calendar via `pandas_market_calendars`. `et_today()` always returns Eastern-timezone date regardless of user's local clock. |
| `vpn_switcher.py` | Mullvad CLI wrapper. Used in bulk scan batches to rotate IP between yfinance request groups to reduce rate-limiting. Optional — gracefully skips if `mullvad` not in PATH. |
| `db.py` | Legacy SQLite schema and query helpers. **Not actively used** — kept as historical reference and because `main.py` still imports it. |
| `data_fetcher.py` | Original combined tech+fundamental fetcher using `yfinance`. Still used by `main.py` CLI and as a fallback in `app.py` for single-ticker technical fetches. |

---

## Database

**Primary: `stock_analysis_v2.duckdb`** (local, untracked)

Key tables:

| Table | Key | Contents |
|-------|-----|----------|
| `tech_indicators` | `(ticker, as_of_date)` | Full extended technical indicator set per trading day |
| `price_history` | `(ticker, date)` | Raw OHLCV rows extracted during technical fetch |
| `fundamentals` | `(ticker, fetch_date)` | Raw fundamental data + full `raw_info_json` |
| `analysis_runs` | `(run_dt, ticker)` | Pass/fail summary for each 10-indicator scan run |
| `analysis_details` | `(run_dt, ticker, indicator_id)` | Detail JSON per indicator per run |
| `peer_cache` | `ticker` | Cached peer valuations (forward PE, P/B) |
| `earnings_history` | `(ticker, earnings_date)` | Finviz earnings data + extended price/vol metrics |
| `earnings_fetch_log` | `date` | Which trading days have been scraped |

**Legacy: `stock_analysis.db`** (SQLite — migration to DuckDB completed Feb 2026; this file may still exist locally but is no longer written to)

---

## The 10 Indicators

All scored as `PASS / PARTIAL / FAIL / NA`. Sub-indicators individually scored as `PASS / FAIL / NA`.

| ID | Name | Logic |
|----|------|-------|
| T1 | Daily Price & Volume | Latest 63D avg vs prior 63D ending 3M and 12M ago |
| T2 | Weekly Price & Volume | Same as T1 but on weekly bars (W-FRI resample) |
| T3 | MA Alignment | SMA10 > SMA20 > SMA50 > SMA150 > SMA200 |
| T4 | Big Moves (90D) | ≥1 day up ≥10%; zero days down ≥10% |
| F1 | Q Profitability | Latest quarter: positive revenue AND positive EPS |
| F2 | Annual Profitability | Latest fiscal year: positive revenue AND positive EPS |
| F3 | Q YoY Growth | Revenue YoY > +10%; EPS YoY > +30% |
| F4 | Annual YoY Growth | Same thresholds on annual figures |
| F5 | Forward PE vs Peers | Ticker's forward PE ≤ peer median (binary) |
| F6 | P/B vs Peers | Ticker's P/B ≤ peer median (binary) |

Scoring: `PASS=1, PARTIAL=0.5, FAIL/NA=0` → total score 0–10 used for scan ranking.

---

## Tech Stack & Dependencies

| Package | Purpose |
|---------|---------|
| `streamlit` | Web UI |
| `duckdb` | Primary database |
| `yfinance` | Stock price/fundamental data |
| `pandas`, `numpy` | Data manipulation |
| `ta` | Technical indicator calculations (RSI, MACD, BBands, etc.) |
| `pandas-market-calendars` | NYSE trading calendar |
| `google-genai` | Gemini AI analysis |
| `requests` | Finviz HTTP scraping |
| `python-dotenv` | `.env` loading |
| `rich`, `tabulate` | CLI output formatting |
| `altair` | Charts in Streamlit |

---

## Environment Variables

Create a `.env` file in the project root:

```
GEMINI_API_KEY=your_key_here   # required for AI analysis tab
# GOOGLE_API_KEY=...           # alternative to GEMINI_API_KEY
# FMP_API_KEY=...              # Financial Modeling Prep — currently unused
```

---

## Coding Conventions & Patterns

- **Timestamps in CST** (`ZoneInfo("America/Chicago")`). All stored timestamps are CST. NYSE calendar logic uses ET (`ZoneInfo("America/New_York")`).
- **`_safe()` helpers** normalise floats to `None` for NaN/Inf — always use these before storing computed values.
- **Indicator result shape**: `{"pass": "PASS"|"PARTIAL"|"FAIL"|"NA", "detail": {..., "sub_checks": {...}}}`. Sub-checks are `bool | None`.
- **DuckDB concurrency**: DuckDB allows one writer at a time. Don't run multiple Streamlit instances or heavy write scripts simultaneously.
- **Bulk yfinance downloads**: `yf.download(tickers, group_by="ticker")` returns a MultiIndex DataFrame. Single-ticker downloads return flat columns. Both shapes must be handled.
- **`is_finalized`** in `tech_indicators`: `False` during the trading day, set to `True` after NYSE 4pm ET close. `technical_fetcher.refetch_unfinalized()` re-fetches rows where this is False.
- **Column catalog**: `column_catalog.py` is the single source of truth for UI column documentation. Update it whenever columns are added or removed in `app.py`'s `VALUE_COL_GROUPS`.
- **Status emojis**: `{"PASS": "✅", "PARTIAL": "⭕", "FAIL": "❌", "NA": "⚪️"}`. User watch-list statuses: `["", "必買", "買", "等", "研究", "X"]`.
- **User preferences** persisted to `user_prefs.json` (gitignored). Loaded at startup, saved on change.

---

## Known Quirks & Constraints

### File Location
**Always work in `~/Code/stock_analysis_tool`**, never in iCloud-synced folders. iCloud's "Optimize Mac Storage" offloads source files to cloud-only placeholders causing `ImportError`.

### Git Safety
- `*.duckdb`, `*.duckdb.wal`, `*.duckdb.tmp` are gitignored. **Never force-add them.**
- **Never run `git clean -fdx`** — it will delete `stock_analysis_v2.duckdb` (the entire database).
- If git crashes with "Bus Error" and leaves `index.lock` behind: `rm .git/index.lock && git reset --mixed HEAD`.

### FMP API Key
`config.py` loads `FMP_API_KEY` from `.env` but it is **not actively used** anywhere in the current codebase. Financial Modeling Prep was an earlier data source that has been replaced by yfinance + Yahoo HTTP APIs.

### `db.py` / SQLite (Legacy)
`db.py` defines the old SQLite schema and is still imported by `main.py`. The database `stock_analysis.db` is no longer written to. All new data goes to `stock_analysis_v2.duckdb` via `storage.py`.

### `migrate.py`
Disabled with an early `sys.exit(0)`. The SQLite→DuckDB migration ran in February 2026 (9 486 runs, 94 860 detail rows migrated). Do not re-enable.

### `data_fetcher.py` vs `technical_fetcher.py` / `fundamental_fetcher.py`
`data_fetcher.py` is the original combined fetcher (still used by `main.py` CLI and for single-ticker lookups in `app.py`). `technical_fetcher.py` and `fundamental_fetcher.py` are the newer, richer replacements that write to DuckDB and are used by the Streamlit scan flow.

### yfinance Rate Limiting
The fundamental fetcher retries with delays `[5, 10, 20]` seconds on 401/rate-limit responses. The VPN switcher (`vpn_switcher.py`) can rotate the Mullvad exit node between batch groups to avoid IP-level blocks. It is optional and silently skips if `mullvad` is not in PATH.

### Earnings Fetcher Timing
BMO (before market open) earnings: 1D change = close(earnings_day) vs close(prior_day).
AMC (after market close) earnings: 1D change = close(next_day) vs close(earnings_day).
The fetcher re-processes dates where `one_day_change IS NULL` on subsequent runs to catch AMC next-day prices.

### tickers.txt
Large comma-separated file of ~2 500 US tickers used for bulk scan mode. `all_tickers.txt` is a similar list. Neither is actively managed — they are reference lists for the scan queue.
