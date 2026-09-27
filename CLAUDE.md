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

# Daily market close newsletter (archives HTML; --email sends it)
uv run python daily_email/daily_close.py --email
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

daily_email/    ← Daily market-close newsletter (standalone CLI, not a UI tab)
   ├── daily_close.py     ← Entry point: builds, archives and emails the report
   ├── news_fetcher.py    ← Google News RSS headlines (48h window)
   ├── mailer.py          ← Gmail SMTP sender
   ├── soxx_holdings.txt  ← Hand-maintained SOXX constituent list
   ├── com.leon.dailyclose.plist ← launchd job (weekdays 16:45 ET)
   └── reports/           ← Archived HTML + logs + universe cache (gitignored)

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
| `daily_email/daily_close.py` | Daily post-close email newsletter: index board (incl. gold/crypto), SOXL RSI + constituent movers, and top-10/bottom-10 $10B+ movers with sector, industry, AI reason and 48h news. Fetches **all prices live from yfinance** and opens DuckDB **read-only** for market cap/sector/industry only. Reads shared `config.py` / `market_calendar.py` from the repo root via a `sys.path` append. Scheduled by `daily_email/com.leon.dailyclose.plist`. |
| `daily_email/news_fetcher.py` | Google News RSS search with the `when:<N>h` recency operator. Never raises — a failed fetch returns `[]`. |
| `daily_email/mailer.py` | Gmail SMTP (`smtp.gmail.com:465`) multipart HTML+text sender. Needs a Google **App Password**, not the account password. `recipients()` reads `recipients.json`, falling back to `NEWSLETTER_TO`. |
| `daily_email/newsletter_control.py` | Start/stop the launchd schedule, report its health, edit recipients, and launch detached manual runs. Drives the sidebar panel; no Streamlit import. |
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
GEMINI_API_KEY=your_key_here   # AI analysis tab; also the newsletter's "why it moved" lines
# GOOGLE_API_KEY=...           # alternative to GEMINI_API_KEY
# FMP_API_KEY=...              # Financial Modeling Prep — currently unused

# Daily close newsletter (daily_close.py)
GMAIL_USER=you@gmail.com
GMAIL_APP_PASSWORD=xxxxxxxxxxxxxxxx   # 16-char Google App Password, requires 2FA
NEWSLETTER_TO=you@gmail.com
```

See `.env.example` for a copy-paste template.

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

### Daily Close Newsletter
`daily_close.py` deliberately does **not** read prices from DuckDB: `price_history` contains no
ETFs, the `asset_type='etf'` rows in `tech_indicators` lag the stock rows by days, and
`GC=F`/`BTC-USD`/`ETH-USD` are absent entirely. All quotes come from yfinance at run time.

**DuckDB locking:** `read_only=True` does *not* bypass DuckDB's file lock — a running Streamlit app
holds a writable connection for its whole lifetime and every other connection is refused. The
newsletter therefore caches the $10B+ universe (ticker → market cap/sector/industry) to
`daily_email/reports/universe_cache.json` whenever the DB is readable, and falls back to that cache when it is
locked. It never calls `storage._conn()`, which would stall 60 s behind a lock that is never
released.

`daily_email/soxx_holdings.txt` is **hand-maintained** — every free full-holdings source is blocked (iShares
serves HTML, stockanalysis.com 403s, yfinance caps at 10 names). Refresh it after each quarterly
rebalance; the script logs a `DRIFT` warning when SOXX's yfinance top-10 contains a name the file
lacks.

**The NYSE calendar and Yahoo's data disagree.** `last_completed_trading_day()` can report a
session closed hours before Yahoo publishes its bars (2026-09-22: still a day stale at 21:00 ET).
Sending on the calendar alone mails the PREVIOUS session under today's heading. Three guards:
a staleness check (refuses to send when the newest bar predates the last completed session), a
duplicate check (`reports/sent_sessions.txt` records every emailed session), and a no-clobber
check (never overwrite an archived report with a smaller one). `--force` bypasses all three.
The duplicate check runs *before* the ~5 s `probe_session()` board fetch, so the four attempts
per day that have nothing to do cost no HTTP at all — which is why the launchd job fires
**five times per weekday** (16:45, 17:30, 18:30, 20:00, plus 08:00 next morning as catch-up)
rather than once. Exactly one newsletter goes out per session regardless.

**Catch-up is by session, not by date (changed Sep 2026).** There used to be a fourth gate that
refused to send unless the last completed session was *today* (ET). It silently destroyed any
session the Mac slept through: the run that fires on wake sees yesterday's session and bailed,
and the 08:00 "catch-up" slot could never work, because at 08:00 the last completed session is
always the previous day. 2026-09-25 was lost exactly this way (hibernated Friday 04:47 at 1%
battery, woke Saturday 11:25). The gate is gone — any completed session not in
`sent_sessions.txt` is sent whenever a run next fires, and running before today's close is still
a no-op because `last_completed_trading_day()` returns yesterday until 16:00 ET and yesterday is
already in the sent log. **A missed session therefore needs no `--force`**; reserve that flag for
deliberately re-sending one that *was* already emailed.

**`reports/run_state.json`** records how the last run ended (`sent` / `skipped` / `error`, plus
session, detail and exit code), written on every exit path and from the `__main__` guard when
`main()` raises. It exists because the exit code cannot carry this — a gate skip and a successful
send both return 0 — and because an unhandled exception never reaches `_flush_log()`, leaving the
log showing the *previous* run. The Streamlit sidebar reads it to decide whether to report that
the newsletter died. Log stamps are `MM-DD HH:MM:SS` in **CST** while the plist schedules in ET.

**Recipients: `daily_email/recipients.json` wins over `NEWSLETTER_TO`.** `mailer.recipients()`
reads the JSON file when it holds any addresses and falls back to `.env` otherwise, so a fresh
clone behaves as it always did. The file is gitignored (personal addresses), written atomically
(temp + replace, since a launchd run may read it mid-write) and edited from the sidebar. All
recipients go on one `To` header — one message, one SMTP transaction; `sent_sessions.txt` stays
per-session, not per-address.

**Start/stop and manual sends live in the Streamlit sidebar** ("📧 Daily Newsletter"), backed by
`daily_email/newsletter_control.py`. Stop does `launchctl bootout` **and** `disable` — `bootout`
alone is undone at the next login, since `~/Library/LaunchAgents` is bootstrapped again. Note the
installed plist is a *copy*; the panel shows 🟠 when it drifts from the repo's. A manual run is a
detached `Popen` (`start_new_session=True`, wrapped in `caffeinate -i`) whose pid is recorded in
`reports/manual_run.json`, so an in-flight run survives a Streamlit rerun, reconnect or restart.
Because the app holds the only writable DuckDB connection, it refreshes `universe_cache.json`
through its own connection before spawning the subprocess.

**Daily % change comes from the quote endpoint, never the daily bars.**
`v7/finance/quote`'s `regularMarketChangePercent` carries the exchange's own previous close.
yfinance's historical array publishes a session's bar with a NULL close for hours and has been
seen to regress a real close back to null; `dropna()` then compares today against the session
*before* yesterday. Observed live on 2026-09-23: XLK reported +0.25% (measured vs 09-21) when the
true move was -0.47%, and QQQ showed the previous day's price outright. The endpoint needs Yahoo's
cookie/crumb handshake, so it is called through `yfinance.data.YfData` (a bare request returns
401). It also batches ~100 symbols per call, making the large-cap sweep ~5 s instead of ~90 s.
Order of preference: quote endpoint -> `price_history` -> daily bars.

**The news phase runs as a subprocess.** Yahoo's feed needs yfinance's `curl_cffi` transport
(plain urllib gets 429), and `libcurl-impersonate` has aborted the interpreter outright — SIGABRT
inside `SSL_write`, exit 134 — when Yahoo throttles, which it does hardest right after the close.
A native abort is uncatchable in Python, so `news_fetcher.py` is invoked via `subprocess` and a
crash costs only the reasons, not the newsletter. Article bodies are fetched in ONE shared pool
across all tickers with a wall-clock deadline; per-ticker pools ran serially at ~40 s each and
overran the timeout.

**Gemini 503s are common.** Retries are bounded by a total deadline (`REASON_DEADLINE_S`) with
growing backoff across a model list, not a fixed per-model count — a 3-attempt budget burned out
in 90 s during a real run. Non-transient errors (400s) drop that model immediately.

Roughly 15–20 of the ~930 screened tickers return no Yahoo data on a given day (delistings, buyouts
and renames the local `fundamentals` table has not caught up with). They are dropped and the count
is printed in the newsletter footer.

### tickers.txt
Large comma-separated file of ~2 500 US tickers used for bulk scan mode. `all_tickers.txt` is a similar list. Neither is actively managed — they are reference lists for the scan queue.
