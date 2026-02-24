# Stock Analysis Tool

A Streamlit-based stock screening tool that evaluates stocks across 10 technical
and fundamental indicators, stores results in SQLite, and displays summary and
detail tables in an interactive web UI.

## Requirements
- Python 3.11+  
- [uv](https://docs.astral.sh/uv/) package manager

## Setup

```bash
cd stock_analyzer
uv sync
```

The `.env` file already contains the FMP API key. To update it:
```
FMP_API_KEY=your_key_here
```

## Run

```bash
uv run streamlit run app.py
```

Then open http://localhost:8501 in your browser.

## Using the App

1. Enter comma-separated tickers in the **sidebar** (e.g. `AAPL, TSLA, MU`)
2. Click **▶ Analyse**
3. View results in two tabs:
   - **📊 Latest Analysis** — summary + expandable detail for the current run
   - **🗂 History** — all past runs stored in the DB; select any run to inspect

## Indicators

### Technical (T1–T4)

| ID   | Indicator | Pass Condition |
|------|-----------|----------------|
| T1   | Daily Price & Volume vs History | 5-day avg price **and** volume both ↑ vs 3M ago **and** 12M ago |
| T2   | Weekly Price & Volume vs History | 4-week avg price **and** volume both ↑ vs 3M ago **and** 12M ago |
| T3   | MA Alignment | MA10 > MA20 > MA50 > MA150 > MA200 (4 pairwise sub-checks) |
| T4   | Big Moves (90-day ≥10%) | ≥1 day with +10% gain **and** zero days with −10% drop |

### Fundamental (F1–F6)

| ID   | Indicator | Pass Condition |
|------|-----------|----------------|
| F1   | Latest Quarter Profitability | Revenue > 0 **and** EPS > 0 |
| F2   | Latest Year Profitability | Revenue > 0 **and** EPS > 0 |
| F3   | Quarter YoY Growth (same quarter) | Revenue YoY > +10% **and** EPS YoY > +30% |
| F4   | Annual YoY Growth | Revenue YoY > +10% **and** EPS YoY > +30% |
| F5   | Forward PE vs Peers | Ticker Fwd PE ≤ peer median |
| F6   | P/B Ratio vs Peers | Ticker P/B ≤ peer median |

> **N/A** is shown when the required data is unavailable from the data source.

### F3 Same-Quarter YoY Note
F3 compares the **latest reported quarter** against the **same calendar quarter one year prior**  
(e.g., 2026-Q1 vs 2025-Q1), not the immediately preceding quarter (2025-Q4).  
It uses date-matching with a ±46-day tolerance window on yfinance quarterly columns.

## Data Sources
| Data | Source |
|------|--------|
| Price / technical | yfinance (3-year daily, resampled weekly in-memory) |
| Fundamentals | yfinance quarterly & annual financials |
| Peer tickers | FMP `/stable/stock-peers` API |
| Peer valuations | yfinance (per-peer forward PE and P/B) |

## Storage
Results are saved to `stock_analysis.db` (SQLite):

| Table | Primary Key | Contents |
|-------|-------------|----------|
| `indicator_summary` | `(analysis_datetime, ticker)` | PASS/FAIL/N/A per indicator column |
| `indicator_detail`  | `(analysis_datetime, ticker, indicator_id)` | JSON detail blob |

## Project Structure
```
stock_analyzer/
├── app.py             # Streamlit UI (main entry point)
├── data_fetcher.py    # yfinance technical + fundamental fetching
├── peers_fetcher.py   # FMP peers API + yfinance peer valuation
├── indicators.py      # All 10 indicator evaluation logic
├── db.py              # SQLite persistence + read helpers
├── config.py          # Loads FMP_API_KEY from .env
├── .env               # API key
└── pyproject.toml     # uv-managed dependencies
```
