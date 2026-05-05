# Stock Analysis Tool

A Streamlit-based stock screening and analysis tool. Scores US equities across 10 technical and fundamental indicators, stores results in DuckDB, and displays them in an interactive multi-tab web UI.

> **Full documentation is in [CLAUDE.md](CLAUDE.md)** — architecture, file roles, indicator logic, coding conventions, and known constraints.

## Quick Start

```bash
uv sync
uv run streamlit run app.py
```

Open http://localhost:8501 in your browser.

## Environment Variables

Create a `.env` file in the project root:

```
GEMINI_API_KEY=your_key_here   # required for AI analysis tab
```

`FMP_API_KEY` is no longer used.

## Requirements

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) package manager
- `stock_analysis_v2.duckdb` (local, not tracked in git)
