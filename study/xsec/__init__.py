"""Full-universe cross-sectional study: do fundamentals explain price action?

Covers every ticker in `tickers.txt` with usable price history from 2024, and
asks — window by window and sector by sector — how much of the cross-sectional
variation in returns the earnings record accounts for.

Read-only with respect to DuckDB: it opens the store with `read_only=True`,
adds no rows and alters no schema. All output is CSV under `study/out_xsec/`.

    uv run python -m study.xsec.run
"""
