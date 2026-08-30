"""One-off backfill: populate fundamentals.financial_currency from the
full-watchlist currency check run on 2026-08-23 (currency_results.json).

Why: financial_currency was added mid-stream -- tickers not re-scanned since
then have NULL, and the display-time FX conversion deliberately falls back
to "unconverted" for NULL (safe default), which leaves all ~220 non-USD
tickers showing raw local-currency figures until their next scan. This
backfill closes that gap at once instead of waiting for scans.

Only fills NULLs -- never overwrites a value written by a real scan (the
scan-fetched value is fresher/authoritative). Then refreshes fx_rates so
every newly visible currency has a conversion rate.

Run with the Streamlit app STOPPED (needs the DuckDB write lock):
    uv run python backfill_currency.py
Safe to re-run; second run is a no-op. Delete this file after use.
"""
import json
from pathlib import Path

import storage
from fx_rates import refresh_fx_rates

RESULTS_JSON = Path(
    "/private/tmp/claude-501/-Users-leonyip-Code-stock-analysis-tool/"
    "617304a0-4f17-4ea4-b78f-547adbda6e62/scratchpad/currency_results.json"
)


def main() -> None:
    data = json.loads(RESULTS_JSON.read_text())
    valid = {
        t: c for t, c in data.items()
        if c and c != "EMPTY" and not str(c).startswith("ERROR")
    }
    print(f"Loaded {len(valid)} ticker->currency mappings ({len(data) - len(valid)} skipped: no data)")

    storage.init_db()
    con = storage._conn()

    before = con.execute(
        "SELECT COUNT(DISTINCT ticker) FROM fundamentals WHERE financial_currency IS NULL"
    ).fetchone()[0]
    print(f"Tickers with NULL financial_currency before: {before}")

    updated = 0
    for ticker, currency in valid.items():
        cur = con.execute(
            "UPDATE fundamentals SET financial_currency = ? "
            "WHERE ticker = ? AND financial_currency IS NULL",
            [currency, ticker],
        )
        updated += 1
    print(f"Applied {updated} ticker updates (NULL rows only; scan-written values untouched)")

    after = con.execute(
        "SELECT COUNT(DISTINCT ticker) FROM fundamentals WHERE financial_currency IS NULL"
    ).fetchone()[0]
    non_usd = con.execute(
        "SELECT financial_currency, COUNT(DISTINCT ticker) FROM fundamentals "
        "WHERE financial_currency IS NOT NULL AND financial_currency != 'USD' "
        "GROUP BY financial_currency ORDER BY 2 DESC"
    ).fetchall()
    print(f"Tickers with NULL financial_currency after: {after}")
    print("Non-USD currency counts:", dict(non_usd))

    spot = con.execute(
        "SELECT DISTINCT financial_currency FROM fundamentals WHERE ticker = 'CIG-C'"
    ).fetchall()
    print(f"CIG-C financial_currency now: {[r[0] for r in spot]}")

    del con  # release the cursor/lock before fx refresh opens its own

    print("\nRefreshing FX rates for all currencies now in use...")
    n = refresh_fx_rates(log=print)
    print(f"Done. {n} currencies have fresh rates.")


if __name__ == "__main__":
    main()
