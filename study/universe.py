"""Sector / industry universes, built from the latest `fundamentals` snapshot.

The study is deliberately sector-parameterised: the semiconductor pass and the
later financials/energy passes all go through `load_universe`.
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pandas as pd

DB_PATH = Path(__file__).resolve().parent.parent / "stock_analysis_v2.duckdb"


def connect() -> duckdb.DuckDBPyConnection:
    """Read-only connection. The study never writes."""
    return duckdb.connect(str(DB_PATH), read_only=True)


def _parse_raw(j: str | None) -> dict:
    """Parse `fundamentals.raw_info_json`.

    A handful of rows contain two concatenated JSON documents, which makes
    plain `json.loads` (and DuckDB's `json_extract_string`) raise. Fall back to
    the first document.
    """
    if not j:
        return {}
    try:
        return json.loads(j)
    except Exception:
        try:
            return json.loads(j[: j.index("}{") + 1])
        except Exception:
            return {}


def load_tags(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """One row per ticker from its most recent fundamentals fetch.

    Returns columns: ticker, sector, industry, market_cap.
    """
    rows = con.execute(
        """
        WITH latest AS (
            SELECT ticker, max(fetch_date) AS fd FROM fundamentals GROUP BY 1
        )
        SELECT f.ticker, f.raw_info_json, f.market_cap
        FROM fundamentals f
        JOIN latest l ON f.ticker = l.ticker AND f.fetch_date = l.fd
        """
    ).fetchall()

    recs = []
    for ticker, raw, mcap in rows:
        d = _parse_raw(raw)
        recs.append(
            {
                "ticker":     ticker,
                "sector":     d.get("sector"),
                "industry":   d.get("industry"),
                "market_cap": mcap,
            }
        )
    return pd.DataFrame(recs)


def load_universe(
    con: duckdb.DuckDBPyConnection,
    sector: str | None = None,
    industry_contains: str | None = None,
    min_market_cap: float | None = None,
) -> list[str]:
    """Tickers matching a sector and/or an industry substring, largest first."""
    df = load_tags(con)
    if sector:
        df = df[df["sector"] == sector]
    if industry_contains:
        df = df[
            df["industry"].fillna("").str.contains(industry_contains, case=False)
        ]
    if min_market_cap is not None:
        df = df[df["market_cap"].fillna(0) >= min_market_cap]
    return (
        df.sort_values("market_cap", ascending=False, na_position="last")["ticker"]
        .tolist()
    )


# Named groups used by the study. `industry_contains="Semiconductor"` catches
# both "Semiconductors" and "Semiconductor Equipment & Materials".
GROUP_SPECS: dict[str, dict] = {
    "Semiconductors":     {"industry_contains": "Semiconductor"},
    "Financial Services": {"sector": "Financial Services"},
    "Energy":             {"sector": "Energy"},
}


def load_groups(
    con: duckdb.DuckDBPyConnection, names: list[str]
) -> dict[str, list[str]]:
    """Resolve group names to ticker lists.

    A name not in GROUP_SPECS is treated as a sector name, so the study extends
    to any sector without a code change.
    """
    out: dict[str, list[str]] = {}
    for name in names:
        spec = GROUP_SPECS.get(name, {"sector": name})
        out[name] = load_universe(con, **spec)
    return out
