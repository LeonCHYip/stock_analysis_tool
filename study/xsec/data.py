"""Loading the three raw panels: universe tags, daily prices, and earnings.

Read-only throughout. The study never writes to DuckDB and never alters a
schema; everything it needs already exists in `price_history`,
`earnings_history` and `fundamentals`.
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path

import duckdb
import pandas as pd

from . import config


def connect(db_path: str | None = None) -> duckdb.DuckDBPyConnection:
    """Read-only DuckDB connection.

    `STUDY_DB` overrides the path, which is what lets the study run against a
    file copy while the Streamlit app holds the write lock on the live store
    (DuckDB allows a single process at a time, even for readers).
    """
    path = db_path or os.environ.get("STUDY_DB") or str(config.DB_PATH)
    return duckdb.connect(path, read_only=True)


def read_universe_file(path=None) -> list[str]:
    """Tickers from `tickers.txt` (comma- and/or whitespace-separated)."""
    raw = (path or config.UNIVERSE_FILE).read_text()
    return sorted({t.strip().upper() for t in re.split(r"[,\s]+", raw) if t.strip()})


def _parse_raw_info(j: str | None) -> dict:
    """Parse `fundamentals.raw_info_json`.

    A few rows hold two concatenated JSON documents, which makes a plain
    `json.loads` raise; fall back to the first document rather than losing the
    ticker's sector tag entirely.
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


def load_tags(con, tickers: list[str]) -> pd.DataFrame:
    """Sector / industry / market cap from each ticker's latest fundamentals row.

    Adds a `group` column: the Yahoo sector, except that semiconductor names
    are lifted out of Technology into their own group. Leaving them inside
    Technology would blend the study's main subject into software, whose 2024H2
    path was quite different.
    """
    rows = con.execute(
        """
        WITH latest AS (
            SELECT ticker, max(fetch_date) AS fd
            FROM fundamentals
            WHERE ticker IN (SELECT * FROM UNNEST(?))
            GROUP BY 1
        )
        SELECT f.ticker, f.raw_info_json, f.market_cap
        FROM fundamentals f
        JOIN latest l ON f.ticker = l.ticker AND f.fetch_date = l.fd
        """,
        [tickers],
    ).fetchall()

    recs = []
    for ticker, raw, mcap in rows:
        d = _parse_raw_info(raw)
        sector = d.get("sector") or "Unknown"
        industry = d.get("industry") or ""
        group = (
            config.SEMI_GROUP
            if config.SEMI_INDUSTRY_MATCH in industry.lower()
            else sector
        )
        recs.append(
            {
                "ticker": ticker,
                "sector": sector,
                "industry": industry,
                "group": group,
                "market_cap": mcap,
            }
        )
    df = pd.DataFrame(recs)
    if df.empty:
        return pd.DataFrame(
            columns=["ticker", "sector", "industry", "group", "market_cap"]
        )
    return df


def load_prices(con, tickers: list[str], start: str, end: str) -> pd.DataFrame:
    """Daily OHLCV for the universe.

    `close` is the raw (unadjusted) close and `adj_close` is adjusted for
    splits and dividends — both fetchers call `yf.download(auto_adjust=False)`.
    Keeping both is what makes the split detection in `technicals` possible.
    """
    df = con.execute(
        """
        SELECT ticker, date, close, adj_close, volume
        FROM price_history
        WHERE ticker IN (SELECT * FROM UNNEST(?))
          AND date BETWEEN ? AND ?
        ORDER BY ticker, date
        """,
        [tickers, start, end],
    ).df()
    df["date"] = pd.to_datetime(df["date"]).astype("datetime64[ns]")
    return df


def load_earnings(con, tickers: list[str], start: str, end: str) -> pd.DataFrame:
    """Finviz earnings rows, one per (ticker, report date).

    `eps_act` / `eps_est` / `eps_sur` are the non-GAAP figures the estimates
    are stated on; the `*_gaap_*` trio is the reported basis. The study stays
    on the non-GAAP basis so that surprises and the growth rates they are
    compared against are measured the same way.
    """
    df = con.execute(
        """
        SELECT ticker, earnings_date, earnings_time,
               eps_est, eps_act, eps_sur,
               eps_gaap_est, eps_gaap_act, eps_gaap_sur,
               rev_est_m, rev_act_m, rev_sur,
               one_day_change, q_rev_yoy, q_eps_yoy
        FROM earnings_history
        WHERE ticker IN (SELECT * FROM UNNEST(?))
          AND earnings_date BETWEEN ? AND ?
        ORDER BY ticker, earnings_date
        """,
        [tickers, start, end],
    ).df()
    df["earnings_date"] = pd.to_datetime(df["earnings_date"]).astype("datetime64[ns]")
    return df


def load_staged_earnings(path, tickers: list[str]) -> pd.DataFrame:
    """Earnings rows from the Finviz staging file written by `backfill_2022`.

    The study reads the staging file directly rather than requiring it to be
    loaded into DuckDB first. That keeps the whole analysis runnable while the
    Streamlit app holds the database, and keeps the store strictly read-only
    unless the user chooses to run the separate `--load` step.
    """
    path = Path(path)
    if not path.exists():
        return pd.DataFrame()
    staged = json.loads(path.read_text())
    keep = set(tickers)
    recs = [
        {"ticker": ticker, **fields}
        for per_ticker in staged.values()
        for ticker, fields in per_ticker.items()
        if ticker in keep
    ]
    if not recs:
        return pd.DataFrame()
    df = pd.DataFrame(recs)
    df["earnings_date"] = pd.to_datetime(df["earnings_date"]).astype("datetime64[ns]")
    # Present in the table but never populated for these years; carried as
    # float NaN so the concat with the database rows keeps one dtype.
    for col in ("q_rev_yoy", "q_eps_yoy"):
        df[col] = float("nan")
    return df


def merge_earnings(db_rows: pd.DataFrame, staged: pd.DataFrame) -> pd.DataFrame:
    """Add staged reports the database does not already hold.

    The database always wins on a (ticker, date) it already has, so a rerun
    after the rows have been loaded into DuckDB gives exactly the same panel.
    """
    if staged.empty:
        return db_rows
    if db_rows.empty:
        return staged
    have = set(map(tuple, db_rows[["ticker", "earnings_date"]].to_numpy()))
    mask = [
        (t, d) not in have
        for t, d in staged[["ticker", "earnings_date"]].to_numpy()
    ]
    extra = staged[mask]
    if extra.empty:
        return db_rows
    out = pd.concat([db_rows, extra], ignore_index=True)
    return out.sort_values(["ticker", "earnings_date"]).reset_index(drop=True)


def resolve_universe(con, tickers: list[str], start: str,
                     min_rows: int = 250) -> list[str]:
    """Tickers already trading when the study window opens.

    Deliberately *not* a full-sample survivorship filter: a name that stops
    trading in 2025 still has a valid 2024 cross-section, and dropping it
    outright would bias every earlier window toward the survivors. Coverage
    inside each window is enforced window by window instead, in
    `technicals.window_stats`.
    """
    rows = con.execute(
        """
        SELECT ticker
        FROM price_history
        WHERE ticker IN (SELECT * FROM UNNEST(?))
        GROUP BY ticker
        HAVING min(date) <= ? AND count(*) >= ?
        ORDER BY ticker
        """,
        [tickers, start, min_rows],
    ).fetchall()
    return [r[0] for r in rows]
