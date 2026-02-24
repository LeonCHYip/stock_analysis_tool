"""
db.py — SQLite persistence.

Schema v2 changes:
  - indicator values stored as PASS/PARTIAL/FAIL/NA strings
  - added sub-indicator columns for all indicators
  - added comments TEXT, market_cap REAL columns
  - all timestamps in CST
  - update_field() allows saving any cell
"""

import sqlite3
import json
from pathlib import Path
from zoneinfo import ZoneInfo

DB_PATH = Path(__file__).parent / "stock_analysis.db"
CST     = ZoneInfo("America/Chicago")

# ── Sub-indicator DB columns ──────────────────────────────────────────────────
# These must match the keys in indicators.py sub_checks dicts
SUB_COLS = {
    "T1": ["T1_sub_3m_price", "T1_sub_3m_vol", "T1_sub_12m_price", "T1_sub_12m_vol"],
    "T2": ["T2_sub_3m_price", "T2_sub_3m_vol", "T2_sub_12m_price", "T2_sub_12m_vol"],
    "T3": ["T3_sub_ma10_20", "T3_sub_ma20_50", "T3_sub_ma50_150", "T3_sub_ma150_200"],
    "T4": ["T4_sub_has_big_up", "T4_sub_no_big_down"],
    "F1": ["F1_sub_q_rev", "F1_sub_q_eps"],
    "F2": ["F2_sub_a_rev", "F2_sub_a_eps"],
    "F3": ["F3_sub_q_rev_yoy", "F3_sub_q_eps_yoy"],
    "F4": ["F4_sub_a_rev_yoy", "F4_sub_a_eps_yoy"],
}

# Map sub col name → the sub_checks key in indicators.py
SUB_KEY_MAP = {
    "T1_sub_3m_price":    "3M Daily Price Up",
    "T1_sub_3m_vol":      "3M Daily Volume Up",
    "T1_sub_12m_price":   "12M Daily Price Up",
    "T1_sub_12m_vol":     "12M Daily Volume Up",
    "T2_sub_3m_price":    "3M Weekly Price Up",
    "T2_sub_3m_vol":      "3M Weekly Volume Up",
    "T2_sub_12m_price":   "12M Weekly Price Up",
    "T2_sub_12m_vol":     "12M Weekly Volume Up",
    "T3_sub_ma10_20":     "MA10>MA20",
    "T3_sub_ma20_50":     "MA20>MA50",
    "T3_sub_ma50_150":    "MA50>MA150",
    "T3_sub_ma150_200":   "MA150>MA200",
    "T4_sub_has_big_up":  "Big Up Days (≥+10%)",
    "T4_sub_no_big_down": "No Big Down Days (≥10% down)",
    "F1_sub_q_rev":       "Positive Q Revenue",
    "F1_sub_q_eps":       "Positive Q EPS",
    "F2_sub_a_rev":       "Positive Annual Revenue",
    "F2_sub_a_eps":       "Positive Annual EPS",
    "F3_sub_q_rev_yoy":   "Q Revenue YoY > +10%",
    "F3_sub_q_eps_yoy":   "Q EPS YoY > +30%",
    "F4_sub_a_rev_yoy":   "Annual Revenue YoY > +10%",
    "F4_sub_a_eps_yoy":   "Annual EPS YoY > +30%",
}

# All sub-cols flat list
ALL_SUB_COLS = [c for cols in SUB_COLS.values() for c in cols]

# Main indicator columns
MAIN_IND_COLS = ["T1","T2","T3","T4","F1","F2","F3","F4","F5","F6"]

# All summary table columns (excluding PK)
SUMMARY_COLS = (
    MAIN_IND_COLS + ALL_SUB_COLS +
    ["market_cap", "comments"]
)


def _col_defs() -> str:
    defs = ["analysis_datetime TEXT NOT NULL", "ticker TEXT NOT NULL"]
    for c in MAIN_IND_COLS:
        defs.append(f"{c} TEXT")
    for c in ALL_SUB_COLS:
        defs.append(f"{c} TEXT")
    defs.append("market_cap REAL")
    defs.append("comments TEXT")
    defs.append("PRIMARY KEY (analysis_datetime, ticker)")
    return ",\n    ".join(defs)


_DDL_SUMMARY = f"""
CREATE TABLE IF NOT EXISTS indicator_summary (
    {_col_defs()}
);
"""

_DDL_DETAIL = """
CREATE TABLE IF NOT EXISTS indicator_detail (
    analysis_datetime TEXT NOT NULL,
    ticker            TEXT NOT NULL,
    indicator_id      TEXT NOT NULL,
    detail_json       TEXT,
    PRIMARY KEY (analysis_datetime, ticker, indicator_id)
);
"""


def _conn():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    return con


def init_db():
    with _conn() as con:
        con.execute(_DDL_SUMMARY)
        con.execute(_DDL_DETAIL)
        # Migration: add missing columns if table existed before
        existing = {r[1] for r in con.execute("PRAGMA table_info(indicator_summary)")}
        for col in SUMMARY_COLS:
            if col not in existing:
                dtype = "REAL" if col == "market_cap" else "TEXT"
                con.execute(f"ALTER TABLE indicator_summary ADD COLUMN {col} {dtype}")


def _bool_to_str(v) -> str:
    """Convert True/False/None sub-check value to PASS/FAIL/NA."""
    if v is True:  return "PASS"
    if v is False: return "FAIL"
    return "NA"


def _sub_val(indicators: dict, ind_id: str, sub_col: str) -> str:
    key = SUB_KEY_MAP.get(sub_col)
    if not key:
        return "NA"
    sub = indicators.get(ind_id, {}).get("detail", {}).get("sub_checks", {})
    return _bool_to_str(sub.get(key))


def save_results(ticker: str, indicators: dict, analysis_dt: str,
                 market_cap: float | None = None):
    """Persist indicator results to DB."""
    row_vals = [analysis_dt, ticker]

    # Main indicators
    for ind in MAIN_IND_COLS:
        row_vals.append(indicators.get(ind, {}).get("pass", "NA"))

    # Sub-indicators
    for sub_col in ALL_SUB_COLS:
        # Determine parent indicator from col name prefix (e.g. T1_sub_... → T1)
        parent = sub_col.split("_")[0]
        row_vals.append(_sub_val(indicators, parent, sub_col))

    # market_cap, comments
    row_vals.append(market_cap)
    row_vals.append(None)  # comments default empty

    placeholders = ", ".join(["?"] * len(row_vals))
    col_names    = "analysis_datetime, ticker, " + ", ".join(SUMMARY_COLS)

    with _conn() as con:
        con.execute(
            f"INSERT OR IGNORE INTO indicator_summary ({col_names}) VALUES ({placeholders})",
            row_vals,
        )
        # Detail rows
        for ind_id, ind_data in indicators.items():
            con.execute(
                "INSERT OR REPLACE INTO indicator_detail VALUES (?,?,?,?)",
                (analysis_dt, ticker, ind_id,
                 json.dumps(ind_data.get("detail", {}), default=str)),
            )


def update_field(analysis_dt: str, ticker: str, column: str, value: str) -> bool:
    """
    Update any column in indicator_summary for a given row.
    Returns True on success.
    """
    allowed = set(MAIN_IND_COLS + ALL_SUB_COLS + ["comments"])
    if column not in allowed:
        return False
    with _conn() as con:
        con.execute(
            f"UPDATE indicator_summary SET {column}=? "
            "WHERE analysis_datetime=? AND ticker=?",
            (value, analysis_dt, ticker),
        )
    return True


# ─────────────────────────────────────────────────────────────────────────────
# Query helpers
# ─────────────────────────────────────────────────────────────────────────────

def get_all_run_datetimes() -> list[str]:
    with _conn() as con:
        rows = con.execute(
            "SELECT DISTINCT analysis_datetime FROM indicator_summary "
            "ORDER BY analysis_datetime DESC"
        ).fetchall()
    return [r["analysis_datetime"] for r in rows]


def get_latest_run_datetime() -> str | None:
    dts = get_all_run_datetimes()
    return dts[0] if dts else None


def get_summary_for_run(analysis_dt: str) -> list[dict]:
    with _conn() as con:
        rows = con.execute(
            "SELECT * FROM indicator_summary WHERE analysis_datetime=? ORDER BY ticker",
            (analysis_dt,),
        ).fetchall()
    return [dict(r) for r in rows]


def get_detail_for_run(analysis_dt: str) -> dict:
    """Returns {ticker: {indicator_id: detail_dict}}."""
    with _conn() as con:
        rows = con.execute(
            "SELECT ticker, indicator_id, detail_json FROM indicator_detail "
            "WHERE analysis_datetime=? ORDER BY ticker, indicator_id",
            (analysis_dt,),
        ).fetchall()
    result: dict = {}
    for r in rows:
        result.setdefault(r["ticker"], {})[r["indicator_id"]] = (
            json.loads(r["detail_json"]) if r["detail_json"] else {}
        )
    return result


def get_all_summaries(
    tickers: list[str] | None = None,
    datetimes: list[str] | None = None,
) -> list[dict]:
    """
    All summary rows filtered optionally by tickers and/or datetimes.
    Default order: newest datetime first, then alphabetical ticker.
    """
    where_clauses = []
    params: list = []

    if tickers:
        placeholders = ",".join(["?"] * len(tickers))
        where_clauses.append(f"ticker IN ({placeholders})")
        params.extend(tickers)
    if datetimes:
        placeholders = ",".join(["?"] * len(datetimes))
        where_clauses.append(f"analysis_datetime IN ({placeholders})")
        params.extend(datetimes)

    where = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

    with _conn() as con:
        rows = con.execute(
            f"SELECT * FROM indicator_summary {where} "
            "ORDER BY analysis_datetime DESC, ticker ASC",
            params,
        ).fetchall()
    return [dict(r) for r in rows]


def get_detail_filtered(
    ticker: str | None = None,
    analysis_dt: str | None = None,
) -> dict:
    """
    Returns {ticker: {indicator_id: detail_dict}}.
    Filters by ticker and/or analysis_dt as specified.
    """
    where_clauses = []
    params: list = []
    if ticker:
        where_clauses.append("ticker=?")
        params.append(ticker)
    if analysis_dt:
        where_clauses.append("analysis_datetime=?")
        params.append(analysis_dt)

    where = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

    with _conn() as con:
        rows = con.execute(
            f"SELECT ticker, indicator_id, detail_json FROM indicator_detail "
            f"{where} ORDER BY ticker, indicator_id",
            params,
        ).fetchall()
    result: dict = {}
    for r in rows:
        result.setdefault(r["ticker"], {})[r["indicator_id"]] = (
            json.loads(r["detail_json"]) if r["detail_json"] else {}
        )
    return result


def get_all_tickers() -> list[str]:
    with _conn() as con:
        rows = con.execute(
            "SELECT DISTINCT ticker FROM indicator_summary ORDER BY ticker"
        ).fetchall()
    return [r["ticker"] for r in rows]


def get_datetimes_for_ticker(ticker: str) -> list[str]:
    """All datetimes that have data for this ticker, newest first."""
    with _conn() as con:
        rows = con.execute(
            "SELECT DISTINCT analysis_datetime FROM indicator_summary "
            "WHERE ticker=? ORDER BY analysis_datetime DESC",
            (ticker,),
        ).fetchall()
    return [r["analysis_datetime"] for r in rows]


def get_tickers_for_datetime(analysis_dt: str) -> list[str]:
    """All tickers present in a given analysis datetime, alphabetically."""
    with _conn() as con:
        rows = con.execute(
            "SELECT DISTINCT ticker FROM indicator_summary "
            "WHERE analysis_datetime=? ORDER BY ticker",
            (analysis_dt,),
        ).fetchall()
    return [r["ticker"] for r in rows]
