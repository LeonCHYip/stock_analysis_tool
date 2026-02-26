"""
storage.py — DuckDB persistence for V2 stock analysis tool.

Tables:
  tech_indicators  — computed technical indicators per (ticker, as_of_date)
  fundamentals     — fundamental data per (ticker, fetch_date)
  analysis_runs    — indicator pass/fail results per (run_dt, ticker)
  analysis_details — detail JSON per (run_dt, ticker, indicator_id)
  peer_cache       — peer valuation cache per ticker

Public API mirrors db.py so app.py can switch with minimal changes.
"""

from __future__ import annotations
import json
import threading
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

import duckdb

DB_PATH = Path(__file__).parent / "stock_analysis_v2.duckdb"
CST = ZoneInfo("America/Chicago")

# ── Indicator column definitions (mirror db.py) ───────────────────────────────
MAIN_IND_COLS = ["T1", "T2", "T3", "T4", "F1", "F2", "F3", "F4", "F5", "F6"]

SUB_COLS: dict[str, list[str]] = {
    "T1": ["T1_sub_3m_price", "T1_sub_3m_vol", "T1_sub_12m_price", "T1_sub_12m_vol"],
    "T2": ["T2_sub_3m_price", "T2_sub_3m_vol", "T2_sub_12m_price", "T2_sub_12m_vol"],
    "T3": ["T3_sub_ma10_20", "T3_sub_ma20_50", "T3_sub_ma50_150", "T3_sub_ma150_200"],
    "T4": ["T4_sub_has_big_up", "T4_sub_no_big_down"],
    "F1": ["F1_sub_q_rev", "F1_sub_q_eps"],
    "F2": ["F2_sub_a_rev", "F2_sub_a_eps"],
    "F3": ["F3_sub_q_rev_yoy", "F3_sub_q_eps_yoy"],
    "F4": ["F4_sub_a_rev_yoy", "F4_sub_a_eps_yoy"],
}

SUB_KEY_MAP: dict[str, str] = {
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

ALL_SUB_COLS = [c for cols in SUB_COLS.values() for c in cols]
SUMMARY_COLS = MAIN_IND_COLS + ALL_SUB_COLS + ["market_cap", "comments"]


# ── DDL ───────────────────────────────────────────────────────────────────────

_DDL_TECH = """
CREATE TABLE IF NOT EXISTS tech_indicators (
    ticker              TEXT    NOT NULL,
    as_of_date          DATE    NOT NULL,
    -- Core OHLCV
    close               DOUBLE,
    volume              BIGINT,
    -- Moving averages
    sma10               DOUBLE,
    sma20               DOUBLE,
    sma50               DOUBLE,
    sma150              DOUBLE,
    sma200              DOUBLE,
    ema9                DOUBLE,
    ema21               DOUBLE,
    ema50_e             DOUBLE,
    ema200              DOUBLE,
    -- MA checks / slope
    ma10_gt_ma20        BOOLEAN,
    ma20_gt_ma50        BOOLEAN,
    ma50_gt_ma150       BOOLEAN,
    ma150_gt_ma200      BOOLEAN,
    sma50_slope_20d     DOUBLE,
    pct_from_sma200     DOUBLE,
    -- Trend
    rsi14               DOUBLE,
    macd_line           DOUBLE,
    macd_signal         DOUBLE,
    macd_hist           DOUBLE,
    bb_upper            DOUBLE,
    bb_middle           DOUBLE,
    bb_lower            DOUBLE,
    bb_pct_b            DOUBLE,
    atr14               DOUBLE,
    atr_pct             DOUBLE,
    adx14               DOUBLE,
    plus_di             DOUBLE,
    minus_di            DOUBLE,
    stoch_k             DOUBLE,
    stoch_d             DOUBLE,
    -- Volume
    obv                 DOUBLE,
    cmf20               DOUBLE,
    ad_line             DOUBLE,
    avg_dollar_vol_20d  DOUBLE,
    avg_dollar_vol_50d  DOUBLE,
    median_volume_50d   DOUBLE,
    -- Price positions
    high_52w            DOUBLE,
    low_52w             DOUBLE,
    pct_from_52w_high   DOUBLE,
    pct_from_52w_low    DOUBLE,
    pos_52w_pct         DOUBLE,
    -- Donchian
    donchian_high_20    DOUBLE,
    donchian_low_20     DOUBLE,
    donchian_high_55    DOUBLE,
    donchian_low_55     DOUBLE,
    donchian_high_252   DOUBLE,
    donchian_low_252    DOUBLE,
    pct_from_20d_high   DOUBLE,
    pct_from_55d_high   DOUBLE,
    pct_from_252d_high  DOUBLE,
    breakout_55d_high   BOOLEAN,
    breakout_3m_high    BOOLEAN,
    -- Volatility
    realized_vol_20d    DOUBLE,
    realized_vol_60d    DOUBLE,
    max_drawdown_63d    DOUBLE,
    max_drawdown_252d   DOUBLE,
    -- Gap stats (60d)
    gap_rate_60d        DOUBLE,
    max_gap_60d         DOUBLE,
    -- Rolling stats 3M (63 trading days)
    up_days_3m          INTEGER,
    down_days_3m        INTEGER,
    up_down_ratio_3m    DOUBLE,
    max_win_streak_3m   INTEGER,
    win_streaks_5p_3m   INTEGER,
    -- Rolling stats 1Y (252 trading days)
    up_days_1y          INTEGER,
    down_days_1y        INTEGER,
    up_down_ratio_1y    DOUBLE,
    max_win_streak_1y   INTEGER,
    win_streaks_5p_1y   INTEGER,
    -- Big move events (JSON arrays)
    big_up_events_90d   TEXT,
    big_down_events_90d TEXT,
    -- Comparison JSONs (for T1/T2 indicators)
    daily_vs_3m         TEXT,
    daily_vs_12m        TEXT,
    weekly_vs_3m        TEXT,
    weekly_vs_12m       TEXT,
    -- Status
    is_finalized        BOOLEAN DEFAULT FALSE,
    computed_at         TIMESTAMP,
    PRIMARY KEY (ticker, as_of_date)
);
"""

_DDL_FUND = """
CREATE TABLE IF NOT EXISTS fundamentals (
    ticker          TEXT NOT NULL,
    fetch_date      DATE NOT NULL,
    -- Key extracted fields
    market_cap      DOUBLE,
    forward_pe      DOUBLE,
    pb_ratio        DOUBLE,
    q_revenue       DOUBLE,
    q_eps           DOUBLE,
    a_revenue       DOUBLE,
    a_eps           DOUBLE,
    q_rev_yoy       DOUBLE,
    q_eps_yoy       DOUBLE,
    a_rev_yoy       DOUBLE,
    a_eps_yoy       DOUBLE,
    q_end_date      TEXT,
    a_end_date      TEXT,
    q_rev_source    TEXT,
    q_eps_source    TEXT,
    -- Full raw info stored as JSON for value table
    raw_info_json   TEXT,
    fetched_at      TIMESTAMP,
    PRIMARY KEY (ticker, fetch_date)
);
"""

_DDL_RUNS = """
CREATE TABLE IF NOT EXISTS analysis_runs (
    run_dt     TEXT NOT NULL,
    ticker     TEXT NOT NULL,
    T1 TEXT, T2 TEXT, T3 TEXT, T4 TEXT,
    F1 TEXT, F2 TEXT, F3 TEXT, F4 TEXT, F5 TEXT, F6 TEXT,
    T1_sub_3m_price TEXT, T1_sub_3m_vol TEXT, T1_sub_12m_price TEXT, T1_sub_12m_vol TEXT,
    T2_sub_3m_price TEXT, T2_sub_3m_vol TEXT, T2_sub_12m_price TEXT, T2_sub_12m_vol TEXT,
    T3_sub_ma10_20 TEXT, T3_sub_ma20_50 TEXT, T3_sub_ma50_150 TEXT, T3_sub_ma150_200 TEXT,
    T4_sub_has_big_up TEXT, T4_sub_no_big_down TEXT,
    F1_sub_q_rev TEXT, F1_sub_q_eps TEXT,
    F2_sub_a_rev TEXT, F2_sub_a_eps TEXT,
    F3_sub_q_rev_yoy TEXT, F3_sub_q_eps_yoy TEXT,
    F4_sub_a_rev_yoy TEXT, F4_sub_a_eps_yoy TEXT,
    market_cap DOUBLE,
    comments   TEXT,
    PRIMARY KEY (run_dt, ticker)
);
"""

_DDL_DETAILS = """
CREATE TABLE IF NOT EXISTS analysis_details (
    run_dt       TEXT NOT NULL,
    ticker       TEXT NOT NULL,
    indicator_id TEXT NOT NULL,
    detail_json  TEXT,
    PRIMARY KEY (run_dt, ticker, indicator_id)
);
"""

_DDL_PEERS = """
CREATE TABLE IF NOT EXISTS peer_cache (
    ticker         TEXT NOT NULL PRIMARY KEY,
    peers_json     TEXT,
    pe_median      DOUBLE,
    pb_median      DOUBLE,
    pe_values_json TEXT,
    pb_values_json TEXT,
    fetched_at     TIMESTAMP
);
"""


_thread_local = threading.local()


def _conn() -> duckdb.DuckDBPyConnection:
    if not hasattr(_thread_local, "conn"):
        _thread_local.conn = duckdb.connect(str(DB_PATH))
    return _thread_local.conn


def init_db() -> None:
    """Create all tables if they don't exist."""
    con = _conn()
    con.execute(_DDL_TECH)
    con.execute(_DDL_FUND)
    con.execute(_DDL_RUNS)
    con.execute(_DDL_DETAILS)
    con.execute(_DDL_PEERS)


# ── Internal helpers ──────────────────────────────────────────────────────────

def _bool_to_str(v) -> str:
    if v is True:  return "PASS"
    if v is False: return "FAIL"
    return "NA"


def _sub_val(indicators: dict, ind_id: str, sub_col: str) -> str:
    key = SUB_KEY_MAP.get(sub_col)
    if not key:
        return "NA"
    sub = indicators.get(ind_id, {}).get("detail", {}).get("sub_checks", {})
    return _bool_to_str(sub.get(key))


# ── Tech indicators ───────────────────────────────────────────────────────────

def save_tech_indicators(ticker: str, as_of_date: str, fields: dict,
                         is_finalized: bool = False) -> None:
    """
    Upsert a row into tech_indicators.
    `fields` is a flat dict matching column names (any subset; missing = NULL).
    """
    now = datetime.now(CST)
    con = _conn()
    # Build column list dynamically from what's in `fields`
    cols = list(fields.keys()) + ["ticker", "as_of_date", "is_finalized", "computed_at"]
    vals = list(fields.values()) + [ticker, as_of_date, is_finalized, now]
    placeholders = ", ".join(["?" for _ in vals])
    col_str = ", ".join(cols)
    con.execute(
        f"INSERT OR REPLACE INTO tech_indicators ({col_str}) VALUES ({placeholders})",
        vals,
    )


def get_latest_tech_date(ticker: str) -> str | None:
    """Latest as_of_date for a ticker (or None)."""
    con = _conn()
    row = con.execute(
        "SELECT MAX(as_of_date) FROM tech_indicators WHERE ticker = ?",
        [ticker],
    ).fetchone()
    return str(row[0]) if row and row[0] else None


def get_unfinalized_tickers() -> list[tuple[str, str]]:
    """Returns [(ticker, as_of_date)] where is_finalized = FALSE."""
    con = _conn()
    rows = con.execute(
        "SELECT ticker, as_of_date FROM tech_indicators "
        "WHERE is_finalized = FALSE ORDER BY ticker"
    ).fetchall()
    return [(r[0], str(r[1])) for r in rows]


def mark_tech_finalized(ticker: str, as_of_date: str) -> None:
    con = _conn()
    con.execute(
        "UPDATE tech_indicators SET is_finalized = TRUE "
        "WHERE ticker = ? AND as_of_date = ?",
        [ticker, as_of_date],
    )


def get_tech_for_ticker(ticker: str, as_of_date: str | None = None) -> dict | None:
    """
    Fetch tech_indicators row for a ticker.
    If as_of_date is None, returns the latest row.
    """
    con = _conn()
    if as_of_date:
        row = con.execute(
            "SELECT * FROM tech_indicators WHERE ticker = ? AND as_of_date = ?",
            [ticker, as_of_date],
        ).fetchone()
    else:
        row = con.execute(
            "SELECT * FROM tech_indicators WHERE ticker = ? "
            "ORDER BY as_of_date DESC LIMIT 1",
            [ticker],
        ).fetchone()
    if row is None:
        return None
    cols = [d[0] for d in con.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'tech_indicators' ORDER BY ordinal_position"
    ).fetchall()]
    return dict(zip(cols, row))


# ── Fundamentals ──────────────────────────────────────────────────────────────

def save_fundamental(ticker: str, fetch_date: str, fields: dict) -> None:
    """
    Upsert a row into fundamentals.
    `fields` should contain all the key fields plus optional raw_info_json.
    """
    now = datetime.now(CST)
    cols = list(fields.keys()) + ["ticker", "fetch_date", "fetched_at"]
    vals = list(fields.values()) + [ticker, fetch_date, now]
    placeholders = ", ".join(["?" for _ in vals])
    col_str = ", ".join(cols)
    con = _conn()
    con.execute(
        f"INSERT OR REPLACE INTO fundamentals ({col_str}) VALUES ({placeholders})",
        vals,
    )


def get_latest_fundamental(ticker: str) -> dict | None:
    """Latest fundamentals row for a ticker."""
    con = _conn()
    row = con.execute(
        "SELECT * FROM fundamentals WHERE ticker = ? "
        "ORDER BY fetch_date DESC LIMIT 1",
        [ticker],
    ).fetchone()
    if row is None:
        return None
    cols = [d[0] for d in con.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'fundamentals' ORDER BY ordinal_position"
    ).fetchall()]
    return dict(zip(cols, row))


def get_all_fundamentals_for_run(tickers: list[str]) -> dict[str, dict]:
    """
    Returns {ticker: latest_fundamental_row} for all given tickers.
    Used to build the value table in the frontend.
    """
    if not tickers:
        return {}
    placeholders = ", ".join(["?" for _ in tickers])
    con = _conn()
    # Get latest fetch_date per ticker
    rows = con.execute(
        f"SELECT f.* FROM fundamentals f "
        f"INNER JOIN ("
        f"  SELECT ticker, MAX(fetch_date) AS fd "
        f"  FROM fundamentals WHERE ticker IN ({placeholders}) GROUP BY ticker"
        f") latest ON f.ticker = latest.ticker AND f.fetch_date = latest.fd",
        tickers,
    ).fetchall()
    if not rows:
        return {}
    cols = [d[0] for d in con.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'fundamentals' ORDER BY ordinal_position"
    ).fetchall()]
    return {dict(zip(cols, r))["ticker"]: dict(zip(cols, r)) for r in rows}


# ── Peer cache ────────────────────────────────────────────────────────────────

def get_cached_peer_valuations(ticker: str,
                               max_age_days: int = 7) -> dict | None:
    """
    Returns peer valuation dict if cached and fresh, else None.
    Format matches peers_fetcher.get_peer_valuations() output.
    """
    con = _conn()
    row = con.execute(
        "SELECT peers_json, pe_median, pb_median, pe_values_json, pb_values_json, fetched_at "
        "FROM peer_cache WHERE ticker = ?",
        [ticker.upper()],
    ).fetchone()
    if row is None:
        return None
    fetched_at = row[5]
    if fetched_at is None:
        return None
    # Check age
    if hasattr(fetched_at, "tzinfo") and fetched_at.tzinfo is None:
        from datetime import timezone
        fetched_at = fetched_at.replace(tzinfo=timezone.utc)
    age = (datetime.now(CST) - fetched_at.astimezone(CST)).days
    if age > max_age_days:
        return None
    return {
        "peers":                  json.loads(row[0] or "[]"),
        "peer_forward_pe_values": json.loads(row[3] or "[]"),
        "peer_pb_values":         json.loads(row[4] or "[]"),
        "pe_median":              row[1],
        "pb_median":              row[2],
    }


def save_peer_valuations(ticker: str, peer_data: dict) -> None:
    """Persist peer valuations to peer_cache table."""
    now = datetime.now(CST)
    con = _conn()
    con.execute(
        "INSERT OR REPLACE INTO peer_cache "
        "(ticker, peers_json, pe_median, pb_median, pe_values_json, pb_values_json, fetched_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            ticker.upper(),
            json.dumps(peer_data.get("peers", [])),
            peer_data.get("pe_median"),
            peer_data.get("pb_median"),
            json.dumps(peer_data.get("peer_forward_pe_values", [])),
            json.dumps(peer_data.get("peer_pb_values", [])),
            now,
        ],
    )


# ── Analysis runs (mirrors db.py public API) ──────────────────────────────────

def save_results(ticker: str, indicators: dict, analysis_dt: str,
                 market_cap: float | None = None) -> None:
    """Persist indicator results to analysis_runs + analysis_details."""
    row_vals = [analysis_dt, ticker]
    for ind in MAIN_IND_COLS:
        row_vals.append(indicators.get(ind, {}).get("pass", "NA"))
    for sub_col in ALL_SUB_COLS:
        parent = sub_col.split("_")[0]
        row_vals.append(_sub_val(indicators, parent, sub_col))
    row_vals.append(market_cap)
    row_vals.append(None)  # comments

    col_names = "run_dt, ticker, " + ", ".join(SUMMARY_COLS)
    placeholders = ", ".join(["?" for _ in row_vals])

    con = _conn()
    con.execute(
        f"INSERT OR IGNORE INTO analysis_runs ({col_names}) VALUES ({placeholders})",
        row_vals,
    )
    for ind_id, ind_data in indicators.items():
        con.execute(
            "INSERT OR REPLACE INTO analysis_details VALUES (?, ?, ?, ?)",
            (analysis_dt, ticker, ind_id,
             json.dumps(ind_data.get("detail", {}), default=str)),
        )


def update_field(analysis_dt: str, ticker: str, column: str, value: str) -> bool:
    allowed = set(MAIN_IND_COLS + ALL_SUB_COLS + ["comments"])
    if column not in allowed:
        return False
    con = _conn()
    con.execute(
        f"UPDATE analysis_runs SET {column} = ? "
        "WHERE run_dt = ? AND ticker = ?",
        [value, analysis_dt, ticker],
    )
    return True


# ── Query helpers (mirrors db.py) ─────────────────────────────────────────────

def get_all_run_datetimes() -> list[str]:
    con = _conn()
    rows = con.execute(
        "SELECT DISTINCT run_dt FROM analysis_runs ORDER BY run_dt DESC"
    ).fetchall()
    return [r[0] for r in rows]


def get_latest_run_datetime() -> str | None:
    dts = get_all_run_datetimes()
    return dts[0] if dts else None


def get_summary_for_run(analysis_dt: str) -> list[dict]:
    con = _conn()
    rows = con.execute(
        "SELECT *, run_dt AS analysis_datetime FROM analysis_runs "
        "WHERE run_dt = ? ORDER BY ticker",
        [analysis_dt],
    ).fetchall()
    if not rows:
        return []
    cols = [d[0] for d in con.description]
    return [dict(zip(cols, r)) for r in rows]


def get_detail_for_run(analysis_dt: str) -> dict:
    """Returns {ticker: {indicator_id: detail_dict}}."""
    con = _conn()
    rows = con.execute(
        "SELECT ticker, indicator_id, detail_json FROM analysis_details "
        "WHERE run_dt = ? ORDER BY ticker, indicator_id",
        [analysis_dt],
    ).fetchall()
    result: dict = {}
    for r in rows:
        result.setdefault(r[0], {})[r[1]] = (
            json.loads(r[2]) if r[2] else {}
        )
    return result


def get_all_summaries(
    tickers: list[str] | None = None,
    datetimes: list[str] | None = None,
) -> list[dict]:
    where_clauses = []
    params: list = []
    if tickers:
        placeholders = ", ".join(["?" for _ in tickers])
        where_clauses.append(f"ticker IN ({placeholders})")
        params.extend(tickers)
    if datetimes:
        placeholders = ", ".join(["?" for _ in datetimes])
        where_clauses.append(f"run_dt IN ({placeholders})")
        params.extend(datetimes)
    where = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
    con = _conn()
    rows = con.execute(
        f"SELECT *, run_dt AS analysis_datetime FROM analysis_runs {where} "
        "ORDER BY run_dt DESC, ticker ASC",
        params,
    ).fetchall()
    if not rows:
        return []
    cols = [d[0] for d in con.description]
    return [dict(zip(cols, r)) for r in rows]


def get_detail_filtered(
    ticker: str | None = None,
    analysis_dt: str | None = None,
) -> dict:
    where_clauses = []
    params: list = []
    if ticker:
        where_clauses.append("ticker = ?")
        params.append(ticker)
    if analysis_dt:
        where_clauses.append("run_dt = ?")
        params.append(analysis_dt)
    where = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
    con = _conn()
    rows = con.execute(
        f"SELECT ticker, indicator_id, detail_json FROM analysis_details "
        f"{where} ORDER BY ticker, indicator_id",
        params,
    ).fetchall()
    result: dict = {}
    for r in rows:
        result.setdefault(r[0], {})[r[1]] = (
            json.loads(r[2]) if r[2] else {}
        )
    return result


def get_all_tickers() -> list[str]:
    con = _conn()
    rows = con.execute(
        "SELECT DISTINCT ticker FROM analysis_runs ORDER BY ticker"
    ).fetchall()
    return [r[0] for r in rows]


def get_datetimes_for_ticker(ticker: str) -> list[str]:
    con = _conn()
    rows = con.execute(
        "SELECT DISTINCT run_dt FROM analysis_runs "
        "WHERE ticker = ? ORDER BY run_dt DESC",
        [ticker],
    ).fetchall()
    return [r[0] for r in rows]


def get_tickers_for_datetime(analysis_dt: str) -> list[str]:
    con = _conn()
    rows = con.execute(
        "SELECT DISTINCT ticker FROM analysis_runs "
        "WHERE run_dt = ? ORDER BY ticker",
        [analysis_dt],
    ).fetchall()
    return [r[0] for r in rows]
