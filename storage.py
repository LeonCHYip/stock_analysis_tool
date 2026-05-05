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
SUMMARY_COLS = MAIN_IND_COLS + ALL_SUB_COLS + [
    "market_cap", "comments", "status",
    "company_summary", "revenue_composition", "tech_pos", "fund_pos", "tech_neg", "fund_neg",
]


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
    -- Price positions (intraday 52W high/low)
    high_52w            DOUBLE,
    low_52w             DOUBLE,
    pct_from_52w_high   DOUBLE,
    pct_from_52w_low    DOUBLE,
    pos_52w_pct         DOUBLE,
    -- Close-based 52W & historical high/low
    high_close_52w          DOUBLE,
    low_close_52w           DOUBLE,
    pct_from_high_close_52w DOUBLE,
    pct_from_low_close_52w  DOUBLE,
    high_close_3y           DOUBLE,
    low_close_3y            DOUBLE,
    pct_from_high_close_3y  DOUBLE,
    pct_from_low_close_3y   DOUBLE,
    days_since_52w_high     INTEGER,
    days_since_52w_low      INTEGER,
    made_high_5d            BOOLEAN,
    made_high_22d           BOOLEAN,
    made_high_252d          BOOLEAN,
    made_high_3m            BOOLEAN,
    made_high_3y            BOOLEAN,
    made_low_5d             BOOLEAN,
    made_low_22d            BOOLEAN,
    made_low_252d           BOOLEAN,
    days_since_5d_high      INTEGER,
    days_since_22d_high     INTEGER,
    days_since_3m_high      INTEGER,
    days_since_3y_high      INTEGER,
    pct_from_high_close_5d  DOUBLE,
    pct_from_high_close_22d DOUBLE,
    pct_from_high_close_3m  DOUBLE,
    -- Consecutive up-close streak
    up_streak_days          INTEGER,
    up_streak_px_pct        DOUBLE,
    up_streak_vol_pct       DOUBLE,
    up_streak_avg_px_pct    DOUBLE,
    up_streak_avg_vol_pct   DOUBLE,
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
    -- Daily price/volume change
    daily_pct_change    DOUBLE,
    daily_vol_pct       DOUBLE,
    -- Price vs MA distances (%)
    pct_from_sma10      DOUBLE,
    pct_from_sma20      DOUBLE,
    pct_from_sma50      DOUBLE,
    pct_from_sma150     DOUBLE,
    pct_from_ema9       DOUBLE,
    pct_from_ema21      DOUBLE,
    pct_from_ema50      DOUBLE,
    pct_from_ema200     DOUBLE,
    -- Additional MA slopes
    sma10_slope_10d     DOUBLE,
    sma20_slope_10d     DOUBLE,
    sma150_slope_20d    DOUBLE,
    sma200_slope_20d    DOUBLE,
    -- Relative volume (today / N-day avg raw volume)
    rel_vol_20d         DOUBLE,
    rel_vol_50d         DOUBLE,
    -- Up/down volume ratio (20D)
    up_down_vol_ratio_20d DOUBLE,
    -- Swing high/low
    swing_high          DOUBLE,
    swing_high_date     TEXT,
    swing_low           DOUBLE,
    swing_low_date      TEXT,
    pct_from_swing_high DOUBLE,
    pct_from_swing_low  DOUBLE,
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
    -- Short interest (defaultKeyStatistics)
    shares_short     DOUBLE,
    shares_short_pm  DOUBLE,
    float_shares     DOUBLE,
    shares_out       DOUBLE,
    implied_shares   DOUBLE,
    short_pct_float  DOUBLE,
    short_pct_out    DOUBLE,
    short_ratio      DOUBLE,
    date_short_int   TEXT,
    avg_volume       DOUBLE,
    -- Insider activity (netSharePurchaseActivity, 6-month window)
    ins_buy_count    DOUBLE,
    ins_buy_shares   DOUBLE,
    ins_sell_count   DOUBLE,
    ins_sell_shares  DOUBLE,
    ins_net_shares   DOUBLE,
    ins_buy_pct      DOUBLE,
    ins_sell_pct     DOUBLE,
    ins_net_pct      DOUBLE,
    -- Margins & ratios (financialData)
    gross_margin     DOUBLE,
    ebitda_margin    DOUBLE,
    op_margin        DOUBLE,
    net_margin       DOUBLE,
    current_ratio    DOUBLE,
    quick_ratio      DOUBLE,
    debt_to_equity   DOUBLE,
    roe              DOUBLE,
    roa              DOUBLE,
    -- Analyst targets (financialData)
    target_median    DOUBLE,
    target_high      DOUBLE,
    target_low       DOUBLE,
    target_mean      DOUBLE,
    current_price_fd DOUBLE,
    rec_mean         DOUBLE,
    rec_key          TEXT,
    analyst_count    DOUBLE,
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
    comments         TEXT,
    status           TEXT DEFAULT '',
    company_summary      TEXT,
    revenue_composition  TEXT,
    tech_pos             TEXT,
    fund_pos         TEXT,
    tech_neg         TEXT,
    fund_neg         TEXT,
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


_DDL_AI_REPORTS = """
CREATE TABLE IF NOT EXISTS ai_reports (
    run_dt  TEXT NOT NULL,
    ticker  TEXT NOT NULL,
    report  TEXT,
    model   TEXT,
    status  TEXT DEFAULT 'complete',
    PRIMARY KEY (run_dt, ticker)
);
"""

_DDL_EARNINGS = """
CREATE TABLE IF NOT EXISTS earnings_history (
    ticker          TEXT    NOT NULL,
    earnings_date   TEXT    NOT NULL,
    earnings_time   TEXT,
    eps_est         DOUBLE,
    eps_act         DOUBLE,
    eps_sur         DOUBLE,
    eps_gaap_est    DOUBLE,
    eps_gaap_act    DOUBLE,
    eps_gaap_sur    DOUBLE,
    rev_est_m       DOUBLE,
    rev_act_m       DOUBLE,
    rev_sur         DOUBLE,
    one_day_change  DOUBLE,
    q_rev_yoy       DOUBLE,
    q_eps_yoy       DOUBLE,
    fetch_date      TEXT,
    PRIMARY KEY (ticker, earnings_date)
);
"""

_DDL_EARNINGS_LOG = """
CREATE TABLE IF NOT EXISTS earnings_fetch_log (
    fetch_date    TEXT NOT NULL PRIMARY KEY,
    scraped_at    TIMESTAMP,
    ticker_count  INTEGER
);
"""

_DDL_PRICE_HISTORY = """
CREATE TABLE IF NOT EXISTS price_history (
    ticker  TEXT   NOT NULL,
    date    DATE   NOT NULL,
    open    DOUBLE,
    high    DOUBLE,
    low     DOUBLE,
    close   DOUBLE,
    volume  BIGINT,
    PRIMARY KEY (ticker, date)
);
"""

_DDL_TRIGGER_CACHE = """
CREATE TABLE IF NOT EXISTS trigger_cache (
    cache_key        TEXT    NOT NULL,
    ticker           TEXT    NOT NULL,
    start_date       TEXT,
    end_date         TEXT,
    trig_px_pct      DOUBLE,
    trig_vol_pct     DOUBLE,
    trig_avg_px_pct  DOUBLE,
    trig_avg_vol_pct DOUBLE,
    max_ph_date      TEXT,
    computed_at      TIMESTAMP,
    PRIMARY KEY (cache_key, ticker)
);
"""

_init_lock = threading.Lock()
_db_lock   = threading.RLock()    # serialises ALL DB operations (reads + writes)
_global_conn: duckdb.DuckDBPyConnection | None = None


class _LockedCursor:
    """Proxy for a DuckDB cursor that holds _db_lock for its entire lifetime.

    Acquires _db_lock on construction; releases it in __del__ when the cursor
    goes out of scope.  Because _db_lock is an RLock, nested calls from the
    same thread (e.g. get_latest_run_datetime → get_all_run_datetimes) work
    correctly without deadlocking.
    """
    __slots__ = ("_cur",)

    def __init__(self) -> None:
        _db_lock.acquire()
        self._cur = _global_conn.cursor()  # type: ignore[union-attr]

    def __del__(self) -> None:
        try:
            _db_lock.release()
        except RuntimeError:
            pass  # already released (shouldn't happen)

    def __getattr__(self, name: str):
        return getattr(self._cur, name)


def _conn() -> "_LockedCursor":
    """Return a locked cursor proxy. _db_lock is held until the proxy is GC'd."""
    global _global_conn
    if _global_conn is None:
        with _init_lock:
            if _global_conn is None:
                _global_conn = duckdb.connect(str(DB_PATH))
    return _LockedCursor()


def init_db() -> None:
    """Create all tables if they don't exist, and migrate schema."""
    con = _conn()
    con.execute(_DDL_TECH)
    con.execute(_DDL_FUND)
    con.execute(_DDL_RUNS)
    con.execute(_DDL_DETAILS)
    con.execute(_DDL_PEERS)
    con.execute(_DDL_AI_REPORTS)
    con.execute(_DDL_EARNINGS)
    con.execute(_DDL_EARNINGS_LOG)
    con.execute(_DDL_PRICE_HISTORY)
    con.execute(_DDL_TRIGGER_CACHE)
    # Migrate: add columns introduced after initial schema creation
    _migrate_add_columns(con)


def _migrate_add_columns(con) -> None:
    """Add new columns to existing tables if they don't already exist."""
    new_tech_cols = [
        ("daily_pct_change",            "DOUBLE"),
        ("daily_vol_pct",               "DOUBLE"),
        ("high_close_52w",              "DOUBLE"),
        ("low_close_52w",               "DOUBLE"),
        ("pct_from_high_close_52w",     "DOUBLE"),
        ("pct_from_low_close_52w",      "DOUBLE"),
        ("high_close_3y",               "DOUBLE"),
        ("low_close_3y",                "DOUBLE"),
        ("pct_from_high_close_3y",      "DOUBLE"),
        ("pct_from_low_close_3y",       "DOUBLE"),
        ("days_since_52w_high",         "INTEGER"),
        ("days_since_52w_low",          "INTEGER"),
        ("made_high_5d",                "BOOLEAN"),
        ("made_high_22d",               "BOOLEAN"),
        ("made_high_252d",              "BOOLEAN"),
        ("made_high_3m",                "BOOLEAN"),
        ("made_high_3y",                "BOOLEAN"),
        ("made_low_5d",                 "BOOLEAN"),
        ("made_low_22d",                "BOOLEAN"),
        ("made_low_252d",               "BOOLEAN"),
        ("days_since_5d_high",          "INTEGER"),
        ("days_since_22d_high",         "INTEGER"),
        ("days_since_3m_high",          "INTEGER"),
        ("days_since_3y_high",          "INTEGER"),
        ("pct_from_high_close_5d",      "DOUBLE"),
        ("pct_from_high_close_22d",     "DOUBLE"),
        ("pct_from_high_close_3m",      "DOUBLE"),
        ("up_streak_days",              "INTEGER"),
        ("up_streak_px_pct",            "DOUBLE"),
        ("up_streak_vol_pct",           "DOUBLE"),
        ("up_streak_avg_px_pct",        "DOUBLE"),
        ("up_streak_avg_vol_pct",       "DOUBLE"),
        ("pct_from_sma10",         "DOUBLE"),
        ("pct_from_sma20",         "DOUBLE"),
        ("pct_from_sma50",         "DOUBLE"),
        ("pct_from_sma150",        "DOUBLE"),
        ("pct_from_ema9",          "DOUBLE"),
        ("pct_from_ema21",         "DOUBLE"),
        ("pct_from_ema50",         "DOUBLE"),
        ("pct_from_ema200",        "DOUBLE"),
        ("sma10_slope_10d",        "DOUBLE"),
        ("sma20_slope_10d",        "DOUBLE"),
        ("sma150_slope_20d",       "DOUBLE"),
        ("sma200_slope_20d",       "DOUBLE"),
        ("rel_vol_20d",            "DOUBLE"),
        ("rel_vol_50d",            "DOUBLE"),
        ("up_down_vol_ratio_20d",  "DOUBLE"),
        ("swing_high",             "DOUBLE"),
        ("swing_high_date",        "TEXT"),
        ("swing_low",              "DOUBLE"),
        ("swing_low_date",         "TEXT"),
        ("pct_from_swing_high",    "DOUBLE"),
        ("pct_from_swing_low",     "DOUBLE"),
    ]
    existing = {row[0] for row in con.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'tech_indicators'"
    ).fetchall()}
    for col, dtype in new_tech_cols:
        if col not in existing:
            con.execute(f"ALTER TABLE tech_indicators ADD COLUMN {col} {dtype}")

    _migrate_earnings_surprise_to_double(con)

    # Add status column to analysis_runs if missing
    runs_cols = {row[0] for row in con.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'analysis_runs'"
    ).fetchall()}
    if "status" not in runs_cols:
        con.execute("ALTER TABLE analysis_runs ADD COLUMN status TEXT DEFAULT ''")
    for _col in ("company_summary", "revenue_composition", "tech_pos", "fund_pos", "tech_neg", "fund_neg"):
        if _col not in runs_cols:
            con.execute(f"ALTER TABLE analysis_runs ADD COLUMN {_col} TEXT")

    # Add status column to ai_reports if missing
    ai_cols = {row[0] for row in con.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'ai_reports'"
    ).fetchall()}
    if "status" not in ai_cols:
        con.execute("ALTER TABLE ai_reports ADD COLUMN status TEXT DEFAULT 'complete'")
        con.execute("UPDATE ai_reports SET status = 'complete' WHERE status IS NULL")

    # Add new fundamentals columns (short interest, insider activity, margins, analyst targets)
    new_fund_cols = [
        ("shares_short",    "DOUBLE"), ("shares_short_pm", "DOUBLE"),
        ("float_shares",    "DOUBLE"), ("shares_out",      "DOUBLE"),
        ("implied_shares",  "DOUBLE"), ("short_pct_float", "DOUBLE"),
        ("short_pct_out",   "DOUBLE"), ("short_ratio",     "DOUBLE"),
        ("date_short_int",  "TEXT"),   ("avg_volume",      "DOUBLE"),
        ("ins_buy_count",   "DOUBLE"), ("ins_buy_shares",  "DOUBLE"),
        ("ins_sell_count",  "DOUBLE"), ("ins_sell_shares", "DOUBLE"),
        ("ins_net_shares",  "DOUBLE"), ("ins_buy_pct",     "DOUBLE"),
        ("ins_sell_pct",    "DOUBLE"), ("ins_net_pct",     "DOUBLE"),
        ("gross_margin",    "DOUBLE"), ("ebitda_margin",   "DOUBLE"),
        ("op_margin",       "DOUBLE"), ("net_margin",      "DOUBLE"),
        ("current_ratio",   "DOUBLE"), ("quick_ratio",     "DOUBLE"),
        ("debt_to_equity",  "DOUBLE"), ("roe",             "DOUBLE"),
        ("roa",             "DOUBLE"),
        ("target_median",   "DOUBLE"), ("target_high",     "DOUBLE"),
        ("target_low",      "DOUBLE"), ("target_mean",     "DOUBLE"),
        ("current_price_fd","DOUBLE"), ("rec_mean",        "DOUBLE"),
        ("rec_key",         "TEXT"),   ("analyst_count",   "DOUBLE"),
        # Extended valuation (Call 1 — quoteSummary, no extra HTTP calls)
        ("beta",                "DOUBLE"),
        ("trailing_pe",         "DOUBLE"),
        ("trailing_eps",        "DOUBLE"),
        ("forward_eps",         "DOUBLE"),
        ("peg_ratio",           "DOUBLE"),
        ("trailing_peg",        "DOUBLE"),
        ("dividend_yield",      "DOUBLE"),   # fraction (0.005 = 0.5%)
        ("dividend_rate",       "DOUBLE"),
        ("payout_ratio",        "DOUBLE"),   # fraction
        ("enterprise_value",    "DOUBLE"),
        ("ev_to_ebitda",        "DOUBLE"),
        ("ev_to_revenue",       "DOUBLE"),
        ("revenue_per_share",   "DOUBLE"),
        ("total_cash_per_share","DOUBLE"),
        ("book_value_per_share","DOUBLE"),
        ("held_pct_insiders",   "DOUBLE"),   # fraction
        ("held_pct_institutions","DOUBLE"),  # fraction
        ("total_cash",          "DOUBLE"),
        ("total_debt_spot",     "DOUBLE"),
        ("fcf_spot",            "DOUBLE"),
        # Income statement Q+A values and YoY (Call 2 timeseries, no extra HTTP calls)
        ("q_gross_profit",      "DOUBLE"), ("a_gross_profit",      "DOUBLE"),
        ("q_gross_profit_yoy",  "DOUBLE"), ("a_gross_profit_yoy",  "DOUBLE"),
        ("q_op_income",         "DOUBLE"), ("a_op_income",         "DOUBLE"),
        ("q_op_income_yoy",     "DOUBLE"), ("a_op_income_yoy",     "DOUBLE"),
        ("q_net_income",        "DOUBLE"), ("a_net_income",        "DOUBLE"),
        ("q_net_income_yoy",    "DOUBLE"), ("a_net_income_yoy",    "DOUBLE"),
        ("q_ebitda",            "DOUBLE"), ("a_ebitda",            "DOUBLE"),
        ("q_ebitda_yoy",        "DOUBLE"), ("a_ebitda_yoy",        "DOUBLE"),
        ("q_rd",                "DOUBLE"), ("a_rd",                "DOUBLE"),
        ("q_rd_yoy",            "DOUBLE"), ("a_rd_yoy",            "DOUBLE"),
        # Cash flow Q+A values and YoY
        ("q_ocf",               "DOUBLE"), ("a_ocf",               "DOUBLE"),
        ("q_ocf_yoy",           "DOUBLE"), ("a_ocf_yoy",           "DOUBLE"),
        ("q_fcf",               "DOUBLE"), ("a_fcf",               "DOUBLE"),
        ("q_fcf_yoy",           "DOUBLE"), ("a_fcf_yoy",           "DOUBLE"),
        ("q_capex",             "DOUBLE"), ("a_capex",             "DOUBLE"),
        ("q_capex_yoy",         "DOUBLE"), ("a_capex_yoy",         "DOUBLE"),
        # Balance sheet Q+A values and YoY
        ("q_total_debt",        "DOUBLE"), ("a_total_debt",        "DOUBLE"),
        ("q_total_debt_yoy",    "DOUBLE"), ("a_total_debt_yoy",    "DOUBLE"),
        ("q_net_debt",          "DOUBLE"), ("a_net_debt",          "DOUBLE"),
        ("q_net_debt_yoy",      "DOUBLE"), ("a_net_debt_yoy",      "DOUBLE"),
        ("q_cash",              "DOUBLE"), ("a_cash",              "DOUBLE"),
        ("q_cash_yoy",          "DOUBLE"), ("a_cash_yoy",          "DOUBLE"),
        ("q_working_cap",       "DOUBLE"), ("a_working_cap",       "DOUBLE"),
        ("q_working_cap_yoy",   "DOUBLE"), ("a_working_cap_yoy",   "DOUBLE"),
        ("q_total_assets",      "DOUBLE"), ("a_total_assets",      "DOUBLE"),
        ("q_total_assets_yoy",  "DOUBLE"), ("a_total_assets_yoy",  "DOUBLE"),
    ]
    existing_fund = {row[0] for row in con.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'fundamentals'"
    ).fetchall()}
    for col, dtype in new_fund_cols:
        if col not in existing_fund:
            con.execute(f"ALTER TABLE fundamentals ADD COLUMN {col} {dtype}")

    # Add earnings extended columns
    new_earn_cols = [
        ("earns_1d_vol_pct",       "DOUBLE"),
        ("earns_5d_px_pct",        "DOUBLE"),
        ("earns_5d_vol_pct",       "DOUBLE"),
        ("earns_5d_roll_px_pct",   "DOUBLE"),
        ("earns_5d_roll_vol_pct",  "DOUBLE"),
        ("post_earns_px_pct",      "DOUBLE"),
        ("post_earns_vol_pct",     "DOUBLE"),
        ("post_earns_avg_px_pct",  "DOUBLE"),
        ("post_earns_avg_vol_pct", "DOUBLE"),
    ]
    existing_earn = {row[0] for row in con.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'earnings_history'"
    ).fetchall()}
    for col, dtype in new_earn_cols:
        if col not in existing_earn:
            con.execute(f"ALTER TABLE earnings_history ADD COLUMN {col} {dtype}")

    # Add OHLC columns to price_history
    new_ph_cols = [("open", "DOUBLE"), ("high", "DOUBLE"), ("low", "DOUBLE")]
    existing_ph = {row[0] for row in con.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'price_history'"
    ).fetchall()}
    for col, dtype in new_ph_cols:
        if col not in existing_ph:
            con.execute(f"ALTER TABLE price_history ADD COLUMN {col} {dtype}")


def _migrate_earnings_surprise_to_double(con) -> None:
    """Convert eps_sur, eps_gaap_sur, rev_sur, one_day_change from TEXT to DOUBLE."""
    try:
        col_types = {row[0]: row[1] for row in con.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_name = 'earnings_history'"
        ).fetchall()}
    except Exception:
        return  # table doesn't exist yet

    text_cols = [c for c in ("eps_sur", "eps_gaap_sur", "rev_sur", "one_day_change")
                 if col_types.get(c, "").upper() in ("VARCHAR", "TEXT")]
    if not text_cols:
        return

    for col in text_cols:
        tmp = f"_{col}_dbl"
        con.execute(f"ALTER TABLE earnings_history ADD COLUMN {tmp} DOUBLE")
        # Strip non-numeric chars (keep digits, dot, leading +/-) then cast
        con.execute(f"""
            UPDATE earnings_history
            SET {tmp} = TRY_CAST(
                regexp_replace({col}, '[^0-9.+\\-]', '', 'g') AS DOUBLE
            )
            WHERE {col} IS NOT NULL AND {col} != '—'
        """)
        con.execute(f"ALTER TABLE earnings_history DROP COLUMN {col}")
        con.execute(f"ALTER TABLE earnings_history RENAME COLUMN {tmp} TO {col}")
    print(f"[storage] Migrated earnings_history columns to DOUBLE: {text_cols}")


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


def get_tickers_with_stale_tech(min_date: str) -> list[str]:
    """
    Return tickers whose latest as_of_date in tech_indicators is strictly
    before min_date.  Used to detect cases where yfinance returned stale
    data during a scan (is_finalized=TRUE but as_of_date behind the most
    recent completed trading day).
    """
    con = _conn()
    rows = con.execute(
        "SELECT ticker FROM tech_indicators "
        "GROUP BY ticker HAVING MAX(CAST(as_of_date AS TEXT)) < ? "
        "ORDER BY ticker",
        [min_date],
    ).fetchall()
    return [r[0] for r in rows]


def mark_old_tech_finalized() -> None:
    """Mark all tech_indicators rows with as_of_date < today as finalized.

    Fast single SQL UPDATE — no API calls. Safe to call on startup.
    """
    from market_calendar import et_today
    today = et_today().isoformat()
    con = _conn()
    con.execute(
        "UPDATE tech_indicators SET is_finalized = TRUE "
        "WHERE is_finalized = FALSE AND CAST(as_of_date AS TEXT) < ?",
        [today],
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


def get_tech_for_tickers(tickers: list[str]) -> dict[str, dict]:
    """Batch-fetch the latest tech_indicators row for each ticker in `tickers`."""
    if not tickers:
        return {}
    placeholders = ", ".join(["?" for _ in tickers])
    con = _conn()
    rows = con.execute(
        f"SELECT t.* FROM tech_indicators t "
        f"INNER JOIN ("
        f"  SELECT ticker, MAX(as_of_date) AS d "
        f"  FROM tech_indicators WHERE ticker IN ({placeholders}) GROUP BY ticker"
        f") latest ON t.ticker = latest.ticker AND t.as_of_date = latest.d",
        tickers,
    ).fetchall()
    if not rows:
        return {}
    cols = [d[0] for d in con.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'tech_indicators' ORDER BY ordinal_position"
    ).fetchall()]
    return {dict(zip(cols, r))["ticker"]: dict(zip(cols, r)) for r in rows}


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

def save_comment_for_ticker(ticker: str, comment: str) -> None:
    """Update comments for ALL analysis_runs rows of this ticker."""
    con = _conn()
    con.execute(
        "UPDATE analysis_runs SET comments = ? WHERE ticker = ?",
        [comment, ticker],
    )


def save_status_for_ticker(ticker: str, status: str) -> None:
    """Update status for ALL analysis_runs rows of this ticker."""
    con = _conn()
    con.execute(
        "UPDATE analysis_runs SET status = ? WHERE ticker = ?",
        [status, ticker],
    )


def save_user_field_for_ticker(ticker: str, column: str, value: str) -> None:
    """Update a user-editable note field for ALL analysis_runs rows of this ticker."""
    _allowed = {"company_summary", "revenue_composition", "tech_pos", "fund_pos", "tech_neg", "fund_neg"}
    if column not in _allowed:
        return
    con = _conn()
    con.execute(
        f"UPDATE analysis_runs SET {column} = ? WHERE ticker = ?",
        [value, ticker],
    )


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

    # Carry forward the most recent comment and status for this ticker (if any)
    con = _conn()
    prev = con.execute(
        "SELECT comments, status, company_summary, revenue_composition, tech_pos, fund_pos, tech_neg, fund_neg "
        "FROM analysis_runs WHERE ticker = ? ORDER BY run_dt DESC LIMIT 1",
        [ticker],
    ).fetchone()
    row_vals.append(prev[0] if prev and prev[0] else None)  # comments
    row_vals.append(prev[1] if prev and prev[1] else "")    # status
    row_vals.append(prev[2] if prev and prev[2] else None)  # company_summary
    row_vals.append(prev[3] if prev and prev[3] else None)  # revenue_composition
    row_vals.append(prev[4] if prev and prev[4] else None)  # tech_pos
    row_vals.append(prev[5] if prev and prev[5] else None)  # fund_pos
    row_vals.append(prev[6] if prev and prev[6] else None)  # tech_neg
    row_vals.append(prev[7] if prev and prev[7] else None)  # fund_neg

    col_names = "run_dt, ticker, " + ", ".join(SUMMARY_COLS)
    placeholders = ", ".join(["?" for _ in row_vals])
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
    allowed = set(MAIN_IND_COLS + ALL_SUB_COLS + [
        "comments", "status",
        "company_summary", "revenue_composition", "tech_pos", "fund_pos", "tech_neg", "fund_neg",
    ])
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


def get_tickers_run_since(cutoff_dt: str) -> set[str]:
    """Return tickers whose latest run_dt >= cutoff_dt (already scanned after cutoff)."""
    con = _conn()
    rows = con.execute(
        "SELECT ticker FROM analysis_runs "
        "GROUP BY ticker HAVING MAX(run_dt) >= ?",
        [cutoff_dt],
    ).fetchall()
    return {r[0] for r in rows}


def get_tickers_missing_f1f6(run_dt: str) -> list[str]:
    """Return tickers in run_dt where any F1–F6 sub-indicator (or F5/F6 parent) is NA.

    Used to find candidates for a targeted fundamentals re-scan.
    """
    con = _conn()
    rows = con.execute(
        """
        SELECT ticker FROM analysis_runs
        WHERE run_dt = ?
          AND (
            F1_sub_q_rev     IS NULL OR F1_sub_q_rev     = 'NA' OR
            F1_sub_q_eps     IS NULL OR F1_sub_q_eps     = 'NA' OR
            F2_sub_a_rev     IS NULL OR F2_sub_a_rev     = 'NA' OR
            F2_sub_a_eps     IS NULL OR F2_sub_a_eps     = 'NA' OR
            F3_sub_q_rev_yoy IS NULL OR F3_sub_q_rev_yoy = 'NA' OR
            F3_sub_q_eps_yoy IS NULL OR F3_sub_q_eps_yoy = 'NA' OR
            F4_sub_a_rev_yoy IS NULL OR F4_sub_a_rev_yoy = 'NA' OR
            F4_sub_a_eps_yoy IS NULL OR F4_sub_a_eps_yoy = 'NA' OR
            F5               IS NULL OR F5               = 'NA' OR
            F6               IS NULL OR F6               = 'NA'
          )
        ORDER BY ticker
        """,
        [run_dt],
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


# ── AI Reports ────────────────────────────────────────────────────────────────

def save_ai_report(run_dt: str, ticker: str, report: str, model: str,
                   status: str = "complete") -> None:
    """Upsert one AI report row."""
    con = _conn()
    con.execute(
        "INSERT OR REPLACE INTO ai_reports (run_dt, ticker, report, model, status) "
        "VALUES (?, ?, ?, ?, ?)",
        [run_dt, ticker.upper(), report, model, status],
    )


def get_ai_reports(tickers: list[str] | None = None,
                   run_dts: list[str] | None = None,
                   status: str | None = None,
                   latest_per_ticker: bool = False) -> list[dict]:
    """Return ai_reports rows matching optional filters, newest first."""
    where_clauses = []
    params: list = []
    if tickers:
        placeholders = ", ".join(["?" for _ in tickers])
        where_clauses.append(f"ticker IN ({placeholders})")
        params.extend([t.upper() for t in tickers])
    if run_dts:
        placeholders = ", ".join(["?" for _ in run_dts])
        where_clauses.append(f"run_dt IN ({placeholders})")
        params.extend(run_dts)
    if status:
        where_clauses.append("status = ?")
        params.append(status)
    where = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
    con = _conn()
    if latest_per_ticker:
        # Only return the most recent run per ticker (matching other filters)
        query = (
            f"SELECT a.run_dt, a.ticker, a.report, a.model, a.status "
            f"FROM ai_reports a "
            f"INNER JOIN ("
            f"  SELECT ticker, MAX(run_dt) AS max_dt FROM ai_reports "
            f"  {where} GROUP BY ticker"
            f") latest ON a.ticker = latest.ticker AND a.run_dt = latest.max_dt "
            f"ORDER BY a.run_dt DESC, a.ticker ASC"
        )
    else:
        query = (
            f"SELECT run_dt, ticker, report, model, status FROM ai_reports {where} "
            "ORDER BY run_dt DESC, ticker ASC"
        )
    rows = con.execute(query, params).fetchall()
    return [{"run_dt": r[0], "ticker": r[1], "report": r[2], "model": r[3], "status": r[4]}
            for r in rows]


def get_ai_report_run_dts() -> list[str]:
    """All distinct run_dt values in ai_reports, newest first."""
    con = _conn()
    rows = con.execute(
        "SELECT DISTINCT run_dt FROM ai_reports ORDER BY run_dt DESC"
    ).fetchall()
    return [r[0] for r in rows]


def get_ai_report_tickers() -> list[str]:
    """All distinct ticker values in ai_reports, alphabetical."""
    con = _conn()
    rows = con.execute(
        "SELECT DISTINCT ticker FROM ai_reports ORDER BY ticker"
    ).fetchall()
    return [r[0] for r in rows]


# ── Earnings history ───────────────────────────────────────────────────────────

def save_earnings(ticker: str, fields: dict) -> None:
    """Upsert one earnings_history row. fields must include earnings_date."""
    con = _conn()
    cols = list(fields.keys()) + ["ticker"]
    vals = list(fields.values()) + [ticker]
    placeholders = ", ".join(["?" for _ in vals])
    col_str = ", ".join(cols)
    con.execute(
        f"INSERT OR REPLACE INTO earnings_history ({col_str}) VALUES ({placeholders})",
        vals,
    )


def get_latest_earnings(ticker: str) -> dict | None:
    """Latest earnings_history row for a ticker (by earnings_date DESC)."""
    con = _conn()
    row = con.execute(
        "SELECT * FROM earnings_history WHERE ticker = ? "
        "ORDER BY earnings_date DESC LIMIT 1",
        [ticker],
    ).fetchone()
    if row is None:
        return None
    cols = [d[0] for d in con.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'earnings_history' ORDER BY ordinal_position"
    ).fetchall()]
    return dict(zip(cols, row))


def get_latest_earnings_for_tickers(tickers: list[str]) -> dict[str, dict]:
    """Batch-fetch latest earnings row per ticker. Returns {ticker: row_dict}."""
    if not tickers:
        return {}
    placeholders = ", ".join(["?" for _ in tickers])
    con = _conn()
    rows = con.execute(
        f"SELECT e.* FROM earnings_history e "
        f"INNER JOIN ("
        f"  SELECT ticker, MAX(earnings_date) AS ed "
        f"  FROM earnings_history WHERE ticker IN ({placeholders}) GROUP BY ticker"
        f") latest ON e.ticker = latest.ticker AND e.earnings_date = latest.ed",
        tickers,
    ).fetchall()
    if not rows:
        return {}
    cols = [d[0] for d in con.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'earnings_history' ORDER BY ordinal_position"
    ).fetchall()]
    return {dict(zip(cols, r))["ticker"]: dict(zip(cols, r)) for r in rows}


def mark_earnings_date_fetched(fetch_date: str, ticker_count: int) -> None:
    """Record that a trading-day has been scraped."""
    con = _conn()
    now = datetime.now(CST)
    con.execute(
        "INSERT OR REPLACE INTO earnings_fetch_log (fetch_date, scraped_at, ticker_count) "
        "VALUES (?, ?, ?)",
        [fetch_date, now, ticker_count],
    )


def get_fetched_earnings_dates() -> set[str]:
    """All dates in earnings_fetch_log."""
    con = _conn()
    rows = con.execute("SELECT fetch_date FROM earnings_fetch_log").fetchall()
    return {r[0] for r in rows}


def get_tickers_in_fundamentals() -> list[str]:
    """All distinct tickers in fundamentals table."""
    con = _conn()
    rows = con.execute(
        "SELECT DISTINCT ticker FROM fundamentals ORDER BY ticker"
    ).fetchall()
    return [r[0] for r in rows]


def get_latest_earnings_date() -> str | None:
    """Max earnings_date across all rows in earnings_history, or None if empty."""
    con = _conn()
    row = con.execute("SELECT MAX(earnings_date) FROM earnings_history").fetchone()
    return str(row[0]) if row and row[0] else None


def get_earnings_dates_with_null_extended(min_date: str | None = None) -> list[str]:
    """Distinct earnings_dates where at least one ticker has any extended column NULL.

    Only returns dates >= min_date (if given).
    """
    con = _conn()
    null_cond = (
        "earns_1d_vol_pct IS NULL OR earns_5d_px_pct IS NULL OR "
        "earns_5d_vol_pct IS NULL OR earns_5d_roll_px_pct IS NULL OR "
        "earns_5d_roll_vol_pct IS NULL"
    )
    if min_date:
        rows = con.execute(
            f"SELECT DISTINCT earnings_date FROM earnings_history "
            f"WHERE ({null_cond}) AND earnings_date >= ? "
            f"ORDER BY earnings_date",
            [min_date],
        ).fetchall()
    else:
        rows = con.execute(
            f"SELECT DISTINCT earnings_date FROM earnings_history "
            f"WHERE ({null_cond}) ORDER BY earnings_date"
        ).fetchall()
    return [r[0] for r in rows]


def get_earnings_dates_with_null_change(min_date: str | None = None) -> list[str]:
    """Distinct earnings_dates where at least one ticker has one_day_change IS NULL.

    Only returns dates >= min_date (if given), so we don't endlessly re-fetch
    old dates where Finviz will never populate one_day_change.
    """
    con = _conn()
    if min_date:
        rows = con.execute(
            "SELECT DISTINCT earnings_date FROM earnings_history "
            "WHERE one_day_change IS NULL AND earnings_date >= ? "
            "ORDER BY earnings_date",
            [min_date],
        ).fetchall()
    else:
        rows = con.execute(
            "SELECT DISTINCT earnings_date FROM earnings_history "
            "WHERE one_day_change IS NULL ORDER BY earnings_date"
        ).fetchall()
    return [r[0] for r in rows]


def update_earnings_1d_change(ticker: str, earnings_date: str, change: float) -> None:
    """Update one_day_change for an existing earnings_history row."""
    con = _conn()
    con.execute(
        "UPDATE earnings_history SET one_day_change = ? "
        "WHERE ticker = ? AND earnings_date = ?",
        [change, ticker, earnings_date],
    )


def get_latest_earnings_with_as_of_dates() -> dict[str, dict]:
    """Return {ticker: {earnings_date, earnings_time, as_of_date}} for the most
    recent earnings per ticker, joined with the latest tech_indicators as_of_date.

    Tickers with no tech_indicators row get as_of_date = None.
    """
    sql = """
    WITH latest_earnings AS (
        SELECT ticker, MAX(earnings_date) AS earnings_date
        FROM earnings_history
        WHERE earnings_date IS NOT NULL
        GROUP BY ticker
    ),
    earnings_info AS (
        SELECT eh.ticker,
               eh.earnings_date::TEXT AS earnings_date,
               eh.earnings_time
        FROM earnings_history eh
        JOIN latest_earnings le
          ON eh.ticker = le.ticker AND eh.earnings_date = le.earnings_date
    ),
    latest_tech AS (
        SELECT ticker, MAX(as_of_date)::TEXT AS as_of_date
        FROM tech_indicators
        GROUP BY ticker
    )
    SELECT ei.ticker, ei.earnings_date, ei.earnings_time, lt.as_of_date
    FROM earnings_info ei
    LEFT JOIN latest_tech lt ON ei.ticker = lt.ticker
    """
    con = _conn()
    rows = con.execute(sql).fetchall()
    result: dict = {}
    for ticker, earnings_date, earnings_time, as_of_date in rows:
        result[ticker] = {
            "earnings_date": earnings_date,
            "earnings_time": earnings_time or "BMO",
            "as_of_date":    as_of_date,
        }
    return result


def update_post_earns_extended(ticker: str, earnings_date: str, fields: dict) -> None:
    """Update post_earns_* columns for a single (ticker, earnings_date) row."""
    allowed = {
        "post_earns_px_pct", "post_earns_vol_pct",
        "post_earns_avg_px_pct", "post_earns_avg_vol_pct",
    }
    safe = {k: v for k, v in fields.items() if k in allowed}
    if not safe:
        return
    set_clause = ", ".join(f"{k} = ?" for k in safe)
    vals = list(safe.values()) + [ticker, earnings_date]
    con = _conn()
    con.execute(
        f"UPDATE earnings_history SET {set_clause} "
        "WHERE ticker = ? AND earnings_date = ?",
        vals,
    )


def get_price_history_range(tickers: list[str], start_date: str, end_date: str) -> dict:
    """Return {ticker: {date_str: {"close": float, "volume": int}}} from price_history.

    Used by earnings_fetcher to compute extended metrics without external API calls.
    """
    if not tickers:
        return {}
    con = _conn()
    placeholders = ",".join("?" * len(tickers))
    rows = con.execute(
        f"SELECT ticker, date::TEXT, close, volume FROM price_history "
        f"WHERE ticker IN ({placeholders}) AND date >= ? AND date <= ? "
        f"ORDER BY ticker, date",
        tickers + [start_date, end_date],
    ).fetchall()
    result: dict = {}
    for ticker, date_str, close, volume in rows:
        if ticker not in result:
            result[ticker] = {}
        result[ticker][date_str] = {"close": close, "volume": volume}
    return result


def get_all_earnings_with_null_extended() -> dict[str, list[dict]]:
    """Return {earnings_date: [row_dict, ...]} for ALL earnings_history records
    where any extended column is NULL and earnings_date/earnings_time are set.

    Used by backfill_extended_columns — no date limit.
    """
    con = _conn()
    null_cond = (
        "earns_1d_vol_pct IS NULL OR earns_5d_px_pct IS NULL OR "
        "earns_5d_vol_pct IS NULL OR earns_5d_roll_px_pct IS NULL OR "
        "earns_5d_roll_vol_pct IS NULL"
    )
    rows = con.execute(
        f"SELECT ticker, earnings_date::TEXT, earnings_time, eps_est, eps_act, eps_sur, "
        f"eps_gaap_est, eps_gaap_act, eps_gaap_sur, rev_est_m, rev_act_m, rev_sur, "
        f"one_day_change, earns_1d_vol_pct, earns_5d_px_pct, earns_5d_vol_pct, "
        f"earns_5d_roll_px_pct, earns_5d_roll_vol_pct, fetch_date "
        f"FROM earnings_history "
        f"WHERE ({null_cond}) AND earnings_date IS NOT NULL AND earnings_time IS NOT NULL "
        f"ORDER BY earnings_date"
    ).fetchall()
    cols = ["ticker","earnings_date","earnings_time","eps_est","eps_act","eps_sur",
            "eps_gaap_est","eps_gaap_act","eps_gaap_sur","rev_est_m","rev_act_m","rev_sur",
            "one_day_change","earns_1d_vol_pct","earns_5d_px_pct","earns_5d_vol_pct",
            "earns_5d_roll_px_pct","earns_5d_roll_vol_pct","fetch_date"]
    by_date: dict = {}
    for row in rows:
        rec = dict(zip(cols, row))
        d = rec["earnings_date"]
        by_date.setdefault(d, []).append(rec)
    return by_date


def get_earnings_for_date(earnings_date: str) -> dict[str, dict]:
    """Return {ticker: row_dict} for all tickers with the given earnings_date."""
    con = _conn()
    rows = con.execute(
        "SELECT * FROM earnings_history WHERE earnings_date = ?",
        [earnings_date],
    ).fetchall()
    if not rows:
        return {}
    cols = [d[0] for d in con.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'earnings_history' ORDER BY ordinal_position"
    ).fetchall()]
    return {dict(zip(cols, r))["ticker"]: dict(zip(cols, r)) for r in rows}


# ── Price history ──────────────────────────────────────────────────────────────

def save_price_history(ticker: str, rows: list[tuple]) -> None:
    """Upsert price history rows for a ticker.

    Args:
        ticker: ticker symbol
        rows: list of (date_str, open, high, low, close, volume) or (date_str, close, volume) tuples
    """
    if not rows:
        return
    con = _conn()
    if len(rows[0]) >= 6:
        # Full OHLCV: (date, open, high, low, close, volume)
        con.executemany(
            "INSERT OR REPLACE INTO price_history (ticker, date, open, high, low, close, volume) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            [(ticker, r[0], r[1], r[2], r[3], r[4], r[5]) for r in rows],
        )
    else:
        # Legacy (date, close, volume) — leave open/high/low NULL
        con.executemany(
            "INSERT OR REPLACE INTO price_history (ticker, date, close, volume) VALUES (?, ?, ?, ?)",
            [(ticker, r[0], r[1], r[2]) for r in rows],
        )


def _pct(new_val, old_val) -> float | None:
    """Return percentage change from old_val to new_val, or None if either is missing."""
    if new_val is None or old_val is None or old_val == 0:
        return None
    return (new_val - old_val) / old_val * 100.0


def compute_returns_for_tickers(tickers: list[str]) -> dict[str, dict]:
    """Compute spot and rolling-average returns for each ticker from price_history.

    Returns {ticker: {col_name: float_or_None}} where col_names are the display
    column names used in VALUE_COL_GROUPS.

    Daily spot (single-day vs single-day):
        "5D Px%", "5D Vol%", "1M Px%", "1M Vol%", "3M Px%", "3M Vol%",
        "6M Px%", "6M Vol%", "12M Px%", "12M Vol%", "2Y Px%", "2Y Vol%",
        "3Y Px%", "3Y Vol%"

    Daily rolling avg (5-day avg vs 5-day avg):
        "1M Avg Px%", "1M Avg Vol%", "3M Avg Px%", "3M Avg Vol%",
        "6M Avg Px%", "6M Avg Vol%", "12M Avg Px%", "12M Avg Vol%",
        "2Y Avg Px%", "2Y Avg Vol%", "3Y Avg Px%", "3Y Avg Vol%"

    Weekly rolling avg (4-week avg vs 4-week avg):
        "1M Wkly Avg Px%", "1M Wkly Avg Vol%", "3M Wkly Avg Px%", "3M Wkly Avg Vol%",
        "6M Wkly Avg Px%", "6M Wkly Avg Vol%", "12M Wkly Avg Px%", "12M Wkly Avg Vol%",
        "2Y Wkly Avg Px%", "2Y Wkly Avg Vol%", "3Y Wkly Avg Px%", "3Y Wkly Avg Vol%"
    """
    if not tickers:
        return {}

    placeholders = ", ".join(["?" for _ in tickers])

    # ── Daily: spot + 5-day rolling avg ───────────────────────────────────────
    daily_sql = f"""
    WITH daily AS (
        SELECT ticker, date, close, volume,
            AVG(close) OVER (
                PARTITION BY ticker ORDER BY date
                ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
            ) AS close_5d_avg,
            AVG(volume) OVER (
                PARTITION BY ticker ORDER BY date
                ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
            ) AS vol_5d_avg,
            ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) AS rn_desc,
            ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date ASC)  AS rn_asc
        FROM price_history
        WHERE ticker IN ({placeholders})
    )
    SELECT ticker,
        MAX(CASE WHEN rn_desc = 1   THEN close        END) AS c_now,
        MAX(CASE WHEN rn_desc = 1   THEN volume       END) AS v_now,
        MAX(CASE WHEN rn_desc = 1   THEN close_5d_avg END) AS c5a_now,
        MAX(CASE WHEN rn_desc = 1   THEN vol_5d_avg   END) AS v5a_now,
        MAX(CASE WHEN rn_desc = 6   THEN close        END) AS c_5d,
        MAX(CASE WHEN rn_desc = 6   THEN volume       END) AS v_5d,
        MAX(CASE WHEN rn_desc = 22  THEN close        END) AS c_1m,
        MAX(CASE WHEN rn_desc = 22  THEN volume       END) AS v_1m,
        MAX(CASE WHEN rn_desc = 22  THEN close_5d_avg END) AS c5a_1m,
        MAX(CASE WHEN rn_desc = 22  THEN vol_5d_avg   END) AS v5a_1m,
        MAX(CASE WHEN rn_desc = 64  THEN close        END) AS c_3m,
        MAX(CASE WHEN rn_desc = 64  THEN volume       END) AS v_3m,
        MAX(CASE WHEN rn_desc = 64  THEN close_5d_avg END) AS c5a_3m,
        MAX(CASE WHEN rn_desc = 64  THEN vol_5d_avg   END) AS v5a_3m,
        MAX(CASE WHEN rn_desc = 127 THEN close        END) AS c_6m,
        MAX(CASE WHEN rn_desc = 127 THEN volume       END) AS v_6m,
        MAX(CASE WHEN rn_desc = 127 THEN close_5d_avg END) AS c5a_6m,
        MAX(CASE WHEN rn_desc = 127 THEN vol_5d_avg   END) AS v5a_6m,
        MAX(CASE WHEN rn_desc = 253 THEN close        END) AS c_12m,
        MAX(CASE WHEN rn_desc = 253 THEN volume       END) AS v_12m,
        MAX(CASE WHEN rn_desc = 253 THEN close_5d_avg END) AS c5a_12m,
        MAX(CASE WHEN rn_desc = 253 THEN vol_5d_avg   END) AS v5a_12m,
        MAX(CASE WHEN rn_desc = 505 THEN close        END) AS c_2y,
        MAX(CASE WHEN rn_desc = 505 THEN volume       END) AS v_2y,
        MAX(CASE WHEN rn_desc = 505 THEN close_5d_avg END) AS c5a_2y,
        MAX(CASE WHEN rn_desc = 505 THEN vol_5d_avg   END) AS v5a_2y,
        MAX(CASE WHEN rn_asc  = 1   THEN close        END) AS c_3y,
        MAX(CASE WHEN rn_asc  = 1   THEN volume       END) AS v_3y,
        MAX(CASE WHEN rn_asc  = 5   THEN close_5d_avg END) AS c5a_3y,
        MAX(CASE WHEN rn_asc  = 5   THEN vol_5d_avg   END) AS v5a_3y
    FROM daily
    GROUP BY ticker
    """

    # ── Weekly: 4-week rolling avg ─────────────────────────────────────────────
    weekly_sql = f"""
    WITH last_of_week AS (
        SELECT ticker,
            DATE_TRUNC('week', date) AS week_start,
            ARG_MAX(close, date)     AS close_w,
            ARG_MAX(volume, date)    AS vol_w
        FROM price_history
        WHERE ticker IN ({placeholders})
        GROUP BY ticker, DATE_TRUNC('week', date)
    ),
    weekly AS (
        SELECT ticker, week_start, close_w, vol_w,
            AVG(close_w) OVER (
                PARTITION BY ticker ORDER BY week_start
                ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
            ) AS close_4w_avg,
            AVG(vol_w) OVER (
                PARTITION BY ticker ORDER BY week_start
                ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
            ) AS vol_4w_avg,
            ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY week_start DESC) AS wn_desc,
            ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY week_start ASC)  AS wn_asc
        FROM last_of_week
    )
    SELECT ticker,
        MAX(CASE WHEN wn_desc = 1  THEN close_4w_avg END) AS wc4a_now,
        MAX(CASE WHEN wn_desc = 1  THEN vol_4w_avg   END) AS wv4a_now,
        MAX(CASE WHEN wn_desc = 5  THEN close_4w_avg END) AS wc4a_1m,
        MAX(CASE WHEN wn_desc = 5  THEN vol_4w_avg   END) AS wv4a_1m,
        MAX(CASE WHEN wn_desc = 13 THEN close_4w_avg END) AS wc4a_3m,
        MAX(CASE WHEN wn_desc = 13 THEN vol_4w_avg   END) AS wv4a_3m,
        MAX(CASE WHEN wn_desc = 26 THEN close_4w_avg END) AS wc4a_6m,
        MAX(CASE WHEN wn_desc = 26 THEN vol_4w_avg   END) AS wv4a_6m,
        MAX(CASE WHEN wn_desc = 52 THEN close_4w_avg END) AS wc4a_12m,
        MAX(CASE WHEN wn_desc = 52 THEN vol_4w_avg   END) AS wv4a_12m,
        MAX(CASE WHEN wn_desc = 105 THEN close_4w_avg END) AS wc4a_2y,
        MAX(CASE WHEN wn_desc = 105 THEN vol_4w_avg   END) AS wv4a_2y,
        MAX(CASE WHEN wn_asc  = 4  THEN close_4w_avg END) AS wc4a_3y,
        MAX(CASE WHEN wn_asc  = 4  THEN vol_4w_avg   END) AS wv4a_3y
    FROM weekly
    GROUP BY ticker
    """

    con = _conn()
    daily_rows = con.execute(daily_sql, tickers).fetchall()
    weekly_rows = con.execute(weekly_sql, tickers).fetchall()

    # Index weekly by ticker
    weekly_map: dict[str, tuple] = {r[0]: r for r in weekly_rows}

    result: dict[str, dict] = {}
    for r in daily_rows:
        t = r[0]
        (c_now, v_now, c5a_now, v5a_now,
         c_5d, v_5d,
         c_1m, v_1m, c5a_1m, v5a_1m,
         c_3m, v_3m, c5a_3m, v5a_3m,
         c_6m, v_6m, c5a_6m, v5a_6m,
         c_12m, v_12m, c5a_12m, v5a_12m,
         c_2y, v_2y, c5a_2y, v5a_2y,
         c_3y, v_3y, c5a_3y, v5a_3y) = r[1:]

        wr = weekly_map.get(t)
        if wr:
            (wc4a_now, wv4a_now,
             wc4a_1m, wv4a_1m,
             wc4a_3m, wv4a_3m,
             wc4a_6m, wv4a_6m,
             wc4a_12m, wv4a_12m,
             wc4a_2y, wv4a_2y,
             wc4a_3y, wv4a_3y) = wr[1:]
        else:
            (wc4a_now, wv4a_now,
             wc4a_1m, wv4a_1m,
             wc4a_3m, wv4a_3m,
             wc4a_6m, wv4a_6m,
             wc4a_12m, wv4a_12m,
             wc4a_2y, wv4a_2y,
             wc4a_3y, wv4a_3y) = (None,) * 14

        result[t] = {
            # Spot (single-day vs single-day)
            "5D Px%":   _pct(c_now, c_5d),
            "5D Vol%":  _pct(v_now, v_5d),
            "1M Px%":   _pct(c_now, c_1m),
            "1M Vol%":  _pct(v_now, v_1m),
            "3M Px%":   _pct(c_now, c_3m),
            "3M Vol%":  _pct(v_now, v_3m),
            "6M Px%":   _pct(c_now, c_6m),
            "6M Vol%":  _pct(v_now, v_6m),
            "12M Px%":  _pct(c_now, c_12m),
            "12M Vol%": _pct(v_now, v_12m),
            "2Y Px%":   _pct(c_now, c_2y),
            "2Y Vol%":  _pct(v_now, v_2y),
            "3Y Px%":   _pct(c_now, c_3y),
            "3Y Vol%":  _pct(v_now, v_3y),
            # Daily rolling avg — NEW periods only (3M/12M come from T1 indicator)
            "1M Avg Px%":  _pct(c5a_now, c5a_1m),
            "1M Avg Vol%": _pct(v5a_now, v5a_1m),
            "6M Avg Px%":  _pct(c5a_now, c5a_6m),
            "6M Avg Vol%": _pct(v5a_now, v5a_6m),
            "2Y Avg Px%":  _pct(c5a_now, c5a_2y),
            "2Y Avg Vol%": _pct(v5a_now, v5a_2y),
            "3Y Avg Px%":  _pct(c5a_now, c5a_3y),
            "3Y Avg Vol%": _pct(v5a_now, v5a_3y),
            # Weekly rolling avg — NEW periods only (3M/12M come from T2 indicator)
            "1M Wkly Avg Px%":  _pct(wc4a_now, wc4a_1m),
            "1M Wkly Avg Vol%": _pct(wv4a_now, wv4a_1m),
            "6M Wkly Avg Px%":  _pct(wc4a_now, wc4a_6m),
            "6M Wkly Avg Vol%": _pct(wv4a_now, wv4a_6m),
            "2Y Wkly Avg Px%":  _pct(wc4a_now, wc4a_2y),
            "2Y Wkly Avg Vol%": _pct(wv4a_now, wv4a_2y),
            "3Y Wkly Avg Px%":  _pct(wc4a_now, wc4a_3y),
            "3Y Wkly Avg Vol%": _pct(wv4a_now, wv4a_3y),
        }

    return result


def get_nth_trading_day_back(tickers: list[str], n: int) -> str | None:
    """Return the date that is the Nth most-recent trading day in price_history.

    Uses the most-common rn_desc=n date across the provided tickers (all tickers
    in the same universe share the same calendar, so this is consistent).
    Returns None if price_history is empty.
    """
    if not tickers:
        return None
    placeholders = ", ".join(["?" for _ in tickers])
    con = _conn()
    row = con.execute(
        f"""
        SELECT CAST(date AS TEXT)
        FROM (
            SELECT date,
                ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) AS rn
            FROM price_history WHERE ticker IN ({placeholders})
        ) t
        WHERE rn = ?
        GROUP BY date
        ORDER BY COUNT(*) DESC
        LIMIT 1
        """,
        tickers + [n],
    ).fetchone()
    return row[0] if row else None


def get_custom_period_returns(
    tickers: list[str],
    start_date: str,
    end_date: str,
) -> dict[str, dict]:
    """Compute custom-period spot and rolling-avg returns from price_history.

    Finds the closest available trading day on or before start_date and on or
    before end_date (i.e. both snap to the previous trading day), then computes:
      - "Cust Px%"     : close at end vs close at start
      - "Cust Avg Px%" : 5d avg at end vs 5d avg at start
      - "Cust Vol%"    : volume at end vs volume at start
      - "Cust Avg Vol%": 5d avg vol at end vs 5d avg vol at start

    Returns {ticker: {col: float_or_None}}.
    """
    if not tickers:
        return {}

    placeholders = ", ".join(["?" for _ in tickers])

    # Compute rolling avgs over ALL rows first (no date filter in the window),
    # then pick the closest available date to start/end in the outer query.
    # This avoids the window being truncated by a WHERE date >= start_date filter,
    # which would give a partial (< 5-day) average at the boundary row.
    combined_sql = f"""
    WITH all_avgs AS (
        SELECT ticker, date, close, volume,
            AVG(close) OVER (
                PARTITION BY ticker ORDER BY date
                ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
            ) AS close_5d_avg,
            AVG(volume) OVER (
                PARTITION BY ticker ORDER BY date
                ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
            ) AS vol_5d_avg
        FROM price_history
        WHERE ticker IN ({placeholders})
    ),
    start_ranked AS (
        SELECT ticker, close, volume, close_5d_avg, vol_5d_avg,
            ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) AS rn
        FROM all_avgs WHERE date <= ?
    ),
    end_ranked AS (
        SELECT ticker, close, volume, close_5d_avg, vol_5d_avg,
            ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) AS rn
        FROM all_avgs WHERE date <= ?
    )
    SELECT 'start' AS side, ticker, close, volume, close_5d_avg, vol_5d_avg
    FROM start_ranked WHERE rn = 1
    UNION ALL
    SELECT 'end'   AS side, ticker, close, volume, close_5d_avg, vol_5d_avg
    FROM end_ranked WHERE rn = 1
    """

    con = _conn()
    rows = con.execute(combined_sql, tickers + [start_date, end_date]).fetchall()

    start_rows: dict[str, tuple] = {}
    end_rows:   dict[str, tuple] = {}
    for side, ticker, close, volume, c5a, v5a in rows:
        if side == "start":
            start_rows[ticker] = (close, volume, c5a, v5a)
        else:
            end_rows[ticker]   = (close, volume, c5a, v5a)

    result: dict[str, dict] = {}
    for ticker in tickers:
        s = start_rows.get(ticker)
        e = end_rows.get(ticker)
        if s is None or e is None:
            result[ticker] = {
                "Cust Px%": None, "Cust Avg Px%": None,
                "Cust Vol%": None, "Cust Avg Vol%": None,
            }
        else:
            result[ticker] = {
                "Cust Px%":     _pct(e[0], s[0]),
                "Cust Avg Px%": _pct(e[2], s[2]),
                "Cust Vol%":    _pct(e[1], s[1]),
                "Cust Avg Vol%":_pct(e[3], s[3]),
            }
    return result


def get_latest_price_dates(tickers: list[str]) -> dict[str, str]:
    """Return {ticker: latest_date_str} for each ticker in price_history."""
    if not tickers:
        return {}
    placeholders = ", ".join(["?" for _ in tickers])
    con = _conn()
    rows = con.execute(
        f"""
        SELECT ticker, CAST(MAX(date) AS TEXT)
        FROM price_history
        WHERE ticker IN ({placeholders})
        GROUP BY ticker
        """,
        tickers,
    ).fetchall()
    return {r[0]: r[1] for r in rows}


def get_price_history_max_date(tickers: list[str]) -> str | None:
    """Return the global max date across all specified tickers in price_history."""
    if not tickers:
        return None
    placeholders = ", ".join(["?" for _ in tickers])
    con = _conn()
    row = con.execute(
        f"SELECT CAST(MAX(date) AS TEXT) FROM price_history WHERE ticker IN ({placeholders})",
        tickers,
    ).fetchone()
    return row[0] if row else None


def get_trigger_period_returns(
    ticker_date_pairs: dict[str, tuple[str, str]],
) -> dict[str, dict]:
    """Compute returns for each ticker between its specific (start_date, end_date).

    Uses the same 5-day rolling average window logic as get_custom_period_returns.
    Returns {ticker: {"px_pct":..., "vol_pct":..., "avg_px_pct":..., "avg_vol_pct":...}}
    """
    if not ticker_date_pairs:
        return {}

    tickers = list(ticker_date_pairs.keys())
    placeholders = ", ".join(["?" for _ in tickers])

    # Build VALUES clause: (ticker, start_date, end_date)
    values_rows = []
    values_params: list = []
    for ticker, (start_date, end_date) in ticker_date_pairs.items():
        values_rows.append("(?, ?, ?)")
        values_params.extend([ticker, start_date, end_date])

    values_sql = ", ".join(values_rows)

    sql = f"""
    WITH date_pairs(ticker, start_date, end_date) AS (
        VALUES {values_sql}
    ),
    all_avgs AS (
        SELECT
            ph.ticker, ph.date, ph.close, ph.volume,
            AVG(ph.close) OVER (
                PARTITION BY ph.ticker ORDER BY ph.date
                ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
            ) AS close_5d_avg,
            AVG(CAST(ph.volume AS DOUBLE)) OVER (
                PARTITION BY ph.ticker ORDER BY ph.date
                ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
            ) AS vol_5d_avg
        FROM price_history ph
        WHERE ph.ticker IN ({placeholders})
    ),
    start_prices AS (
        SELECT
            dp.ticker,
            aa.close AS s_close, aa.volume AS s_vol,
            aa.close_5d_avg AS s_c5a, aa.vol_5d_avg AS s_v5a,
            ROW_NUMBER() OVER (PARTITION BY dp.ticker ORDER BY aa.date DESC) AS rn
        FROM date_pairs dp
        JOIN all_avgs aa
            ON dp.ticker = aa.ticker
            AND aa.date <= CAST(dp.start_date AS DATE)
    ),
    end_prices AS (
        SELECT
            dp.ticker,
            aa.close AS e_close, aa.volume AS e_vol,
            aa.close_5d_avg AS e_c5a, aa.vol_5d_avg AS e_v5a,
            ROW_NUMBER() OVER (PARTITION BY dp.ticker ORDER BY aa.date DESC) AS rn
        FROM date_pairs dp
        JOIN all_avgs aa
            ON dp.ticker = aa.ticker
            AND aa.date <= CAST(dp.end_date AS DATE)
    )
    SELECT
        s.ticker,
        CASE WHEN s.s_close > 0  THEN (e.e_close / s.s_close - 1) * 100  ELSE NULL END AS px_pct,
        CASE WHEN s.s_vol > 0    THEN (CAST(e.e_vol AS DOUBLE) / s.s_vol - 1) * 100 ELSE NULL END AS vol_pct,
        CASE WHEN s.s_c5a > 0   THEN (e.e_c5a / s.s_c5a - 1) * 100     ELSE NULL END AS avg_px_pct,
        CASE WHEN s.s_v5a > 0   THEN (e.e_v5a / s.s_v5a - 1) * 100     ELSE NULL END AS avg_vol_pct
    FROM (SELECT * FROM start_prices WHERE rn = 1) s
    JOIN (SELECT * FROM end_prices WHERE rn = 1) e ON s.ticker = e.ticker
    """

    con = _conn()
    rows = con.execute(sql, values_params + tickers).fetchall()

    def _r(v):
        return round(v, 4) if v is not None else None

    result: dict[str, dict] = {}
    for ticker, px, vol, avg_px, avg_vol in rows:
        result[ticker] = {
            "px_pct":     _r(px),
            "vol_pct":    _r(vol),
            "avg_px_pct": _r(avg_px),
            "avg_vol_pct":_r(avg_vol),
        }
    return result


def get_trigger_cache(
    cache_key: str,
    tickers: list[str],
    current_max_date: str | None,
) -> dict[str, dict] | None:
    """Return cached trigger results if valid; None on miss or stale.

    Cache is valid when:
    - All requested tickers have an entry for this cache_key
    - Every entry has max_ph_date == current_max_date
    """
    if not tickers or current_max_date is None:
        return None

    placeholders = ", ".join(["?" for _ in tickers])
    con = _conn()
    rows = con.execute(
        f"""
        SELECT ticker, start_date, end_date,
               trig_px_pct, trig_vol_pct, trig_avg_px_pct, trig_avg_vol_pct
        FROM trigger_cache
        WHERE cache_key = ?
          AND ticker IN ({placeholders})
          AND (max_ph_date = ? OR (max_ph_date IS NULL AND ? IS NULL))
        """,
        [cache_key] + tickers + [current_max_date, current_max_date],
    ).fetchall()

    if len(rows) < len(tickers):
        return None  # incomplete coverage

    result: dict[str, dict] = {}
    for row in rows:
        ticker, start_d, end_d, px, vol, avg_px, avg_vol = row
        result[ticker] = {
            "Trig Start Date": start_d,
            "Trig End Date":   end_d,
            "Trig Px%":        px,
            "Trig Vol%":       vol,
            "Trig Avg Px%":    avg_px,
            "Trig Avg Vol%":   avg_vol,
        }
    return result


def save_trigger_cache(
    cache_key: str,
    results: dict[str, dict],
    max_ph_date: str | None,
) -> None:
    """Upsert trigger results into trigger_cache for the given cache_key."""
    if not results:
        return
    now = datetime.now(ZoneInfo("America/Chicago"))
    con = _conn()
    # Remove stale entries for this cache_key first
    con.execute("DELETE FROM trigger_cache WHERE cache_key = ?", [cache_key])
    for ticker, r in results.items():
        con.execute(
            """
            INSERT OR REPLACE INTO trigger_cache
                (cache_key, ticker, start_date, end_date,
                 trig_px_pct, trig_vol_pct, trig_avg_px_pct, trig_avg_vol_pct,
                 max_ph_date, computed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                cache_key, ticker,
                r.get("Trig Start Date"), r.get("Trig End Date"),
                r.get("Trig Px%"), r.get("Trig Vol%"),
                r.get("Trig Avg Px%"), r.get("Trig Avg Vol%"),
                max_ph_date, now,
            ],
        )


def get_event_scan_stats(
    pairs: list[tuple[str, str]],
) -> list[dict]:
    """Given [(ticker, trigger_date), ...], compute post-trigger stats for Event Scanner.

    Supports multiple trigger dates per ticker. Returns a list of dicts (one per
    input pair), each containing:
        "ticker", "trigger_date", "trig_close", "trig_vol", "trig_dollar_vol",
        "latest_date", "latest_close", "latest_vol", "latest_dollar_vol",
        "px_pct", "vol_pct",
        "max_high", "max_date", "max_close", "max_vol", "max_dollar_vol",
        "max_high_pct", "max_vol_pct",
        "min_low", "min_date", "min_close", "min_vol", "min_dollar_vol",
        "min_low_pct", "min_vol_pct",
        "days_since_trig", "days_to_max", "days_to_min",
        "trig_mkt_cap_b", "earns_1d_px_pct", "sector", "industry", "_run_row",
    """
    if not pairs:
        return []

    tickers = list(dict.fromkeys(t for t, _ in pairs))
    placeholders = ", ".join(["?" for _ in tickers])

    val_rows = ", ".join("(?, ?)" for _ in pairs)
    val_params: list = []
    for t, d in pairs:
        val_params.extend([t, d])

    # ── OHLC-based post-trigger stats via QUALIFY CTEs ────────────────────────
    # All window functions partition by (ticker, trigger_date) so multiple
    # trigger dates per ticker are handled independently.
    sql_ph = f"""
    WITH triggers(ticker, trigger_date) AS (
        VALUES {val_rows}
    ),
    post_range AS (
        SELECT
            ph.ticker,
            tr.trigger_date,
            ph.date,
            COALESCE(ph.high, ph.close)                  AS day_high,
            COALESCE(ph.low,  ph.close)                  AS day_low,
            ph.close,
            CAST(ph.volume AS DOUBLE)                    AS volume,
            ph.close * CAST(ph.volume AS DOUBLE)         AS dollar_vol
        FROM price_history ph
        JOIN triggers tr ON ph.ticker = tr.ticker
        WHERE ph.date >= CAST(tr.trigger_date AS DATE)
    ),
    trig_row AS (
        SELECT ticker, trigger_date,
               CAST(date AS TEXT) AS trig_date,
               close              AS trig_close,
               volume             AS trig_vol,
               dollar_vol         AS trig_dollar_vol
        FROM post_range
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY ticker, trigger_date ORDER BY date ASC
        ) = 1
    ),
    latest_row AS (
        SELECT ticker, trigger_date,
               CAST(date AS TEXT) AS last_date,
               close              AS last_close,
               volume             AS last_vol,
               dollar_vol         AS last_dollar_vol
        FROM post_range
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY ticker, trigger_date ORDER BY date DESC
        ) = 1
    ),
    max_row AS (
        SELECT ticker, trigger_date,
               CAST(date AS TEXT) AS max_date,
               day_high           AS max_high,
               close              AS max_close,
               volume             AS max_vol,
               dollar_vol         AS max_dollar_vol
        FROM post_range
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY ticker, trigger_date ORDER BY day_high DESC, date ASC
        ) = 1
    ),
    min_row AS (
        SELECT ticker, trigger_date,
               CAST(date AS TEXT) AS min_date,
               day_low            AS min_low,
               close              AS min_close,
               volume             AS min_vol,
               dollar_vol         AS min_dollar_vol
        FROM post_range
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY ticker, trigger_date ORDER BY day_low ASC, date ASC
        ) = 1
    )
    SELECT
        tr.ticker,
        CAST(tr.trigger_date AS TEXT),
        tr.trig_date,    tr.trig_close,   tr.trig_vol,   tr.trig_dollar_vol,
        la.last_date,    la.last_close,   la.last_vol,   la.last_dollar_vol,
        mx.max_date,     mx.max_high,     mx.max_close,  mx.max_vol,   mx.max_dollar_vol,
        mn.min_date,     mn.min_low,      mn.min_close,  mn.min_vol,   mn.min_dollar_vol
    FROM trig_row tr
    LEFT JOIN latest_row la
        ON tr.ticker = la.ticker AND tr.trigger_date = la.trigger_date
    LEFT JOIN max_row mx
        ON tr.ticker = mx.ticker AND tr.trigger_date = mx.trigger_date
    LEFT JOIN min_row mn
        ON tr.ticker = mn.ticker AND tr.trigger_date = mn.trigger_date
    """

    con = _conn()
    try:
        ph_rows = con.execute(sql_ph, val_params).fetchall()
    except Exception:
        ph_rows = []

    # Keyed by (ticker, trigger_date)
    ph_map: dict[tuple[str, str], dict] = {}
    for row in ph_rows:
        (ticker, trigger_date,
         trig_date,  trig_close,  trig_vol,  trig_dollar_vol,
         last_date,  last_close,  last_vol,  last_dollar_vol,
         max_date,   max_high,    max_close, max_vol,  max_dollar_vol,
         min_date,   min_low,     min_close, min_vol,  min_dollar_vol) = row
        ph_map[(ticker, str(trigger_date))] = {
            "trig_date":        str(trig_date)  if trig_date  else None,
            "trig_close":       trig_close,
            "trig_vol":         trig_vol,
            "trig_dollar_vol":  trig_dollar_vol,
            "last_date":        str(last_date)  if last_date  else None,
            "last_close":       last_close,
            "last_vol":         last_vol,
            "last_dollar_vol":  last_dollar_vol,
            "max_date":         str(max_date)   if max_date   else None,
            "max_high":         max_high,
            "max_close":        max_close,
            "max_vol":          max_vol,
            "max_dollar_vol":   max_dollar_vol,
            "min_date":         str(min_date)   if min_date   else None,
            "min_low":          min_low,
            "min_close":        min_close,
            "min_vol":          min_vol,
            "min_dollar_vol":   min_dollar_vol,
        }

    # ── Latest score from analysis_runs ──────────────────────────────────────
    try:
        score_rows = con.execute(
            f"""
            SELECT ticker,
                   T1, T2, T3, T4, F1, F2, F3, F4, F5, F6,
                   T1_sub_3m_price, T1_sub_3m_vol, T1_sub_12m_price, T1_sub_12m_vol,
                   T2_sub_3m_price, T2_sub_3m_vol, T2_sub_12m_price, T2_sub_12m_vol,
                   T3_sub_ma10_20, T3_sub_ma20_50, T3_sub_ma50_150, T3_sub_ma150_200,
                   T4_sub_has_big_up, T4_sub_no_big_down,
                   F1_sub_q_rev, F1_sub_q_eps, F2_sub_a_rev, F2_sub_a_eps,
                   F3_sub_q_rev_yoy, F3_sub_q_eps_yoy, F4_sub_a_rev_yoy, F4_sub_a_eps_yoy,
                   market_cap,
                   run_dt
            FROM analysis_runs
            WHERE ticker IN ({placeholders})
            QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY run_dt DESC) = 1
            """,
            tickers,
        ).fetchall()
    except Exception:
        score_rows = []

    _score_cols = [
        "ticker", "T1", "T2", "T3", "T4", "F1", "F2", "F3", "F4", "F5", "F6",
        "T1_sub_3m_price", "T1_sub_3m_vol", "T1_sub_12m_price", "T1_sub_12m_vol",
        "T2_sub_3m_price", "T2_sub_3m_vol", "T2_sub_12m_price", "T2_sub_12m_vol",
        "T3_sub_ma10_20", "T3_sub_ma20_50", "T3_sub_ma50_150", "T3_sub_ma150_200",
        "T4_sub_has_big_up", "T4_sub_no_big_down",
        "F1_sub_q_rev", "F1_sub_q_eps", "F2_sub_a_rev", "F2_sub_a_eps",
        "F3_sub_q_rev_yoy", "F3_sub_q_eps_yoy", "F4_sub_a_rev_yoy", "F4_sub_a_eps_yoy",
        "market_cap", "run_dt",
    ]
    score_map: dict[str, dict] = {}
    for row in score_rows:
        d = dict(zip(_score_cols, row))
        score_map[d["ticker"]] = d

    # ── Sector/industry from fundamentals ─────────────────────────────────────
    try:
        fund_rows = con.execute(
            f"""
            SELECT ticker, raw_info_json
            FROM fundamentals
            WHERE ticker IN ({placeholders})
            QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY fetch_date DESC) = 1
            """,
            tickers,
        ).fetchall()
    except Exception:
        fund_rows = []

    si_map: dict[str, tuple[str, str]] = {}
    for ticker, raw_json in fund_rows:
        try:
            info = json.loads(raw_json) if raw_json else {}
            si_map[ticker] = (info.get("sector") or "N/A", info.get("industry") or "N/A")
        except Exception:
            si_map[ticker] = ("N/A", "N/A")

    # ── Earnings 1D px% closest to each trigger date ─────────────────────────
    # Partition by (ticker, trigger_date) so each event gets its own lookup.
    try:
        earns_rows = con.execute(
            f"""
            WITH triggers(ticker, trigger_date) AS (VALUES {val_rows}),
            ranked AS (
                SELECT e.ticker,
                       CAST(tr.trigger_date AS TEXT) AS trigger_date,
                       e.one_day_change,
                       ROW_NUMBER() OVER (
                           PARTITION BY e.ticker, tr.trigger_date
                           ORDER BY ABS(DATEDIFF('day',
                               CAST(e.earnings_date AS DATE),
                               CAST(tr.trigger_date AS DATE)
                           ))
                       ) AS rn
                FROM earnings_history e
                JOIN triggers tr ON e.ticker = tr.ticker
                WHERE e.earnings_date <= tr.trigger_date
            )
            SELECT ticker, trigger_date, one_day_change FROM ranked WHERE rn = 1
            """,
            val_params,
        ).fetchall()
    except Exception:
        earns_rows = []

    # Keyed by (ticker, trigger_date)
    earns_map: dict[tuple[str, str], float | None] = {
        (r[0], r[1]): r[2] for r in earns_rows
    }

    # ── Mkt cap from analysis_runs at each trigger date ───────────────────────
    try:
        trig_mc_rows = con.execute(
            f"""
            WITH triggers(ticker, trigger_date) AS (VALUES {val_rows})
            SELECT ar.ticker,
                   CAST(tr.trigger_date AS TEXT) AS trigger_date,
                   ar.market_cap
            FROM analysis_runs ar
            JOIN triggers tr ON ar.ticker = tr.ticker
            WHERE CAST(ar.run_dt AS DATE) = CAST(tr.trigger_date AS DATE)
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY ar.ticker, tr.trigger_date ORDER BY ar.run_dt DESC
            ) = 1
            """,
            val_params,
        ).fetchall()
    except Exception:
        trig_mc_rows = []

    trig_mc_map: dict[tuple[str, str], float | None] = {
        (r[0], r[1]): r[2] for r in trig_mc_rows
    }

    from datetime import date as _date

    def _days_between(d1: str | None, d2: str | None) -> int | None:
        if not d1 or not d2:
            return None
        try:
            return (_date.fromisoformat(d2) - _date.fromisoformat(d1)).days
        except Exception:
            return None

    def _pct_change(new_val, base_val) -> float | None:
        try:
            if base_val and base_val != 0:
                return round((float(new_val) / float(base_val) - 1) * 100, 2)
        except Exception:
            pass
        return None

    result: list[dict] = []
    for ticker, td in pairs:
        key = (ticker, td)
        ph  = ph_map.get(key, {})
        sc  = score_map.get(ticker, {})
        sector, industry = si_map.get(ticker, ("N/A", "N/A"))

        trig_close      = ph.get("trig_close")
        trig_vol        = ph.get("trig_vol")
        trig_dollar_vol = ph.get("trig_dollar_vol")
        last_close      = ph.get("last_close")
        last_vol        = ph.get("last_vol")
        last_dollar_vol = ph.get("last_dollar_vol")
        last_date       = ph.get("last_date")
        max_high        = ph.get("max_high")
        max_date        = ph.get("max_date")
        max_close       = ph.get("max_close")
        max_vol         = ph.get("max_vol")
        max_dollar_vol  = ph.get("max_dollar_vol")
        min_low         = ph.get("min_low")
        min_date        = ph.get("min_date")
        min_close       = ph.get("min_close")
        min_vol         = ph.get("min_vol")
        min_dollar_vol  = ph.get("min_dollar_vol")

        trig_mc   = trig_mc_map.get(key)
        latest_mc = sc.get("market_cap")
        trig_mkt_cap_b = None
        if trig_mc:
            trig_mkt_cap_b = round(trig_mc / 1e9, 2)
        elif latest_mc and trig_close and last_close and last_close != 0:
            trig_mkt_cap_b = round(latest_mc * (trig_close / last_close) / 1e9, 2)

        result.append({
            "ticker":           ticker,
            "trigger_date":     td,
            "trig_close":       trig_close,
            "trig_vol":         trig_vol,
            "trig_dollar_vol":  trig_dollar_vol,
            "latest_date":      last_date,
            "latest_close":     last_close,
            "latest_vol":       last_vol,
            "latest_dollar_vol": last_dollar_vol,
            "px_pct":           _pct_change(last_close, trig_close),
            "vol_pct":          _pct_change(last_vol, trig_vol),
            "max_high":         max_high,
            "max_date":         max_date,
            "max_close":        max_close,
            "max_vol":          max_vol,
            "max_dollar_vol":   max_dollar_vol,
            "max_high_pct":     _pct_change(max_high, trig_close),
            "max_vol_pct":      _pct_change(max_vol, trig_vol),
            "min_low":          min_low,
            "min_date":         min_date,
            "min_close":        min_close,
            "min_vol":          min_vol,
            "min_dollar_vol":   min_dollar_vol,
            "min_low_pct":      _pct_change(min_low, trig_close),
            "min_vol_pct":      _pct_change(min_vol, trig_vol),
            "days_since_trig":  _days_between(td, last_date),
            "days_to_max":      _days_between(td, max_date),
            "days_to_min":      _days_between(td, min_date),
            "trig_mkt_cap_b":   trig_mkt_cap_b,
            "earns_1d_px_pct":  earns_map.get(key),
            "sector":           sector,
            "industry":         industry,
            "_run_row":         sc,
        })

    return result


def update_earnings_extended(ticker: str, earnings_date: str, fields: dict) -> None:
    """Update extended earnings columns for an existing earnings_history row.

    fields may contain any subset of:
        earns_1d_vol_pct, earns_5d_px_pct, earns_5d_vol_pct,
        earns_5d_roll_px_pct, earns_5d_roll_vol_pct
    """
    if not fields:
        return
    allowed = {
        "earns_1d_vol_pct", "earns_5d_px_pct", "earns_5d_vol_pct",
        "earns_5d_roll_px_pct", "earns_5d_roll_vol_pct",
    }
    safe_fields = {k: v for k, v in fields.items() if k in allowed}
    if not safe_fields:
        return
    set_clause = ", ".join(f"{k} = ?" for k in safe_fields)
    vals = list(safe_fields.values()) + [ticker, earnings_date]
    con = _conn()
    con.execute(
        f"UPDATE earnings_history SET {set_clause} "
        "WHERE ticker = ? AND earnings_date = ?",
        vals,
    )
