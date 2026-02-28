"""
app.py — Streamlit UI for Stock Analysis Tool (v3)

Run: streamlit run app.py
"""

from __future__ import annotations
import json
import threading
import concurrent.futures
import time
from datetime import datetime, date
from pathlib import Path
from zoneinfo import ZoneInfo

# ─────────────────────────────────────────────────────────────────────────────
# User preferences (persisted to JSON)
# ─────────────────────────────────────────────────────────────────────────────

_PREFS_PATH = Path(__file__).parent / "user_prefs.json"


def _load_prefs() -> dict:
    if _PREFS_PATH.exists():
        try:
            return json.loads(_PREFS_PATH.read_text())
        except Exception:
            pass
    return {}


def _save_prefs(prefs: dict) -> None:
    _PREFS_PATH.write_text(json.dumps(prefs, indent=2))

import pandas as pd
import streamlit as st

from data_fetcher  import fetch_technical, fetch_technical_bulk
from technical_fetcher import fetch_and_store_bulk as fetch_technical_bulk_v2
from fundamental_fetcher import fetch_fundamental
from peers_fetcher import get_peer_valuations, clear_peer_cache
import vpn_switcher
from indicators    import evaluate_all, score_indicators
from storage import (
    init_db, save_results, update_field,
    get_all_run_datetimes, get_latest_run_datetime,
    get_summary_for_run, get_detail_for_run, get_all_summaries,
    get_detail_filtered, get_all_tickers,
    get_datetimes_for_ticker, get_tickers_for_datetime,
    get_cached_peer_valuations, save_peer_valuations,
    get_all_fundamentals_for_run, get_tech_for_tickers,
    MAIN_IND_COLS, ALL_SUB_COLS, SUB_COLS,
)

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

CST = ZoneInfo("America/Chicago")

EMOJI = {"PASS": "✅", "PARTIAL": "⭕", "FAIL": "❌", "NA": "⚪️"}
EMOJI_TO_DB = {v: k for k, v in EMOJI.items()}
EMOJI_OPTIONS = ["✅", "⭕", "❌", "⚪️"]

TICKERS_FILE = Path(__file__).parent / "tickers.txt"

# ── Value table column groups ──────────────────────────────────────────────────
VALUE_COL_GROUPS: dict[str, list[str]] = {
    # ── Indicator-derived groups (from analysis detail JSON) ──────────────────
    "Price & Volume — Daily (T1)": [
        "3M Daily Px%", "3M Daily Vol%", "12M Daily Px%", "12M Daily Vol%",
    ],
    "Price & Volume — Weekly (T2)": [
        "3M Wkly Px%", "3M Wkly Vol%", "12M Wkly Px%", "12M Wkly Vol%",
    ],
    "MA Checks (T3)": [
        "MA10>20", "MA20>50", "MA50>150", "MA150>200",
    ],
    "MA Values (T3)": [
        "MA10", "MA20", "MA50", "MA150", "MA200",
    ],
    "Big Moves 90d (T4)": [
        "# Up≥10%", "# Dn≥10%",
    ],
    "Quarterly (F1/F3)": [
        "Q Rev", "Q EPS", "Q End Date", "Q Rev YoY%", "Q EPS YoY%",
    ],
    "Annual (F2/F4)": [
        "A Rev", "A EPS", "A End Date", "A Rev YoY%", "A EPS YoY%",
    ],
    "Valuation (F5/F6)": [
        "Fwd PE", "Fwd PE vs Med%", "P/B", "P/B vs Med%",
    ],
    "Fundamentals": [
        "Mkt Cap ($B)", "Sector", "Industry",
    ],
    # ── tech_indicators groups ─────────────────────────────────────────────────
    "Price & 52W": [
        "Tech Date", "Close", "Latest Px %", "52W High", "52W Low", "From 52W High%", "From 52W Low%", "52W Pos%",
    ],
    "EMA & Slope": [
        "EMA9", "EMA21", "EMA50", "EMA200", "SMA50 Slope 20D", "From SMA200%",
    ],
    "Momentum": [
        "RSI14", "MACD Line", "MACD Signal", "MACD Hist", "Stoch K", "Stoch D",
    ],
    "Bollinger Bands": [
        "BB Upper", "BB Middle", "BB Lower", "BB %B",
    ],
    "Volatility": [
        "ATR14", "ATR%", "ADX14", "+DI", "-DI", "RVol 20D%", "RVol 60D%",
    ],
    "Drawdown": [
        "Max DD 63D%", "Max DD 252D%",
    ],
    "Volume": [
        "OBV", "CMF20", "AD Line", "Avg $Vol 20D", "Avg $Vol 50D", "Med Vol 50D",
    ],
    "Donchian": [
        "Don High 20", "Don Low 20", "Don High 55", "Don Low 55",
        "Don High 252", "Don Low 252",
        "From 20D High%", "From 55D High%", "From 252D High%",
        "Breakout 55D", "Breakout 3M",
    ],
    "Rolling Stats 3M": [
        "Up Days 3M", "Down Days 3M", "UD Ratio 3M", "Max Win Str 3M", "Win Str 5% 3M",
    ],
    "Rolling Stats 1Y": [
        "Up Days 1Y", "Down Days 1Y", "UD Ratio 1Y", "Max Win Str 1Y", "Win Str 5% 1Y",
    ],
    "Gap Stats": [
        "Gap Rate 60D%", "Max Gap 60D%",
    ],
    "Big Moves 90D": [
        "# Big Up 90D", "# Big Down 90D",
    ],
    "Volume (Raw)": [
        "Volume",
    ],
    "SMA Values": [
        "SMA10", "SMA20", "SMA50", "SMA150", "SMA200",
    ],
    "MA Alignment (Raw)": [
        "MA10>MA20", "MA20>MA50", "MA50>MA150", "MA150>MA200",
    ],
    "Tech Metadata": [
        "Finalized",
    ],
}

# tech_indicators column name → display column name
TECH_COL_MAP: dict[str, str] = {
    "close":               "Close",
    "high_52w":            "52W High",
    "low_52w":             "52W Low",
    "pct_from_52w_high":   "From 52W High%",
    "pct_from_52w_low":    "From 52W Low%",
    "pos_52w_pct":         "52W Pos%",
    "ema9":                "EMA9",
    "ema21":               "EMA21",
    "ema50_e":             "EMA50",
    "ema200":              "EMA200",
    "sma50_slope_20d":     "SMA50 Slope 20D",
    "pct_from_sma200":     "From SMA200%",
    "rsi14":               "RSI14",
    "macd_line":           "MACD Line",
    "macd_signal":         "MACD Signal",
    "macd_hist":           "MACD Hist",
    "stoch_k":             "Stoch K",
    "stoch_d":             "Stoch D",
    "bb_upper":            "BB Upper",
    "bb_middle":           "BB Middle",
    "bb_lower":            "BB Lower",
    "bb_pct_b":            "BB %B",
    "atr14":               "ATR14",
    "atr_pct":             "ATR%",
    "adx14":               "ADX14",
    "plus_di":             "+DI",
    "minus_di":            "-DI",
    "realized_vol_20d":    "RVol 20D%",
    "realized_vol_60d":    "RVol 60D%",
    "max_drawdown_63d":    "Max DD 63D%",
    "max_drawdown_252d":   "Max DD 252D%",
    "obv":                 "OBV",
    "cmf20":               "CMF20",
    "ad_line":             "AD Line",
    "avg_dollar_vol_20d":  "Avg $Vol 20D",
    "avg_dollar_vol_50d":  "Avg $Vol 50D",
    "median_volume_50d":   "Med Vol 50D",
    "donchian_high_20":    "Don High 20",
    "donchian_low_20":     "Don Low 20",
    "donchian_high_55":    "Don High 55",
    "donchian_low_55":     "Don Low 55",
    "donchian_high_252":   "Don High 252",
    "donchian_low_252":    "Don Low 252",
    "pct_from_20d_high":   "From 20D High%",
    "pct_from_55d_high":   "From 55D High%",
    "pct_from_252d_high":  "From 252D High%",
    "breakout_55d_high":   "Breakout 55D",
    "breakout_3m_high":    "Breakout 3M",
    "up_days_3m":          "Up Days 3M",
    "down_days_3m":        "Down Days 3M",
    "up_down_ratio_3m":    "UD Ratio 3M",
    "max_win_streak_3m":   "Max Win Str 3M",
    "win_streaks_5p_3m":   "Win Str 5% 3M",
    "up_days_1y":          "Up Days 1Y",
    "down_days_1y":        "Down Days 1Y",
    "up_down_ratio_1y":    "UD Ratio 1Y",
    "max_win_streak_1y":   "Max Win Str 1Y",
    "win_streaks_5p_1y":   "Win Str 5% 1Y",
    "gap_rate_60d":        "Gap Rate 60D%",
    "max_gap_60d":         "Max Gap 60D%",
    "volume":              "Volume",
    "sma10":               "SMA10",
    "sma20":               "SMA20",
    "sma50":               "SMA50",
    "sma150":              "SMA150",
    "sma200":              "SMA200",
    "ma10_gt_ma20":        "MA10>MA20",
    "ma20_gt_ma50":        "MA20>MA50",
    "ma50_gt_ma150":       "MA50>MA150",
    "ma150_gt_ma200":      "MA150>MA200",
    "as_of_date":          "Tech Date",
    "is_finalized":        "Finalized",
    "daily_pct_change":    "Latest Px %",
}
# Reverse map: display name → tech_indicators field
TECH_DISPLAY_COL_MAP: dict[str, str] = {v: k for k, v in TECH_COL_MAP.items()}

# Columns from tech_indicators that are boolean (rendered as ✅/❌)
TECH_BOOL_COLS = {
    "Breakout 55D", "Breakout 3M",
    "MA10>MA20", "MA20>MA50", "MA50>MA150", "MA150>MA200",
    "Finalized",
}

# Default groups shown on load (indicator-derived groups only)
DEFAULT_VALUE_GROUPS = [
    "Price & Volume — Daily (T1)", "Price & Volume — Weekly (T2)",
    "MA Checks (T3)", "MA Values (T3)", "Big Moves 90d (T4)",
    "Quarterly (F1/F3)", "Annual (F2/F4)", "Valuation (F5/F6)", "Fundamentals",
]

ALL_VALUE_COLS = list(dict.fromkeys(c for cols in VALUE_COL_GROUPS.values() for c in cols))

# ── Column filter classification ───────────────────────────────────────────────
_COL_FILTER_SKIP = {
    "Ticker", "Sector", "Industry", "Next Earnings Date", "Mkt Cap ($B)",
}
_COL_FILTER_EMOJI = {
    "MA10>20", "MA20>50", "MA50>150", "MA150>200",
    "MA10>MA20", "MA20>MA50", "MA50>MA150", "MA150>MA200",
    "Breakout 55D", "Breakout 3M", "Finalized",
}
_COL_FILTER_DATES = {"Q End Date", "A End Date", "Tech Date"}
_COL_FILTER_OPS   = [">=", "<=", ">", "<", "="]
# All user-filterable value columns (ordered, deduplicated)
_FILTERABLE_COLS  = [c for c in ALL_VALUE_COLS if c not in _COL_FILTER_SKIP]

# Sub-indicator display labels (for column headers)
SUB_DISPLAY = {
    "T1_sub_3m_price":    "T1.1",
    "T1_sub_3m_vol":      "T1.2",
    "T1_sub_12m_price":   "T1.3",
    "T1_sub_12m_vol":     "T1.4",
    "T2_sub_3m_price":    "T2.1",
    "T2_sub_3m_vol":      "T2.2",
    "T2_sub_12m_price":   "T2.3",
    "T2_sub_12m_vol":     "T2.4",
    "T3_sub_ma10_20":     "T3.1",
    "T3_sub_ma20_50":     "T3.2",
    "T3_sub_ma50_150":    "T3.3",
    "T3_sub_ma150_200":   "T3.4",
    "T4_sub_has_big_up":  "T4.1",
    "T4_sub_no_big_down": "T4.2",
    "F1_sub_q_rev":       "F1.1",
    "F1_sub_q_eps":       "F1.2",
    "F2_sub_a_rev":       "F2.1",
    "F2_sub_a_eps":       "F2.2",
    "F3_sub_q_rev_yoy":   "F3.1",
    "F3_sub_q_eps_yoy":   "F3.2",
    "F4_sub_a_rev_yoy":   "F4.1",
    "F4_sub_a_eps_yoy":   "F4.2",
}

init_db()

# ─────────────────────────────────────────────────────────────────────────────
# Session state init
# ─────────────────────────────────────────────────────────────────────────────

def _ss(key, default):
    if key not in st.session_state:
        st.session_state[key] = default

_ss("last_analysis_dt",  None)
_ss("last_tickers",      [])
_ss("last_detail_map",   {})
_ss("scan_thread",       None)
_ss("scan_pause_event",  None)
_ss("scan_stop_event",   None)
_ss("scan_progress",     {})


# ─────────────────────────────────────────────────────────────────────────────
# Formatting helpers
# ─────────────────────────────────────────────────────────────────────────────

def _e(val: str) -> str:
    """DB value → emoji."""
    return EMOJI.get(str(val).upper(), "⚪️")

def _pct(v) -> str:
    try:   return f"{float(v):+.2f}%" if v is not None else "N/A"
    except: return "N/A"

def _num(v, d=2) -> str:
    try:   return f"{float(v):,.{d}f}" if v is not None else "N/A"
    except: return str(v) if v is not None else "N/A"

def _vol(v) -> str:
    try:   return f"{int(v):,}" if v is not None else "N/A"
    except: return "N/A"

def _millify(v) -> str:
    try:
        v = float(v)
        if abs(v) >= 1e12: return f"${v/1e12:.2f}T"
        if abs(v) >= 1e9:  return f"${v/1e9:.2f}B"
        if abs(v) >= 1e6:  return f"${v/1e6:.2f}M"
        return f"${v:.2f}"
    except: return "N/A"

def _date_range(lst) -> str:
    if not lst or not isinstance(lst, list):
        return "N/A"
    try:
        parts = [str(x) for x in lst if x is not None]
        return " → ".join(parts) if parts else "N/A"
    except: return "N/A"

def _price_list(lst) -> str:
    if not lst:
        return "N/A"
    try:
        return ", ".join(f"{float(x):.2f}" for x in lst if x is not None)
    except: return str(lst)

def _now_cst() -> str:
    return datetime.now(CST).strftime("%Y-%m-%d %H:%M:%S CST")

def _f(v) -> float | None:
    """Convert to float for native sorting; None if missing."""
    try:
        return float(v) if v is not None else None
    except Exception:
        return None

def _mkt_cap_b(v) -> float | None:
    """Market cap in billions (2 dp) for readable display without clicking."""
    f = _f(v)
    return round(f / 1e9, 2) if f is not None else None

def _parse_raw_info(f_db: dict) -> dict:
    rij = f_db.get("raw_info_json")
    if not rij:
        return {}
    try:
        return json.loads(rij)
    except Exception:
        return {}

def _extract_si(fund_map: dict) -> dict[str, tuple[str, str]]:
    """Extract {ticker: (sector, industry)} from a fund_map dict."""
    result: dict[str, tuple[str, str]] = {}
    for ticker, f_db in fund_map.items():
        raw_info = _parse_raw_info(f_db)
        result[ticker] = (
            raw_info.get("sector") or "N/A",
            raw_info.get("industry") or "N/A",
        )
    return result

def _extract_ne(fund_map: dict) -> dict[str, str]:
    """Extract {ticker: next_earnings_date_str} from a fund_map dict."""
    from datetime import timezone
    result: dict[str, str] = {}
    for ticker, f_db in fund_map.items():
        raw_info = _parse_raw_info(f_db)
        v = raw_info.get("earningsDate") or raw_info.get("earningsTimestamp")
        if v is None:
            result[ticker] = "N/A"
            continue
        if isinstance(v, list):
            v = v[0] if v else None
        if v is None:
            result[ticker] = "N/A"
            continue
        try:
            result[ticker] = datetime.fromtimestamp(float(v), tz=timezone.utc).strftime("%Y-%m-%d")
        except Exception:
            result[ticker] = "N/A"
    return result


def _build_value_record(ticker: str, detail: dict, row: dict, f_db: dict,
                        tech: dict | None = None) -> dict:
    """Build one value-table row. Numeric columns are native float for correct sorting."""
    t1 = detail.get("T1", {})
    t2 = detail.get("T2", {})
    t3 = detail.get("T3", {})
    t4 = detail.get("T4", {})
    f1 = detail.get("F1", {})
    f2 = detail.get("F2", {})
    f3 = detail.get("F3", {})
    f4 = detail.get("F4", {})
    f5 = detail.get("F5", {})
    f6 = detail.get("F6", {})
    sub3 = t3.get("sub_checks", {})
    tc = tech or {}
    raw_info = _parse_raw_info(f_db)

    def _bi(v):  # bool → icon
        if v is True:  return "✅"
        if v is False: return "❌"
        return "⚪️"

    def _big_count(json_str) -> int | None:
        if not json_str:
            return None
        try:
            events = json.loads(json_str)
            return len(events) if isinstance(events, list) else None
        except Exception:
            return None

    def _next_earnings(info: dict) -> str:
        v = info.get("earningsDate") or info.get("earningsTimestamp")
        if v is None:
            return "N/A"
        # yfinance returns a list of epoch timestamps or a single timestamp
        if isinstance(v, list):
            v = v[0] if v else None
        if v is None:
            return "N/A"
        try:
            from datetime import timezone
            return datetime.fromtimestamp(float(v), tz=timezone.utc).strftime("%Y-%m-%d")
        except Exception:
            return str(v)

    rec = {
        "Ticker":          ticker,
        # T1 — daily comparisons (float %)
        "3M Daily Px%":   _f(t1.get("3M Price Change %")),
        "3M Daily Vol%":  _f(t1.get("3M Volume Change %")),
        "12M Daily Px%":  _f(t1.get("12M Price Change %")),
        "12M Daily Vol%": _f(t1.get("12M Volume Change %")),
        # T2 — weekly comparisons (float %)
        "3M Wkly Px%":    _f(t2.get("3M Price Change %")),
        "3M Wkly Vol%":   _f(t2.get("3M Volume Change %")),
        "12M Wkly Px%":   _f(t2.get("12M Price Change %")),
        "12M Wkly Vol%":  _f(t2.get("12M Volume Change %")),
        # T3 — MA booleans
        "MA10>20":        _bi(sub3.get("MA10>MA20")),
        "MA20>50":        _bi(sub3.get("MA20>MA50")),
        "MA50>150":       _bi(sub3.get("MA50>MA150")),
        "MA150>200":      _bi(sub3.get("MA150>MA200")),
        # T3 — MA values (float)
        "MA10":           _f(t3.get("MA10")),
        "MA20":           _f(t3.get("MA20")),
        "MA50":           _f(t3.get("MA50")),
        "MA150":          _f(t3.get("MA150")),
        "MA200":          _f(t3.get("MA200")),
        # T4 — big move counts (int)
        "# Up≥10%":       t4.get("Big Up Days Count"),
        "# Dn≥10%":       t4.get("Big Down Days Count"),
        # F1/F3 — quarterly (float)
        "Q Rev":          _f(f1.get("Q Revenue") or f_db.get("q_revenue")),
        "Q EPS":          _f(f1.get("Q EPS") or f_db.get("q_eps")),
        "Q End Date":     f_db.get("q_end_date") or "N/A",
        "Q Rev YoY%":     _f(f3.get("Q Revenue YoY %") or f_db.get("q_rev_yoy")),
        "Q EPS YoY%":     _f(f3.get("Q EPS YoY %") or f_db.get("q_eps_yoy")),
        # F2/F4 — annual (float)
        "A Rev":          _f(f2.get("Annual Revenue") or f_db.get("a_revenue")),
        "A EPS":          _f(f2.get("Annual EPS") or f_db.get("a_eps")),
        "A End Date":     f_db.get("a_end_date") or "N/A",
        "A Rev YoY%":     _f(f4.get("Annual Revenue YoY %") or f_db.get("a_rev_yoy")),
        "A EPS YoY%":     _f(f4.get("Annual EPS YoY %") or f_db.get("a_eps_yoy")),
        # F5/F6 — valuation vs peers (float)
        "Fwd PE":         _f(f5.get("Ticker Fwd PE") or f_db.get("forward_pe")),
        "Fwd PE vs Med%": _f(f5.get("Ticker vs Median %")),
        "P/B":            _f(f6.get("Ticker P/B") or f_db.get("pb_ratio")),
        "P/B vs Med%":    _f(f6.get("Ticker vs Median %")),
        # Fundamentals
        "Mkt Cap ($B)":   _mkt_cap_b(row.get("market_cap") or f_db.get("market_cap")),
        "Sector":         raw_info.get("sector") or "N/A",
        "Industry":       raw_info.get("industry") or "N/A",
        # Next earnings
        "Next Earnings Date": _next_earnings(raw_info),
        # ── tech_indicators columns ───────────────────────────────────────────
        "Close":           _f(tc.get("close")),
        "52W High":        _f(tc.get("high_52w")),
        "52W Low":         _f(tc.get("low_52w")),
        "From 52W High%":  _f(tc.get("pct_from_52w_high")),
        "From 52W Low%":   _f(tc.get("pct_from_52w_low")),
        "52W Pos%":        _f(tc.get("pos_52w_pct")),
        "EMA9":            _f(tc.get("ema9")),
        "EMA21":           _f(tc.get("ema21")),
        "EMA50":           _f(tc.get("ema50_e")),
        "EMA200":          _f(tc.get("ema200")),
        "SMA50 Slope 20D": _f(tc.get("sma50_slope_20d")),
        "From SMA200%":    _f(tc.get("pct_from_sma200")),
        "RSI14":           _f(tc.get("rsi14")),
        "MACD Line":       _f(tc.get("macd_line")),
        "MACD Signal":     _f(tc.get("macd_signal")),
        "MACD Hist":       _f(tc.get("macd_hist")),
        "Stoch K":         _f(tc.get("stoch_k")),
        "Stoch D":         _f(tc.get("stoch_d")),
        "BB Upper":        _f(tc.get("bb_upper")),
        "BB Middle":       _f(tc.get("bb_middle")),
        "BB Lower":        _f(tc.get("bb_lower")),
        "BB %B":           _f(tc.get("bb_pct_b")),
        "ATR14":           _f(tc.get("atr14")),
        "ATR%":            _f(tc.get("atr_pct")),
        "ADX14":           _f(tc.get("adx14")),
        "+DI":             _f(tc.get("plus_di")),
        "-DI":             _f(tc.get("minus_di")),
        "RVol 20D%":       _f(tc.get("realized_vol_20d")),
        "RVol 60D%":       _f(tc.get("realized_vol_60d")),
        "Max DD 63D%":     _f(tc.get("max_drawdown_63d")),
        "Max DD 252D%":    _f(tc.get("max_drawdown_252d")),
        "OBV":             _f(tc.get("obv")),
        "CMF20":           _f(tc.get("cmf20")),
        "AD Line":         _f(tc.get("ad_line")),
        "Avg $Vol 20D":    _f(tc.get("avg_dollar_vol_20d")),
        "Avg $Vol 50D":    _f(tc.get("avg_dollar_vol_50d")),
        "Med Vol 50D":     _f(tc.get("median_volume_50d")),
        "Don High 20":     _f(tc.get("donchian_high_20")),
        "Don Low 20":      _f(tc.get("donchian_low_20")),
        "Don High 55":     _f(tc.get("donchian_high_55")),
        "Don Low 55":      _f(tc.get("donchian_low_55")),
        "Don High 252":    _f(tc.get("donchian_high_252")),
        "Don Low 252":     _f(tc.get("donchian_low_252")),
        "From 20D High%":  _f(tc.get("pct_from_20d_high")),
        "From 55D High%":  _f(tc.get("pct_from_55d_high")),
        "From 252D High%": _f(tc.get("pct_from_252d_high")),
        "Breakout 55D":    _bi(tc.get("breakout_55d_high")),
        "Breakout 3M":     _bi(tc.get("breakout_3m_high")),
        "Up Days 3M":      tc.get("up_days_3m"),
        "Down Days 3M":    tc.get("down_days_3m"),
        "UD Ratio 3M":     _f(tc.get("up_down_ratio_3m")),
        "Max Win Str 3M":  tc.get("max_win_streak_3m"),
        "Win Str 5% 3M":   tc.get("win_streaks_5p_3m"),
        "Up Days 1Y":      tc.get("up_days_1y"),
        "Down Days 1Y":    tc.get("down_days_1y"),
        "UD Ratio 1Y":     _f(tc.get("up_down_ratio_1y")),
        "Max Win Str 1Y":  tc.get("max_win_streak_1y"),
        "Win Str 5% 1Y":   tc.get("win_streaks_5p_1y"),
        "Gap Rate 60D%":   _f(tc.get("gap_rate_60d")),
        "Max Gap 60D%":    _f(tc.get("max_gap_60d")),
        "# Big Up 90D":    _big_count(tc.get("big_up_events_90d")),
        "# Big Down 90D":  _big_count(tc.get("big_down_events_90d")),
        # Additional tech_indicators columns
        "Volume":          tc.get("volume"),
        "SMA10":           _f(tc.get("sma10")),
        "SMA20":           _f(tc.get("sma20")),
        "SMA50":           _f(tc.get("sma50")),
        "SMA150":          _f(tc.get("sma150")),
        "SMA200":          _f(tc.get("sma200")),
        "MA10>MA20":       _bi(tc.get("ma10_gt_ma20")),
        "MA20>MA50":       _bi(tc.get("ma20_gt_ma50")),
        "MA50>MA150":      _bi(tc.get("ma50_gt_ma150")),
        "MA150>MA200":     _bi(tc.get("ma150_gt_ma200")),
        "Tech Date":       str(tc.get("as_of_date") or "N/A"),
        "Finalized":       _bi(tc.get("is_finalized")),
        "Latest Px %":     _f(tc.get("daily_pct_change")),
    }
    return rec


# ─────────────────────────────────────────────────────────────────────────────
# Tickers list
# ─────────────────────────────────────────────────────────────────────────────

def load_ticker_list() -> list[str]:
    if TICKERS_FILE.exists():
        return [t.strip() for t in TICKERS_FILE.read_text().split(",") if t.strip()]
    return []


# ─────────────────────────────────────────────────────────────────────────────
# Scan thread
# ─────────────────────────────────────────────────────────────────────────────

# Batch scan settings
SCAN_BATCH_SIZE     = 100   # tickers per bulk technical download
SCAN_FUND_WORKERS   = 5     # parallel threads for fundamental + peer fetches
SCAN_BATCH_COOLDOWN = 10    # seconds to rest between batches

# Mullvad VPN rotation — countries cycled through between batches
VPN_COUNTRIES = ["us", "nl", "de", "se", "ch", "gb", "ca", "fr"]


def _run_one(ticker: str, daily_date, weekly_date, fetch_peers: bool = True) -> tuple[dict, dict, dict]:
    """Run full analysis for one ticker (used in manual/single-ticker mode)."""
    tech      = fetch_technical(ticker, daily_date, weekly_date)
    if "error" in tech: tech = {}
    fund      = fetch_fundamental(ticker)
    if "error" in fund: fund = {}
    peer_data = get_peer_valuations(ticker, skip_peers=not fetch_peers)
    return tech, fund, peer_data


def _run_fund_and_peers(ticker: str, fetch_peers: bool = True) -> tuple[dict, dict]:
    """Fetch fundamentals + peer valuations for one ticker (runs in thread pool).

    Peer valuations are served from DuckDB peer_cache when fresh (< 7 days),
    avoiding redundant API calls across scans.
    """
    fund = fetch_fundamental(ticker, skip_normalize=True)
    if "error" in fund:
        fund = {}

    if not fetch_peers:
        empty_peers = {
            "peers": [], "peer_forward_pe_values": [],
            "peer_pb_values": [], "pe_median": None, "pb_median": None,
        }
        return fund, empty_peers

    # DB-first peer lookup
    cached = get_cached_peer_valuations(ticker)
    if cached is not None:
        return fund, cached

    time.sleep(0.5)   # brief gap between fund and peer calls
    peer_data = get_peer_valuations(ticker, skip_peers=False)

    # Persist to DB peer cache
    save_peer_valuations(ticker, peer_data)

    return fund, peer_data


def scan_thread_func(tickers, analysis_dt, daily_date, weekly_date,
                     pause_event, stop_event, progress,
                     fetch_peers: bool = True, vpn_rotate: bool = False):
    clear_peer_cache()   # fresh cache for each scan run
    total = len(tickers)
    progress.update({"total": total, "done": 0, "current": "", "finished": False, "error": None})

    done = 0
    for batch_start in range(0, total, SCAN_BATCH_SIZE):
        if stop_event.is_set():
            break

        # Pause loop
        while pause_event.is_set():
            progress["paused"] = True
            time.sleep(0.3)
            if stop_event.is_set():
                break
        progress["paused"] = False

        batch = tickers[batch_start: batch_start + SCAN_BATCH_SIZE]
        batch_num = batch_start // SCAN_BATCH_SIZE + 1
        progress["current"] = f"Batch {batch_num}: {batch[0]}…{batch[-1]} (bulk download)"

        # ── Step 1: one bulk download for the whole batch ─────────────────────
        # Use V2 fetcher (stores extended indicators to DuckDB + returns legacy dicts)
        # Fall back to old fetcher only when date cutoffs are specified (backtesting mode)
        if daily_date or weekly_date:
            bulk_tech = fetch_technical_bulk(batch, daily_date, weekly_date)
        else:
            bulk_tech = fetch_technical_bulk_v2(batch, log=lambda m: None)

        # ── Step 2: parallel fund + peer fetches ──────────────────────────────
        progress["current"] = f"Batch {batch_num}: {batch[0]}…{batch[-1]} (fund + peers)"
        consecutive_failures   = 0
        vpn_switched_this_batch = False
        rate_limited_tickers:  list[str] = []

        def _do_vpn_switch(reason: str):
            nonlocal consecutive_failures, vpn_switched_this_batch
            country = VPN_COUNTRIES[batch_num % len(VPN_COUNTRIES)]
            progress["current"] = f"{reason} — switching VPN to {country.upper()}…"
            vpn_switcher.switch_server(
                country,
                log=lambda msg: progress.update({"current": msg}),
            )
            consecutive_failures   = 0
            vpn_switched_this_batch = True

        # Skip tickers the bulk download already flagged as delisted / no data
        def _is_delisted(t):
            err = bulk_tech.get(t, {}).get("error", "")
            return "No price data" in err or "delisted" in err.lower() or "No bulk data" in err

        valid_batch   = [t for t in batch if not _is_delisted(t)]
        skipped_count = len(batch) - len(valid_batch)
        if skipped_count:
            done += skipped_count
            progress["done"] = done

        with concurrent.futures.ThreadPoolExecutor(max_workers=SCAN_FUND_WORKERS) as pool:
            future_map = {pool.submit(_run_fund_and_peers, t, fetch_peers): t for t in valid_batch}
            for future in concurrent.futures.as_completed(future_map):
                if stop_event.is_set():
                    break
                t = future_map[future]
                try:
                    fund, peer_data = future.result()
                    if fund.get("rate_limited"):
                        consecutive_failures += 1
                        rate_limited_tickers.append(t)
                    else:
                        consecutive_failures = 0
                        tech = bulk_tech.get(t, {"error": "Not in bulk data"})
                        indicators = evaluate_all(t, tech, fund, peer_data)
                        save_results(t, indicators, analysis_dt, fund.get("market_cap"))
                    # Reactive VPN switch: 3 consecutive auth blocks, once per batch
                    if vpn_rotate and consecutive_failures >= 3 and not vpn_switched_this_batch:
                        _do_vpn_switch("Persistent auth block detected")
                except Exception:
                    pass
                done += 1
                progress["done"] = done

        # ── Step 2b: re-queue rate-limited tickers after VPN switch ───────────
        if rate_limited_tickers and vpn_switched_this_batch and not stop_event.is_set():
            progress["current"] = (
                f"Re-processing {len(rate_limited_tickers)} rate-limited tickers on new IP…"
            )
            # Use 2 workers — gentle on the freshly switched IP
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as retry_pool:
                retry_map = {
                    retry_pool.submit(_run_fund_and_peers, t, fetch_peers): t
                    for t in rate_limited_tickers
                }
                for future in concurrent.futures.as_completed(retry_map):
                    if stop_event.is_set():
                        break
                    t = retry_map[future]
                    try:
                        fund, peer_data = future.result()
                        if not fund.get("rate_limited"):
                            tech = bulk_tech.get(t, {"error": "Not in bulk data"})
                            indicators = evaluate_all(t, tech, fund, peer_data)
                            save_results(t, indicators, analysis_dt, fund.get("market_cap"))
                    except Exception:
                        pass

        # ── Step 3: cooldown + optional proactive VPN switch ──────────────────
        if batch_start + SCAN_BATCH_SIZE < total and not stop_event.is_set():
            progress["current"] = f"Cooldown {SCAN_BATCH_COOLDOWN}s before next batch…"
            for _ in range(SCAN_BATCH_COOLDOWN * 10):
                if stop_event.is_set():
                    break
                time.sleep(0.1)

            # Proactive switch — skip if reactive already fired this batch
            if vpn_rotate and not stop_event.is_set() and not vpn_switched_this_batch:
                _do_vpn_switch("Batch complete")

    progress["finished"] = True
    progress["current"]  = ""


# ─────────────────────────────────────────────────────────────────────────────
# Summary DataFrame builder
# ─────────────────────────────────────────────────────────────────────────────

def build_summary_df(rows: list[dict],
                     show_sub: bool = False,
                     selected_inds: list[str] | None = None,
                     include_datetime: bool = False,
                     si_map: dict | None = None,
                     ne_map: dict | None = None) -> pd.DataFrame:
    """
    Build the indicator summary DataFrame.
    si_map: {ticker: (sector, industry)}
    ne_map: {ticker: next_earnings_date_str}
    """
    if not rows:
        return pd.DataFrame()

    inds_to_show = selected_inds if selected_inds else MAIN_IND_COLS
    si = si_map or {}
    ne = ne_map or {}

    records = []
    for r in rows:
        rec: dict = {}
        if include_datetime:
            rec["Datetime"] = r.get("analysis_datetime", "")
        rec["Ticker"] = r.get("ticker", "")

        for ind in inds_to_show:
            rec[ind] = _e(r.get(ind, "NA"))

        if show_sub:
            for ind in inds_to_show:
                for sc in SUB_COLS.get(ind, []):
                    rec[SUB_DISPLAY.get(sc, sc)] = _e(r.get(sc, "NA"))

        rec["Comments"] = r.get("comments") or ""

        # Rightmost: Mkt Cap, Sector, Industry, Next Earnings
        ticker = r.get("ticker", "")
        rec["Mkt Cap ($B)"] = _mkt_cap_b(r.get("market_cap"))
        sector, industry = si.get(ticker, ("N/A", "N/A"))
        rec["Sector"]         = sector
        rec["Industry"]       = industry
        rec["Next Earnings"]  = ne.get(ticker, "N/A")

        records.append(rec)

    return pd.DataFrame(records)


# ─────────────────────────────────────────────────────────────────────────────
# Save edits from data_editor
# ─────────────────────────────────────────────────────────────────────────────

def save_edits(original_rows: list[dict], edited_df: pd.DataFrame,
               include_datetime: bool = False):
    """Compare edited_df to original_rows and persist any changes."""
    orig_lookup: dict = {}
    for r in original_rows:
        key = (r["analysis_datetime"], r["ticker"])
        orig_lookup[key] = r

    # Build reverse col name map: display → DB col
    disp_to_db = {v: k for k, v in SUB_DISPLAY.items()}
    disp_to_db.update({ind: ind for ind in MAIN_IND_COLS})
    disp_to_db["Comments"] = "comments"

    for _, row in edited_df.iterrows():
        ticker = row.get("Ticker", "")
        if include_datetime:
            analysis_dt = row.get("Datetime", "")
        else:
            # Find the analysis_dt from original rows matching this ticker
            matches = [r for r in original_rows if r["ticker"] == ticker]
            if not matches:
                continue
            analysis_dt = matches[0]["analysis_datetime"]

        key = (analysis_dt, ticker)
        orig = orig_lookup.get(key, {})

        for col in row.index:
            if col in ("Ticker", "Datetime"):
                continue
            db_col = disp_to_db.get(col)
            if not db_col:
                continue

            new_val = row[col]
            # Convert emoji back to DB string for indicator cols
            if col in MAIN_IND_COLS or col in SUB_DISPLAY.values():
                new_val = EMOJI_TO_DB.get(str(new_val), str(new_val))

            update_field(analysis_dt, ticker, db_col, str(new_val) if new_val is not None else "")


# ─────────────────────────────────────────────────────────────────────────────
# Column config for data_editor
# ─────────────────────────────────────────────────────────────────────────────

def make_column_config(df: pd.DataFrame) -> dict:
    config = {}
    for col in df.columns:
        if col in ("Ticker", "Datetime"):
            config[col] = st.column_config.TextColumn(col, disabled=True)
        elif col in MAIN_IND_COLS or col in SUB_DISPLAY.values():
            config[col] = st.column_config.SelectboxColumn(
                col, options=EMOJI_OPTIONS, required=True
            )
        elif col == "Comments":
            config[col] = st.column_config.TextColumn("Comments", width="large")
        elif col == "Mkt Cap ($B)":
            config[col] = st.column_config.NumberColumn(
                "Mkt Cap ($B)", format="%.2f", disabled=True
            )
        elif col in ("Sector", "Industry", "Next Earnings"):
            config[col] = st.column_config.TextColumn(col, disabled=True)
    return config


# ─────────────────────────────────────────────────────────────────────────────
# Detail section renderer
# ─────────────────────────────────────────────────────────────────────────────

def _sub_emoji(sub_checks: dict, key: str) -> str:
    v = sub_checks.get(key)
    if v is True:  return "✅"
    if v is False: return "❌"
    return "⚪️"

def _grade_to_emoji(grade: str) -> str:
    return EMOJI.get(grade.upper(), "⚪️")


def render_detail_t1(ticker: str, ind_result: dict, detail: dict):
    grade = ind_result.get("pass", "NA")
    sub   = detail.get("sub_checks", {})

    with st.expander(f"{_grade_to_emoji(grade)} T1: Daily Price & Volume — {ticker}", expanded=True):
        sub_items = [
            ("3M Daily Price Up",   sub.get("3M Daily Price Up")),
            ("3M Daily Volume Up",  sub.get("3M Daily Volume Up")),
            ("12M Daily Price Up",  sub.get("12M Daily Price Up")),
            ("12M Daily Volume Up", sub.get("12M Daily Volume Up")),
        ]
        for label, val in sub_items:
            e = "✅" if val is True else ("❌" if val is False else "⚪️")
            st.markdown(f"  {e} **{label}**")

        st.markdown("---")
        for period, key in [("3M", "3M"), ("12M", "12M")]:
            st.markdown(f"**{period} Comparison**")
            rows = [
                ("Latest Date Range",  _date_range(detail.get(f"{period} Latest Date Range"))),
                ("Prior Date Range",   _date_range(detail.get(f"{period} Prior Date Range"))),
                ("Latest Prices",      _price_list(detail.get(f"{period} Latest Prices"))),
                ("Prior Prices",       _price_list(detail.get(f"{period} Prior Prices"))),
                ("Latest Price Avg",   _num(detail.get(f"{period} Latest Price Avg"))),
                ("Prior Price Avg",    _num(detail.get(f"{period} Prior Price Avg"))),
                ("Price Change %",     _pct(detail.get(f"{period} Price Change %"))),
                ("Latest Volume Avg",  _vol(detail.get(f"{period} Latest Volume Avg"))),
                ("Prior Volume Avg",   _vol(detail.get(f"{period} Prior Volume Avg"))),
                ("Volume Change %",    _pct(detail.get(f"{period} Volume Change %"))),
            ]
            st.dataframe(pd.DataFrame(rows, columns=["Field", "Value"]),
                         width="stretch", hide_index=True)


def render_detail_t2(ticker: str, ind_result: dict, detail: dict):
    grade = ind_result.get("pass", "NA")
    sub   = detail.get("sub_checks", {})

    with st.expander(f"{_grade_to_emoji(grade)} T2: Weekly Price & Volume — {ticker}", expanded=True):
        sub_items = [
            ("3M Weekly Price Up",   sub.get("3M Weekly Price Up")),
            ("3M Weekly Volume Up",  sub.get("3M Weekly Volume Up")),
            ("12M Weekly Price Up",  sub.get("12M Weekly Price Up")),
            ("12M Weekly Volume Up", sub.get("12M Weekly Volume Up")),
        ]
        for label, val in sub_items:
            e = "✅" if val is True else ("❌" if val is False else "⚪️")
            st.markdown(f"  {e} **{label}**")

        st.markdown("---")
        for period, key in [("3M", "3M"), ("12M", "12M")]:
            st.markdown(f"**{period} Comparison**")
            rows = [
                ("Latest Date Range",  _date_range(detail.get(f"{period} Latest Date Range"))),
                ("Prior Date Range",   _date_range(detail.get(f"{period} Prior Date Range"))),
                ("Latest Prices",      _price_list(detail.get(f"{period} Latest Prices"))),
                ("Prior Prices",       _price_list(detail.get(f"{period} Prior Prices"))),
                ("Latest Price Avg",   _num(detail.get(f"{period} Latest Price Avg"))),
                ("Prior Price Avg",    _num(detail.get(f"{period} Prior Price Avg"))),
                ("Price Change %",     _pct(detail.get(f"{period} Price Change %"))),
                ("Latest Volume Avg",  _vol(detail.get(f"{period} Latest Volume Avg"))),
                ("Prior Volume Avg",   _vol(detail.get(f"{period} Prior Volume Avg"))),
                ("Volume Change %",    _pct(detail.get(f"{period} Volume Change %"))),
            ]
            st.dataframe(pd.DataFrame(rows, columns=["Field", "Value"]),
                         width="stretch", hide_index=True)


def render_detail_t3(ticker: str, ind_result: dict, detail: dict):
    grade = ind_result.get("pass", "NA")
    sub   = detail.get("sub_checks", {})

    with st.expander(f"{_grade_to_emoji(grade)} T3: MA Alignment — {ticker}", expanded=True):
        sub_items = [
            ("MA10>MA20",   sub.get("MA10>MA20")),
            ("MA20>MA50",   sub.get("MA20>MA50")),
            ("MA50>MA150",  sub.get("MA50>MA150")),
            ("MA150>MA200", sub.get("MA150>MA200")),
        ]
        for label, val in sub_items:
            e = "✅" if val is True else ("❌" if val is False else "⚪️")
            st.markdown(f"  {e} **{label}**")
        st.markdown("---")
        rows = [
            ("MA10",  _num(detail.get("MA10"))),
            ("MA20",  _num(detail.get("MA20"))),
            ("MA50",  _num(detail.get("MA50"))),
            ("MA150", _num(detail.get("MA150"))),
            ("MA200", _num(detail.get("MA200"))),
        ]
        st.dataframe(pd.DataFrame(rows, columns=["Field", "Value"]),
                     width="stretch", hide_index=True)


def render_detail_t4(ticker: str, ind_result: dict, detail: dict):
    grade = ind_result.get("pass", "NA")
    sub   = detail.get("sub_checks", {})

    with st.expander(f"{_grade_to_emoji(grade)} T4: Big Moves ≥10% (90d) — {ticker}", expanded=True):
        up_ok   = sub.get("Big Up Days (≥+10%)")
        down_ok = sub.get("No Big Down Days (≥10% down)")
        st.markdown(f"  {'✅' if up_ok is True else ('❌' if up_ok is False else '⚪️')} **Has ≥1 day +10% up** (count: {detail.get('Big Up Days Count', 0)})")
        st.markdown(f"  {'✅' if down_ok is True else ('❌' if down_ok is False else '⚪️')} **No day −10% down** (count: {detail.get('Big Down Days Count', 0)})")

        up_evts = detail.get("Big Up Events") or []
        dn_evts = detail.get("Big Down Events") or []
        if up_evts:
            st.markdown("**⬆️ Big Up Events:**")
            rows = [[ev["date"], f"{ev['pct_change']:+.1f}%",
                     _vol(ev.get("volume")), _vol(ev.get("vol_30d_avg")),
                     "✅" if ev.get("vol_above_avg") else "❌"]
                    for ev in up_evts]
            st.dataframe(pd.DataFrame(rows, columns=["Date","Change%","Volume","30d Avg Vol","Vol>Avg"]),
                         width="stretch", hide_index=True)
        if dn_evts:
            st.markdown("**⬇️ Big Down Events:**")
            rows = [[ev["date"], f"{ev['pct_change']:+.1f}%",
                     _vol(ev.get("volume")), _vol(ev.get("vol_30d_avg")),
                     "✅" if ev.get("vol_above_avg") else "❌"]
                    for ev in dn_evts]
            st.dataframe(pd.DataFrame(rows, columns=["Date","Change%","Volume","30d Avg Vol","Vol>Avg"]),
                         width="stretch", hide_index=True)


def render_detail_f1(ticker: str, ind_result: dict, detail: dict):
    grade = ind_result.get("pass", "NA")
    sub   = detail.get("sub_checks", {})
    with st.expander(f"{_grade_to_emoji(grade)} F1: Latest Quarter Profitability — {ticker}", expanded=True):
        for label, key in [("Positive Q Revenue","Positive Q Revenue"),("Positive Q EPS","Positive Q EPS")]:
            v = sub.get(key)
            e = "✅" if v is True else ("❌" if v is False else "⚪️")
            st.markdown(f"  {e} **{label}**")
        st.markdown("---")
        rows = [("Q Revenue", _millify(detail.get("Q Revenue"))), ("Q EPS", _num(detail.get("Q EPS")))]
        st.dataframe(pd.DataFrame(rows, columns=["Field","Value"]), width="stretch", hide_index=True)


def render_detail_f2(ticker: str, ind_result: dict, detail: dict):
    grade = ind_result.get("pass", "NA")
    sub   = detail.get("sub_checks", {})
    with st.expander(f"{_grade_to_emoji(grade)} F2: Latest Year Profitability — {ticker}", expanded=True):
        for label, key in [("Positive Annual Revenue","Positive Annual Revenue"),("Positive Annual EPS","Positive Annual EPS")]:
            v = sub.get(key)
            e = "✅" if v is True else ("❌" if v is False else "⚪️")
            st.markdown(f"  {e} **{label}**")
        st.markdown("---")
        rows = [("Annual Revenue", _millify(detail.get("Annual Revenue"))), ("Annual EPS", _num(detail.get("Annual EPS")))]
        st.dataframe(pd.DataFrame(rows, columns=["Field","Value"]), width="stretch", hide_index=True)


def render_detail_f3(ticker: str, ind_result: dict, detail: dict):
    grade = ind_result.get("pass", "NA")
    sub   = detail.get("sub_checks", {})
    with st.expander(f"{_grade_to_emoji(grade)} F3: Quarter YoY Growth — {ticker}", expanded=True):
        for label, key in [("Q Revenue YoY > +10%","Q Revenue YoY > +10%"),("Q EPS YoY > +30%","Q EPS YoY > +30%")]:
            v = sub.get(key)
            e = "✅" if v is True else ("❌" if v is False else "⚪️")
            st.markdown(f"  {e} **{label}**")
        st.markdown("---")
        rows = [
            ("Q Revenue YoY %",  _pct(detail.get("Q Revenue YoY %"))),
            ("Q EPS YoY %",      _pct(detail.get("Q EPS YoY %"))),
            ("Quarter End Date",  detail.get("Quarter End Date") or "N/A"),
            ("Q Revenue Source",  detail.get("Q Revenue Source") or "N/A"),
            ("Q EPS Source",      detail.get("Q EPS Source") or "N/A"),
            ("Threshold",         "Revenue > +10%  |  EPS > +30%"),
        ]
        st.dataframe(pd.DataFrame(rows, columns=["Field","Value"]), width="stretch", hide_index=True)


def render_detail_f4(ticker: str, ind_result: dict, detail: dict):
    grade = ind_result.get("pass", "NA")
    sub   = detail.get("sub_checks", {})
    with st.expander(f"{_grade_to_emoji(grade)} F4: Annual YoY Growth — {ticker}", expanded=True):
        for label, key in [("Annual Revenue YoY > +10%","Annual Revenue YoY > +10%"),("Annual EPS YoY > +30%","Annual EPS YoY > +30%")]:
            v = sub.get(key)
            e = "✅" if v is True else ("❌" if v is False else "⚪️")
            st.markdown(f"  {e} **{label}**")
        st.markdown("---")
        rows = [
            ("Annual Revenue YoY %", _pct(detail.get("Annual Revenue YoY %"))),
            ("Annual EPS YoY %",     _pct(detail.get("Annual EPS YoY %"))),
            ("Fiscal Year End Date", detail.get("Fiscal Year End Date") or "N/A"),
            ("Annual Revenue Source",detail.get("Annual Revenue Source") or "N/A"),
            ("Annual EPS Source",    detail.get("Annual EPS Source") or "N/A"),
            ("Threshold",            "Revenue > +10%  |  EPS > +30%"),
        ]
        st.dataframe(pd.DataFrame(rows, columns=["Field","Value"]), width="stretch", hide_index=True)


def render_detail_f5(ticker: str, ind_result: dict, detail: dict):
    grade = ind_result.get("pass", "NA")
    with st.expander(f"{_grade_to_emoji(grade)} F5: Forward PE vs Peers — {ticker}", expanded=True):
        pct_diff = detail.get("Ticker vs Median %")
        rows = [
            ("Ticker Fwd PE",       _num(detail.get("Ticker Fwd PE"))),
            ("Peer Median Fwd PE",  _num(detail.get("Peer Median Fwd PE"))),
            ("Ticker vs Median %",  _pct(pct_diff)),
            ("Peers with PE Data",  str(detail.get("Peers with PE Data", 0))),
            ("Peer Tickers",        ", ".join(detail.get("Peer Tickers") or [])),
            ("Peer Fwd PE Values",  str(detail.get("Peer Fwd PE Values") or [])),
        ]
        st.dataframe(pd.DataFrame(rows, columns=["Field","Value"]), width="stretch", hide_index=True)


def render_detail_f6(ticker: str, ind_result: dict, detail: dict):
    grade = ind_result.get("pass", "NA")
    with st.expander(f"{_grade_to_emoji(grade)} F6: P/B Ratio vs Peers — {ticker}", expanded=True):
        pct_diff = detail.get("Ticker vs Median %")
        rows = [
            ("Ticker P/B",          _num(detail.get("Ticker P/B"))),
            ("Peer Median P/B",     _num(detail.get("Peer Median P/B"))),
            ("Ticker vs Median %",  _pct(pct_diff)),
            ("Peers with P/B Data", str(detail.get("Peers with P/B Data", 0))),
            ("Peer Tickers",        ", ".join(detail.get("Peer Tickers") or [])),
            ("Peer P/B Values",     str(detail.get("Peer P/B Values") or [])),
        ]
        st.dataframe(pd.DataFrame(rows, columns=["Field","Value"]), width="stretch", hide_index=True)


DETAIL_RENDERERS = {
    "T1": render_detail_t1, "T2": render_detail_t2,
    "T3": render_detail_t3, "T4": render_detail_t4,
    "F1": render_detail_f1, "F2": render_detail_f2,
    "F3": render_detail_f3, "F4": render_detail_f4,
    "F5": render_detail_f5, "F6": render_detail_f6,
}


def render_detail_for_tickers(tickers: list[str], detail_map: dict,
                               indicators_live: dict | None = None,
                               state_key: str = "default",
                               dt_label: str | None = None):
    """Render collapsible detail tables for each ticker, with per-ticker collapse toggle."""
    collapsed_key = f"collapsed_tickers_{state_key}"
    if collapsed_key not in st.session_state:
        st.session_state[collapsed_key] = set()

    for ticker in sorted(tickers):
        is_collapsed = ticker in st.session_state[collapsed_key]

        hcol, bcol = st.columns([10, 1])
        with hcol:
            arrow = "▶" if is_collapsed else "▼"
            header = f"### {arrow} 📋 {ticker}"
            if dt_label:
                header += f"  —  {dt_label}"
            st.markdown(header)
        with bcol:
            btn_label = "Expand" if is_collapsed else "Collapse"
            if st.button(btn_label, key=f"ticker_toggle_{state_key}_{ticker}"):
                if is_collapsed:
                    st.session_state[collapsed_key].discard(ticker)
                else:
                    st.session_state[collapsed_key].add(ticker)
                st.rerun()

        if is_collapsed:
            continue

        ticker_detail = detail_map.get(ticker, {})
        ticker_inds   = indicators_live.get(ticker, {}) if indicators_live else {}

        for ind_id, renderer in DETAIL_RENDERERS.items():
            detail    = ticker_detail.get(ind_id, {})
            ind_result = ticker_inds.get(ind_id, {"pass": "NA"}) if ticker_inds else {"pass": "NA"}

            # If we don't have live results, reconstruct pass from detail (best effort)
            if not ticker_inds and detail:
                sub = detail.get("sub_checks", {})
                if sub:
                    vals = list(sub.values())
                    if any(v is None for v in vals):
                        grade = "NA"
                    elif all(v is True for v in vals):
                        grade = "PASS"
                    elif all(v is False for v in vals):
                        grade = "FAIL"
                    else:
                        grade = "PARTIAL"
                    ind_result = {"pass": grade}
                elif "Result" in detail:
                    # F5/F6 store grade directly (no sub_checks)
                    ind_result = {"pass": detail["Result"]}

            renderer(ticker, ind_result, detail)


# ─────────────────────────────────────────────────────────────────────────────
# Scan progress UI
# ─────────────────────────────────────────────────────────────────────────────

def render_scan_progress():
    prog = st.session_state.scan_progress
    if not prog:
        return

    total     = prog.get("total", 0)
    done      = prog.get("done", 0)
    current   = prog.get("current", "")
    finished  = prog.get("finished", False)
    paused    = prog.get("paused", False)

    if finished:
        st.success(f"✅ Scan complete — {done} tickers processed")
        return

    frac = done / total if total else 0
    st.progress(frac, text=f"{'⏸ PAUSED — ' if paused else ''}Processed {done}/{total} tickers"
                           + (f" | Current: {current}" if current and not paused else ""))

    col1, col2 = st.columns(2)
    with col1:
        if paused:
            if st.button("▶ Resume", key="resume_scan"):
                st.session_state.scan_pause_event.clear()
        else:
            if st.button("⏸ Pause", key="pause_scan"):
                st.session_state.scan_pause_event.set()
    with col2:
        if st.button("⏹ Stop Scan", key="stop_scan"):
            st.session_state.scan_stop_event.set()


@st.fragment(run_every=2)
def _scan_progress_autorefresh():
    """Auto-refreshes progress every 2 s without triggering a full-page rerun."""
    prog = st.session_state.scan_progress
    if prog:
        render_scan_progress()




# ── Per-tab filter session-state key mapping ──────────────────────────────────
_TAB_FILTER_KEYS: dict[str, dict[str, str]] = {
    "latest": {
        "f_ticker_text": "latest_f_ticker_name",
        "f_sector":      "latest_f_sector",
        "f_industry":    "latest_f_industry",
        "mc_lo":         "latest_mc_lo",
        "mc_hi":         "latest_mc_hi",
        "show_sub":      "latest_show_sub",
    },
    "history": {
        "f_ticker_multi": "hist_f_tick",
        "f_dt":           "hist_f_dt",
        "f_sector":       "hist_f_sector",
        "f_industry":     "hist_f_industry",
        "mc_lo":          "hist_mc_lo",
        "mc_hi":          "hist_mc_hi",
        "show_sub":       "all_queries_show_sub",
        "only_latest":    "hist_only_latest",
    },
}

_TAB_FILTER_DEFAULTS: dict[str, dict[str, object]] = {
    "latest": {
        "f_ticker_text": "",
        "f_sector":      [],
        "f_industry":    [],
        "mc_lo":         None,
        "mc_hi":         None,
        "show_sub":      False,
    },
    "history": {
        "f_ticker_multi": [],
        "f_dt":           [],
        "f_sector":       [],
        "f_industry":     [],
        "mc_lo":          None,
        "mc_hi":          None,
        "show_sub":       False,
        "only_latest":    True,
    },
}


def _actually_clear_filter_keys(tab_key: str) -> None:
    """Reset all filter session state keys for tab_key. Call before widgets render."""
    defaults = _TAB_FILTER_DEFAULTS.get(tab_key, {})
    mapping  = _TAB_FILTER_KEYS.get(tab_key, {})
    for logical_key, ss_key in mapping.items():
        st.session_state[ss_key] = defaults.get(logical_key)
    st.session_state[f"filt_inds_{tab_key}"] = []
    for ind in MAIN_IND_COLS:
        k = f"filt_vals_{tab_key}_{ind}"
        if k in st.session_state:
            del st.session_state[k]
    # Clear column filter keys
    st.session_state[f"col_filt_cols_{tab_key}"] = []
    stale = [k for k in st.session_state
             if k.startswith(f"col_filt_op_{tab_key}_")
             or k.startswith(f"col_filt_numval_{tab_key}_")
             or k.startswith(f"col_filt_catvals_{tab_key}_")]
    for k in stale:
        del st.session_state[k]


def _actually_apply_filter_group(tab_key: str, group: dict) -> None:
    """Apply a saved filter group dict to session state. Call before widgets render."""
    mapping = _TAB_FILTER_KEYS.get(tab_key, {})
    for logical_key, ss_key in mapping.items():
        if logical_key in group:
            st.session_state[ss_key] = group[logical_key]
    st.session_state[f"filt_inds_{tab_key}"] = group.get("selected_inds", [])
    for ind, vals in group.get("ind_vals", {}).items():
        st.session_state[f"filt_vals_{tab_key}_{ind}"] = vals
    # Column filter
    st.session_state[f"col_filt_cols_{tab_key}"] = group.get("col_filt_cols", [])
    for col in group.get("col_filt_cols", []):
        if col in _COL_FILTER_EMOJI:
            st.session_state[f"col_filt_catvals_{tab_key}_{col}"] = group.get(f"_cfcatvals_{col}", [])
        else:
            st.session_state[f"col_filt_op_{tab_key}_{col}"]     = group.get(f"_cfop_{col}", ">=")
            st.session_state[f"col_filt_numval_{tab_key}_{col}"] = group.get(f"_cfval_{col}")


def _queue_filter_clear(tab_key: str) -> None:
    """Queue a filter clear to be applied before next widget render."""
    st.session_state[f"_pending_filt_clear_{tab_key}"] = True


def _queue_filter_group(tab_key: str, group: dict) -> None:
    """Queue a filter group load to be applied before next widget render."""
    st.session_state[f"_pending_filt_group_{tab_key}"] = group


def _process_pending_ops() -> None:
    """Apply any pending filter/column ops. Must be called before any widgets render."""
    for tab_key in list(_TAB_FILTER_KEYS.keys()):
        # Filter pending ops
        clear_key = f"_pending_filt_clear_{tab_key}"
        group_key = f"_pending_filt_group_{tab_key}"
        if st.session_state.pop(clear_key, False):
            _actually_clear_filter_keys(tab_key)
        if group_key in st.session_state:
            _actually_apply_filter_group(tab_key, st.session_state.pop(group_key))
        # Column group pending ops
        col_cols_key = f"_pending_col_cols_{tab_key}"
        if col_cols_key in st.session_state:
            st.session_state[f"val_cols_{tab_key}"] = st.session_state.pop(col_cols_key)


def _snapshot_filter_group(tab_key: str) -> dict:
    """Snapshot current filter state into a serialisable dict."""
    mapping = _TAB_FILTER_KEYS.get(tab_key, {})
    group: dict = {}
    for logical_key, ss_key in mapping.items():
        group[logical_key] = st.session_state.get(ss_key)
    group["selected_inds"] = list(st.session_state.get(f"filt_inds_{tab_key}", []))
    ind_vals: dict[str, list] = {}
    for ind in group["selected_inds"]:
        k = f"filt_vals_{tab_key}_{ind}"
        ind_vals[ind] = st.session_state.get(k, ["PASS"])
    group["ind_vals"] = ind_vals
    # Column filter snapshot
    col_filt_cols = list(st.session_state.get(f"col_filt_cols_{tab_key}", []))
    group["col_filt_cols"] = col_filt_cols
    for col in col_filt_cols:
        if col in _COL_FILTER_EMOJI:
            group[f"_cfcatvals_{col}"] = st.session_state.get(f"col_filt_catvals_{tab_key}_{col}", [])
        else:
            group[f"_cfop_{col}"]  = st.session_state.get(f"col_filt_op_{tab_key}_{col}", ">=")
            group[f"_cfval_{col}"] = st.session_state.get(f"col_filt_numval_{tab_key}_{col}")
    return group


def render_indicator_filter(tab_key: str) -> tuple[dict[str, set[str]], dict]:
    """
    Renders per-indicator value filter UI inline.
    Returns (ind_filters, col_filter):
      ind_filters: {indicator_id: set_of_accepted_values} for active filters only.
      col_filter:  {col_name: {"type":..., ...}} for active column value filters.
    Includes clear/reset buttons, a filter group manager, and column value filter.
    """
    # ── Auto-load custom default group (once per session per tab) ─────────────
    _loaded_key = f"filt_group_loaded_{tab_key}"
    if _loaded_key not in st.session_state:
        prefs = _load_prefs()
        fd = prefs.get("filter_default", {}).get(tab_key)
        fg = prefs.get("filter_groups", {}).get(tab_key, {})
        st.session_state[_loaded_key] = True
        if fd and fd in fg:
            _queue_filter_group(tab_key, fg[fd])
            st.rerun()

    # ── Action buttons ────────────────────────────────────────────────────────
    bc1, bc2 = st.columns(2)
    with bc1:
        if st.button("Clear all filters", key=f"filt_clear_{tab_key}"):
            _queue_filter_clear(tab_key)
            st.rerun()
    with bc2:
        if st.button("Reset to default", key=f"filt_reset_{tab_key}"):
            _queue_filter_clear(tab_key)
            st.rerun()

    # ── Indicator filter ──────────────────────────────────────────────────────
    st.markdown("**Filter by Indicator Values**")
    selected_inds = st.multiselect(
        "Filter on indicators:",
        options=MAIN_IND_COLS,
        key=f"filt_inds_{tab_key}",
        label_visibility="collapsed",
        placeholder="Select indicators to filter by value…",
    )

    ind_filters: dict[str, set[str]] = {}
    if selected_inds:
        vcols = st.columns(min(len(selected_inds), 5))
        for i, ind in enumerate(selected_inds):
            with vcols[i % 5]:
                vals = st.multiselect(
                    f"{ind}:",
                    options=["PASS", "PARTIAL", "FAIL", "NA"],
                    default=["PASS"],
                    format_func=lambda v: f"{EMOJI.get(v, v)} {v}",
                    key=f"filt_vals_{tab_key}_{ind}",
                )
                if vals:
                    ind_filters[ind] = set(vals)

    # ── Column value filter ────────────────────────────────────────────────────
    st.markdown("**Filter by Column Values**")
    col_filt_selected = st.multiselect(
        "Filter on columns:",
        options=_FILTERABLE_COLS,
        key=f"col_filt_cols_{tab_key}",
        label_visibility="collapsed",
        placeholder="Select columns to filter by value…",
    )
    if col_filt_selected:
        for col in col_filt_selected:
            if col in _COL_FILTER_EMOJI:
                st.multiselect(
                    f"{col}",
                    options=["✅", "❌", "⚪️"],
                    key=f"col_filt_catvals_{tab_key}_{col}",
                )
            elif col in _COL_FILTER_DATES:
                dc1, dc2, dc3 = st.columns([2, 1, 3])
                with dc1:
                    st.markdown(f"**{col}**")
                with dc2:
                    st.selectbox(
                        f"op_{col}",
                        options=_COL_FILTER_OPS,
                        key=f"col_filt_op_{tab_key}_{col}",
                        label_visibility="collapsed",
                    )
                with dc3:
                    st.text_input(
                        f"val_{col}",
                        key=f"col_filt_numval_{tab_key}_{col}",
                        label_visibility="collapsed",
                        placeholder="YYYY-MM-DD",
                    )
            else:
                nc1, nc2, nc3 = st.columns([2, 1, 3])
                with nc1:
                    st.markdown(f"**{col}**")
                with nc2:
                    st.selectbox(
                        f"op_{col}",
                        options=_COL_FILTER_OPS,
                        key=f"col_filt_op_{tab_key}_{col}",
                        label_visibility="collapsed",
                    )
                with nc3:
                    st.number_input(
                        f"val_{col}",
                        key=f"col_filt_numval_{tab_key}_{col}",
                        label_visibility="collapsed",
                        value=None,
                        placeholder="numeric value",
                    )

    # Build col_filter dict from current widget states
    col_filter: dict = {}
    for col in st.session_state.get(f"col_filt_cols_{tab_key}", []):
        if col in _COL_FILTER_EMOJI:
            catvals = st.session_state.get(f"col_filt_catvals_{tab_key}_{col}", [])
            if catvals:
                col_filter[col] = {"type": "cat", "vals": set(catvals)}
        elif col in _COL_FILTER_DATES:
            op  = st.session_state.get(f"col_filt_op_{tab_key}_{col}", ">=")
            val = st.session_state.get(f"col_filt_numval_{tab_key}_{col}", "")
            if val:
                col_filter[col] = {"type": "date", "op": op, "val": str(val)}
        else:
            op  = st.session_state.get(f"col_filt_op_{tab_key}_{col}", ">=")
            val = st.session_state.get(f"col_filt_numval_{tab_key}_{col}")
            if val is not None:
                col_filter[col] = {"type": "num", "op": op, "val": float(val)}

    # ── Custom filter groups manager ──────────────────────────────────────────
    with st.expander("Manage filter groups"):
        prefs = _load_prefs()
        fg = prefs.setdefault("filter_groups", {}).setdefault(tab_key, {})
        fd = prefs.get("filter_default", {}).get(tab_key)
        group_names = list(fg.keys())

        if fd:
            st.caption(f"Default group: **{fd}**")

        # Save current filters as a named group
        sg1, sg2 = st.columns([3, 1])
        with sg1:
            save_name = st.text_input(
                "Save current filters as group name:",
                key=f"filt_save_name_{tab_key}",
                label_visibility="collapsed",
                placeholder="Group name…",
            )
        with sg2:
            if st.button("Save", key=f"filt_save_{tab_key}"):
                if save_name:
                    fg[save_name] = _snapshot_filter_group(tab_key)
                    _save_prefs(prefs)
                    st.success(f"Saved '{save_name}'")
                    st.rerun()
                else:
                    st.warning("Enter a group name first.")

        # Load / Set default / Delete existing groups
        if group_names:
            gg1, gg2, gg3, gg4 = st.columns([3, 1, 1, 1])
            with gg1:
                sel_group = st.selectbox(
                    "Group:", options=group_names,
                    key=f"filt_sel_group_{tab_key}",
                    label_visibility="collapsed",
                )
            with gg2:
                if st.button("Load", key=f"filt_load_{tab_key}"):
                    _queue_filter_group(tab_key, fg[sel_group])
                    st.rerun()
            with gg3:
                if st.button("Set default", key=f"filt_setdef_{tab_key}"):
                    prefs.setdefault("filter_default", {})[tab_key] = sel_group
                    _save_prefs(prefs)
                    st.success(f"'{sel_group}' set as default")
                    st.rerun()
            with gg4:
                if st.button("Delete", key=f"filt_del_{tab_key}"):
                    fg.pop(sel_group, None)
                    if prefs.get("filter_default", {}).get(tab_key) == sel_group:
                        prefs["filter_default"].pop(tab_key, None)
                    _save_prefs(prefs)
                    st.rerun()
        else:
            st.caption("No saved groups yet.")

    return ind_filters, col_filter


def apply_indicator_filter(rows: list[dict],
                           ind_filters: dict[str, set[str]]) -> list[dict]:
    if not ind_filters:
        return rows
    return [
        r for r in rows
        if all(str(r.get(ind, "NA")).upper() in vals
               for ind, vals in ind_filters.items())
    ]


def _col_filter_passes(rec: dict, col: str, spec: dict) -> bool:
    """Return True if a value-table record passes one column filter spec."""
    val = rec.get(col)
    if val is None:
        return False
    t = spec["type"]
    if t == "cat":
        return str(val) in spec["vals"]
    op, fv = spec["op"], spec["val"]
    if t == "date":
        sval = str(val)
        return (op == "=" and sval == fv) or (op == ">" and sval > fv) \
            or (op == "<" and sval < fv) or (op == ">=" and sval >= fv) \
            or (op == "<=" and sval <= fv)
    # numeric
    try:
        nval = float(val)
    except (TypeError, ValueError):
        return False
    return (op == "=" and nval == fv) or (op == ">" and nval > fv) \
        or (op == "<" and nval < fv) or (op == ">=" and nval >= fv) \
        or (op == "<=" and nval <= fv)


def apply_col_filter(tickers: list[str], col_filter: dict,
                     detail_map: dict, rows_by_ticker: dict,
                     fund_map: dict, tech_map: dict) -> list[str]:
    """Return subset of tickers whose value-table records pass all column filters."""
    if not col_filter:
        return tickers
    out = []
    for t in tickers:
        rec = _build_value_record(
            t,
            detail_map.get(t, {}),
            rows_by_ticker.get(t, {}),
            fund_map.get(t, {}),
            tech_map.get(t, {}),
        )
        if all(_col_filter_passes(rec, col, spec) for col, spec in col_filter.items()):
            out.append(t)
    return out


def _value_col_config(cols: list[str]) -> dict:
    """Build st.column_config for the value table based on column names."""
    cfg: dict = {}
    pct_suffix = {"%"}
    for col in cols:
        if col == "Ticker":
            cfg[col] = st.column_config.TextColumn(col, disabled=True)
        elif col in ("Sector", "Industry", "Q End Date", "A End Date",
                     "Next Earnings Date", "Tech Date",
                     "Breakout 55D", "Breakout 3M", "Finalized",
                     "MA10>20", "MA20>50", "MA50>150", "MA150>200",
                     "MA10>MA20", "MA20>MA50", "MA50>MA150", "MA150>MA200"):
            cfg[col] = st.column_config.TextColumn(col, disabled=True)
        elif col == "Mkt Cap ($B)":
            cfg[col] = st.column_config.NumberColumn(col, format="%.2f", disabled=True)
        elif col == "Volume":
            cfg[col] = st.column_config.NumberColumn(col, format="%d", disabled=True)
        elif col.endswith("%"):
            cfg[col] = st.column_config.NumberColumn(col, format="%.2f", disabled=True)
        else:
            cfg[col] = st.column_config.NumberColumn(col, format="%.4g", disabled=True)
    return cfg


def render_value_table(tickers: list[str], detail_map: dict,
                       rows_by_ticker: dict, fund_map: dict,
                       tab_key: str, tech_map: dict | None = None):
    """
    Render the Indicator Values Table with:
      - Group multiselect + individual column multiselect
      - Select all / Clear all / Reset to default buttons
      - Custom named column groups (saved to user_prefs.json)
      - Numeric columns as native floats for correct sorting
      - tech_indicators columns when tech_map is provided
    """
    tm = tech_map or {}

    # ── Load custom groups and build merged group dict ────────────────────────
    prefs = _load_prefs()
    custom_col_groups: dict[str, list[str]] = prefs.get("col_groups", {})
    # Build effective group dict: built-in + custom (prefixed with ⭐)
    effective_groups: dict[str, list[str]] = dict(VALUE_COL_GROUPS)
    for name, cols in custom_col_groups.items():
        effective_groups[f"⭐ {name}"] = cols
    all_groups = list(effective_groups.keys())

    st.markdown("### 📐 Indicator Values Table")

    # ── Auto-load custom default column group (once per session per tab) ─────
    _col_loaded_key = f"col_group_loaded_{tab_key}"
    if _col_loaded_key not in st.session_state:
        cd = prefs.get("col_default", {}).get(tab_key)
        if cd and cd in custom_col_groups:
            st.session_state[f"val_cols_{tab_key}"] = list(custom_col_groups[cd])
        st.session_state[_col_loaded_key] = True

    # all_cols: every column across all groups (options for individual selector)
    # Computed here so button handlers can reference it before widgets are rendered.
    all_cols = list(dict.fromkeys(c for g in all_groups for c in effective_groups.get(g, [])))

    # ── Select all / Clear all / Reset to default buttons ────────────────────
    # No st.rerun() — session state changes made before the multiselects below
    # are picked up immediately in the same render pass.
    vb1, vb2, vb3 = st.columns(3)
    with vb1:
        if st.button("Select all columns", key=f"val_sel_all_{tab_key}"):
            st.session_state[f"val_cols_{tab_key}"] = list(all_cols)
    with vb2:
        if st.button("Clear all columns", key=f"val_clear_{tab_key}"):
            st.session_state[f"val_groups_{tab_key}"] = []
            st.session_state[f"val_cols_{tab_key}"] = []
    with vb3:
        if st.button("Reset to default", key=f"val_reset_{tab_key}"):
            st.session_state[f"val_groups_{tab_key}"] = DEFAULT_VALUE_GROUPS
            st.session_state[f"val_cols_{tab_key}"] = []

    # ── Column group selector ────────────────────────────────────────────────
    sel_groups = st.multiselect(
        "Column groups:",
        options=all_groups,
        default=DEFAULT_VALUE_GROUPS,
        key=f"val_groups_{tab_key}",
    )

    # group_cols: columns from selected groups (fallback when no individual cols chosen)
    group_cols = list(dict.fromkeys(c for g in sel_groups for c in effective_groups.get(g, [])))
    # Filter stale val_cols to valid options (avoid Streamlit invalid-default error)
    cur_val_cols = st.session_state.get(f"val_cols_{tab_key}", [])
    if any(c not in all_cols for c in cur_val_cols):
        st.session_state[f"val_cols_{tab_key}"] = [c for c in cur_val_cols if c in all_cols]
    sel_cols = st.multiselect(
        "Individual columns (leave blank = all columns in selected groups):",
        options=all_cols,
        default=[],
        key=f"val_cols_{tab_key}",
    )

    # ── Custom column group manager ───────────────────────────────────────────
    with st.expander("Manage column groups"):
        cg_default = prefs.get("col_default", {}).get(tab_key)
        if cg_default:
            st.caption(f"Default group: **{cg_default}**")

        cg1, cg2 = st.columns([3, 1])
        with cg1:
            cg_save_name = st.text_input(
                "Save current column selection as group:",
                key=f"col_save_name_{tab_key}",
                label_visibility="collapsed",
                placeholder="Column group name…",
            )
        with cg2:
            if st.button("Save", key=f"col_save_{tab_key}"):
                if cg_save_name:
                    cols_to_save = sel_cols if sel_cols else group_cols
                    prefs.setdefault("col_groups", {})[cg_save_name] = cols_to_save
                    _save_prefs(prefs)
                    st.success(f"Saved '{cg_save_name}'")
                    st.rerun()
                else:
                    st.warning("Enter a group name first.")

        cg_names = list(custom_col_groups.keys())
        if cg_names:
            cc1, cc2, cc3, cc4 = st.columns([3, 1, 1, 1])
            with cc1:
                sel_cg = st.selectbox(
                    "Column group:", options=cg_names,
                    key=f"col_sel_group_{tab_key}",
                    label_visibility="collapsed",
                )
            with cc2:
                if st.button("Load", key=f"col_load_{tab_key}"):
                    st.session_state[f"_pending_col_cols_{tab_key}"] = list(custom_col_groups[sel_cg])
                    st.rerun()
            with cc3:
                if st.button("Set default", key=f"col_setdef_{tab_key}"):
                    prefs.setdefault("col_default", {})[tab_key] = sel_cg
                    _save_prefs(prefs)
                    st.success(f"'{sel_cg}' set as default")
                    st.rerun()
            with cc4:
                if st.button("Delete", key=f"col_del_{tab_key}"):
                    prefs.get("col_groups", {}).pop(sel_cg, None)
                    if prefs.get("col_default", {}).get(tab_key) == sel_cg:
                        prefs["col_default"].pop(tab_key, None)
                    _save_prefs(prefs)
                    st.rerun()
        else:
            st.caption("No saved column groups yet.")

    _fixed_right = {"Mkt Cap ($B)", "Sector", "Industry", "Next Earnings Date"}
    data_cols = [c for c in (sel_cols if sel_cols else group_cols) if c not in _fixed_right]
    # Order: Ticker | data columns | Mkt Cap ($B) | Sector | Industry | Next Earnings Date
    show_cols = ["Ticker"] + data_cols + ["Mkt Cap ($B)", "Sector", "Industry", "Next Earnings Date"]

    # ── Build records ────────────────────────────────────────────────────────
    records = []
    for ticker in tickers:
        detail = detail_map.get(ticker, {})
        row    = rows_by_ticker.get(ticker, {})
        f_db   = fund_map.get(ticker, {})
        tc     = tm.get(ticker, {})
        rec    = _build_value_record(ticker, detail, row, f_db, tc)
        records.append({c: rec.get(c) for c in show_cols})

    if records:
        df = pd.DataFrame(records)
        st.caption(f"{len(df)} rows")
        st.dataframe(df, column_config=_value_col_config(show_cols),
                     width="stretch", hide_index=True)
    else:
        st.info("No data available.")


# ─────────────────────────────────────────────────────────────────────────────
# Page layout
# ─────────────────────────────────────────────────────────────────────────────

st.set_page_config(page_title="Stock Analysis Tool", page_icon="📈", layout="wide")
st.title("📈 Stock Analysis Tool")
st.caption("Evaluates stocks across 10 technical & fundamental indicators · Times in CST")

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("🔍 Analyze Stocks")
    st.caption("Leave blank to scan all tickers from tickers.txt")

    ticker_input = st.text_input("Tickers (comma-separated)", placeholder="AAPL, TSLA, MU")

    st.markdown("**Date Cutoffs** *(optional)*")
    daily_date_input  = st.date_input("Latest Date for Daily",  value=None, key="daily_date")
    weekly_date_input = st.date_input("Latest Date for Weekly", value=None, key="weekly_date")

    fetch_peers_cb = st.checkbox(
        "Fetch peer valuations (F5 & F6)",
        value=True,
        key="fetch_peers",
        help="Uncheck to skip peer API calls. F5 & F6 will show N/A.",
    )

    vpn_rotate_cb = st.checkbox(
        "Switch Mullvad VPN between batches",
        value=True,
        key="vpn_rotate",
        help="Rotates Mullvad server after each batch to avoid rate limiting. Requires Mullvad CLI.",
    )

    run_btn  = st.button("▶ Run Analysis", type="primary", width="stretch")
    st.divider()

    st.markdown("""
**Indicators**  
T1 Daily Price & Volume  
T2 Weekly Price & Volume  
T3 MA Alignment (10>20>50>150>200)  
T4 Big Moves ≥10% (90d)  
F1 Qtr: +Revenue & +EPS  
F2 Year: +Revenue & +EPS  
F3 Qtr YoY (Rev>+10%, EPS>+30%)  
F4 Year YoY (Rev>+10%, EPS>+30%)  
F5 Fwd PE ≤ Peer Median  
F6 P/B ≤ Peer Median  
    """)
    st.markdown("""
**Sub-indicators**  
T1.1 3M Daily Price↑  
T1.2 3M Daily Vol↑  
T1.3 12M Daily Price↑  
T1.4 12M Daily Vol↑  
T2.1 3M Weekly Price↑  
T2.2 3M Weekly Vol↑  
T2.3 12M Weekly Price↑  
T2.4 12M Weekly Vol↑  
T3.1 MA10>MA20  
T3.2 MA20>MA50  
T3.3 MA50>MA150  
T3.4 MA150>MA200  
T4.1 Has ≥1 day +10%  
T4.2 No day ≥−10%  
F1.1 Q Rev+  
F1.2 Q EPS+  
F2.1 A Rev+  
F2.2 A EPS+  
F3.1 Q Rev YoY>+10%  
F3.2 Q EPS YoY>+30%  
F4.1 A Rev YoY>+10%  
F4.2 A EPS YoY>+30%  
    """)

# ── Handle Run button ─────────────────────────────────────────────────────────
if run_btn:
    raw = ticker_input.strip()
    daily_str   = str(daily_date_input)  if daily_date_input  else None
    weekly_str  = str(weekly_date_input) if weekly_date_input else None
    analysis_dt = _now_cst()
    fetch_peers = st.session_state.get("fetch_peers", True)
    vpn_rotate  = st.session_state.get("vpn_rotate",  False)

    # Determine ticker list — manual input or full tickers.txt
    if raw:
        ticker_list = [t.strip().upper() for t in raw.split(",") if t.strip()]
        label = f"Analyzing {len(ticker_list)} ticker(s): {', '.join(ticker_list)}"
    else:
        ticker_list = load_ticker_list()
        label = f"Scan started — {len(ticker_list)} tickers"

    if not ticker_list:
        st.sidebar.error("No tickers to process. Enter tickers or check tickers.txt.")
    else:
        # Stop any existing scan/run
        if st.session_state.scan_stop_event:
            st.session_state.scan_stop_event.set()
            time.sleep(0.2)

        pause_event = threading.Event()
        stop_event  = threading.Event()
        progress    = {}

        st.session_state.scan_pause_event  = pause_event
        st.session_state.scan_stop_event   = stop_event
        st.session_state.scan_progress     = progress
        st.session_state.last_analysis_dt  = analysis_dt
        st.session_state.last_tickers      = []
        st.session_state.last_detail_map   = {}
        st.session_state["last_inds_live"] = {}

        t = threading.Thread(
            target=scan_thread_func,
            args=(ticker_list, analysis_dt, daily_str, weekly_str,
                  pause_event, stop_event, progress, fetch_peers, vpn_rotate),
            daemon=True,
        )
        st.session_state.scan_thread = t
        t.start()
        st.sidebar.success(label)

# ─────────────────────────────────────────────────────────────────────────────
# Scan progress (fragment — auto-refreshes every 2 s without full-page rerun)
# ─────────────────────────────────────────────────────────────────────────────
_scan_progress_autorefresh()

# ─────────────────────────────────────────────────────────────────────────────
# Apply any pending filter/column ops (must run before any filter widgets render)
# ─────────────────────────────────────────────────────────────────────────────
_process_pending_ops()

# ─────────────────────────────────────────────────────────────────────────────
# Tabs
# ─────────────────────────────────────────────────────────────────────────────
tab_latest, tab_history = st.tabs(["📊 Latest Query", "🗂 All Queries"])


# ══════════════════════════════════════════════════════════════════════════════
# TAB 1: Latest Query
# ══════════════════════════════════════════════════════════════════════════════

with tab_latest:
    last_dt = st.session_state.last_analysis_dt or get_latest_run_datetime()

    if last_dt is None:
        st.info("No analysis run yet. Enter tickers or leave blank for a full scan.")
        st.stop()

    summary_rows = get_summary_for_run(last_dt)
    detail_map   = st.session_state.last_detail_map
    inds_live    = st.session_state.get("last_inds_live", {})
    tickers      = st.session_state.last_tickers

    # Scan mode: show top-50 in Latest Query
    is_scan = not tickers and bool(summary_rows)
    if is_scan:
        tickers = [r["ticker"] for r in summary_rows]
        # Score and sort: rank by score desc, then market_cap desc
        scored = []
        for r in summary_rows:
            inds_dict = {ind: {"pass": r.get(ind, "NA")} for ind in MAIN_IND_COLS}
            sc = score_indicators(inds_dict)
            mc = r.get("market_cap") or 0
            scored.append((sc, mc, r["ticker"], r))
        scored.sort(key=lambda x: (x[0], x[1]), reverse=True)
        top50_rows   = [s[3] for s in scored[:50]]
        top50_tickers = [r["ticker"] for r in top50_rows]
    else:
        top50_rows    = summary_rows
        top50_tickers = tickers or [r["ticker"] for r in summary_rows]

    # Rebuild detail_map from DB if page reloaded
    if not detail_map and summary_rows:
        detail_map = get_detail_for_run(last_dt)
        tickers    = top50_tickers

    st.subheader(f"Analysis: {last_dt}")
    if is_scan:
        total_scanned = len(summary_rows)
        st.caption(f"Scan mode — showing top 50 of {total_scanned} tickers "
                   f"(ranked by score then market cap)")

    # ── Fetch fund/tech data ───────────────────────────────────────────────────
    fund_map_latest = get_all_fundamentals_for_run(top50_tickers)
    tech_map_latest = get_tech_for_tickers(top50_tickers)
    si_map_latest   = _extract_si(fund_map_latest)
    ne_map_latest   = _extract_ne(fund_map_latest)

    # Build sector→industries mapping for cascade
    _s2i_l: dict[str, set[str]] = {}
    for s, i in si_map_latest.values():
        if s != "N/A" and i != "N/A":
            _s2i_l.setdefault(s, set()).add(i)

    # Pre-read sector selection for cascade
    _f_sectors_l_pre = st.session_state.get("latest_f_sector", [])
    all_sectors_l    = sorted({s for s, _ in si_map_latest.values() if s != "N/A"})
    if _f_sectors_l_pre:
        avail_industries_l = sorted({i for s in _f_sectors_l_pre for i in _s2i_l.get(s, set())})
    else:
        avail_industries_l = sorted({i for _, i in si_map_latest.values() if i != "N/A"})

    # ── Row 1: ticker name | sector | industry ────────────────────────────────
    lf1, lf2, lf3 = st.columns(3)
    with lf1:
        f_ticker_l = st.text_input("Filter by Ticker", placeholder="e.g. AAPL",
                                   key="latest_f_ticker_name")
    with lf2:
        f_sectors_l = st.multiselect("Filter by Sector", options=all_sectors_l,
                                     key="latest_f_sector")
    with lf3:
        f_industries_l = st.multiselect("Filter by Industry", options=avail_industries_l,
                                        key="latest_f_industry")

    # ── Market cap filter ─────────────────────────────────────────────────────
    mc1, mc2 = st.columns(2)
    with mc1:
        mc_lo_l = st.number_input("Mkt Cap min ($B)", value=None, min_value=0.0,
                                  placeholder="no min", key="latest_mc_lo",
                                  format="%.2f", step=1.0)
    with mc2:
        mc_hi_l = st.number_input("Mkt Cap max ($B)", value=None, min_value=0.0,
                                  placeholder="no max", key="latest_mc_hi",
                                  format="%.2f", step=1.0)

    # ── Indicator filter — always visible ─────────────────────────────────────
    latest_ind_filter, latest_col_filter = render_indicator_filter("latest")

    # ── Options below indicator filter ────────────────────────────────────────
    show_sub = st.checkbox("Show sub-indicators", value=False, key="latest_show_sub")

    # Apply filters
    display_rows = top50_rows
    if f_ticker_l:
        display_rows = [r for r in display_rows
                        if f_ticker_l.upper() in r["ticker"].upper()]
    if f_sectors_l:
        display_rows = [r for r in display_rows
                        if si_map_latest.get(r["ticker"], ("N/A",))[0] in f_sectors_l]
    if f_industries_l:
        display_rows = [r for r in display_rows
                        if si_map_latest.get(r["ticker"], ("N/A", "N/A"))[1] in f_industries_l]
    if mc_lo_l is not None:
        display_rows = [r for r in display_rows
                        if _mkt_cap_b(r.get("market_cap")) is not None
                        and _mkt_cap_b(r.get("market_cap")) >= mc_lo_l]
    if mc_hi_l is not None:
        display_rows = [r for r in display_rows
                        if _mkt_cap_b(r.get("market_cap")) is not None
                        and _mkt_cap_b(r.get("market_cap")) <= mc_hi_l]
    display_rows = apply_indicator_filter(display_rows, latest_ind_filter)
    display_tickers = [r["ticker"] for r in display_rows]
    # Apply column value filter
    if latest_col_filter:
        all_rows_by_ticker_l = {r["ticker"]: r for r in top50_rows}
        display_tickers = apply_col_filter(
            display_tickers, latest_col_filter, detail_map,
            all_rows_by_ticker_l, fund_map_latest, tech_map_latest,
        )
        display_rows = [r for r in display_rows if r["ticker"] in set(display_tickers)]

    st.caption(f"Showing **{len(display_rows)}** / {len(top50_rows)} rows")

    # ── Indicator Summary ─────────────────────────────────────────────────────
    st.markdown("### 📊 Indicator Summary")
    st.caption("✅ PASS · ⭕ PARTIAL · ❌ FAIL · ⚪️ N/A  —  Edit any cell and click **Save Edits**")

    sum_df  = build_summary_df(display_rows, show_sub=show_sub,
                               si_map=si_map_latest, ne_map=ne_map_latest)
    col_cfg = make_column_config(sum_df)

    edited = st.data_editor(
        sum_df, column_config=col_cfg,
        width="stretch", hide_index=True,
        key="latest_sum_editor",
    )
    if st.button("💾 Save Edits", key="save_latest"):
        save_edits(display_rows, edited, include_datetime=False)
        st.success("Saved.")

    st.markdown("---")

    # ── Value Table ───────────────────────────────────────────────────────────
    rows_by_ticker_latest = {r["ticker"]: r for r in display_rows}
    render_value_table(display_tickers, detail_map,
                       rows_by_ticker_latest, fund_map_latest, "latest",
                       tech_map=tech_map_latest)

    st.markdown("---")

    # ── Indicator Detail ──────────────────────────────────────────────────────
    st.markdown("### 🔍 Indicator Detail")
    render_detail_for_tickers(display_tickers, detail_map, inds_live or None, state_key="latest")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2: All Queries
# ══════════════════════════════════════════════════════════════════════════════

with tab_history:
    all_dts = get_all_run_datetimes()

    if not all_dts:
        st.info("No historical data yet.")
        st.stop()

    # ── All-Runs Summary ──────────────────────────────────────────────────────
    st.markdown("### 🗂 All-Runs Summary")
    st.caption("Rows sorted: newest datetime first, then alphabetical ticker")

    # Pre-read widget states for data fetching before widget rendering
    _f_tickers_pre   = st.session_state.get("hist_f_tick", [])
    _f_dts_pre       = st.session_state.get("hist_f_dt", [])
    _only_latest_pre = st.session_state.get("hist_only_latest", True)   # default True
    _f_sectors_pre   = st.session_state.get("hist_f_sector", [])
    _mc_lo_pre       = st.session_state.get("hist_mc_lo", None)
    _mc_hi_pre       = st.session_state.get("hist_mc_hi", None)

    # ── Fetch rows ────────────────────────────────────────────────────────────
    filt_rows = get_all_summaries(
        tickers   = _f_tickers_pre if _f_tickers_pre else None,
        datetimes = _f_dts_pre     if _f_dts_pre     else None,
    )
    total_before = len(filt_rows)

    # Apply "latest only" using pre-read value
    if _only_latest_pre:
        _seen: dict[str, str] = {}
        for r in filt_rows:
            t, dt = r["ticker"], r.get("analysis_datetime", "")
            if t not in _seen or dt > _seen[t]:
                _seen[t] = dt
        filt_rows = [r for r in filt_rows
                     if r.get("analysis_datetime") == _seen.get(r["ticker"])]

    # Apply mkt cap pre-filter (so sector options reflect mkt-cap-filtered set)
    if _mc_lo_pre is not None:
        filt_rows = [r for r in filt_rows
                     if _mkt_cap_b(r.get("market_cap")) is not None
                     and _mkt_cap_b(r.get("market_cap")) >= _mc_lo_pre]
    if _mc_hi_pre is not None:
        filt_rows = [r for r in filt_rows
                     if _mkt_cap_b(r.get("market_cap")) is not None
                     and _mkt_cap_b(r.get("market_cap")) <= _mc_hi_pre]

    # ── Fetch fund/tech data for filtered tickers ─────────────────────────────
    _hist_tickers_pre = list({r["ticker"] for r in filt_rows})
    hist_fund_map     = get_all_fundamentals_for_run(_hist_tickers_pre)
    hist_tech_map     = get_tech_for_tickers(_hist_tickers_pre)
    si_map_hist       = _extract_si(hist_fund_map)
    ne_map_hist       = _extract_ne(hist_fund_map)

    # Build sector→industries cascade mapping
    _s2i_h: dict[str, set[str]] = {}
    for s, i in si_map_hist.values():
        if s != "N/A" and i != "N/A":
            _s2i_h.setdefault(s, set()).add(i)
    all_sectors_h = sorted({s for s, _ in si_map_hist.values() if s != "N/A"})
    if _f_sectors_pre:
        avail_industries_h = sorted({i for s in _f_sectors_pre for i in _s2i_h.get(s, set())})
    else:
        avail_industries_h = sorted({i for _, i in si_map_hist.values() if i != "N/A"})

    # ── Row 1: ticker | datetime | sector | industry ──────────────────────────
    all_stored_tickers = get_all_tickers()
    fc1, fc2, fc3, fc4 = st.columns(4)
    with fc1:
        f_tickers = st.multiselect("Filter by Ticker", options=all_stored_tickers,
                                   key="hist_f_tick")
    with fc2:
        f_dts = st.multiselect("Filter by Datetime", options=all_dts, key="hist_f_dt")
    with fc3:
        f_sectors_h = st.multiselect("Filter by Sector", options=all_sectors_h,
                                     key="hist_f_sector")
    with fc4:
        f_industries_h = st.multiselect("Filter by Industry", options=avail_industries_h,
                                        key="hist_f_industry")

    # ── Market cap filter ─────────────────────────────────────────────────────
    mc1, mc2 = st.columns(2)
    with mc1:
        mc_lo_h = st.number_input("Mkt Cap min ($B)", value=None, min_value=0.0,
                                  placeholder="no min", key="hist_mc_lo",
                                  format="%.2f", step=1.0)
    with mc2:
        mc_hi_h = st.number_input("Mkt Cap max ($B)", value=None, min_value=0.0,
                                  placeholder="no max", key="hist_mc_hi",
                                  format="%.2f", step=1.0)

    # ── Indicator filter — always visible ─────────────────────────────────────
    hist_ind_filter, hist_col_filter = render_indicator_filter("history")

    # ── Options below indicator filter ────────────────────────────────────────
    oc1, oc2 = st.columns(2)
    with oc1:
        all_show_sub = st.checkbox("Show sub-indicators", value=False,
                                   key="all_queries_show_sub")
    with oc2:
        only_latest = st.checkbox("Latest entry per ticker only", value=True,
                                  key="hist_only_latest")

    # Apply remaining filters using current-run widget values
    if f_sectors_h:
        filt_rows = [r for r in filt_rows
                     if si_map_hist.get(r["ticker"], ("N/A",))[0] in f_sectors_h]
    if f_industries_h:
        filt_rows = [r for r in filt_rows
                     if si_map_hist.get(r["ticker"], ("N/A", "N/A"))[1] in f_industries_h]

    filt_rows = apply_indicator_filter(filt_rows, hist_ind_filter)

    # Build detail map (needed for col filter): use most-recent datetime per ticker
    _ticker_latest: dict[str, str] = {}
    for r in filt_rows:
        t  = r["ticker"]
        dt = r.get("analysis_datetime", "")
        if t not in _ticker_latest or dt > _ticker_latest[t]:
            _ticker_latest[t] = dt
    hist_detail_map: dict = {}
    for t, dt in _ticker_latest.items():
        t_det = get_detail_filtered(ticker=t, analysis_dt=dt)
        if t in t_det:
            hist_detail_map[t] = t_det[t]

    # Apply column value filter
    if hist_col_filter:
        hist_rows_by_ticker_all = {r["ticker"]: r for r in filt_rows}
        _hist_pass = set(apply_col_filter(
            [r["ticker"] for r in filt_rows], hist_col_filter, hist_detail_map,
            hist_rows_by_ticker_all, hist_fund_map, hist_tech_map,
        ))
        filt_rows = [r for r in filt_rows if r["ticker"] in _hist_pass]

    st.caption(f"Showing **{len(filt_rows)}** / {total_before} rows")

    all_df = build_summary_df(filt_rows, show_sub=all_show_sub,
                              include_datetime=True, si_map=si_map_hist,
                              ne_map=ne_map_hist)
    all_col_cfg = make_column_config(all_df)

    all_edited = st.data_editor(
        all_df, column_config=all_col_cfg,
        width="stretch", hide_index=True,
        key="all_sum_editor",
    )
    if st.button("💾 Save Edits", key="save_all"):
        save_edits(filt_rows, all_edited, include_datetime=True)
        st.success("Saved.")

    st.markdown("---")

    # ── Value Table ───────────────────────────────────────────────────────────
    hist_tickers = [r["ticker"] for r in filt_rows]
    hist_rows_by_ticker = {r["ticker"]: r for r in filt_rows}
    render_value_table(hist_tickers, hist_detail_map,
                       hist_rows_by_ticker, hist_fund_map, "history",
                       tech_map=hist_tech_map)

    st.markdown("---")

    # ── Detail by Analysis Run ────────────────────────────────────────────────
    st.markdown("### 🔍 Detail by Analysis Run")
    st.caption("Ticker dropdown filters to datetimes for that ticker, and vice versa")

    # Read current selections to compute dependent options
    cur_ticker = st.session_state.get("det_ticker", "")
    cur_dt     = st.session_state.get("det_dt", "")

    # Compute filtered options based on the other field's current value
    avail_dts     = get_datetimes_for_ticker(cur_ticker) if cur_ticker else all_dts
    avail_tickers = get_tickers_for_datetime(cur_dt)     if cur_dt     else all_stored_tickers

    # Reset the other field if its stored value is no longer valid
    if cur_dt and cur_dt not in avail_dts:
        st.session_state["det_dt"] = ""
        cur_dt = ""
        avail_dts = get_datetimes_for_ticker(cur_ticker) if cur_ticker else all_dts
    if cur_ticker and cur_ticker not in avail_tickers:
        st.session_state["det_ticker"] = ""
        cur_ticker = ""
        avail_tickers = get_tickers_for_datetime(cur_dt) if cur_dt else all_stored_tickers

    dcol1, dcol2 = st.columns(2)
    with dcol1:
        det_ticker = st.selectbox("Ticker (optional)", options=[""] + avail_tickers,
                                  key="det_ticker")
    with dcol2:
        det_dt = st.selectbox("Datetime (optional)", options=[""] + avail_dts,
                              key="det_dt")

    det_ticker_val = det_ticker if det_ticker else None
    det_dt_val     = det_dt     if det_dt     else None

    if det_ticker_val and not det_dt_val:
        # Show all records for this ticker, one section per datetime
        dts = get_datetimes_for_ticker(det_ticker_val)
        if not dts:
            st.info(f"No data found for {det_ticker_val}.")
        for dt in dts:
            det_map = get_detail_filtered(ticker=det_ticker_val, analysis_dt=dt)
            render_detail_for_tickers(
                [det_ticker_val], det_map,
                dt_label=dt,
                state_key=f"history_{det_ticker_val}_{dt}",
            )
    elif det_ticker_val and det_dt_val:
        det_map = get_detail_filtered(ticker=det_ticker_val, analysis_dt=det_dt_val)
        render_detail_for_tickers(
            [det_ticker_val], det_map,
            dt_label=det_dt_val,
            state_key=f"history_{det_ticker_val}_{det_dt_val}",
        )
    elif det_dt_val:
        det_map = get_detail_filtered(analysis_dt=det_dt_val)
        render_detail_for_tickers(
            sorted(det_map.keys()), det_map,
            state_key=f"history_dt_{det_dt_val}",
        )
    else:
        st.info("Select a ticker and/or datetime above to view detail.")
