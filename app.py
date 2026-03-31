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
import fundamental_fetcher
from fundamental_fetcher import fetch_fundamental
from peers_fetcher import get_peer_valuations, clear_peer_cache
import vpn_switcher
from indicators    import evaluate_all, score_indicators
import storage
from storage import (
    init_db, save_results, update_field, save_comment_for_ticker, save_status_for_ticker,
    save_user_field_for_ticker,
    get_all_run_datetimes, get_latest_run_datetime,
    get_summary_for_run, get_detail_for_run, get_all_summaries,
    get_detail_filtered, get_all_tickers,
    get_datetimes_for_ticker, get_tickers_for_datetime,
    get_cached_peer_valuations, save_peer_valuations,
    get_all_fundamentals_for_run, get_tech_for_tickers,
    get_latest_earnings_for_tickers,
    MAIN_IND_COLS, ALL_SUB_COLS, SUB_COLS,
)

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

CST = ZoneInfo("America/Chicago")

EMOJI = {"PASS": "✅", "PARTIAL": "⭕", "FAIL": "❌", "NA": "⚪️"}
EMOJI_TO_DB = {v: k for k, v in EMOJI.items()}
EMOJI_OPTIONS = ["✅", "⭕", "❌", "⚪️"]
STATUS_OPTIONS = ["", "必買", "買", "等", "研究", "X"]

TICKERS_FILE = Path(__file__).parent / "tickers.txt"

# ── Value table column groups ──────────────────────────────────────────────────
VALUE_COL_GROUPS: dict[str, list[str]] = {
    "User": ["Status"],
    # ── Indicator-derived groups (from analysis detail JSON) ──────────────────
    "Price & Volume — Daily (T1)": [
        "3M Avg Px%", "3M Avg Vol%", "12M Avg Px%", "12M Avg Vol%",
    ],
    "Price & Volume — Weekly (T2)": [
        "3M Wkly Avg Px%", "3M Wkly Avg Vol%", "12M Wkly Avg Px%", "12M Wkly Avg Vol%",
    ],
    # ── price_history-derived groups ──────────────────────────────────────────
    "Spot Returns": [
        "5D Px%", "5D Vol%",
        "1M Px%", "1M Vol%",
        "3M Px%", "3M Vol%",
        "6M Px%", "6M Vol%",
        "12M Px%", "12M Vol%",
        "2Y Px%", "2Y Vol%",
        "3Y Px%", "3Y Vol%",
    ],
    "Rolling Returns (Daily)": [
        "1M Avg Px%", "1M Avg Vol%",
        "6M Avg Px%", "6M Avg Vol%",
        "2Y Avg Px%", "2Y Avg Vol%",
        "3Y Avg Px%", "3Y Avg Vol%",
    ],
    "Rolling Returns (Weekly)": [
        "1M Wkly Avg Px%", "1M Wkly Avg Vol%",
        "6M Wkly Avg Px%", "6M Wkly Avg Vol%",
        "2Y Wkly Avg Px%", "2Y Wkly Avg Vol%",
        "3Y Wkly Avg Px%", "3Y Wkly Avg Vol%",
    ],
    "Custom Period Returns": [
        "Cust Px%", "Cust Vol%", "Cust Avg Px%", "Cust Avg Vol%",
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
        "Mkt Cap ($B)", "Sector", "Industry", "Company Name", "Company Description",
    ],
    "Earnings Detail": [
        "Next Earnings Date", "Next Earnings Time",
        "Last Earnings Date", "Last Earnings Time", "Earns 1D Px%",
        "EPS Est", "EPS Act", "EPS Sur",
        "EPS GAAP Est", "EPS GAAP Act", "EPS GAAP Sur",
        "Rev Est ($M)", "Rev Act ($M)", "Rev Sur",
    ],
    "Earnings Extended": [
        "Earns 1D Px%", "Earns 1D Vol%", "Earns 5D Px%", "Earns 5D Vol%",
        "Earns 5D Roll Px%", "Earns 5D Roll Vol%",
    ],
    "Score": ["Score"],
    "Short Interest": [
        "Short % Float (Y)", "Short % Float (Calc)",
        "Short % Out (Y)", "Short % Out (Calc)", "Short % Impl Out",
        "Days to Cover", "Shares Short (M)", "Float Shares (M)",
        "Shares Out (M)", "Short MoM Chg%", "Short Interest Date", "Avg Vol (M)",
    ],
    "Insider Activity (6M)": [
        "Ins Buy #", "Ins Sell #", "Ins Buy Shares (M)",
        "Ins Sell Shares (M)", "Ins Net Shares (M)",
        "Ins Buy %", "Ins Sell %", "Ins Net %",
    ],
    "Margins & Ratios": [
        "Gross Margin%", "EBITDA Margin%", "Op Margin%", "Net Margin%",
        "Current Ratio", "Quick Ratio", "D/E Ratio", "ROE%", "ROA%",
    ],
    "Analyst Targets": [
        "Target Median", "Target High", "Target Low", "Target Mean",
        "Price vs Target%", "Rec Score", "Rec Key", "Analyst Count",
    ],
    # ── tech_indicators groups ─────────────────────────────────────────────────
    "Price & 52W": [
        "Last Close Date", "Close", "Change %", "52W High", "52W Low", "From 52W High%", "From 52W Low%", "52W Pos%",
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
    "as_of_date":          "Last Close Date",
    "is_finalized":        "Finalized",
    "daily_pct_change":    "Change %",
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
_USER_NOTE_COLS = {
    "Company Summary", "Revenue Composition",
    "technical +ve", "fundamental +ve", "technical -ve", "fundamental -ve",
}

_COL_FILTER_SKIP = {
    "Ticker", "Sector", "Industry",
    "Rec Key", "Company Name", "Company Description",
} | _USER_NOTE_COLS
_COL_FILTER_EMOJI = {
    "MA10>20", "MA20>50", "MA50>150", "MA150>200",
    "MA10>MA20", "MA20>MA50", "MA50>MA150", "MA150>MA200",
    "Breakout 55D", "Breakout 3M", "Finalized",
}
_COL_FILTER_TEXT_CAT: dict[str, list[str]] = {
    "Last Earnings Time":  ["BMO", "AMC"],
    "Next Earnings Time":  ["BMO", "AMC"],
    "Status":              ["", "必買", "買", "等", "研究", "X"],
}
_COL_FILTER_DATES = {"Q End Date", "A End Date", "Last Close Date", "Last Earnings Date", "Short Interest Date", "Next Earnings Date"}
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

try:
    init_db()
except Exception as _init_err:
    print(f"[storage] init_db warning: {_init_err}")

if "_startup_finalized" not in st.session_state:
    st.session_state["_startup_finalized"] = True
    try:
        storage.mark_old_tech_finalized()
    except Exception as _e:
        print(f"[startup] mark_old_tech_finalized failed: {_e}")

if "_earnings_fetched" not in st.session_state:
    st.session_state["_earnings_fetched"] = True
    try:
        from earnings_fetcher import run_daily_fetch as _earnings_daily
        from storage import get_latest_earnings_date as _get_latest_earnings_date
        _since = _get_latest_earnings_date()
        if _since:
            print(f"[earnings] Latest earnings date in DB: {_since} — fetching from there")
            _earnings_daily(since_date=_since)
        else:
            _earnings_daily(lookback_days=7)
    except Exception as _e:
        print(f"[earnings] Daily fetch failed: {_e}")

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


def compute_score(row: dict) -> float:
    """Compute 0–100 composite score from analysis_runs sub-indicator columns.

    Scoring rules (max 100):
      T1/T2 pairs — price PASS → 2.5; price+vol both PASS → 5  (4 pairs × 5 = 20)
      T3.1–T3.4   — each 5 pts                                  (4 × 5 = 20)
      T4.1–T4.2   — each 5 pts                                  (2 × 5 = 10)
      F1.1–F4.2   — each 5 pts                                  (8 × 5 = 40)
      F5, F6      — each 5 pts                                  (2 × 5 = 10)
    """
    def p(col: str) -> bool:
        return str(row.get(col, "NA")).upper() == "PASS"

    s = 0.0
    # T1 pairs: price only → 2.5; price + vol → 5
    if p("T1_sub_3m_price"):  s += 2.5 + (2.5 if p("T1_sub_3m_vol") else 0)
    if p("T1_sub_12m_price"): s += 2.5 + (2.5 if p("T1_sub_12m_vol") else 0)
    # T2 pairs
    if p("T2_sub_3m_price"):  s += 2.5 + (2.5 if p("T2_sub_3m_vol") else 0)
    if p("T2_sub_12m_price"): s += 2.5 + (2.5 if p("T2_sub_12m_vol") else 0)
    # T3 (each 5)
    for c in ("T3_sub_ma10_20", "T3_sub_ma20_50", "T3_sub_ma50_150", "T3_sub_ma150_200"):
        if p(c): s += 5
    # T4 (each 5)
    for c in ("T4_sub_has_big_up", "T4_sub_no_big_down"):
        if p(c): s += 5
    # F1–F4 sub-indicators (each 5)
    for c in ("F1_sub_q_rev", "F1_sub_q_eps", "F2_sub_a_rev", "F2_sub_a_eps",
              "F3_sub_q_rev_yoy", "F3_sub_q_eps_yoy", "F4_sub_a_rev_yoy", "F4_sub_a_eps_yoy"):
        if p(c): s += 5
    # F5, F6 main indicators (each 5)
    for c in ("F5", "F6"):
        if p(c): s += 5
    return s


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

def _extract_company(fund_map: dict) -> dict[str, tuple[str, str]]:
    """Extract {ticker: (longName, longBusinessSummary)} from a fund_map dict."""
    result: dict[str, tuple[str, str]] = {}
    for ticker, f_db in fund_map.items():
        raw_info = _parse_raw_info(f_db)
        result[ticker] = (
            raw_info.get("longName") or "N/A",
            raw_info.get("longBusinessSummary") or "N/A",
        )
    return result


def _earnings_ts_to_date_time(v) -> tuple[str, str]:
    """Convert a Yahoo Finance epoch earnings timestamp to (date_str, time_str).

    time_str is "BMO" if hour < 16 UTC (pre-market / before 4pm ET),
    "AMC" if hour >= 16 UTC (after-hours / 4pm+ ET), or "N/A" on error.
    """
    from datetime import timezone as _tz
    if v is None:
        return "N/A", "N/A"
    if isinstance(v, list):
        v = v[0] if v else None
    if v is None:
        return "N/A", "N/A"
    try:
        dt = datetime.fromtimestamp(float(v), tz=_tz.utc)
        date_str = dt.strftime("%Y-%m-%d")
        time_str = "BMO" if dt.hour < 16 else "AMC"
        return date_str, time_str
    except Exception:
        return "N/A", "N/A"


def _extract_ne(fund_map: dict) -> dict[str, str]:
    """Extract {ticker: next_earnings_date_str} from a fund_map dict."""
    result: dict[str, str] = {}
    for ticker, f_db in fund_map.items():
        raw_info = _parse_raw_info(f_db)
        v = raw_info.get("earningsDate") or raw_info.get("earningsTimestamp")
        result[ticker], _ = _earnings_ts_to_date_time(v)
    return result


def _extract_net(fund_map: dict) -> dict[str, str]:
    """Extract {ticker: next_earnings_time (BMO/AMC/N/A)} from a fund_map dict."""
    result: dict[str, str] = {}
    for ticker, f_db in fund_map.items():
        raw_info = _parse_raw_info(f_db)
        v = raw_info.get("earningsDate") or raw_info.get("earningsTimestamp")
        _, result[ticker] = _earnings_ts_to_date_time(v)
    return result


def _build_value_record(ticker: str, detail: dict, row: dict, f_db: dict,
                        tech: dict | None = None,
                        earnings: dict | None = None,
                        returns: dict | None = None) -> dict:
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

    def _next_earnings_both(info: dict) -> tuple[str, str]:
        v = info.get("earningsDate") or info.get("earningsTimestamp")
        return _earnings_ts_to_date_time(v)

    rec = {
        "Ticker":          ticker,
        "Company Summary":     row.get("company_summary") or "",
        "Revenue Composition": row.get("revenue_composition") or "",
        "Status":              row.get("status") or "",
        "Comments":        row.get("comments") or "",
        "technical +ve":   row.get("tech_pos") or "",
        "fundamental +ve": row.get("fund_pos") or "",
        "technical -ve":   row.get("tech_neg") or "",
        "fundamental -ve": row.get("fund_neg") or "",
        "Score":           compute_score(row),
        # T1 — daily comparisons (float %) — renamed from "3M Daily Px%" etc.
        "3M Avg Px%":    _f(t1.get("3M Price Change %")),
        "3M Avg Vol%":   _f(t1.get("3M Volume Change %")),
        "12M Avg Px%":   _f(t1.get("12M Price Change %")),
        "12M Avg Vol%":  _f(t1.get("12M Volume Change %")),
        # T2 — weekly comparisons (float %) — renamed from "3M Wkly Px%" etc.
        "3M Wkly Avg Px%":  _f(t2.get("3M Price Change %")),
        "3M Wkly Avg Vol%": _f(t2.get("3M Volume Change %")),
        "12M Wkly Avg Px%": _f(t2.get("12M Price Change %")),
        "12M Wkly Avg Vol%":_f(t2.get("12M Volume Change %")),
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
        "Sector":              raw_info.get("sector") or "N/A",
        "Industry":            raw_info.get("industry") or "N/A",
        "Company Name":        raw_info.get("longName") or "N/A",
        "Company Description": raw_info.get("longBusinessSummary") or "N/A",
        # Next earnings (date + BMO/AMC time from same timestamp)
        "Next Earnings Date": _next_earnings_both(raw_info)[0],
        "Next Earnings Time": _next_earnings_both(raw_info)[1],
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
        "Last Close Date":  str(tc.get("as_of_date") or "N/A"),
        "Finalized":        _bi(tc.get("is_finalized")),
        "Change %":         _f(tc.get("daily_pct_change")),
    }
    # ── Earnings columns (from earnings_history) ──────────────────────────────
    e = earnings or {}
    rec["Last Earnings Date"]      = e.get("earnings_date") or "N/A"
    rec["Last Earnings Time"]      = e.get("earnings_time") or "N/A"
    rec["Earns 1D Px%"] = _f(e.get("one_day_change"))
    rec["EPS Est"]       = _f(e.get("eps_est"))
    rec["EPS Act"]       = _f(e.get("eps_act"))
    rec["EPS Sur"]       = _f(e.get("eps_sur"))
    rec["EPS GAAP Est"]  = _f(e.get("eps_gaap_est"))
    rec["EPS GAAP Act"]  = _f(e.get("eps_gaap_act"))
    rec["EPS GAAP Sur"]  = _f(e.get("eps_gaap_sur"))
    rec["Rev Est ($M)"]  = _f(e.get("rev_est_m"))
    rec["Rev Act ($M)"]  = _f(e.get("rev_act_m"))
    rec["Rev Sur"]       = _f(e.get("rev_sur"))
    # ── Earnings extended columns ─────────────────────────────────────────────
    rec["Earns 1D Vol%"]       = _f(e.get("earns_1d_vol_pct"))
    rec["Earns 5D Px%"]        = _f(e.get("earns_5d_px_pct"))
    rec["Earns 5D Vol%"]       = _f(e.get("earns_5d_vol_pct"))
    rec["Earns 5D Roll Px%"]   = _f(e.get("earns_5d_roll_px_pct"))
    rec["Earns 5D Roll Vol%"]  = _f(e.get("earns_5d_roll_vol_pct"))

    # ── Short interest (from fundamentals DB) ─────────────────────────────────
    def _pct(v):
        return round(_f(v) * 100, 2) if v is not None else None

    sh      = f_db.get("shares_short")
    sh_pm   = f_db.get("shares_short_pm")
    fl      = f_db.get("float_shares")
    so      = f_db.get("shares_out")
    si      = f_db.get("implied_shares")
    spf     = f_db.get("short_pct_float")
    spo     = f_db.get("short_pct_out")
    avgvol  = f_db.get("avg_volume")

    rec["Short % Float (Y)"]    = _pct(spf)
    rec["Short % Float (Calc)"] = round(_f(sh) / _f(fl) * 100, 2) if sh and fl else None
    rec["Short % Out (Y)"]      = _pct(spo)
    rec["Short % Out (Calc)"]   = round(_f(sh) / _f(so) * 100, 2) if sh and so else None
    rec["Short % Impl Out"]     = round(_f(sh) / _f(si) * 100, 2) if sh and si else None
    rec["Days to Cover"]        = _f(f_db.get("short_ratio"))
    rec["Shares Short (M)"]     = round(_f(sh) / 1e6, 2) if sh else None
    rec["Float Shares (M)"]     = round(_f(fl) / 1e6, 2) if fl else None
    rec["Shares Out (M)"]       = round(_f(so) / 1e6, 2) if so else None
    rec["Short MoM Chg%"]       = round((_f(sh) - _f(sh_pm)) / abs(_f(sh_pm)) * 100, 2) \
                                  if sh and sh_pm else None
    rec["Short Interest Date"]  = f_db.get("date_short_int") or "N/A"
    rec["Avg Vol (M)"]          = round(_f(avgvol) / 1e6, 2) if avgvol else None

    # ── Insider activity (6M) ─────────────────────────────────────────────────
    rec["Ins Buy #"]           = f_db.get("ins_buy_count")
    rec["Ins Sell #"]          = f_db.get("ins_sell_count")
    rec["Ins Buy Shares (M)"]  = round(_f(f_db.get("ins_buy_shares"))  / 1e6, 3) \
                                 if f_db.get("ins_buy_shares")  else None
    rec["Ins Sell Shares (M)"] = round(_f(f_db.get("ins_sell_shares")) / 1e6, 3) \
                                 if f_db.get("ins_sell_shares") else None
    rec["Ins Net Shares (M)"]  = round(_f(f_db.get("ins_net_shares"))  / 1e6, 3) \
                                 if f_db.get("ins_net_shares")  else None
    rec["Ins Buy %"]           = _pct(f_db.get("ins_buy_pct"))
    rec["Ins Sell %"]          = _pct(f_db.get("ins_sell_pct"))
    rec["Ins Net %"]           = _pct(f_db.get("ins_net_pct"))

    # ── Margins & ratios ──────────────────────────────────────────────────────
    rec["Gross Margin%"]  = _pct(f_db.get("gross_margin"))
    rec["EBITDA Margin%"] = _pct(f_db.get("ebitda_margin"))
    rec["Op Margin%"]     = _pct(f_db.get("op_margin"))
    rec["Net Margin%"]    = _pct(f_db.get("net_margin"))
    rec["Current Ratio"]  = _f(f_db.get("current_ratio"))
    rec["Quick Ratio"]    = _f(f_db.get("quick_ratio"))
    rec["D/E Ratio"]      = _f(f_db.get("debt_to_equity"))
    rec["ROE%"]           = _pct(f_db.get("roe"))
    rec["ROA%"]           = _pct(f_db.get("roa"))

    # ── Analyst targets ───────────────────────────────────────────────────────
    t_med = _f(f_db.get("target_median"))
    c_px  = _f(f_db.get("current_price_fd"))
    rec["Target Median"]    = t_med
    rec["Target High"]      = _f(f_db.get("target_high"))
    rec["Target Low"]       = _f(f_db.get("target_low"))
    rec["Target Mean"]      = _f(f_db.get("target_mean"))
    rec["Price vs Target%"] = round((c_px / t_med - 1) * 100, 2) if c_px and t_med else None
    rec["Rec Score"]        = _f(f_db.get("rec_mean"))
    rec["Rec Key"]          = f_db.get("rec_key") or "N/A"
    rec["Analyst Count"]    = f_db.get("analyst_count")

    # ── Price-history returns (spot, rolling avg, custom period) ──────────────
    ph = returns or {}
    # Spot
    for col in ("5D Px%", "5D Vol%", "1M Px%", "1M Vol%", "3M Px%", "3M Vol%",
                "6M Px%", "6M Vol%", "12M Px%", "12M Vol%", "2Y Px%", "2Y Vol%",
                "3Y Px%", "3Y Vol%"):
        rec[col] = _f(ph.get(col))
    # Daily rolling avg (new periods)
    for col in ("1M Avg Px%", "1M Avg Vol%", "6M Avg Px%", "6M Avg Vol%",
                "2Y Avg Px%", "2Y Avg Vol%", "3Y Avg Px%", "3Y Avg Vol%"):
        rec[col] = _f(ph.get(col))
    # Weekly rolling avg (new periods)
    for col in ("1M Wkly Avg Px%", "1M Wkly Avg Vol%",
                "6M Wkly Avg Px%", "6M Wkly Avg Vol%",
                "2Y Wkly Avg Px%", "2Y Wkly Avg Vol%",
                "3Y Wkly Avg Px%", "3Y Wkly Avg Vol%"):
        rec[col] = _f(ph.get(col))
    # Custom period
    for col in ("Cust Px%", "Cust Vol%", "Cust Avg Px%", "Cust Avg Vol%"):
        rec[col] = _f(ph.get(col))

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
    fundamental_fetcher.set_stop_event(stop_event)
    clear_peer_cache()   # fresh cache for each scan run
    total = len(tickers)
    progress.update({
        "total": total, "done": 0, "current": "", "finished": False,
        "error": None, "failures": {},  # {ticker: {reason, missing_fields}}
    })

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
        # V2 fetcher stores extended indicators (Close/as_of_date/etc.) to DuckDB.
        # Use V1 only when daily_date is set (historical backtest: Close itself is
        # historical).  When only weekly_date is set, V2 is used so that Close /
        # Last Close Date / Change % always reflect today's data; the weekly cutoff
        # is passed through to limit only the T2 weekly comparison window.
        if daily_date:
            bulk_tech = fetch_technical_bulk(batch, daily_date, weekly_date)
        else:
            bulk_tech = fetch_technical_bulk_v2(batch, weekly_latest_date=weekly_date,
                                                log=lambda m: None)

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
            for t in batch:
                if _is_delisted(t):
                    err = bulk_tech.get(t, {}).get("error", "no price data")
                    progress["failures"][t] = {"reason": err, "missing": ["all tech data"]}
            done += skipped_count
            progress["done"] = done

        pool = concurrent.futures.ThreadPoolExecutor(max_workers=SCAN_FUND_WORKERS)
        future_map = {pool.submit(_run_fund_and_peers, t, fetch_peers): t for t in valid_batch}
        try:
            for future in concurrent.futures.as_completed(future_map):
                if stop_event.is_set():
                    break
                t = future_map[future]
                try:
                    fund, peer_data = future.result()
                    if fund.get("rate_limited"):
                        consecutive_failures += 1
                        rate_limited_tickers.append(t)
                        progress["failures"][t] = {"reason": "rate limited (fund fetch)", "missing": ["fundamentals", "peers"]}
                    else:
                        consecutive_failures = 0
                        tech = bulk_tech.get(t, {"error": "Not in bulk data"})
                        # Collect missing fields for this ticker
                        missing = []
                        if tech.get("error"):
                            missing.append(f"tech: {tech['error']}")
                        for f_key, f_label in [("q_revenue","q_revenue"), ("q_eps","q_eps"),
                                               ("a_revenue","a_revenue"), ("a_eps","a_eps"),
                                               ("q_rev_yoy","q_rev_yoy"), ("q_eps_yoy","q_eps_yoy"),
                                               ("a_rev_yoy","a_rev_yoy"), ("a_eps_yoy","a_eps_yoy"),
                                               ("forward_pe","forward_pe"), ("pb_ratio","pb_ratio")]:
                            if fund.get(f_key) is None:
                                missing.append(f_label)
                        if missing:
                            progress["failures"][t] = {"reason": "partial data", "missing": missing}
                        indicators = evaluate_all(t, tech, fund, peer_data)
                        save_results(t, indicators, analysis_dt, fund.get("market_cap"))
                    # Reactive VPN switch: 3 consecutive auth blocks, once per batch
                    if vpn_rotate and consecutive_failures >= 3 and not vpn_switched_this_batch:
                        _do_vpn_switch("Persistent auth block detected")
                except Exception as ex:
                    progress["failures"][t] = {"reason": str(ex), "missing": ["all"]}
                done += 1
                progress["done"] = done
        finally:
            pool.shutdown(wait=False, cancel_futures=True)

        # ── Step 2b: re-queue rate-limited tickers after VPN switch ───────────
        if rate_limited_tickers and vpn_switched_this_batch and not stop_event.is_set():
            progress["current"] = (
                f"Re-processing {len(rate_limited_tickers)} rate-limited tickers on new IP…"
            )
            # Use 2 workers — gentle on the freshly switched IP
            retry_pool = concurrent.futures.ThreadPoolExecutor(max_workers=2)
            retry_map = {
                retry_pool.submit(_run_fund_and_peers, t, fetch_peers): t
                for t in rate_limited_tickers
            }
            try:
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
            finally:
                retry_pool.shutdown(wait=False, cancel_futures=True)

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

    fundamental_fetcher.set_stop_event(None)
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
                     ne_map: dict | None = None,
                     net_map: dict | None = None,
                     tech_map: dict | None = None,
                     company_map: dict | None = None) -> pd.DataFrame:
    """
    Build the indicator summary DataFrame.
    si_map:      {ticker: (sector, industry)}
    ne_map:      {ticker: next_earnings_date_str}
    net_map:     {ticker: next_earnings_time ("BMO"/"AMC"/"N/A")}
    tech_map:    {ticker: tech_indicators dict} for Close / Change % / Last Close Date
    company_map: {ticker: (longName, longBusinessSummary)}
    """
    if not rows:
        return pd.DataFrame()

    inds_to_show = selected_inds if selected_inds else MAIN_IND_COLS
    si  = si_map      or {}
    ne  = ne_map      or {}
    net = net_map     or {}
    tm  = tech_map    or {}
    cm  = company_map or {}

    records = []
    for r in rows:
        rec: dict = {}
        if include_datetime:
            rec["Datetime"] = r.get("analysis_datetime", "")
        rec["Ticker"] = r.get("ticker", "")

        for ind in inds_to_show:
            grade = r.get(ind, "NA")
            subs  = SUB_COLS.get(ind, [])
            # If ANY sub-indicator is NA, show ⚪️ for the parent (data incomplete)
            if subs and any(r.get(sc, "NA") == "NA" for sc in subs):
                rec[ind] = "⚪️"
            else:
                rec[ind] = _e(grade)

        if show_sub:
            for ind in inds_to_show:
                for sc in SUB_COLS.get(ind, []):
                    rec[SUB_DISPLAY.get(sc, sc)] = _e(r.get(sc, "NA"))

        rec["Score"]           = compute_score(r)
        rec["Company Summary"]     = r.get("company_summary") or ""
        rec["Revenue Composition"] = r.get("revenue_composition") or ""
        rec["Status"]              = r.get("status") or ""
        rec["Comments"]        = r.get("comments") or ""
        rec["technical +ve"]   = r.get("tech_pos") or ""
        rec["fundamental +ve"] = r.get("fund_pos") or ""
        rec["technical -ve"]   = r.get("tech_neg") or ""
        rec["fundamental -ve"] = r.get("fund_neg") or ""

        # Rightmost: Mkt Cap, Sector, Industry, Next Earnings, Close, Change %, Last Close Date
        ticker = r.get("ticker", "")
        rec["Mkt Cap ($B)"] = _mkt_cap_b(r.get("market_cap"))
        sector, industry = si.get(ticker, ("N/A", "N/A"))
        rec["Sector"]          = sector
        rec["Industry"]        = industry
        name, desc = cm.get(ticker, ("N/A", "N/A"))
        rec["Company Name"]        = name
        rec["Company Description"] = desc
        rec["Next Earnings Date"] = ne.get(ticker, "N/A")
        rec["Next Earnings Time"] = net.get(ticker, "N/A")
        tc = tm.get(ticker, {})
        rec["Close"]           = tc.get("close")
        rec["Change %"]        = tc.get("daily_pct_change")
        rec["Last Close Date"] = str(tc.get("as_of_date") or "N/A")

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
    disp_to_db["Comments"]        = "comments"
    disp_to_db["Status"]          = "status"
    disp_to_db["Company Summary"]     = "company_summary"
    disp_to_db["Revenue Composition"] = "revenue_composition"
    disp_to_db["technical +ve"]       = "tech_pos"
    disp_to_db["fundamental +ve"] = "fund_pos"
    disp_to_db["technical -ve"]   = "tech_neg"
    disp_to_db["fundamental -ve"] = "fund_neg"

    for ticker, row in edited_df.iterrows():
        # ticker comes from the DataFrame index (set_index("Ticker"))
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
            if col in ("Datetime", "Score"):
                continue
            db_col = disp_to_db.get(col)
            if not db_col:
                continue

            new_val = row[col]
            # Convert emoji back to DB string for indicator cols
            if col in MAIN_IND_COLS or col in SUB_DISPLAY.values():
                new_val = EMOJI_TO_DB.get(str(new_val), str(new_val))

            update_field(analysis_dt, ticker, db_col, str(new_val) if new_val is not None else "")
            # Keep all rows for this ticker in sync when comments or status change
            if db_col == "comments":
                save_comment_for_ticker(str(ticker), str(new_val) if new_val is not None else "")
            elif db_col == "status":
                save_status_for_ticker(str(ticker), str(new_val) if new_val is not None else "")
            elif db_col in ("company_summary", "revenue_composition", "tech_pos", "fund_pos", "tech_neg", "fund_neg"):
                save_user_field_for_ticker(str(ticker), db_col, str(new_val) if new_val is not None else "")


# ─────────────────────────────────────────────────────────────────────────────
# Column config for data_editor
# ─────────────────────────────────────────────────────────────────────────────

def make_column_config(df: pd.DataFrame) -> dict:
    config = {}
    for col in df.columns:
        if col == "#":
            config[col] = st.column_config.NumberColumn("#", format="%d", disabled=True)
        elif col in ("Ticker", "Datetime"):
            config[col] = st.column_config.TextColumn(col, disabled=True)
        elif col in MAIN_IND_COLS or col in SUB_DISPLAY.values():
            config[col] = st.column_config.SelectboxColumn(
                col, options=EMOJI_OPTIONS, required=True
            )
        elif col == "Score":
            config[col] = st.column_config.NumberColumn("Score", format="%.1f", disabled=True)
        elif col == "Status":
            config[col] = st.column_config.SelectboxColumn(
                "Status", options=STATUS_OPTIONS, required=False
            )
        elif col == "Comments":
            config[col] = st.column_config.TextColumn("Comments", width="large")
        elif col in ("Company Summary", "Revenue Composition"):
            config[col] = st.column_config.TextColumn(col, width="medium")
        elif col in ("technical +ve", "fundamental +ve", "technical -ve", "fundamental -ve"):
            config[col] = st.column_config.TextColumn(col, width="medium")
        elif col == "Mkt Cap ($B)":
            config[col] = st.column_config.NumberColumn(
                "Mkt Cap ($B)", format="%.2f", disabled=True
            )
        elif col in ("Sector", "Industry", "Next Earnings Date", "Next Earnings Time", "Last Close Date"):
            config[col] = st.column_config.TextColumn(col, disabled=True)
        elif col == "Company Name":
            config[col] = st.column_config.TextColumn("Company Name", width="medium", disabled=True)
        elif col == "Company Description":
            config[col] = st.column_config.TextColumn("Company Description", width="large", disabled=True)
        elif col == "Close":
            config[col] = st.column_config.NumberColumn("Close", format="%.2f", disabled=True)
        elif col == "Change %":
            config[col] = st.column_config.NumberColumn("Change %", format="%.2f", disabled=True)
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
        failures = prog.get("failures", {})
        st.success(f"✅ Scan complete — {done} tickers processed"
                   + (f" | ⚠️ {len(failures)} with missing/failed data" if failures else ""))
        if failures:
            with st.expander(f"⚠️ Data issues ({len(failures)} tickers)", expanded=False):
                for tkr, info in sorted(failures.items()):
                    reason  = info.get("reason", "unknown")
                    missing = info.get("missing", [])
                    st.markdown(f"**{tkr}** — {reason}"
                                + (f"  \n&nbsp;&nbsp;&nbsp;&nbsp;missing: `{', '.join(missing)}`"
                                   if missing else ""))
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


# ─────────────────────────────────────────────────────────────────────────────
# AI Detailed Analysis
# ─────────────────────────────────────────────────────────────────────────────

AI_MODEL       = "gemini-3.1-pro-preview"
AI_MAX_TICKERS = 50
AI_MAX_WORKERS = 3

_ss("ai_progress",     None)   # shared progress dict (written by bg thread)
_ss("ai_cancel_event", None)   # threading.Event to cancel
_ss("ai_thread",       None)   # background Thread


def _launch_ai_analysis(tickers: list[str]) -> None:
    """Start a background thread to run AI analysis on up to AI_MAX_TICKERS tickers."""
    from ai_analyzer import synthesize_analysis  # lazy import

    run_dt      = _now_cst()
    cancel_evt  = threading.Event()
    error_evt   = threading.Event()   # set when any error occurs → stop new tasks
    lock        = threading.Lock()
    progress: dict = {
        "done":       0,
        "total":      len(tickers),
        "active":     [],
        "run_dt":     run_dt,
        "errors":     [],       # list of "TICKER: message" strings
        "status":     "running",
    }

    def _analyze_one(ticker: str) -> None:
        # Skip if cancelled or a prior task errored
        if cancel_evt.is_set() or error_evt.is_set():
            with lock:
                progress["done"] += 1
            return
        with lock:
            progress["active"].append(ticker)
        report_status = "complete"
        try:
            report = synthesize_analysis(ticker, model=AI_MODEL)
        except Exception as e:
            err_msg = str(e)
            report  = f"Error generating analysis: {err_msg}"
            report_status = "error"
            with lock:
                progress["errors"].append(f"{ticker}: {err_msg}")
            error_evt.set()   # stop dispatching new tasks
        try:
            storage.save_ai_report(run_dt, ticker, report, AI_MODEL, status=report_status)
        except Exception:
            pass
        with lock:
            progress["done"] += 1
            if ticker in progress["active"]:
                progress["active"].remove(ticker)

    def _run_all() -> None:
        with concurrent.futures.ThreadPoolExecutor(max_workers=AI_MAX_WORKERS) as ex:
            futures = [ex.submit(_analyze_one, t) for t in tickers]
            for f in concurrent.futures.as_completed(futures):
                try:
                    f.result()
                except Exception:
                    pass
        with lock:
            progress["status"] = "done"

    st.session_state.ai_cancel_event = cancel_evt
    st.session_state.ai_progress     = progress
    t = threading.Thread(target=_run_all, daemon=True)
    st.session_state.ai_thread = t
    t.start()


@st.fragment(run_every=2)
def _ai_progress_autorefresh():
    """Auto-refreshes AI analysis progress every 2 s."""
    progress = st.session_state.get("ai_progress")
    if not progress:
        return

    status  = progress.get("status", "running")
    done    = progress.get("done", 0)
    total   = progress.get("total", 1)
    active  = progress.get("active", [])
    run_dt  = progress.get("run_dt", "")
    errors  = progress.get("errors", [])

    if status == "done":
        # When done: show a refresh button instead of auto-rerunning the full app,
        # which would clear all widget states (unsaved comments, filter thresholds, etc.)
        if not progress.get("shown"):
            progress["shown"] = True
        if errors:
            st.warning(f"⚠️ AI analysis finished with errors — {done} ticker(s) processed | Run: {run_dt}")
            with st.expander("Error details", expanded=True):
                for err_line in errors:
                    st.code(err_line)
        else:
            st.success(f"✅ AI analysis complete — {done} ticker(s) | Run: {run_dt}")
        if st.button("Clear", key="ai_clear_status"):
            st.session_state.ai_progress = None
            st.rerun(scope="app")
        return

    frac       = done / total if total else 0
    active_str = ", ".join(active) if active else "queuing…"
    is_stopped = bool(errors)   # error_evt fired → tasks being skipped
    status_txt = (f"🛑 Error — waiting for active tasks ({done}/{total})" if is_stopped
                  else f"🤖 AI analyzing… {done}/{total} complete | Active: {active_str}")
    st.progress(frac, text=status_txt)
    if errors and not is_stopped:
        pass  # already shown above
    if st.button("⏹ Cancel AI Analysis", key="ai_cancel_btn"):
        evt = st.session_state.get("ai_cancel_event")
        if evt:
            evt.set()


@st.dialog("Confirm AI Analysis")
def _ai_confirm_dialog(tickers: list[str]) -> None:
    n       = len(tickers)
    preview = ", ".join(tickers[:15]) + ("…" if n > 15 else "")
    st.markdown(
        f"**Run AI analysis on {n} ticker(s)?**\n\n"
        f"{preview}\n\n"
        f"Uses Gemini ({AI_MODEL}) with live web search. "
        f"Up to {AI_MAX_WORKERS} tickers run in parallel.",
    )
    c1, c2 = st.columns(2)
    with c1:
        if st.button("✅ Confirm", type="primary", width="stretch"):
            _launch_ai_analysis(tickers)
            del st.session_state["ai_confirm_pending"]
            st.rerun()
    with c2:
        if st.button("❌ Cancel", width="stretch"):
            del st.session_state["ai_confirm_pending"]
            st.rerun()




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
        if col in _COL_FILTER_EMOJI or col in _COL_FILTER_TEXT_CAT:
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
            cols = st.session_state.pop(col_cols_key)
            st.session_state[f"val_cols_{tab_key}"] = cols
            st.session_state[f"_val_cols_shadow_{tab_key}"] = cols


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
        if col in _COL_FILTER_EMOJI or col in _COL_FILTER_TEXT_CAT:
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
            elif col in _COL_FILTER_TEXT_CAT:
                st.multiselect(
                    f"{col}",
                    options=_COL_FILTER_TEXT_CAT[col],
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
        if col in _COL_FILTER_EMOJI or col in _COL_FILTER_TEXT_CAT:
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

        # Load / Set default / Rename / Delete existing groups
        if group_names:
            gg1, gg2, gg3 = st.columns([3, 1, 1])
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
            rn1, rn2, rn3 = st.columns([3, 1, 1])
            with rn1:
                filt_new_name = st.text_input(
                    "Rename to:",
                    key=f"filt_rename_val_{tab_key}",
                    label_visibility="collapsed",
                    placeholder="New name…",
                )
            with rn2:
                if st.button("Rename", key=f"filt_rename_{tab_key}"):
                    new_n = filt_new_name.strip()
                    if new_n and new_n != sel_group:
                        fg[new_n] = fg.pop(sel_group)
                        if prefs.get("filter_default", {}).get(tab_key) == sel_group:
                            prefs["filter_default"][tab_key] = new_n
                        _save_prefs(prefs)
                        st.rerun()
            with rn3:
                if st.button("Delete", key=f"filt_del_{tab_key}"):
                    fg.pop(sel_group, None)
                    if prefs.get("filter_default", {}).get(tab_key) == sel_group:
                        prefs["filter_default"].pop(tab_key, None)
                    _save_prefs(prefs)
                    st.rerun()
        else:
            st.caption("No saved groups yet.")

    # ── Action buttons (after manage groups) ──────────────────────────────────
    bc1, bc2 = st.columns(2)
    with bc1:
        if st.button("Clear all filters", key=f"filt_clear_{tab_key}"):
            _queue_filter_clear(tab_key)
            st.rerun()
    with bc2:
        if st.button("Reset to default", key=f"filt_reset_{tab_key}"):
            _queue_filter_clear(tab_key)
            st.rerun()

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
        if sval in ("N/A", "", "None", "nan"):
            return False
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
                     fund_map: dict, tech_map: dict,
                     earnings_map: dict | None = None) -> list[str]:
    """Return subset of tickers whose value-table records pass all column filters."""
    if not col_filter:
        return tickers
    em = earnings_map or {}
    out = []
    for t in tickers:
        rec = _build_value_record(
            t,
            detail_map.get(t, {}),
            rows_by_ticker.get(t, {}),
            fund_map.get(t, {}),
            tech_map.get(t, {}),
            em.get(t),
        )
        if all(_col_filter_passes(rec, col, spec) for col, spec in col_filter.items()):
            out.append(t)
    return out


def _value_col_config(cols: list[str]) -> dict:
    """Build st.column_config for the value table based on column names."""
    cfg: dict = {}
    pct_suffix = {"%"}
    for col in cols:
        if col == "#":
            cfg[col] = st.column_config.NumberColumn("#", format="%d", disabled=True)
        elif col == "Ticker":
            cfg[col] = st.column_config.TextColumn(col, disabled=True)
        elif col in ("Sector", "Industry", "Q End Date", "A End Date",
                     "Next Earnings Date", "Last Close Date",
                     "Breakout 55D", "Breakout 3M", "Finalized",
                     "MA10>20", "MA20>50", "MA50>150", "MA150>200",
                     "MA10>MA20", "MA20>MA50", "MA50>MA150", "MA150>MA200"):
            cfg[col] = st.column_config.TextColumn(col, disabled=True)
        elif col == "Score":
            cfg[col] = st.column_config.NumberColumn(col, format="%.1f", disabled=True)
        elif col == "Comments":
            try:
                cfg[col] = st.column_config.TextColumn("Comments", width="large", wrap_text=True)
            except TypeError:
                cfg[col] = st.column_config.TextColumn("Comments", width="large")
        elif col in ("Company Summary", "Revenue Composition"):
            cfg[col] = st.column_config.TextColumn(col, width="medium")
        elif col in ("technical +ve", "fundamental +ve", "technical -ve", "fundamental -ve"):
            cfg[col] = st.column_config.TextColumn(col, width="medium")
        elif col in ("Last Earnings Date", "Last Earnings Time",
                     "Next Earnings Time"):
            cfg[col] = st.column_config.TextColumn(col, disabled=True)
        elif col == "Company Name":
            cfg[col] = st.column_config.TextColumn("Company Name", width="medium", disabled=True)
        elif col == "Company Description":
            cfg[col] = st.column_config.TextColumn("Company Description", width="large", disabled=True)
        elif col == "Status":
            cfg[col] = st.column_config.SelectboxColumn(
                "Status", options=STATUS_OPTIONS, required=False, disabled=False
            )
        elif col in ("Earns 1D Px%", "EPS Sur", "EPS GAAP Sur", "Rev Sur"):
            cfg[col] = st.column_config.NumberColumn(col, format="%.2f%%", disabled=True)
        elif col in ("EPS Est", "EPS Act", "EPS GAAP Est", "EPS GAAP Act"):
            cfg[col] = st.column_config.NumberColumn(col, format="%.2f", disabled=True)
        elif col in ("Rev Est ($M)", "Rev Act ($M)"):
            cfg[col] = st.column_config.NumberColumn(col, format="%.1f", disabled=True)
        elif col == "Mkt Cap ($B)":
            cfg[col] = st.column_config.NumberColumn(col, format="%.2f", disabled=True)
        elif col == "Volume":
            cfg[col] = st.column_config.NumberColumn(col, format="%d", disabled=True)
        elif col in (
            "Short % Float (Y)", "Short % Float (Calc)",
            "Short % Out (Y)", "Short % Out (Calc)", "Short % Impl Out",
            "Short MoM Chg%", "Ins Buy %", "Ins Sell %", "Ins Net %",
            "Gross Margin%", "EBITDA Margin%", "Op Margin%", "Net Margin%",
            "ROE%", "ROA%", "Price vs Target%",
        ):
            cfg[col] = st.column_config.NumberColumn(col, format="%.2f%%", disabled=True)
        elif col in (
            "Shares Short (M)", "Float Shares (M)", "Shares Out (M)", "Avg Vol (M)",
            "Ins Buy Shares (M)", "Ins Sell Shares (M)", "Ins Net Shares (M)",
            "Target Median", "Target High", "Target Low", "Target Mean",
            "Days to Cover", "D/E Ratio", "Rec Score",
            "Current Ratio", "Quick Ratio",
        ):
            cfg[col] = st.column_config.NumberColumn(col, format="%.2f", disabled=True)
        elif col in ("Ins Buy #", "Ins Sell #", "Analyst Count"):
            cfg[col] = st.column_config.NumberColumn(col, format="%d", disabled=True)
        elif col in ("Short Interest Date", "Rec Key"):
            cfg[col] = st.column_config.TextColumn(col, disabled=True)
        elif col.endswith("%"):
            cfg[col] = st.column_config.NumberColumn(col, format="%.2f", disabled=True)
        else:
            cfg[col] = st.column_config.NumberColumn(col, format="%.4g", disabled=True)
    return cfg


def render_value_table(tickers: list[str], detail_map: dict,
                       rows_by_ticker: dict, fund_map: dict,
                       tab_key: str, tech_map: dict | None = None,
                       sort_col: str | None = None,
                       pre_built_records: dict | None = None,
                       pre_built_earnings: dict | None = None,
                       rank_map: dict | None = None,
                       returns_map: dict | None = None):
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

    # ── Shadow-key restore: protect col selection from Streamlit widget-key
    # cleanup that can occur when st.rerun() fires before this widget renders
    # (e.g. from render_indicator_filter which runs above render_value_table).
    _shadow_key = f"_val_cols_shadow_{tab_key}"
    if f"val_cols_{tab_key}" not in st.session_state and _shadow_key in st.session_state:
        st.session_state[f"val_cols_{tab_key}"] = list(st.session_state[_shadow_key])

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
            st.session_state[_shadow_key] = list(all_cols)
    with vb2:
        if st.button("Clear all columns", key=f"val_clear_{tab_key}"):
            st.session_state[f"val_groups_{tab_key}"] = []
            st.session_state[f"val_cols_{tab_key}"] = []
            st.session_state[_shadow_key] = []
    with vb3:
        if st.button("Reset to default", key=f"val_reset_{tab_key}"):
            st.session_state[f"val_groups_{tab_key}"] = DEFAULT_VALUE_GROUPS
            st.session_state[f"val_cols_{tab_key}"] = []
            st.session_state[_shadow_key] = []

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
    # Keep shadow in sync with current multiselect value so future restores are correct
    st.session_state[_shadow_key] = list(sel_cols)

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
            cc1, cc2, cc3 = st.columns([3, 1, 1])
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
            crn1, crn2, crn3 = st.columns([3, 1, 1])
            with crn1:
                col_new_name = st.text_input(
                    "Rename to:",
                    key=f"col_rename_val_{tab_key}",
                    label_visibility="collapsed",
                    placeholder="New name…",
                )
            with crn2:
                if st.button("Rename", key=f"col_rename_{tab_key}"):
                    new_cn = col_new_name.strip()
                    if new_cn and new_cn != sel_cg:
                        cg_store = prefs.setdefault("col_groups", {})
                        cg_store[new_cn] = cg_store.pop(sel_cg)
                        if prefs.get("col_default", {}).get(tab_key) == sel_cg:
                            prefs["col_default"][tab_key] = new_cn
                        _save_prefs(prefs)
                        st.rerun()
            with crn3:
                if st.button("Delete", key=f"col_del_{tab_key}"):
                    prefs.get("col_groups", {}).pop(sel_cg, None)
                    if prefs.get("col_default", {}).get(tab_key) == sel_cg:
                        prefs["col_default"].pop(tab_key, None)
                    _save_prefs(prefs)
                    st.rerun()
        else:
            st.caption("No saved column groups yet.")

    _fixed_right = {"Mkt Cap ($B)", "Sector", "Industry", "Company Name", "Company Description",
                    "Next Earnings Date", "Next Earnings Time",
                    "Last Earnings Date", "Last Earnings Time", "Earns 1D Px%"}
    data_cols = [c for c in (sel_cols if sel_cols else group_cols)
                 if c not in _fixed_right and c != "Score" and c != "Status" and c != "Comments"
                 and c not in _USER_NOTE_COLS]
    # Auto-inject sort col if not already visible and it's a value-table column
    if (sort_col and sort_col not in data_cols
            and sort_col not in _fixed_right
            and sort_col not in ("Score", "Comments", "#", "Ticker")
            and sort_col not in _USER_NOTE_COLS):
        data_cols = [sort_col] + data_cols
    # Order: Ticker | # | Score | data columns | Company Summary | Status | Comments | note cols | fixed-right columns
    show_cols = (
        ["Ticker", "#", "Score"] + data_cols
        + ["Company Summary", "Revenue Composition", "Status", "Comments",
           "technical +ve", "fundamental +ve", "technical -ve", "fundamental -ve",
           "Mkt Cap ($B)", "Sector", "Industry", "Company Name", "Company Description",
           "Next Earnings Date", "Next Earnings Time",
           "Last Earnings Date", "Last Earnings Time", "Earns 1D Px%"]
    )

    # ── Build records ────────────────────────────────────────────────────────
    if pre_built_records is None:
        earnings_map = pre_built_earnings or (get_latest_earnings_for_tickers(tickers) if tickers else {})
    else:
        earnings_map = pre_built_earnings or {}
    records = []
    for ticker in tickers:
        if pre_built_records is not None and ticker in pre_built_records:
            rec = pre_built_records[ticker]
        else:
            detail = detail_map.get(ticker, {})
            row    = rows_by_ticker.get(ticker, {})
            f_db   = fund_map.get(ticker, {})
            tc     = tm.get(ticker, {})
            rm = returns_map or {}
            rec    = _build_value_record(ticker, detail, row, f_db, tc,
                                         earnings_map.get(ticker), rm.get(ticker))
        records.append({c: rec.get(c) for c in show_cols})

    if records:
        # ── Build + assign # (order comes from caller's pre-sorted tickers list) ──
        ordered_cols = [c for c in show_cols if c != "Ticker"]
        df = pd.DataFrame(records)[show_cols].set_index("Ticker")
        if rank_map:
            df["#"] = [rank_map.get(t, i + 1) for i, t in enumerate(df.index)]
        else:
            df["#"] = range(1, len(df) + 1)

        st.caption(f"{len(df)} rows")
        col_cfg = _value_col_config(ordered_cols)
        try:
            val_edited = st.data_editor(
                df, column_config=col_cfg, column_order=ordered_cols,
                width="stretch", hide_index=False,
                key=f"val_editor_{tab_key}", row_height=80,
            )
        except TypeError:
            val_edited = st.data_editor(
                df, column_config=col_cfg, column_order=ordered_cols,
                width="stretch", hide_index=False,
                key=f"val_editor_{tab_key}",
            )
        if st.button("💾 Save", key=f"save_val_comments_{tab_key}"):
            _user_note_map = {
                "Company Summary":     "company_summary",
                "Revenue Composition": "revenue_composition",
                "technical +ve":       "tech_pos",
                "fundamental +ve": "fund_pos",
                "technical -ve":   "tech_neg",
                "fundamental -ve": "fund_neg",
            }
            for ticker, vrow in val_edited.iterrows():
                new_comment = str(vrow.get("Comments") or "")
                orig_comment = str(df.loc[ticker, "Comments"]) if ticker in df.index else ""
                if new_comment != orig_comment:
                    save_comment_for_ticker(str(ticker), new_comment)
                new_status = str(vrow.get("Status") or "")
                orig_status = str(df.loc[ticker, "Status"]) if ticker in df.index else ""
                if new_status != orig_status:
                    save_status_for_ticker(str(ticker), new_status)
                for disp_col, db_col in _user_note_map.items():
                    new_val = str(vrow.get(disp_col) or "")
                    orig_val = str(df.loc[ticker, disp_col]) if ticker in df.index else ""
                    if new_val != orig_val:
                        save_user_field_for_ticker(str(ticker), db_col, new_val)
            st.success("Saved.")
            st.rerun()
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
    st.header("📊 Market Scan")
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

    run_btn      = st.button("▶ Run Market Scan", type="primary", width="stretch")
    run_unseen_btn = st.button(
        "▶ Run Unseen Today",
        width="stretch",
        help="Scans only tickers whose last run was before today 3 pm CST (not yet run today after market close).",
    )
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
F5 Fwd PE ≤ Peer Median  
F6 P/B ≤ Peer Median  
    """)

    st.divider()
    st.markdown("**🤖 AI Analysis**")
    ai_sidebar_input = st.text_area(
        "Tickers (comma-separated)",
        placeholder="MU, SNDK, LITE",
        key="ai_sidebar_tickers",
        height=80,
        label_visibility="collapsed",
    )
    if st.button("Run AI Analysis", key="ai_sidebar_run", width="stretch"):
        raw_ai = ai_sidebar_input.strip()
        if not raw_ai:
            st.error("Enter at least one ticker.")
        else:
            ai_tickers = [t.strip().upper() for t in raw_ai.split(",") if t.strip()]
            if len(ai_tickers) > AI_MAX_TICKERS:
                st.error(f"Max {AI_MAX_TICKERS} tickers. You entered {len(ai_tickers)}.")
            else:
                st.session_state["ai_confirm_pending"] = {"tickers": ai_tickers}
                st.rerun()

# ── Handle Run buttons ────────────────────────────────────────────────────────

def _launch_scan(ticker_list: list[str], label: str) -> None:
    """Start a scan thread for the given ticker list."""
    if not ticker_list:
        st.sidebar.error("No tickers to process. Enter tickers or check tickers.txt.")
        return
    daily_str   = str(daily_date_input)  if daily_date_input  else None
    weekly_str  = str(weekly_date_input) if weekly_date_input else None
    analysis_dt = _now_cst()
    fetch_peers = st.session_state.get("fetch_peers", True)
    vpn_rotate  = st.session_state.get("vpn_rotate",  False)

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


if run_btn:
    raw = ticker_input.strip()
    if raw:
        ticker_list = [t.strip().upper() for t in raw.split(",") if t.strip()]
        label = f"Analyzing {len(ticker_list)} ticker(s): {', '.join(ticker_list)}"
    else:
        ticker_list = load_ticker_list()
        label = f"Scan started — {len(ticker_list)} tickers"
    _launch_scan(ticker_list, label)

if run_unseen_btn:
    # Build cutoff: today at 15:00 CST in the same format as run_dt
    _cutoff = datetime.now(CST).replace(hour=15, minute=0, second=0, microsecond=0)
    cutoff_str = _cutoff.strftime("%Y-%m-%d %H:%M:%S CST")
    already_done = storage.get_tickers_run_since(cutoff_str)

    raw = ticker_input.strip()
    if raw:
        all_tickers = [t.strip().upper() for t in raw.split(",") if t.strip()]
    else:
        all_tickers = load_ticker_list()

    ticker_list = [t for t in all_tickers if t not in already_done]
    if not ticker_list:
        st.sidebar.info("All tickers already scanned today after 3 pm CST.")
    else:
        label = (
            f"Unseen scan — {len(ticker_list)} tickers "
            f"({len(already_done)} already run today skipped)"
        )
        _launch_scan(ticker_list, label)

# ─────────────────────────────────────────────────────────────────────────────
# Scan progress (fragment — auto-refreshes every 2 s without full-page rerun)
# ─────────────────────────────────────────────────────────────────────────────
_scan_progress_autorefresh()

# ─────────────────────────────────────────────────────────────────────────────
# AI Analysis progress (fragment — auto-refreshes every 2 s)
# ─────────────────────────────────────────────────────────────────────────────
_ai_progress_autorefresh()

# ─────────────────────────────────────────────────────────────────────────────
# AI Analysis confirmation dialog (modal — freezes page)
# ─────────────────────────────────────────────────────────────────────────────
if "ai_confirm_pending" in st.session_state:
    _ai_confirm_dialog(st.session_state["ai_confirm_pending"]["tickers"])

# ─────────────────────────────────────────────────────────────────────────────
# Apply any pending filter/column ops (must run before any filter widgets render)
# ─────────────────────────────────────────────────────────────────────────────
_process_pending_ops()

# ─────────────────────────────────────────────────────────────────────────────
# Tabs
# ─────────────────────────────────────────────────────────────────────────────
tab_history, tab_ai = st.tabs(["📊 Market Scan", "🤖 AI Analysis"])


# ══════════════════════════════════════════════════════════════════════════════
# TAB 1: Market Scan
# ══════════════════════════════════════════════════════════════════════════════

with tab_history:
    all_dts = get_all_run_datetimes()

    if not all_dts:
        st.info("No historical data yet.")
        st.stop()

    # ── Market Scan Summary ───────────────────────────────────────────────────
    st.markdown("### 📊 Market Scan")
    st.caption("Rows sorted: newest datetime first, then alphabetical ticker")

    # Pre-read widget states for data fetching before widget rendering
    _f_tickers_pre   = st.session_state.get("hist_f_tick", [])
    _f_dts_pre       = st.session_state.get("hist_f_dt", [])
    _only_latest_pre = st.session_state.get("hist_only_latest", True)   # default True
    _show_sub_pre    = st.session_state.get("all_queries_show_sub", False)
    _f_sectors_pre   = st.session_state.get("hist_f_sector", [])
    _mc_lo_pre       = st.session_state.get("hist_mc_lo", None)
    _mc_hi_pre       = st.session_state.get("hist_mc_hi", None)
    _cust_start_pre  = st.session_state.get("hist_cust_start", "").strip()
    _cust_end_pre    = st.session_state.get("hist_cust_end", "").strip()

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
    net_map_hist      = _extract_net(hist_fund_map)
    company_map_hist  = _extract_company(hist_fund_map)

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

    # ── Row 2: Mkt cap + custom period dates ──────────────────────────────────
    mc1, mc2, cp1, cp2 = st.columns(4)
    with mc1:
        mc_lo_h = st.number_input("Mkt Cap min ($B)", value=None, min_value=0.0,
                                  placeholder="no min", key="hist_mc_lo",
                                  format="%.2f", step=1.0)
    with mc2:
        mc_hi_h = st.number_input("Mkt Cap max ($B)", value=None, min_value=0.0,
                                  placeholder="no max", key="hist_mc_hi",
                                  format="%.2f", step=1.0)
    with cp1:
        _cust_start_val = st.text_input(
            "Custom Period Start (YYYY-MM-DD)", value="",
            placeholder="e.g. 2024-01-01", key="hist_cust_start",
        ).strip()
    with cp2:
        _cust_end_val = st.text_input(
            "Custom Period End (YYYY-MM-DD)", value="",
            placeholder="e.g. 2024-12-31", key="hist_cust_end",
        ).strip()

    # ── Indicator filter — always visible ─────────────────────────────────────
    hist_ind_filter, hist_col_filter = render_indicator_filter("history")

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
        _hist_earnings_map = get_latest_earnings_for_tickers(
            [r["ticker"] for r in filt_rows]
        ) if filt_rows else {}
        _hist_pass = set(apply_col_filter(
            [r["ticker"] for r in filt_rows], hist_col_filter, hist_detail_map,
            hist_rows_by_ticker_all, hist_fund_map, hist_tech_map,
            earnings_map=_hist_earnings_map,
        ))
        filt_rows = [r for r in filt_rows if r["ticker"] in _hist_pass]

    # ── Pre-build value records for all-column sort support ───────────────────
    _pre_rows_by_ticker = {r["ticker"]: r for r in filt_rows}
    _pre_earnings_map   = get_latest_earnings_for_tickers(list(_pre_rows_by_ticker)) if filt_rows else {}

    # ── Price-history returns (spot + rolling avg) ────────────────────────────
    _ph_tickers = list(_pre_rows_by_ticker.keys())
    _ph_returns: dict[str, dict] = storage.compute_returns_for_tickers(_ph_tickers) if _ph_tickers else {}

    # ── Custom period returns ─────────────────────────────────────────────────
    from datetime import date as _date, timedelta as _td
    _today_str = _date.today().isoformat()
    # Default when blank: 22 trading days back (same reference as "1M Avg Px%")
    _cust_s = _cust_start_pre or storage.get_nth_trading_day_back(_ph_tickers, 22) or (
        _date.today() - _td(days=31)
    ).isoformat()
    _cust_e = _cust_end_pre or _today_str
    _cust_returns = storage.get_custom_period_returns(_ph_tickers, _cust_s, _cust_e) if _ph_tickers else {}
    for _t, _cr in _cust_returns.items():
        _ph_returns.setdefault(_t, {}).update(_cr)

    _pre_val_records: dict[str, dict] = {
        t: _build_value_record(
            t,
            hist_detail_map.get(t, {}),
            _pre_rows_by_ticker[t],
            hist_fund_map.get(t, {}),
            hist_tech_map.get(t, {}),
            _pre_earnings_map.get(t),
            _ph_returns.get(t),
        )
        for t in _pre_rows_by_ticker
    }

    all_df = build_summary_df(filt_rows, show_sub=_show_sub_pre,
                              include_datetime=True, si_map=si_map_hist,
                              ne_map=ne_map_hist, net_map=net_map_hist,
                              company_map=company_map_hist,
                              tech_map=hist_tech_map)

    # ── Sort controls + Find Ranking (shared: applies to both tables) ─────────
    _sort_rank = pd.Series(dtype=int)  # default; populated below when df non-empty
    if not all_df.empty:
        sc1, sc2, sc3 = st.columns([2, 1, 2])
        with sc1:
            _sort_col = st.selectbox(
                "Sort by", _FILTERABLE_COLS,
                index=_FILTERABLE_COLS.index("Score") if "Score" in _FILTERABLE_COLS else 0,
                key="sort_col_history",
            )
        with sc2:
            _sort_asc = st.radio(
                "Order", ["Desc", "Asc"], key="sort_dir_history", horizontal=True,
            ) == "Asc"
        with sc3:
            _find_ticker = st.text_input(
                "Find ranking", placeholder="e.g. AAPL", key="find_rank_history",
            ).upper().strip()

        # Sort using pre-built records (covers all value-table columns)
        _sort_vals = pd.Series({t: _pre_val_records.get(t, {}).get(_sort_col)
                                 for t in all_df["Ticker"]})
        # Treat "N/A" and empty strings as missing so they always sort last regardless of direction
        _sort_vals = _sort_vals.replace({"N/A": None, "": None})
        # Compute SQL RANK() (tied rows share same rank) before sorting
        _sort_rank = _sort_vals.rank(method="min", ascending=_sort_asc, na_option="bottom").astype(int)
        _sort_vals = _sort_vals.sort_values(ascending=_sort_asc, na_position="last")
        _sorted_tickers = list(_sort_vals.index)
        _t_order_map = {t: i for i, t in enumerate(_sorted_tickers)}
        all_df = (all_df
                  .assign(_si=all_df["Ticker"].map(_t_order_map))
                  .sort_values("_si")
                  .drop(columns=["_si"])
                  .reset_index(drop=True))

        # Auto-inject sort col into emoji table if not already present
        if _sort_col not in all_df.columns:
            all_df[_sort_col] = all_df["Ticker"].map(
                lambda t: _pre_val_records.get(t, {}).get(_sort_col)
            )

        all_df.insert(0, "#", all_df["Ticker"].map(_sort_rank))

        if _find_ticker:
            _match = all_df[all_df["Ticker"] == _find_ticker]
            if not _match.empty:
                st.info(f"**{_find_ticker}** is ranked **#{int(_match['#'].iloc[0])}** "
                        f"out of {len(all_df)} "
                        f"(sorted by {_sort_col} {'↑' if _sort_asc else '↓'})")
            else:
                st.warning(f"**{_find_ticker}** not found in current filtered results")
    else:
        _sort_col = st.session_state.get("sort_col_history", "Score")
        _sorted_tickers = []

    # ── Row 8: Show sub-indicators | Latest per ticker | Showing X rows ───────
    row8c1, row8c2, row8c3 = st.columns(3)
    with row8c1:
        all_show_sub = st.checkbox("Show sub-indicators", value=False,
                                   key="all_queries_show_sub")
    with row8c2:
        only_latest = st.checkbox("Latest entry per ticker only", value=True,
                                  key="hist_only_latest")
    with row8c3:
        st.caption(f"Showing **{len(filt_rows)}** / {total_before} rows")

    # CSS: make user-note cells honour newline characters (\n → visual line break)
    st.markdown(
        """<style>
        .stDataEditor [col-id="Comments"] .ag-cell-value,
        .stDataEditor [col-id="Comments"] .ag-group-value,
        .stDataEditor [col-id="Company Summary"] .ag-cell-value,
        .stDataEditor [col-id="Company Summary"] .ag-group-value,
        .stDataEditor [col-id="Revenue Composition"] .ag-cell-value,
        .stDataEditor [col-id="Revenue Composition"] .ag-group-value,
        .stDataEditor [col-id="technical +ve"] .ag-cell-value,
        .stDataEditor [col-id="technical +ve"] .ag-group-value,
        .stDataEditor [col-id="fundamental +ve"] .ag-cell-value,
        .stDataEditor [col-id="fundamental +ve"] .ag-group-value,
        .stDataEditor [col-id="technical -ve"] .ag-cell-value,
        .stDataEditor [col-id="technical -ve"] .ag-group-value,
        .stDataEditor [col-id="fundamental -ve"] .ag-cell-value,
        .stDataEditor [col-id="fundamental -ve"] .ag-group-value {
            white-space: pre-wrap !important;
        }
        </style>""",
        unsafe_allow_html=True,
    )

    all_df_disp = all_df.set_index("Ticker") if "Ticker" in all_df.columns else all_df
    all_col_cfg = make_column_config(all_df_disp)

    all_edited = st.data_editor(
        all_df_disp, column_config=all_col_cfg,
        width="stretch", hide_index=False,
        key="all_sum_editor",
    )
    if st.button("💾 Save", key="save_all"):
        save_edits(filt_rows, all_edited, include_datetime=True)
        st.success("Saved.")
        st.rerun()

    # AI Analysis button for filtered tickers
    _hist_ai_tickers = list(dict.fromkeys(r["ticker"] for r in filt_rows))
    _n_hist_ai = len(_hist_ai_tickers)
    if _n_hist_ai > 0:
        if st.button(
            f"🤖 Run AI Analysis on {_n_hist_ai} filtered ticker(s)",
            key="ai_run_history_btn",
        ):
            if _n_hist_ai > AI_MAX_TICKERS:
                st.error(f"Too many tickers ({_n_hist_ai}). Max is {AI_MAX_TICKERS}.")
            else:
                st.session_state["ai_confirm_pending"] = {"tickers": _hist_ai_tickers}
                st.rerun()

    st.markdown("---")

    # ── Value Table ───────────────────────────────────────────────────────────
    # Use sorted order from emoji table so both tables share the same ranking
    hist_tickers = (
        _sorted_tickers if _sorted_tickers
        else [r["ticker"] for r in filt_rows]
    )
    render_value_table(hist_tickers, hist_detail_map,
                       _pre_rows_by_ticker, hist_fund_map, "history",
                       tech_map=hist_tech_map,
                       sort_col=_sort_col,
                       pre_built_records=_pre_val_records,
                       pre_built_earnings=_pre_earnings_map,
                       rank_map=dict(_sort_rank) if not _sort_rank.empty else None,
                       returns_map=_ph_returns)

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


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2: AI Analysis
# ══════════════════════════════════════════════════════════════════════════════

with tab_ai:
    st.markdown("### 🤖 AI Analysis")
    st.caption(
        "Reports are generated by Gemini with live web search. "
        "Use the sidebar or the Run AI Analysis buttons to start an analysis."
    )

    all_rpt_tickers = storage.get_ai_report_tickers()
    all_rpt_run_dts = storage.get_ai_report_run_dts()

    if not all_rpt_tickers:
        st.info("No AI reports yet. Run an analysis from the sidebar or the emoji table buttons.")
    else:
        ai_col1, ai_col2 = st.columns(2)
        with ai_col1:
            ai_f_tickers = st.multiselect(
                "Filter by ticker",
                options=all_rpt_tickers,
                key="ai_rpt_f_ticker",
            )
        with ai_col2:
            ai_f_dt_opts = ["All"] + all_rpt_run_dts
            ai_f_dt = st.selectbox(
                "Filter by run datetime",
                options=ai_f_dt_opts,
                key="ai_rpt_f_dt",
            )

        ai_cb1, ai_cb2 = st.columns(2)
        with ai_cb1:
            ai_completed_only = st.checkbox(
                "Show completed analysis only",
                value=True,
                key="ai_rpt_completed_only",
            )
        with ai_cb2:
            ai_latest_only = st.checkbox(
                "Latest entry per ticker only",
                value=False,
                key="ai_rpt_latest_only",
            )

        filter_dts = [ai_f_dt] if ai_f_dt != "All" else None
        reports = storage.get_ai_reports(
            tickers=ai_f_tickers or None,
            run_dts=filter_dts,
            status="complete" if ai_completed_only else None,
            latest_per_ticker=ai_latest_only,
        )

        if not reports:
            st.info("No reports match the current filter.")
        else:
            st.caption(f"Showing **{len(reports)}** report(s)")
            for rep in reports:
                rep_status = rep.get("status", "complete")
                status_icon = "✅" if rep_status == "complete" else "❌"
                hdr = (f"{status_icon} **{rep['ticker']}** — {rep['run_dt']}"
                       f"  ·  model: {rep['model'] or 'unknown'}")
                with st.expander(hdr, expanded=(len(reports) == 1)):
                    content = rep["report"] or "_No content returned._"
                    if rep_status == "error":
                        st.error(content)
                    else:
                        st.markdown(content)
