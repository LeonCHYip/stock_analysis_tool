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
    get_all_fundamentals_for_run,
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
        "Q Rev", "Q EPS", "Q Rev YoY%", "Q EPS YoY%",
    ],
    "Annual (F2/F4)": [
        "A Rev", "A EPS", "A Rev YoY%", "A EPS YoY%",
    ],
    "Valuation (F5/F6)": [
        "Fwd PE", "Fwd PE vs Med%", "P/B", "P/B vs Med%",
    ],
    "Fundamentals": [
        "Mkt Cap", "Sector", "Industry",
    ],
}
ALL_VALUE_COLS = [c for cols in VALUE_COL_GROUPS.values() for c in cols]
DEFAULT_VALUE_GROUPS = list(VALUE_COL_GROUPS.keys())

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


def _build_value_record(ticker: str, detail: dict, row: dict, f_db: dict) -> dict:
    """Build one value-table row from indicator detail JSON + DB fundamentals."""
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

    raw_info = {}
    rij = f_db.get("raw_info_json")
    if rij:
        try:
            raw_info = json.loads(rij)
        except Exception:
            pass

    def _bi(v):  # bool → icon
        if v is True:  return "✅"
        if v is False: return "❌"
        return "⚪️"

    return {
        "Ticker":          ticker,
        # T1 — daily comparisons
        "3M Daily Px%":   _pct(t1.get("3M Price Change %")),
        "3M Daily Vol%":  _pct(t1.get("3M Volume Change %")),
        "12M Daily Px%":  _pct(t1.get("12M Price Change %")),
        "12M Daily Vol%": _pct(t1.get("12M Volume Change %")),
        # T2 — weekly comparisons
        "3M Wkly Px%":    _pct(t2.get("3M Price Change %")),
        "3M Wkly Vol%":   _pct(t2.get("3M Volume Change %")),
        "12M Wkly Px%":   _pct(t2.get("12M Price Change %")),
        "12M Wkly Vol%":  _pct(t2.get("12M Volume Change %")),
        # T3 — MA booleans
        "MA10>20":        _bi(sub3.get("MA10>MA20")),
        "MA20>50":        _bi(sub3.get("MA20>MA50")),
        "MA50>150":       _bi(sub3.get("MA50>MA150")),
        "MA150>200":      _bi(sub3.get("MA150>MA200")),
        # T3 — MA values
        "MA10":           _num(t3.get("MA10")),
        "MA20":           _num(t3.get("MA20")),
        "MA50":           _num(t3.get("MA50")),
        "MA150":          _num(t3.get("MA150")),
        "MA200":          _num(t3.get("MA200")),
        # T4 — big move counts
        "# Up≥10%":       t4.get("Big Up Days Count", "N/A"),
        "# Dn≥10%":       t4.get("Big Down Days Count", "N/A"),
        # F1/F3 — quarterly
        "Q Rev":          _millify(f1.get("Q Revenue") or f_db.get("q_revenue")),
        "Q EPS":          _num(f1.get("Q EPS") or f_db.get("q_eps")),
        "Q Rev YoY%":     _pct(f3.get("Q Revenue YoY %") or f_db.get("q_rev_yoy")),
        "Q EPS YoY%":     _pct(f3.get("Q EPS YoY %") or f_db.get("q_eps_yoy")),
        # F2/F4 — annual
        "A Rev":          _millify(f2.get("Annual Revenue") or f_db.get("a_revenue")),
        "A EPS":          _num(f2.get("Annual EPS") or f_db.get("a_eps")),
        "A Rev YoY%":     _pct(f4.get("Annual Revenue YoY %") or f_db.get("a_rev_yoy")),
        "A EPS YoY%":     _pct(f4.get("Annual EPS YoY %") or f_db.get("a_eps_yoy")),
        # F5/F6 — valuation vs peers
        "Fwd PE":         _num(f5.get("Ticker Fwd PE") or f_db.get("forward_pe")),
        "Fwd PE vs Med%": _pct(f5.get("Ticker vs Median %")),
        "P/B":            _num(f6.get("Ticker P/B") or f_db.get("pb_ratio")),
        "P/B vs Med%":    _pct(f6.get("Ticker vs Median %")),
        # Fundamentals
        "Mkt Cap":        _millify(row.get("market_cap") or f_db.get("market_cap")),
        "Sector":         raw_info.get("sector") or f_db.get("sector") or "N/A",
        "Industry":       raw_info.get("industry") or f_db.get("industry") or "N/A",
    }


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
                     include_datetime: bool = False) -> pd.DataFrame:
    """
    Build the indicator summary DataFrame.
    rows: list of DB dicts (from get_summary_for_run / get_all_summaries)
    show_sub: whether to include sub-indicator columns
    selected_inds: if set, only include these main indicator columns
    include_datetime: add Datetime column (for All Queries tab)
    """
    if not rows:
        return pd.DataFrame()

    inds_to_show = selected_inds if selected_inds else MAIN_IND_COLS

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




def render_indicator_filter(tab_key: str) -> dict[str, set[str]]:
    """
    Renders per-indicator value filter UI inside an expander.
    Returns {indicator_id: set_of_accepted_values} for active filters only.
    Empty dict = no filtering.
    """
    ind_filters: dict[str, set[str]] = {}
    with st.expander("🔽 Filter by Indicator Values", expanded=False):
        selected_inds = st.multiselect(
            "Filter on indicators:",
            options=MAIN_IND_COLS,
            key=f"filt_inds_{tab_key}",
        )
        if not selected_inds:
            st.caption("Select indicators above to filter rows by their result values.")
            return {}

        cols = st.columns(min(len(selected_inds), 5))
        for i, ind in enumerate(selected_inds):
            with cols[i % 5]:
                vals = st.multiselect(
                    f"{ind}:",
                    options=["PASS", "PARTIAL", "FAIL", "NA"],
                    default=["PASS"],
                    format_func=lambda v: f"{EMOJI.get(v, v)} {v}",
                    key=f"filt_vals_{tab_key}_{ind}",
                )
                if vals:
                    ind_filters[ind] = set(vals)
    return ind_filters


def apply_indicator_filter(rows: list[dict],
                           ind_filters: dict[str, set[str]]) -> list[dict]:
    if not ind_filters:
        return rows
    return [
        r for r in rows
        if all(str(r.get(ind, "NA")).upper() in vals
               for ind, vals in ind_filters.items())
    ]


def render_value_table(tickers: list[str], detail_map: dict,
                       rows_by_ticker: dict, fund_map: dict,
                       tab_key: str):
    """
    Render the value table with:
      - Column group multiselect (user-selectable groups)
      - Ticker text search
      - Data from indicator detail JSON + DuckDB fundamentals
    """
    with st.expander("📐 Indicator Values Table", expanded=False):
        vcol1, vcol2 = st.columns([3, 1])
        with vcol1:
            sel_groups = st.multiselect(
                "Column groups:",
                options=list(VALUE_COL_GROUPS.keys()),
                default=DEFAULT_VALUE_GROUPS,
                key=f"val_groups_{tab_key}",
            )
        with vcol2:
            val_search = st.text_input(
                "Search ticker:",
                placeholder="e.g. AAPL",
                key=f"val_search_{tab_key}",
            )

        show_cols = ["Ticker"] + [
            c for g in sel_groups for c in VALUE_COL_GROUPS.get(g, [])
        ]

        records = []
        for ticker in tickers:
            if val_search and val_search.upper() not in ticker.upper():
                continue
            detail = detail_map.get(ticker, {})
            row    = rows_by_ticker.get(ticker, {})
            f_db   = fund_map.get(ticker, {})
            rec    = _build_value_record(ticker, detail, row, f_db)
            records.append({c: rec.get(c, "N/A") for c in show_cols})

        if records:
            st.dataframe(pd.DataFrame(records), width="stretch", hide_index=True)
        else:
            st.info("No data matches current search.")


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

    # ── Ticker search + indicator value filter ────────────────────────────────
    lsrch_col, lsub_col = st.columns([2, 1])
    with lsrch_col:
        latest_search = st.text_input(
            "Search ticker:", placeholder="e.g. AAPL",
            key="latest_ticker_search",
        )
    with lsub_col:
        show_sub = st.checkbox("Show sub-indicators", value=False, key="latest_show_sub")

    latest_ind_filter = render_indicator_filter("latest")

    # Apply filters to displayed rows
    display_rows = top50_rows
    if latest_search:
        display_rows = [r for r in display_rows
                        if latest_search.upper() in r["ticker"].upper()]
    display_rows = apply_indicator_filter(display_rows, latest_ind_filter)
    display_tickers = [r["ticker"] for r in display_rows]

    # ── Indicator Summary ─────────────────────────────────────────────────────
    st.markdown("### 📊 Indicator Summary")
    st.caption("✅ PASS · ⭕ PARTIAL · ❌ FAIL · ⚪️ N/A  —  Edit any cell and click **Save Edits**")

    sum_df  = build_summary_df(display_rows, show_sub=show_sub)
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
    fund_map_latest = get_all_fundamentals_for_run(display_tickers)
    rows_by_ticker_latest = {r["ticker"]: r for r in display_rows}
    render_value_table(display_tickers, detail_map,
                       rows_by_ticker_latest, fund_map_latest, "latest")

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

    # Filters
    all_stored_tickers = get_all_tickers()
    fcol1, fcol2, fcol3 = st.columns(3)
    with fcol1:
        f_tickers = st.multiselect("Filter by Ticker", options=all_stored_tickers, key="hist_f_tick")
    with fcol2:
        f_dts = st.multiselect("Filter by Datetime", options=all_dts, key="hist_f_dt")
    with fcol3:
        all_show_sub = st.checkbox("Show sub-indicators", value=False, key="all_queries_show_sub")

    # Ticker text search
    hist_search = st.text_input(
        "Search ticker:", placeholder="e.g. AAPL",
        key="hist_ticker_search",
    )

    hist_ind_filter = render_indicator_filter("history")

    # Fetch filtered rows
    filt_rows = get_all_summaries(
        tickers   = f_tickers if f_tickers else None,
        datetimes = f_dts     if f_dts     else None,
    )

    # Apply ticker text search
    if hist_search:
        filt_rows = [r for r in filt_rows
                     if hist_search.upper() in r["ticker"].upper()]

    # Apply indicator filter
    filt_rows = apply_indicator_filter(filt_rows, hist_ind_filter)

    all_df = build_summary_df(filt_rows, show_sub=all_show_sub, include_datetime=True)
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
    # Build detail map: use most-recent datetime per ticker
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

    hist_fund_map       = get_all_fundamentals_for_run(hist_tickers)
    hist_rows_by_ticker = {r["ticker"]: r for r in filt_rows}
    render_value_table(hist_tickers, hist_detail_map,
                       hist_rows_by_ticker, hist_fund_map, "history")

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
