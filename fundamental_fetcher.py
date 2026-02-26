"""
fundamental_fetcher.py — Enhanced fundamental data via yfinance.

Improvements over data_fetcher.fetch_fundamental():
  - Stores complete raw info dict as JSON for the value table.
  - Checks DuckDB for existing same-day data before making API calls.
  - Returns structured dict compatible with indicators.py (F1-F6).
  - Rate-limit handling and VPN awareness unchanged from v1.
"""

from __future__ import annotations
import json
import time
from datetime import date
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import yfinance as yf

import storage

CST = ZoneInfo("America/Chicago")
_RETRY_DELAYS = [5, 10, 20]


# ── Internal helpers ──────────────────────────────────────────────────────────

def _is_auth_err(e) -> bool:
    s = str(e)
    return "401" in s or "Unauthorized" in s or "Invalid Crumb" in s


def _safe(v, ndigits=None):
    try:
        if v is None:
            return None
        f = float(v)
        if np.isnan(f) or np.isinf(f):
            return None
        return round(f, ndigits) if ndigits is not None else f
    except Exception:
        return None


def _col_date(columns, idx=0) -> str | None:
    try:
        col = columns[idx]
        if hasattr(col, "date"):
            return str(col.date())
        return str(col)[:10]
    except Exception:
        return None


def _yoy_from_frame(frame, row) -> float | None:
    try:
        if frame.empty or row not in frame.index:
            return None
        s = frame.loc[row].dropna()
        if len(s) < 2:
            return None
        curr, prev = float(s.iloc[0]), float(s.iloc[1])
        if prev == 0:
            return None
        return round((curr - prev) / abs(prev) * 100, 2)
    except Exception:
        return None


def _get(frame, row, col_idx=0):
    try:
        if frame.empty or row not in frame.index:
            return None
        val = frame.loc[row].iloc[col_idx]
        return None if pd.isna(val) else float(val)
    except Exception:
        return None


def _fetch_info_with_retry(stock) -> dict:
    """Fetch stock.info with retry on rate-limit. Raises on 401."""
    for attempt, delay in enumerate(_RETRY_DELAYS, 1):
        try:
            info = stock.info
            if len(info) >= 10:
                return info
            print(f"  [fund] Rate-limited on info (attempt {attempt}), waiting {delay}s…")
            time.sleep(delay)
        except Exception as e:
            if _is_auth_err(e):
                raise
            time.sleep(delay)
    return stock.info


def _fetch_qf(stock) -> pd.DataFrame:
    """Fetch quarterly_financials. Empty = accept; retry only on exceptions."""
    try:
        return stock.quarterly_financials
    except Exception as e:
        if _is_auth_err(e):
            raise
        for attempt, delay in enumerate(_RETRY_DELAYS, 1):
            print(f"  [fund] Retry quarterly_financials ({attempt}), waiting {delay}s…")
            time.sleep(delay)
            try:
                return stock.quarterly_financials
            except Exception as e2:
                if _is_auth_err(e2):
                    raise
        return pd.DataFrame()


def _fetch_af(stock) -> pd.DataFrame:
    """Fetch annual financials. Empty = accept; retry only on exceptions."""
    try:
        return stock.financials
    except Exception as e:
        if _is_auth_err(e):
            raise
        for attempt, delay in enumerate(_RETRY_DELAYS, 1):
            print(f"  [fund] Retry financials ({attempt}), waiting {delay}s…")
            time.sleep(delay)
            try:
                return stock.financials
            except Exception as e2:
                if _is_auth_err(e2):
                    raise
        return pd.DataFrame()


# ── Public API ────────────────────────────────────────────────────────────────

def fetch_fundamental(ticker: str,
                      skip_normalize: bool = False,
                      use_db_cache: bool = True) -> dict:
    """
    Fetch fundamental data for a ticker via yfinance.

    Returns a dict with all fields needed by indicators.py (F1-F6),
    plus 'raw_info' containing the full yfinance info dict.

    Also stores results in DuckDB fundamentals table.

    On rate-limit / auth error: returns {'error': ..., 'rate_limited': True}.
    On other error:             returns {'error': ...}.
    """
    today = date.today().isoformat()

    # ── DB cache check ────────────────────────────────────────────────────────
    if use_db_cache:
        cached = storage.get_latest_fundamental(ticker)
        if cached and str(cached.get("fetch_date", ""))[:10] == today:
            # Return the cached fundamental without hitting the API
            result = _cached_to_result(ticker, cached)
            if result:
                return result

    try:
        sym   = ticker if skip_normalize else _normalize(ticker)
        stock = yf.Ticker(sym)
        info  = _fetch_info_with_retry(stock)
        qf    = _fetch_qf(stock)
        af    = _fetch_af(stock)

        # ── Extract key fields ────────────────────────────────────────────────
        q_revenue = _get(qf, "Total Revenue", 0)
        q_eps     = _get(qf, "Basic EPS",     0)
        a_revenue = _get(af, "Total Revenue", 0)
        a_eps     = _get(af, "Basic EPS",     0)
        q_end_date = _col_date(qf.columns, 0) if not qf.empty else None
        a_end_date = _col_date(af.columns, 0) if not af.empty else None

        # F3: quarterly YoY
        raw_q_rev = info.get("revenueGrowth")
        raw_q_eps = info.get("earningsGrowth")
        if raw_q_rev is not None:
            q_rev_yoy    = _safe(raw_q_rev * 100, 2)
            q_rev_source = "Yahoo Finance info.revenueGrowth"
        else:
            q_rev_yoy    = _yoy_from_frame(qf, "Total Revenue")
            q_rev_source = "Computed from quarterly_financials"
        if raw_q_eps is not None:
            q_eps_yoy    = _safe(raw_q_eps * 100, 2)
            q_eps_source = "Yahoo Finance info.earningsGrowth"
        else:
            q_eps_yoy    = _yoy_from_frame(qf, "Basic EPS")
            q_eps_source = "Computed from quarterly_financials"

        # F4: annual YoY
        a_rev_yoy = _yoy_from_frame(af, "Total Revenue")
        a_eps_yoy = _yoy_from_frame(af, "Basic EPS")

        forward_pe = _safe(info.get("forwardPE"),   2)
        pb_ratio   = _safe(info.get("priceToBook"), 2)
        market_cap = _safe(info.get("marketCap"),   0)

        # Serialize full info as JSON (filter non-serializable types)
        raw_info_json = _serialize_info(info)

        # ── Persist to DuckDB ─────────────────────────────────────────────────
        storage.save_fundamental(ticker, today, {
            "market_cap":    market_cap,
            "forward_pe":    forward_pe,
            "pb_ratio":      pb_ratio,
            "q_revenue":     q_revenue,
            "q_eps":         q_eps,
            "a_revenue":     a_revenue,
            "a_eps":         a_eps,
            "q_rev_yoy":     q_rev_yoy,
            "q_eps_yoy":     q_eps_yoy,
            "a_rev_yoy":     a_rev_yoy,
            "a_eps_yoy":     a_eps_yoy,
            "q_end_date":    q_end_date,
            "a_end_date":    a_end_date,
            "q_rev_source":  q_rev_source,
            "q_eps_source":  q_eps_source,
            "raw_info_json": raw_info_json,
        })

        return {
            "ticker":        sym,
            "q_revenue":     q_revenue,
            "q_eps":         q_eps,
            "a_revenue":     a_revenue,
            "a_eps":         a_eps,
            "q_rev_yoy":     q_rev_yoy,
            "q_eps_yoy":     q_eps_yoy,
            "a_rev_yoy":     a_rev_yoy,
            "a_eps_yoy":     a_eps_yoy,
            "q_rev_source":  q_rev_source,
            "q_eps_source":  q_eps_source,
            "a_rev_source":  "Computed from annual financials",
            "a_eps_source":  "Computed from annual financials",
            "q_end_date":    q_end_date,
            "a_end_date":    a_end_date,
            "forward_pe":    forward_pe,
            "pb_ratio":      pb_ratio,
            "market_cap":    market_cap,
            "raw_info":      info,
        }

    except Exception as e:
        err = str(e)
        if "401" in err or "Unauthorized" in err or "Invalid Crumb" in err:
            return {"error": f"Auth block for {ticker}: {e}", "rate_limited": True}
        return {"error": f"Fundamental fetch failed for {ticker}: {e}"}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _normalize(ticker: str) -> str:
    ticker = ticker.upper().strip()
    if "." in ticker:
        return ticker
    for suffix in ["", ".NS", ".BO"]:
        try:
            if not yf.Ticker(ticker + suffix).history(period="1d").empty:
                return ticker + suffix
        except Exception:
            pass
    return ticker


def _serialize_info(info: dict) -> str:
    """Convert info dict to JSON, coercing non-serializable values."""
    def _clean(v):
        if isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
            return None
        if isinstance(v, (np.integer, np.floating)):
            return v.item()
        if isinstance(v, np.ndarray):
            return v.tolist()
        return v
    cleaned = {k: _clean(v) for k, v in info.items()}
    try:
        return json.dumps(cleaned)
    except Exception:
        return json.dumps({})


def _cached_to_result(ticker: str, row: dict) -> dict | None:
    """Convert a DuckDB fundamentals row back to the fetch_fundamental() dict format."""
    try:
        raw_info = {}
        if row.get("raw_info_json"):
            try:
                raw_info = json.loads(row["raw_info_json"])
            except Exception:
                pass
        return {
            "ticker":        ticker,
            "q_revenue":     row.get("q_revenue"),
            "q_eps":         row.get("q_eps"),
            "a_revenue":     row.get("a_revenue"),
            "a_eps":         row.get("a_eps"),
            "q_rev_yoy":     row.get("q_rev_yoy"),
            "q_eps_yoy":     row.get("q_eps_yoy"),
            "a_rev_yoy":     row.get("a_rev_yoy"),
            "a_eps_yoy":     row.get("a_eps_yoy"),
            "q_rev_source":  row.get("q_rev_source", "cached"),
            "q_eps_source":  row.get("q_eps_source", "cached"),
            "a_rev_source":  "cached",
            "a_eps_source":  "cached",
            "q_end_date":    row.get("q_end_date"),
            "a_end_date":    row.get("a_end_date"),
            "forward_pe":    row.get("forward_pe"),
            "pb_ratio":      row.get("pb_ratio"),
            "market_cap":    row.get("market_cap"),
            "raw_info":      raw_info,
            "_from_cache":   True,
        }
    except Exception:
        return None
