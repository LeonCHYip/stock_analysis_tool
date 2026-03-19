"""
fundamental_fetcher.py — Fundamental data via 2 API calls per ticker.

Two HTTP requests (down from the original 3):

  Call 1 — quoteSummary (7 modules, replaces stock.info):
    price                           → marketCap (live, price-driven)
    defaultKeyStatistics            → forwardPE, priceToBook (live, price-driven)
    financialData                   → revenueGrowth, earningsGrowth (used as YoY fallback)
    assetProfile                    → sector, industry
    calendarEvents                  → next earningsDate
    incomeStatementHistoryQuarterly → q_end_date (revenue/EPS come from Call 2)
    earningsHistory                 → kept as last-resort Q EPS fallback only

  Call 2 — v8 timeseries (single request, replaces stock.quarterly_financials + stock.financials):
    quarterlyBasicEPS   → GAAP Q EPS; YoY = entry[0] vs entry[4] (same quarter last year)
    quarterlyTotalRevenue
    annualBasicEPS      → GAAP annual EPS; YoY = entry[0] vs entry[1]
    annualTotalRevenue
      ↳ All four fields fetched in one HTTP request to the v8 timeseries endpoint.
        This endpoint is the only reliable source for GAAP per-share EPS history.
        quoteSummary income statement modules do not carry basicEps.
"""

from __future__ import annotations
import json
import time
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import numpy as np
import yfinance as yf

import storage

CST = ZoneInfo("America/Chicago")
_RETRY_DELAYS = [5, 10, 20]

# ── Call 1: quoteSummary modules ───────────────────────────────────────────────
_MODULES = ",".join([
    "price",                            # marketCap (live, price-driven)
    "defaultKeyStatistics",             # forwardPE, priceToBook, short interest, shares (live)
    "financialData",                    # margins, ratios, analyst targets
    "summaryDetail",                    # averageVolume (3-month average daily volume)
    "assetProfile",                     # sector, industry
    "calendarEvents",                   # next earningsDate
    "incomeStatementHistoryQuarterly",  # q_end_date
    "earningsHistory",                  # Q EPS fallback only
    "netSharePurchaseActivity",         # insider buy/sell activity (6-month window)
])
_QS_URLS = [
    "https://query2.finance.yahoo.com/v10/finance/quoteSummary/{sym}",
    "https://query1.finance.yahoo.com/v10/finance/quoteSummary/{sym}",
]

# ── Call 2: v8 timeseries fields ───────────────────────────────────────────────
_TS_TYPES = [
    "quarterlyBasicEPS",     # GAAP Q EPS per share
    "quarterlyTotalRevenue", # Q revenue
    "annualBasicEPS",        # GAAP annual EPS per share
    "annualTotalRevenue",    # Annual revenue
]
_TS_URLS = [
    "https://query2.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/{sym}",
    "https://query1.finance.yahoo.com/ws/fundamentals-timeseries/v1/finance/timeseries/{sym}",
]


# ── Internal helpers ──────────────────────────────────────────────────────────

def _is_auth_err(e) -> bool:
    s = str(e)
    return "401" in s or "Unauthorized" in s or "Invalid Crumb" in s


def _epoch_to_date(val) -> str | None:
    """Convert a Unix epoch (int or float) to 'YYYY-MM-DD', or None on failure."""
    try:
        return datetime.utcfromtimestamp(int(val)).strftime("%Y-%m-%d")
    except Exception:
        return None


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


def _raw(d, *keys):
    """Navigate nested dicts and unwrap Yahoo Finance {raw, fmt} value objects."""
    for k in keys:
        if not isinstance(d, dict):
            return None
        d = d.get(k)
    if isinstance(d, dict):
        return d.get("raw")
    return d


def _stmt_value(stmt_list: list, key: str, idx: int = 0):
    """Extract a .raw value from a quoteSummary income statement entry."""
    try:
        return _raw(stmt_list[idx], key)
    except Exception:
        return None


def _stmt_end_date(stmt_list: list, idx: int = 0) -> str | None:
    """Get end date string (YYYY-MM-DD) from a quoteSummary income statement entry."""
    try:
        end = stmt_list[idx].get("endDate", {})
        fmt = end.get("fmt") if isinstance(end, dict) else None
        if fmt:
            return fmt
        raw = end.get("raw") if isinstance(end, dict) else None
        if raw:
            return datetime.fromtimestamp(float(raw), tz=timezone.utc).strftime("%Y-%m-%d")
        return None
    except Exception:
        return None


def _ts_value(entries: list, idx: int = 0) -> float | None:
    """Get reportedValue.raw from a v8 timeseries entry."""
    try:
        v = entries[idx].get("reportedValue", {}).get("raw")
        return _safe(v)
    except Exception:
        return None


def _ts_date(entries: list, idx: int = 0) -> str | None:
    """Get asOfDate string from a v8 timeseries entry."""
    try:
        return entries[idx].get("asOfDate")
    except Exception:
        return None


def _ts_yoy(entries: list, curr_idx: int = 0, prev_idx: int = 1) -> float | None:
    """
    YoY % from v8 timeseries entries (newest-first).
    For quarterly YoY use prev_idx=4 (same quarter last year).
    For annual    YoY use prev_idx=1 (previous fiscal year).
    """
    curr = _ts_value(entries, curr_idx)
    prev = _ts_value(entries, prev_idx)
    if curr is None or prev is None or float(prev) == 0:
        return None
    return round((float(curr) - float(prev)) / abs(float(prev)) * 100, 2)


# ── API call helpers ──────────────────────────────────────────────────────────

def _fetch_quote_summary(stock) -> dict:
    """
    Call 1: single quoteSummary HTTP request using yfinance's session.
    Handles cookies and crumb automatically. Retries on empty response.
    """
    sym    = stock.ticker
    params = {"modules": _MODULES, "corsDomain": "finance.yahoo.com"}

    for attempt, delay in enumerate(_RETRY_DELAYS, 1):
        for url_tmpl in _QS_URLS:
            try:
                raw    = stock._data.get_raw_json(url_tmpl.format(sym=sym), params=params)
                result = (raw or {}).get("quoteSummary", {}).get("result") or []
                if result:
                    return result[0]
            except Exception as e:
                if _is_auth_err(e):
                    raise
        print(f"  [fund] Empty quoteSummary for {sym} (attempt {attempt}), waiting {delay}s…")
        time.sleep(delay)
    return {}


def _fetch_timeseries(stock, sym: str) -> dict[str, list]:
    """
    Call 2: single v8 timeseries request for both quarterly and annual GAAP data.
    Returns {type_name: [entries sorted newest-first]}.

    Fetches 10 years of history so quarterly YoY (entry[0] vs entry[4]) has enough data.
    """
    now     = int(time.time())
    params  = {
        "symbol":      sym,
        "type":        ",".join(_TS_TYPES),
        "period1":     now - 10 * 365 * 86400,  # 10 years back
        "period2":     now + 86400,
        "merge":       "false",
        "padMissing":  "true",
    }

    for attempt, delay in enumerate(_RETRY_DELAYS, 1):
        for url_tmpl in _TS_URLS:
            try:
                raw    = stock._data.get_raw_json(url_tmpl.format(sym=sym), params=params)
                result = (raw or {}).get("timeseries", {}).get("result") or []
                if result:
                    data: dict[str, list] = {}
                    for item in result:
                        type_name = ((item.get("meta") or {}).get("type") or [""])[0]
                        entries   = [e for e in (item.get(type_name) or []) if e is not None]
                        entries.sort(key=lambda x: x.get("asOfDate", ""), reverse=True)
                        data[type_name] = entries
                    return data
            except Exception as e:
                if _is_auth_err(e):
                    raise
        print(f"  [fund] Empty timeseries for {sym} (attempt {attempt}), waiting {delay}s…")
        time.sleep(delay)
    return {}


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


def _serialize(d: dict) -> str:
    """JSON-serialize a dict, coercing numpy / nan / inf values."""
    def _clean(v):
        if isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
            return None
        if isinstance(v, (np.integer, np.floating)):
            return v.item()
        if isinstance(v, np.ndarray):
            return v.tolist()
        return v
    try:
        return json.dumps({k: _clean(v) for k, v in d.items()})
    except Exception:
        return json.dumps({})


# ── Public API ────────────────────────────────────────────────────────────────

def fetch_fundamental(ticker: str,
                      skip_normalize: bool = False,
                      use_db_cache: bool = True) -> dict:
    """
    Fetch fundamental data for a ticker using 2 API calls (down from original 3).

    Call 1 — quoteSummary:
      price, defaultKeyStatistics, financialData  → live PE, PB, market cap
      assetProfile                                → sector, industry
      calendarEvents                              → next earnings date
      incomeStatementHistoryQuarterly             → q_end_date
      earningsHistory                             → Q EPS fallback

    Call 2 — v8 timeseries (one request, four fields):
      quarterlyBasicEPS    → GAAP Q EPS; YoY vs same quarter last year (entry[4])
      quarterlyTotalRevenue→ Q revenue; YoY vs same quarter last year
      annualBasicEPS       → GAAP annual EPS; YoY vs previous year (entry[1])
      annualTotalRevenue   → Annual revenue; YoY vs previous year

    On rate-limit / auth error: returns {'error': ..., 'rate_limited': True}.
    On other error:             returns {'error': ...}.
    """
    today_str = date.today().isoformat()

    try:
        sym   = ticker if skip_normalize else _normalize(ticker)
        stock = yf.Ticker(sym)

        # ── Call 1: quoteSummary ──────────────────────────────────────────────
        qs = _fetch_quote_summary(stock)

        price_m  = qs.get("price", {})
        stats_m  = qs.get("defaultKeyStatistics", {})
        fin_m    = qs.get("financialData", {})
        profile  = qs.get("assetProfile", {})
        cal      = qs.get("calendarEvents", {})
        q_stmts  = qs.get("incomeStatementHistoryQuarterly", {}) \
                      .get("incomeStatementHistory", [])
        eps_hist = qs.get("earningsHistory", {}).get("history", [])

        # Live fields (price-driven, always fresh)
        market_cap = _safe(_raw(price_m, "marketCap"), 0)
        forward_pe = _safe(_raw(stats_m, "forwardPE"),   2)
        pb_ratio   = _safe(_raw(stats_m, "priceToBook"), 2)
        sector     = profile.get("sector")   or "N/A"
        industry   = profile.get("industry") or "N/A"

        # ── Short interest (defaultKeyStatistics) ─────────────────────────────
        shares_short    = _safe(_raw(stats_m, "sharesShort"), 0)
        shares_short_pm = _safe(_raw(stats_m, "sharesShortPriorMonth"), 0)
        float_shares    = _safe(_raw(stats_m, "floatShares"), 0)
        shares_out      = _safe(_raw(stats_m, "sharesOutstanding"), 0)
        implied_shares  = _safe(_raw(stats_m, "impliedSharesOutstanding"), 0)
        short_pct_float = _safe(_raw(stats_m, "shortPercentOfFloat"), 6)   # 0–1
        short_pct_out   = _safe(_raw(stats_m, "sharesPercentSharesOut"), 6) # 0–1
        short_ratio     = _safe(_raw(stats_m, "shortRatio"), 2)
        date_short_int  = _epoch_to_date(_raw(stats_m, "dateShortInterest"))
        summary_m       = qs.get("summaryDetail", {})
        avg_volume      = _safe(_raw(summary_m, "averageVolume"), 0)

        # ── Insider activity (netSharePurchaseActivity, 6-month window) ───────
        ins_m           = qs.get("netSharePurchaseActivity", {})
        ins_buy_count   = _safe(_raw(ins_m, "buyInfoCount"), 0)
        ins_buy_shares  = _safe(_raw(ins_m, "buyInfoShares"), 0)
        ins_sell_count  = _safe(_raw(ins_m, "sellInfoCount"), 0)
        ins_sell_shares = _safe(_raw(ins_m, "sellInfoShares"), 0)
        ins_net_shares  = _safe(_raw(ins_m, "netInfoShares"), 0)
        ins_buy_pct     = _safe(_raw(ins_m, "buyPercentInsiderShares"),  6)  # 0–1
        ins_sell_pct    = _safe(_raw(ins_m, "sellPercentInsiderShares"), 6)  # 0–1
        ins_net_pct     = _safe(_raw(ins_m, "netPercentInsiderShares"),  6)  # 0–1

        # ── Margins & ratios (financialData) ──────────────────────────────────
        gross_margin    = _safe(_raw(fin_m, "grossMargins"), 6)
        ebitda_margin   = _safe(_raw(fin_m, "ebitdaMargins"), 6)
        op_margin       = _safe(_raw(fin_m, "operatingMargins"), 6)
        net_margin      = _safe(_raw(fin_m, "profitMargins"), 6)
        current_ratio   = _safe(_raw(fin_m, "currentRatio"), 3)
        quick_ratio     = _safe(_raw(fin_m, "quickRatio"), 3)
        debt_to_equity  = _safe(_raw(fin_m, "debtToEquity"), 2)
        roe             = _safe(_raw(fin_m, "returnOnEquity"), 6)
        roa             = _safe(_raw(fin_m, "returnOnAssets"), 6)

        # ── Analyst targets (financialData) ───────────────────────────────────
        target_median   = _safe(_raw(fin_m, "targetMedianPrice"), 2)
        target_high     = _safe(_raw(fin_m, "targetHighPrice"), 2)
        target_low      = _safe(_raw(fin_m, "targetLowPrice"), 2)
        target_mean     = _safe(_raw(fin_m, "targetMeanPrice"), 2)
        current_price_fd = _safe(_raw(fin_m, "currentPrice"), 2)
        rec_mean        = _safe(_raw(fin_m, "recommendationMean"), 2)
        rec_key         = fin_m.get("recommendationKey") or None
        analyst_count   = _safe(_raw(fin_m, "numberOfAnalystOpinions"), 0)

        earnings_dates = cal.get("earnings", {}).get("earningsDate", [])
        earnings_date_epochs = [
            d.get("raw") for d in earnings_dates
            if isinstance(d, dict) and d.get("raw")
        ]

        # q_end_date from income statement (revenue/EPS will be overwritten by Call 2)
        q_end_date = _stmt_end_date(q_stmts, 0)

        # ── Call 2: v8 timeseries — GAAP EPS + revenue for Q and annual ───────
        ts = _fetch_timeseries(stock, sym)

        q_eps_ts  = ts.get("quarterlyBasicEPS",    [])
        q_rev_ts  = ts.get("quarterlyTotalRevenue", [])
        a_eps_ts  = ts.get("annualBasicEPS",        [])
        a_rev_ts  = ts.get("annualTotalRevenue",    [])

        # Quarterly fields (GAAP Basic EPS from timeseries)
        q_revenue  = _ts_value(q_rev_ts, 0)
        q_eps      = _ts_value(q_eps_ts, 0)
        # q_end_date: prefer timeseries date (more precise), fall back to quoteSummary
        q_end_date = _ts_date(q_eps_ts, 0) or q_end_date

        # Q YoY: prefer Yahoo Finance's pre-computed rate; fall back to same-Q-last-year (idx 4)
        raw_q_rev = _raw(fin_m, "revenueGrowth")
        raw_q_eps = _raw(fin_m, "earningsGrowth")
        if raw_q_rev is not None:
            q_rev_yoy    = _safe(raw_q_rev * 100, 2)
            q_rev_source = "Yahoo Finance financialData.revenueGrowth"
        else:
            q_rev_yoy    = _ts_yoy(q_rev_ts, 0, 4)   # same quarter last year
            q_rev_source = "Computed from quarterlyTotalRevenue timeseries"
        if raw_q_eps is not None:
            q_eps_yoy    = _safe(raw_q_eps * 100, 2)
            q_eps_source = "Yahoo Finance financialData.earningsGrowth"
        else:
            q_eps_yoy    = _ts_yoy(q_eps_ts, 0, 4)   # same quarter last year
            q_eps_source = "Computed from quarterlyBasicEPS timeseries"
            # Last resort: non-GAAP epsActual from earningsHistory
            if q_eps_yoy is None and len(eps_hist) >= 5:
                q_eps_yoy    = _ts_yoy(
                    [{"reportedValue": {"raw": _raw(e, "epsActual")}} for e in eps_hist], 0, 4
                )
                q_eps_source = "Computed from earningsHistory (non-GAAP fallback)"

        # Annual fields (GAAP Basic EPS from timeseries)
        a_revenue  = _ts_value(a_rev_ts, 0)
        a_eps      = _ts_value(a_eps_ts, 0)
        a_end_date = _ts_date(a_eps_ts, 0)
        a_rev_yoy  = _ts_yoy(a_rev_ts, 0, 1)   # previous fiscal year
        a_eps_yoy  = _ts_yoy(a_eps_ts, 0, 1)   # previous fiscal year

        # ── Finviz override: use newer earnings data if available ──────────────
        fviz = storage.get_latest_earnings(ticker)
        if fviz and fviz.get("earnings_date"):
            fviz_date    = fviz["earnings_date"]
            yahoo_q_date = q_end_date or ""
            if fviz_date > yahoo_q_date:
                # Finviz has a more recent quarter — override q_eps and q_revenue
                if fviz.get("eps_gaap_act") is not None:
                    q_eps        = _safe(fviz["eps_gaap_act"])
                    q_eps_source = (
                        f"Finviz EPS GAAP Act ({fviz_date}) + computed vs Yahoo prior-year timeseries"
                    )
                if fviz.get("rev_act_m") is not None:
                    q_revenue    = _safe(fviz["rev_act_m"]) * 1_000_000
                    q_rev_source = (
                        f"Finviz Revenue Act $M ({fviz_date}) + computed vs Yahoo prior-year timeseries"
                    )

                # Recompute YoY using Yahoo timeseries as denominator
                target_prior = date.fromisoformat(fviz_date) - timedelta(days=365)

                def _closest_ts_value(ts_list, target_date):
                    best, best_delta = None, float("inf")
                    for e in (ts_list or []):
                        d_str = e.get("asOfDate", "")
                        if not d_str:
                            continue
                        try:
                            d = date.fromisoformat(d_str)
                            delta = abs((d - target_date).days)
                            if delta < best_delta:
                                best_delta = delta
                                best = e.get("reportedValue", {}).get("raw")
                        except Exception:
                            pass
                    return _safe(best)

                prior_rev = _closest_ts_value(q_rev_ts, target_prior)
                if q_revenue and prior_rev and prior_rev != 0:
                    q_rev_yoy    = round((q_revenue - prior_rev) / abs(prior_rev) * 100, 2)
                else:
                    q_rev_source = q_rev_source.replace(
                        "+ computed vs Yahoo prior-year timeseries", "— YoY unavailable"
                    )

                prior_eps = _closest_ts_value(q_eps_ts, target_prior)
                if q_eps is not None and prior_eps and prior_eps != 0:
                    q_eps_yoy    = round((q_eps - prior_eps) / abs(prior_eps) * 100, 2)
                else:
                    q_eps_source = q_eps_source.replace(
                        "+ computed vs Yahoo prior-year timeseries", "— YoY unavailable"
                    )

                # Store computed YoY back to earnings_history
                storage.save_earnings(ticker, {
                    "earnings_date": fviz_date,
                    "q_rev_yoy":     q_rev_yoy,
                    "q_eps_yoy":     q_eps_yoy,
                })

                # Yahoo's q_end_date refers to the prior (stale) quarter — clear it
                q_end_date = None

        # Build flat info dict (backward-compatible with app.py raw_info_json)
        flat_info = {
            "sector":               sector,
            "industry":             industry,
            "longName":             price_m.get("longName") or None,
            "longBusinessSummary":  profile.get("longBusinessSummary") or None,
            "earningsDate":         earnings_date_epochs,
            "marketCap":            market_cap,
            "forwardPE":            forward_pe,
            "priceToBook":          pb_ratio,
            "revenueGrowth":        raw_q_rev,
            "earningsGrowth":       raw_q_eps,
        }
        raw_info_json = _serialize(flat_info)

        # Persist to DuckDB
        storage.save_fundamental(ticker, today_str, {
            "market_cap":     market_cap,
            "forward_pe":     forward_pe,
            "pb_ratio":       pb_ratio,
            "q_revenue":      q_revenue,
            "q_eps":          q_eps,
            "a_revenue":      a_revenue,
            "a_eps":          a_eps,
            "q_rev_yoy":      q_rev_yoy,
            "q_eps_yoy":      q_eps_yoy,
            "a_rev_yoy":      a_rev_yoy,
            "a_eps_yoy":      a_eps_yoy,
            "q_end_date":     q_end_date,
            "a_end_date":     a_end_date,
            "q_rev_source":   q_rev_source,
            "q_eps_source":   q_eps_source,
            "raw_info_json":  raw_info_json,
            # Short interest
            "shares_short":     shares_short,
            "shares_short_pm":  shares_short_pm,
            "float_shares":     float_shares,
            "shares_out":       shares_out,
            "implied_shares":   implied_shares,
            "short_pct_float":  short_pct_float,
            "short_pct_out":    short_pct_out,
            "short_ratio":      short_ratio,
            "date_short_int":   date_short_int,
            "avg_volume":       avg_volume,
            # Insider activity
            "ins_buy_count":    ins_buy_count,
            "ins_buy_shares":   ins_buy_shares,
            "ins_sell_count":   ins_sell_count,
            "ins_sell_shares":  ins_sell_shares,
            "ins_net_shares":   ins_net_shares,
            "ins_buy_pct":      ins_buy_pct,
            "ins_sell_pct":     ins_sell_pct,
            "ins_net_pct":      ins_net_pct,
            # Margins & ratios
            "gross_margin":     gross_margin,
            "ebitda_margin":    ebitda_margin,
            "op_margin":        op_margin,
            "net_margin":       net_margin,
            "current_ratio":    current_ratio,
            "quick_ratio":      quick_ratio,
            "debt_to_equity":   debt_to_equity,
            "roe":              roe,
            "roa":              roa,
            # Analyst targets
            "target_median":    target_median,
            "target_high":      target_high,
            "target_low":       target_low,
            "target_mean":      target_mean,
            "current_price_fd": current_price_fd,
            "rec_mean":         rec_mean,
            "rec_key":          rec_key,
            "analyst_count":    analyst_count,
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
            "a_rev_source":  "Computed from annualTotalRevenue timeseries",
            "a_eps_source":  "Computed from annualBasicEPS timeseries",
            "q_end_date":    q_end_date,
            "a_end_date":    a_end_date,
            "forward_pe":    forward_pe,
            "pb_ratio":      pb_ratio,
            "market_cap":    market_cap,
            "raw_info":      flat_info,
        }

    except Exception as e:
        err = str(e)
        if "401" in err or "Unauthorized" in err or "Invalid Crumb" in err:
            return {"error": f"Auth block for {ticker}: {e}", "rate_limited": True}
        return {"error": f"Fundamental fetch failed for {ticker}: {e}"}
