"""
indicators.py — evaluates all 10 indicators with PASS/PARTIAL/FAIL/NA logic.

Result strings:
  "PASS"    — all sub-indicators True
  "PARTIAL" — mix of True and False (no None)
  "FAIL"    — all sub-indicators False
  "NA"      — any sub-indicator is None (data unavailable)

F5/F6 have no sub-indicators: binary PASS/FAIL/NA only.
"""

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _pf(condition) -> bool | None:
    if condition is None:
        return None
    return bool(condition)


def _grade(sub_dict: dict) -> str:
    """Compute PASS/PARTIAL/FAIL/NA from a dict of bool|None sub-check values."""
    vals = list(sub_dict.values())
    if any(v is None for v in vals):
        return "NA"
    if all(v is True for v in vals):
        return "PASS"
    if all(v is False for v in vals):
        return "FAIL"
    return "PARTIAL"


def _single(condition) -> str:
    """PASS/FAIL/NA for single-check indicators (F5, F6)."""
    if condition is None:
        return "NA"
    return "PASS" if condition else "FAIL"


# ─────────────────────────────────────────────────────────────────────────────
# T1 — Daily Price & Volume vs 3M and 12M
# ─────────────────────────────────────────────────────────────────────────────

def indicator_t1(tech: dict) -> dict:
    d3  = tech.get("daily_vs_3m")  or {}
    d12 = tech.get("daily_vs_12m") or {}

    sub = {
        "3M Daily Price Up":   _pf(d3.get("price_up"))   if d3  else None,
        "3M Daily Volume Up":  _pf(d3.get("volume_up"))  if d3  else None,
        "12M Daily Price Up":  _pf(d12.get("price_up"))  if d12 else None,
        "12M Daily Volume Up": _pf(d12.get("volume_up")) if d12 else None,
    }

    detail = {
        # 3M
        "3M Latest Date Range":  d3.get("latest_date_range"),
        "3M Prior Date Range":   d3.get("prior_date_range"),
        "3M Latest Prices":      d3.get("latest_prices"),
        "3M Prior Prices":       d3.get("prior_prices"),
        "3M Latest Price Avg":   d3.get("latest_price_avg"),
        "3M Prior Price Avg":    d3.get("prior_price_avg"),
        "3M Price Change %":     d3.get("price_pct_change"),
        "3M Latest Volume Avg":  d3.get("latest_volume_avg"),
        "3M Prior Volume Avg":   d3.get("prior_volume_avg"),
        "3M Volume Change %":    d3.get("volume_pct_change"),
        # 12M
        "12M Latest Date Range": d12.get("latest_date_range"),
        "12M Prior Date Range":  d12.get("prior_date_range"),
        "12M Latest Prices":     d12.get("latest_prices"),
        "12M Prior Prices":      d12.get("prior_prices"),
        "12M Latest Price Avg":  d12.get("latest_price_avg"),
        "12M Prior Price Avg":   d12.get("prior_price_avg"),
        "12M Price Change %":    d12.get("price_pct_change"),
        "12M Latest Volume Avg": d12.get("latest_volume_avg"),
        "12M Prior Volume Avg":  d12.get("prior_volume_avg"),
        "12M Volume Change %":   d12.get("volume_pct_change"),
        "sub_checks": sub,
    }
    return {"pass": _grade(sub), "detail": detail}


# ─────────────────────────────────────────────────────────────────────────────
# T2 — Weekly Price & Volume vs 3M and 12M
# ─────────────────────────────────────────────────────────────────────────────

def indicator_t2(tech: dict) -> dict:
    w3  = tech.get("weekly_vs_3m")  or {}
    w12 = tech.get("weekly_vs_12m") or {}

    sub = {
        "3M Weekly Price Up":   _pf(w3.get("price_up"))   if w3  else None,
        "3M Weekly Volume Up":  _pf(w3.get("volume_up"))  if w3  else None,
        "12M Weekly Price Up":  _pf(w12.get("price_up"))  if w12 else None,
        "12M Weekly Volume Up": _pf(w12.get("volume_up")) if w12 else None,
    }

    detail = {
        # 3M
        "3M Latest Date Range":  w3.get("latest_date_range"),
        "3M Prior Date Range":   w3.get("prior_date_range"),
        "3M Latest Prices":      w3.get("latest_prices"),
        "3M Prior Prices":       w3.get("prior_prices"),
        "3M Latest Price Avg":   w3.get("latest_price_avg"),
        "3M Prior Price Avg":    w3.get("prior_price_avg"),
        "3M Price Change %":     w3.get("price_pct_change"),
        "3M Latest Volume Avg":  w3.get("latest_volume_avg"),
        "3M Prior Volume Avg":   w3.get("prior_volume_avg"),
        "3M Volume Change %":    w3.get("volume_pct_change"),
        # 12M
        "12M Latest Date Range": w12.get("latest_date_range"),
        "12M Prior Date Range":  w12.get("prior_date_range"),
        "12M Latest Prices":     w12.get("latest_prices"),
        "12M Prior Prices":      w12.get("prior_prices"),
        "12M Latest Price Avg":  w12.get("latest_price_avg"),
        "12M Prior Price Avg":   w12.get("prior_price_avg"),
        "12M Price Change %":    w12.get("price_pct_change"),
        "12M Latest Volume Avg": w12.get("latest_volume_avg"),
        "12M Prior Volume Avg":  w12.get("prior_volume_avg"),
        "12M Volume Change %":   w12.get("volume_pct_change"),
        "sub_checks": sub,
    }
    return {"pass": _grade(sub), "detail": detail}


# ─────────────────────────────────────────────────────────────────────────────
# T3 — MA Alignment
# ─────────────────────────────────────────────────────────────────────────────

def indicator_t3(tech: dict) -> dict:
    checks = tech.get("ma_checks") or {}
    vals   = tech.get("ma_values") or {}

    sub = {
        "MA10>MA20":   _pf(checks.get("MA10>MA20")),
        "MA20>MA50":   _pf(checks.get("MA20>MA50")),
        "MA50>MA150":  _pf(checks.get("MA50>MA150")),
        "MA150>MA200": _pf(checks.get("MA150>MA200")),
    }
    detail = {
        "MA10":  vals.get("MA10"),
        "MA20":  vals.get("MA20"),
        "MA50":  vals.get("MA50"),
        "MA150": vals.get("MA150"),
        "MA200": vals.get("MA200"),
        "sub_checks": sub,
    }
    return {"pass": _grade(sub), "detail": detail}


# ─────────────────────────────────────────────────────────────────────────────
# T4 — Big Moves (90-day, ≥10%)
# ─────────────────────────────────────────────────────────────────────────────

def indicator_t4(tech: dict) -> dict:
    up   = tech.get("big_up_events")   or []
    down = tech.get("big_down_events") or []

    sub = {
        "Big Up Days (≥+10%)":   _pf(len(up) > 0),
        "No Big Down Days (≥10% down)": _pf(len(down) == 0),
    }
    detail = {
        "Big Up Days Count":   len(up),
        "Big Down Days Count": len(down),
        "Big Up Events":       up,
        "Big Down Events":     down,
        "sub_checks": sub,
    }
    return {"pass": _grade(sub), "detail": detail}


# ─────────────────────────────────────────────────────────────────────────────
# F1 — Latest Quarter Profitability
# ─────────────────────────────────────────────────────────────────────────────

def indicator_f1(fund: dict) -> dict:
    rev = fund.get("q_revenue")
    eps = fund.get("q_eps")

    sub = {
        "Positive Q Revenue": _pf(rev > 0) if rev is not None else None,
        "Positive Q EPS":     _pf(eps > 0) if eps is not None else None,
    }
    detail = {"Q Revenue": rev, "Q EPS": eps, "sub_checks": sub}
    return {"pass": _grade(sub), "detail": detail}


# ─────────────────────────────────────────────────────────────────────────────
# F2 — Latest Year Profitability
# ─────────────────────────────────────────────────────────────────────────────

def indicator_f2(fund: dict) -> dict:
    rev = fund.get("a_revenue")
    eps = fund.get("a_eps")

    sub = {
        "Positive Annual Revenue": _pf(rev > 0) if rev is not None else None,
        "Positive Annual EPS":     _pf(eps > 0) if eps is not None else None,
    }
    detail = {"Annual Revenue": rev, "Annual EPS": eps, "sub_checks": sub}
    return {"pass": _grade(sub), "detail": detail}


# ─────────────────────────────────────────────────────────────────────────────
# F3 — Quarter YoY Growth (Revenue > +10%, EPS > +30%)
# ─────────────────────────────────────────────────────────────────────────────

def indicator_f3(fund: dict) -> dict:
    rev_yoy = fund.get("q_rev_yoy")
    eps_yoy = fund.get("q_eps_yoy")

    sub = {
        "Q Revenue YoY > +10%": _pf(rev_yoy > 10) if rev_yoy is not None else None,
        "Q EPS YoY > +30%":     _pf(eps_yoy > 30) if eps_yoy is not None else None,
    }
    detail = {
        "Q Revenue YoY %":  rev_yoy,
        "Q EPS YoY %":      eps_yoy,
        "Q Revenue Source": fund.get("q_rev_source"),
        "Q EPS Source":     fund.get("q_eps_source"),
        "Quarter End Date": fund.get("q_end_date"),
        "sub_checks": sub,
    }
    return {"pass": _grade(sub), "detail": detail}


# ─────────────────────────────────────────────────────────────────────────────
# F4 — Annual YoY Growth (Revenue > +10%, EPS > +30%)
# ─────────────────────────────────────────────────────────────────────────────

def indicator_f4(fund: dict) -> dict:
    rev_yoy = fund.get("a_rev_yoy")
    eps_yoy = fund.get("a_eps_yoy")

    sub = {
        "Annual Revenue YoY > +10%": _pf(rev_yoy > 10) if rev_yoy is not None else None,
        "Annual EPS YoY > +30%":     _pf(eps_yoy > 30) if eps_yoy is not None else None,
    }
    detail = {
        "Annual Revenue YoY %":  rev_yoy,
        "Annual EPS YoY %":      eps_yoy,
        "Annual Revenue Source": fund.get("a_rev_source"),
        "Annual EPS Source":     fund.get("a_eps_source"),
        "Fiscal Year End Date":  fund.get("a_end_date"),
        "sub_checks": sub,
    }
    return {"pass": _grade(sub), "detail": detail}


# ─────────────────────────────────────────────────────────────────────────────
# F5 — Forward PE vs Peers (no sub-indicators, binary)
# ─────────────────────────────────────────────────────────────────────────────

def indicator_f5(fund: dict, peer_data: dict) -> dict:
    fpe    = fund.get("forward_pe")
    median = peer_data.get("pe_median")
    peers  = peer_data.get("peers", [])
    vals   = peer_data.get("peer_forward_pe_values", [])

    if fpe is not None and fpe <= 0:
        result = "FAIL"
        pct_diff = round((fpe - median) / abs(median) * 100, 2) if median else None
    elif fpe is None or median is None:
        result = "NA"
        pct_diff = None
    else:
        result   = "PASS" if fpe <= median else "FAIL"
        pct_diff = round((fpe - median) / abs(median) * 100, 2) if median else None

    detail = {
        "Result":                result,
        "Ticker Fwd PE":         fpe,
        "Peer Median Fwd PE":    median,
        "Ticker vs Median %":    pct_diff,
        "Peer Tickers":          peers,
        "Peer Fwd PE Values":    vals,
        "Peers with PE Data":    len(vals),
    }
    return {"pass": result, "detail": detail}


# ─────────────────────────────────────────────────────────────────────────────
# F6 — P/B Ratio vs Peers (no sub-indicators, binary)
# ─────────────────────────────────────────────────────────────────────────────

def indicator_f6(fund: dict, peer_data: dict) -> dict:
    pb     = fund.get("pb_ratio")
    median = peer_data.get("pb_median")
    peers  = peer_data.get("peers", [])
    vals   = peer_data.get("peer_pb_values", [])

    if pb is None or median is None:
        result = "NA"
        pct_diff = None
    else:
        result   = "PASS" if (pb > 0 and pb <= median) else "FAIL"
        pct_diff = round((pb - median) / abs(median) * 100, 2) if median else None

    detail = {
        "Result":              result,
        "Ticker P/B":          pb,
        "Peer Median P/B":     median,
        "Ticker vs Median %":  pct_diff,
        "Peer Tickers":        peers,
        "Peer P/B Values":     vals,
        "Peers with P/B Data": len(vals),
    }
    return {"pass": result, "detail": detail}


# ─────────────────────────────────────────────────────────────────────────────
# Master evaluator
# ─────────────────────────────────────────────────────────────────────────────

def evaluate_all(ticker: str, tech: dict, fund: dict, peer_data: dict) -> dict:
    return {
        "T1": indicator_t1(tech),
        "T2": indicator_t2(tech),
        "T3": indicator_t3(tech),
        "T4": indicator_t4(tech),
        "F1": indicator_f1(fund),
        "F2": indicator_f2(fund),
        "F3": indicator_f3(fund),
        "F4": indicator_f4(fund),
        "F5": indicator_f5(fund, peer_data),
        "F6": indicator_f6(fund, peer_data),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Scoring for scan ranking (half credit for PARTIAL)
# ─────────────────────────────────────────────────────────────────────────────

def score_indicators(indicators: dict) -> float:
    """
    Returns a score 0..10 for ranking scan results.
    PASS=1, PARTIAL=0.5, FAIL/NA=0.
    """
    score = 0.0
    for ind in indicators.values():
        p = ind.get("pass", "NA")
        if p == "PASS":
            score += 1.0
        elif p == "PARTIAL":
            score += 0.5
    return score
