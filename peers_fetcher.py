"""
peers_fetcher.py
----------------
Fetches competitor tickers via Yahoo Finance recommendations API,
then retrieves their forward PE and P/B ratios via yfinance.

In-memory caches (_competitors_cache, _valuation_cache) persist for the
lifetime of the process.  Call clear_peer_cache() at the start of each
new scan to ensure fresh data.
"""

import time
import requests
import yfinance as yf
import numpy as np

_YAHOO_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
}

_RETRY_DELAYS = [5, 10, 20]

# ── In-memory caches ──────────────────────────────────────────────────────────
_competitors_cache: dict[str, list[str]] = {}   # ticker → [peer, ...]
_valuation_cache:   dict[str, dict]      = {}   # ticker → {forward_pe, pb}


def clear_peer_cache() -> None:
    """Reset both caches.  Call at the start of each scan run."""
    _competitors_cache.clear()
    _valuation_cache.clear()


# ── Competitor tickers ────────────────────────────────────────────────────────

def get_competitor_tickers(ticker: str) -> list[str]:
    """Returns list of peer tickers from Yahoo Finance recommendationsbysymbol endpoint."""
    key = ticker.upper()
    if key in _competitors_cache:
        return _competitors_cache[key]

    url = f"https://query1.finance.yahoo.com/v6/finance/recommendationsbysymbol/{key}"
    for attempt, delay in enumerate([0] + _RETRY_DELAYS, 0):
        if delay:
            time.sleep(delay)
        try:
            resp = requests.get(url, headers=_YAHOO_HEADERS, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            results = data.get("finance", {}).get("result", [])
            if not results:
                _competitors_cache[key] = []
                return []
            symbols = [
                item["symbol"]
                for item in results[0].get("recommendedSymbols", [])
                if "symbol" in item
            ]
            peers = [s for s in symbols if s.upper() != key]
            _competitors_cache[key] = peers
            return peers
        except Exception as e:
            if attempt < len(_RETRY_DELAYS):
                print(f"  [peers] Retry {attempt+1} for {ticker} competitors after {_RETRY_DELAYS[attempt]}s: {e}")
            else:
                print(f"  [peers] Failed to get competitors for {ticker}: {e}")

    _competitors_cache[key] = []
    return []


# ── Per-peer valuation ────────────────────────────────────────────────────────

def _get_valuation(ticker: str) -> dict:
    """Fetch forward PE and P/B for a single ticker (cached, throttled, no retry).

    Peer valuations are best-effort — a single attempt is sufficient.
    Retrying causes excessive delays for delisted/404 tickers because yfinance
    handles HTTP 404 internally and returns an empty dict without raising an
    exception, making it indistinguishable from a rate-limit at call time.
    """
    key = ticker.upper()
    if key in _valuation_cache:
        return _valuation_cache[key]

    time.sleep(0.3)   # throttle concurrent peer calls

    def _clean(v):
        try:
            f = float(v)
            return None if (np.isnan(f) or np.isinf(f) or f <= 0) else round(f, 2)
        except Exception:
            return None

    empty = {"forward_pe": None, "pb": None}
    try:
        info = yf.Ticker(ticker).info
        if len(info) >= 5:
            result = {"forward_pe": _clean(info.get("forwardPE")),
                      "pb":         _clean(info.get("priceToBook"))}
            _valuation_cache[key] = result
            return result
    except Exception:
        pass

    _valuation_cache[key] = empty
    return empty


# ── Public API ────────────────────────────────────────────────────────────────

def get_peer_valuations(ticker: str, skip_peers: bool = False) -> dict:
    """
    Returns peer valuation stats used to benchmark a ticker's forward PE and P/B.

    skip_peers=True: returns an empty result immediately (F5/F6 will be NA).
    """
    empty = {
        "peers":                  [],
        "peer_forward_pe_values": [],
        "peer_pb_values":         [],
        "pe_median":              None,
        "pb_median":              None,
    }

    if skip_peers:
        return empty

    peers   = get_competitor_tickers(ticker)
    pe_vals = []
    pb_vals = []

    for p in peers:
        vals = _get_valuation(p)
        if vals["forward_pe"] is not None:
            pe_vals.append(vals["forward_pe"])
        if vals["pb"] is not None:
            pb_vals.append(vals["pb"])

    def _median(lst):
        return round(float(np.median(lst)), 2) if lst else None

    return {
        "peers":                  peers,
        "peer_forward_pe_values": pe_vals,
        "peer_pb_values":         pb_vals,
        "pe_median":              _median(pe_vals),
        "pb_median":              _median(pb_vals),
    }
