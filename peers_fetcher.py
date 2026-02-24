"""
peers_fetcher.py
----------------
Fetches competitor tickers via Yahoo Finance recommendations API,
then retrieves their forward PE and P/B ratios via yfinance.
"""

import requests
import yfinance as yf
import numpy as np

_YAHOO_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
}


def get_competitor_tickers(ticker):
    """Returns list of peer tickers from Yahoo Finance recommendationsbysymbol endpoint."""
    try:
        url  = f"https://query1.finance.yahoo.com/v6/finance/recommendationsbysymbol/{ticker.upper()}"
        resp = requests.get(url, headers=_YAHOO_HEADERS, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        results = data.get("finance", {}).get("result", [])
        if not results:
            return []
        symbols = [
            item["symbol"]
            for item in results[0].get("recommendedSymbols", [])
            if "symbol" in item
        ]
        return [s for s in symbols if s.upper() != ticker.upper()]
    except Exception as e:
        print(f"  [peers] Failed to get competitors for {ticker}: {e}")
        return []


def _get_valuation(ticker):
    """Fetch forward PE and P/B for a single ticker via yfinance."""
    try:
        info = yf.Ticker(ticker).info

        def _clean(v):
            try:
                f = float(v)
                return None if (np.isnan(f) or np.isinf(f) or f <= 0) else round(f, 2)
            except Exception:
                return None

        return {"forward_pe": _clean(info.get("forwardPE")),
                "pb":         _clean(info.get("priceToBook"))}
    except Exception:
        return {"forward_pe": None, "pb": None}


def get_peer_valuations(ticker):
    """
    Returns peer valuation stats used to benchmark a ticker's forward PE and P/B.
    Peers fetched via FMP; valuations fetched via yfinance (one call per peer).
    """
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
