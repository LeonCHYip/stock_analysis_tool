"""fx_rates.py — Currency conversion rates for non-USD-reporting tickers.

Some watchlist tickers report quarterly/annual financials in a non-USD
currency (see fundamentals.financial_currency, captured in
fundamental_fetcher.py from Yahoo's financialData.financialCurrency). This
module fetches current USD conversion rates for exactly those currencies via
Yahoo's "{CUR}=X" tickers, so app.py can convert at display time.

Yahoo's "{CUR}=X" quote is uniformly LOCAL currency units per 1 USD --
confirmed empirically across EUR, GBP, CNY, JPY, CAD, BRL (e.g. JPY=X ~159,
EUR=X ~0.86, both consistent with "units of that currency per 1 USD", not
the reverse -- there is no direct-quote exception for any currency). So
rate_to_usd is always stored as 1/raw_close, meaning downstream conversion
code just does local_amount * rate_to_usd with no per-currency branching.

This is a spot conversion using today's rate applied to whatever period the
underlying financial figure was reported for -- not a historical, period-
specific rate. Simple and adequate for screening purposes; not accounting-
grade precision.
"""
from __future__ import annotations
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import yfinance as yf

import storage
from yf_session import YF_SESSION, YF_DL_LOCK

CST = ZoneInfo("America/Chicago")


def refresh_fx_rates(log=print) -> int:
    """Fetch current FX rates for every non-USD currency currently in use
    across fundamentals.financial_currency, and persist to storage.fx_rates.
    One small bulk yf.download() call, not one call per currency. Safe to
    call on every scan. Returns the number of currencies successfully
    updated (a currency Yahoo has no reliable "=X" quote for is simply
    skipped, not a hard failure -- its conversion stays unavailable
    downstream rather than using a wrong/stale rate).
    """
    currencies = storage.get_distinct_financial_currencies()
    if not currencies:
        return 0

    pairs = [f"{c}=X" for c in currencies]
    now = datetime.now(CST)
    rows = []
    got: set[str] = set()

    # ── Pass 1: one bulk download for all pairs ───────────────────────────────
    try:
        with YF_DL_LOCK:  # see yf_session.py -- concurrent downloads cross-contaminate
            raw = yf.download(
                tickers=pairs,
                period="5d",
                group_by="ticker",
                auto_adjust=False,
                threads=False,
                progress=False,
                session=YF_SESSION,
            )
    except Exception as e:
        log(f"[fx] bulk download failed: {e}")
        raw = None

    if raw is not None and not raw.empty:
        is_multi = isinstance(raw.columns, pd.MultiIndex)
        for currency, pair in zip(currencies, pairs):
            try:
                if is_multi:
                    if pair not in raw.columns.get_level_values(0):
                        continue
                    series = raw[pair]["Close"].dropna()
                else:
                    # Only happens when a single currency was requested.
                    series = raw["Close"].dropna()
                if series.empty:
                    continue
                local_units_per_usd = float(series.iloc[-1])
                if local_units_per_usd == 0:
                    continue
                rows.append({
                    "currency": currency,
                    "rate_to_usd": 1.0 / local_units_per_usd,
                    "updated_at": now,
                })
                got.add(currency)
            except Exception as e:
                log(f"[fx] Failed to process {pair}: {e}")

    # ── Pass 2: retry stragglers individually ─────────────────────────────────
    # Yahoo's multi-symbol bulk endpoint intermittently drops some symbols
    # (observed: several =X pairs failing with TypeError inside yfinance
    # while others in the same call succeed). Single-symbol history() goes
    # through the chart endpoint, which has proven much more reliable --
    # retry only what the bulk pass missed. Currencies that still fail keep
    # their previously saved rate (save_fx_rates upserts, never deletes), so
    # a transient failure degrades to a slightly stale rate, not a missing
    # conversion.
    _missing = [c for c in currencies if c not in got]
    for currency in _missing:
        try:
            with YF_DL_LOCK:
                hist = yf.Ticker(f"{currency}=X", session=YF_SESSION).history(period="5d")
            closes = hist["Close"].dropna() if hist is not None and not hist.empty else None
            if closes is None or closes.empty:
                continue
            local_units_per_usd = float(closes.iloc[-1])
            if local_units_per_usd == 0:
                continue
            rows.append({
                "currency": currency,
                "rate_to_usd": 1.0 / local_units_per_usd,
                "updated_at": now,
            })
            got.add(currency)
        except Exception as e:
            log(f"[fx] Individual retry failed for {currency}=X: {e}")

    if rows:
        storage.save_fx_rates(rows)
    log(f"[fx] Refreshed {len(got)}/{len(currencies)} currencies"
        + (f"  (still missing: {sorted(set(currencies) - got)})" if len(got) < len(currencies) else ""))
    return len(got)
