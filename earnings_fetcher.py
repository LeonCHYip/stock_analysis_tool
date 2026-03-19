"""
earnings_fetcher.py — Finviz earnings history scraper + gap-fill + backfill.

Scrapes https://finviz.com/calendar/earnings for each trading day,
stores results in earnings_history + earnings_fetch_log DuckDB tables.

Usage:
  python earnings_fetcher.py --backfill /path/to/finviz_earnings_all.json
  python earnings_fetcher.py --daily --lookback 30
"""

from __future__ import annotations
import argparse
import json
import random
import time
from datetime import date, timedelta
from pathlib import Path

import requests
import yfinance as yf
from bs4 import BeautifulSoup

import storage
from storage import init_db

# ── Configuration ──────────────────────────────────────────────────────────────
SORT = "-oneDayPriceReaction"
DELAY_BETWEEN_PAGES = (1.5, 3.0)
DELAY_BETWEEN_DATES = (3.0, 6.0)


# ── Helpers ────────────────────────────────────────────────────────────────────

def _make_headers(referer: str) -> dict:
    return {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": referer,
        "X-Requested-With": "XMLHttpRequest",
    }


def _fmt_surprise(val) -> float | None:
    """Return surprise as a plain float (e.g. 63.89 for +63.89%), or None."""
    if val is None:
        return None
    try:
        return round(float(val), 4)
    except Exception:
        return None


def _parse_pct_str(s) -> float | None:
    """Parse a formatted surprise string like '+63.89%' or '—' back to a float."""
    if s is None or str(s).strip() in ("—", "", "N/A", "None"):
        return None
    try:
        return round(float(str(s).replace("%", "")), 4)
    except Exception:
        return None


def parse_item(item: dict) -> dict:
    """Extract and normalize fields from one Finviz API item."""
    earnings_date_str = str(item.get("earningsDate", ""))[:10]
    earnings_time = "AMC" if "16:30" in str(item.get("earningsDate", "")) else "BMO"
    return {
        "earnings_date":  earnings_date_str,
        "earnings_time":  earnings_time,
        "eps_est":        item.get("epsEstimate"),
        "eps_act":        item.get("epsActual"),
        "eps_sur":        _fmt_surprise(item.get("epsSurprise")),
        "eps_gaap_est":   item.get("epsReportedEstimate"),
        "eps_gaap_act":   item.get("epsReportedActual"),
        "eps_gaap_sur":   _fmt_surprise(item.get("epsReportedSurprise")),
        "rev_est_m":      item.get("salesEstimate"),
        "rev_act_m":      item.get("salesActual"),
        "rev_sur":        _fmt_surprise(item.get("salesSurprise")),
        "one_day_change": _fmt_surprise(item.get("oneDayPriceReaction")),
    }


def _extract_items(payload: dict) -> list:
    """Handle both Finviz HTML bootstrap and API response shapes."""
    if "data" in payload:
        payload = payload["data"]
    if "entries" in payload:
        return payload["entries"].get("items", [])
    return payload.get("items", [])


def fetch_earnings_for_date(session: requests.Session, date_str: str) -> dict:
    """Fetch all pages of earnings for a single date. Returns {ticker: fields_dict}."""
    base_url = f"https://finviz.com/calendar/earnings?dateFrom={date_str}&sort={SORT}"
    api_url  = "https://finviz.com/api/calendar/earnings"
    earnings: dict = {}

    resp = session.get(base_url, headers=_make_headers("https://finviz.com/"), timeout=15)
    if resp.status_code == 404:
        print(f"  {date_str}: no data (404)")
        return {}
    if resp.status_code != 200:
        print(f"  {date_str}: HTTP {resp.status_code} — skipping")
        return {}

    soup = BeautifulSoup(resp.text, "html.parser")
    script_tag = soup.find("script", id="route-init-data")
    if not script_tag:
        print(f"  {date_str}: no route-init-data — no earnings this day")
        return {}

    bootstrap   = json.loads(script_tag.string)
    items_p1    = _extract_items(bootstrap)
    total_pages = (
        bootstrap.get("data", bootstrap)
        .get("entries", {})
        .get("totalPages", 1)
    )

    for item in items_p1:
        if item.get("ticker"):
            earnings[item["ticker"]] = parse_item(item)

    print(f"  {date_str}: page 1/{total_pages} → {len(earnings)} tickers", end="", flush=True)

    for page in range(2, total_pages + 1):
        time.sleep(random.uniform(*DELAY_BETWEEN_PAGES))
        params  = {"dateFrom": date_str, "page": page, "sort": SORT}
        headers = _make_headers(f"{base_url}&page={page - 1}")
        try:
            r = session.get(api_url, params=params, headers=headers, timeout=15)
            if r.status_code == 429:
                wait = 30
                print(f"\n  Rate-limited (429) — waiting {wait}s …")
                time.sleep(wait)
                r = session.get(api_url, params=params, headers=headers, timeout=15)
            if r.status_code != 200:
                print(f"\n  {date_str} page {page}: HTTP {r.status_code} — stopping")
                break
            items = _extract_items(r.json())
            for item in items:
                if item.get("ticker"):
                    earnings[item["ticker"]] = parse_item(item)
            print(f" | p{page} +{len(items)}", end="", flush=True)
        except Exception as e:
            print(f"\n  {date_str} page {page} error: {e}")
            break

    print(f"  →  total {len(earnings)}")
    return earnings


def _get_trading_days(start: date, end: date) -> list[str]:
    """Return list of YYYY-MM-DD strings for NYSE trading days in [start, end]."""
    try:
        import pandas_market_calendars as mcal
        nyse = mcal.get_calendar("NYSE")
        schedule = nyse.schedule(
            start_date=start.isoformat(),
            end_date=end.isoformat(),
        )
        return [str(d.date()) for d in schedule.index]
    except Exception:
        # Fallback: weekdays only
        days = []
        cur = start
        while cur <= end:
            if cur.weekday() < 5:
                days.append(cur.isoformat())
            cur += timedelta(days=1)
        return days


def get_missing_dates(lookback_days: int = 30,
                      since_date: str | None = None) -> list[str]:
    """Return sorted list of dates to fetch, combining two sources:

    1. Trading days not yet in earnings_fetch_log (new days to scrape).
    2. Dates already in earnings_history but with one_day_change IS NULL for
       at least one ticker — Finviz may not have posted the reaction yet when
       we first scraped that day, so re-fetch until populated.

    If since_date (YYYY-MM-DD) is given, use it as the window start instead of
    today - lookback_days.
    """
    today     = date.today()
    today_str = today.isoformat()
    start     = date.fromisoformat(since_date) if since_date else today - timedelta(days=lookback_days)
    start_str = start.isoformat()

    # Source 1: trading days in window not yet logged
    fetched  = storage.get_fetched_earnings_dates()
    trading  = _get_trading_days(start, today)
    missing  = {d for d in trading if d not in fetched and d <= today_str}

    # Source 2: dates within window that have entries with null one_day_change
    null_change = set(storage.get_earnings_dates_with_null_change(min_date=start_str))

    all_dates = sorted(missing | null_change)
    if all_dates:
        sources = []
        if missing & set(all_dates):
            sources.append(f"{len(missing)} new")
        if null_change:
            sources.append(f"{len(null_change)} with null 1D change")
        print(f"[earnings] Dates to fetch: {', '.join(sources)} → {all_dates}")
    return all_dates


def _compute_1d_changes(date_str: str, earnings_dict: dict) -> None:
    """Fetch Yahoo Finance prices and compute actual 1-day post-earnings change %.

    BMO: change% = (close_on_earnings_date - close_prior_day) / close_prior_day * 100
    AMC: change% = (close_next_trading_day - close_earnings_date) / close_earnings_date * 100

    Updates earnings_history.one_day_change in-place for each ticker where data exists.
    """
    if not earnings_dict:
        return

    tickers = list(earnings_dict.keys())
    d = date.fromisoformat(date_str)

    # Get a window of trading days to find prev/next day
    window_start = d - timedelta(days=7)
    window_end   = d + timedelta(days=7)
    try:
        trading_days = _get_trading_days(window_start, window_end)
    except Exception:
        # fallback: weekdays
        trading_days = [
            (window_start + timedelta(days=i)).isoformat()
            for i in range((window_end - window_start).days + 1)
            if (window_start + timedelta(days=i)).weekday() < 5
        ]

    prev_day = next_day = None
    for i, td in enumerate(trading_days):
        if td == date_str:
            if i > 0:
                prev_day = trading_days[i - 1]
            if i + 1 < len(trading_days):
                next_day = trading_days[i + 1]
            break

    # Determine the date range we need price data for
    fetch_start = prev_day or date_str
    fetch_end_dt = date.fromisoformat(next_day) if next_day else d
    fetch_end_str = (fetch_end_dt + timedelta(days=1)).isoformat()  # yf end is exclusive

    print(f"  [1D change] Fetching prices {fetch_start}→{fetch_end_dt} for {len(tickers)} tickers…", flush=True)
    try:
        raw = yf.download(
            tickers if len(tickers) > 1 else tickers[0],
            start=fetch_start,
            end=fetch_end_str,
            auto_adjust=True,
            progress=False,
            threads=True,
        )
        if raw.empty:
            print("  [1D change] No price data returned — skipping")
            return
        closes = raw["Close"] if "Close" in raw.columns else raw
    except Exception as e:
        print(f"  [1D change] Price fetch error: {e}")
        return

    # Normalise to a DataFrame with ticker columns
    if hasattr(closes, "columns"):
        close_df = closes
    else:
        # Single-ticker Series → single-column DataFrame
        close_df = closes.to_frame(name=tickers[0])

    # Build date→close lookup per ticker
    close_df.index = close_df.index.strftime("%Y-%m-%d")

    updated = 0
    for ticker, rec in earnings_dict.items():
        etime = rec.get("earnings_time", "BMO")
        try:
            if ticker not in close_df.columns:
                continue
            series = close_df[ticker].dropna()
            closes_map = series.to_dict()  # {date_str: price}

            if etime == "BMO":
                if prev_day and date_str in closes_map and prev_day in closes_map:
                    chg = (closes_map[date_str] - closes_map[prev_day]) / closes_map[prev_day] * 100
                    storage.update_earnings_1d_change(ticker, rec.get("earnings_date", date_str), round(chg, 4))
                    updated += 1
            else:  # AMC
                if next_day and next_day in closes_map and date_str in closes_map:
                    chg = (closes_map[next_day] - closes_map[date_str]) / closes_map[date_str] * 100
                    storage.update_earnings_1d_change(ticker, rec.get("earnings_date", date_str), round(chg, 4))
                    updated += 1
        except Exception:
            pass  # silently skip individual ticker failures

    print(f"  [1D change] Updated {updated}/{len(tickers)} tickers")


def store_earnings_batch(date_str: str, earnings_dict: dict) -> int:
    """Persist one day's earnings dict, log the fetch, then compute 1D price changes.

    Returns record count.
    """
    today = date.today().isoformat()
    for ticker, rec in earnings_dict.items():
        rec["fetch_date"] = today
        storage.save_earnings(ticker, rec)
    storage.mark_earnings_date_fetched(date_str, len(earnings_dict))
    # Compute real 1D change from Yahoo Finance prices (overrides Finviz value)
    try:
        _compute_1d_changes(date_str, earnings_dict)
    except Exception as e:
        print(f"  [1D change] Failed for {date_str}: {e}")
    return len(earnings_dict)


def backfill_from_json(json_path: str) -> int:
    """Load a historical Finviz JSON file and insert records into earnings_history."""
    data  = json.loads(Path(json_path).read_text())
    today = date.today().isoformat()
    stored = 0
    for ticker, rec in data.items():
        earnings_date = rec.get("Date")
        if not earnings_date:
            continue
        storage.save_earnings(ticker, {
            "earnings_date":  earnings_date,
            "earnings_time":  rec.get("Time"),
            "eps_est":        rec.get("EPS Est"),
            "eps_act":        rec.get("EPS Act"),
            "eps_sur":        _parse_pct_str(rec.get("EPS Sur")),
            "eps_gaap_est":   rec.get("EPS GAAP Est"),
            "eps_gaap_act":   rec.get("EPS GAAP Act"),
            "eps_gaap_sur":   _parse_pct_str(rec.get("EPS GAAP Sur")),
            "rev_est_m":      rec.get("Revenue Est"),
            "rev_act_m":      rec.get("Revenue Act"),
            "rev_sur":        _parse_pct_str(rec.get("Revenue Sur")),
            "one_day_change": _parse_pct_str(rec.get("1-Day Change")),
            "fetch_date":     today,
        })
        stored += 1
    return stored


def compute_missing_1d_changes() -> int:
    """Compute Yahoo 1D price changes for earnings records already in DB but missing change %.

    Called for dates that are in earnings_fetch_log (Finviz data exists) but still have
    null one_day_change — e.g., backfilled records or AMC earnings where next-day price
    wasn't available at fetch time.
    Returns number of tickers updated.
    """
    fetched   = storage.get_fetched_earnings_dates()
    null_dates = storage.get_earnings_dates_with_null_change()
    # Only process dates where we already have Finviz earnings data in DB
    dates = [d for d in null_dates if d in fetched]
    if not dates:
        return 0
    print(f"[earnings] Computing Yahoo 1D change for {len(dates)} already-fetched date(s)…")
    total = 0
    for date_str in dates:
        try:
            records = storage.get_earnings_for_date(date_str)
            if records:
                _compute_1d_changes(date_str, records)
                total += len(records)
        except Exception as e:
            print(f"  [1D change] Failed for {date_str}: {e}")
    return total


def run_daily_fetch(lookback_days: int = 7,
                    since_date: str | None = None) -> int:
    """Fetch all missing trading days and store results. Returns total tickers stored.

    If since_date (YYYY-MM-DD) is provided, scan from that date instead of
    using lookback_days.
    """
    missing = get_missing_dates(lookback_days, since_date=since_date)
    session = requests.Session()
    total   = 0
    for d in missing:
        print(f"[earnings] Fetching {d}...")
        try:
            earnings = fetch_earnings_for_date(session, d)
            total   += store_earnings_batch(d, earnings)
        except Exception as e:
            print(f"[earnings] Error fetching {d}: {e}")
        if d != missing[-1]:
            time.sleep(random.uniform(*DELAY_BETWEEN_DATES))
    if missing:
        print(f"[earnings] Done: {total} records across {len(missing)} days.")
    # For already-fetched dates still missing 1D change (e.g., AMC next-day catch-up)
    try:
        compute_missing_1d_changes()
    except Exception as e:
        print(f"[earnings] compute_missing_1d_changes error: {e}")
    if not missing:
        print("[earnings] Up to date.")
    return total


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Finviz earnings fetcher")
    parser.add_argument("--backfill", nargs="+", metavar="JSON",
                        help="JSON file paths to backfill from")
    parser.add_argument("--daily", action="store_true",
                        help="Run daily fetch with gap-filling")
    parser.add_argument("--lookback", type=int, default=30,
                        help="Days to look back for gaps (default: 30)")
    args = parser.parse_args()

    init_db()

    if args.backfill:
        for path in args.backfill:
            n = backfill_from_json(path)
            print(f"Backfilled {n} records from {path}")

    if args.daily:
        run_daily_fetch(args.lookback)
