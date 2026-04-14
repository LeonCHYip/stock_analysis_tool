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


def _pct_chg(new_val, old_val) -> float | None:
    """Return (new-old)/old*100 rounded to 4dp, or None if inputs invalid."""
    try:
        if new_val is None or old_val is None or old_val == 0:
            return None
        return round((new_val - old_val) / old_val * 100, 4)
    except Exception:
        return None


def _avg(values: list) -> float | None:
    vals = [v for v in values if v is not None]
    return sum(vals) / len(vals) if vals else None


def _compute_1d_changes(date_str: str, earnings_dict: dict) -> None:
    """Compute earnings return metrics from price_history (no external API calls).

    Updates:
      - earnings_history.one_day_change  (existing)
      - earns_1d_vol_pct, earns_5d_px_pct, earns_5d_vol_pct,
        earns_5d_roll_px_pct, earns_5d_roll_vol_pct  (new)

    BMO windows: pre = [-5..-1], post = [0..4]
    AMC windows: pre = [-4..0],  post = [1..5]
    """
    if not earnings_dict:
        return

    tickers = list(earnings_dict.keys())
    d = date.fromisoformat(date_str)

    # Query price_history — ±14 calendar days gives enough trading days on both sides
    window_start = (d - timedelta(days=14)).isoformat()
    window_end   = (d + timedelta(days=14)).isoformat()
    price_data = storage.get_price_history_range(tickers, window_start, window_end)

    updated = 0
    for ticker, rec in earnings_dict.items():
        etime = rec.get("earnings_time", "BMO")
        earn_date = rec.get("earnings_date", date_str)
        try:
            ticker_data = price_data.get(ticker, {})
            if not ticker_data:
                continue

            # Per-ticker sorted trading days from price_history
            trading_days = sorted(ticker_data.keys())
            if date_str not in trading_days:
                continue
            earnings_idx = trading_days.index(date_str)

            c_map = {d: ticker_data[d]["close"]  for d in trading_days if ticker_data[d]["close"]  is not None}
            v_map = {d: ticker_data[d]["volume"] for d in trading_days if ticker_data[d]["volume"] is not None}
            prev_day = trading_days[earnings_idx - 1] if earnings_idx > 0 else None

            if etime == "BMO":
                # ── 1D change ────────────────────────────────────────────────
                if prev_day and date_str in c_map and prev_day in c_map:
                    storage.update_earnings_1d_change(
                        ticker, earn_date,
                        round((c_map[date_str] - c_map[prev_day]) / c_map[prev_day] * 100, 4),
                    )
                    updated += 1

                # ── Extended metrics ─────────────────────────────────────────
                ext = {}
                if v_map and prev_day and date_str in v_map and prev_day in v_map:
                    ext["earns_1d_vol_pct"] = _pct_chg(v_map[date_str], v_map[prev_day])
                if earnings_idx + 4 < len(trading_days):
                    d4 = trading_days[earnings_idx + 4]
                    ext["earns_5d_px_pct"]  = _pct_chg(c_map.get(d4), c_map.get(prev_day))
                    ext["earns_5d_vol_pct"] = _pct_chg(v_map.get(d4), v_map.get(prev_day)) if v_map and prev_day else None
                pre_days  = [trading_days[i] for i in range(earnings_idx - 5, earnings_idx)      if 0 <= i < len(trading_days)]
                post_days = [trading_days[i] for i in range(earnings_idx,     earnings_idx + 5)  if 0 <= i < len(trading_days)]
                ext["earns_5d_roll_px_pct"]  = _pct_chg(_avg([c_map.get(d) for d in post_days]), _avg([c_map.get(d) for d in pre_days]))
                if v_map:
                    ext["earns_5d_roll_vol_pct"] = _pct_chg(_avg([v_map.get(d) for d in post_days]), _avg([v_map.get(d) for d in pre_days]))
                if ext:
                    storage.update_earnings_extended(ticker, earn_date, ext)

            else:  # AMC
                next_day = trading_days[earnings_idx + 1] if earnings_idx + 1 < len(trading_days) else None

                # ── 1D change ────────────────────────────────────────────────
                if next_day and next_day in c_map and date_str in c_map:
                    storage.update_earnings_1d_change(
                        ticker, earn_date,
                        round((c_map[next_day] - c_map[date_str]) / c_map[date_str] * 100, 4),
                    )
                    updated += 1

                # ── Extended metrics ─────────────────────────────────────────
                ext = {}
                if v_map and next_day and next_day in v_map and date_str in v_map:
                    ext["earns_1d_vol_pct"] = _pct_chg(v_map[next_day], v_map[date_str])
                if earnings_idx + 5 < len(trading_days):
                    d5 = trading_days[earnings_idx + 5]
                    ext["earns_5d_px_pct"]  = _pct_chg(c_map.get(d5), c_map.get(date_str))
                    ext["earns_5d_vol_pct"] = _pct_chg(v_map.get(d5), v_map.get(date_str)) if v_map else None
                pre_days  = [trading_days[i] for i in range(earnings_idx - 4, earnings_idx + 1) if 0 <= i < len(trading_days)]
                post_days = [trading_days[i] for i in range(earnings_idx + 1, earnings_idx + 6) if 0 <= i < len(trading_days)]
                ext["earns_5d_roll_px_pct"]  = _pct_chg(_avg([c_map.get(d) for d in post_days]), _avg([c_map.get(d) for d in pre_days]))
                if v_map:
                    ext["earns_5d_roll_vol_pct"] = _pct_chg(_avg([v_map.get(d) for d in post_days]), _avg([v_map.get(d) for d in pre_days]))
                if ext:
                    storage.update_earnings_extended(ticker, earn_date, ext)

        except Exception:
            pass  # silently skip individual ticker failures

    print(f"  [earnings ext] {date_str}: updated {updated}/{len(tickers)} tickers from price_history")


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


def compute_missing_1d_changes(extended_lookback_days: int = 10) -> int:
    """Compute Yahoo 1D price changes for recent earnings records missing change % or extended columns.

    Daily check: covers the last `extended_lookback_days` calendar days (~7 trading days),
    which is enough to catch AMC next-day prices and 5D post-earnings data settling.

    For a one-time historical backfill of older records use backfill_extended_columns().
    Returns number of tickers processed.
    """
    fetched = storage.get_fetched_earnings_dates()

    # Dates missing one_day_change — only recent (same window)
    recent_cutoff = (date.today() - timedelta(days=extended_lookback_days)).isoformat()
    null_change_dates = set(storage.get_earnings_dates_with_null_change(min_date=recent_cutoff))

    # Dates missing extended columns — same recent window
    null_ext_dates = set(storage.get_earnings_dates_with_null_extended(min_date=recent_cutoff))

    # Union: process any date that needs work AND has Finviz data
    dates = sorted((null_change_dates | null_ext_dates) & fetched)
    if not dates:
        return 0

    print(f"[earnings] Computing 1D/extended changes for {len(dates)} date(s)…")
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


def recompute_extended_columns() -> int:
    """Recompute extended earnings columns for ALL historical records (even non-NULL).

    Use this after fixing BMO/AMC reference-day logic.  Steps:
      1. NULL out the three columns that were computed with wrong reference days:
         - earns_5d_px_pct  for BMO rows
         - earns_1d_vol_pct for AMC rows
         - earns_5d_vol_pct for AMC rows
      2. NULL out all five extended columns for every other row (so backfill
         picks them up if price_history has been updated).
      3. Delegate to backfill_extended_columns() which processes all NULL rows.

    No external API calls — reads only from price_history.
    Returns number of tickers processed.
    """
    con = storage._conn()

    # Step 1: NULL the columns that had wrong reference-day logic
    con.execute("""
        UPDATE earnings_history
        SET earns_5d_px_pct = NULL
        WHERE earnings_time = 'BMO'
    """)
    con.execute("""
        UPDATE earnings_history
        SET earns_1d_vol_pct = NULL,
            earns_5d_vol_pct = NULL
        WHERE earnings_time = 'AMC'
    """)

    # Step 2: NULL the remaining extended columns for all rows so they are
    # recomputed fresh (picks up any price_history improvements too).
    con.execute("""
        UPDATE earnings_history
        SET earns_5d_px_pct      = NULL,
            earns_1d_vol_pct     = NULL,
            earns_5d_vol_pct     = NULL,
            earns_5d_roll_px_pct = NULL,
            earns_5d_roll_vol_pct = NULL
    """)

    print("[earnings recompute] Cleared all extended columns — recomputing from price_history…")
    return backfill_extended_columns()


def backfill_extended_columns() -> int:
    """One-time backfill: compute extended earnings columns for ALL historical records.

    Uses price_history (no external API calls). Covers all earnings records with valid
    earnings_date/time where any extended column is NULL — no date cutoff.
    Returns number of tickers processed.
    """
    # Get all earnings records with null extended columns, grouped by date
    by_date = storage.get_all_earnings_with_null_extended()
    if not by_date:
        print("[earnings backfill] Nothing to backfill.")
        return 0

    dates = sorted(by_date.keys())
    print(f"[earnings backfill] Backfilling {len(dates)} date(s) from price_history…")
    total = 0
    for date_str in dates:
        try:
            # Convert list of row dicts to {ticker: rec} format expected by _compute_1d_changes
            records = {r["ticker"]: r for r in by_date[date_str]}
            _compute_1d_changes(date_str, records)
            total += len(records)
        except Exception as e:
            print(f"  [backfill] Failed for {date_str}: {e}")
    print(f"[earnings backfill] Done: {total} tickers processed.")
    return total


def update_post_earns_columns() -> int:
    """Recompute post_earns_* columns for all tickers that have both earnings and tech data.

    Reference day:
      BMO → D-1 (last trading day before earnings day); pre 5-day avg = D-5..D-1
      AMC → D0  (earnings day itself);                  pre 5-day avg = D-4..D0
    Latest day = as_of_date from tech_indicators.
    Latest 5-day avg = 5 trading days ending at as_of_date.

    No external API calls — reads only from price_history and tech_indicators.
    Returns number of tickers updated.
    """
    ticker_info = storage.get_latest_earnings_with_as_of_dates()
    if not ticker_info:
        print("[post-earns] No earnings data found.")
        return 0

    # Skip tickers with no tech data or where latest date ≤ earnings date
    valid = {
        t: info for t, info in ticker_info.items()
        if info.get("as_of_date") and info["as_of_date"] > info["earnings_date"]
    }
    if not valid:
        print("[post-earns] No tickers with both earnings and tech data.")
        return 0

    # Group by earnings_date to batch price_history queries
    by_earnings_date: dict[str, dict] = {}
    for ticker, info in valid.items():
        ed = info["earnings_date"]
        by_earnings_date.setdefault(ed, {})[ticker] = info

    total = 0
    for earnings_date, group in sorted(by_earnings_date.items()):
        tickers = list(group.keys())
        max_as_of = max(info["as_of_date"] for info in group.values())
        window_start = (date.fromisoformat(earnings_date) - timedelta(days=14)).isoformat()

        price_data = storage.get_price_history_range(tickers, window_start, max_as_of)

        for ticker, info in group.items():
            et       = info["earnings_time"]
            ed       = info["earnings_date"]
            as_of    = info["as_of_date"]

            ticker_data = price_data.get(ticker, {})
            if not ticker_data:
                continue

            trading_days = sorted(ticker_data.keys())

            # Snap as_of to closest available trading day on or before as_of
            available_up_to = [d for d in trading_days if d <= as_of]
            if not available_up_to or ed not in trading_days:
                continue
            latest = available_up_to[-1]
            latest_idx    = trading_days.index(latest)
            earnings_idx  = trading_days.index(ed)

            c_map = {d: ticker_data[d]["close"]  for d in trading_days if ticker_data[d]["close"]  is not None}
            v_map = {d: ticker_data[d]["volume"] for d in trading_days if ticker_data[d]["volume"] is not None}

            # Pre-earnings anchor and 5-day pre window
            if et == "BMO":
                anchor   = trading_days[earnings_idx - 1] if earnings_idx > 0 else None
                pre_days = [trading_days[i] for i in range(earnings_idx - 5, earnings_idx)
                            if 0 <= i < len(trading_days)]
            else:  # AMC
                anchor   = ed
                pre_days = [trading_days[i] for i in range(earnings_idx - 4, earnings_idx + 1)
                            if 0 <= i < len(trading_days)]

            if not anchor or anchor not in c_map:
                continue

            # Latest 5-day window ending at latest
            latest_days = [trading_days[i] for i in range(latest_idx - 4, latest_idx + 1)
                           if 0 <= i < len(trading_days)]

            ext: dict = {}

            # Spot price & volume
            if latest in c_map:
                ext["post_earns_px_pct"] = _pct_chg(c_map[latest], c_map[anchor])
            if anchor in v_map and latest in v_map:
                ext["post_earns_vol_pct"] = _pct_chg(v_map[latest], v_map[anchor])

            # Rolling avg price & volume
            pre_avg_px  = _avg([c_map.get(d) for d in pre_days])
            late_avg_px = _avg([c_map.get(d) for d in latest_days])
            if pre_avg_px and late_avg_px:
                ext["post_earns_avg_px_pct"] = _pct_chg(late_avg_px, pre_avg_px)

            pre_avg_vol  = _avg([v_map.get(d) for d in pre_days])
            late_avg_vol = _avg([v_map.get(d) for d in latest_days])
            if pre_avg_vol and late_avg_vol:
                ext["post_earns_avg_vol_pct"] = _pct_chg(late_avg_vol, pre_avg_vol)

            if ext:
                storage.update_post_earns_extended(ticker, ed, ext)
                total += 1

    print(f"[post-earns] Updated {total} tickers.")
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
    parser.add_argument("--backfill-extended", action="store_true",
                        help="One-time backfill of extended earnings columns for all historical records")
    parser.add_argument("--recompute-extended", action="store_true",
                        help="Recompute all extended earnings columns (NULLs stale values first, then recomputes from price_history)")
    parser.add_argument("--update-post-earns", action="store_true",
                        help="Recompute post_earns_* columns (price/vol from earnings to latest close date)")
    args = parser.parse_args()

    init_db()

    if args.backfill:
        for path in args.backfill:
            n = backfill_from_json(path)
            print(f"Backfilled {n} records from {path}")

    if args.backfill_extended:
        backfill_extended_columns()

    if args.recompute_extended:
        recompute_extended_columns()

    if args.update_post_earns:
        update_post_earns_columns()

    if args.daily:
        run_daily_fetch(args.lookback)
