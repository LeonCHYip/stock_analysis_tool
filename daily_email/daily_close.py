#!/usr/bin/env python3
"""
daily_close.py -- Daily market close newsletter.

Builds an HTML digest after the NYSE close and (optionally) emails it via Gmail
SMTP.  Three sections:

  1. Index board      -- SOXX, DRAM, XLK, XLV, QQQ, XLU, XLI, MAGS, SPY, XLF,
                         XLE, plus gold and crypto in their own block.
  2. SOXL             -- RSI(14), daily move, and the top-5/bottom-5 movers
                         among its semiconductor constituents.
  3. Large-cap movers -- top-10 winners and losers among $10B+ stocks
                         (ex-ETFs), with sector, industry, a one-line reason
                         and news headlines from the last 48 hours.

WHY PRICES ARE FETCHED LIVE RATHER THAN READ FROM DUCKDB:
price_history contains no ETFs at all, the asset_type='etf' rows in
tech_indicators lag the stock rows by days, and GC=F / BTC-USD / ETH-USD are not
in the database in any form.  The database is therefore used *only* for the
things it is authoritative about -- market cap, sector and industry -- and is
opened READ-ONLY so this can run while the Streamlit app holds the write lock
(DuckDB allows a single writer).

Usage:
    uv run python daily_close.py                # build + archive, no email
    uv run python daily_close.py --email        # also send
    uv run python daily_close.py --force        # bypass the trading-day gate
    uv run python daily_close.py --open         # open the archived HTML
"""

from __future__ import annotations

import argparse
import json
import math
import re
import subprocess
import sys
import time
import webbrowser
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb
import pandas as pd
import ta
import yfinance as yf

HERE = Path(__file__).resolve().parent           # daily_email/
PROJECT_ROOT = HERE.parent                       # repo root: DB + shared modules
DB_PATH = PROJECT_ROOT / "stock_analysis_v2.duckdb"
REPORTS_DIR = HERE / "reports"
HOLDINGS_FILE = HERE / "soxx_holdings.txt"

# config.py and market_calendar.py live at the repo root and this repo is flat
# (no packages anywhere), so the root goes on sys.path before importing them.
# Appended, not inserted at 0, so this directory keeps priority for its own
# modules. Keeps `uv run python daily_email/daily_close.py` working from any cwd.
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

import config
import market_calendar as mc
import news_fetcher
# The repo's shared curl_cffi session. yfinance 1.x otherwise builds a new
# session per download and destroys the previous one, whose native handle
# aborts the process on GC (see yf_session.py) -- the same SIGABRT that killed
# the 16:45 run. YF_DL_LOCK serialises price downloads, which cross-contaminate
# across threads on a shared session.
from yf_session import YF_SESSION, YF_DL_LOCK
# Small snapshot (~100 KB) of ticker -> market cap/sector/industry, refreshed
# whenever the DB is readable so a locked DB still yields a full report.
UNIVERSE_CACHE = REPORTS_DIR / "universe_cache.json"
# One line per session actually emailed, so repeated attempts in a day produce
# exactly one newsletter. Needed because the NYSE calendar can say a session has
# closed hours before Yahoo publishes its bars, which makes a naive retry
# re-send the previous session's data as if it were new.
SENT_LOG = REPORTS_DIR / "sent_sessions.txt"
# Machine-readable outcome of the most recent run, overwritten every time.
# The exit code cannot carry this: a gate skip and a successful send both return
# 0, and an unhandled exception never reaches _flush_log() at all, so the log is
# silent about the runs that failed hardest. The Streamlit sidebar reads this to
# decide whether to tell the user the newsletter died.
RUN_STATE = REPORTS_DIR / "run_state.json"
# Rolling store of the SOXL sector narrative, newest last. The narrative has to
# read coherently against previous days, and nothing else in this file survives
# a run, so the previous sessions are fed back into the prompt from here.
NARRATIVE_STORE = REPORTS_DIR / "soxl_narrative.json"
NARRATIVE_KEEP = 10          # sessions retained on disk
NARRATIVE_CONTEXT = 3        # sessions fed into the prompt

CST = ZoneInfo("America/Chicago")
ET = ZoneInfo("America/New_York")  # repo convention: displayed timestamps are CST

# ── Index board ──────────────────────────────────────────────────────────────
# Order as requested. Equities settle on the NYSE session; gold and crypto trade
# around the clock and routinely print a *later* bar date, so they are fetched
# together but rendered in a separate block with their own as-of stamp.
INDEX_EQUITIES = [
    ("SOXX", "Semiconductors"),
    ("DRAM", "Memory / DRAM"),
    ("XLK",  "Technology"),
    ("XLV",  "Health Care"),
    ("QQQ",  "Nasdaq 100"),
    ("XLU",  "Utilities"),
    ("XLI",  "Industrials"),
    ("MAGS", "Magnificent 7"),
    ("SPY",  "S&P 500"),
    ("XLF",  "Financials"),
    ("XLE",  "Energy"),
]
INDEX_ALT = [
    ("GC=F",    "Gold (front future)"),
    ("SI=F",    "Silver (front future)"),
    ("BTC-USD", "Bitcoin"),
    ("ETH-USD", "Ethereum"),
]

# Mirrors DASHBOARD_SOXL_RSI_{BUY,SELL}_THRESHOLD in app.py:4101-4102.
# Deliberately re-declared rather than imported: app.py is a Streamlit script
# that renders at module scope, so importing it would execute the whole app.
SOXL_RSI_BUY_THRESHOLD = 35.0
SOXL_RSI_SELL_THRESHOLD = 70.2

LARGE_CAP_MIN = 10e9
MOVERS_N = 10
SOXX_MOVERS_N = 5
# Sub-segment for each constituent, so the sector narrative can say "memory"
# rather than listing tickers. Hand-maintained alongside soxx_holdings.txt;
# anything missing falls back to "other".
SOXX_SEGMENTS = {
    "MU": "memory", "RMBS": "memory",
    "TSM": "foundry", "UMC": "foundry", "ASX": "foundry/packaging",
    "GFS": "foundry",
    "NVDA": "GPU/accelerator", "AMD": "GPU/accelerator", "ALAB": "AI interconnect",
    "CRDO": "AI interconnect", "MRVL": "AI interconnect", "AVGO": "AI interconnect",
    "INTC": "CPU", "ARM": "CPU IP", "QCOM": "mobile SoC",
    "AMAT": "equipment", "KLAC": "equipment", "LRCX": "equipment",
    "ASML": "equipment", "TER": "equipment", "NVMI": "equipment",
    "ENTG": "materials",
    "TXN": "analog/MCU", "ADI": "analog/MCU", "NXPI": "analog/MCU",
    "MCHP": "analog/MCU", "ON": "analog/power", "STM": "analog/MCU",
    "MPWR": "analog/power", "SWKS": "RF", "MTSI": "RF",
}
CHUNK = 200
RETRY_CHUNK = 50
RETRY_BACKOFF_S = 15
# A single bad Yahoo bar can otherwise top the winners table. Real $10B+ names
# essentially never move this much in a session without it being a data error or
# a corporate action, so anything beyond this is dropped and logged.
MAX_PLAUSIBLE_MOVE_PCT = 60.0

_log_lines: list[str] = []


def log(msg: str) -> None:
    # Dated deliberately: the plist schedules in ET while these stamps are CST,
    # and a time-only stamp made "did Friday's run even happen?" unanswerable
    # from daily_close.log -- which is exactly the question after a missed send.
    stamp = datetime.now(CST).strftime("%m-%d %H:%M:%S")
    line = f"[{stamp}] {msg}"
    print(line, flush=True)
    _log_lines.append(line)


def _safe(v) -> float | None:
    """Normalise NaN/Inf to None -- repo-wide convention before storing or
    rendering a computed float."""
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return None if (math.isnan(f) or math.isinf(f)) else f


# ─────────────────────────────────────────────────────────────────────────────
# Database (read-only)
# ─────────────────────────────────────────────────────────────────────────────

def open_db() -> duckdb.DuckDBPyConnection | None:
    """Open the DB read-only, or return None.

    NOTE: read_only=True does NOT bypass DuckDB's file lock -- if the Streamlit
    app is running it holds a writable connection for its whole lifetime and
    every other connection, read-only included, is refused. storage._conn() is
    deliberately NOT used as a fallback: it retries for 60 s (storage.py:678)
    and would just stall the newsletter behind a lock that is never released.

    Instead a failure here falls through to the cached universe snapshot, so a
    running app degrades the report rather than breaking it.
    """
    for attempt in range(3):
        try:
            return duckdb.connect(str(DB_PATH), read_only=True)
        except Exception as exc:
            if attempt == 2:
                log(f"WARN: DuckDB is locked ({type(exc).__name__}) -- most likely the "
                    f"Streamlit app is running. Falling back to the cached universe.")
                return None
            time.sleep(2)
    return None


def _read_universe_cache() -> dict[str, dict]:
    """Last known good universe, used when the DB is locked."""
    try:
        blob = json.loads(UNIVERSE_CACHE.read_text(encoding="utf-8"))
        uni = blob.get("universe") or {}
        log(f"  using cached universe from {blob.get('as_of', 'unknown')} ({len(uni)} stocks)")
        return uni
    except Exception:
        log("ERROR: no cached universe available -- movers will have no sector/industry "
            "and no market-cap screen. Run once with the Streamlit app closed.")
        return {}


def _write_universe_cache(universe: dict[str, dict],
                          soxx_meta: dict[str, dict] | None = None) -> None:
    """Persist the universe, preserving the SOXX block when not re-supplied."""
    try:
        if soxx_meta is None:
            soxx_meta = _read_soxx_meta_cache()
        REPORTS_DIR.mkdir(exist_ok=True)
        UNIVERSE_CACHE.write_text(json.dumps(
            {"as_of": mc.et_today().isoformat(), "universe": universe,
             "soxx_meta": soxx_meta}), encoding="utf-8")
    except Exception as exc:
        log(f"WARN: could not write universe cache ({type(exc).__name__}: {exc}).")


def _read_soxx_meta_cache() -> dict[str, dict]:
    try:
        return json.loads(UNIVERSE_CACHE.read_text(encoding="utf-8")).get("soxx_meta") or {}
    except Exception:
        return {}


def load_soxx_meta(con, tickers: list[str]) -> dict[str, dict]:
    """name / sector / industry for the constituents, with NO market-cap screen.

    load_large_cap_universe() only covers $10B+ names, so ASX, UMC, NVMI and
    MTSI came back with an empty name -- which made the Google query fall back
    to a bare ticker, and printed "n/a / n/a" into the reason prompt. Cached
    alongside the universe so a locked DB still has it.
    """
    if not tickers:
        return {}
    if con is None:
        return _read_soxx_meta_cache()
    try:
        rows = con.execute("""
            WITH latest AS (
                SELECT ticker, max(fetch_date) AS fd FROM fundamentals
                WHERE ticker IN (SELECT unnest(?)) GROUP BY ticker
            )
            SELECT f.ticker,
                   COALESCE(TRY(json_extract_string(f.raw_info_json, '$.longName')),
                            TRY(json_extract_string(f.raw_info_json, '$.shortName'))) AS name,
                   TRY(json_extract_string(f.raw_info_json, '$.sector'))    AS sector,
                   TRY(json_extract_string(f.raw_info_json, '$.industry'))  AS industry
            FROM fundamentals f
            JOIN latest l ON f.ticker = l.ticker AND f.fetch_date = l.fd
        """, [tickers]).fetchall()
    except Exception as exc:
        log(f"WARN: SOXX metadata query failed ({type(exc).__name__}: {exc}).")
        return _read_soxx_meta_cache()

    meta = {r[0]: {"name": r[1] or "", "sector": r[2] or "", "industry": r[3] or ""}
            for r in rows}
    missing = [t for t in tickers if t not in meta]
    if missing:
        log(f"  soxx meta: no fundamentals row for {', '.join(missing)}")
    return meta


def load_large_cap_universe(con) -> dict[str, dict]:
    """$10B+ stocks with sector/industry, ETFs excluded.

    TRY() is load-bearing: some fundamentals.raw_info_json rows are malformed
    ('unexpected content after document') and a bare json_extract_string aborts
    the entire query. quoteType is absent from the blob, so ETFs are excluded by
    joining against the two places the repo records them instead.
    """
    if con is None:
        return _read_universe_cache()
    sql = """
        WITH latest AS (
            SELECT ticker, max(fetch_date) AS fd FROM fundamentals GROUP BY ticker
        )
        SELECT f.ticker,
               f.market_cap,
               TRY(json_extract_string(f.raw_info_json, '$.sector'))    AS sector,
               TRY(json_extract_string(f.raw_info_json, '$.industry'))  AS industry,
               -- fundamental_fetcher.py:698 stores longName, never shortName,
               -- so the old '$.shortName' lookup returned NULL for all 936 rows.
               -- Every news query was therefore falling back to a bare ticker
               -- instead of the company name.
               COALESCE(TRY(json_extract_string(f.raw_info_json, '$.longName')),
                        TRY(json_extract_string(f.raw_info_json, '$.shortName'))) AS name
        FROM fundamentals f
        JOIN latest l ON f.ticker = l.ticker AND f.fetch_date = l.fd
        WHERE f.market_cap >= ?
          AND f.ticker NOT IN (SELECT DISTINCT ticker FROM tech_indicators WHERE asset_type = 'etf')
          AND f.ticker NOT IN (SELECT ticker FROM etf_profile)
    """
    try:
        rows = con.execute(sql, [LARGE_CAP_MIN]).fetchall()
    except Exception as exc:
        log(f"ERROR: large-cap query failed ({type(exc).__name__}: {exc}).")
        return _read_universe_cache()
    universe = {
        r[0]: {"ticker": r[0], "market_cap": _safe(r[1]),
               "sector": r[2] or "", "industry": r[3] or "", "name": r[4] or ""}
        for r in rows
    }
    _write_universe_cache(universe)
    return universe


def load_prices_from_db(con, tickers: list[str], session: str) -> dict[str, dict]:
    """Daily move for `session` straight from price_history.

    The scan writes real closes for ~2 500 stocks each evening. Yahoo's own
    daily bar for the same session can carry a NULL close for hours (and has
    been seen to regress back to null after serving a real one), so the local
    table is both faster and more reliable than re-fetching. ETFs are absent
    from price_history, so index/board tickers still come from the network.
    """
    if con is None or not tickers:
        return {}
    try:
        rows = con.execute("""
            WITH ranked AS (
                SELECT ticker, date, close, volume,
                       row_number() OVER (PARTITION BY ticker ORDER BY date DESC) AS rn
                FROM price_history
                WHERE date <= ? AND ticker IN (SELECT unnest(?))
            )
            SELECT ticker,
                   max(CASE WHEN rn = 1 THEN date   END) AS d1,
                   max(CASE WHEN rn = 1 THEN close  END) AS c1,
                   max(CASE WHEN rn = 1 THEN volume END) AS v1,
                   max(CASE WHEN rn = 2 THEN close  END) AS c2
            FROM ranked WHERE rn <= 2 GROUP BY ticker
        """, [session, tickers]).fetchall()
    except Exception as exc:
        log(f"WARN: price_history lookup failed ({type(exc).__name__}: {exc}).")
        return {}

    out: dict[str, dict] = {}
    for tkr, d1, c1, v1, c2 in rows:
        close, prev = _safe(c1), _safe(c2)
        # Only counts if it is the session asked for -- an older row would
        # silently present stale prices as today's.
        if close is None or prev in (None, 0) or str(d1) != session:
            continue
        out[tkr] = {"ticker": tkr, "close": close, "prev_close": prev,
                    "pct_change": (close / prev - 1.0) * 100.0,
                    "volume": _safe(v1), "bar_date": str(d1), "source": "db"}
    return out


def load_soxx_holdings(con) -> list[str]:
    """Parse the hand-maintained constituent file and warn on drift."""
    if not HOLDINGS_FILE.exists():
        log(f"ERROR: {HOLDINGS_FILE.name} missing -- SOXL constituents section skipped.")
        return []
    text = re.sub(r"#.*", "", HOLDINGS_FILE.read_text())
    tickers, seen = [], set()
    for tok in re.split(r"[,\s]+", text):
        t = tok.strip().upper()
        if t and t not in seen:
            seen.add(t)
            tickers.append(t)

    # Drift detector: yfinance only exposes SOXX's top 10, but if one of those is
    # missing from the file the file is certainly stale.
    if con is not None:
        try:
            row = con.execute(
                "SELECT top_holdings_json FROM etf_profile WHERE ticker='SOXX' "
                "ORDER BY fetch_date DESC LIMIT 1"
            ).fetchone()
            if row and row[0]:
                top = [h.get("symbol", "").upper() for h in json.loads(row[0])]
                missing = [s for s in top if s and s not in seen]
                if missing:
                    log(f"DRIFT: SOXX top-10 names absent from {HOLDINGS_FILE.name}: "
                        f"{', '.join(missing)} -- refresh the file.")
        except Exception:
            pass  # drift check is advisory only
    return tickers


HOLDINGS_SOURCE = ("https://www.direxion.com/product/"
                   "daily-semiconductor-bull-bear-3x-etfs?keyword=SOXL"
                   "  (see 'All Index Holdings')")
HOLDINGS_MAX_AGE_DAYS = 100     # roughly one quarterly rebalance


def check_holdings() -> int:
    """Audit soxx_holdings.txt against every signal available locally.

    There is no free automated feed for the official membership: Direxion
    serves it from an authenticated AppSync GraphQL API behind Cloudflare,
    iShares returns HTML instead of its holdings CSV, stockanalysis.com 403s,
    and yfinance exposes only the top 10. So the file is curated, and this
    command makes reviewing it cheap.
    """
    con = open_db()
    holdings = load_soxx_holdings(con)
    asof = holdings_asof()
    print(f"soxx_holdings.txt -- {len(holdings)} names, as-of {asof}")
    print(f"official source: {HOLDINGS_SOURCE}\n")

    stale_days = None
    try:
        stale_days = (mc.et_today() - date.fromisoformat(asof)).days
        if stale_days > HOLDINGS_MAX_AGE_DAYS:
            print(f"!! list is {stale_days} days old (> {HOLDINGS_MAX_AGE_DAYS}) "
                  f"-- re-check it against the source above\n")
    except Exception:
        print("!! could not read an as-of date from the file\n")

    # Industry sanity: a name the fundamentals table does not call a
    # semiconductor business is the most likely mistake in a hand-made list.
    suspects = []
    if con is not None:
        try:
            rows = con.execute(
                "WITH l AS (SELECT ticker, max(fetch_date) fd FROM fundamentals GROUP BY ticker) "
                "SELECT f.ticker, TRY(json_extract_string(f.raw_info_json,'$.industry')), f.market_cap "
                "FROM fundamentals f JOIN l ON f.ticker=l.ticker AND f.fetch_date=l.fd "
                "WHERE f.ticker IN (SELECT unnest(?))", [holdings]).fetchall()
            known = {r[0]: (r[1] or "?", r[2]) for r in rows}
            for t in holdings:
                ind, cap = known.get(t, ("(not in fundamentals)", None))
                if "Semiconductor" not in ind:
                    suspects.append(f"  {t:6s} {ind[:44]:46s}"
                                    f"{f'${cap/1e9:.1f}B' if cap else ''}")
        except Exception as exc:
            print(f"(industry check unavailable: {type(exc).__name__})")

    if suspects:
        print("Names NOT classified as a semiconductor industry -- verify against the source:")
        print("\n".join(suspects), "\n")

    # yfinance exposes SOXX's and SMH's top 10; anything there but missing here
    # is a certain omission.
    for proxy in ("SOXX", "SMH"):
        try:
            with YF_DL_LOCK:
                top = list(yf.Ticker(proxy, session=YF_SESSION).funds_data.top_holdings.index)
            missing = [t for t in top if t not in holdings]
            print(f"{proxy} top-10: {'all present' if not missing else 'MISSING ' + ', '.join(missing)}")
        except Exception as exc:
            print(f"{proxy} top-10 unavailable ({type(exc).__name__})")

    if con is not None:
        try:
            con.close()
        except Exception:
            pass
    return 0


def holdings_asof() -> str:
    if not HOLDINGS_FILE.exists():
        return "unknown"
    m = re.search(r"as-of:\s*(\d{4}-\d{2}-\d{2})", HOLDINGS_FILE.read_text())
    return m.group(1) if m else "unknown"


# ─────────────────────────────────────────────────────────────────────────────
# Price fetch
# ─────────────────────────────────────────────────────────────────────────────

QUOTE_URL = "https://query2.finance.yahoo.com/v7/finance/quote"
QUOTE_BATCH = 100


def fetch_quotes(tickers: list[str], label: str = "") -> dict[str, dict]:
    """Daily move from Yahoo's quote endpoint -- the authoritative source.

    WHY NOT THE DAILY BARS: yfinance's historical array publishes a session's
    bar with a NULL close for hours and has been seen to regress a real close
    back to null. dropna() then silently compares today against the session
    BEFORE yesterday -- observed live, XLK reported +0.25% (vs 09-21) when the
    true move was -0.47%. regularMarketChangePercent carries the exchange's own
    previous close and is immune to those gaps.

    Needs Yahoo's cookie/crumb handshake, so it goes through yfinance's YfData
    rather than a bare request (a plain call returns 401).
    """
    tickers = [t for t in dict.fromkeys(tickers) if t]
    if not tickers:
        return {}
    try:
        from yfinance.data import YfData
        client = YfData(session=YF_SESSION)
    except Exception as exc:
        log(f"WARN: quote client unavailable ({type(exc).__name__}: {exc}).")
        return {}

    out: dict[str, dict] = {}
    batches = [tickers[i:i + QUOTE_BATCH] for i in range(0, len(tickers), QUOTE_BATCH)]
    for i, batch in enumerate(batches, 1):
        try:
            with YF_DL_LOCK:
                js = client.get_raw_json(QUOTE_URL, params={"symbols": ",".join(batch)})
            results = (js or {}).get("quoteResponse", {}).get("result", []) or []
        except Exception as exc:
            log(f"  {label} quote batch {i}/{len(batches)} failed "
                f"({type(exc).__name__}: {exc})")
            continue

        for q in results:
            sym = q.get("symbol")
            px = _safe(q.get("regularMarketPrice"))
            pct = _safe(q.get("regularMarketChangePercent"))
            if not sym or px is None or pct is None:
                continue
            ts = q.get("regularMarketTime")
            try:
                bar_date = datetime.fromtimestamp(float(ts), ET).date().isoformat()
            except (TypeError, ValueError, OSError):
                bar_date = None
            out[sym] = {
                "ticker": sym, "close": px,
                "prev_close": _safe(q.get("regularMarketPreviousClose")),
                "pct_change": pct,
                "volume": _safe(q.get("regularMarketVolume")),
                "bar_date": bar_date,
                "market_state": q.get("marketState") or "",
                "source": "quote",
            }
        if len(batches) > 1:
            log(f"  {label} quotes {i}/{len(batches)}: {len(out)}/{len(tickers)} resolved")
    return out


def _extract(df: pd.DataFrame, ticker: str, multi: bool) -> dict | None:
    """Pull the last two valid closes for one ticker.

    yf.download returns a MultiIndex frame for multiple tickers and flat columns
    for a single one -- both shapes have to be handled (CLAUDE.md convention).

    Yahoo publishes the current session's bar with a NULL close for hours after
    the close (and has been observed to regress a real close back to null), so
    a bar that has volume but no close is reported as `pending_date`: the
    session exists, its close just has not landed. Callers fill those from the
    live quote.
    """
    try:
        sub = df[ticker] if multi else df
    except (KeyError, TypeError):
        return None
    if sub is None or getattr(sub, "empty", True) or "Close" not in sub.columns:
        return None

    # Scan backwards for the newest bar that traded (has volume). Checking only
    # the final row is wrong: a board download mixes equities with crypto, and
    # crypto's extra weekend/next-day rows pad the equity frames with all-NaN
    # rows, hiding the real pending bar behind them.
    pending = None
    for i in range(len(sub) - 1, -1, -1):
        row = sub.iloc[i]
        if not _safe(row.get("Volume")):
            continue                      # a padded row, not a session
        if pd.isna(row.get("Close")):
            pending = sub.index[i].date().isoformat()
        break

    valid = sub.dropna(subset=["Close"])
    if len(valid) < 2:
        return None
    close = _safe(valid["Close"].iloc[-1])
    prev = _safe(valid["Close"].iloc[-2])
    if close is None or prev in (None, 0):
        return None
    vol = _safe(valid["Volume"].iloc[-1]) if "Volume" in valid.columns else None
    return {
        "ticker": ticker,
        "close": close,
        "prev_close": prev,
        "pct_change": (close / prev - 1.0) * 100.0,
        "volume": vol,
        "bar_date": valid.index[-1].date().isoformat(),
        "pending_date": pending,
        "source": "bar",
    }


def fill_pending_closes(quotes: dict[str, dict], session: str) -> int:
    """Use the live quote for tickers whose `session` bar has no close yet.

    After the bell Yahoo's regularMarketPrice is the settled close, so this
    recovers the current session when its daily bar is still null -- the same
    fallback swing_analysis.get_latest_quote_price() uses for the dashboard.
    Only applied to a bar Yahoo itself reports for `session`, never to invent
    one.
    """
    filled = 0
    for tkr, rec in quotes.items():
        if rec.get("pending_date") != session or rec.get("bar_date") == session:
            continue
        try:
            with YF_DL_LOCK:
                price = yf.Ticker(tkr, session=YF_SESSION).fast_info.last_price
            price = _safe(price)
        except Exception:
            price = None
        if price is None:
            continue
        prev = rec["close"]                      # the last settled close
        rec.update(close=price, prev_close=prev,
                   pct_change=(price / prev - 1.0) * 100.0 if prev else None,
                   bar_date=session, source="quote")
        filled += 1
    return filled


def fetch_daily_moves(tickers: list[str], label: str = "") -> dict[str, dict]:
    """Latest close + 1-day % change for each ticker, chunked with one retry."""
    tickers = [t for t in dict.fromkeys(tickers) if t]
    if not tickers:
        return {}

    out: dict[str, dict] = {}
    chunks = [tickers[i:i + CHUNK] for i in range(0, len(tickers), CHUNK)]
    for i, chunk in enumerate(chunks, 1):
        try:
            with YF_DL_LOCK:
                df = yf.download(chunk, period="5d", group_by="ticker", progress=False,
                                 auto_adjust=False, threads=False, session=YF_SESSION)
        except Exception as exc:
            log(f"WARN: download failed for chunk {i}/{len(chunks)} "
                f"({type(exc).__name__}: {exc}).")
            continue
        multi = isinstance(df.columns, pd.MultiIndex)
        for t in chunk:
            rec = _extract(df, t, multi)
            if rec:
                out[t] = rec
        log(f"  {label or 'fetch'} chunk {i}/{len(chunks)}: {len(out)}/{len(tickers)} resolved")

    missing = [t for t in tickers if t not in out]
    if missing:
        # Yahoo intermittently reports live large caps as "possibly delisted";
        # a single retry recovers nearly all of them.
        log(f"  retrying {len(missing)} ticker(s) that returned no data")
        # Yahoo answers 401 "Invalid Crumb" when the retry follows the bulk pass
        # too closely, so back off first and retry in small, unthreaded batches.
        time.sleep(RETRY_BACKOFF_S)
        for i in range(0, len(missing), RETRY_CHUNK):
            chunk = missing[i:i + RETRY_CHUNK]
            if i:
                time.sleep(2)
            try:
                df = yf.download(chunk, period="5d", group_by="ticker", progress=False,
                                 auto_adjust=False, threads=False)
            except Exception:
                continue
            multi = isinstance(df.columns, pd.MultiIndex)
            for t in chunk:
                rec = _extract(df, t, multi)
                if rec:
                    out[t] = rec
        still = [t for t in tickers if t not in out]
        if still:
            log(f"  dropped {len(still)} ticker(s) with no usable data: "
                f"{', '.join(still[:12])}{' ...' if len(still) > 12 else ''}")
    return out


def fetch_soxl_rsi(session: str | None = None,
                   latest_close: float | None = None) -> tuple[float | None, str | None]:
    """RSI(14) for SOXL, computed as technical_fetcher.py:319 does so the number
    agrees with the rest of the tool.

    When Yahoo's history has not yet published `session`, the quote close is
    appended so the RSI describes the session the newsletter reports. Without
    this the SOXL block was stamped a day behind the rest of the email.
    """
    try:
        with YF_DL_LOCK:
            df = yf.download("SOXL", period="6mo", progress=False, auto_adjust=False,
                             threads=False, session=YF_SESSION)
        if isinstance(df.columns, pd.MultiIndex):
            df = df.xs("SOXL", axis=1, level=1) if "SOXL" in df.columns.get_level_values(1) else df["SOXL"]
        close = df["Close"].dropna()
        if len(close) < 20:
            return None, None

        if session and latest_close is not None:
            last = close.index[-1].date().isoformat()
            if last < session:
                close = pd.concat([close, pd.Series([latest_close],
                                                    index=[pd.Timestamp(session)])])
            elif last == session:
                close.iloc[-1] = latest_close

        rsi = ta.momentum.RSIIndicator(close, window=14).rsi()
        return _safe(rsi.iloc[-1]), close.index[-1].date().isoformat()
    except Exception as exc:
        log(f"WARN: SOXL RSI computation failed ({type(exc).__name__}: {exc}).")
        return None, None


# ─────────────────────────────────────────────────────────────────────────────
# Multi-horizon returns (5d / 3m / 1y, price and volume)
# ─────────────────────────────────────────────────────────────────────────────
# Bar offsets follow storage.compute_returns_for_tickers (storage.py:2589-2608):
# N+1 rows back, so "5d" spans five completed sessions.
HZ_PRICE = (("px_5d", 6), ("px_3m", 64), ("px_1y", 253))
# Volume is averaged over the window and compared with the preceding window of
# equal length. A single day's volume 3 months ago is noise, not a baseline.
HZ_VOL = (("vol_5d", 5), ("vol_3m", 63), ("vol_1y", 252))


def fetch_multi_horizon(tickers: list[str]) -> dict[str, dict]:
    """5d/3m/1y price and volume change for the handful of rendered movers.

    Straight from Yahoo rather than from price_history, for the same reason the
    rest of this file takes prices from the network: open_db() returns None
    whenever the Streamlit app is running, which is the common case, and the
    universe cache carries no price data -- a DB-sourced column would render
    blank most days. One batched download for ~30 tickers costs a few seconds.

    period="3y", not "2y": the 1-year volume comparison needs 504 bars (252
    current + 252 prior) and two calendar years returns only ~501 trading days,
    which silently nulled every vol_1y.
    """
    if not tickers:
        return {}
    uniq = sorted(set(tickers))
    try:
        with YF_DL_LOCK:
            # group_by="ticker" matters: the default groups by FIELD, giving a
            # MultiIndex of (Close, AAPL) so df[ticker] raises and every
            # horizon silently comes back None.
            df = yf.download(uniq, period="3y", group_by="ticker", progress=False,
                             auto_adjust=False, threads=False, session=YF_SESSION)
    except Exception as exc:
        log(f"WARN: multi-horizon download failed ({type(exc).__name__}: {exc}) -- "
            f"5d/3m/1y omitted.")
        return {}

    multi = isinstance(df.columns, pd.MultiIndex)
    out: dict[str, dict] = {}
    for t in uniq:
        try:
            sub = df[t] if multi else df
        except (KeyError, TypeError):
            continue
        if sub is None or getattr(sub, "empty", True) or "Close" not in sub.columns:
            continue

        close = sub["Close"].dropna()
        vol = sub["Volume"].dropna() if "Volume" in sub.columns else None
        rec: dict[str, float | None] = {}

        for key, back in HZ_PRICE:
            # A recent listing (ALAB, CRDO) simply has no bar that far back.
            if len(close) < back:
                rec[key] = None
                continue
            now, then = _safe(close.iloc[-1]), _safe(close.iloc[-back])
            rec[key] = _safe((now / then - 1.0) * 100.0) if (now and then) else None

        for key, win in HZ_VOL:
            if vol is None or len(vol) < win * 2:
                rec[key] = None
                continue
            cur = _safe(vol.iloc[-win:].mean())
            prior = _safe(vol.iloc[-win * 2:-win].mean())
            rec[key] = _safe((cur / prior - 1.0) * 100.0) if (cur and prior) else None

        out[t] = rec

    n_full = sum(1 for r in out.values() if r.get("px_1y") is not None)
    log(f"  horizons: {len(out)}/{len(uniq)} tickers resolved ({n_full} with a full year)")
    return out


def attach_horizons(movers: list[dict], horizons: dict[str, dict]) -> None:
    """Copy the horizon figures onto each mover dict, in place."""
    for m in movers:
        m.update(horizons.get(m["ticker"], {}))


# ─────────────────────────────────────────────────────────────────────────────
# News + AI reasons
# ─────────────────────────────────────────────────────────────────────────────

NEWS_TIMEOUT_S = 420
BODY_DEADLINE_S = 200


def attach_news(movers: list[dict], hours: int = 48, limit: int = 6,
                bodies_for: set[str] | None = None) -> None:
    """Attach recent articles (headline + body where retrievable) to each mover.

    Runs news_fetcher as a SUBPROCESS. Yahoo's feed requires yfinance's
    curl_cffi transport, and libcurl-impersonate has aborted the interpreter
    outright (SIGABRT in SSL_write) when Yahoo throttles -- which it does
    hardest right after the close, exactly when this job runs. A native abort
    is uncatchable in-process, so isolation is the only way to keep a crash
    from costing the whole newsletter.
    """
    for m in movers:
        m["news"] = []

    # Bodies are best-effort: whatever has not arrived by the deadline keeps its
    # headline. Bounded so this phase cannot overrun NEWS_TIMEOUT_S and cost the
    # newsletter its reasons entirely.
    spec = {"movers": [{"ticker": m["ticker"], "name": m.get("name")} for m in movers],
            "hours": hours, "limit": limit, "body_deadline": BODY_DEADLINE_S}
    if bodies_for is not None:
        spec["bodies_for"] = sorted(bodies_for)
    try:
        proc = subprocess.run(
            [sys.executable, str(HERE / "news_fetcher.py")],
            input=json.dumps(spec), capture_output=True, text=True, timeout=NEWS_TIMEOUT_S)
    except subprocess.TimeoutExpired:
        log(f"WARN: news subprocess exceeded {NEWS_TIMEOUT_S}s -- continuing without news.")
        return

    if proc.returncode != 0:
        detail = (proc.stderr or "").strip().splitlines()[-1:] or ["no stderr"]
        crashed = proc.returncode < 0 or proc.returncode > 128
        log(f"WARN: news subprocess {'CRASHED' if crashed else 'failed'} "
            f"(exit {proc.returncode}: {detail[0][:120]}) -- continuing without news.")
        return

    try:
        data = json.loads(proc.stdout or "{}")
    except json.JSONDecodeError:
        log("WARN: news subprocess returned unparseable output -- continuing without news.")
        return

    for m in movers:
        m["news"] = data.get(m["ticker"], [])

    tail = (proc.stderr or "").strip().splitlines()[-1:]
    if tail and tail[0].startswith("bodies:"):
        log(f"  news: {tail[0]}")
    got = sum(1 for m in movers if m["news"])
    bodies = sum(1 for m in movers for h in m["news"] if h.get("body"))
    total = sum(len(m["news"]) for m in movers)
    log(f"  news: {got}/{len(movers)} movers have articles in the last {hours}h "
        f"({total} articles, {bodies} with readable text)")


_REASON_PROMPT = """You are writing a factual daily market-close briefing.

For each stock below you get its one-day price move, then the news published
about it in the last 48 hours -- headline, publisher, and where available the
article text.

For each ticker, give 1 to 3 SHORT reasons (max 20 words each) explaining why
the stock moved, ordered most to least important, using ONLY what the articles
actually say.

CRITICAL RULES:
- Use the article text, not just the headline, when text is present.
- Give FEWER reasons rather than padding: one solid reason beats three weak ones.
- If the articles do not explain the move, return an empty list for that ticker.
  Do NOT guess, and do NOT restate the price move as if it were its own cause.
- Never invent numbers, deals, analyst actions or events not in the source text.
- Generic market-wide commentary ("stocks rose broadly") is only a valid reason
  if nothing company-specific is available, and must be labelled as such.
- No hedging filler ("investors may be reacting to..."). Be concrete or return [].

Return ONLY a JSON object mapping each ticker to an array of reason strings.
Example: {{"AMD": ["Landed a multi-year AI accelerator order", "Analyst raised target to $300"], "XYZ": []}}
No markdown, no commentary.

Stocks:
{payload}"""


# Tried in order; the first that answers wins. Newer keys cannot access
# gemini-2.5-flash at all ("no longer available to new users"), and individual
# models return transient 503s, so an unattended nightly job needs a fallback.
REASON_MODELS = ["gemini-3.8-flash", "gemini-3.6-flash", "gemini-flash-latest"]
# Gemini 503s ("high demand") are frequent and can persist for minutes. A fixed
# 3-attempts-per-model budget burned out in 90 s during a real run and lost the
# reasons, so retries are bounded by a total deadline with growing backoff
# instead -- the job is unattended and can afford to wait.
REASON_DEADLINE_S = 360
REASON_BACKOFFS = [5, 15, 30, 60, 60]


def _gemini_json(prompt: str, *, label: str, model: str | None = None,
                 temperature: float = 0.2,
                 deadline_s: float = REASON_DEADLINE_S) -> dict:
    """One Gemini call returning parsed JSON, with model fallback and backoff.

    Shared by the per-mover reasons and the SOXL sector narrative. 503 "high
    demand" is common and short-lived, so each model is retried with backoff
    before falling through to the next one -- switching models immediately just
    spends the whole chain during one transient spike. A non-transient error
    (a 400) drops that model for good rather than burning retries on it.

    Raises on failure; callers decide whether that degrades a section or the run.
    """
    from google import genai
    from google.genai import types
    client = genai.Client(api_key=config.GEMINI_API_KEY)
    cfg = types.GenerateContentConfig(response_mime_type="application/json",
                                      temperature=temperature)

    candidates = [model] if model else list(REASON_MODELS)
    resp, last_exc, started, round_n = None, None, time.time(), 0

    while resp is None and time.time() - started < deadline_s:
        for candidate in candidates:
            try:
                resp = client.models.generate_content(
                    model=candidate, contents=prompt, config=cfg)
                log(f"  {label}: model {candidate}"
                    f"{f' (round {round_n + 1})' if round_n else ''}")
                break
            except Exception as exc:
                last_exc = exc
                if not any(c in str(exc) for c in ("503", "429", "UNAVAILABLE")):
                    log(f"  {label}: {candidate} rejected the request "
                        f"({type(exc).__name__}) -- not retrying this model")
                    candidates = [c for c in candidates if c != candidate]
                    break
        if resp is not None or not candidates:
            break
        wait = REASON_BACKOFFS[min(round_n, len(REASON_BACKOFFS) - 1)]
        elapsed = time.time() - started
        if elapsed + wait >= deadline_s:
            log(f"  {label}: all models busy and the {deadline_s:.0f}s budget is "
                f"spent -- giving up")
            break
        log(f"  {label}: all {len(candidates)} model(s) busy "
            f"({elapsed:.0f}s elapsed), waiting {wait}s")
        time.sleep(wait)
        round_n += 1

    if resp is None:
        raise last_exc or RuntimeError("no model available")

    data = json.loads((resp.text or "").strip())
    if not isinstance(data, dict):
        raise ValueError("response was not a JSON object")
    return data


def generate_reasons(movers: list[dict], model: str | None = None) -> dict[str, list[str]]:
    """One batched Gemini call for all movers -> 1-3 reasons each.

    Grounding is the fetched articles themselves (Google Search tooling is NOT
    enabled), so every sentence can be checked against the links printed beside
    it. Any failure degrades to headlines-only -- a missing API key must never
    block the newsletter.
    """
    if not config.GEMINI_API_KEY:
        log("  reasons: GEMINI_API_KEY not set -- rendering headlines only. "
            "Add it to .env to get 'why it moved' lines.")
        return {}

    with_news = [m for m in movers if m.get("news")]
    if not with_news:
        log("  reasons: no articles to summarise -- skipping Gemini call.")
        return {}

    lines, with_body = [], 0
    for m in with_news:
        lines.append(f'\n### {m["ticker"]} ({m.get("name") or m["ticker"]}, '
                     f'{m.get("sector") or "n/a"} / {m.get("industry") or "n/a"}) '
                     f'moved {m["pct_change"]:+.2f}% today.')
        for h in m["news"]:
            lines.append(f'- HEADLINE: {h["title"]} ({h["publisher"]}, {h["age"]})')
            if h.get("body"):
                with_body += 1
                lines.append(f'  TEXT: {h["body"]}')

    n_articles = sum(len(m["news"]) for m in with_news)
    payload = "\n".join(lines)
    log(f"  reasons: sending {n_articles} articles ({with_body} with text, "
        f"~{len(payload)//4:,} tokens) for {len(with_news)} movers")

    try:
        data = _gemini_json(_REASON_PROMPT.format(payload=payload),
                            label="reasons", model=model)

        reasons: dict[str, list[str]] = {}
        for k, v in data.items():
            if isinstance(v, str):
                v = [v]
            if isinstance(v, list):
                cleaned = [r.strip() for r in v if isinstance(r, str) and r.strip()][:3]
                if cleaned:
                    reasons[k.upper()] = cleaned
        log(f"  reasons: Gemini explained {len(reasons)}/{len(with_news)} movers")
        return reasons
    except Exception as exc:
        log(f"WARN: Gemini reason synthesis failed ({type(exc).__name__}: {exc}). "
            f"Rendering headlines only.")
        return {}


# ─────────────────────────────────────────────────────────────────────────────
# SOXL sector narrative
# ─────────────────────────────────────────────────────────────────────────────
_NARRATIVE_PROMPT = """You are writing the semiconductor section of a daily
market-close briefing for one reader who reads it every trading day.

Below: today's SOXL move and RSI, every SOXX constituent's move grouped by
sub-segment, the news published about them in the last 48 hours, and the
narrative you wrote on the PREVIOUS sessions.

Write 2 to 4 short themes (max 30 words each) about what is actually driving the
sector today -- memory pricing, foundry utilisation, AI accelerator demand,
analog/MCU inventory, equipment capex, and so on.

CRITICAL RULES:
- Name the sub-segment and the driver. "Memory: contract DRAM pricing firmed for
  a third session on HBM allocation" -- not "chip stocks rose".
- Use ONLY what the articles say. Never invent numbers, deals or analyst actions.
- CONTINUITY: you are given previous sessions. Where today extends, reverses or
  breaks one of those threads, say so explicitly ("a third session of...",
  "reversing yesterday's..."). Where it does not, do NOT manufacture a link.
- Do not simply restate the price moves. The move is the fact; you supply why.
- If the articles genuinely do not explain the sector's day, return fewer themes,
  or an empty list. No hedging filler.

Return ONLY a JSON object: {{"themes": ["...", "..."]}}
No markdown, no commentary.

{payload}"""


def load_narratives() -> list[dict]:
    """Stored narratives, oldest first. Never raises."""
    try:
        blob = json.loads(NARRATIVE_STORE.read_text(encoding="utf-8"))
        return [n for n in (blob.get("narratives") or []) if n.get("session")]
    except Exception:
        return []


def save_narrative(session: str, themes: list[str]) -> None:
    """Upsert this session's narrative, keeping the last NARRATIVE_KEEP.

    Refuses to overwrite a stored narrative with an empty one, mirroring the
    archive's no-clobber rule: a later --no-news or Gemini-failed rerun of the
    same session must not wipe a good entry.
    """
    if not themes:
        return
    try:
        entries = [n for n in load_narratives() if n.get("session") != session]
        entries.append({"session": session,
                        "written_at": datetime.now(CST).isoformat(timespec="seconds"),
                        "themes": themes})
        entries.sort(key=lambda n: n["session"])
        REPORTS_DIR.mkdir(exist_ok=True)
        tmp = NARRATIVE_STORE.with_suffix(".json.tmp")
        tmp.write_text(json.dumps({"narratives": entries[-NARRATIVE_KEEP:]}, indent=2),
                       encoding="utf-8")
        tmp.replace(NARRATIVE_STORE)
    except Exception as exc:
        log(f"WARN: could not store the SOXL narrative ({type(exc).__name__}: {exc}).")


def generate_soxl_narrative(session: str, soxl: dict | None, rsi: float | None,
                            constituents: list[dict]) -> dict:
    """The sector read. Returns {"themes": [...], "prior_sessions": [...]}.

    Degrades to an empty theme list on any failure -- the narrative is the one
    part of the email that is nice-to-have, and it must never block a send.
    """
    empty = {"themes": [], "prior_sessions": []}
    if not config.GEMINI_API_KEY:
        return empty

    prior = [n for n in load_narratives() if n["session"] < session][-NARRATIVE_CONTEXT:]

    lines = []
    if soxl:
        lines.append(f'SOXL closed {soxl["close"]:,.2f}, {soxl["pct_change"]:+.2f}%'
                     + (f", RSI(14) {rsi:.1f}." if rsi is not None else "."))

    by_seg: dict[str, list[dict]] = {}
    for c in constituents:
        by_seg.setdefault(SOXX_SEGMENTS.get(c["ticker"], "other"), []).append(c)
    lines.append("\nCONSTITUENT MOVES BY SEGMENT:")
    for seg in sorted(by_seg):
        moves = ", ".join(f'{c["ticker"]} {c["pct_change"]:+.2f}%'
                          for c in sorted(by_seg[seg],
                                          key=lambda r: r["pct_change"], reverse=True))
        lines.append(f"- {seg}: {moves}")

    n_articles = 0
    lines.append("\nNEWS:")
    for c in constituents:
        arts = c.get("news") or []
        if not arts:
            continue
        lines.append(f'\n### {c["ticker"]} '
                     f'({SOXX_SEGMENTS.get(c["ticker"], "other")}, '
                     f'{c["pct_change"]:+.2f}%)')
        for h in arts:
            n_articles += 1
            lines.append(f'- HEADLINE: {h["title"]} ({h["publisher"]}, {h["age"]})')
            if h.get("body"):
                lines.append(f'  TEXT: {h["body"]}')

    if not n_articles:
        log("  narrative: no constituent articles -- skipping.")
        return empty

    if prior:
        lines.append("\nYOUR PREVIOUS SESSIONS (oldest first):")
        for n in prior:
            for t in n.get("themes") or []:
                lines.append(f'- [{n["session"]}] {t}')
    else:
        lines.append("\nNo previous narrative is available; do not claim continuity.")

    payload = "\n".join(lines)
    log(f"  narrative: sending {n_articles} articles across "
        f"{len(constituents)} constituents (~{len(payload)//4:,} tokens), "
        f"{len(prior)} prior session(s)")

    try:
        data = _gemini_json(_NARRATIVE_PROMPT.format(payload=payload),
                            label="narrative", temperature=0.3)
        themes = [t.strip() for t in (data.get("themes") or [])
                  if isinstance(t, str) and t.strip()][:4]
        log(f"  narrative: {len(themes)} theme(s)")
        return {"themes": themes, "prior_sessions": [n["session"] for n in prior]}
    except Exception as exc:
        log(f"WARN: SOXL narrative failed ({type(exc).__name__}: {exc}).")
        return empty


# ─────────────────────────────────────────────────────────────────────────────
# Rendering
# ─────────────────────────────────────────────────────────────────────────────
# Inline-styled <table> markup, no images, no flex/grid.
#
# The styles that repeat -- the font stack above all -- live in ONE <style>
# block instead of on every element. Gmail has supported an embedded <style> in
# <head> since 2016 on web and in both mobile apps; the older belief that it
# strips them (which this file used to assert) cost 69 KB of duplicated
# style="" attributes out of a 119 KB report, and Gmail clips an HTML body at
# roughly 102 KB -- so the tail of the newsletter was being cut off.
#
# The one thing kept inline is the red/green on a percentage (_pct_html): an
# unstyled table is still readable, but a table where losses are not red is
# actively misleading, so that colour must survive even a client that drops the
# block entirely.

FONT = "font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Helvetica,Arial,sans-serif"
GREEN, RED, GREY, INK, MUTED = "#0a7f3f", "#c0392b", "#8a8f98", "#1a1d21", "#6b7280"
BORDER = "#e5e7eb"

_CSS = (
    "body{margin:0;padding:0;background:#f6f7f9}"
    f"body,td,th,div,span,a,h2{{{FONT}}}"
    f".w{{max-width:680px;margin:0 auto;padding:18px;background:#fff;color:{INK}}}"
    ".hd{font-size:21px;font-weight:700}"
    f".sb{{font-size:13px;color:{MUTED};margin-top:2px}}"
    f"h2{{font-size:17px;color:{INK};margin:26px 0 4px;padding-bottom:6px;"
    f"border-bottom:2px solid {INK}}}"
    f".s2{{font-size:12px;color:{MUTED};margin:2px 0 10px}}"
    "table{border-collapse:collapse;width:100%}"
    ".nr{max-width:560px}"
    f".q{{padding:7px 10px;border-bottom:1px solid {BORDER}}}"
    ".r{text-align:right}"
    f".b{{font-weight:600;color:{INK}}}"
    f".d{{font-size:11px;font-weight:400;color:{MUTED}}}"
    f".mv{{padding:11px 10px;border-bottom:1px solid {BORDER}}}"
    f".mt{{font-size:14px;font-weight:700;color:{INK}}}"
    ".mp{font-size:14px}"
    f".mm{{font-size:12px;color:{MUTED}}}"
    f".mn{{font-size:11.5px;color:{MUTED};margin-top:2px}}"
    f".hz{{font-size:11.5px;color:{INK};margin-top:3px}}"
    f".rs{{font-size:12.5px;color:{INK};margin-top:4px}}"
    f".rn{{color:{MUTED}}}"
    f".em{{font-size:12px;color:{GREY};margin-top:5px;font-style:italic}}"
    ".a{font-size:11.5px;margin-top:3px}"
    ".a a{color:#1a56db;text-decoration:none}"
    f".src{{color:{MUTED}}}"
    ".lbl{font-size:13px;font-weight:600;margin:14px 0 4px}"
    f".nd{{font-size:12px;color:{GREY}}}"
    f".fn{{font-size:11px;color:{MUTED};margin-top:8px}}"
    f".ft{{font-size:11px;color:{MUTED};margin-top:26px;padding-top:10px;"
    f"border-top:1px solid {BORDER}}}"
)


def _esc(s) -> str:
    return (str(s or "").replace("&", "&amp;").replace("<", "&lt;")
            .replace(">", "&gt;").replace('"', "&quot;"))


def _pct_html(v: float | None) -> str:
    # Colour stays inline on purpose -- see the note at the top of this section.
    if v is None:
        return f'<span style="color:{GREY}">n/a</span>'
    color = GREEN if v > 0 else (RED if v < 0 else GREY)
    return f'<span style="color:{color};font-weight:600">{v:+.2f}%</span>'


def _fmt_price(v: float | None) -> str:
    """Two decimals for everything, so BTC/gold line up with the equity rows."""
    if v is None:
        return "n/a"
    return f"{v:,.2f}"


def _millify(v: float | None) -> str:
    if v is None:
        return "n/a"
    for div, suf in ((1e12, "T"), (1e9, "B"), (1e6, "M")):
        if abs(v) >= div:
            return f"${v/div:.2f}{suf}"
    return f"${v:,.0f}"


def _h2(text: str, sub: str = "") -> str:
    s = f'<div class="s2">{_esc(sub)}</div>' if sub else ""
    return f'<h2>{_esc(text)}</h2>{s}'


def _sort_board(pairs: list[tuple[str, str]], quotes: dict[str, dict]
                ) -> list[tuple[str, str, dict | None]]:
    """Order a board by daily % change, best first; no-data tickers sink last."""
    rows = [(t, d, quotes.get(t)) for t, d in pairs]
    return sorted(rows, key=lambda r: (r[2] is not None, r[2]["pct_change"] if r[2] else 0.0),
                  reverse=True)


def _quote_table(rows: list[tuple[str, str, dict | None]]) -> str:
    cells = []
    for tkr, desc, rec in rows:
        if rec is None:
            price, pct = f'<span style="color:{GREY}">no data</span>', ""
        else:
            price, pct = _fmt_price(rec["close"]), _pct_html(rec["pct_change"])
        cells.append(
            f'<tr><td class="q b">{_esc(tkr)}<div class="d">{_esc(desc)}</div></td>'
            f'<td class="q r" style="color:{INK}">{price}</td>'
            f'<td class="q r">{pct}</td></tr>'
        )
    return ('<table cellpadding="0" cellspacing="0" border="0" class="nr">'
            f'{"".join(cells)}</table>')


def empty_note(mover: dict) -> str:
    """Say WHY there is no reason. The old wording claimed nothing was found in
    the headlines even when the real cause was that Gemini never ran."""
    if not config.GEMINI_API_KEY:
        return "set GEMINI_API_KEY in .env to get reasons"
    if not mover.get("news"):
        return "no news in the last 48h"
    return "articles found, but none explained the move"


def _trio(vals: list[float | None]) -> str:
    """Render 5d/3m/1y as one slash-joined group, em-dash for missing history."""
    return " / ".join("&mdash;" if v is None else f"{v:+.0f}%" for v in vals)


def _horizon_html(m: dict) -> str:
    """The compact 5d/3m/1y line. Omitted entirely when nothing resolved, rather
    than printing a row of em-dashes."""
    px = [m.get("px_5d"), m.get("px_3m"), m.get("px_1y")]
    vol = [m.get("vol_5d"), m.get("vol_3m"), m.get("vol_1y")]
    if all(v is None for v in px + vol):
        return ""
    return (f'<div class="hz"><span class="src">Px 5d/3m/1y</span> {_trio(px)}'
            f'<span class="src"> &middot; Vol</span> {_trio(vol)}</div>')


def _horizon_text(m: dict) -> str:
    """Plain-text twin of _horizon_html."""
    px = [m.get("px_5d"), m.get("px_3m"), m.get("px_1y")]
    vol = [m.get("vol_5d"), m.get("vol_3m"), m.get("vol_1y")]
    if all(v is None for v in px + vol):
        return ""
    fmt = lambda vs: " / ".join("--" if v is None else f"{v:+.0f}%" for v in vs)
    return f"Px 5d/3m/1y {fmt(px)}  |  Vol {fmt(vol)}"


def _narrative_html(ctx: dict) -> str:
    nar = ctx.get("narrative") or {}
    themes = nar.get("themes") or []
    if not themes:
        return ""
    items = "".join(f'<div class="rs"><span class="rn">&bull;</span> {_esc(t)}</div>'
                    for t in themes)
    n_prior = len(nar.get("prior_sessions") or [])
    prior_note = (f' and the previous {n_prior} session'
                  f'{"s" if n_prior != 1 else ""} of this section') if n_prior else ""
    return (f'<div class="lbl" style="margin-top:16px">Sector read</div>{items}'
            f'<div class="fn">Synthesised by Gemini from the constituent '
            f'headlines below{prior_note}.</div>')


def _narrative_text(ctx: dict) -> list[str]:
    nar = ctx.get("narrative") or {}
    themes = nar.get("themes") or []
    if not themes:
        return []
    return ["", "  Sector read", "  " + "-" * 44] + [f"  * {t}" for t in themes]


def _mover_rows(movers: list[dict], reasons: dict[str, list[str]],
                max_articles: int = 6) -> str:
    out = []
    for m in movers:
        reason_list = reasons.get(m["ticker"]) or []
        if reason_list:
            reason_html = "".join(
                f'<div class="rs"><span class="rn">{i}.</span> {_esc(r)}</div>'
                for i, r in enumerate(reason_list, 1))
        else:
            reason_html = f'<div class="em">{_esc(empty_note(m))}</div>'
        news_html = "".join(
            f'<div class="a"><a href="{_esc(h["link"])}">{_esc(h["title"])}</a>'
            f'<span class="src"> &middot; {_esc(h["publisher"])} &middot; {_esc(h["age"])}</span></div>'
            for h in (m.get("news") or [])[:max_articles]
        )
        cap = m.get("market_cap")
        out.append(
            f'<tr><td class="mv">'
            f'<div><span class="mt">{_esc(m["ticker"])}</span> '
            f'<span class="mp">{_pct_html(m["pct_change"])}</span> '
            f'<span class="mm">&middot; {_fmt_price(m["close"])}'
            + (f' &middot; {_millify(cap)}' if cap else "") +
            f'</span></div>'
            f'<div class="mn">{_esc(m.get("name") or "")}'
            f'{" &middot; " if m.get("name") else ""}'
            f'{_esc(m.get("sector") or "n/a")} &rsaquo; {_esc(m.get("industry") or "n/a")}</div>'
            f'{_horizon_html(m)}{reason_html}{news_html}</td></tr>'
        )
    return ('<table cellpadding="0" cellspacing="0" border="0">'
            f'{"".join(out)}</table>')


def render_html(ctx: dict) -> str:
    eq_asof = ctx["equity_asof"]
    n_art = ctx.get("render_articles", 6)
    # A full document (not a bare fragment) so the archived file renders its
    # unicode correctly in a browser.
    p = ['<!doctype html><html><head><meta charset="utf-8">'
         '<meta name="viewport" content="width=device-width,initial-scale=1">'
         f'<title>Market Close {_esc(ctx["headline_date"])}</title>'
         f'<style>{_CSS}</style></head><body><div class="w">']
    p.append(f'<div class="hd">Market Close</div>'
             f'<div class="sb">{_esc(ctx["headline_date"])} &middot; generated '
             f'{_esc(ctx["generated_at"])}</div>')

    # 1. Index board
    p.append(_h2("1. Index Board", f"US session close · {eq_asof}"))
    p.append(_quote_table(_sort_board(INDEX_EQUITIES, ctx["quotes"])))
    if ctx["alt_rows"]:
        p.append('<div class="s2" style="margin:16px 0 6px">'
                 'Commodities &amp; crypto &middot; trade outside NYSE hours '
                 f'&middot; as of {_esc(ctx["alt_asof"])}</div>')
        p.append(_quote_table(_sort_board(INDEX_ALT, ctx["quotes"])))

    # 2. SOXL
    rsi, rsi_date = ctx["soxl_rsi"], ctx["soxl_rsi_date"]
    if rsi is None:
        rsi_html = f'<span style="color:{GREY}">RSI(14) unavailable</span>'
    else:
        if rsi <= SOXL_RSI_BUY_THRESHOLD:
            tone, band = GREEN, f"at/below buy threshold ({SOXL_RSI_BUY_THRESHOLD:g})"
        elif rsi >= SOXL_RSI_SELL_THRESHOLD:
            tone, band = RED, f"at/above sell threshold ({SOXL_RSI_SELL_THRESHOLD:g})"
        else:
            tone, band = INK, f"neutral ({SOXL_RSI_BUY_THRESHOLD:g}-{SOXL_RSI_SELL_THRESHOLD:g})"
        rsi_html = (f'<span style="font-size:22px;font-weight:700;color:{tone}">{rsi:.1f}</span>'
                    f'<span class="mm"> RSI(14) &middot; {band}</span>')
    soxl = ctx["quotes"].get("SOXL")
    soxl_line = (f'<span style="font-size:15px;font-weight:600">{_fmt_price(soxl["close"])}</span> '
                 f'{_pct_html(soxl["pct_change"])}' if soxl else
                 f'<span style="color:{GREY}">no data</span>')
    p.append(_h2("2. SOXL", f"as of {_esc(rsi_date or eq_asof)}"))
    p.append(f'<div style="margin-bottom:6px">{soxl_line}</div><div>{rsi_html}</div>')
    p.append(_narrative_html(ctx))

    for title, rows in ((f"Top {SOXX_MOVERS_N} constituent gainers", ctx["soxx_up"]),
                        (f"Top {SOXX_MOVERS_N} constituent losers", ctx["soxx_down"])):
        p.append(f'<div class="lbl">{_esc(title)}</div>')
        p.append(_mover_rows(rows, ctx["reasons"], n_art) if rows
                 else '<div class="nd">no data</div>')
    p.append(f'<div class="fn">Constituents from soxx_holdings.txt '
             f'({ctx["holdings_n"]} names, as-of {_esc(ctx["holdings_asof"])}). '
             f'SOXL is swap-based and holds no equities; SOXX is used as the '
             f'constituent proxy.</div>')

    # 3. Movers
    p.append(_h2(f"3. Top {MOVERS_N} Winners — $10B+ cap",
                 f'{ctx["universe_n"]} stocks screened · ETFs excluded · {eq_asof}'))
    p.append(_mover_rows(ctx["winners"], ctx["reasons"], n_art))
    p.append(_h2(f"Top {MOVERS_N} Losers — $10B+ cap", f"{eq_asof}"))
    p.append(_mover_rows(ctx["losers"], ctx["reasons"], n_art))

    p.append(f'<div class="ft">'
             f'Prices from Yahoo Finance. Sector, industry and market cap from the local '
             f'stock_analysis_v2 database'
             + (f' ({ctx["unavailable_n"]} screened tickers had no Yahoo data today '
                f'&mdash; delistings or renames &mdash; and were excluded)'
                if ctx.get("unavailable_n") else "")
             + f'. Headlines from Google News (last 48h)'
             f'{"; one-line reasons synthesised by Gemini from those headlines only" if ctx["reasons"] else ""}. '
             f'5d/3m/1y price is a spot change; volume compares the window average '
             f'against the preceding window. '
             f'Informational only &mdash; not investment advice.</div></div>'
             f'</body></html>')
    return "".join(p)


# Gmail clips at roughly 102 KB, showing "[Message clipped]" and hiding
# everything past the cut -- which is what was happening to the old 119 KB
# report. Article links are ~75% of the bytes and their count swings with the
# news day, so rather than hope a fixed cap fits, drop articles-per-mover until
# the message does.
#
# The budget counts the HTML *and* the plain-text alternative, because the
# threshold is generally described as applying to the whole message rather than
# to the HTML part alone -- and this is not something the code can measure from
# here. If a real send at the full six articles turns out NOT to be clipped,
# raising MSG_BUDGET_BYTES is the one knob to turn.
MSG_BUDGET_BYTES = 95_000
TEXT_ARTICLES = 2


def render_within_budget(ctx: dict) -> tuple[str, str]:
    """Render both parts, trimming articles per mover until they fit."""
    html = text = ""
    for cap in (6, 5, 4, 3, 2, 1):
        ctx["render_articles"] = cap
        html, text = render_html(ctx), render_text(ctx)
        size = len(html.encode()) + len(text.encode())
        if size <= MSG_BUDGET_BYTES:
            log(f"  render: {size:,} bytes at {cap} article(s) per mover "
                f"(budget {MSG_BUDGET_BYTES:,})")
            return html, text
    log(f"WARN: message is {len(html.encode()) + len(text.encode()):,} bytes even "
        f"at one article per mover -- Gmail will probably clip it.")
    return html, text


def render_text(ctx: dict) -> str:
    L = [f'MARKET CLOSE -- {ctx["headline_date"]}',
         f'generated {ctx["generated_at"]}', "", "1. INDEX BOARD", "-" * 46]
    for t, d, r in _sort_board(INDEX_EQUITIES, ctx["quotes"]):
        L.append(f'{t:<9}{_fmt_price(r["close"]) if r else "no data":>12}'
                 f'{(f"{r["pct_change"]:+.2f}%" if r else ""):>10}   {d}')
    if ctx["alt_rows"]:
        L += ["", f'Commodities & crypto (as of {ctx["alt_asof"]})', "-" * 46]
        for t, d, r in _sort_board(INDEX_ALT, ctx["quotes"]):
            L.append(f'{t:<9}{_fmt_price(r["close"]) if r else "no data":>12}'
                     f'{(f"{r["pct_change"]:+.2f}%" if r else ""):>10}   {d}')

    soxl = ctx["quotes"].get("SOXL")
    L += ["", "2. SOXL", "-" * 46]
    L.append(f'close {_fmt_price(soxl["close"])}  {soxl["pct_change"]:+.2f}%' if soxl else "no data")
    L.append(f'RSI(14) {ctx["soxl_rsi"]:.1f}' if ctx["soxl_rsi"] is not None else "RSI(14) unavailable")
    for line in _narrative_text(ctx):
        L.append(line)

    def _mover_block(movers: list[dict]) -> None:
        for m in movers:
            L.append(f'{m["ticker"]:<7}{m["pct_change"]:+7.2f}%  {_fmt_price(m["close"]):>10}  '
                     f'{m.get("sector") or "n/a"} / {m.get("industry") or "n/a"}')
            hz = _horizon_text(m)
            if hz:
                L.append(f'         {hz}')
            for i, r in enumerate(ctx["reasons"].get(m["ticker"]) or [], 1):
                L.append(f'         {i}. {r}')
            # Deliberately thinner than the HTML part: this alternative is a
            # fallback almost nobody reads, but it still counts against the
            # message size Gmail clips on.
            for h in (m.get("news") or [])[:TEXT_ARTICLES]:
                L.append(f'         - {h["title"]} ({h["publisher"]}, {h["age"]})'
                         f' {h["link"]}')

    for title, rows in ((f"Top {SOXX_MOVERS_N} constituent gainers", ctx["soxx_up"]),
                        (f"Top {SOXX_MOVERS_N} constituent losers", ctx["soxx_down"])):
        L += ["", f"  {title}", "  " + "-" * 44]
        if rows:
            _mover_block(rows)
        else:
            L.append("  no data")

    for title, movers in ((f"3. TOP {MOVERS_N} WINNERS ($10B+)", ctx["winners"]),
                          (f"TOP {MOVERS_N} LOSERS ($10B+)", ctx["losers"])):
        L += ["", title, "-" * 46]
        _mover_block(movers)

    if ctx.get("unavailable_n"):
        L += ["", f'Note: {ctx["unavailable_n"]} of {ctx["universe_n"]} screened tickers had no '
                  f'Yahoo data today (delistings/renames) and were excluded.']
    L += ["", "Informational only -- not investment advice."]
    return "\n".join(L)


# ─────────────────────────────────────────────────────────────────────────────
# Orchestration
# ─────────────────────────────────────────────────────────────────────────────

def probe_session(target: str | None) -> tuple[dict, str, str]:
    """Establish which session we can report, from the quote endpoint.

    Cheap (one or two HTTP calls), so the staleness and duplicate guards can
    bail out before the large-cap sweep.
    """
    board = [t for t, _ in INDEX_EQUITIES] + [t for t, _ in INDEX_ALT] + ["SOXL"]
    log(f"probing {len(board)} index/board tickers")
    quotes = fetch_quotes(board, "board")

    missing = [t for t in board if t not in quotes]
    if missing:
        log(f"  {len(missing)} ticker(s) had no quote, falling back to daily bars: "
            f"{', '.join(missing[:8])}")
        bars = fetch_daily_moves(missing, "board-bars")
        if target:
            fill_pending_closes(bars, target)
        quotes.update(bars)

    open_now = [t for t, r in quotes.items()
                if r.get("market_state") == "REGULAR" and t in dict(INDEX_EQUITIES)]
    if open_now:
        log(f"  NOTE: {len(open_now)} equity ticker(s) still in a REGULAR session -- "
            f"prices are intraday, not settled closes")

    eq_dates = [quotes[t]["bar_date"] for t, _ in INDEX_EQUITIES
                if t in quotes and quotes[t].get("bar_date")]
    equity_asof = max(set(eq_dates), key=eq_dates.count) if eq_dates else "unknown"
    alt_dates = [quotes[t]["bar_date"] for t, _ in INDEX_ALT
                 if t in quotes and quotes[t].get("bar_date")]
    alt_asof = max(alt_dates) if alt_dates else "unknown"
    return quotes, equity_asof, alt_asof


def get_moves(con, tickers: list[str], session: str, label: str) -> dict[str, dict]:
    """Daily moves for `session`: quote endpoint first, then the local DB.

    The quote endpoint carries the exchange's own previous close, so it is
    immune to the null/regressed bars that made the historical array compare
    against the wrong session. price_history covers whatever the quotes miss.
    """
    quotes = fetch_quotes(tickers, label)
    fresh = {t: r for t, r in quotes.items() if r.get("bar_date") == session}
    stale = len(quotes) - len(fresh)

    missing = [t for t in tickers if t not in fresh]
    from_db = load_prices_from_db(con, missing, session) if missing else {}
    log(f"  {label}: {len(fresh)} from quotes"
        f"{f' ({stale} quoted for another session)' if stale else ''}"
        f"{f', {len(from_db)} from price_history' if from_db else ''}"
        f" of {len(tickers)}")

    merged = {**from_db, **fresh}
    still = [t for t in tickers if t not in merged]
    if still:
        bars = fetch_daily_moves(still, f"{label}-bars")
        fill_pending_closes(bars, session)
        merged.update({t: r for t, r in bars.items() if r["bar_date"] == session})
    return merged


def build_report(quotes: dict, equity_asof: str, alt_asof: str,
                 news_hours: int = 48, skip_news: bool = False,
                 max_articles: int = 6) -> dict:
    con = open_db()
    try:
        universe = load_large_cap_universe(con)
        holdings = load_soxx_holdings(con)
        soxx_meta = load_soxx_meta(con, holdings)
        if con is not None and soxx_meta:
            _write_universe_cache(universe, soxx_meta)
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass
    log(f"universe: {len(universe)} stocks >= $10B (ETFs excluded); "
        f"{len(holdings)} SOXX constituents")

    con2 = open_db()
    try:
        soxx_quotes = get_moves(con2, holdings, equity_asof, "soxx")
        cap_quotes = get_moves(con2, sorted(universe), equity_asof, "large-cap")
    finally:
        if con2 is not None:
            try:
                con2.close()
            except Exception:
                pass

    # SOXX constituents. Metadata comes from soxx_meta first (no market-cap
    # screen) and the $10B+ universe second, so sub-$10B names like NVMI still
    # get a real company name for the news query and the reason prompt.
    soxx_rows = []
    for t, rec in soxx_quotes.items():
        rec = dict(rec)
        meta = soxx_meta.get(t) or universe.get(t) or {}
        for k in ("name", "sector", "industry"):
            rec[k] = meta.get(k) or ""
        soxx_rows.append(rec)
    soxx_rows.sort(key=lambda r: r["pct_change"], reverse=True)
    soxx_up = soxx_rows[:SOXX_MOVERS_N]
    soxx_down = sorted(soxx_rows[-SOXX_MOVERS_N:], key=lambda r: r["pct_change"]) if soxx_rows else []

    # Large-cap movers
    candidates, rejected = [], []
    for t, rec in cap_quotes.items():
        meta = universe.get(t, {})
        if rec.get("volume") in (None, 0):
            continue
        if abs(rec["pct_change"]) > MAX_PLAUSIBLE_MOVE_PCT:
            rejected.append(f'{t} {rec["pct_change"]:+.1f}%')
            continue
        candidates.append({**rec, **{k: meta.get(k) for k in ("sector", "industry", "name", "market_cap")}})
    if rejected:
        log(f"  filtered {len(rejected)} implausible move(s) (>{MAX_PLAUSIBLE_MOVE_PCT:g}%): "
            f"{', '.join(rejected[:8])}")
    # Tickers in fundamentals that Yahoo no longer serves -- delistings, buyouts
    # and renames the local DB has not caught up with. Surfaced in the footer so
    # an incomplete screen is visible rather than silent.
    unavailable = len(universe) - len(cap_quotes)
    candidates.sort(key=lambda r: r["pct_change"], reverse=True)
    winners, losers = candidates[:MOVERS_N], list(reversed(candidates[-MOVERS_N:]))
    log(f"  movers: {len(candidates)} ranked -> top {len(winners)} / bottom {len(losers)}")

    # SOXL's own RSI is needed before the narrative, so it is fetched here
    # rather than in main() after this function returns, as it used to be.
    soxl = quotes.get("SOXL") or {}
    soxl_rsi, soxl_rsi_date = fetch_soxl_rsi(equity_asof, soxl.get("close"))

    # 5d/3m/1y for everything that gets rendered, in one download.
    rendered = winners + losers + soxx_up + soxx_down
    attach_horizons(rendered, fetch_multi_horizon([m["ticker"] for m in rendered]))

    # ── News + AI ────────────────────────────────────────────────────────────
    # ONE subprocess for the union of the large-cap movers and every SOXX
    # constituent. news_fetcher shares a single body-fetch pool across all
    # tickers under one deadline, which is the whole reason it finishes in
    # time; a second subprocess would forfeit that and pay another yfinance
    # import besides. Bodies are limited to the names that get written up --
    # the rest supply headlines for the sector read only.
    reasons: dict[str, list[str]] = {}
    narrative = {"themes": [], "prior_sessions": []}
    summarised = winners + losers + soxx_up + soxx_down
    if skip_news:
        log("news: skipped (--no-news)")
        for m in summarised + soxx_rows:
            m.setdefault("news", [])
    else:
        log("fetching news headlines")
        by_ticker: dict[str, dict] = {}
        for m in summarised + soxx_rows:          # summarised first: it wins ties
            by_ticker.setdefault(m["ticker"], m)
        news_targets = list(by_ticker.values())
        attach_news(news_targets, hours=news_hours, limit=max_articles,
                    bodies_for={m["ticker"] for m in summarised})
        # attach_news mutates the deduped representative; fan the result back
        # out to the other dicts holding the same ticker.
        for m in summarised + soxx_rows:
            if m is not by_ticker[m["ticker"]]:
                m["news"] = by_ticker[m["ticker"]].get("news", [])

        reasons = generate_reasons(summarised)
        narrative = generate_soxl_narrative(equity_asof, quotes.get("SOXL"),
                                            soxl_rsi, soxx_rows)
        save_narrative(equity_asof, narrative.get("themes") or [])

    spy, qqq = quotes.get("SPY"), quotes.get("QQQ")
    bits = [f'{n} {q["pct_change"]:+.2f}%' for n, q in (("SPY", spy), ("QQQ", qqq)) if q]
    try:
        headline_date = datetime.strptime(equity_asof, "%Y-%m-%d").strftime("%a %d %b %Y")
    except ValueError:
        headline_date = equity_asof

    return {
        "quotes": quotes,
        "alt_rows": [(t, d, quotes.get(t)) for t, d in INDEX_ALT],
        "equity_asof": equity_asof,
        "alt_asof": alt_asof,
        "headline_date": headline_date,
        "generated_at": datetime.now(CST).strftime("%Y-%m-%d %H:%M CST"),
        "soxl_rsi": soxl_rsi, "soxl_rsi_date": soxl_rsi_date,
        "soxx_up": soxx_up, "soxx_down": soxx_down, "narrative": narrative,
        "holdings_n": len(holdings), "holdings_asof": holdings_asof(),
        "winners": winners, "losers": losers, "reasons": reasons,
        "universe_n": len(universe),
        "unavailable_n": unavailable,
        "filtered_n": len(rejected),
        "subject": "Market Close — " + headline_date + (" — " + " · ".join(bits) if bits else ""),
    }


def already_sent(session: str) -> bool:
    try:
        return session in SENT_LOG.read_text(encoding="utf-8").split()
    except Exception:
        return False


def mark_sent(session: str) -> None:
    try:
        REPORTS_DIR.mkdir(exist_ok=True)
        with SENT_LOG.open("a", encoding="utf-8") as fh:
            fh.write(f"{session}\n")
    except Exception as exc:
        log(f"WARN: could not record sent session ({type(exc).__name__}: {exc}).")


def _flush_log() -> None:
    """Append this run's lines to the log file. Called on EVERY exit path so a
    skipped run is still recorded -- otherwise the log shows the previous run
    and a gate exit looks like the job never fired."""
    try:
        REPORTS_DIR.mkdir(exist_ok=True)
        with (REPORTS_DIR / "daily_close.log").open("a", encoding="utf-8") as fh:
            fh.write("\n".join(_log_lines) + "\n")
    except Exception:
        pass


def _write_run_state(outcome: str, session: str | None,
                     detail: str = "", exit_code: int = 0) -> None:
    """Record how this run ended, for the Streamlit sidebar to read.

    outcome is "sent", "skipped" or "error". Written on every exit path, and
    from the __main__ guard when main() raises, so a crashed run leaves a trace
    even though it never reached _flush_log().
    """
    try:
        REPORTS_DIR.mkdir(exist_ok=True)
        tmp = RUN_STATE.with_suffix(".json.tmp")
        tmp.write_text(json.dumps({
            "finished_at": datetime.now(CST).isoformat(timespec="seconds"),
            "outcome": outcome,
            "session": session,
            "detail": detail,
            "exit_code": exit_code,
            "argv": sys.argv[1:],
        }, indent=2), encoding="utf-8")
        tmp.replace(RUN_STATE)
    except Exception:
        pass


def _finish(outcome: str, session: str | None, detail: str = "",
            exit_code: int = 0) -> int:
    """Flush the log and record the run state together. Every return in main()
    goes through here so the two can never disagree."""
    _flush_log()
    _write_run_state(outcome, session, detail, exit_code)
    return exit_code


def refresh_universe_cache(con) -> int:
    """Rewrite universe_cache.json from a live DuckDB connection; returns the
    row count.

    Exists for app.py: Streamlit holds the only writable DuckDB connection for
    its whole lifetime, so a run launched from the sidebar cannot open the DB and
    would fall back to whatever snapshot was last written. The app has the
    connection already, so it refreshes the cache on the subprocess's behalf.
    """
    # load_large_cap_universe() writes the cache itself on a successful query,
    # so this is just the public, app-facing name for "do that now".
    return len(load_large_cap_universe(con))


def main() -> int:
    ap = argparse.ArgumentParser(description="Daily market close newsletter.")
    ap.add_argument("--email", action="store_true", help="send the newsletter via Gmail SMTP")
    ap.add_argument("--force", action="store_true",
                    help="resend a session already in sent_sessions.txt, and bypass the "
                         "staleness and no-clobber checks")
    ap.add_argument("--open", action="store_true", dest="open_html", help="open the archived HTML")
    ap.add_argument("--no-news", action="store_true", help="skip news + AI reasons (fast)")
    ap.add_argument("--news-hours", type=int, default=48, help="article lookback window")
    ap.add_argument("--check-holdings", action="store_true",
                    help="audit soxx_holdings.txt and exit")
    ap.add_argument("--articles", type=int, default=6,
                    help="max articles fetched per mover (default 6)")
    args = ap.parse_args()

    if args.check_holdings:
        return check_holdings()

    # Fail on missing credentials BEFORE spending a minute fetching.
    if args.email:
        ok, reason = mail_preflight()
        if not ok:
            log(f"ERROR: --email requested but {reason}")
            return _finish("error", None, f"--email requested but {reason}", 2)

    # ── Which session are we here for? ───────────────────────────────────────
    # CATCH-UP SEMANTICS. This used to require the last completed session to be
    # *today* (ET) and bail otherwise, which lost a session permanently whenever
    # the Mac slept through all of its evening slots: the run that fires on wake
    # sees yesterday's session and refused it. It also meant the 08:00 catch-up
    # slot in the plist never worked even once, since at 08:00 ET the last
    # completed session is always the previous day.
    #
    # The gate is gone. Any completed session that is not yet in sent_sessions.txt
    # gets sent, whatever day it is now. Running before today's close is still a
    # no-op, because last_completed_trading_day() returns yesterday until 16:00 ET
    # and yesterday is already in the sent log -- so the duplicate check below
    # covers the case the old gate was really protecting against.
    last_done = mc.last_completed_trading_day()
    today = mc.et_today().isoformat()
    if last_done is None:
        log("No completed trading session in the calendar lookback. Nothing sent.")
        return _finish("skipped", None, "no completed session in lookback")

    # Cheap duplicate check first: a no-op attempt now costs no HTTP at all,
    # instead of the ~5s probe it used to spend before reaching the same answer.
    # Four of the five daily slots normally land here.
    if args.email and already_sent(last_done) and not args.force:
        log(f"Session {last_done} was already emailed -- nothing sent. "
            f"Use --force to send it again.")
        return _finish("skipped", last_done, "already emailed")

    if last_done != today:
        log(f"Catching up: last completed session {last_done} has not been sent "
            f"(ET today {today}).")

    t0 = time.time()
    quotes, equity_asof, alt_asof = probe_session(last_done)
    session = equity_asof

    # "unknown" means every equity quote failed. It is not a date, and because
    # "unknown" > any ISO date as a string it slips straight past the staleness
    # check below and would archive an unknown_close.html.
    if session == "unknown":
        log("No equity quote resolved a bar date -- Yahoo returned nothing usable. "
            "Nothing sent, will retry on the next scheduled run.")
        return _finish("skipped", None, "no equity quotes resolved")

    if last_done and session < last_done and not args.force:
        log(f"Data is stale: latest bar is {session} but the last completed session is "
            f"{last_done}. Yahoo has not published it yet -- nothing sent, will retry "
            f"on the next scheduled run. Use --force to send anyway.")
        return _finish("skipped", session, f"stale: newest bar {session} < {last_done}")

    # session (the bar date Yahoo actually served) can differ from last_done, so
    # this is not redundant with the check above.
    if args.email and already_sent(session) and not args.force:
        log(f"Session {session} was already emailed -- nothing sent. "
            f"Use --force to send it again.")
        return _finish("skipped", session, "already emailed")

    ctx = build_report(quotes, equity_asof, alt_asof,
                       news_hours=args.news_hours, skip_news=args.no_news,
                       max_articles=args.articles)
    html_body, text_body = render_within_budget(ctx)

    REPORTS_DIR.mkdir(exist_ok=True)
    out = REPORTS_DIR / f"{session}_close.html"
    # Never replace a richer archived report with a thinner one (e.g. a retry
    # that lost its news) -- that destroyed a good report once already.
    if out.exists() and out.stat().st_size > len(html_body.encode()) and not args.force:
        log(f"keeping existing {out.name} ({out.stat().st_size:,} bytes) -- "
            f"this run produced less ({len(html_body):,}).")
    else:
        out.write_text(html_body, encoding="utf-8")
        log(f"saved {out}  ({len(html_body):,} bytes, {time.time() - t0:.0f}s)")

    status, outcome, detail = 0, "built", ""
    if args.email:
        try:
            import mailer
            to = mailer.send_html(ctx["subject"], html_body, text_body)
            mark_sent(session)
            log(f"emailed to {to} (session {session})")
            outcome, detail = "sent", to
        except Exception as exc:
            log(f"ERROR: {exc}")
            status, outcome, detail = 1, "error", str(exc)

    _flush_log()
    _write_run_state(outcome, session, detail, status)

    if args.open_html:
        webbrowser.open(out.resolve().as_uri())
    return status


def mail_preflight() -> tuple[bool, str]:
    try:
        import mailer
        return mailer.credentials_available()
    except Exception as exc:
        return False, f"mailer unavailable ({type(exc).__name__}: {exc})"


if __name__ == "__main__":
    # An exception out of main() otherwise leaves no trace anywhere: _flush_log()
    # never runs, so daily_close.log still shows the *previous* run and the
    # failure is invisible to anything watching.
    try:
        sys.exit(main())
    except SystemExit:
        raise
    except BaseException as _exc:
        log(f"ERROR: unhandled {type(_exc).__name__}: {_exc}")
        _flush_log()
        _write_run_state("error", None, f"unhandled {type(_exc).__name__}: {_exc}", 1)
        raise
