"""
swing_analysis.py — Swing Cycle Analysis (Bottom -> Peak -> Bottom) detection.

Phase 1 scope: percentage-reversal ZigZag only, upward cycles only, no
charting. Detection/metrics logic is kept UI-free so app.py only calls
analyze_swings() and renders the result.

Price data: fetched on-demand per ticker via fetch_and_cache_swing_history()
into a dedicated DuckDB table (storage.swing_price_history), fully split/
dividend-adjusted (yf.download auto_adjust=True). This is deliberately
SEPARATE from the main scan pipeline's price_history table -- see the
_DDL_SWING_PRICE_HISTORY comment in storage.py for why mixing decades of
extra history into that table would silently corrupt existing indicators
(made_high_3y, days_since_prior_high_3y).

Look-ahead-bias handling: every TurningPoint carries both the historical
extreme (date/price) and the later confirmation (confirmation_date/
confirmation_price) at which the reversal rule had enough information to
recognize it. Metrics below use ONLY the extreme date/price for cycle
pricing and duration -- callers building trading signals must use the
confirmation_date fields instead to avoid look-ahead bias.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import numpy as np
import pandas as pd
import ta
import yfinance as yf

import storage
from market_calendar import et_today
from yf_session import YF_SESSION, YF_DL_LOCK

MAX_HISTORY_YEARS_DEFAULT = 30
MIN_TRADING_DAYS_REQUIRED = 30

PointType = Literal["peak", "bottom"]


@dataclass
class TurningPoint:
    date: pd.Timestamp
    price: float
    point_type: PointType
    confirmation_date: pd.Timestamp
    confirmation_price: float
    threshold_used: float | None = None


# ── Price history: fetch + cache ────────────────────────────────────────────

def _safe_float(v) -> float | None:
    try:
        f = float(v)
        return None if (np.isnan(f) or np.isinf(f)) else round(f, 4)
    except (TypeError, ValueError):
        return None


def _safe_int(v) -> int | None:
    try:
        f = float(v)
        return None if np.isnan(f) else int(f)
    except (TypeError, ValueError):
        return None


def _rows_to_df(rows: list[tuple]) -> pd.DataFrame:
    df = pd.DataFrame(rows, columns=["date", "open", "high", "low", "close", "volume"])
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date").sort_index()
    df["typical"] = (df["high"] + df["low"] + df["close"]) / 3.0
    return df


def fetch_and_cache_swing_history(
    ticker: str, years: int = MAX_HISTORY_YEARS_DEFAULT, force_refresh: bool = False
) -> pd.DataFrame:
    """Return fully split/dividend-adjusted OHLCV for `ticker`, up to `years`
    of history, via a per-ticker DuckDB cache (storage.swing_price_history).

    Freshness is by ET CALENDAR DAY of the last fetch, not by comparing the
    cached data's latest date against an "expected" trading day -- yfinance
    can lag the expected trading day by a day even right after a fresh
    download, which would make an expected-date comparison see the cache as
    permanently stale and re-download on every call, defeating the cache.
    So: reused freely for repeated analysis within the same ET day: refetched
    (and fully replaced) once a new day begins.

    `force_refresh=True` bypasses that same-day reuse and always re-downloads.
    Needed by callers that want the true latest close regardless of when the
    cache was last populated today -- e.g. a pre-market fetch would otherwise
    "lock in" yesterday's close as today's cache for the rest of the day, even
    after the market closes and a genuinely new close is available.
    """
    ticker = ticker.upper().strip()
    if not ticker:
        raise ValueError("ticker must not be empty")

    today_str = et_today().isoformat()
    if not force_refresh and storage.get_swing_history_fetch_date(ticker) == today_str:
        return _rows_to_df(storage.get_swing_price_history(ticker))

    with YF_DL_LOCK:  # see yf_session.py -- concurrent downloads cross-contaminate
        raw = yf.download(
            tickers=ticker,
            period="max",
            auto_adjust=True,
            threads=False,
            progress=False,
            session=YF_SESSION,
        )
    if raw is None or raw.empty:
        raise ValueError(f"No price data returned for {ticker!r}")

    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = raw.columns.get_level_values(0)

    required = ["Open", "High", "Low", "Close", "Volume"]
    missing = [c for c in required if c not in raw.columns]
    if missing:
        raise ValueError(f"Missing columns {missing} in downloaded data for {ticker!r}")

    df = raw[required].dropna(subset=["Close"]).copy()
    df.index = pd.to_datetime(df.index)
    df = df.sort_index()
    if df.empty:
        raise ValueError(f"No usable price rows for {ticker!r} after cleaning")

    # Sanity-check the download BEFORE trusting it enough to cache under
    # today's date -- once cached as "fetched today", nothing re-validates
    # it and every call for the rest of the ET day reuses it as-is (see the
    # freshness check above). A silently bad/truncated download (observed
    # once in practice: a normally-liquid ticker came back with only 2 rows
    # months stale) would otherwise get "locked in" until the next calendar
    # day. Better to raise here -- the caller's caching logic then simply
    # doesn't advance fetched_on, so the very next call retries fresh
    # instead of being stuck on garbage for the rest of the day.
    if len(df) < 20:
        raise ValueError(
            f"Downloaded data for {ticker!r} has only {len(df)} usable row(s) -- "
            "too few to trust; not caching. Possibly a transient yfinance issue."
        )
    days_stale = (pd.Timestamp.now() - df.index.max()).days
    if days_stale > 10:
        raise ValueError(
            f"Downloaded data for {ticker!r} is stale -- most recent usable row is "
            f"{df.index.max().date()}, {days_stale} days ago. Not caching."
        )

    cutoff = df.index.max() - pd.DateOffset(years=years)
    df = df[df.index >= cutoff]

    rows = [
        (idx.date().isoformat(), _safe_float(o), _safe_float(h), _safe_float(l), _safe_float(c), _safe_int(v))
        for idx, o, h, l, c, v in zip(df.index, df["Open"], df["High"], df["Low"], df["Close"], df["Volume"])
    ]

    # Never let an OLDER download overwrite newer cached data. A long-running
    # YF_SESSION can degrade and return day-stale prices (observed: the index
    # dashboard silently rolled back from 8/28 to 8/27 when a force_refresh
    # pulled stale data). rows/cached are date-ascending, so compare last
    # dates. Strict '<' only: an equal latest date is allowed to overwrite so
    # a same-day re-fetch still picks up retroactive split/dividend
    # re-adjustment of older bars. When we refuse, return the newer cache.
    cached = storage.get_swing_price_history(ticker)
    if cached and rows and rows[-1][0] < cached[-1][0]:
        return _rows_to_df(cached)

    storage.save_swing_price_history(ticker, rows, fetched_on=today_str)

    return _rows_to_df(rows)


def get_latest_quote_price(ticker: str) -> float | None:
    """Return the latest live quote price (Yahoo regularMarketPrice) for
    `ticker`, or None on any failure.

    Fallback for the index dashboard when Yahoo's most-recent daily bar still
    has a null close (its historical array lags the finalized close by up to a
    day), so the settled series is one trading day behind. NOTE: this is a
    LIVE quote -- during an open session it is an intraday price, not a settled
    close -- so callers should use it only to fill a genuine gap, not to
    replace an already-current settled close.
    """
    ticker = ticker.upper().strip()
    try:
        with YF_DL_LOCK:  # serialize with all other shared-session use
            fi = yf.Ticker(ticker, session=YF_SESSION).fast_info
            lp = fi.last_price
        lp = float(lp)
        return lp if lp == lp else None  # NaN guard (NaN != NaN)
    except Exception:
        return None


# ── Detection: Method A -- percentage-reversal ZigZag ───────────────────────

def detect_percentage_zigzag(
    data: pd.DataFrame,
    price_column: str,
    reversal_pct: float,
    min_duration: int = 5,
) -> list[TurningPoint]:
    """Detect alternating peak/bottom turning points using a percentage-
    reversal ZigZag.

    A candidate extreme updates every bar while the trend continues. It is
    CONFIRMED as a turning point only once price reverses by at least
    `reversal_pct`% from it AND at least `min_duration` trading days have
    elapsed since the previous confirmed turning point (a debounce on
    confirmation timing, not a filter on the swing's own length -- the
    candidate extreme keeps tracking the true local max/min throughout).

    The very last candidate extreme at the end of `data`, if any, is never
    confirmed/emitted -- an in-progress swing is excluded rather than marked
    incomplete (phase-1 simplification).

    Returns points in chronological order, strictly alternating peak/bottom.
    """
    if price_column not in data.columns:
        raise ValueError(f"price_column {price_column!r} not in columns {list(data.columns)}")
    if reversal_pct <= 0:
        raise ValueError(f"reversal_pct must be > 0, got {reversal_pct}")
    if min_duration < 0:
        raise ValueError(f"min_duration must be >= 0, got {min_duration}")

    prices = data[price_column].to_numpy(dtype=float)
    dates = data.index
    n = len(prices)
    if n < 2:
        return []

    frac = reversal_pct / 100.0
    turning_points: list[TurningPoint] = []

    pivot_idx = 0
    pivot_price = prices[0]
    direction: int | None = None   # +1 rising (seeking peak), -1 falling (seeking bottom)
    extreme_idx = 0
    extreme_price = prices[0]

    for i in range(1, n):
        p = prices[i]

        if direction is None:
            if p >= pivot_price * (1 + frac):
                turning_points.append(TurningPoint(
                    date=dates[pivot_idx], price=float(pivot_price), point_type="bottom",
                    confirmation_date=dates[i], confirmation_price=float(p),
                    threshold_used=reversal_pct,
                ))
                direction = 1
                extreme_idx, extreme_price = i, p
            elif p <= pivot_price * (1 - frac):
                turning_points.append(TurningPoint(
                    date=dates[pivot_idx], price=float(pivot_price), point_type="peak",
                    confirmation_date=dates[i], confirmation_price=float(p),
                    threshold_used=reversal_pct,
                ))
                direction = -1
                extreme_idx, extreme_price = i, p
            continue

        if direction == 1:
            if p >= extreme_price:
                extreme_idx, extreme_price = i, p
            elif (i - pivot_idx) >= min_duration and p <= extreme_price * (1 - frac):
                turning_points.append(TurningPoint(
                    date=dates[extreme_idx], price=float(extreme_price), point_type="peak",
                    confirmation_date=dates[i], confirmation_price=float(p),
                    threshold_used=reversal_pct,
                ))
                pivot_idx, pivot_price = extreme_idx, extreme_price
                direction = -1
                extreme_idx, extreme_price = i, p
        else:
            if p <= extreme_price:
                extreme_idx, extreme_price = i, p
            elif (i - pivot_idx) >= min_duration and p >= extreme_price * (1 + frac):
                turning_points.append(TurningPoint(
                    date=dates[extreme_idx], price=float(extreme_price), point_type="bottom",
                    confirmation_date=dates[i], confirmation_price=float(p),
                    threshold_used=reversal_pct,
                ))
                pivot_idx, pivot_price = extreme_idx, extreme_price
                direction = 1
                extreme_idx, extreme_price = i, p

    return turning_points


# ── Cycle assembly ───────────────────────────────────────────────────────────

def build_swing_cycles(
    turning_points: list[TurningPoint],
    direction: Literal["up", "down"] = "up",
) -> list[tuple[int, TurningPoint, TurningPoint, TurningPoint]]:
    """Pair consecutive alternating turning points into complete cycles.

    direction="up": bottom -> peak -> next bottom.
    direction="down": peak -> bottom -> next peak.

    turning_points must already strictly alternate (true for
    detect_percentage_zigzag's output). Consecutive cycles share their
    boundary point (cycle 2's start bottom = cycle 1's end bottom), matching
    how sequential swings are normally read off a chart.
    """
    if direction not in ("up", "down"):
        raise ValueError(f"direction must be 'up' or 'down', got {direction!r}")
    start_type: PointType = "bottom" if direction == "up" else "peak"
    mid_type: PointType = "peak" if direction == "up" else "bottom"

    cycles = []
    cycle_num = 0
    for i in range(len(turning_points) - 2):
        a, b, c = turning_points[i], turning_points[i + 1], turning_points[i + 2]
        if a.point_type == start_type and b.point_type == mid_type and c.point_type == start_type:
            cycle_num += 1
            cycles.append((cycle_num, a, b, c))
    return cycles


def _safe_round(v, nd=4):
    if v is None:
        return None
    try:
        f = float(v)
        return None if (np.isnan(f) or np.isinf(f)) else round(f, nd)
    except (TypeError, ValueError):
        return None


def calculate_swing_metrics(
    cycle_number: int,
    start: TurningPoint,
    mid: TurningPoint,
    end: TurningPoint,
    data: pd.DataFrame,
    atr: pd.Series,
    ticker: str,
    method: str,
) -> dict:
    """Compute the phase-1 metric set for one bottom-peak-bottom cycle.

    Trading-day counts are index-position differences in `data` (which has
    one row per trading day, no gaps): e.g. bottom at position 10, peak at
    position 15 -> 5 trading days -- exclusive of the start day, inclusive of
    the end day. Uses turning-point EXTREME dates/prices (not confirmation
    dates) -- these are the analytical cycle boundaries, not trading signals.
    """
    up_days = data.index.get_loc(mid.date) - data.index.get_loc(start.date)
    down_days = data.index.get_loc(end.date) - data.index.get_loc(mid.date)

    up_slice = data.loc[start.date:mid.date]
    down_slice = data.loc[mid.date:end.date]

    return {
        "Cycle #": cycle_number,
        "Ticker": ticker,
        "Method": method,
        "Bottom Start Date": start.date.date(),
        "Peak Date": mid.date.date(),
        "Next Bottom Date": end.date.date(),
        "Bottom Confirmation Date": start.confirmation_date.date(),
        "Peak Confirmation Date": mid.confirmation_date.date(),
        "Next Bottom Confirmation Date": end.confirmation_date.date(),
        "Complete": True,
        "Start Bottom Price": round(start.price, 4),
        "Peak Price": round(mid.price, 4),
        "End Bottom Price": round(end.price, 4),
        "Bottom-to-Peak Trading Days": up_days,
        "Peak-to-Bottom Trading Days": down_days,
        "Total Trading Days": up_days + down_days,
        "Bottom-to-Peak Calendar Days": (mid.date - start.date).days,
        "Peak-to-Bottom Calendar Days": (end.date - mid.date).days,
        "Total Calendar Days": (end.date - start.date).days,
        "Up Return %": round((mid.price / start.price - 1) * 100, 2),
        "Down Return %": round((end.price / mid.price - 1) * 100, 2),
        "Cycle Return %": round((end.price / start.price - 1) * 100, 2),
        "Avg Volume (Up Phase)": _safe_round(up_slice["volume"].mean(), 0),
        "Avg Volume (Down Phase)": _safe_round(down_slice["volume"].mean(), 0),
        "ATR @ Start Bottom": _safe_round(atr.get(start.date)),
        "ATR @ Peak": _safe_round(atr.get(mid.date)),
        "ATR @ End Bottom": _safe_round(atr.get(end.date)),
    }


# ── Orchestration ────────────────────────────────────────────────────────────

def analyze_swings(
    ticker: str,
    reversal_pct: float,
    min_duration: int = 5,
    price_column: str = "close",
    years: int = MAX_HISTORY_YEARS_DEFAULT,
    start_date: str | None = None,
    end_date: str | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Fetch, detect, and summarize upward swing cycles for `ticker`.

    Returns (cycles_df, price_data). price_data is the (possibly date-range-
    filtered) OHLCV+typical DataFrame actually used for detection -- returned
    for a future chart, unused by the table itself.

    Raises ValueError on bad tickers, missing data, invalid parameters, or
    insufficient history (fewer than MIN_TRADING_DAYS_REQUIRED trading days
    in the selected range).
    """
    data = fetch_and_cache_swing_history(ticker, years=years)
    if data.empty:
        raise ValueError(f"No price data available for {ticker!r}")

    if start_date:
        data = data[data.index >= pd.to_datetime(start_date)]
    if end_date:
        data = data[data.index <= pd.to_datetime(end_date)]

    if len(data) < MIN_TRADING_DAYS_REQUIRED:
        raise ValueError(
            f"Only {len(data)} trading days available for {ticker!r} in the selected "
            f"range -- need at least {MIN_TRADING_DAYS_REQUIRED}."
        )
    if price_column not in data.columns:
        raise ValueError(f"Unknown price_column {price_column!r}; expected one of "
                          f"{['close', 'high', 'low', 'typical']}")

    turning_points = detect_percentage_zigzag(data, price_column, reversal_pct, min_duration)

    atr = ta.volatility.AverageTrueRange(
        data["high"], data["low"], data["close"], window=14
    ).average_true_range()

    raw_cycles = build_swing_cycles(turning_points, direction="up")
    rows = [
        calculate_swing_metrics(num, a, b, c, data, atr, ticker.upper().strip(), "Percentage ZigZag")
        for num, a, b, c in raw_cycles
    ]
    cycles_df = pd.DataFrame(rows)
    return cycles_df, data
