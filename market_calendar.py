"""
market_calendar.py — NYSE trading calendar utilities.

Uses pandas_market_calendars to detect missing trading days between
the last stored date and today.  Handles weekends, holidays, and gaps
caused by multi-week scanning pauses.

All "today" references use US/Eastern time so the logic is correct
regardless of the user's local timezone or VPN exit location.
"""

from __future__ import annotations
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import pandas_market_calendars as mcal

_NYSE = mcal.get_calendar("NYSE")
_ET   = ZoneInfo("America/New_York")


def et_today() -> date:
    """Return the current calendar date in US/Eastern (NYSE) timezone.

    Use this instead of date.today() so the result is always consistent
    with NYSE trading hours regardless of the user's local clock or VPN.
    """
    return datetime.now(_ET).date()


def now_et() -> datetime:
    """Return the current datetime (with time-of-day) in US/Eastern (NYSE)
    timezone. Use for any check needing wall-clock time relative to NYSE
    hours -- et_today() only gives the date, not the time-of-day."""
    return datetime.now(_ET)


def get_trading_days(start: str | date, end: str | date) -> list[str]:
    """
    Return list of NYSE trading day strings 'YYYY-MM-DD' in [start, end].
    """
    schedule = _NYSE.schedule(start_date=str(start), end_date=str(end))
    if schedule.empty:
        return []
    return [str(d.date()) for d in schedule.index]


def get_missing_trading_days(
    last_stored_date: str | date | None,
    today: str | date | None = None,
) -> list[str]:
    """
    Returns list of trading day strings that are AFTER last_stored_date
    and up to (but NOT including) today (ET).

    If last_stored_date is None, returns [] — caller should do a full fetch.
    If there are no missing days (last_stored_date == yesterday's trading day),
    returns [].

    'today' defaults to et_today().  It is excluded because the current
    trading day's data is only final after NYSE close (4pm ET).  The
    is_finalized flag in tech_indicators handles today separately.
    """
    if last_stored_date is None:
        return []

    if today is None:
        today = et_today()

    last = pd.Timestamp(str(last_stored_date)).date()
    end  = pd.Timestamp(str(today)).date() - timedelta(days=1)

    if end <= last:
        return []

    all_days = get_trading_days(last + timedelta(days=1), end)
    return all_days


def is_market_open_now() -> bool:
    """
    True if the NYSE is currently in its regular session.
    Uses pandas_market_calendars' is_open_now() (requires pytz).
    """
    try:
        today_et = et_today().isoformat()
        return _NYSE.open_at_time(
            _NYSE.schedule(start_date=today_et, end_date=today_et),
            pd.Timestamp.now(tz="America/New_York"),
        )
    except Exception:
        return False


def get_last_trading_day_before_today() -> str | None:
    """
    Return the most recent NYSE trading day strictly before today (ET) as an
    ISO string.  Used to detect stale tech_indicators rows.
    """
    today = et_today()
    for days_back in range(1, 11):
        d = today - timedelta(days=days_back)
        trading = get_trading_days(d.isoformat(), d.isoformat())
        if trading:
            return trading[0]
    return None


def nyse_close_passed_today() -> bool:
    """
    True if NYSE regular session has closed for today ET (after 4pm ET).
    Used to decide whether to set is_finalized=True for today's tech data.

    NOTE: time-only -- returns True after 4pm ET even on a NON-trading day
    (weekend/holiday). Callers that want "the latest trading day that has
    actually closed" should use last_completed_trading_day() instead.
    """
    return datetime.now(_ET).hour >= 16


def last_completed_trading_day() -> str | None:
    """
    Return the most recent NYSE trading day whose regular session has already
    closed, as an ISO string (or None if none found in the lookback window).

    Today if it is a trading day AND its 4pm ET close has passed; otherwise
    (weekend, holiday, or intraday before close) the last trading day before
    today. Prefer this over the
    `et_today() if nyse_close_passed_today() else get_last_trading_day_before_today()`
    idiom -- nyse_close_passed_today() is time-only, so that idiom overshoots
    to a weekend/holiday date (e.g. Saturday), which mislabels the latest
    completed session and breaks stale/freshness comparisons.
    """
    today = et_today()
    if get_trading_days(today.isoformat(), today.isoformat()) and nyse_close_passed_today():
        return today.isoformat()
    return get_last_trading_day_before_today()


def next_trading_day_on_or_after(d: str | date) -> str | None:
    """
    Return the first NYSE trading day on or after `d` as an ISO string.
    Used by the backtester to roll a scheduled date forward when it falls on
    a weekend/holiday.  A 14-calendar-day lookahead window is generous for
    any real NYSE gap (longest is ~9 days around Christmas/New Year); None
    signals a caller bug or a date far outside the calendar's known range,
    not a normal weekend/holiday case.
    """
    start = pd.Timestamp(str(d)).date()
    candidates = get_trading_days(start, start + timedelta(days=14))
    return candidates[0] if candidates else None
