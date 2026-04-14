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
    """
    return datetime.now(_ET).hour >= 16
