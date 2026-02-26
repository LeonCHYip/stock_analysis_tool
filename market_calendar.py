"""
market_calendar.py — NYSE trading calendar utilities.

Uses pandas_market_calendars to detect missing trading days between
the last stored date and today.  Handles weekends, holidays, and gaps
caused by multi-week scanning pauses.
"""

from __future__ import annotations
from datetime import date, timedelta

import pandas as pd
import pandas_market_calendars as mcal

_NYSE = mcal.get_calendar("NYSE")


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
    and up to (but NOT including) today.

    If last_stored_date is None, returns [] — caller should do a full fetch.
    If there are no missing days (last_stored_date == yesterday's trading day),
    returns [].

    'today' defaults to date.today().  It is excluded because the current
    trading day's data is only final after NYSE close (4pm ET).  The
    is_finalized flag in tech_indicators handles today separately.
    """
    if last_stored_date is None:
        return []

    if today is None:
        today = date.today()

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
        return _NYSE.open_at_time(
            _NYSE.schedule(
                start_date=date.today().isoformat(),
                end_date=date.today().isoformat(),
            ),
            pd.Timestamp.now(tz="America/New_York"),
        )
    except Exception:
        return False


def nyse_close_passed_today() -> bool:
    """
    True if NYSE regular session has closed for today (after 4pm ET).
    Used to decide whether to set is_finalized=True for today's tech data.
    """
    from datetime import datetime
    from zoneinfo import ZoneInfo
    ET = ZoneInfo("America/New_York")
    now_et = datetime.now(ET)
    return now_et.hour >= 16
