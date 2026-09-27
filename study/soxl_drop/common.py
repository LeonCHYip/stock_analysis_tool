"""Shared loaders / helpers for the SOXL drop study (read-only; no DB writes)."""
from pathlib import Path
import numpy as np
import pandas as pd

HERE = Path(__file__).parent
START = pd.Timestamp("2023-09-14")   # 3-year primary window
END = pd.Timestamp("2026-09-14")
HORIZONS = [1, 2, 3, 5, 10, 21, 63]
THRESHOLDS = [-5, -7, -8, -10, -12, -15, -20]


def load():
    p = pd.read_csv(HERE / "prices.csv", parse_dates=["date"])
    close = p.pivot(index="date", columns="ticker", values="close")
    opn = p.pivot(index="date", columns="ticker", values="open")
    # Yahoo prints VIX on some exchange holidays (Memorial/Labor Day 2026); those
    # empty equity rows would break rolling windows and returns, so drop them.
    keep = close["MU"].notna()
    return close[keep], opn[keep]


def daily_ret(close):
    return close.pct_change(fill_method=None) * 100


def fwd_ret(s, h):
    """Close t -> close t+h, in %. NaN when t+h is beyond the data."""
    return (s.shift(-h) / s - 1) * 100


def fwd_min(s, h):
    """Worst close over t+1..t+h vs close t, in % (max adverse excursion)."""
    fut = pd.concat([s.shift(-k) for k in range(1, h + 1)], axis=1)
    out = (fut.min(axis=1) / s - 1) * 100
    out[s.shift(-h).isna()] = np.nan
    return out


def fwd_max(s, h):
    fut = pd.concat([s.shift(-k) for k in range(1, h + 1)], axis=1)
    out = (fut.max(axis=1) / s - 1) * 100
    out[s.shift(-h).isna()] = np.nan
    return out
