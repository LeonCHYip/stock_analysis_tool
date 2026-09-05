"""
technical_fetcher.py — Extended technical indicators + DuckDB storage.

Key differences from data_fetcher.py:
  - Computes the full extended indicator set (RSI, MACD, BBands, ATR, ADX,
    Stochastic, EMA, Donchian, CMF, A/D, realized vol, max drawdown, gaps,
    rolling streaks) in addition to the original T1-T4 comparison dicts.
  - Stores results in tech_indicators via storage.py.
  - Sets is_finalized based on whether NYSE has closed today.
  - fetch_and_store_bulk(): downloads 3y OHLCV for a batch, computes, saves.
  - detect_and_backfill(): finds tickers with unfinalized or missing trading
    days and re-fetches only those tickers.
"""

from __future__ import annotations
import json
import math
import socket
import threading
import time
from datetime import date, datetime
from zoneinfo import ZoneInfo

try:
    import psutil as _psutil
    _DIAG_PROC = _psutil.Process()
except Exception:
    _DIAG_PROC = None


def _diag_res() -> str:
    """One-line resource snapshot for diagnostic log lines."""
    if _DIAG_PROC is None:
        return "rss=? threads=?"
    return (f"rss={_DIAG_PROC.memory_info().rss / 1024 / 1024:.0f}MB "
            f"threads={_DIAG_PROC.num_threads()}")

# Prevent yf.download(threads=False) from hanging indefinitely on a dead TCP
# connection (e.g. after a VPN switch kills in-flight sockets).
# This timeout applies per socket read/write operation, not per whole request,
# so legitimate large downloads still complete; only stalled connections fire.
socket.setdefaulttimeout(120)

import numpy as np
import pandas as pd
import ta
import yfinance as yf

from yf_session import YF_SESSION, YF_DL_LOCK

from market_calendar import (
    get_missing_trading_days, nyse_close_passed_today,
    get_last_trading_day_before_today, et_today,
)
import storage

ET  = ZoneInfo("America/New_York")
CST = ZoneInfo("America/Chicago")

_BATCH_SIZE = 100   # yf.download tickers per call

# Two-tier incremental download (fetch_and_store_bulk): tickers with
# confirmed full-depth history (see ticker_history_depth) only need a small
# recent window downloaded each scan, not the whole 10y window every time.
_CHECK_WINDOW_DAYS = 60      # small check-window download size
# NOTE: the fast path's DB-history read (get_price_history_before) is
# deliberately NOT capped to _compute_all_indicators' bounded-window fields
# (756 trading days / "3Y" fields etc.) -- OBV and the A/D line are
# UNBOUNDED cumulative sums over whatever series is passed in, so truncating
# the read would silently change their value versus what a full download
# would have produced (verified empirically: capping to ~3y produced OBV/
# ad_line off by orders of magnitude between two consecutive scans of the
# same ticker with no new trading activity). The read is local DuckDB I/O,
# not a network call, so reading everything stored is cheap regardless.
# Fast-path safety net: if the assembled DataFrame is anomalously shallow for a
# ticker that SHOULD have deep history (a transient empty/short older-DB read),
# computing it would silently store a partial row -- every window >= 50 bars
# (sma50/150/200, ema200) NULL while short windows/RSI look fine. Below this
# many rows such a ticker is re-routed to a full download instead of saved
# partial. 220 covers sma200's 200-bar need plus buffer.
_MIN_ASSEMBLED_ROWS = 220
# A depth anchor (ticker_history_depth.earliest_date) older than this many
# calendar days (~275 trading days) means the ticker genuinely should have
# >= 220 bars, so a shallow assembly is a bug, not a young ticker.
_DEEP_HISTORY_MIN_DAYS = 400


# ── Helpers ───────────────────────────────────────────────────────────────────

def _extract_price_rows(df: pd.DataFrame, since: str | None = None) -> list[tuple]:
    """Extract (date_str, open, high, low, close, adj_close, volume) tuples
    from an OHLCV DataFrame. adj_close is None if the DataFrame has no
    "Adj Close" column (defensive -- yf.download with auto_adjust=False
    always includes it in practice).

    `since`: if given, only rows with date >= since are returned. Only pass
    this when a revision check (see _price_history_needs_full_reupload) has
    confirmed the ticker's older history hasn't been retroactively restated
    (e.g. by a stock split) since it was last stored -- a split rewrites the
    ENTIRE historical series, not just recent days, so trimming to a recent
    tail is only safe once that's been verified, not by default."""
    cols = ["Open", "High", "Low", "Close", "Volume"]
    has_adj = "Adj Close" in df.columns
    if has_adj:
        cols.append("Adj Close")
    sub = df[cols].dropna(subset=["Close"])
    if since is not None:
        sub = sub[sub.index >= since]
    rows = []
    for idx, row in sub.iterrows():
        d = idx.date().isoformat() if hasattr(idx, "date") else str(idx)[:10]
        o = _safe(row["Open"], 4)
        h = _safe(row["High"], 4)
        l = _safe(row["Low"], 4)
        c = _safe(row["Close"], 4)
        ac = _safe(row["Adj Close"], 4) if has_adj else None
        v = _safe_int(row["Volume"])
        if c is not None:
            rows.append((d, o, h, l, c, ac, v))
    return rows


def _price_history_needs_full_reupload(last_stored_date: str | None,
                                        stored_close: float | None,
                                        df: pd.DataFrame) -> bool:
    """True if `df`'s freshly downloaded raw Close at `last_stored_date`
    doesn't exactly match what's already stored there -- signals the
    ticker's historical OHLC may have been retroactively revised (a stock
    split restates the ENTIRE series, not just recent rows) since we last
    saved it, so a full re-upload is required rather than just the recent
    tail. Any inconclusive case (no prior date, no stored value, date not
    present in the fresh download) also returns True -- default to the
    slow-but-correct full upload whenever the fast path can't be verified.
    Compares against raw "Close" (not adjusted), matching what
    _extract_price_rows actually stores."""
    if not last_stored_date or stored_close is None:
        return True
    try:
        ts = pd.Timestamp(last_stored_date)
        if ts not in df.index:
            return True
        fresh_close = _safe(df.loc[ts, "Close"], 4)
    except Exception:
        return True
    return fresh_close is None or fresh_close != stored_close


def _assemble_price_df(older_rows: list[tuple], fresh_df: pd.DataFrame) -> pd.DataFrame:
    """Concatenate DB-stored history (`older_rows`, strictly before
    fresh_df's start -- see storage.get_price_history_before) with a
    freshly downloaded small check-window DataFrame into one continuous
    DataFrame matching _compute_all_indicators' expected input shape
    (DatetimeIndex; Open/High/Low/Close/Volume/Adj Close columns).

    older_rows: (date_str, open, high, low, close, adj_close, volume)
    tuples. Older rows generally lack adj_close (added to price_history
    after this feature shipped) -- left NaN, which
    _compute_all_indicators' existing per-row fallback already handles by
    using raw Close for just those rows."""
    if older_rows:
        older = pd.DataFrame(
            older_rows,
            columns=["date", "Open", "High", "Low", "Close", "Adj Close", "Volume"],
        )
        older["date"] = pd.to_datetime(older["date"])
        older = older.set_index("date")
    else:
        older = pd.DataFrame(
            columns=["Open", "High", "Low", "Close", "Adj Close", "Volume"],
            index=pd.DatetimeIndex([], name="date"),
        )

    fresh = fresh_df.copy()
    if isinstance(fresh.columns, pd.MultiIndex):
        fresh.columns = fresh.columns.get_level_values(0)
    if "Adj Close" not in fresh.columns:
        fresh["Adj Close"] = pd.NA
    fresh = fresh[["Open", "High", "Low", "Close", "Adj Close", "Volume"]]

    combined = pd.concat([older, fresh])
    combined = combined[~combined.index.duplicated(keep="last")]
    return combined.sort_index()


def _safe(v, ndigits=2):
    try:
        f = float(v)
        if math.isnan(f) or math.isinf(f):
            return None
        return round(f, ndigits) if ndigits is not None else f
    except Exception:
        return None


def _safe_int(v) -> int | None:
    try:
        f = float(v)
        if math.isnan(f) or math.isinf(f):
            return None
        return int(round(f))
    except Exception:
        return None


def _compare_averages(frame: pd.DataFrame, bars_back: int,
                      price_col: str, avg_window: int) -> dict | None:
    """Compare latest avg_window-bar avg vs avg_window-bar avg ending bars_back ago."""
    if frame is None or frame.empty:
        return None
    f = frame.dropna(subset=[price_col, "Volume"]).copy()
    if len(f) <= bars_back:
        return None
    latest_end = len(f) - 1
    prior_end  = latest_end - bars_back
    latest_slice = f.iloc[max(0, latest_end - avg_window + 1): latest_end + 1]
    prior_slice  = f.iloc[max(0, prior_end  - avg_window + 1): prior_end  + 1]
    if latest_slice.empty or prior_slice.empty:
        return None
    lp = float(latest_slice[price_col].mean())
    pp = float(prior_slice[price_col].mean())
    lv = float(latest_slice["Volume"].mean())
    pv = float(prior_slice["Volume"].mean())
    latest_dates  = [str(d.date()) for d in latest_slice.index]
    prior_dates   = [str(d.date()) for d in prior_slice.index]
    latest_prices = [_safe(v, 2) for v in latest_slice[price_col]]
    prior_prices  = [_safe(v, 2) for v in prior_slice[price_col]]
    price_pct = _safe((lp - pp) / abs(pp) * 100, 2) if pp else None
    vol_pct   = _safe((lv - pv) / abs(pv) * 100, 2) if pv else None
    return {
        "latest_date_range":  [latest_dates[0], latest_dates[-1]],
        "prior_date_range":   [prior_dates[0],  prior_dates[-1]],
        "latest_prices":      latest_prices,
        "prior_prices":       prior_prices,
        "latest_price_avg":   _safe(lp, 2),
        "prior_price_avg":    _safe(pp, 2),
        "price_up":           (lp > pp) if pp else None,
        "price_pct_change":   price_pct,
        "latest_volume_avg":  _safe_int(lv),
        "prior_volume_avg":   _safe_int(pv),
        "volume_up":          (lv > pv) if pv else None,
        "volume_pct_change":  vol_pct,
    }


def _rolling_streaks(up_series: pd.Series) -> tuple[int, int]:
    """
    Given a boolean series of 'up days', returns:
      (max_win_streak, count_of_win_streaks_ge_5)
    """
    max_streak = 0
    current    = 0
    streaks_5p = 0
    for val in up_series:
        if val:
            current += 1
            if current > max_streak:
                max_streak = current
        else:
            if current >= 5:
                streaks_5p += 1
            current = 0
    if current >= 5:
        streaks_5p += 1
    return max_streak, streaks_5p


# ── Core computation ──────────────────────────────────────────────────────────

def _compute_all_indicators(ticker: str, df_raw: pd.DataFrame,
                             weekly_latest_date: str | None = None) -> dict:
    """
    Given 3y+ of OHLCV data, compute all technical indicators.
    Returns a flat dict matching tech_indicators column names.

    weekly_latest_date: if set (e.g. '2026-03-13'), the weekly series is
    truncated at that date before computing T2 (weekly_vs_3m / weekly_vs_12m).
    Daily indicators and Close/as_of_date are always based on the full data.
    """
    try:
        df = df_raw.copy()

        # Flatten MultiIndex columns if present (single-ticker slice)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        required = ["Open", "High", "Low", "Close", "Volume"]
        if any(c not in df.columns for c in required):
            return {"error": f"Missing OHLCV columns for {ticker}"}

        df = df.dropna(subset=["Open", "High", "Low", "Close"])
        if df.empty:
            return {"error": f"No OHLCV data for {ticker}"}

        # Use Adj Close for return-based calculations when available
        if "Adj Close" in df.columns and df["Adj Close"].notna().any():
            df["RC"] = df["Adj Close"].where(df["Adj Close"].notna(), df["Close"])
        else:
            df["RC"] = df["Close"]

        close  = df["RC"]
        high   = df["High"]
        low    = df["Low"]
        volume = df["Volume"]
        n      = len(df)

        # ── Moving averages ───────────────────────────────────────────────────
        sma10  = ta.trend.SMAIndicator(close, window=10).sma_indicator()
        sma20  = ta.trend.SMAIndicator(close, window=20).sma_indicator()
        sma50  = ta.trend.SMAIndicator(close, window=50).sma_indicator()
        sma100 = ta.trend.SMAIndicator(close, window=100).sma_indicator()
        sma150 = ta.trend.SMAIndicator(close, window=150).sma_indicator()
        sma200 = ta.trend.SMAIndicator(close, window=200).sma_indicator()
        ema9   = ta.trend.EMAIndicator(close, window=9).ema_indicator()
        ema21  = ta.trend.EMAIndicator(close, window=21).ema_indicator()
        ema50e = ta.trend.EMAIndicator(close, window=50).ema_indicator()
        ema200 = ta.trend.EMAIndicator(close, window=200).ema_indicator()

        # ── RSI ───────────────────────────────────────────────────────────────
        rsi14 = ta.momentum.RSIIndicator(close, window=14).rsi()

        # ── MACD ─────────────────────────────────────────────────────────────
        _macd = ta.trend.MACD(close, window_fast=12, window_slow=26, window_sign=9)
        macd_line   = _macd.macd()
        macd_signal = _macd.macd_signal()
        macd_hist   = _macd.macd_diff()

        # ── Bollinger Bands (20, 2σ) ──────────────────────────────────────────
        _bb   = ta.volatility.BollingerBands(close, window=20, window_dev=2)
        bb_upper  = _bb.bollinger_hband()
        bb_middle = _bb.bollinger_mavg()
        bb_lower  = _bb.bollinger_lband()
        bb_pct_b  = _bb.bollinger_pband()   # (close - lower) / (upper - lower)

        # ── ATR (14) ──────────────────────────────────────────────────────────
        atr14_ser = ta.volatility.AverageTrueRange(high, low, close, window=14).average_true_range()

        # ── ADX (14) ──────────────────────────────────────────────────────────
        _adx    = ta.trend.ADXIndicator(high, low, close, window=14)
        adx14   = _adx.adx()
        plus_di = _adx.adx_pos()
        minus_di = _adx.adx_neg()

        # ── Stochastic (14, 3) ────────────────────────────────────────────────
        _stoch = ta.momentum.StochasticOscillator(high, low, close, window=14, smooth_window=3)
        stoch_k = _stoch.stoch()
        stoch_d = _stoch.stoch_signal()

        # ── OBV ───────────────────────────────────────────────────────────────
        obv = ta.volume.OnBalanceVolumeIndicator(close, volume).on_balance_volume()

        # ── Chaikin Money Flow (20) ───────────────────────────────────────────
        cmf20 = ta.volume.ChaikinMoneyFlowIndicator(
            high, low, close, volume, window=20
        ).chaikin_money_flow()

        # ── Accumulation/Distribution line ───────────────────────────────────
        ad_line = ta.volume.AccDistIndexIndicator(
            high, low, close, volume
        ).acc_dist_index()

        # ── Latest values ─────────────────────────────────────────────────────
        latest_close  = _safe(close.iloc[-1])
        latest_volume = _safe_int(volume.iloc[-1])
        latest_date   = str(df.index[-1].date())

        def _last(s: pd.Series):
            try:
                v = s.iloc[-1]
                return None if pd.isna(v) else float(v)
            except Exception:
                return None

        # MA values at latest
        v_sma10  = _safe(_last(sma10))
        v_sma20  = _safe(_last(sma20))
        v_sma50  = _safe(_last(sma50))
        v_sma100 = _safe(_last(sma100))
        v_sma150 = _safe(_last(sma150))
        v_sma200 = _safe(_last(sma200))
        v_ema9   = _safe(_last(ema9))
        v_ema21  = _safe(_last(ema21))
        v_ema50e = _safe(_last(ema50e))
        v_ema200 = _safe(_last(ema200))

        # MA checks
        _all_ma = all(v is not None for v in [v_sma10, v_sma20, v_sma50, v_sma150, v_sma200])
        ma10_gt_ma20   = (v_sma10 > v_sma20)   if _all_ma else None
        ma20_gt_ma50   = (v_sma20 > v_sma50)   if _all_ma else None
        ma50_gt_ma150  = (v_sma50 > v_sma150)  if _all_ma else None
        ma150_gt_ma200 = (v_sma150 > v_sma200) if _all_ma else None
        # MA50 > MA100 > MA150 (momentum screen) -- SMA100 is not part of _all_ma,
        # so guard on its own availability.
        ma50_gt_ma100  = (v_sma50 > v_sma100)  if (v_sma50 is not None and v_sma100 is not None) else None
        ma100_gt_ma150 = (v_sma100 > v_sma150) if (v_sma100 is not None and v_sma150 is not None) else None

        # MA slopes
        def _slope(series: pd.Series, lookback: int) -> float | None:
            if series is None or len(series) <= lookback:
                return None
            cur  = _last(series)
            prev = _last(series.iloc[:-lookback])
            if cur is None or prev is None or prev == 0:
                return None
            return _safe((cur - prev) / abs(prev) * 100)

        sma10_slope_10d  = _slope(sma10,  10)
        sma20_slope_10d  = _slope(sma20,  10)
        sma50_slope_20d  = _slope(sma50,  20)
        sma150_slope_20d = _slope(sma150, 20)
        sma200_slope_20d = _slope(sma200, 20)

        def _slope_gt(a, b):
            return (a > b) if (a is not None and b is not None) else None

        slope10_gt_slope20   = _slope_gt(sma10_slope_10d,  sma20_slope_10d)
        slope20_gt_slope50   = _slope_gt(sma20_slope_10d,  sma50_slope_20d)
        slope50_gt_slope150  = _slope_gt(sma50_slope_20d,  sma150_slope_20d)
        slope150_gt_slope200 = _slope_gt(sma150_slope_20d, sma200_slope_20d)

        # % distance from each MA
        def _pct_from_ma(close_val, ma_val) -> float | None:
            if close_val and ma_val and ma_val != 0:
                return _safe((close_val - ma_val) / abs(ma_val) * 100)
            return None

        pct_from_sma10  = _pct_from_ma(latest_close, v_sma10)
        pct_from_sma20  = _pct_from_ma(latest_close, v_sma20)
        pct_from_sma50  = _pct_from_ma(latest_close, v_sma50)
        pct_from_sma100 = _pct_from_ma(latest_close, v_sma100)
        pct_from_sma150 = _pct_from_ma(latest_close, v_sma150)
        pct_from_sma200 = _pct_from_ma(latest_close, v_sma200)
        pct_from_ema9   = _pct_from_ma(latest_close, v_ema9)
        pct_from_ema21  = _pct_from_ma(latest_close, v_ema21)
        pct_from_ema50  = _pct_from_ma(latest_close, v_ema50e)
        pct_from_ema200 = _pct_from_ma(latest_close, v_ema200)

        # ATR%
        v_atr14 = _safe(_last(atr14_ser))
        atr_pct = _safe(v_atr14 / latest_close * 100) if (v_atr14 and latest_close) else None

        # ── Volume metrics ────────────────────────────────────────────────────
        dollar_vol = (close * volume).replace(0, np.nan)
        avg_dv20 = _safe(dollar_vol.tail(20).mean(), 0)
        avg_dv50 = _safe(dollar_vol.tail(50).mean(), 0)
        med_vol50 = _safe(volume.tail(50).median(), 0)

        # Median-volume regime shift: median of the last N sessions vs the
        # median of the prior N (% change). Median (not mean) to resist single
        # blow-off-volume days. Needs 2N sessions; else None.
        def _med_vol_ratio(nwin: int) -> float | None:
            if len(volume) < 2 * nwin:
                return None
            recent = volume.tail(nwin).median()
            prior  = volume.iloc[-2 * nwin:-nwin].median()
            if prior and prior != 0 and not pd.isna(recent) and not pd.isna(prior):
                return _safe((recent - prior) / abs(prior) * 100)
            return None
        med_vol_ratio_60d = _med_vol_ratio(60)
        med_vol_ratio_90d = _med_vol_ratio(90)

        # 1D volume %
        raw_vol = volume.replace(0, np.nan)
        daily_vol_pct = None
        if len(raw_vol) >= 2:
            v_today = raw_vol.iloc[-1]
            v_prev  = raw_vol.iloc[-2]
            if v_today and v_prev and v_prev != 0:
                daily_vol_pct = _safe((v_today - v_prev) / abs(v_prev) * 100)

        # Relative volume (today vs N-day avg raw volume; excludes today)
        avg_vol_20d = _safe(raw_vol.iloc[:-1].tail(20).mean(), 0)
        avg_vol_50d = _safe(raw_vol.iloc[:-1].tail(50).mean(), 0)
        rel_vol_20d = None
        rel_vol_50d = None
        if latest_volume and avg_vol_20d and avg_vol_20d > 0:
            rel_vol_20d = _safe(latest_volume / avg_vol_20d, 2)
        if latest_volume and avg_vol_50d and avg_vol_50d > 0:
            rel_vol_50d = _safe(latest_volume / avg_vol_50d, 2)

        # Up/down volume ratio (20D): sum vol on up days / sum vol on down days
        up_down_vol_ratio_20d = None
        if n >= 21:
            vol_window  = volume.iloc[-20:]
            prev_close  = close.shift(1).iloc[-20:]
            cur_close   = close.iloc[-20:]
            is_up       = (cur_close.values > prev_close.values)
            up_vol      = float(vol_window.values[is_up].sum())
            dn_vol      = float(vol_window.values[~is_up].sum())
            if dn_vol > 0:
                up_down_vol_ratio_20d = _safe(up_vol / dn_vol, 3)

        # ── 52-week high/low ──────────────────────────────────────────────────
        high_52w = _safe(high.tail(252).max())
        low_52w  = _safe(low.tail(252).min())
        pct_from_52w_high = None
        pct_from_52w_low  = None
        pos_52w_pct       = None
        if latest_close and high_52w and high_52w != 0:
            pct_from_52w_high = _safe((latest_close - high_52w) / abs(high_52w) * 100)
        if latest_close and low_52w and low_52w != 0:
            pct_from_52w_low = _safe((latest_close - low_52w) / abs(low_52w) * 100)
        if high_52w and low_52w and (high_52w - low_52w) != 0 and latest_close:
            pos_52w_pct = _safe((latest_close - low_52w) / (high_52w - low_52w) * 100)

        # ── Close-based 52W & historical high/low ─────────────────────────────
        close_52w = close.tail(252)
        high_close_52w = _safe(close_52w.max())
        low_close_52w  = _safe(close_52w.min())
        # "3Y" fields are explicitly capped to the trailing 756 trading days
        # (252*3) -- NOT close.max()/close.min() over the whole fetched
        # series. With a 3y fetch window those were equivalent, which is
        # exactly why this was previously written as the unbounded form; now
        # that the fetch window is 10y, close.max() would silently become an
        # ALL-TIME high/low while the column stays labeled "3Y". .tail(756)
        # degrades gracefully to "all available" for tickers with under 3
        # years of history, same as before.
        close_3y = close.tail(756)
        high_close_3y  = _safe(close_3y.max())
        low_close_3y   = _safe(close_3y.min())

        # 26-week (126 trading day) low + how many times price is above it
        # (momentum screen: "3x above 26W low" -> px_over_26w_low >= 3).
        close_26w = close.tail(126)
        low_close_26w = _safe(close_26w.min())
        px_over_26w_low = _safe(latest_close / low_close_26w) \
            if (latest_close and low_close_26w and low_close_26w != 0) else None

        # 20-trading-day close return % (momentum screen: >= +10).
        ret_20d = _safe((latest_close / _safe(close.iloc[-21]) - 1) * 100) \
            if (len(close) >= 21 and latest_close and _safe(close.iloc[-21])) else None

        # Rolling max RSI(14) over the last 60 / 90 sessions (momentum screen:
        # peaked > 80 recently). rsi14 is the full Series computed above.
        max_rsi_60d = _safe(rsi14.tail(60).max()) if len(rsi14.dropna()) else None
        max_rsi_90d = _safe(rsi14.tail(90).max()) if len(rsi14.dropna()) else None

        pct_from_high_close_52w = _pct_from_ma(latest_close, high_close_52w)
        pct_from_low_close_52w  = _pct_from_ma(latest_close, low_close_52w)
        pct_from_high_close_3y  = _pct_from_ma(latest_close, high_close_3y)
        pct_from_low_close_3y   = _pct_from_ma(latest_close, low_close_3y)

        # Days since the 52W high/low close (0 = today is the high/low)
        days_since_52w_high = None
        days_since_52w_low  = None
        if len(close_52w) > 0:
            idx_high = close_52w.index.get_loc(close_52w.idxmax())
            idx_low  = close_52w.index.get_loc(close_52w.idxmin())
            days_since_52w_high = int(len(close_52w) - 1 - idx_high)
            days_since_52w_low  = int(len(close_52w) - 1 - idx_low)

        # Whether today's intraday high/low is the extreme of the window
        h_today = _safe(high.iloc[-1]) or 0.0
        l_today = _safe(low.iloc[-1])  or 0.0
        made_high_5d   = bool(h_today >= float(high.tail(5).max()))   if n >= 5   else None
        made_high_22d  = bool(h_today >= float(high.tail(22).max()))  if n >= 22  else None
        made_high_252d = bool(h_today >= float(high.tail(252).max())) if n >= 252 else None
        made_high_3m   = bool(h_today >= float(high.tail(63).max()))  if n >= 63  else None
        # Capped to trailing 756 trading days, not high.max() over the whole
        # fetch window -- see the close_3y comment above for why.
        made_high_3y   = bool(h_today >= float(high.tail(756).max())) if n >= 1   else None
        made_low_5d    = bool(l_today <= float(low.tail(5).min()))    if n >= 5   else None
        made_low_22d   = bool(l_today <= float(low.tail(22).min()))   if n >= 22  else None
        made_low_252d  = bool(l_today <= float(low.tail(252).min()))  if n >= 252 else None

        # Days since intraday high in each window (0 = today is the high)
        def _days_since_intraday_high(window: pd.Series) -> int | None:
            if window.empty:
                return None
            loc = window.index.get_loc(window.idxmax())
            return int(len(window) - 1 - loc)

        days_since_5d_high  = _days_since_intraday_high(high.tail(5))   if n >= 5   else None
        days_since_22d_high = _days_since_intraday_high(high.tail(22))  if n >= 22  else None
        days_since_3m_high  = _days_since_intraday_high(high.tail(63))  if n >= 63  else None
        # Capped to trailing 756 trading days -- see the close_3y comment above.
        days_since_3y_high  = _days_since_intraday_high(high.tail(756)) if n >= 1   else None

        # Days since the prior-window high, EXCLUDING today (0 = yesterday was the high).
        # Useful for measuring how long a consolidation lasted before a breakout.
        def _days_since_prior_high(series: pd.Series, window: int) -> int | None:
            """Argmax of the `window` bars immediately before today."""
            if len(series) <= window:
                return None
            prior = series.iloc[len(series) - 1 - window : len(series) - 1]
            return int(len(prior) - 1 - prior.values.argmax())

        days_since_prior_high_5d   = _days_since_prior_high(high, 5)
        days_since_prior_high_22d  = _days_since_prior_high(high, 22)
        days_since_prior_high_63d  = _days_since_prior_high(high, 63)
        days_since_prior_high_252d = _days_since_prior_high(high, 252)
        # 3Y: same helper as the other windows now that it's a real bounded
        # window (756 trading days) rather than "whatever we happened to
        # fetch". Previously this was special-cased to use ALL history
        # before today because, at the old 3y fetch window, "all history"
        # and "756 trading days" were roughly the same thing -- with a 10y
        # window they are not. This also changes behavior for tickers with
        # under 756+1 days of history: None (insufficient data), matching
        # every sibling *_prior_high_Nd field's convention, rather than the
        # old silent fallback to whatever partial history existed.
        days_since_prior_high_3y = _days_since_prior_high(high, 756)

        # Close-based pct from high for short windows
        high_close_5d  = _safe(close.tail(5).max())   if n >= 5  else None
        high_close_22d = _safe(close.tail(22).max())  if n >= 22 else None
        high_close_3m  = _safe(close.tail(63).max())  if n >= 63 else None
        pct_from_high_close_5d  = _pct_from_ma(latest_close, high_close_5d)
        pct_from_high_close_22d = _pct_from_ma(latest_close, high_close_22d)
        pct_from_high_close_3m  = _pct_from_ma(latest_close, high_close_3m)

        # ── Donchian channels ─────────────────────────────────────────────────
        def _donchian(window):
            return (
                _safe(high.tail(window).max()),
                _safe(low.tail(window).min()),
            )

        d_high_20, d_low_20   = _donchian(20)
        d_high_55, d_low_55   = _donchian(55)
        d_high_252, d_low_252 = _donchian(252)

        def _pct_from_high(close_val, h_val):
            if close_val and h_val and h_val != 0:
                return _safe((close_val - h_val) / abs(h_val) * 100)
            return None

        pct_from_20d_high  = _pct_from_high(latest_close, d_high_20)
        pct_from_55d_high  = _pct_from_high(latest_close, d_high_55)
        pct_from_252d_high = _pct_from_high(latest_close, d_high_252)

        # Breakouts: close >= donchian high (i.e. making new high)
        breakout_55d_high = bool(latest_close >= d_high_55)  if (latest_close and d_high_55)  else None
        breakout_3m_high  = bool(latest_close >= _safe(high.tail(63).max())) \
                            if latest_close else None

        # ── Realized volatility (annualized) ─────────────────────────────────
        log_ret = np.log(close / close.shift(1)).dropna()
        realized_vol_20d = None
        realized_vol_60d = None
        if len(log_ret) >= 20:
            realized_vol_20d = _safe(log_ret.tail(20).std() * math.sqrt(252) * 100)
        if len(log_ret) >= 60:
            realized_vol_60d = _safe(log_ret.tail(60).std() * math.sqrt(252) * 100)

        # ── ETF-screener extras (benchmark-free; also useful for stocks) ──────
        # Multi-horizon Adj-Close returns (%); ret_20d is computed above.
        def _hret(n: int):
            if len(close) < n + 1 or not latest_close:
                return None
            prev = _safe(close.iloc[-(n + 1)])
            return _safe((latest_close / prev - 1) * 100) if prev else None
        ret_1d   = _hret(1)
        ret_5d   = _hret(5)
        ret_10d  = _hret(10)
        ret_60d  = _hret(60)
        ret_126d = _hret(126)
        ret_252d = _hret(252)

        # Bollinger band width (% of middle band) + its 1y rolling percentile.
        bb_width = None
        bb_width_percentile = None
        _bbw_series = ((bb_upper - bb_lower) / bb_middle * 100).replace(
            [np.inf, -np.inf], np.nan)
        if _bbw_series.notna().any():
            bb_width = _safe(_last(_bbw_series))
            _bbw_win = _bbw_series.dropna().tail(252)
            if len(_bbw_win) >= 20 and bb_width is not None:
                bb_width_percentile = _safe((_bbw_win < bb_width).mean() * 100)

        # RSI(14) change over the last 5 sessions.
        rsi_change_5d = None
        if len(rsi14.dropna()) >= 6:
            _r_now, _r_prev = _last(rsi14), _safe(rsi14.iloc[-6])
            if _r_now is not None and _r_prev is not None:
                rsi_change_5d = _safe(_r_now - _r_prev)

        # Short/long realized-vol ratio (>1 = volatility expanding).
        vol_ratio = _safe(realized_vol_20d / realized_vol_60d) \
            if (realized_vol_20d and realized_vol_60d) else None

        # ── Max drawdown ──────────────────────────────────────────────────────
        def _max_drawdown(series: pd.Series) -> float | None:
            if series.empty:
                return None
            roll_max = series.cummax()
            dd = (series - roll_max) / roll_max.replace(0, np.nan) * 100
            val = dd.min()
            return _safe(val) if not pd.isna(val) else None

        max_dd_63  = _max_drawdown(close.tail(63))
        max_dd_252 = _max_drawdown(close.tail(252))

        # ── Gap stats (60d) ───────────────────────────────────────────────────
        prev_close = close.shift(1)
        gap_pct    = (df["Open"] - prev_close) / prev_close.replace(0, np.nan) * 100
        gaps_60d   = gap_pct.tail(60).dropna()
        gap_rate_60d = _safe(len(gaps_60d[gaps_60d.abs() > 3]) / len(gaps_60d) * 100) \
                       if len(gaps_60d) > 0 else None
        max_gap_60d  = _safe(gaps_60d.abs().max()) if len(gaps_60d) > 0 else None

        # ── Swing high/low (5-bar pivot) ──────────────────────────────────────
        swing_high_val = swing_high_date = swing_low_val = swing_low_date = None
        pct_from_swing_high = pct_from_swing_low = None
        _N = 5   # bars required on each side of a pivot
        if n >= 2 * _N + 1:
            # Scan from most recent valid pivot position backward
            # A pivot high at i: high[i] > max of N bars before AND N bars after
            # Skip the last _N bars (can't confirm right side yet)
            found_h = found_l = False
            for _i in range(n - _N - 1, _N - 1, -1):
                if not found_h:
                    _h = float(high.iloc[_i])
                    if (_h > float(high.iloc[_i - _N: _i].max()) and
                            _h > float(high.iloc[_i + 1: _i + _N + 1].max())):
                        swing_high_val  = _safe(_h)
                        swing_high_date = str(high.index[_i].date())
                        found_h = True
                if not found_l:
                    _l = float(low.iloc[_i])
                    if (_l < float(low.iloc[_i - _N: _i].min()) and
                            _l < float(low.iloc[_i + 1: _i + _N + 1].min())):
                        swing_low_val  = _safe(_l)
                        swing_low_date = str(low.index[_i].date())
                        found_l = True
                if found_h and found_l:
                    break
        if latest_close and swing_high_val and swing_high_val != 0:
            pct_from_swing_high = _safe((latest_close - swing_high_val) / abs(swing_high_val) * 100)
        if latest_close and swing_low_val and swing_low_val != 0:
            pct_from_swing_low = _safe((latest_close - swing_low_val) / abs(swing_low_val) * 100)

        # ── Rolling up/down stats ─────────────────────────────────────────────
        daily_up = (close.diff() > 0)

        def _rolling_stats(window: int) -> tuple:
            s = daily_up.tail(window + 1).iloc[1:]  # skip first (no diff)
            up   = int(s.sum())
            down = int((~s).sum())
            ratio = _safe(up / down) if down > 0 else None
            ms, sp = _rolling_streaks(s)
            return up, down, ratio, ms, sp

        up3m, dn3m, r3m, ms3m, sp3m = _rolling_stats(63)
        up1y, dn1y, r1y, ms1y, sp1y = _rolling_stats(252)

        # ── Consecutive up-close streak ───────────────────────────────────────
        up_streak_days = 0
        up_streak_px_pct = up_streak_vol_pct = None
        up_streak_avg_px_pct = up_streak_avg_vol_pct = None
        if n >= 2:
            _daily_px_chg = close.pct_change() * 100
            _daily_vol_chg = volume.replace(0, np.nan).pct_change() * 100
            # Count consecutive up-close days ending today
            streak = 0
            for _i in range(n - 1, 0, -1):
                if float(close.iloc[_i]) > float(close.iloc[_i - 1]):
                    streak += 1
                else:
                    break
            up_streak_days = streak
            if streak > 0:
                streak_vol   = volume.replace(0, np.nan).iloc[-streak:]
                prior_vol    = volume.replace(0, np.nan).iloc[max(0, n - streak * 2): n - streak]
                # Price: total gain from close before streak to today's close
                pre_streak_close = float(close.iloc[n - streak - 1]) if n > streak else None
                if pre_streak_close and pre_streak_close != 0:
                    up_streak_px_pct = _safe(
                        (float(close.iloc[-1]) / pre_streak_close - 1) * 100
                    )
                if len(prior_vol) > 0 and prior_vol.mean() != 0:
                    up_streak_vol_pct = _safe(
                        (streak_vol.mean() / prior_vol.mean() - 1) * 100
                    )
                _streak_daily_px  = _daily_px_chg.iloc[-streak:].dropna()
                _streak_daily_vol = _daily_vol_chg.iloc[-streak:].dropna()
                if not _streak_daily_px.empty:
                    up_streak_avg_px_pct = _safe(_streak_daily_px.mean())
                if not _streak_daily_vol.empty:
                    up_streak_avg_vol_pct = _safe(_streak_daily_vol.mean())

        # ── Big moves (90d, ≥10%) ─────────────────────────────────────────────
        pct_change = close.pct_change() * 100
        vol_30d_avg = volume.rolling(window=30, min_periods=10).mean().shift(1)
        recent_90 = df.tail(90)
        big_up_events:   list[dict] = []
        big_down_events: list[dict] = []
        for idx, row in recent_90.iterrows():
            pct = _safe(pct_change.get(idx), 2)
            if pct is None or abs(pct) < 10:
                continue
            vol     = _safe_int(row["Volume"])
            avg_vol = _safe_int(vol_30d_avg.get(idx))
            event = {
                "date":          str(idx.date()),
                "pct_change":    pct,
                "close":         _safe(row["Close"], 2),
                "volume":        vol,
                "vol_30d_avg":   avg_vol,
                "vol_above_avg": (vol > avg_vol) if (vol and avg_vol) else None,
            }
            (big_up_events if pct >= 10 else big_down_events).append(event)
        # 5% threshold counts (includes 10%+ days)
        _pct_90 = pct_change.iloc[-90:].dropna()
        big_up_5p_count_90d = int((_pct_90 >= 5).sum())
        big_dn_5p_count_90d = int((_pct_90 <= -5).sum())

        # ── T1/T2 comparison dicts (for indicator compatibility) ──────────────
        weekly = (
            df[["Open", "High", "Low", "Close", "Volume", "RC"]]
            .resample("W-FRI")
            .agg({"Open": "first", "High": "max", "Low": "min",
                  "Close": "last", "Volume": "sum", "RC": "last"})
            .dropna(subset=["Open", "High", "Low", "Close"])
        )
        daily_3m   = _compare_averages(df, 63,  "RC", 5)
        daily_12m  = _compare_averages(df, 252, "RC", 5)
        weekly_cut = weekly.loc[:weekly_latest_date] if weekly_latest_date else weekly
        weekly_3m  = _compare_averages(weekly_cut, 13, "RC", 4)
        weekly_12m = _compare_averages(weekly_cut, 52, "RC", 4)

        # ── Assemble flat dict ────────────────────────────────────────────────
        return {
            "close":               latest_close,
            "volume":              latest_volume,
            "sma10":               v_sma10,
            "sma20":               v_sma20,
            "sma50":               v_sma50,
            "sma100":              v_sma100,
            "sma150":              v_sma150,
            "sma200":              v_sma200,
            "ema9":                v_ema9,
            "ema21":               v_ema21,
            "ema50_e":             v_ema50e,
            "ema200":              v_ema200,
            "ma10_gt_ma20":        ma10_gt_ma20,
            "ma20_gt_ma50":        ma20_gt_ma50,
            "ma50_gt_ma100":       ma50_gt_ma100,
            "ma100_gt_ma150":      ma100_gt_ma150,
            "ma50_gt_ma150":       ma50_gt_ma150,
            "ma150_gt_ma200":      ma150_gt_ma200,
            "sma50_slope_20d":     sma50_slope_20d,
            "pct_from_sma200":     pct_from_sma200,
            "rsi14":               _safe(_last(rsi14)),
            "max_rsi_60d":         max_rsi_60d,
            "max_rsi_90d":         max_rsi_90d,
            "macd_line":           _safe(_last(macd_line)),
            "macd_signal":         _safe(_last(macd_signal)),
            "macd_hist":           _safe(_last(macd_hist)),
            "bb_upper":            _safe(_last(bb_upper)),
            "bb_middle":           _safe(_last(bb_middle)),
            "bb_lower":            _safe(_last(bb_lower)),
            "bb_pct_b":            _safe(_last(bb_pct_b), 4),
            "atr14":               v_atr14,
            "atr_pct":             atr_pct,
            "adx14":               _safe(_last(adx14)),
            "plus_di":             _safe(_last(plus_di)),
            "minus_di":            _safe(_last(minus_di)),
            "stoch_k":             _safe(_last(stoch_k)),
            "stoch_d":             _safe(_last(stoch_d)),
            "obv":                 _safe(_last(obv), 0),
            "cmf20":               _safe(_last(cmf20), 4),
            "ad_line":             _safe(_last(ad_line), 0),
            "avg_dollar_vol_20d":  avg_dv20,
            "avg_dollar_vol_50d":  avg_dv50,
            "median_volume_50d":   med_vol50,
            "med_vol_ratio_60d":   med_vol_ratio_60d,
            "med_vol_ratio_90d":   med_vol_ratio_90d,
            "high_52w":            high_52w,
            "low_52w":             low_52w,
            "pct_from_52w_high":   pct_from_52w_high,
            "pct_from_52w_low":    pct_from_52w_low,
            "pos_52w_pct":         pos_52w_pct,
            "high_close_52w":          high_close_52w,
            "low_close_52w":           low_close_52w,
            "pct_from_high_close_52w": pct_from_high_close_52w,
            "pct_from_low_close_52w":  pct_from_low_close_52w,
            "low_close_26w":           low_close_26w,
            "px_over_26w_low":         px_over_26w_low,
            "ret_20d":                 ret_20d,
            "ret_1d":                  ret_1d,
            "ret_5d":                  ret_5d,
            "ret_10d":                 ret_10d,
            "ret_60d":                 ret_60d,
            "ret_126d":                ret_126d,
            "ret_252d":                ret_252d,
            "bb_width":                bb_width,
            "bb_width_percentile":     bb_width_percentile,
            "rsi_change_5d":           rsi_change_5d,
            "vol_ratio":               vol_ratio,
            "high_close_3y":           high_close_3y,
            "low_close_3y":            low_close_3y,
            "pct_from_high_close_3y":  pct_from_high_close_3y,
            "pct_from_low_close_3y":   pct_from_low_close_3y,
            "days_since_52w_high":     days_since_52w_high,
            "days_since_52w_low":      days_since_52w_low,
            "made_high_5d":            made_high_5d,
            "made_high_22d":           made_high_22d,
            "made_high_252d":          made_high_252d,
            "made_high_3m":            made_high_3m,
            "made_high_3y":            made_high_3y,
            "made_low_5d":             made_low_5d,
            "made_low_22d":            made_low_22d,
            "made_low_252d":           made_low_252d,
            "days_since_5d_high":      days_since_5d_high,
            "days_since_22d_high":     days_since_22d_high,
            "days_since_3m_high":      days_since_3m_high,
            "days_since_3y_high":      days_since_3y_high,
            "days_since_prior_high_5d":   days_since_prior_high_5d,
            "days_since_prior_high_22d":  days_since_prior_high_22d,
            "days_since_prior_high_63d":  days_since_prior_high_63d,
            "days_since_prior_high_252d": days_since_prior_high_252d,
            "days_since_prior_high_3y":   days_since_prior_high_3y,
            "pct_from_high_close_5d":  pct_from_high_close_5d,
            "pct_from_high_close_22d": pct_from_high_close_22d,
            "pct_from_high_close_3m":  pct_from_high_close_3m,
            "up_streak_days":          up_streak_days,
            "up_streak_px_pct":        up_streak_px_pct,
            "up_streak_vol_pct":       up_streak_vol_pct,
            "up_streak_avg_px_pct":    up_streak_avg_px_pct,
            "up_streak_avg_vol_pct":   up_streak_avg_vol_pct,
            "donchian_high_20":    d_high_20,
            "donchian_low_20":     d_low_20,
            "donchian_high_55":    d_high_55,
            "donchian_low_55":     d_low_55,
            "donchian_high_252":   d_high_252,
            "donchian_low_252":    d_low_252,
            "pct_from_20d_high":   pct_from_20d_high,
            "pct_from_55d_high":   pct_from_55d_high,
            "pct_from_252d_high":  pct_from_252d_high,
            "breakout_55d_high":   breakout_55d_high,
            "breakout_3m_high":    breakout_3m_high,
            "realized_vol_20d":    realized_vol_20d,
            "realized_vol_60d":    realized_vol_60d,
            "max_drawdown_63d":    max_dd_63,
            "max_drawdown_252d":   max_dd_252,
            "gap_rate_60d":        gap_rate_60d,
            "max_gap_60d":         max_gap_60d,
            "up_days_3m":          up3m,
            "down_days_3m":        dn3m,
            "up_down_ratio_3m":    r3m,
            "max_win_streak_3m":   ms3m,
            "win_streaks_5p_3m":   sp3m,
            "up_days_1y":          up1y,
            "down_days_1y":        dn1y,
            "up_down_ratio_1y":    r1y,
            "max_win_streak_1y":   ms1y,
            "win_streaks_5p_1y":   sp1y,
            "daily_pct_change":    _safe(float(pct_change.iloc[-1]), 2) if len(pct_change) >= 2 else None,
            "daily_vol_pct":       daily_vol_pct,
            # Price vs MA distances (%)
            "pct_from_sma10":      pct_from_sma10,
            "pct_from_sma20":      pct_from_sma20,
            "pct_from_sma50":      pct_from_sma50,
            "pct_from_sma100":     pct_from_sma100,
            "pct_from_sma150":     pct_from_sma150,
            "pct_from_ema9":       pct_from_ema9,
            "pct_from_ema21":      pct_from_ema21,
            "pct_from_ema50":      pct_from_ema50,
            "pct_from_ema200":     pct_from_ema200,
            # Additional MA slopes
            "sma10_slope_10d":     sma10_slope_10d,
            "sma20_slope_10d":     sma20_slope_10d,
            "sma150_slope_20d":    sma150_slope_20d,
            "sma200_slope_20d":    sma200_slope_20d,
            # MA slope alignment booleans
            "slope10_gt_slope20":   slope10_gt_slope20,
            "slope20_gt_slope50":   slope20_gt_slope50,
            "slope50_gt_slope150":  slope50_gt_slope150,
            "slope150_gt_slope200": slope150_gt_slope200,
            # Relative volume & up/down vol ratio
            "rel_vol_20d":         rel_vol_20d,
            "rel_vol_50d":         rel_vol_50d,
            "up_down_vol_ratio_20d": up_down_vol_ratio_20d,
            # Swing high/low
            "swing_high":          swing_high_val,
            "swing_high_date":     swing_high_date,
            "swing_low":           swing_low_val,
            "swing_low_date":      swing_low_date,
            "pct_from_swing_high": pct_from_swing_high,
            "pct_from_swing_low":  pct_from_swing_low,
            "big_up_events_90d":   json.dumps(big_up_events, default=str),
            "big_down_events_90d": json.dumps(big_down_events, default=str),
            "big_up_5p_count_90d": big_up_5p_count_90d,
            "big_dn_5p_count_90d": big_dn_5p_count_90d,
            "daily_vs_3m":         json.dumps(daily_3m,   default=str),
            "daily_vs_12m":        json.dumps(daily_12m,  default=str),
            "weekly_vs_3m":        json.dumps(weekly_3m,  default=str),
            "weekly_vs_12m":       json.dumps(weekly_12m, default=str),
            "_as_of_date":         latest_date,
        }

    except Exception as e:
        return {"error": f"Technical compute failed for {ticker}: {e}"}


def compute_benchmark_rs(etf_close: "pd.Series", spy_close: "pd.Series") -> dict:
    """Relative strength / beta / correlation of a series vs a benchmark (SPY).

    etf_close, spy_close: Adj-Close (or return-adjusted close) series indexed by
    date. They are inner-joined on date so mismatched histories align.

    Returns rs_spy_20d/60d/126d -- the cumulative-return ratio
    (1 + etf_ret) / (1 + spy_ret) over the window, where 1.0 means "in line with
    SPY" and > 1 means out-performance -- plus beta_spy_60d and corr_spy_60d
    from a 60-session daily-return regression. A window without enough
    overlapping data yields None for that field.
    """
    out = {"rs_spy_20d": None, "rs_spy_60d": None, "rs_spy_126d": None,
           "beta_spy_60d": None, "corr_spy_60d": None}
    if etf_close is None or spy_close is None:
        return out
    df = pd.concat([etf_close.rename("e"), spy_close.rename("s")], axis=1).dropna()
    if len(df) < 2:
        return out
    e, s = df["e"], df["s"]

    def _rs(n: int):
        if len(df) < n + 1:
            return None
        er = e.iloc[-1] / e.iloc[-(n + 1)] - 1
        sr = s.iloc[-1] / s.iloc[-(n + 1)] - 1
        denom = 1 + sr
        return _safe((1 + er) / denom, 4) if denom else None

    out["rs_spy_20d"]  = _rs(20)
    out["rs_spy_60d"]  = _rs(60)
    out["rs_spy_126d"] = _rs(126)

    j = pd.concat([e.pct_change().rename("e"), s.pct_change().rename("s")],
                  axis=1).dropna().tail(60)
    if len(j) >= 30:
        var = j["s"].var()
        out["beta_spy_60d"] = _safe(j["e"].cov(j["s"]) / var, 3) if var else None
        out["corr_spy_60d"] = _safe(j["e"].corr(j["s"]), 3)
    return out


# ── Also expose a dict compatible with the old data_fetcher.py format ─────────

def tech_dict_to_legacy(fields: dict) -> dict:
    """
    Convert flat tech_indicators fields dict back to the nested format
    expected by indicators.py (ma_checks, ma_values, daily_vs_3m, etc.)
    so the existing indicator evaluators still work.
    """
    return {
        "ticker":  fields.get("ticker"),
        "date":    fields.get("_as_of_date") or fields.get("as_of_date"),
        "close":   fields.get("close"),
        "volume":  fields.get("volume"),
        "ma_values": {
            "MA10":  fields.get("sma10"),
            "MA20":  fields.get("sma20"),
            "MA50":  fields.get("sma50"),
            "MA150": fields.get("sma150"),
            "MA200": fields.get("sma200"),
        },
        "ma_checks": {
            "MA10>MA20":      fields.get("ma10_gt_ma20"),
            "MA20>MA50":      fields.get("ma20_gt_ma50"),
            "MA50>MA150":     fields.get("ma50_gt_ma150"),
            "MA150>MA200":    fields.get("ma150_gt_ma200"),
            "full_alignment": (
                all([fields.get("ma10_gt_ma20"), fields.get("ma20_gt_ma50"),
                     fields.get("ma50_gt_ma150"), fields.get("ma150_gt_ma200")])
                if all(fields.get(k) is not None
                       for k in ["ma10_gt_ma20","ma20_gt_ma50","ma50_gt_ma150","ma150_gt_ma200"])
                else None
            ),
        },
        "daily_vs_3m":   _load_json(fields.get("daily_vs_3m")),
        "daily_vs_12m":  _load_json(fields.get("daily_vs_12m")),
        "weekly_vs_3m":  _load_json(fields.get("weekly_vs_3m")),
        "weekly_vs_12m": _load_json(fields.get("weekly_vs_12m")),
        "big_up_events":   _load_json(fields.get("big_up_events_90d"), default=[]),
        "big_down_events": _load_json(fields.get("big_down_events_90d"), default=[]),
    }


def _load_json(v, default=None):
    if v is None:
        return default
    if isinstance(v, (dict, list)):
        return v
    try:
        return json.loads(v)
    except Exception:
        return default


# ── Bulk fetch + store ────────────────────────────────────────────────────────

def fetch_and_store_bulk(tickers: list[str],
                         weekly_latest_date: str | None = None,
                         log=print) -> dict[str, dict]:
    """
    Compute all indicators for `tickers` in batches of 100 and store in
    tech_indicators table.

    Two-tier download per batch: tickers with confirmed full-depth history
    (ticker_history_depth) only need a small recent window downloaded
    (_CHECK_WINDOW_DAYS) -- the older portion of their indicator-compute
    DataFrame is read back from price_history instead of re-downloaded.
    Everything else (new tickers, not-yet-depth-confirmed tickers, and
    tickers where the small window's revision check fails or is
    inconclusive) gets the full 10y download, same as every ticker used to
    get on every single scan. See _price_history_needs_full_reupload for
    the revision-check mechanics and the ticker_history_depth DDL comment
    in storage.py for why depth-confirmation uses a completion flag rather
    than an absolute calendar-age threshold.

    Returns {ticker: legacy_tech_dict | {"error": ...}}.
    The legacy_tech_dict format is what indicators.py / evaluate_all() expects
    (ma_checks, daily_vs_3m, big_up_events, etc.).

    weekly_latest_date: if set, limits the weekly comparison window (T2) to
    that date. Close / as_of_date always reflect the latest available data.
    """
    from datetime import timedelta
    is_final_session = nyse_close_passed_today()
    results: dict[str, dict] = {}
    today_et  = et_today()
    today_str = today_et.isoformat()
    # Expected most-recent completed trading day in ET: today if NYSE closed, else prior day
    expected_date = today_str if is_final_session else (
        get_last_trading_day_before_today() or today_str
    )
    # Explicit date range so yfinance always tries to include today's close.
    # 10y+10d: the +10 buffer just guards against weekday/holiday edge cases
    # at the boundary, same as the previous 3y+10 convention. NOTE: the
    # "3Y"-labeled indicator fields below (high_close_3y etc.) are
    # deliberately capped to an explicit trailing-756-trading-day slice
    # rather than "whatever this fetch window is" -- see the comments at
    # each of those fields. If this window is ever extended further, those
    # fields do NOT need to change; only truly *unbounded* computations
    # would need re-auditing.
    _start = (today_et - timedelta(days=10 * 365 + 10)).isoformat()
    _end   = (today_et + timedelta(days=1)).isoformat()
    _check_start = (today_et - timedelta(days=_CHECK_WINDOW_DAYS)).isoformat()

    for i in range(0, len(tickers), _BATCH_SIZE):
        batch = tickers[i: i + _BATCH_SIZE]
        batch_no = i // _BATCH_SIZE + 1

        # Per-ticker last-stored price_history date + full-depth confirmation,
        # both via one batched query each. A ticker only qualifies for the
        # small check-window download once it's both been stored before AND
        # already had a full-window download complete at some point.
        _max_dates = storage.get_price_history_max_dates(batch)
        _depth = storage.get_ticker_history_depth(batch)

        needs_full: list[str] = []
        check_candidates: list[str] = []
        for ticker in batch:
            if ticker in _max_dates and ticker in _depth:
                check_candidates.append(ticker)
            else:
                needs_full.append(ticker)

        fast_path_dfs: dict[str, pd.DataFrame] = {}

        # ── Tier 1: small check-window download + revision check ──────────
        if check_candidates:
            _t0 = time.time()
            log(f"  [tech] CHECK yf.download START  batch={batch_no}  tickers={len(check_candidates)}"
                f"  thread={threading.current_thread().name}  {_diag_res()}")
            try:
                with YF_DL_LOCK:  # see yf_session.py -- concurrent downloads cross-contaminate
                    check_raw = yf.download(
                        tickers=check_candidates,
                        start=_check_start,
                        end=_end,
                        group_by="ticker",
                        auto_adjust=False,
                        threads=False,
                        progress=False,
                        session=YF_SESSION,
                    )
                log(f"  [tech] CHECK yf.download DONE   batch={batch_no}  elapsed={time.time()-_t0:.1f}s"
                    f"  shape={check_raw.shape}  {_diag_res()}")
            except Exception as e:
                log(f"  [tech] CHECK yf.download ERROR  batch={batch_no}  elapsed={time.time()-_t0:.1f}s"
                    f"  err={type(e).__name__}: {e}  {_diag_res()}")
                # Check download failed outright -- fall back to full for all of them.
                needs_full.extend(check_candidates)
                check_candidates = []

            if check_candidates:
                is_check_multi = isinstance(check_raw.columns, pd.MultiIndex)
                _stored_closes = storage.get_price_history_close_at(
                    [(t, _max_dates[t]) for t in check_candidates]
                )
                for ticker in check_candidates:
                    try:
                        if is_check_multi:
                            if ticker not in check_raw.columns.get_level_values(0):
                                needs_full.append(ticker)
                                continue
                            small_df = check_raw[ticker].copy()
                        else:
                            small_df = check_raw.copy()

                        if small_df.empty or small_df["Close"].isna().all():
                            needs_full.append(ticker)
                            continue

                        _last_stored = _max_dates[ticker]
                        if _price_history_needs_full_reupload(
                            _last_stored, _stored_closes.get((ticker, _last_stored)), small_df
                        ):
                            needs_full.append(ticker)
                            continue

                        fast_path_dfs[ticker] = small_df
                    except Exception:
                        needs_full.append(ticker)

        # ── Fast path: assemble DB history + fresh tail, compute, write tail ──
        if fast_path_dfs:
            # No min_date: read everything stored so unbounded cumulative
            # indicators (OBV, A/D line) match what a full download would
            # have produced (see _CHECK_WINDOW_DAYS comment above).
            _older = storage.get_price_history_before(
                list(fast_path_dfs.keys()), _check_start
            )
            for ticker, small_df in fast_path_dfs.items():
                try:
                    assembled = _assemble_price_df(_older.get(ticker, []), small_df)

                    # Safety net against storing a partial row: a depth-confirmed
                    # ticker with a genuinely old history anchor should assemble
                    # deep. If it's shallow, the older-DB read came back short
                    # (transient) -- re-route to a full download rather than
                    # compute NULL long-window MAs. Young tickers (recent anchor)
                    # are let through; their shallow history is real.
                    _anchor = _depth.get(ticker)
                    _deep_cutoff = (today_et - timedelta(days=_DEEP_HISTORY_MIN_DAYS)).isoformat()
                    if (len(assembled) < _MIN_ASSEMBLED_ROWS
                            and _anchor is not None and _anchor < _deep_cutoff):
                        needs_full.append(ticker)
                        continue

                    fields = _compute_all_indicators(ticker, assembled, weekly_latest_date)
                    if "error" in fields:
                        results[ticker] = {"error": fields["error"]}
                        continue

                    as_of = fields.pop("_as_of_date", today_str)
                    row_is_final = is_final_session and (as_of >= expected_date)
                    storage.save_tech_indicators(ticker, as_of, fields, row_is_final)

                    # Revision check already confirmed the ticker's older
                    # history is unchanged -- only the fresh tail needs writing.
                    _since = (pd.Timestamp(_max_dates[ticker]) - pd.Timedelta(days=10)).date().isoformat()
                    storage.save_price_history(ticker, _extract_price_rows(small_df, since=_since))

                    fields["_as_of_date"] = as_of
                    results[ticker] = tech_dict_to_legacy(fields)
                except Exception as e:
                    results[ticker] = {"error": f"Technical compute failed for {ticker}: {e}"}

        # ── Tier 2: full 10y download for new/unconfirmed/revised tickers ──
        if needs_full:
            _t0 = time.time()
            log(f"  [tech] FULL yf.download START  batch={batch_no}  tickers={len(needs_full)}"
                f"  thread={threading.current_thread().name}  {_diag_res()}")
            try:
                with YF_DL_LOCK:
                    raw = yf.download(
                        tickers=needs_full,
                        start=_start,
                        end=_end,
                        group_by="ticker",
                        auto_adjust=False,
                        threads=False,
                        progress=False,
                        session=YF_SESSION,
                    )
                log(f"  [tech] FULL yf.download DONE   batch={batch_no}  elapsed={time.time()-_t0:.1f}s"
                    f"  shape={raw.shape}  {_diag_res()}")
            except Exception as e:
                log(f"  [tech] FULL yf.download ERROR  batch={batch_no}  elapsed={time.time()-_t0:.1f}s"
                    f"  err={type(e).__name__}: {e}  {_diag_res()}")
                for t in needs_full:
                    results[t] = {"error": f"Batch download failed: {e}"}
                continue

            is_multi = isinstance(raw.columns, pd.MultiIndex)
            _depth_updates: dict[str, str] = {}

            for ticker in needs_full:
                try:
                    if is_multi:
                        if ticker not in raw.columns.get_level_values(0):
                            results[ticker] = {"error": f"No bulk data for {ticker}"}
                            continue
                        df = raw[ticker].copy()
                    else:
                        df = raw.copy()

                    if df.empty or df["Close"].isna().all():
                        results[ticker] = {"error": f"No price data for {ticker}"}
                        continue

                    fields = _compute_all_indicators(ticker, df, weekly_latest_date)
                    if "error" in fields:
                        results[ticker] = {"error": fields["error"]}
                        continue

                    as_of = fields.pop("_as_of_date", today_str)
                    # Only finalize if yfinance actually returned the expected trading day's data.
                    # If it returned stale data, keep is_finalized=FALSE so refetch_unfinalized
                    # will pick it up on the next session.
                    row_is_final = is_final_session and (as_of >= expected_date)
                    storage.save_tech_indicators(ticker, as_of, fields, row_is_final)

                    # Always write everything returned, not just the tail: the
                    # whole point of a full download here is to (re)establish
                    # full historical depth (new ticker, deeper-than-before
                    # backfill, or a detected revision) -- a tail-only write
                    # would silently defeat that.
                    storage.save_price_history(ticker, _extract_price_rows(df, since=None))

                    _valid_dates = df.dropna(subset=["Close"]).index
                    if len(_valid_dates):
                        _depth_updates[ticker] = _valid_dates.min().date().isoformat()

                    # Convert flat fields dict → legacy nested dict for indicators.py
                    fields["_as_of_date"] = as_of  # restore for conversion
                    results[ticker] = tech_dict_to_legacy(fields)

                except Exception as e:
                    results[ticker] = {"error": f"Technical compute failed for {ticker}: {e}"}

            storage.save_ticker_history_depth(_depth_updates)

    return results


def compute_indicators_history(ticker: str, start_date: str | None,
                                end_date: str | None, progress=None) -> list[dict]:
    """Compute the full indicator set AS OF each trading day in [start_date,
    end_date] for one ticker, from stored price_history -- the on-demand
    historical fill for the export (tech_indicators itself only holds rows
    from when daily scanning began).

    For a target day D the indicators are computed on the OHLCV window from the
    ticker's earliest stored bar THROUGH D (so long-window MAs and the
    cumulative OBV / A-D line are correct as of D, not just a fixed lookback).
    O(days-in-range) compute calls; use `progress(done, total)` for a bar.

    Returns [{"ticker", "as_of_date", <indicator fields...>}, ...] oldest first;
    days whose compute errors (e.g. too little data) are skipped.
    """
    ohlcv = storage.get_price_history_ohlcv_range(ticker, None, end_date)
    if not ohlcv:
        return []
    df = pd.DataFrame(ohlcv).rename(columns={
        "open": "Open", "high": "High", "low": "Low",
        "close": "Close", "adj_close": "Adj Close", "volume": "Volume",
    })
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date").sort_index()
    dates = [d.date().isoformat() for d in df.index]
    lo = start_date or dates[0]
    hi = end_date or dates[-1]
    idxs = [i for i, d in enumerate(dates) if lo <= d <= hi]
    total = len(idxs)
    out: list[dict] = []
    for n_done, i in enumerate(idxs):
        fields = _compute_all_indicators(ticker, df.iloc[: i + 1])
        if "error" not in fields:
            fields.pop("_as_of_date", None)
            out.append({"ticker": ticker, "as_of_date": dates[i], **fields})
        if progress and (n_done % 25 == 0 or n_done == total - 1):
            progress(n_done + 1, total)
    return out


def refetch_unfinalized(log=print) -> int:
    """
    Re-fetch and update all (ticker, as_of_date) rows where is_finalized=FALSE.
    Only re-fetches if NYSE has since closed for those dates.
    Returns number of tickers updated.
    """
    pending = storage.get_unfinalized_tickers()
    if not pending:
        return 0

    today = et_today().isoformat()
    # Only re-fetch entries whose as_of_date < today ET (yesterday's data is always final)
    # or entries that are today ET but NYSE has now closed
    to_refetch = []
    for ticker, as_of_date in pending:
        if as_of_date < today:
            to_refetch.append(ticker)
        elif nyse_close_passed_today():
            to_refetch.append(ticker)

    if not to_refetch:
        return 0

    log(f"  [tech] Re-fetching {len(to_refetch)} unfinalized tickers…")
    _refetch_single_batch(to_refetch, log)
    return len(to_refetch)


def refetch_stale_tickers(log=print) -> int:
    """
    Re-fetch tickers whose stored as_of_date is behind the most recent completed
    trading day.  Handles the case where yfinance returned stale data during a
    scan (stored with is_finalized=TRUE but as_of_date is from a prior day).
    Uses a single SQL query for efficiency.

    Target date logic:
      - NYSE has closed today  → target = today   (today is now a completed day)
      - NYSE not yet closed    → target = last trading day before today
    Tickers with as_of_date < target are re-fetched.

    Returns number of tickers re-fetched.
    """
    if nyse_close_passed_today():
        target_date = et_today().isoformat()
    else:
        target_date = get_last_trading_day_before_today()
    if not target_date:
        return 0
    stale = storage.get_tickers_with_stale_tech(target_date)
    if not stale:
        return 0
    log(f"  [tech] Re-fetching {len(stale)} tickers with stale as_of_date "
        f"(behind {target_date})…")
    _refetch_single_batch(stale, log)
    return len(stale)


def backfill_missing_days(tickers: list[str], log=print) -> int:
    """
    For each ticker, check for missing NYSE trading days since last stored date.
    Re-fetches tickers that have gaps.
    Returns count of tickers re-fetched.
    """
    to_refetch = []
    for ticker in tickers:
        last_date = storage.get_latest_tech_date(ticker)
        missing = get_missing_trading_days(last_date)
        if missing:
            to_refetch.append(ticker)

    if not to_refetch:
        return 0

    log(f"  [tech] Backfilling {len(to_refetch)} tickers with missing trading days…")
    _refetch_single_batch(to_refetch, log)
    return len(to_refetch)


def _refetch_single_batch(tickers: list[str], log=print) -> None:
    """Re-download and re-compute for a list of tickers (used internally)."""
    from datetime import timedelta
    is_final_session = nyse_close_passed_today()
    today_et  = et_today()
    today_str = today_et.isoformat()
    expected_date = today_str if is_final_session else (
        get_last_trading_day_before_today() or today_str
    )
    # Must match fetch_and_store_bulk's window -- this function re-fetches
    # individual tickers (unfinalized/stale/missing-days) and writes to the
    # SAME price_history/tech_indicators rows; a shorter window here would
    # silently truncate a ticker's history back down on its next refetch.
    _start = (today_et - timedelta(days=10 * 365 + 10)).isoformat()
    _end   = (today_et + timedelta(days=1)).isoformat()

    for i in range(0, len(tickers), _BATCH_SIZE):
        batch = tickers[i: i + _BATCH_SIZE]
        try:
            with YF_DL_LOCK:  # see yf_session.py -- concurrent downloads cross-contaminate
                raw = yf.download(
                    tickers=batch,
                    start=_start,
                    end=_end,
                    group_by="ticker",
                    auto_adjust=False,
                    threads=False,
                    progress=False,
                    session=YF_SESSION,
                )
        except Exception as e:
            log(f"  [tech] Re-fetch batch failed: {e}")
            continue

        is_multi = isinstance(raw.columns, pd.MultiIndex)
        _max_dates = storage.get_price_history_max_dates(batch)
        _stored_closes = storage.get_price_history_close_at(list(_max_dates.items()))
        _depth_updates: dict[str, str] = {}
        for ticker in batch:
            try:
                if is_multi:
                    if ticker not in raw.columns.get_level_values(0):
                        continue
                    df = raw[ticker].copy()
                else:
                    df = raw.copy()

                if df.empty or df["Close"].isna().all():
                    continue

                fields = _compute_all_indicators(ticker, df)
                if "error" in fields:
                    continue
                as_of = fields.pop("_as_of_date", today_str)
                row_is_final = is_final_session and (as_of >= expected_date)
                storage.save_tech_indicators(ticker, as_of, fields, row_is_final)

                _last_stored = _max_dates.get(ticker)
                _since = None
                if not _price_history_needs_full_reupload(
                    _last_stored, _stored_closes.get((ticker, _last_stored)), df
                ):
                    _since = (pd.Timestamp(_last_stored) - pd.Timedelta(days=10)).date().isoformat()
                storage.save_price_history(ticker, _extract_price_rows(df, since=_since))
                storage.mark_tech_finalized(ticker, as_of)

                # Only record depth-confirmation when the FULL returned range
                # was actually written (_since is None) -- a tail-only write
                # doesn't establish full depth for a ticker that didn't
                # already have it, and this always downloads the full 10y
                # window regardless of which write path was taken.
                if _since is None:
                    _valid_dates = df.dropna(subset=["Close"]).index
                    if len(_valid_dates):
                        _depth_updates[ticker] = _valid_dates.min().date().isoformat()
            except Exception as e:
                log(f"  [tech] Re-fetch compute error for {ticker}: {e}")

        storage.save_ticker_history_depth(_depth_updates)
