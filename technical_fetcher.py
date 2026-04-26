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
import time
from datetime import date, datetime
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import ta
import yfinance as yf

from market_calendar import (
    get_missing_trading_days, nyse_close_passed_today,
    get_last_trading_day_before_today, et_today,
)
import storage

ET  = ZoneInfo("America/New_York")
CST = ZoneInfo("America/Chicago")

_BATCH_SIZE = 100   # yf.download tickers per call


# ── Helpers ───────────────────────────────────────────────────────────────────

def _extract_price_rows(df: pd.DataFrame) -> list[tuple]:
    """Extract (date_str, close, volume) tuples from an OHLCV DataFrame."""
    sub = df[["Close", "Volume"]].dropna(subset=["Close"])
    rows = []
    for idx, row in sub.iterrows():
        d = idx.date().isoformat() if hasattr(idx, "date") else str(idx)[:10]
        c = _safe(row["Close"], 4)
        v = _safe_int(row["Volume"])
        if c is not None:
            rows.append((d, c, v))
    return rows

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

        # % distance from each MA
        def _pct_from_ma(close_val, ma_val) -> float | None:
            if close_val and ma_val and ma_val != 0:
                return _safe((close_val - ma_val) / abs(ma_val) * 100)
            return None

        pct_from_sma10  = _pct_from_ma(latest_close, v_sma10)
        pct_from_sma20  = _pct_from_ma(latest_close, v_sma20)
        pct_from_sma50  = _pct_from_ma(latest_close, v_sma50)
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
        high_close_3y  = _safe(close.max())
        low_close_3y   = _safe(close.min())

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

        # Whether today's close is the high/low of the window
        c_today = latest_close or 0.0
        made_high_5d   = bool(c_today >= float(close.tail(5).max()))   if n >= 5   else None
        made_high_22d  = bool(c_today >= float(close.tail(22).max()))  if n >= 22  else None
        made_high_252d = bool(c_today >= float(close.tail(252).max())) if n >= 252 else None
        made_low_5d    = bool(c_today <= float(close.tail(5).min()))   if n >= 5   else None
        made_low_22d   = bool(c_today <= float(close.tail(22).min()))  if n >= 22  else None
        made_low_252d  = bool(c_today <= float(close.tail(252).min())) if n >= 252 else None

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
            "sma150":              v_sma150,
            "sma200":              v_sma200,
            "ema9":                v_ema9,
            "ema21":               v_ema21,
            "ema50_e":             v_ema50e,
            "ema200":              v_ema200,
            "ma10_gt_ma20":        ma10_gt_ma20,
            "ma20_gt_ma50":        ma20_gt_ma50,
            "ma50_gt_ma150":       ma50_gt_ma150,
            "ma150_gt_ma200":      ma150_gt_ma200,
            "sma50_slope_20d":     sma50_slope_20d,
            "pct_from_sma200":     pct_from_sma200,
            "rsi14":               _safe(_last(rsi14)),
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
            "high_52w":            high_52w,
            "low_52w":             low_52w,
            "pct_from_52w_high":   pct_from_52w_high,
            "pct_from_52w_low":    pct_from_52w_low,
            "pos_52w_pct":         pos_52w_pct,
            "high_close_52w":          high_close_52w,
            "low_close_52w":           low_close_52w,
            "pct_from_high_close_52w": pct_from_high_close_52w,
            "pct_from_low_close_52w":  pct_from_low_close_52w,
            "high_close_3y":           high_close_3y,
            "low_close_3y":            low_close_3y,
            "pct_from_high_close_3y":  pct_from_high_close_3y,
            "pct_from_low_close_3y":   pct_from_low_close_3y,
            "days_since_52w_high":     days_since_52w_high,
            "days_since_52w_low":      days_since_52w_low,
            "made_high_5d":            made_high_5d,
            "made_high_22d":           made_high_22d,
            "made_high_252d":          made_high_252d,
            "made_low_5d":             made_low_5d,
            "made_low_22d":            made_low_22d,
            "made_low_252d":           made_low_252d,
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
            "daily_vs_3m":         json.dumps(daily_3m,   default=str),
            "daily_vs_12m":        json.dumps(daily_12m,  default=str),
            "weekly_vs_3m":        json.dumps(weekly_3m,  default=str),
            "weekly_vs_12m":       json.dumps(weekly_12m, default=str),
            "_as_of_date":         latest_date,
        }

    except Exception as e:
        return {"error": f"Technical compute failed for {ticker}: {e}"}


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
    Download 3y OHLCV for `tickers` in batches of 100, compute all indicators,
    store in tech_indicators table.

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
    # Explicit date range so yfinance always tries to include today's close
    _start = (today_et - timedelta(days=3 * 365 + 10)).isoformat()
    _end   = (today_et + timedelta(days=1)).isoformat()

    for i in range(0, len(tickers), _BATCH_SIZE):
        batch = tickers[i: i + _BATCH_SIZE]
        log(f"  [tech] Downloading batch {i//100 + 1} ({len(batch)} tickers)…")
        try:
            raw = yf.download(
                tickers=batch,
                start=_start,
                end=_end,
                group_by="ticker",
                auto_adjust=False,
                threads=True,
                progress=False,
            )
        except Exception as e:
            log(f"  [tech] Batch download failed: {e}")
            for t in batch:
                results[t] = {"error": f"Batch download failed: {e}"}
            continue

        is_multi = isinstance(raw.columns, pd.MultiIndex)

        for ticker in batch:
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
                storage.save_price_history(ticker, _extract_price_rows(df))

                # Convert flat fields dict → legacy nested dict for indicators.py
                fields["_as_of_date"] = as_of  # restore for conversion
                results[ticker] = tech_dict_to_legacy(fields)

            except Exception as e:
                results[ticker] = {"error": f"Technical compute failed for {ticker}: {e}"}

    return results


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
    _start = (today_et - timedelta(days=3 * 365 + 10)).isoformat()
    _end   = (today_et + timedelta(days=1)).isoformat()

    for i in range(0, len(tickers), _BATCH_SIZE):
        batch = tickers[i: i + _BATCH_SIZE]
        try:
            raw = yf.download(
                tickers=batch,
                start=_start,
                end=_end,
                group_by="ticker",
                auto_adjust=False,
                threads=True,
                progress=False,
            )
        except Exception as e:
            log(f"  [tech] Re-fetch batch failed: {e}")
            continue

        is_multi = isinstance(raw.columns, pd.MultiIndex)
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
                storage.save_price_history(ticker, _extract_price_rows(df))
                storage.mark_tech_finalized(ticker, as_of)
            except Exception as e:
                log(f"  [tech] Re-fetch compute error for {ticker}: {e}")
