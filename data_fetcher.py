"""
data_fetcher.py — technical + fundamental data via yfinance.

Changes from previous version:
- _compare_averages now returns raw price lists, date ranges, pct-change (not ratio)
- fetch_technical accepts daily_latest_date / weekly_latest_date cutoffs
- fetch_fundamental: F3 quarterly YoY tries info.revenueGrowth first, falls back
  to computing from quarterly_financials; includes quarter/year end dates
- fetch_fundamental: adds market_cap
- All timestamps in CST (America/Chicago)
"""

from __future__ import annotations
import yfinance as yf
import pandas as pd
import numpy as np
import ta
from zoneinfo import ZoneInfo

CST = ZoneInfo("America/Chicago")


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def normalize_ticker(ticker: str) -> str:
    ticker = ticker.upper().strip()
    if "." in ticker:
        return ticker
    for suffix in ["", ".NS", ".BO"]:
        try:
            if not yf.Ticker(ticker + suffix).history(period="1d").empty:
                return ticker + suffix
        except Exception:
            pass
    return ticker


def _safe(v, ndigits=None, as_int=False):
    try:
        if v is None:
            return None
        f = float(v)
        if np.isnan(f) or np.isinf(f):
            return None
        if as_int:
            return int(f)
        return round(f, ndigits) if ndigits is not None else f
    except Exception:
        return None


def _compare_averages(frame: pd.DataFrame, bars_back: int,
                      price_col: str, avg_window: int) -> dict | None:
    """
    Compare latest avg_window-bar avg vs avg_window-bar avg ending bars_back ago.
    Returns raw price lists, date ranges, pct-change (not ratio).
    """
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

    # Raw values
    latest_dates  = [str(d.date()) for d in latest_slice.index]
    prior_dates   = [str(d.date()) for d in prior_slice.index]
    latest_prices = [_safe(v, 2) for v in latest_slice[price_col]]
    prior_prices  = [_safe(v, 2) for v in prior_slice[price_col]]

    price_pct = _safe((lp - pp) / abs(pp) * 100, 2) if pp else None
    vol_pct   = _safe((lv - pv) / abs(pv) * 100, 2) if pv else None

    return {
        # Date ranges
        "latest_date_range":  [latest_dates[0], latest_dates[-1]],
        "prior_date_range":   [prior_dates[0],  prior_dates[-1]],
        # Raw prices
        "latest_prices":      latest_prices,
        "prior_prices":       prior_prices,
        # Aggregates
        "latest_price_avg":   _safe(lp, 2),
        "prior_price_avg":    _safe(pp, 2),
        "price_up":           (lp > pp) if pp else None,
        "price_pct_change":   price_pct,
        "latest_volume_avg":  int(round(lv)) if not np.isnan(lv) else None,
        "prior_volume_avg":   int(round(pv)) if not np.isnan(pv) else None,
        "volume_up":          (lv > pv) if pv else None,
        "volume_pct_change":  vol_pct,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Technical data
# ─────────────────────────────────────────────────────────────────────────────

def fetch_technical(ticker: str,
                    daily_latest_date=None,
                    weekly_latest_date=None) -> dict:
    """
    3 years of daily OHLCV → technical indicators.
    daily_latest_date / weekly_latest_date: str 'YYYY-MM-DD' or None.
    When provided, data is truncated to that date before computing indicators,
    so incomplete day/week doesn't skew results.
    """
    try:
        sym   = normalize_ticker(ticker)
        stock = yf.Ticker(sym)
        data  = stock.history(period="3y", interval="1d", auto_adjust=False)

        if data.empty:
            return {"error": f"No price data for {ticker}"}

        if isinstance(data.columns, pd.MultiIndex):
            data.columns = data.columns.get_level_values(0)

        required = ["Open", "High", "Low", "Close", "Volume"]
        missing  = [c for c in required if c not in data.columns]
        if missing:
            return {"error": f"Missing columns: {missing}"}

        df = data.copy().dropna(subset=["Open", "High", "Low", "Close"])
        if df.empty:
            return {"error": f"No valid OHLC data for {ticker}"}

        # Adj Close for MA / return comparisons
        if "Adj Close" in df.columns and df["Adj Close"].notna().any():
            df["ReturnClose"] = df["Adj Close"].where(df["Adj Close"].notna(), df["Close"])
        else:
            df["ReturnClose"] = df["Close"]

        # Moving averages (need full history for accurate SMAs)
        for w in [10, 20, 50, 150, 200]:
            df[f"SMA{w}"] = ta.trend.SMAIndicator(df["ReturnClose"], window=w).sma_indicator()

        # Weekly resample (from daily — no second API call)
        weekly = (
            df[["Open", "High", "Low", "Close", "Volume", "ReturnClose"]]
            .resample("W-FRI")
            .agg({"Open": "first", "High": "max", "Low": "min",
                  "Close": "last", "Volume": "sum", "ReturnClose": "last"})
            .dropna(subset=["Open", "High", "Low", "Close"])
        )

        # Apply user-specified cutoff dates
        df_daily = df.copy()
        if daily_latest_date:
            df_daily = df_daily.loc[:daily_latest_date]
        df_weekly = weekly.copy()
        if weekly_latest_date:
            df_weekly = df_weekly.loc[:weekly_latest_date]

        # Use the cutoff-aware dataframe for latest bar
        latest = df_daily.iloc[-1] if not df_daily.empty else df.iloc[-1]

        # MA alignment (always from full df, cutoff-aware for latest bar)
        ma = {
            "MA10":  _safe(latest.get("SMA10"),  2),
            "MA20":  _safe(latest.get("SMA20"),  2),
            "MA50":  _safe(latest.get("SMA50"),  2),
            "MA150": _safe(latest.get("SMA150"), 2),
            "MA200": _safe(latest.get("SMA200"), 2),
        }
        all_ma = all(v is not None for v in ma.values())
        m10, m20, m50, m150, m200 = ma["MA10"], ma["MA20"], ma["MA50"], ma["MA150"], ma["MA200"]
        ma_checks = {
            "MA10>MA20":      (m10  > m20)  if all_ma else None,
            "MA20>MA50":      (m20  > m50)  if all_ma else None,
            "MA50>MA150":     (m50  > m150) if all_ma else None,
            "MA150>MA200":    (m150 > m200) if all_ma else None,
            "full_alignment": (m10 > m20 > m50 > m150 > m200) if all_ma else None,
        }

        # Daily comparisons (5-bar smoothing)
        daily_3m  = _compare_averages(df_daily, bars_back=63,  price_col="ReturnClose", avg_window=5)
        daily_12m = _compare_averages(df_daily, bars_back=252, price_col="ReturnClose", avg_window=5)

        # Weekly comparisons (4-bar smoothing)
        weekly_3m  = _compare_averages(df_weekly, bars_back=13, price_col="ReturnClose", avg_window=4)
        weekly_12m = _compare_averages(df_weekly, bars_back=52, price_col="ReturnClose", avg_window=4)

        # Big moves — last 90 days on ReturnClose
        df_bm = df_daily[["Close", "ReturnClose", "Volume"]].copy()
        df_bm["daily_pct"] = df_bm["ReturnClose"].pct_change() * 100
        df_bm["vol_30d_avg"] = (
            df_bm["Volume"].rolling(window=30, min_periods=10).mean().shift(1)
        )
        recent_90 = df_bm.tail(90)
        big_up_events:   list[dict] = []
        big_down_events: list[dict] = []

        for idx, row in recent_90.iterrows():
            pct = _safe(row["daily_pct"], 2)
            if pct is None or abs(pct) < 10:
                continue
            vol     = _safe(row["Volume"],      as_int=True)
            avg_vol = _safe(row["vol_30d_avg"], as_int=True)
            event = {
                "date":          str(idx.date()),
                "pct_change":    pct,
                "close":         _safe(row["Close"], 2),
                "volume":        vol,
                "vol_30d_avg":   avg_vol,
                "vol_above_avg": (vol > avg_vol) if (vol and avg_vol) else None,
            }
            (big_up_events if pct >= 10 else big_down_events).append(event)

        return {
            "ticker":          sym,
            "date":            str(latest.name.date()),
            "close":           _safe(latest["Close"], 2),
            "volume":          _safe(latest["Volume"], as_int=True),
            "ma_values":       ma,
            "ma_checks":       ma_checks,
            "daily_vs_3m":     daily_3m,
            "daily_vs_12m":    daily_12m,
            "weekly_vs_3m":    weekly_3m,
            "weekly_vs_12m":   weekly_12m,
            "big_up_events":   big_up_events,
            "big_down_events": big_down_events,
        }

    except Exception as e:
        return {"error": f"Technical fetch failed for {ticker}: {e}"}


# ─────────────────────────────────────────────────────────────────────────────
# Fundamental data
# ─────────────────────────────────────────────────────────────────────────────

def _col_date(columns, idx=0) -> str | None:
    """Extract date string from DataFrame column (usually a Timestamp)."""
    try:
        col = columns[idx]
        if hasattr(col, "date"):
            return str(col.date())
        return str(col)[:10]
    except Exception:
        return None


def _yoy_from_frame(frame, row) -> float | None:
    """YoY % from financial statement frame: (col0 - col1) / |col1| * 100."""
    try:
        if frame.empty or row not in frame.index:
            return None
        s = frame.loc[row].dropna()
        if len(s) < 2:
            return None
        curr, prev = float(s.iloc[0]), float(s.iloc[1])
        if prev == 0:
            return None
        return round((curr - prev) / abs(prev) * 100, 2)
    except Exception:
        return None


def fetch_fundamental(ticker: str) -> dict:
    """
    Fundamental metrics via yfinance.

    F3 quarterly YoY:
      Tries info.revenueGrowth / earningsGrowth first (Yahoo Finance built-in).
      Falls back to computing from quarterly_financials if None.
      Records source used and quarter end date.

    F4 annual YoY:
      Same approach: tries info fields (revenueGrowth is quarterly, no direct
      annual equivalent in info), so always computed from financials.
      Records source and fiscal year end date.
    """
    try:
        sym   = normalize_ticker(ticker)
        stock = yf.Ticker(sym)
        info  = stock.info

        qf = stock.quarterly_financials   # newest quarter = col 0
        af = stock.financials             # newest year    = col 0

        def _get(frame, row, col_idx=0):
            try:
                if frame.empty or row not in frame.index:
                    return None
                val = frame.loc[row].iloc[col_idx]
                return None if pd.isna(val) else float(val)
            except Exception:
                return None

        # Latest values
        q_revenue = _get(qf, "Total Revenue", 0)
        q_eps     = _get(qf, "Basic EPS",     0)
        a_revenue = _get(af, "Total Revenue", 0)
        a_eps     = _get(af, "Basic EPS",     0)

        # Quarter end date
        q_end_date = _col_date(qf.columns, 0) if not qf.empty else None
        # Fiscal year end date
        a_end_date = _col_date(af.columns, 0) if not af.empty else None

        # ── F3: Quarterly YoY — Yahoo Finance first, then fallback ────────────
        raw_q_rev = info.get("revenueGrowth")
        raw_q_eps = info.get("earningsGrowth")

        if raw_q_rev is not None:
            q_rev_yoy = _safe(raw_q_rev * 100, 2)
            q_rev_source = "Yahoo Finance info.revenueGrowth"
        else:
            q_rev_yoy = _yoy_from_frame(qf, "Total Revenue")
            q_rev_source = "Computed from quarterly_financials"

        if raw_q_eps is not None:
            q_eps_yoy = _safe(raw_q_eps * 100, 2)
            q_eps_source = "Yahoo Finance info.earningsGrowth"
        else:
            q_eps_yoy = _yoy_from_frame(qf, "Basic EPS")
            q_eps_source = "Computed from quarterly_financials"

        # ── F4: Annual YoY — computed from annual statements ──────────────────
        a_rev_yoy = _yoy_from_frame(af, "Total Revenue")
        a_eps_yoy = _yoy_from_frame(af, "Basic EPS")
        a_rev_source = "Computed from annual financials"
        a_eps_source = "Computed from annual financials"

        return {
            "ticker":        sym,
            # Latest values
            "q_revenue":     q_revenue,
            "q_eps":         q_eps,
            "a_revenue":     a_revenue,
            "a_eps":         a_eps,
            # YoY growth (in %)
            "q_rev_yoy":     q_rev_yoy,
            "q_eps_yoy":     q_eps_yoy,
            "a_rev_yoy":     a_rev_yoy,
            "a_eps_yoy":     a_eps_yoy,
            # Sources
            "q_rev_source":  q_rev_source,
            "q_eps_source":  q_eps_source,
            "a_rev_source":  a_rev_source,
            "a_eps_source":  a_eps_source,
            # Period end dates
            "q_end_date":    q_end_date,
            "a_end_date":    a_end_date,
            # Valuation
            "forward_pe":    _safe(info.get("forwardPE"),   2),
            "pb_ratio":      _safe(info.get("priceToBook"), 2),
            # Market cap
            "market_cap":    _safe(info.get("marketCap"),   0),
        }

    except Exception as e:
        return {"error": f"Fundamental fetch failed for {ticker}: {e}"}
