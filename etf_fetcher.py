"""
etf_fetcher.py — fetch + compute + store the ETF screener dataset.

For each ETF ticker this:
  1. downloads OHLCV (reusing the yf.download pipeline / session used by the
     stock scan) and computes the full technical indicator set via
     technical_fetcher._compute_all_indicators;
  2. adds SPY relative-strength / beta / correlation
     (technical_fetcher.compute_benchmark_rs) — SPY is downloaded once per run;
  3. computes a 0–100 composite trend_score (compute_trend_score);
  4. stores technicals in tech_indicators with asset_type='etf', and fund
     fundamentals (expense ratio, AUM, category, yield, trailing returns,
     asset-class splits, sector weights, top holdings) in etf_profile.

ETF *technicals* share the tech_indicators table with stocks (an ETF is just
another ticker); ETF *fundamentals* live in the separate etf_profile table
because they share nothing with stock EPS/revenue/PE fundamentals.

Usage:
    uv run python -c "import etf_fetcher; etf_fetcher.fetch_and_store_etfs(['QQQ','SOXL','VOO'])"
"""

from __future__ import annotations

import json
import threading
import time
from datetime import timedelta

import pandas as pd
import yfinance as yf

import storage
from yf_session import YF_SESSION, YF_DL_LOCK, close_thread_curl
from market_calendar import (
    nyse_close_passed_today, get_last_trading_day_before_today, et_today,
)
from technical_fetcher import (
    _compute_all_indicators, compute_benchmark_rs, _safe,
)

BENCHMARK = "SPY"
_BATCH_SIZE = 100
# ~4 years covers every indicator window (200-day MA, 252-day return, the
# 756-trading-day "3Y" high/low slice) with margin.
_HISTORY_DAYS = 4 * 365 + 15


# ── Trend score ────────────────────────────────────────────────────────────────
# Weighted composite of six sub-scores, each normalised to [0,1] with a
# documented clip band; trend_score = 100 * sum(w_i * s_i), divided by the FULL
# weight total (1.0). A sub-score whose inputs are entirely missing contributes
# 0 (it is NOT renormalised away) -- so incomplete data drags the score down
# rather than getting a free pass. Only a row with no computable sub-score at
# all (i.e. no technicals -- a fetch failure) returns None / unranked.
_TREND_WEIGHTS = {
    "ma_alignment": 0.25,
    "momentum":     0.25,
    "rel_strength": 0.20,
    "rsi_health":   0.10,
    "drawdown":     0.10,
    "volume":       0.10,
}
# Return clip bands (%) per horizon for the momentum sub-score.
_RET_BANDS = {"ret_20d": 20.0, "ret_60d": 40.0, "ret_126d": 60.0, "ret_252d": 80.0}


def _clip01(x: float) -> float:
    return 0.0 if x < 0 else 1.0 if x > 1 else x


def compute_trend_score(t: dict) -> float | None:
    """Return a 0–100 composite trend score for a tech row dict.

    Missing components count as 0 against the full weight total, so incomplete
    data lowers the score. Returns None only when NO sub-score is computable
    (no technicals at all)."""
    subs: dict[str, float] = {}

    # 1. MA alignment — fraction of trend conditions satisfied.
    ma_flags = []
    for col, positive in (
        ("pct_from_sma20", True), ("pct_from_sma50", True),
        ("pct_from_sma100", True), ("pct_from_sma200", True),
    ):
        v = t.get(col)
        if v is not None:
            ma_flags.append(1.0 if v > 0 else 0.0)
    for col in ("ma20_gt_ma50", "ma50_gt_ma150", "ma150_gt_ma200"):
        v = t.get(col)
        if v is not None:
            ma_flags.append(1.0 if v else 0.0)
    if ma_flags:
        subs["ma_alignment"] = sum(ma_flags) / len(ma_flags)

    # 2. Momentum — mean of clipped horizon returns, rescaled [-1,1] -> [0,1].
    mom = []
    for col, band in _RET_BANDS.items():
        v = t.get(col)
        if v is not None and band:
            mom.append(max(-1.0, min(1.0, v / band)))
    if mom:
        subs["momentum"] = _clip01((sum(mom) / len(mom) + 1.0) / 2.0)

    # 3. Relative strength vs SPY — out-performance capped at +30%.
    rs = []
    for col in ("rs_spy_60d", "rs_spy_126d"):
        v = t.get(col)
        if v is not None:
            rs.append(_clip01((v - 1.0) / 0.30))
    if rs:
        subs["rel_strength"] = sum(rs) / len(rs)

    # 4. RSI health — triangular, peak at 60, ->0 at <=40 or >=85.
    rsi = t.get("rsi14")
    if rsi is not None:
        if rsi <= 40 or rsi >= 85:
            subs["rsi_health"] = 0.0
        elif rsi <= 60:
            subs["rsi_health"] = (rsi - 40) / 20.0
        else:
            subs["rsi_health"] = _clip01(1.0 - (rsi - 60) / 25.0)

    # 5. Drawdown — shallower current drawdown is better (cap at -25%).
    dd = t.get("current_drawdown")
    if dd is None:                      # fall back to 63d max drawdown
        dd = t.get("max_drawdown_63d")
    if dd is not None:
        subs["drawdown"] = 1.0 - _clip01(abs(dd) / 25.0)

    # 6. Volume trend — accumulation via up/down-vol ratio + relative volume.
    vol_parts = []
    udr = t.get("up_down_vol_ratio_20d")
    if udr is not None:
        vol_parts.append(_clip01(udr / 2.0))
    rv = t.get("rel_vol_20d")
    if rv is not None:
        vol_parts.append(_clip01(rv / 2.0))
    if vol_parts:
        subs["volume"] = sum(vol_parts) / len(vol_parts)

    if not subs:
        return None
    # Divide by the FULL weight total: any missing sub-score contributes 0, so
    # incomplete data drags the score down rather than being renormalised away.
    total_w = sum(_TREND_WEIGHTS.values())
    score = sum(_TREND_WEIGHTS[k] * v for k, v in subs.items()) / total_w
    return _safe(score * 100.0, 1)


# ── Profile extraction ──────────────────────────────────────────────────────────

def _extract_profile(ticker: str) -> dict:
    """Fetch ETF fund fundamentals via yfinance (.info + .funds_data), which
    wrap the quoteSummary fundProfile/topHoldings/defaultKeyStatistics modules.
    Returns a flat dict of etf_profile columns (missing fields -> None)."""
    with YF_DL_LOCK:
        tk = yf.Ticker(ticker, session=YF_SESSION)
        info = tk.info or {}
        try:
            fd = tk.funds_data
        except Exception:
            fd = None

    def g(*keys):
        for k in keys:
            v = info.get(k)
            if v is not None:
                return v
        return None

    def _pct(v):  # fraction -> percent (e.g. 0.0107 -> 1.07)
        return _safe(v * 100, 4) if v is not None else None

    # Yahoo's units are inconsistent across fund fields: netExpenseRatio and
    # ytdReturn are ALREADY in percent (VOO 0.03, 10.12), while yield and the
    # multi-year average returns are FRACTIONS (0.0107, 0.21). annualReport-
    # ExpenseRatio, when present, is a fraction. Normalise everything to percent
    # here so etf_profile stores display-ready percentages.
    _are = g("annualReportExpenseRatio")
    expense_pct = _pct(_are) if _are is not None else _safe(g("netExpenseRatio"), 4)

    fields: dict = {
        "long_name":   g("longName", "shortName"),
        "category":    g("category"),
        "fund_family": g("fundFamily"),
        "legal_type":  g("legalType"),
        "expense_ratio": expense_pct,                       # percent
        "aum":         _safe(g("totalAssets"), 0),
        "nav":         _safe(g("navPrice"), 2),
        "yield_pct":   _pct(g("yield")),                    # fraction -> percent
        "ytd_return":  _safe(g("ytdReturn"), 4),            # already percent
        "three_year_return": _pct(g("threeYearAverageReturn")),  # fraction -> percent
        "five_year_return":  _pct(g("fiveYearAverageReturn")),   # fraction -> percent
        "beta_3y":     _safe(g("beta3Year"), 3),
        "avg_volume":  _safe(g("averageVolume"), 0),
        "trailing_pe": _safe(g("trailingPE"), 2),
        "fifty_two_wk_high": _safe(g("fiftyTwoWeekHigh"), 2),
        "fifty_two_wk_low":  _safe(g("fiftyTwoWeekLow"), 2),
        "prev_close":  _safe(g("previousClose"), 2),
    }

    if fd is not None:
        try:
            ac = fd.asset_classes or {}
            fields["stock_pct"] = _safe(ac.get("stockPosition"), 4)
            fields["bond_pct"]  = _safe(ac.get("bondPosition"), 4)
            fields["cash_pct"]  = _safe(ac.get("cashPosition"), 4)
            fields["other_pct"] = _safe(ac.get("otherPosition"), 4)
        except Exception:
            pass
        try:
            sw = fd.sector_weightings or {}
            if sw:
                fields["sector_weightings_json"] = json.dumps(
                    {k: _safe(v, 4) for k, v in sw.items()}, default=str)
        except Exception:
            pass
        try:
            th = fd.top_holdings
            if th is not None and not th.empty:
                holdings = [
                    {"symbol": str(sym),
                     "name": str(r.get("Name", "")),
                     "pct": _safe(r.get("Holding Percent"), 4)}
                    for sym, r in th.head(10).iterrows()
                ]
                fields["top_holdings_json"] = json.dumps(holdings, default=str)
        except Exception:
            pass
        try:
            if fd.description:
                fields["description"] = str(fd.description)[:4000]
        except Exception:
            pass

    fields["raw_info_json"] = json.dumps(
        {k: info.get(k) for k in (
            "longName", "category", "fundFamily", "legalType",
            "annualReportExpenseRatio", "netExpenseRatio", "totalAssets",
            "navPrice", "yield", "ytdReturn", "threeYearAverageReturn",
            "fiveYearAverageReturn", "beta3Year", "trailingPE",
        ) if info.get(k) is not None},
        default=str,
    )
    return fields


# ── Orchestration ───────────────────────────────────────────────────────────────

def _adj_close(df: pd.DataFrame) -> pd.Series:
    """Return the return-adjusted close series from an OHLCV frame."""
    if "Adj Close" in df.columns and df["Adj Close"].notna().any():
        return df["Adj Close"].where(df["Adj Close"].notna(), df["Close"])
    return df["Close"]


def _download_batch(tickers: list[str], start: str, end: str, log) -> pd.DataFrame:
    with YF_DL_LOCK:
        return yf.download(
            tickers=tickers, start=start, end=end, group_by="ticker",
            auto_adjust=False, threads=False, progress=False, session=YF_SESSION,
        )


def fetch_and_store_etfs(tickers: list[str], log=print,
                         stop_event: "threading.Event | None" = None) -> dict:
    """Fetch, compute and store the screener dataset for `tickers`.

    Returns a summary dict {ok, failed, errors}. Safe to run in a background
    thread; closes its thread-local curl handle on exit (see yf_session)."""
    tickers = [t.strip().upper() for t in tickers if t and t.strip()]
    results = {"ok": 0, "failed": 0, "errors": {}}
    if not tickers:
        return results

    today_et = et_today()
    today_str = today_et.isoformat()
    is_final_session = nyse_close_passed_today()
    expected_date = today_str if is_final_session else (
        get_last_trading_day_before_today() or today_str)
    start = (today_et - timedelta(days=_HISTORY_DAYS)).isoformat()
    end = (today_et + timedelta(days=1)).isoformat()

    try:
        # Benchmark (SPY) once per run — reused for every ETF's RS/beta/corr.
        spy_close = None
        try:
            spy_raw = _download_batch([BENCHMARK], start, end, log)
            if spy_raw is not None and not spy_raw.empty:
                if isinstance(spy_raw.columns, pd.MultiIndex):
                    spy_raw = spy_raw[BENCHMARK]
                spy_close = _adj_close(spy_raw.dropna(subset=["Close"]))
        except Exception as e:
            log(f"  [etf] SPY benchmark download failed: {e}")

        for i in range(0, len(tickers), _BATCH_SIZE):
            if stop_event is not None and stop_event.is_set():
                log("  [etf] stop requested — halting.")
                break
            batch = tickers[i: i + _BATCH_SIZE]
            batch_no = i // _BATCH_SIZE + 1
            log(f"  [etf] batch {batch_no}: downloading {len(batch)} tickers…")
            try:
                raw = _download_batch(batch, start, end, log)
            except Exception as e:
                log(f"  [etf] batch {batch_no} download error: {e}")
                for t in batch:
                    results["failed"] += 1
                    results["errors"][t] = f"download failed: {e}"
                continue

            is_multi = isinstance(raw.columns, pd.MultiIndex)
            for ticker in batch:
                if stop_event is not None and stop_event.is_set():
                    break
                try:
                    if is_multi:
                        if ticker not in raw.columns.get_level_values(0):
                            raise ValueError("no bulk data")
                        df = raw[ticker].copy()
                    else:
                        df = raw.copy()
                    if df.empty or df["Close"].isna().all():
                        raise ValueError("no price data")

                    fields = _compute_all_indicators(ticker, df)
                    if "error" in fields:
                        raise ValueError(fields["error"])

                    # SPY relative strength / beta / correlation.
                    fields.update(compute_benchmark_rs(_adj_close(df), spy_close))
                    # Composite trend score (needs the fields above).
                    fields["trend_score"] = compute_trend_score(fields)

                    as_of = fields.pop("_as_of_date", today_str)
                    row_is_final = is_final_session and (as_of >= expected_date)
                    storage.save_tech_indicators(
                        ticker, as_of, fields, row_is_final, asset_type="etf")

                    # Fund profile (per-ticker; rate-limited like stock fundamentals).
                    try:
                        prof = _extract_profile(ticker)
                        storage.save_etf_profile(ticker, today_str, prof)
                    except Exception as e:
                        log(f"  [etf] {ticker}: profile fetch failed: {e}")

                    results["ok"] += 1
                except Exception as e:
                    results["failed"] += 1
                    results["errors"][ticker] = str(e)
                    log(f"  [etf] {ticker}: {e}")

        log(f"  [etf] done: {results['ok']} ok, {results['failed']} failed.")
        return results
    finally:
        close_thread_curl()


if __name__ == "__main__":
    import sys
    _tickers = sys.argv[1].split(",") if len(sys.argv) > 1 else ["QQQ", "SOXL", "VOO", "ARKK"]
    storage.init_db()
    print(fetch_and_store_etfs(_tickers))
