"""
trigger_engine.py — Trigger-based custom column computation.

For each ticker, finds the LATEST date in price_history that satisfies a
user-defined set of trigger conditions, then computes price/volume returns
between start and end trigger dates.

Usage (from app.py):
    from trigger_engine import (
        TRIG_FIELD_OPTIONS, TRIG_FIELD_DISPLAY, TRIG_OP_OPTIONS,
        compute_trigger_returns, make_cache_key,
    )

    results = compute_trigger_returns(
        tickers=["AAPL", "MSFT"],
        start_conds=[{"field": "daily_px_pct", "op": ">", "value": 5.0}],
        start_logic="AND",
        end_conds=[],
        end_logic="AND",
    )
    # returns: {ticker: {"Trig Px%":..., "Trig Vol%":..., "Trig Avg Px%":...,
    #                    "Trig Avg Vol%":..., "Trig Start Date":..., "Trig End Date":...}}
"""

from __future__ import annotations
import hashlib
import json
from datetime import datetime
from zoneinfo import ZoneInfo

import storage

# ─────────────────────────────────────────────────────────────────────────────
# Available trigger field definitions
# ─────────────────────────────────────────────────────────────────────────────

# Display label → SQL column name in the merged CTE
TRIG_FIELD_OPTIONS: dict[str, str] = {
    "Daily Px%":            "daily_px_pct",
    "Vol vs Prior Day%":    "vol_vs_prior_pct",
    "Vol vs 5D Avg%":       "vol_vs_5d_avg_pct",
    "Earns 1D Px%":         "earns_1d_px_pct",
    "Earns 1D Vol%":        "earns_1d_vol_pct",
    "Earns 5D Px%":         "earns_5d_px_pct",
    "Earns 5D Vol%":        "earns_5d_vol_pct",
    "Earns 5D Roll Px%":    "earns_5d_roll_px_pct",
    "Earns 5D Roll Vol%":   "earns_5d_roll_vol_pct",
    "Post-Earns Px%":       "post_earns_px_pct",
    "Post-Earns Vol%":      "post_earns_vol_pct",
    "Post-Earns Avg Px%":   "post_earns_avg_px_pct",
    "Post-Earns Avg Vol%":  "post_earns_avg_vol_pct",
}

# Reverse: SQL column → display label
TRIG_FIELD_DISPLAY: dict[str, str] = {v: k for k, v in TRIG_FIELD_OPTIONS.items()}

TRIG_OP_OPTIONS: list[str] = [">", ">=", "<", "<=", "=="]

_EMPTY_ROW: dict = {
    "Trig Start Date": None,
    "Trig End Date":   None,
    "Trig Px%":        None,
    "Trig Vol%":       None,
    "Trig Avg Px%":    None,
    "Trig Avg Vol%":   None,
}

CST = ZoneInfo("America/Chicago")


# ─────────────────────────────────────────────────────────────────────────────
# Cache key
# ─────────────────────────────────────────────────────────────────────────────

def make_cache_key(
    start_conds: list[dict],
    start_logic: str,
    end_conds: list[dict],
    end_logic: str,
) -> str:
    """Return a 16-char hex SHA256 hash of the trigger configuration."""
    def _clean(conds):
        return sorted(
            [{"field": c["field"], "op": c["op"], "value": float(c["value"])}
             for c in conds],
            key=lambda c: json.dumps(c, sort_keys=True),
        )

    config = {
        "start":       _clean(start_conds),
        "start_logic": start_logic,
        "end":         _clean(end_conds),
        "end_logic":   end_logic,
    }
    return hashlib.sha256(json.dumps(config, sort_keys=True).encode()).hexdigest()[:16]


# ─────────────────────────────────────────────────────────────────────────────
# SQL helpers
# ─────────────────────────────────────────────────────────────────────────────

def _build_condition_sql(conds: list[dict], logic: str) -> str:
    """Build a NULL-safe SQL WHERE clause fragment from a list of conditions.

    Each condition: {"field": str, "op": str, "value": float}
    Supports operators: >, >=, <, <=, == (mapped to =)
    NULL values never satisfy any condition (NULL-safe).
    """
    if not conds:
        return "1=1"

    parts = []
    for c in conds:
        field = c.get("field", "")
        op    = c.get("op", ">")
        val   = float(c.get("value", 0))
        sql_op = "=" if op == "==" else op
        parts.append(f"({field} IS NOT NULL AND {field} {sql_op} {val})")

    joiner = " AND " if logic == "AND" else " OR "
    return "(" + joiner.join(parts) + ")"


# SQL CTE that computes per-row fields from price_history + earnings_history
_MERGED_CTE = """
WITH base AS (
    SELECT
        ph.ticker,
        ph.date,
        ph.close,
        ph.volume,
        LAG(ph.close)  OVER (PARTITION BY ph.ticker ORDER BY ph.date) AS prior_close,
        LAG(ph.volume) OVER (PARTITION BY ph.ticker ORDER BY ph.date) AS prior_vol,
        AVG(CAST(ph.volume AS DOUBLE)) OVER (
            PARTITION BY ph.ticker ORDER BY ph.date
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS vol_5d_avg,
        AVG(ph.close) OVER (
            PARTITION BY ph.ticker ORDER BY ph.date
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS close_5d_avg
    FROM price_history ph
    WHERE ph.ticker IN ({placeholders})
),
daily AS (
    SELECT
        ticker, date, close, volume, close_5d_avg, vol_5d_avg,
        CASE WHEN prior_close > 0
            THEN (close / prior_close - 1) * 100
        ELSE NULL END AS daily_px_pct,
        CASE WHEN prior_vol > 0
            THEN (CAST(volume AS DOUBLE) / prior_vol - 1) * 100
        ELSE NULL END AS vol_vs_prior_pct,
        CASE WHEN vol_5d_avg > 0
            THEN (CAST(volume AS DOUBLE) / vol_5d_avg - 1) * 100
        ELSE NULL END AS vol_vs_5d_avg_pct
    FROM base
),
earnings_cols AS (
    SELECT
        ticker,
        CAST(earnings_date AS DATE)    AS date,
        one_day_change * 100           AS earns_1d_px_pct,
        earns_1d_vol_pct,
        earns_5d_px_pct,
        earns_5d_vol_pct,
        earns_5d_roll_px_pct,
        earns_5d_roll_vol_pct,
        post_earns_px_pct,
        post_earns_vol_pct,
        post_earns_avg_px_pct,
        post_earns_avg_vol_pct
    FROM earnings_history
    WHERE ticker IN ({placeholders})
),
merged AS (
    SELECT
        d.*,
        e.earns_1d_px_pct,
        e.earns_1d_vol_pct,
        e.earns_5d_px_pct,
        e.earns_5d_vol_pct,
        e.earns_5d_roll_px_pct,
        e.earns_5d_roll_vol_pct,
        e.post_earns_px_pct,
        e.post_earns_vol_pct,
        e.post_earns_avg_px_pct,
        e.post_earns_avg_vol_pct
    FROM daily d
    LEFT JOIN earnings_cols e
        ON d.ticker = e.ticker AND d.date = e.date
)
"""


def _get_trigger_dates(
    tickers: list[str],
    conditions: list[dict],
    logic: str,
) -> dict[str, str]:
    """Return {ticker: latest_date_str} for tickers that have ≥1 matching row."""
    if not tickers or not conditions:
        return {}

    placeholders = ", ".join(["?" for _ in tickers])
    cond_sql = _build_condition_sql(conditions, logic)
    cte = _MERGED_CTE.format(placeholders=placeholders)

    sql = f"""
    {cte}
    SELECT ticker, CAST(MAX(date) AS TEXT) AS trigger_date
    FROM merged
    WHERE {cond_sql}
    GROUP BY ticker
    """

    con = storage._conn()
    rows = con.execute(sql, tickers + tickers).fetchall()
    return {r[0]: r[1] for r in rows if r[1] is not None}


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def compute_trigger_returns(
    tickers: list[str],
    start_conds: list[dict],
    start_logic: str,
    end_conds: list[dict],
    end_logic: str,
    use_cache: bool = True,
) -> dict[str, dict]:
    """Compute trigger-based returns for each ticker.

    Workflow:
    1. Find the latest start trigger date per ticker.
    2. Find the latest end trigger date (or latest available price date).
    3. Compute Px%, Vol%, Avg Px%, Avg Vol% from start → end.
    4. Cache results in DuckDB; invalidate when price_history advances.

    Returns {ticker: {
        "Trig Start Date": str | None,
        "Trig End Date":   str | None,
        "Trig Px%":        float | None,
        "Trig Vol%":       float | None,
        "Trig Avg Px%":    float | None,
        "Trig Avg Vol%":   float | None,
    }}
    """
    if not tickers or not start_conds:
        return {t: dict(_EMPTY_ROW) for t in tickers}

    cache_key = make_cache_key(start_conds, start_logic, end_conds, end_logic)

    # ── Cache check ───────────────────────────────────────────────────────────
    if use_cache:
        current_max = storage.get_price_history_max_date(tickers)
        cached = storage.get_trigger_cache(cache_key, tickers, current_max)
        if cached is not None:
            return cached

    # ── Step 1: Start dates ───────────────────────────────────────────────────
    start_dates = _get_trigger_dates(tickers, start_conds, start_logic)

    # ── Step 2: End dates ─────────────────────────────────────────────────────
    triggered = list(start_dates.keys())
    if not triggered:
        result = {t: dict(_EMPTY_ROW) for t in tickers}
        _save_and_return(cache_key, result, current_max if use_cache else None)
        return result

    if end_conds:
        end_dates = _get_trigger_dates(triggered, end_conds, end_logic)
    else:
        end_dates = {}

    # Fill missing end dates with the latest available price_history date
    need_latest = [t for t in triggered if t not in end_dates]
    if need_latest:
        latest = storage.get_latest_price_dates(need_latest)
        end_dates.update(latest)

    # ── Step 3: Validate start < end ──────────────────────────────────────────
    valid_pairs: dict[str, tuple[str, str]] = {}
    for t in triggered:
        s = start_dates.get(t)
        e = end_dates.get(t)
        if s and e and e >= s:
            valid_pairs[t] = (s, e)

    # ── Step 4: Compute returns ───────────────────────────────────────────────
    period_returns = storage.get_trigger_period_returns(valid_pairs) if valid_pairs else {}

    # ── Assemble results ──────────────────────────────────────────────────────
    results: dict[str, dict] = {}
    for t in tickers:
        if t in valid_pairs and t in period_returns:
            r = period_returns[t]
            results[t] = {
                "Trig Start Date": valid_pairs[t][0],
                "Trig End Date":   valid_pairs[t][1],
                "Trig Px%":        r.get("px_pct"),
                "Trig Vol%":       r.get("vol_pct"),
                "Trig Avg Px%":    r.get("avg_px_pct"),
                "Trig Avg Vol%":   r.get("avg_vol_pct"),
            }
        else:
            results[t] = dict(_EMPTY_ROW)

    _save_and_return(cache_key, results, current_max if use_cache else None)
    return results


def _save_and_return(cache_key: str, results: dict, max_date: str | None) -> None:
    if max_date:
        try:
            storage.save_trigger_cache(cache_key, results, max_date)
        except Exception:
            pass  # cache save failure is non-fatal
