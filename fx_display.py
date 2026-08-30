"""fx_display.py — Currency conversion + data-sanity logic for display columns.

Extracted from app.py's _build_value_record so the exact shipped logic is
directly unit-testable (tested against real DB rows, not a replica).

Two concerns live here:

1. USD conversion: Yahoo reports financial-statement figures in the
   company's reporting currency (fundamentals.financial_currency), not
   necessarily USD. EXCEPTION: quarterly revenue/EPS may come from the
   Finviz override (fundamental_fetcher), which is already USD -- the
   caller passes q_already_usd=True for those (detected via the persisted
   q_rev_source/q_eps_source strings starting with "Finviz").

2. Source-data sanity: Yahoo's v8 timeseries occasionally ships a value
   with a unit error -- observed live 2026-08-24: CIG/CIG-C latest-quarter
   revenue/gross profit/op income exactly 1000x reality (BRL 10.34T
   quarterly vs BRL 42.75B annual, against Yahoo's OWN sane prior
   quarters). A quarter physically cannot exceed ~1.5x its trailing year,
   yet moderate q/a ratios (2-20x) can be legitimate: hypergrowth
   micro-caps' first big quarter, or near-zero annual bases (INTC op
   income at 85x is a tiny-denominator artifact, not corruption). The
   guard therefore requires BOTH an impossible ratio AND absurd absolute
   size relative to market cap before blanking a value -- corrupted data
   shows None, never a plausible-looking wrong number.
"""
from __future__ import annotations


def to_usd(value, currency: str | None, fx_cache: dict) -> float | None:
    """Convert a local-currency raw value to USD. Returns the value
    unchanged for USD/unknown currency (unknown = pre-migration rows;
    conversion becomes possible once the ticker's next scan or the
    backfill populates financial_currency). Returns None when the
    currency is known but no rate is cached -- never a silently-wrong
    number."""
    try:
        f = float(value)
        if f != f:  # NaN
            return None
    except (TypeError, ValueError):
        return None
    if not currency or currency == "USD":
        return f
    rate = fx_cache.get(currency)
    return f * rate if rate is not None else None


# A single quarter above 50x the trailing year is physically implausible
# (implies >200x quarter-over-average growth); requiring the value to also
# exceed 5x market cap screens out legitimate near-zero-annual-base ratios.
_Q_RATIO_LIMIT = 50.0
# Annual side: expected ~4x quarterly, so 200x quarterly is the analogous
# impossibility threshold.
_A_RATIO_LIMIT = 200.0
_MKT_CAP_MULT = 5.0


def flow_pair_usd(q_raw, a_raw, currency: str | None, fx_cache: dict,
                  mkt_cap_usd: float | None,
                  q_already_usd: bool = False) -> tuple[float | None, float | None, bool, bool]:
    """USD-convert a quarterly/annual FLOW-metric pair (revenue, net income,
    OCF, ... -- NOT point-in-time balance-sheet items), then blank a side
    whose magnitude is impossible (unit-corrupted at the source).

    Returns (q_usd, a_usd, q_blanked, a_blanked). The *_blanked flags let
    the caller also blank the corresponding YoY% column, which is computed
    from the same corrupted series.
    """
    if q_already_usd:
        try:
            q_usd = float(q_raw) if q_raw is not None else None
            if q_usd is not None and q_usd != q_usd:
                q_usd = None
        except (TypeError, ValueError):
            q_usd = None
    else:
        q_usd = to_usd(q_raw, currency, fx_cache)
    a_usd = to_usd(a_raw, currency, fx_cache)

    q_bad = a_bad = False
    if q_usd is not None and a_usd is not None and mkt_cap_usd:
        if abs(q_usd) > _Q_RATIO_LIMIT * abs(a_usd) and abs(q_usd) > _MKT_CAP_MULT * mkt_cap_usd:
            q_bad, q_usd = True, None
        elif q_usd != 0 and abs(a_usd) > _A_RATIO_LIMIT * abs(q_usd) \
                and abs(a_usd) > _MKT_CAP_MULT * mkt_cap_usd:
            a_bad, a_usd = True, None
    return q_usd, a_usd, q_bad, a_bad
