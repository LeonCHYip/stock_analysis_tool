"""
reporter.py
-----------
Renders two tables to the console:
  1. Summary table — pass/fail/N/A for each indicator per ticker
  2. Detail table  — raw values and sub-check results per ticker
"""

from __future__ import annotations
from tabulate import tabulate


# ─────────────────────────────────────────────────────────────────────────────
# Symbols
# ─────────────────────────────────────────────────────────────────────────────

def _sym(v) -> str:
    if v is True:
        return "✅"
    if v is False:
        return "❌"
    return "N/A"


def _pct(v) -> str:
    if v is None:
        return "N/A"
    return f"{v:+.1f}%"


def _num(v, decimals=2) -> str:
    if v is None:
        return "N/A"
    return f"{v:,.{decimals}f}"


def _millify(v) -> str:
    """Format large numbers in millions / billions."""
    if v is None:
        return "N/A"
    try:
        v = float(v)
        if abs(v) >= 1e9:
            return f"${v/1e9:.2f}B"
        if abs(v) >= 1e6:
            return f"${v/1e6:.2f}M"
        return f"${v:.2f}"
    except Exception:
        return "N/A"


# ─────────────────────────────────────────────────────────────────────────────
# Summary Table
# ─────────────────────────────────────────────────────────────────────────────

INDICATOR_LABELS = [
    ("T1", "Daily P&V vs History"),
    ("T2", "Weekly P&V vs History"),
    ("T3", "MA Alignment (10>20>50>150>200)"),
    ("T3a", "  ↳ MA10 > MA20"),
    ("T3b", "  ↳ MA20 > MA50"),
    ("T3c", "  ↳ MA50 > MA150"),
    ("T3d", "  ↳ MA150 > MA200"),
    ("T4", "Big Moves 90d (≥10%)"),
    ("T4a", "  ↳ Has ≥1 day +10% up"),
    ("T4b", "  ↳ No day -10% down"),
    ("F1", "Latest Qtr: +Rev & +EPS"),
    ("F2", "Latest Year: +Rev & +EPS"),
    ("F3", "Qtr YoY: Rev>+10%, EPS>+30%"),
    ("F4", "Year YoY: Rev>+10%, EPS>+30%"),
    ("F5", "Fwd PE ≤ Peer Median"),
    ("F6", "P/B Ratio ≤ Peer Median"),
]


def _extract_summary_col(ticker_indicators: dict) -> dict:
    """Extract pass/fail values keyed by short indicator code."""
    inds = ticker_indicators

    def pf(ind_id):
        return inds.get(ind_id, {}).get("pass")

    def sub(ind_id, key):
        return inds.get(ind_id, {}).get("detail", {}).get("sub_checks", {}).get(key)

    return {
        "T1":  pf("T1_daily_price_volume"),
        "T2":  pf("T2_weekly_price_volume"),
        "T3":  pf("T3_ma_alignment"),
        "T3a": sub("T3_ma_alignment", "MA10>MA20"),
        "T3b": sub("T3_ma_alignment", "MA20>MA50"),
        "T3c": sub("T3_ma_alignment", "MA50>MA150"),
        "T3d": sub("T3_ma_alignment", "MA150>MA200"),
        "T4":  pf("T4_big_moves"),
        "T4a": sub("T4_big_moves", "has_big_up_day_>=10pct"),
        "T4b": sub("T4_big_moves", "no_big_down_day_>=-10pct"),
        "F1":  pf("F1_q_profitability"),
        "F2":  pf("F2_a_profitability"),
        "F3":  pf("F3_q_yoy_growth"),
        "F4":  pf("F4_a_yoy_growth"),
        "F5":  pf("F5_forward_pe_vs_peers"),
        "F6":  pf("F6_pb_ratio_vs_peers"),
    }


def print_summary_table(all_results: dict[str, dict]):
    """
    all_results: {ticker: indicators_dict}
    """
    tickers = list(all_results.keys())
    cols = {t: _extract_summary_col(all_results[t]) for t in tickers}

    headers = ["#", "Indicator"] + tickers
    rows = []
    for code, label in INDICATOR_LABELS:
        row = [code, label]
        for t in tickers:
            row.append(_sym(cols[t].get(code)))
        rows.append(row)

    print("\n" + "═" * 70)
    print("  INDICATOR SUMMARY TABLE")
    print("═" * 70)
    print(tabulate(rows, headers=headers, tablefmt="simple"))


# ─────────────────────────────────────────────────────────────────────────────
# Detail Table
# ─────────────────────────────────────────────────────────────────────────────

def _build_detail_rows(ticker: str, indicators: dict, collapsed_indicators: set | None = None) -> list[list]:
    """Build detail rows for one ticker."""
    rows = []
    collapsed_indicators = collapsed_indicators or set()

    def add(label, value):
        rows.append([ticker, label, value])

    def d(ind_id):
        return indicators.get(ind_id, {}).get("detail", {})

    def pass_sym(ind_id):
        return _sym(indicators.get(ind_id, {}).get("pass"))

    # ── T1 ──────────────────────────────────────────────────────────────────
    rows.append([ticker, f"━━ {pass_sym('T1_daily_price_volume')} T1: Daily Price & Volume vs History ━━", ""])
    if "T1" not in collapsed_indicators:
        dd = d("T1_daily_price_volume")
        for period, key in [("3M", "3m"), ("12M", "12m")]:
            add(f"  {period} Comparison Date",     dd.get(f"{key}_comparison_date"))
            add(f"  {period} Latest Price Avg",    _num(dd.get(f"{key}_latest_price_avg")))
            add(f"  {period} Prior Price Avg",     _num(dd.get(f"{key}_prior_price_avg")))
            add(f"  {period} Price Ratio",         _num(dd.get(f"{key}_price_ratio"), 3))
            add(f"  {period} Price Up?",           _sym(dd.get("sub_checks", {}).get(f"daily_price_up_{key}")))
            add(f"  {period} Latest Volume Avg",   f"{dd.get(f'{key}_latest_volume_avg'):,}" if dd.get(f"{key}_latest_volume_avg") else "N/A")
            add(f"  {period} Prior Volume Avg",    f"{dd.get(f'{key}_prior_volume_avg'):,}"  if dd.get(f"{key}_prior_volume_avg")  else "N/A")
            add(f"  {period} Volume Ratio",        _num(dd.get(f"{key}_volume_ratio"), 3))
            add(f"  {period} Volume Up?",          _sym(dd.get("sub_checks", {}).get(f"daily_volume_up_{key}")))

    # ── T2 ──────────────────────────────────────────────────────────────────
    rows.append([ticker, f"━━ {pass_sym('T2_weekly_price_volume')} T2: Weekly Price & Volume vs History ━━", ""])
    if "T2" not in collapsed_indicators:
        dd = d("T2_weekly_price_volume")
        for period, key in [("3M", "3m"), ("12M", "12m")]:
            add(f"  {period} Comparison Date",    dd.get(f"{key}_comparison_date"))
            add(f"  {period} Latest Price Avg",   _num(dd.get(f"{key}_latest_price_avg")))
            add(f"  {period} Prior Price Avg",    _num(dd.get(f"{key}_prior_price_avg")))
            add(f"  {period} Price Ratio",        _num(dd.get(f"{key}_price_ratio"), 3))
            add(f"  {period} Price Up?",          _sym(dd.get("sub_checks", {}).get(f"weekly_price_up_{key}")))
            add(f"  {period} Latest Volume Avg",  f"{dd.get(f'{key}_latest_volume_avg'):,}" if dd.get(f"{key}_latest_volume_avg") else "N/A")
            add(f"  {period} Prior Volume Avg",   f"{dd.get(f'{key}_prior_volume_avg'):,}"  if dd.get(f"{key}_prior_volume_avg")  else "N/A")
            add(f"  {period} Volume Ratio",       _num(dd.get(f"{key}_volume_ratio"), 3))
            add(f"  {period} Volume Up?",         _sym(dd.get("sub_checks", {}).get(f"weekly_volume_up_{key}")))

    # ── T3 ──────────────────────────────────────────────────────────────────
    rows.append([ticker, f"━━ {pass_sym('T3_ma_alignment')} T3: Moving Average Alignment ━━", ""])
    if "T3" not in collapsed_indicators:
        dd = d("T3_ma_alignment")
        for ma in ["MA10", "MA20", "MA50", "MA150", "MA200"]:
            add(f"  {ma}", _num(dd.get(ma)))
        sub = dd.get("sub_checks", {})
        add("  MA10 > MA20",   _sym(sub.get("MA10>MA20")))
        add("  MA20 > MA50",   _sym(sub.get("MA20>MA50")))
        add("  MA50 > MA150",  _sym(sub.get("MA50>MA150")))
        add("  MA150 > MA200", _sym(sub.get("MA150>MA200")))

    # ── T4 ──────────────────────────────────────────────────────────────────
    rows.append([ticker, f"━━ {pass_sym('T4_big_moves')} T4: Big Moves (≥10%, last 90 days) ━━", ""])
    if "T4" not in collapsed_indicators:
        dd = d("T4_big_moves")
        add("  ≥10% UP days (count)",   dd.get("big_up_days_count", 0))
        add("  ≥10% DOWN days (count)", dd.get("big_down_days_count", 0))
        for ev in dd.get("big_up_events", []):
            vol = ev.get("volume")
            vol_str = f"{vol:,}" if vol is not None else "N/A"
            vol_flag = f"vol_above_30d_avg={ev.get('vol_above_avg')}"
            add(f"    UP  {ev['date']}  {ev['pct_change']:+.1f}%  vol={vol_str}", vol_flag)
        for ev in dd.get("big_down_events", []):
            vol = ev.get("volume")
            vol_str = f"{vol:,}" if vol is not None else "N/A"
            vol_flag = f"vol_above_30d_avg={ev.get('vol_above_avg')}"
            add(f"    DWN {ev['date']}  {ev['pct_change']:+.1f}%  vol={vol_str}", vol_flag)

    # ── F1 ──────────────────────────────────────────────────────────────────
    rows.append([ticker, f"━━ {pass_sym('F1_q_profitability')} F1: Latest Quarter Profitability ━━", ""])
    if "F1" not in collapsed_indicators:
        dd = d("F1_q_profitability")
        add("  Q Revenue",   _millify(dd.get("q_revenue")))
        add("  Q EPS",       _num(dd.get("q_eps")))

    # ── F2 ──────────────────────────────────────────────────────────────────
    rows.append([ticker, f"━━ {pass_sym('F2_a_profitability')} F2: Latest Year Profitability ━━", ""])
    if "F2" not in collapsed_indicators:
        dd = d("F2_a_profitability")
        add("  Annual Revenue", _millify(dd.get("a_revenue")))
        add("  Annual EPS",     _num(dd.get("a_eps")))

    # ── F3 ──────────────────────────────────────────────────────────────────
    rows.append([ticker, f"━━ {pass_sym('F3_q_yoy_growth')} F3: Quarter YoY Growth ━━", ""])
    if "F3" not in collapsed_indicators:
        dd = d("F3_q_yoy_growth")
        add("  Q Revenue YoY", _pct(dd.get("q_rev_yoy_pct")))
        add("  Q EPS YoY",     _pct(dd.get("q_eps_yoy_pct")))
        add("  Threshold",     "Rev >+10% / EPS >+30%")

    # ── F4 ──────────────────────────────────────────────────────────────────
    rows.append([ticker, f"━━ {pass_sym('F4_a_yoy_growth')} F4: Annual YoY Growth ━━", ""])
    if "F4" not in collapsed_indicators:
        dd = d("F4_a_yoy_growth")
        add("  Annual Revenue YoY", _pct(dd.get("a_rev_yoy_pct")))
        add("  Annual EPS YoY",     _pct(dd.get("a_eps_yoy_pct")))
        add("  Threshold",          "Rev >+10% / EPS >+30%")

    # ── F5 ──────────────────────────────────────────────────────────────────
    rows.append([ticker, f"━━ {pass_sym('F5_forward_pe_vs_peers')} F5: Forward PE vs Peers ━━", ""])
    if "F5" not in collapsed_indicators:
        dd = d("F5_forward_pe_vs_peers")
        add("  Ticker Fwd PE",       _num(dd.get("ticker_forward_pe")))
        add("  Peer PE Median",      _num(dd.get("peer_pe_median")))
        add("  Peer Count (w/ PE)",  dd.get("peer_count_with_pe_data"))
        add("  Peer PE Values",      str(dd.get("peer_pe_values", [])))
        add("  Classification",      dd.get("classification") or "N/A")

    # ── F6 ──────────────────────────────────────────────────────────────────
    rows.append([ticker, f"━━ {pass_sym('F6_pb_ratio_vs_peers')} F6: P/B Ratio vs Peers ━━", ""])
    if "F6" not in collapsed_indicators:
        dd = d("F6_pb_ratio_vs_peers")
        add("  Ticker P/B",          _num(dd.get("ticker_pb_ratio")))
        add("  Peer P/B Median",     _num(dd.get("peer_pb_median")))
        add("  Peer Count (w/ P/B)", dd.get("peer_count_with_pb_data"))
        add("  Peer P/B Values",     str(dd.get("peer_pb_values", [])))
        add("  Classification",      dd.get("classification") or "N/A")

    return rows


def print_detail_table(
    all_results: dict[str, dict],
    collapsed_tickers: set | None = None,
    collapsed_indicators: set | None = None,
):
    """
    all_results: {ticker: indicators_dict}
    collapsed_tickers:   set of ticker strings to collapse (show header only)
    collapsed_indicators: set of indicator codes (T1–F6) to collapse (show header only)
    """
    collapsed_tickers = collapsed_tickers or set()
    collapsed_indicators = collapsed_indicators or set()

    print("\n" + "═" * 70)
    print("  INDICATOR DETAIL TABLE")
    print("═" * 70)

    all_rows = []
    for ticker, indicators in all_results.items():
        if ticker in collapsed_tickers:
            all_rows.append([ticker, "── [TICKER COLLAPSED] ──────────────────────────────", ""])
        else:
            all_rows.extend(_build_detail_rows(ticker, indicators, collapsed_indicators))

    print(tabulate(all_rows, headers=["Ticker", "Data Point", "Value"], tablefmt="simple"))
