"""
column_catalog.py — authoritative column reference for the stock analysis tool.
Update this file whenever columns are added/edited/removed in app.py.
"""

# Source system shorthands
_YFD = "yfinance yf.download"
_QS  = "yfinance quoteSummary"
_TS  = "Yahoo HTTP v8 timeseries"
_FYF = "Finviz + yfinance yf.download"
_IND = "indicators.py rule engine"
_PHD = "price_history DB"
_TRG = "trigger_engine DB"

_SC = "per scan"
_ST = "at startup"
_UT = "user-set"


def _c(n, g, src, freq, rc, inp, typ, desc):
    return {"column_name": n, "group": g, "source_system": src,
            "update_freq": freq, "raw_or_computed": rc,
            "inputs": inp, "type": typ, "description": desc}


# Sorted by VALUE_COL_GROUPS order, then alphabetically within each group.
COLUMNS = [
    # ── User ──────────────────────────────────────────────────────────────────
    _c("Status", "User", _QS, _UT, "raw", "status", "str", "User watch-list tag (必買/買/等/研究/X)"),

    # ── Price & Volume — Daily (T1) ────────────────────────────────────────────
    _c("12M Avg Px%",  "Price & Volume — Daily (T1)", _YFD, _SC, "computed", "daily_vs_12m.price_pct_change",  "float%", "Avg daily close: latest 63D vs prior 63D ending 12M ago (T1.3)"),
    _c("12M Avg Vol%", "Price & Volume — Daily (T1)", _YFD, _SC, "computed", "daily_vs_12m.volume_pct_change", "float%", "Avg daily vol: latest 63D vs prior 63D ending 12M ago (T1.4)"),
    _c("3M Avg Px%",   "Price & Volume — Daily (T1)", _YFD, _SC, "computed", "daily_vs_3m.price_pct_change",   "float%", "Avg daily close: latest 63D vs prior 63D ending 3M ago (T1.1)"),
    _c("3M Avg Vol%",  "Price & Volume — Daily (T1)", _YFD, _SC, "computed", "daily_vs_3m.volume_pct_change",  "float%", "Avg daily vol: latest 63D vs prior 63D ending 3M ago (T1.2)"),

    # ── Price & Volume — Weekly (T2) ───────────────────────────────────────────
    _c("12M Wkly Avg Px%",  "Price & Volume — Weekly (T2)", _YFD, _SC, "computed", "weekly_vs_12m.price_pct_change",  "float%", "Avg weekly close: latest 13W vs prior 13W ending 12M ago (T2.3)"),
    _c("12M Wkly Avg Vol%", "Price & Volume — Weekly (T2)", _YFD, _SC, "computed", "weekly_vs_12m.volume_pct_change", "float%", "Avg weekly vol: latest 13W vs prior 13W ending 12M ago (T2.4)"),
    _c("3M Wkly Avg Px%",   "Price & Volume — Weekly (T2)", _YFD, _SC, "computed", "weekly_vs_3m.price_pct_change",   "float%", "Avg weekly close: latest 13W vs prior 13W ending 3M ago (T2.1)"),
    _c("3M Wkly Avg Vol%",  "Price & Volume — Weekly (T2)", _YFD, _SC, "computed", "weekly_vs_3m.volume_pct_change",  "float%", "Avg weekly vol: latest 13W vs prior 13W ending 3M ago (T2.2)"),

    # ── Spot Returns ──────────────────────────────────────────────────────────
    _c("12M Px%", "Spot Returns", _PHD, _SC, "computed", "price_history", "float%", "Spot price % change over trailing 12 months"),
    _c("12M Vol%","Spot Returns", _PHD, _SC, "computed", "price_history", "float%", "Spot volume % change over trailing 12 months"),
    _c("1M Px%",  "Spot Returns", _PHD, _SC, "computed", "price_history", "float%", "Spot price % change over trailing 1 month"),
    _c("1M Vol%", "Spot Returns", _PHD, _SC, "computed", "price_history", "float%", "Spot volume % change over trailing 1 month"),
    _c("2Y Px%",  "Spot Returns", _PHD, _SC, "computed", "price_history", "float%", "Spot price % change over trailing 2 years"),
    _c("2Y Vol%", "Spot Returns", _PHD, _SC, "computed", "price_history", "float%", "Spot volume % change over trailing 2 years"),
    _c("3M Px%",  "Spot Returns", _PHD, _SC, "computed", "price_history", "float%", "Spot price % change over trailing 3 months"),
    _c("3M Vol%", "Spot Returns", _PHD, _SC, "computed", "price_history", "float%", "Spot volume % change over trailing 3 months"),
    _c("3Y Px%",  "Spot Returns", _PHD, _SC, "computed", "price_history", "float%", "Spot price % change over trailing 3 years"),
    _c("3Y Vol%", "Spot Returns", _PHD, _SC, "computed", "price_history", "float%", "Spot volume % change over trailing 3 years"),
    _c("5D Px%",  "Spot Returns", _PHD, _SC, "computed", "price_history", "float%", "Spot price % change over trailing 5 days"),
    _c("5D Vol%", "Spot Returns", _PHD, _SC, "computed", "price_history", "float%", "Spot volume % change over trailing 5 days"),
    _c("6M Px%",  "Spot Returns", _PHD, _SC, "computed", "price_history", "float%", "Spot price % change over trailing 6 months"),
    _c("6M Vol%", "Spot Returns", _PHD, _SC, "computed", "price_history", "float%", "Spot volume % change over trailing 6 months"),

    # ── Rolling Returns (Daily) ───────────────────────────────────────────────
    _c("1M Avg Px%",  "Rolling Returns (Daily)", _PHD, _SC, "computed", "price_history", "float%", "Rolling avg daily price % change over 1M window"),
    _c("1M Avg Vol%", "Rolling Returns (Daily)", _PHD, _SC, "computed", "price_history", "float%", "Rolling avg daily volume % change over 1M window"),
    _c("2Y Avg Px%",  "Rolling Returns (Daily)", _PHD, _SC, "computed", "price_history", "float%", "Rolling avg daily price % change over 2Y window"),
    _c("2Y Avg Vol%", "Rolling Returns (Daily)", _PHD, _SC, "computed", "price_history", "float%", "Rolling avg daily volume % change over 2Y window"),
    _c("3Y Avg Px%",  "Rolling Returns (Daily)", _PHD, _SC, "computed", "price_history", "float%", "Rolling avg daily price % change over 3Y window"),
    _c("3Y Avg Vol%", "Rolling Returns (Daily)", _PHD, _SC, "computed", "price_history", "float%", "Rolling avg daily volume % change over 3Y window"),
    _c("6M Avg Px%",  "Rolling Returns (Daily)", _PHD, _SC, "computed", "price_history", "float%", "Rolling avg daily price % change over 6M window"),
    _c("6M Avg Vol%", "Rolling Returns (Daily)", _PHD, _SC, "computed", "price_history", "float%", "Rolling avg daily volume % change over 6M window"),

    # ── Rolling Returns (Weekly) ──────────────────────────────────────────────
    _c("1M Wkly Avg Px%",  "Rolling Returns (Weekly)", _PHD, _SC, "computed", "price_history", "float%", "Rolling avg weekly price % change over 1M window"),
    _c("1M Wkly Avg Vol%", "Rolling Returns (Weekly)", _PHD, _SC, "computed", "price_history", "float%", "Rolling avg weekly volume % change over 1M window"),
    _c("2Y Wkly Avg Px%",  "Rolling Returns (Weekly)", _PHD, _SC, "computed", "price_history", "float%", "Rolling avg weekly price % change over 2Y window"),
    _c("2Y Wkly Avg Vol%", "Rolling Returns (Weekly)", _PHD, _SC, "computed", "price_history", "float%", "Rolling avg weekly volume % change over 2Y window"),
    _c("3Y Wkly Avg Px%",  "Rolling Returns (Weekly)", _PHD, _SC, "computed", "price_history", "float%", "Rolling avg weekly price % change over 3Y window"),
    _c("3Y Wkly Avg Vol%", "Rolling Returns (Weekly)", _PHD, _SC, "computed", "price_history", "float%", "Rolling avg weekly volume % change over 3Y window"),
    _c("6M Wkly Avg Px%",  "Rolling Returns (Weekly)", _PHD, _SC, "computed", "price_history", "float%", "Rolling avg weekly price % change over 6M window"),
    _c("6M Wkly Avg Vol%", "Rolling Returns (Weekly)", _PHD, _SC, "computed", "price_history", "float%", "Rolling avg weekly volume % change over 6M window"),

    # ── Custom Period Returns ─────────────────────────────────────────────────
    _c("Cust Avg Px%",  "Custom Period Returns", _PHD, _SC, "computed", "price_history", "float%", "Rolling avg daily price % change over user-defined date range"),
    _c("Cust Avg Vol%", "Custom Period Returns", _PHD, _SC, "computed", "price_history", "float%", "Rolling avg daily volume % change over user-defined date range"),
    _c("Cust Px%",      "Custom Period Returns", _PHD, _SC, "computed", "price_history", "float%", "Spot price % change over user-defined date range"),
    _c("Cust Vol%",     "Custom Period Returns", _PHD, _SC, "computed", "price_history", "float%", "Spot volume % change over user-defined date range"),

    # ── Trigger Returns ───────────────────────────────────────────────────────
    _c("Trig Avg Px%",   "Trigger Returns", _TRG, _SC, "computed", "trigger_engine", "float%", "Rolling avg daily price % change since trigger fired"),
    _c("Trig Avg Vol%",  "Trigger Returns", _TRG, _SC, "computed", "trigger_engine", "float%", "Rolling avg daily volume % change since trigger fired"),
    _c("Trig End Date",  "Trigger Returns", _TRG, _SC, "computed", "trigger_engine", "date",   "Date trigger condition ended (or today if still active)"),
    _c("Trig Px%",       "Trigger Returns", _TRG, _SC, "computed", "trigger_engine", "float%", "Spot price % change since trigger fired"),
    _c("Trig Start Date","Trigger Returns", _TRG, _SC, "computed", "trigger_engine", "date",   "Date trigger condition first fired"),
    _c("Trig Vol%",      "Trigger Returns", _TRG, _SC, "computed", "trigger_engine", "float%", "Spot volume % change since trigger fired"),

    # ── MA Checks (T3) ────────────────────────────────────────────────────────
    _c("MA10>20",   "MA Checks (T3)", _IND, _SC, "computed", "ma_checks.MA10>MA20",   "bool", "SMA10 > SMA20 at latest close — from indicator engine T3.1"),
    _c("MA150>200", "MA Checks (T3)", _IND, _SC, "computed", "ma_checks.MA150>MA200", "bool", "SMA150 > SMA200 at latest close — from indicator engine T3.4"),
    _c("MA20>50",   "MA Checks (T3)", _IND, _SC, "computed", "ma_checks.MA20>MA50",   "bool", "SMA20 > SMA50 at latest close — from indicator engine T3.2"),
    _c("MA50>150",  "MA Checks (T3)", _IND, _SC, "computed", "ma_checks.MA50>MA150",  "bool", "SMA50 > SMA150 at latest close — from indicator engine T3.3"),

    # ── MA Values (T3) ────────────────────────────────────────────────────────
    _c("MA10",  "MA Values (T3)", _IND, _SC, "computed", "ma_values.MA10",  "float", "10-day SMA value from indicator T3 detail"),
    _c("MA150", "MA Values (T3)", _IND, _SC, "computed", "ma_values.MA150", "float", "150-day SMA value from indicator T3 detail"),
    _c("MA20",  "MA Values (T3)", _IND, _SC, "computed", "ma_values.MA20",  "float", "20-day SMA value from indicator T3 detail"),
    _c("MA200", "MA Values (T3)", _IND, _SC, "computed", "ma_values.MA200", "float", "200-day SMA value from indicator T3 detail"),
    _c("MA50",  "MA Values (T3)", _IND, _SC, "computed", "ma_values.MA50",  "float", "50-day SMA value from indicator T3 detail"),

    # ── Big Moves 90d (T4) ────────────────────────────────────────────────────
    _c("# Dn≥10%", "Big Moves 90d (T4)", _IND, _SC, "computed", "big_down_events", "int", "Days with ≥10% decline in 90D (T4.2; fewer is better)"),
    _c("# Up≥10%", "Big Moves 90d (T4)", _IND, _SC, "computed", "big_up_events",   "int", "Days with ≥10% gain in 90D (T4.1)"),

    # ── Quarterly (F1/F3) ─────────────────────────────────────────────────────
    _c("Q EPS",      "Quarterly (F1/F3)", _TS, _SC, "raw",      "quarterlyBasicEPS",              "float",  "Latest quarterly GAAP EPS per share (F1.2)"),
    _c("Q EPS YoY%", "Quarterly (F1/F3)", _TS, _SC, "computed", "quarterlyBasicEPS",              "float%", "Q EPS YoY % vs same quarter prior year (F3.2)"),
    _c("Q End Date", "Quarterly (F1/F3)", _QS, _SC, "raw",      "incomeStatementHistoryQuarterly","date",   "End date of the latest reported quarter"),
    _c("Q Rev",      "Quarterly (F1/F3)", _TS, _SC, "raw",      "quarterlyTotalRevenue",          "float",  "Latest quarterly total revenue $ (F1.1)"),
    _c("Q Rev YoY%", "Quarterly (F1/F3)", _TS, _SC, "computed", "quarterlyTotalRevenue",          "float%", "Q revenue YoY % vs same quarter prior year (F3.1)"),

    # ── Annual (F2/F4) ────────────────────────────────────────────────────────
    _c("A EPS",      "Annual (F2/F4)", _TS, _SC, "raw",      "annualBasicEPS",     "float",  "Latest annual GAAP EPS per share (F2.2)"),
    _c("A EPS YoY%", "Annual (F2/F4)", _TS, _SC, "computed", "annualBasicEPS",     "float%", "Annual EPS YoY % vs prior fiscal year (F4.2)"),
    _c("A End Date", "Annual (F2/F4)", _TS, _SC, "raw",      "annualTotalRevenue", "date",   "End date of the latest reported fiscal year"),
    _c("A Rev",      "Annual (F2/F4)", _TS, _SC, "raw",      "annualTotalRevenue", "float",  "Latest annual total revenue $ (F2.1)"),
    _c("A Rev YoY%", "Annual (F2/F4)", _TS, _SC, "computed", "annualTotalRevenue", "float%", "Annual revenue YoY % vs prior fiscal year (F4.1)"),

    # ── Valuation (F5/F6) ─────────────────────────────────────────────────────
    _c("Fwd PE",         "Valuation (F5/F6)", _QS,  _SC, "raw",      "defaultKeyStatistics.forwardPE",  "float",  "Forward 12M P/E ratio (F5 input)"),
    _c("Fwd PE vs Med%", "Valuation (F5/F6)", _IND, _SC, "computed", "F5.Ticker vs Median %",           "float%", "Ticker Fwd PE vs peer median Fwd PE % diff (F5)"),
    _c("P/B",            "Valuation (F5/F6)", _QS,  _SC, "raw",      "defaultKeyStatistics.priceToBook","float",  "Price-to-book ratio (F6 input)"),
    _c("P/B vs Med%",    "Valuation (F5/F6)", _IND, _SC, "computed", "F6.Ticker vs Median %",           "float%", "Ticker P/B vs peer median P/B % diff (F6)"),

    # ── Fundamentals ──────────────────────────────────────────────────────────
    _c("Company Description","Fundamentals", _QS, _SC, "raw", "longBusinessSummary", "str",   "Long business description"),
    _c("Company Name",       "Fundamentals", _QS, _SC, "raw", "longName",            "str",   "Full company name"),
    _c("Industry",           "Fundamentals", _QS, _SC, "raw", "industry",            "str",   "Industry classification"),
    _c("Mkt Cap ($B)",       "Fundamentals", _QS, _SC, "raw", "marketCap",           "float", "Market capitalisation in billions USD"),
    _c("Sector",             "Fundamentals", _QS, _SC, "raw", "sector",              "str",   "Sector classification"),

    # ── Earnings Detail ───────────────────────────────────────────────────────
    _c("EPS Act",           "Earnings Detail", _FYF, _ST, "raw",      "eps_act",       "float",  "Reported EPS for most recent quarter (Finviz)"),
    _c("EPS Est",           "Earnings Detail", _FYF, _ST, "raw",      "eps_est",       "float",  "Analyst EPS estimate for most recent quarter (Finviz)"),
    _c("EPS GAAP Act",      "Earnings Detail", _FYF, _ST, "raw",      "eps_gaap_act",  "float",  "Reported GAAP EPS (Finviz)"),
    _c("EPS GAAP Est",      "Earnings Detail", _FYF, _ST, "raw",      "eps_gaap_est",  "float",  "Analyst GAAP EPS estimate (Finviz)"),
    _c("EPS GAAP Sur",      "Earnings Detail", _FYF, _ST, "computed", "eps_gaap_sur",  "float%", "GAAP EPS surprise % (Finviz)"),
    _c("EPS Sur",           "Earnings Detail", _FYF, _ST, "computed", "eps_sur",       "float%", "EPS surprise % (Finviz)"),
    _c("Earns 1D Px%",      "Earnings Detail", _FYF, _ST, "computed", "one_day_change","float%", "Earnings 1-day reaction: BMO → open/prev_close−1; AMC → next_close/earns_close−1. Date+flag from Finviz, prices from yf.download"),
    _c("Last Earnings Date","Earnings Detail", _FYF, _ST, "raw",      "earnings_date", "date",   "Date of most recent earnings release (Finviz)"),
    _c("Last Earnings Time","Earnings Detail", _FYF, _ST, "raw",      "earnings_time", "str",    "BMO or AMC flag for most recent earnings (Finviz)"),
    _c("Next Earnings Date","Earnings Detail", _QS,  _SC, "raw",      "earningsDate",  "date",   "Next scheduled earnings date (Yahoo calendarEvents)"),
    _c("Next Earnings Time","Earnings Detail", _QS,  _SC, "computed", "earningsDate",  "str",    "BMO if earningsDate timestamp hour<16 UTC, else AMC"),
    _c("Rev Act ($M)",      "Earnings Detail", _FYF, _ST, "raw",      "rev_act_m",     "float",  "Reported revenue $M, most recent quarter (Finviz)"),
    _c("Rev Est ($M)",      "Earnings Detail", _FYF, _ST, "raw",      "rev_est_m",     "float",  "Analyst revenue estimate $M (Finviz)"),
    _c("Rev Sur",           "Earnings Detail", _FYF, _ST, "computed", "rev_sur",       "float%", "Revenue surprise % (Finviz)"),

    # ── Earnings Extended ─────────────────────────────────────────────────────
    _c("Earns 1D Vol%",      "Earnings Extended", _FYF, _ST, "computed", "earns_1d_vol_pct",      "float%", "Volume % change on earnings day vs prior day"),
    _c("Earns 5D Px%",       "Earnings Extended", _FYF, _ST, "computed", "earns_5d_px_pct",       "float%", "Spot price % change over 5 days after earnings"),
    _c("Earns 5D Roll Px%",  "Earnings Extended", _FYF, _ST, "computed", "earns_5d_roll_px_pct",  "float%", "Rolling avg daily price % change over 5 days after earnings"),
    _c("Earns 5D Roll Vol%", "Earnings Extended", _FYF, _ST, "computed", "earns_5d_roll_vol_pct", "float%", "Rolling avg daily volume % change over 5 days after earnings"),
    _c("Earns 5D Vol%",      "Earnings Extended", _FYF, _ST, "computed", "earns_5d_vol_pct",      "float%", "Spot volume % change over 5 days after earnings"),
    _c("Post-Earns Avg Px%", "Earnings Extended", _FYF, _ST, "computed", "post_earns_avg_px_pct", "float%", "Rolling avg daily price % change from earnings to latest close"),
    _c("Post-Earns Avg Vol%","Earnings Extended", _FYF, _ST, "computed", "post_earns_avg_vol_pct","float%", "Rolling avg daily volume % change from earnings to latest close"),
    _c("Post-Earns Px%",     "Earnings Extended", _FYF, _ST, "computed", "post_earns_px_pct",     "float%", "Spot price % change from earnings date to latest close"),
    _c("Post-Earns Vol%",    "Earnings Extended", _FYF, _ST, "computed", "post_earns_vol_pct",    "float%", "Spot volume % change from earnings date to latest close"),

    # ── Score ─────────────────────────────────────────────────────────────────
    _c("F Score", "Score", _IND, _SC, "computed", "F1–F6 sub-indicators", "float", "Fundamental score 0–50 (F1–F6; each sub worth 5 pts)"),
    _c("Score",   "Score", _IND, _SC, "computed", "T1–T4, F1–F6",         "float", "Composite score 0–100 (T1/T2 price+vol pairs; T3/T4/F subs each 5 pts)"),
    _c("T Score", "Score", _IND, _SC, "computed", "T1–T4 sub-indicators", "float", "Technical score 0–50 (T1–T4; each sub worth 5 pts)"),

    # ── Extended Valuation ────────────────────────────────────────────────────
    _c("Beta",                "Extended Valuation", _QS, _SC, "raw", "beta",                   "float",  "Market beta"),
    _c("Book Value/Share",    "Extended Valuation", _QS, _SC, "raw", "book_value_per_share",    "float",  "Book value per share"),
    _c("Cash/Share",          "Extended Valuation", _QS, _SC, "raw", "total_cash_per_share",    "float",  "Total cash per share"),
    _c("Div Rate",            "Extended Valuation", _QS, _SC, "raw", "dividend_rate",           "float",  "Annual dividend per share"),
    _c("Div Yield%",          "Extended Valuation", _QS, _SC, "raw", "dividend_yield",          "float%", "Trailing annual dividend yield"),
    _c("EV/EBITDA",           "Extended Valuation", _QS, _SC, "raw", "ev_to_ebitda",            "float",  "Enterprise value / EBITDA"),
    _c("EV/Revenue",          "Extended Valuation", _QS, _SC, "raw", "ev_to_revenue",           "float",  "Enterprise value / revenue"),
    _c("Enterprise Value ($B)","Extended Valuation",_QS, _SC, "raw", "enterprise_value",        "float",  "Enterprise value in billions USD"),
    _c("FCF Spot ($B)",       "Extended Valuation", _QS, _SC, "raw", "fcf_spot",                "float",  "Trailing free cash flow in billions USD"),
    _c("Forward EPS",         "Extended Valuation", _QS, _SC, "raw", "forward_eps",             "float",  "Forward 12M EPS estimate"),
    _c("Insiders%",           "Extended Valuation", _QS, _SC, "raw", "held_pct_insiders",       "float%", "% shares held by insiders"),
    _c("Institutions%",       "Extended Valuation", _QS, _SC, "raw", "held_pct_institutions",   "float%", "% shares held by institutions"),
    _c("PEG Ratio",           "Extended Valuation", _QS, _SC, "raw", "peg_ratio",               "float",  "PEG ratio (Fwd PE / EPS growth rate)"),
    _c("Payout Ratio%",       "Extended Valuation", _QS, _SC, "raw", "payout_ratio",            "float%", "Dividend payout ratio"),
    _c("Revenue/Share",       "Extended Valuation", _QS, _SC, "raw", "revenue_per_share",       "float",  "Trailing 12M revenue per share"),
    _c("Total Cash ($B)",     "Extended Valuation", _QS, _SC, "raw", "total_cash",              "float",  "Total cash and equivalents in billions USD"),
    _c("Total Debt ($B)",     "Extended Valuation", _QS, _SC, "raw", "total_debt_spot",         "float",  "Total debt in billions USD (spot from quoteSummary)"),
    _c("Trailing EPS",        "Extended Valuation", _QS, _SC, "raw", "trailing_eps",            "float",  "Trailing 12M EPS"),
    _c("Trailing PE",         "Extended Valuation", _QS, _SC, "raw", "trailing_pe",             "float",  "Trailing 12M P/E ratio"),
    _c("Trailing PEG",        "Extended Valuation", _QS, _SC, "raw", "trailing_peg",            "float",  "Trailing PEG ratio"),

    # ── Avg $Vol / Mkt Cap ────────────────────────────────────────────────────
    _c("Avg $Vol 20D / Mkt Cap%", "Avg $Vol / Mkt Cap", _YFD, _SC, "computed", "avg_dollar_vol_20d / market_cap", "float%", "20D avg dollar volume as % of mkt cap (liquidity proxy)"),
    _c("Avg $Vol 50D / Mkt Cap%", "Avg $Vol / Mkt Cap", _YFD, _SC, "computed", "avg_dollar_vol_50d / market_cap", "float%", "50D avg dollar volume as % of mkt cap (liquidity proxy)"),

    # ── Income Statement ──────────────────────────────────────────────────────
    _c("A EBITDA ($B)",       "Income Statement", _TS, _SC, "raw",      "annualEBITDA",                "float",  "Latest annual EBITDA in billions"),
    _c("A EBITDA YoY%",       "Income Statement", _TS, _SC, "computed", "annualEBITDA",                "float%", "Annual EBITDA YoY % change"),
    _c("A Gross Profit ($B)", "Income Statement", _TS, _SC, "raw",      "annualGrossProfit",           "float",  "Latest annual gross profit in billions"),
    _c("A Gross Profit YoY%", "Income Statement", _TS, _SC, "computed", "annualGrossProfit",           "float%", "Annual gross profit YoY % change"),
    _c("A Net Income ($B)",   "Income Statement", _TS, _SC, "raw",      "annualNetIncome",             "float",  "Latest annual net income in billions"),
    _c("A Net Income YoY%",   "Income Statement", _TS, _SC, "computed", "annualNetIncome",             "float%", "Annual net income YoY % change"),
    _c("A Op Income ($B)",    "Income Statement", _TS, _SC, "raw",      "annualOperatingIncome",       "float",  "Latest annual operating income in billions"),
    _c("A Op Income YoY%",    "Income Statement", _TS, _SC, "computed", "annualOperatingIncome",       "float%", "Annual operating income YoY % change"),
    _c("A R&D ($B)",          "Income Statement", _TS, _SC, "raw",      "annualResearchAndDevelopment","float",  "Latest annual R&D spend in billions"),
    _c("A R&D YoY%",          "Income Statement", _TS, _SC, "computed", "annualResearchAndDevelopment","float%", "Annual R&D YoY % change"),
    _c("Q EBITDA ($B)",       "Income Statement", _TS, _SC, "raw",      "quarterlyEBITDA",             "float",  "Latest quarterly EBITDA in billions"),
    _c("Q EBITDA YoY%",       "Income Statement", _TS, _SC, "computed", "quarterlyEBITDA",             "float%", "Quarterly EBITDA YoY % vs same quarter prior year"),
    _c("Q Gross Profit ($B)", "Income Statement", _TS, _SC, "raw",      "quarterlyGrossProfit",        "float",  "Latest quarterly gross profit in billions"),
    _c("Q Gross Profit YoY%", "Income Statement", _TS, _SC, "computed", "quarterlyGrossProfit",        "float%", "Quarterly gross profit YoY %"),
    _c("Q Net Income ($B)",   "Income Statement", _TS, _SC, "raw",      "quarterlyNetIncome",          "float",  "Latest quarterly net income in billions"),
    _c("Q Net Income YoY%",   "Income Statement", _TS, _SC, "computed", "quarterlyNetIncome",          "float%", "Quarterly net income YoY %"),
    _c("Q Op Income ($B)",    "Income Statement", _TS, _SC, "raw",      "quarterlyOperatingIncome",    "float",  "Latest quarterly operating income in billions"),
    _c("Q Op Income YoY%",    "Income Statement", _TS, _SC, "computed", "quarterlyOperatingIncome",    "float%", "Quarterly operating income YoY %"),
    _c("Q R&D ($B)",          "Income Statement", _TS, _SC, "raw",      "quarterlyResearchAndDevelopment","float","Latest quarterly R&D spend in billions"),
    _c("Q R&D YoY%",          "Income Statement", _TS, _SC, "computed", "quarterlyResearchAndDevelopment","float%","Quarterly R&D YoY %"),

    # ── Cash Flow ─────────────────────────────────────────────────────────────
    _c("A CapEx ($B)",  "Cash Flow", _TS, _SC, "raw",      "annualCapitalExpenditure",    "float",  "Latest annual capex in billions (stored negative)"),
    _c("A CapEx YoY%",  "Cash Flow", _TS, _SC, "computed", "annualCapitalExpenditure",    "float%", "Annual capex YoY % change"),
    _c("A FCF ($B)",    "Cash Flow", _TS, _SC, "raw",      "annualFreeCashFlow",          "float",  "Latest annual free cash flow in billions"),
    _c("A FCF YoY%",    "Cash Flow", _TS, _SC, "computed", "annualFreeCashFlow",          "float%", "Annual FCF YoY % change"),
    _c("A OCF ($B)",    "Cash Flow", _TS, _SC, "computed", "annualFreeCashFlow + annualCapitalExpenditure", "float", "Latest annual OCF in billions (FCF − CapEx)"),
    _c("A OCF YoY%",    "Cash Flow", _TS, _SC, "computed", "annualFreeCashFlow",          "float%", "Annual OCF YoY % change"),
    _c("Q CapEx ($B)",  "Cash Flow", _TS, _SC, "raw",      "quarterlyCapitalExpenditure", "float",  "Latest quarterly capex in billions"),
    _c("Q CapEx YoY%",  "Cash Flow", _TS, _SC, "computed", "quarterlyCapitalExpenditure", "float%", "Quarterly capex YoY %"),
    _c("Q FCF ($B)",    "Cash Flow", _TS, _SC, "raw",      "quarterlyFreeCashFlow",       "float",  "Latest quarterly free cash flow in billions"),
    _c("Q FCF YoY%",    "Cash Flow", _TS, _SC, "computed", "quarterlyFreeCashFlow",       "float%", "Quarterly FCF YoY %"),
    _c("Q OCF ($B)",    "Cash Flow", _TS, _SC, "computed", "quarterlyFreeCashFlow + quarterlyCapitalExpenditure", "float", "Latest quarterly OCF in billions"),
    _c("Q OCF YoY%",    "Cash Flow", _TS, _SC, "computed", "quarterlyFreeCashFlow",       "float%", "Quarterly OCF YoY %"),

    # ── Balance Sheet ─────────────────────────────────────────────────────────
    _c("A Cash ($B)",         "Balance Sheet", _TS, _SC, "raw",      "annualCashAndCashEquivalents",    "float",  "Latest annual cash & equivalents in billions"),
    _c("A Cash YoY%",         "Balance Sheet", _TS, _SC, "computed", "annualCashAndCashEquivalents",    "float%", "Annual cash YoY % change"),
    _c("A Net Debt ($B)",     "Balance Sheet", _TS, _SC, "raw",      "annualNetDebt",                   "float",  "Latest annual net debt in billions"),
    _c("A Net Debt YoY%",     "Balance Sheet", _TS, _SC, "computed", "annualNetDebt",                   "float%", "Annual net debt YoY % change"),
    _c("A Total Assets ($B)", "Balance Sheet", _TS, _SC, "raw",      "annualTotalAssets",               "float",  "Latest annual total assets in billions"),
    _c("A Total Assets YoY%", "Balance Sheet", _TS, _SC, "computed", "annualTotalAssets",               "float%", "Annual total assets YoY % change"),
    _c("A Total Debt ($B)",   "Balance Sheet", _TS, _SC, "raw",      "annualTotalDebt",                 "float",  "Latest annual total debt in billions"),
    _c("A Total Debt YoY%",   "Balance Sheet", _TS, _SC, "computed", "annualTotalDebt",                 "float%", "Annual total debt YoY % change"),
    _c("A Working Cap ($B)",  "Balance Sheet", _TS, _SC, "raw",      "annualWorkingCapital",            "float",  "Latest annual working capital in billions"),
    _c("A Working Cap YoY%",  "Balance Sheet", _TS, _SC, "computed", "annualWorkingCapital",            "float%", "Annual working capital YoY % change"),
    _c("Q Cash ($B)",         "Balance Sheet", _TS, _SC, "raw",      "quarterlyCashAndCashEquivalents", "float",  "Latest quarterly cash & equivalents in billions"),
    _c("Q Cash YoY%",         "Balance Sheet", _TS, _SC, "computed", "quarterlyCashAndCashEquivalents", "float%", "Quarterly cash YoY %"),
    _c("Q Net Debt ($B)",     "Balance Sheet", _TS, _SC, "raw",      "quarterlyNetDebt",                "float",  "Latest quarterly net debt in billions"),
    _c("Q Net Debt YoY%",     "Balance Sheet", _TS, _SC, "computed", "quarterlyNetDebt",                "float%", "Quarterly net debt YoY %"),
    _c("Q Total Assets ($B)", "Balance Sheet", _TS, _SC, "raw",      "quarterlyTotalAssets",            "float",  "Latest quarterly total assets in billions"),
    _c("Q Total Assets YoY%", "Balance Sheet", _TS, _SC, "computed", "quarterlyTotalAssets",            "float%", "Quarterly total assets YoY %"),
    _c("Q Total Debt ($B)",   "Balance Sheet", _TS, _SC, "raw",      "quarterlyTotalDebt",              "float",  "Latest quarterly total debt in billions"),
    _c("Q Total Debt YoY%",   "Balance Sheet", _TS, _SC, "computed", "quarterlyTotalDebt",              "float%", "Quarterly total debt YoY %"),
    _c("Q Working Cap ($B)",  "Balance Sheet", _TS, _SC, "raw",      "quarterlyWorkingCapital",         "float",  "Latest quarterly working capital in billions"),
    _c("Q Working Cap YoY%",  "Balance Sheet", _TS, _SC, "computed", "quarterlyWorkingCapital",         "float%", "Quarterly working capital YoY %"),

    # ── Short Interest ────────────────────────────────────────────────────────
    _c("Avg Vol (M)",         "Short Interest", _QS, _SC, "raw",      "averageVolume",                          "float",  "3-month avg daily volume in millions"),
    _c("Days to Cover",       "Short Interest", _QS, _SC, "raw",      "shortRatio",                             "float",  "Days to cover = shares short / avg daily volume"),
    _c("Float Shares (M)",    "Short Interest", _QS, _SC, "raw",      "floatShares",                            "float",  "Float shares in millions"),
    _c("Shares Short (M)",    "Short Interest", _QS, _SC, "raw",      "sharesShort",                            "float",  "Shares short in millions"),
    _c("Shares Out (M)",      "Short Interest", _QS, _SC, "raw",      "sharesOutstanding",                      "float",  "Shares outstanding in millions"),
    _c("Short % Float (Calc)","Short Interest", _QS, _SC, "computed", "sharesShort / floatShares",              "float%", "Short interest as % of float (computed)"),
    _c("Short % Float (Y)",   "Short Interest", _QS, _SC, "raw",      "shortPercentOfFloat",                    "float%", "Short interest as % of float (Yahoo reported)"),
    _c("Short % Impl Out",    "Short Interest", _QS, _SC, "computed", "sharesShort / impliedSharesOutstanding", "float%", "Short interest as % of implied shares outstanding"),
    _c("Short % Out (Calc)",  "Short Interest", _QS, _SC, "computed", "sharesShort / sharesOutstanding",        "float%", "Short interest as % of shares outstanding (computed)"),
    _c("Short % Out (Y)",     "Short Interest", _QS, _SC, "raw",      "sharesPercentSharesOut",                 "float%", "Short interest as % of shares outstanding (Yahoo reported)"),
    _c("Short Interest Date", "Short Interest", _QS, _SC, "raw",      "dateShortInterest",                      "date",   "Date of the latest short interest report"),
    _c("Short MoM Chg%",      "Short Interest", _QS, _SC, "computed", "sharesShort / sharesShortPriorMonth",    "float%", "Month-over-month % change in shares short"),

    # ── Insider Activity (6M) ─────────────────────────────────────────────────
    _c("Ins Buy #",          "Insider Activity (6M)", _QS, _SC, "raw",      "netSharePurchaseActivity.buys",                    "int",    "Insider buy transaction count in 6M window"),
    _c("Ins Buy %",          "Insider Activity (6M)", _QS, _SC, "raw",      "netSharePurchaseActivity.buyPercentInsiderShares",  "float%", "Insider buys as % of insider-held shares"),
    _c("Ins Buy Shares (M)", "Insider Activity (6M)", _QS, _SC, "raw",      "netSharePurchaseActivity.totalBought",             "float",  "Total insider shares purchased in 6M (millions)"),
    _c("Ins Net %",          "Insider Activity (6M)", _QS, _SC, "computed", "netSharePurchaseActivity",                         "float%", "Net insider activity % (buy % − sell %)"),
    _c("Ins Net Shares (M)", "Insider Activity (6M)", _QS, _SC, "computed", "netSharePurchaseActivity",                         "float",  "Net insider shares (bought − sold) in 6M (millions)"),
    _c("Ins Sell #",         "Insider Activity (6M)", _QS, _SC, "raw",      "netSharePurchaseActivity.sells",                   "int",    "Insider sell transaction count in 6M window"),
    _c("Ins Sell %",         "Insider Activity (6M)", _QS, _SC, "raw",      "netSharePurchaseActivity.sellPercentInsiderShares", "float%", "Insider sells as % of insider-held shares"),
    _c("Ins Sell Shares (M)","Insider Activity (6M)", _QS, _SC, "raw",      "netSharePurchaseActivity.totalSold",               "float",  "Total insider shares sold in 6M (millions)"),

    # ── Margins & Ratios ──────────────────────────────────────────────────────
    _c("Current Ratio",  "Margins & Ratios", _QS, _SC, "raw", "currentRatio",     "float",  "Current assets / current liabilities"),
    _c("D/E Ratio",      "Margins & Ratios", _QS, _SC, "raw", "debtToEquity",     "float",  "Total debt / stockholder equity"),
    _c("EBITDA Margin%", "Margins & Ratios", _QS, _SC, "raw", "ebitdaMargins",    "float%", "EBITDA / revenue"),
    _c("Gross Margin%",  "Margins & Ratios", _QS, _SC, "raw", "grossMargins",     "float%", "Gross profit / revenue"),
    _c("Net Margin%",    "Margins & Ratios", _QS, _SC, "raw", "profitMargins",    "float%", "Net income / revenue"),
    _c("Op Margin%",     "Margins & Ratios", _QS, _SC, "raw", "operatingMargins", "float%", "Operating income / revenue"),
    _c("Quick Ratio",    "Margins & Ratios", _QS, _SC, "raw", "quickRatio",       "float",  "Liquid assets / current liabilities"),
    _c("ROA%",           "Margins & Ratios", _QS, _SC, "raw", "returnOnAssets",   "float%", "Return on assets"),
    _c("ROE%",           "Margins & Ratios", _QS, _SC, "raw", "returnOnEquity",   "float%", "Return on equity"),

    # ── Analyst Targets ───────────────────────────────────────────────────────
    _c("Analyst Count",    "Analyst Targets", _QS, _SC, "raw",      "numberOfAnalystOpinions",           "int",    "Number of analyst price-target opinions"),
    _c("Price vs Target%", "Analyst Targets", _QS, _SC, "computed", "current_price / target_median − 1", "float%", "Current price vs analyst median target (positive = above target)"),
    _c("Rec Key",          "Analyst Targets", _QS, _SC, "raw",      "recommendationKey",                 "str",    "Analyst consensus key (strongBuy/buy/hold/sell/strongSell)"),
    _c("Rec Score",        "Analyst Targets", _QS, _SC, "raw",      "recommendationMean",                "float",  "Consensus score 1–5 (1=strongBuy, 5=strongSell)"),
    _c("Target High",      "Analyst Targets", _QS, _SC, "raw",      "targetHighPrice",                   "float",  "Analyst high price target"),
    _c("Target Low",       "Analyst Targets", _QS, _SC, "raw",      "targetLowPrice",                    "float",  "Analyst low price target"),
    _c("Target Mean",      "Analyst Targets", _QS, _SC, "raw",      "targetMeanPrice",                   "float",  "Analyst mean price target"),
    _c("Target Median",    "Analyst Targets", _QS, _SC, "raw",      "targetMedianPrice",                 "float",  "Analyst median price target"),

    # ── Price & 52W ───────────────────────────────────────────────────────────
    _c("1D Vol%",        "Price & 52W", _YFD, _SC, "computed", "daily_vol_pct",    "float%", "Today's volume vs prior-day volume %"),
    _c("52W High",       "Price & 52W", _YFD, _SC, "raw",      "high_52w",         "float",  "52-week intraday high price"),
    _c("52W Low",        "Price & 52W", _YFD, _SC, "raw",      "low_52w",          "float",  "52-week intraday low price"),
    _c("52W Pos%",       "Price & 52W", _YFD, _SC, "computed", "pos_52w_pct",      "float%", "Where close sits in 52W range: 0%=at low, 100%=at high"),
    _c("Change %",       "Price & 52W", _YFD, _SC, "computed", "daily_pct_change", "float%", "Today's price % change vs prior close"),
    _c("Close",          "Price & 52W", _YFD, _SC, "raw",      "close",            "float",  "Latest session closing price"),
    _c("From 52W High%", "Price & 52W", _YFD, _SC, "computed", "pct_from_52w_high","float%", "% below 52W intraday high (negative)"),
    _c("From 52W Low%",  "Price & 52W", _YFD, _SC, "computed", "pct_from_52w_low", "float%", "% above 52W intraday low (positive)"),
    _c("Last Close Date","Price & 52W", _YFD, _SC, "raw",      "as_of_date",       "date",   "Date of the latest available close in tech_indicators"),

    # ── High/Low Now (Intraday) ───────────────────────────────────────────────
    _c("5D High Now",          "High/Low Now (Intraday)", _YFD, _SC, "computed", "made_high_5d",          "bool", "True if today's intraday high equals the 5-day high (OHLC-based)"),
    _c("22D High Now",         "High/Low Now (Intraday)", _YFD, _SC, "computed", "made_high_22d",         "bool", "True if today's intraday high equals the 22-day high (OHLC-based)"),
    _c("3M High Now",          "High/Low Now (Intraday)", _YFD, _SC, "computed", "made_high_3m",          "bool", "True if today's intraday high equals the 3-month high (OHLC-based)"),
    _c("52W High Now",         "High/Low Now (Intraday)", _YFD, _SC, "computed", "made_high_252d",        "bool", "True if today's intraday high equals the 52-week high (OHLC-based)"),
    _c("3Y High Now",          "High/Low Now (Intraday)", _YFD, _SC, "computed", "made_high_3y",          "bool", "True if today's intraday high equals the 3-year high (OHLC-based)"),
    _c("5D Low Now",           "High/Low Now (Intraday)", _YFD, _SC, "computed", "made_low_5d",           "bool", "True if today's intraday low equals the 5-day low (OHLC-based)"),
    _c("22D Low Now",          "High/Low Now (Intraday)", _YFD, _SC, "computed", "made_low_22d",          "bool", "True if today's intraday low equals the 22-day low (OHLC-based)"),
    _c("52W Low Now",          "High/Low Now (Intraday)", _YFD, _SC, "computed", "made_low_252d",         "bool", "True if today's intraday low equals the 52-week low (OHLC-based)"),
    _c("Days Since 5D High",   "High/Low Now (Intraday)", _YFD, _SC, "computed", "days_since_5d_high",    "int",  "Trading days since the 5-day intraday high was set"),
    _c("Days Since 22D High",  "High/Low Now (Intraday)", _YFD, _SC, "computed", "days_since_22d_high",   "int",  "Trading days since the 22-day intraday high was set"),
    _c("Days Since 3M High",   "High/Low Now (Intraday)", _YFD, _SC, "computed", "days_since_3m_high",    "int",  "Trading days since the 3-month intraday high was set"),
    _c("Days Since 52W High",  "High/Low Now (Intraday)", _YFD, _SC, "computed", "days_since_52w_high",   "int",  "Trading days since the 52-week intraday high was set"),
    _c("Days Since 3Y High",   "High/Low Now (Intraday)", _YFD, _SC, "computed", "days_since_3y_high",    "int",  "Trading days since the 3-year intraday high was set"),
    _c("Days Since 52W Low",   "High/Low Now (Intraday)", _YFD, _SC, "computed", "days_since_52w_low",    "int",  "Trading days since the 52-week intraday low was set"),

    # ── High/Low Levels (Close-Based) ─────────────────────────────────────────
    _c("From 5D High Close%",  "High/Low Levels (Close-Based)", _YFD, _SC, "computed", "pct_from_high_close_5d",  "float%", "% below the 5-day closing high (negative means below)"),
    _c("From 22D High Close%", "High/Low Levels (Close-Based)", _YFD, _SC, "computed", "pct_from_high_close_22d", "float%", "% below the 22-day closing high"),
    _c("From 3M High Close%",  "High/Low Levels (Close-Based)", _YFD, _SC, "computed", "pct_from_high_close_3m",  "float%", "% below the 3-month closing high"),
    _c("From 52W High Close%", "High/Low Levels (Close-Based)", _YFD, _SC, "computed", "pct_from_high_close_52w", "float%", "% below the 52-week closing high"),
    _c("From 3Y High Close%",  "High/Low Levels (Close-Based)", _YFD, _SC, "computed", "pct_from_high_close_3y",  "float%", "% below the 3-year closing high"),
    _c("52W High Close",       "High/Low Levels (Close-Based)", _YFD, _SC, "raw",      "high_close_52w",          "float",  "Highest closing price over trailing 52 weeks"),
    _c("52W Low Close",        "High/Low Levels (Close-Based)", _YFD, _SC, "raw",      "low_close_52w",           "float",  "Lowest closing price over trailing 52 weeks"),
    _c("From 52W Low Close%",  "High/Low Levels (Close-Based)", _YFD, _SC, "computed", "pct_from_low_close_52w",  "float%", "% above the 52-week closing low"),
    _c("3Y High Close",        "High/Low Levels (Close-Based)", _YFD, _SC, "raw",      "high_close_3y",           "float",  "Highest closing price over trailing 3 years"),
    _c("3Y Low Close",         "High/Low Levels (Close-Based)", _YFD, _SC, "raw",      "low_close_3y",            "float",  "Lowest closing price over trailing 3 years"),
    _c("From 3Y Low Close%",   "High/Low Levels (Close-Based)", _YFD, _SC, "computed", "pct_from_low_close_3y",   "float%", "% above the 3-year closing low"),

    # ── Up Streak ─────────────────────────────────────────────────────────────
    _c("Up Streak Days",     "Up Streak", _YFD, _SC, "computed", "up_streak_days",     "int",    "Consecutive up-close days ending today (0 = today was a down day)"),
    _c("Up Streak Px%",      "Up Streak", _YFD, _SC, "computed", "up_streak_px_pct",   "float%", "Spot price % change over the current up-close streak"),
    _c("Up Streak Vol%",     "Up Streak", _YFD, _SC, "computed", "up_streak_vol_pct",  "float%", "Avg streak volume vs avg of equal-length prior period (%)"),
    _c("Up Streak Avg Px%",  "Up Streak", _YFD, _SC, "computed", "up_streak_avg_px_pct","float%","Avg daily price % during streak vs equal-length prior period"),
    _c("Up Streak Avg Vol%", "Up Streak", _YFD, _SC, "computed", "up_streak_avg_vol_pct","float%","Avg daily volume % during streak vs equal-length prior period"),

    # ── Price vs MA (%) ───────────────────────────────────────────────────────
    _c("From EMA200%", "Price vs MA (%)", _YFD, _SC, "computed", "pct_from_ema200",  "float%", "% above/below 200-day EMA"),
    _c("From EMA21%",  "Price vs MA (%)", _YFD, _SC, "computed", "pct_from_ema21",   "float%", "% above/below 21-day EMA"),
    _c("From EMA50%",  "Price vs MA (%)", _YFD, _SC, "computed", "pct_from_ema50",   "float%", "% above/below 50-day EMA"),
    _c("From EMA9%",   "Price vs MA (%)", _YFD, _SC, "computed", "pct_from_ema9",    "float%", "% above/below 9-day EMA"),
    _c("From SMA10%",  "Price vs MA (%)", _YFD, _SC, "computed", "pct_from_sma10",   "float%", "% above/below 10-day SMA"),
    _c("From SMA150%", "Price vs MA (%)", _YFD, _SC, "computed", "pct_from_sma150",  "float%", "% above/below 150-day SMA"),
    _c("From SMA20%",  "Price vs MA (%)", _YFD, _SC, "computed", "pct_from_sma20",   "float%", "% above/below 20-day SMA"),
    _c("From SMA200%", "Price vs MA (%)", _YFD, _SC, "computed", "pct_from_sma200",  "float%", "% above/below 200-day SMA"),
    _c("From SMA50%",  "Price vs MA (%)", _YFD, _SC, "computed", "pct_from_sma50",   "float%", "% above/below 50-day SMA"),

    # ── MA Slopes ─────────────────────────────────────────────────────────────
    _c("SMA10 Slope 10D",  "MA Slopes", _YFD, _SC, "computed", "sma10_slope_10d",  "float", "SMA10 slope over last 10 days (price rise per bar)"),
    _c("SMA150 Slope 20D", "MA Slopes", _YFD, _SC, "computed", "sma150_slope_20d", "float", "SMA150 slope over last 20 days"),
    _c("SMA20 Slope 10D",  "MA Slopes", _YFD, _SC, "computed", "sma20_slope_10d",  "float", "SMA20 slope over last 10 days"),
    _c("SMA200 Slope 20D", "MA Slopes", _YFD, _SC, "computed", "sma200_slope_20d", "float", "SMA200 slope over last 20 days"),
    _c("SMA50 Slope 20D",  "MA Slopes", _YFD, _SC, "computed", "sma50_slope_20d",  "float", "SMA50 slope over last 20 days"),

    # ── EMA & Slope ───────────────────────────────────────────────────────────
    _c("EMA200", "EMA & Slope", _YFD, _SC, "raw", "ema200",  "float", "200-day exponential moving average"),
    _c("EMA21",  "EMA & Slope", _YFD, _SC, "raw", "ema21",   "float", "21-day exponential moving average"),
    _c("EMA50",  "EMA & Slope", _YFD, _SC, "raw", "ema50_e", "float", "50-day exponential moving average"),
    _c("EMA9",   "EMA & Slope", _YFD, _SC, "raw", "ema9",    "float", "9-day exponential moving average"),

    # ── Momentum ──────────────────────────────────────────────────────────────
    _c("MACD Hist",   "Momentum", _YFD, _SC, "computed", "macd_hist",   "float", "MACD histogram (line − signal)"),
    _c("MACD Line",   "Momentum", _YFD, _SC, "computed", "macd_line",   "float", "MACD line (12-day EMA − 26-day EMA)"),
    _c("MACD Signal", "Momentum", _YFD, _SC, "computed", "macd_signal", "float", "MACD signal line (9-day EMA of MACD)"),
    _c("RSI14",       "Momentum", _YFD, _SC, "computed", "rsi14",       "float", "14-period RSI (>70 overbought, <30 oversold)"),
    _c("Stoch D",     "Momentum", _YFD, _SC, "computed", "stoch_d",     "float", "Stochastic %D (3-period SMA of %K)"),
    _c("Stoch K",     "Momentum", _YFD, _SC, "computed", "stoch_k",     "float", "Stochastic %K (14-period)"),

    # ── Bollinger Bands ───────────────────────────────────────────────────────
    _c("BB %B",    "Bollinger Bands", _YFD, _SC, "computed", "bb_pct_b",  "float", "BB %B: 0=at lower band, 1=at upper band"),
    _c("BB Lower", "Bollinger Bands", _YFD, _SC, "computed", "bb_lower",  "float", "Lower band (20-day SMA − 2σ)"),
    _c("BB Middle","Bollinger Bands", _YFD, _SC, "computed", "bb_middle", "float", "Middle band (20-day SMA)"),
    _c("BB Upper", "Bollinger Bands", _YFD, _SC, "computed", "bb_upper",  "float", "Upper band (20-day SMA + 2σ)"),

    # ── Volatility ────────────────────────────────────────────────────────────
    _c("+DI",           "Volatility", _YFD, _SC, "computed", "plus_di",          "float",  "Positive Directional Indicator (14-period)"),
    _c("-DI",           "Volatility", _YFD, _SC, "computed", "minus_di",         "float",  "Negative Directional Indicator (14-period)"),
    _c("ADX14",         "Volatility", _YFD, _SC, "computed", "adx14",            "float",  "Average Directional Index 14-period (>25 = trending)"),
    _c("ATR%",          "Volatility", _YFD, _SC, "computed", "atr_pct",          "float%", "ATR as % of close (normalised volatility)"),
    _c("ATR14",         "Volatility", _YFD, _SC, "computed", "atr14",            "float",  "Average True Range (14-period)"),
    _c("Real Vol 20D%", "Volatility", _YFD, _SC, "computed", "realized_vol_20d", "float%", "Annualised realised volatility over 20 trading days"),
    _c("Real Vol 60D%", "Volatility", _YFD, _SC, "computed", "realized_vol_60d", "float%", "Annualised realised volatility over 60 trading days"),

    # ── Drawdown ──────────────────────────────────────────────────────────────
    _c("Max DD 252D%", "Drawdown", _YFD, _SC, "computed", "max_drawdown_252d", "float%", "Max peak-to-trough drawdown over trailing 252 trading days"),
    _c("Max DD 63D%",  "Drawdown", _YFD, _SC, "computed", "max_drawdown_63d",  "float%", "Max peak-to-trough drawdown over trailing 63 trading days"),

    # ── Volume ────────────────────────────────────────────────────────────────
    _c("AD Line",            "Volume", _YFD, _SC, "computed", "ad_line",               "float", "Accumulation/Distribution line (cumulative)"),
    _c("Avg $Vol 20D",       "Volume", _YFD, _SC, "computed", "avg_dollar_vol_20d",    "float", "Avg daily dollar volume over 20 days (close × volume)"),
    _c("Avg $Vol 50D",       "Volume", _YFD, _SC, "computed", "avg_dollar_vol_50d",    "float", "Avg daily dollar volume over 50 days"),
    _c("CMF20",              "Volume", _YFD, _SC, "computed", "cmf20",                 "float", "Chaikin Money Flow over 20 periods (−1 to +1)"),
    _c("Med Vol 50D",        "Volume", _YFD, _SC, "computed", "median_volume_50d",     "float", "Median daily volume over 50 days"),
    _c("OBV",                "Volume", _YFD, _SC, "computed", "obv",                   "float", "On-Balance Volume (cumulative)"),
    _c("Rel Vol 20D",        "Volume", _YFD, _SC, "computed", "rel_vol_20d",           "float", "Today's volume / 20-day median volume"),
    _c("Rel Vol 50D",        "Volume", _YFD, _SC, "computed", "rel_vol_50d",           "float", "Today's volume / 50-day median volume"),
    _c("Up/Dn Vol Ratio 20D","Volume", _YFD, _SC, "computed", "up_down_vol_ratio_20d", "float", "Sum of up-day volumes / down-day volumes over 20 days"),

    # ── Donchian ──────────────────────────────────────────────────────────────
    _c("Breakout 3M",     "Donchian", _YFD, _SC, "computed", "breakout_3m_high",   "bool",   "True if close breaks the 3M Donchian channel high"),
    _c("Breakout 55D",    "Donchian", _YFD, _SC, "computed", "breakout_55d_high",  "bool",   "True if close breaks the 55-day Donchian channel high"),
    _c("Don High 20",     "Donchian", _YFD, _SC, "raw",      "donchian_high_20",   "float",  "Highest close over 20 periods"),
    _c("Don High 252",    "Donchian", _YFD, _SC, "raw",      "donchian_high_252",  "float",  "Highest close over 252 periods"),
    _c("Don High 55",     "Donchian", _YFD, _SC, "raw",      "donchian_high_55",   "float",  "Highest close over 55 periods"),
    _c("Don Low 20",      "Donchian", _YFD, _SC, "raw",      "donchian_low_20",    "float",  "Lowest close over 20 periods"),
    _c("Don Low 252",     "Donchian", _YFD, _SC, "raw",      "donchian_low_252",   "float",  "Lowest close over 252 periods"),
    _c("Don Low 55",      "Donchian", _YFD, _SC, "raw",      "donchian_low_55",    "float",  "Lowest close over 55 periods"),
    _c("From 20D High%",  "Donchian", _YFD, _SC, "computed", "pct_from_20d_high",  "float%", "% below the 20-day Donchian high"),
    _c("From 252D High%", "Donchian", _YFD, _SC, "computed", "pct_from_252d_high", "float%", "% below the 252-day Donchian high"),
    _c("From 55D High%",  "Donchian", _YFD, _SC, "computed", "pct_from_55d_high",  "float%", "% below the 55-day Donchian high"),

    # ── Swing Points ──────────────────────────────────────────────────────────
    _c("From Swing High%","Swing Points", _YFD, _SC, "computed", "pct_from_swing_high", "float%", "% below the most recent swing high"),
    _c("From Swing Low%", "Swing Points", _YFD, _SC, "computed", "pct_from_swing_low",  "float%", "% above the most recent swing low"),
    _c("Swing High",      "Swing Points", _YFD, _SC, "raw",      "swing_high",          "float",  "Price of the most recent swing high"),
    _c("Swing High Date", "Swing Points", _YFD, _SC, "raw",      "swing_high_date",     "date",   "Date of the most recent swing high"),
    _c("Swing Low",       "Swing Points", _YFD, _SC, "raw",      "swing_low",           "float",  "Price of the most recent swing low"),
    _c("Swing Low Date",  "Swing Points", _YFD, _SC, "raw",      "swing_low_date",      "date",   "Date of the most recent swing low"),

    # ── Rolling Stats 3M ──────────────────────────────────────────────────────
    _c("Down Days 3M",   "Rolling Stats 3M", _YFD, _SC, "computed", "down_days_3m",      "int",   "Count of down-close days in trailing 3 months"),
    _c("Max Win Str 3M", "Rolling Stats 3M", _YFD, _SC, "computed", "max_win_streak_3m", "int",   "Longest consecutive up-close streak in trailing 3 months"),
    _c("UD Ratio 3M",    "Rolling Stats 3M", _YFD, _SC, "computed", "up_down_ratio_3m",  "float", "Up-days / down-days ratio in trailing 3 months"),
    _c("Up Days 3M",     "Rolling Stats 3M", _YFD, _SC, "computed", "up_days_3m",        "int",   "Count of up-close days in trailing 3 months"),
    _c("Win Str 5% 3M",  "Rolling Stats 3M", _YFD, _SC, "computed", "win_streaks_5p_3m", "int",   "Count of ≥5-day up-close streaks in trailing 3 months"),

    # ── Rolling Stats 1Y ──────────────────────────────────────────────────────
    _c("Down Days 1Y",   "Rolling Stats 1Y", _YFD, _SC, "computed", "down_days_1y",      "int",   "Count of down-close days in trailing 1 year"),
    _c("Max Win Str 1Y", "Rolling Stats 1Y", _YFD, _SC, "computed", "max_win_streak_1y", "int",   "Longest consecutive up-close streak in trailing 1 year"),
    _c("UD Ratio 1Y",    "Rolling Stats 1Y", _YFD, _SC, "computed", "up_down_ratio_1y",  "float", "Up-days / down-days ratio in trailing 1 year"),
    _c("Up Days 1Y",     "Rolling Stats 1Y", _YFD, _SC, "computed", "up_days_1y",        "int",   "Count of up-close days in trailing 1 year"),
    _c("Win Str 5% 1Y",  "Rolling Stats 1Y", _YFD, _SC, "computed", "win_streaks_5p_1y", "int",   "Count of ≥5-day up-close streaks in trailing 1 year"),

    # ── Gap Stats ─────────────────────────────────────────────────────────────
    _c("Gap Rate 60D%", "Gap Stats", _YFD, _SC, "computed", "gap_rate_60d", "float%", "% of sessions in 60D with an opening gap >1%"),
    _c("Max Gap 60D%",  "Gap Stats", _YFD, _SC, "computed", "max_gap_60d",  "float%", "Largest opening gap % in trailing 60 days"),

    # ── Big Moves 90D ─────────────────────────────────────────────────────────
    _c("# Big Down 90D", "Big Moves 90D", _YFD, _SC, "computed", "big_down_events_90d", "int", "Days with ≥10% intraday decline in trailing 90 days"),
    _c("# Big Up 90D",   "Big Moves 90D", _YFD, _SC, "computed", "big_up_events_90d",   "int", "Days with ≥10% intraday gain in trailing 90 days"),

    # ── Volume (Raw) ──────────────────────────────────────────────────────────
    _c("Volume", "Volume (Raw)", _YFD, _SC, "raw", "volume", "int", "Raw session volume from yf.download"),

    # ── SMA Values ────────────────────────────────────────────────────────────
    _c("SMA10",  "SMA Values", _YFD, _SC, "raw", "sma10",  "float", "10-day simple moving average of close"),
    _c("SMA150", "SMA Values", _YFD, _SC, "raw", "sma150", "float", "150-day simple moving average of close"),
    _c("SMA20",  "SMA Values", _YFD, _SC, "raw", "sma20",  "float", "20-day simple moving average of close"),
    _c("SMA200", "SMA Values", _YFD, _SC, "raw", "sma200", "float", "200-day simple moving average of close"),
    _c("SMA50",  "SMA Values", _YFD, _SC, "raw", "sma50",  "float", "50-day simple moving average of close"),

    # ── MA Alignment (Raw) ────────────────────────────────────────────────────
    _c("MA10>MA20",   "MA Alignment (Raw)", _YFD, _SC, "raw", "ma10_gt_ma20",   "bool", "Raw DB boolean: SMA10 > SMA20 (tech_indicators)"),
    _c("MA150>MA200", "MA Alignment (Raw)", _YFD, _SC, "raw", "ma150_gt_ma200", "bool", "Raw DB boolean: SMA150 > SMA200"),
    _c("MA20>MA50",   "MA Alignment (Raw)", _YFD, _SC, "raw", "ma20_gt_ma50",   "bool", "Raw DB boolean: SMA20 > SMA50"),
    _c("MA50>MA150",  "MA Alignment (Raw)", _YFD, _SC, "raw", "ma50_gt_ma150",  "bool", "Raw DB boolean: SMA50 > SMA150"),

    # ── Tech Metadata ─────────────────────────────────────────────────────────
    _c("Finalized", "Tech Metadata", _YFD, _SC, "raw", "is_finalized", "bool", "True if tech data was fetched after NYSE 4pm ET close (data is final for the day)"),
]


# ─────────────────────────────────────────────────────────────────────────────
# Streamlit render function
# ─────────────────────────────────────────────────────────────────────────────

def render_column_reference_tab() -> None:
    """Render the Column Reference tab: searchable, filterable table of COLUMNS."""
    import streamlit as st
    import pandas as pd

    st.markdown("### 📖 Column Reference")
    st.caption(
        f"{len(COLUMNS)} columns across {len({c['group'] for c in COLUMNS})} groups. "
        "Update `column_catalog.py` whenever columns are added, renamed, or removed."
    )

    # ── Filters ───────────────────────────────────────────────────────────────
    f1, f2, f3, f4 = st.columns([3, 2, 2, 2])
    with f1:
        search = st.text_input("Search name / description", key="_cr_search").strip().lower()
    with f2:
        all_groups = ["All"] + sorted({c["group"] for c in COLUMNS},
                                      key=lambda g: next((i for i, c in enumerate(COLUMNS) if c["group"] == g), 999))
        grp = st.selectbox("Group", all_groups, key="_cr_group")
    with f3:
        all_sources = ["All"] + sorted({c["source_system"] for c in COLUMNS})
        src = st.selectbox("Source", all_sources, key="_cr_source")
    with f4:
        all_types = ["All"] + sorted({c["type"] for c in COLUMNS})
        typ = st.selectbox("Type", all_types, key="_cr_type")

    # ── Filter logic ──────────────────────────────────────────────────────────
    rows = COLUMNS
    if search:
        rows = [c for c in rows if search in c["column_name"].lower() or search in c["description"].lower()]
    if grp != "All":
        rows = [c for c in rows if c["group"] == grp]
    if src != "All":
        rows = [c for c in rows if c["source_system"] == src]
    if typ != "All":
        rows = [c for c in rows if c["type"] == typ]

    st.caption(f"Showing **{len(rows)}** of {len(COLUMNS)} columns")

    if not rows:
        st.info("No columns match the current filters.")
        return

    df = pd.DataFrame(rows)[
        ["column_name", "group", "type", "raw_or_computed",
         "source_system", "update_freq", "inputs", "description"]
    ]
    df.columns = ["Column", "Group", "Type", "Raw/Computed",
                  "Source", "Update Freq", "Inputs", "Description"]

    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Description": st.column_config.TextColumn(width="large"),
            "Inputs":      st.column_config.TextColumn(width="medium"),
        },
    )
