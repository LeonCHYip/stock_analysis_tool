"""CLI entry point for the rotation study.

    uv run python -m study.run_study
    uv run python -m study.run_study --groups Semiconductors "Financial Services" Energy
    uv run python -m study.run_study --industry Semiconductor --out study/out

Read-only: it opens DuckDB with `read_only=True` and writes only CSVs.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from . import analyze, panel, universe
from .regimes import STUDY_END, STUDY_START

# Trailing-EPS sums need four quarters of history before the window opens, so
# earnings are loaded from further back than the study window. Prices are
# loaded from the same date: split factors are derived from the price series,
# so a report with no price history behind it cannot be restated onto the
# current share basis.
WARMUP_START = "2023-01-01"

DEFAULT_GROUPS = ["Semiconductors", "Financial Services", "Energy"]


def build(con, groups: dict[str, list[str]], eps_col: str = "eps_act") -> dict:
    """Assemble the price, earnings and valuation panels for each group."""
    all_tickers = sorted({t for tks in groups.values() for t in tks})
    print(f"[study] {len(groups)} groups, {len(all_tickers)} tickers")

    px = panel.load_prices(con, all_tickers, WARMUP_START, STUDY_END)
    print(f"[study] price rows: {len(px):,}")
    px = panel.add_share_basis(px)

    earn = panel.load_earnings(con, all_tickers, WARMUP_START, STUDY_END)
    print(f"[study] earnings rows: {len(earn):,} "
          f"({earn['ticker'].nunique() if not earn.empty else 0} tickers)")
    earn = panel.enrich_earnings(earn, px, eps_col=eps_col)

    pe_px = panel.trailing_pe(px, earn)

    px_by, earn_by, pe_by = {}, {}, {}
    for name, tks in groups.items():
        s = set(tks)
        px_by[name]   = px[px["ticker"].isin(s)]
        earn_by[name] = earn[earn["ticker"].isin(s)]
        pe_by[name]   = pe_px[pe_px["ticker"].isin(s)]
    return {"px": px_by, "earn": earn_by, "pe": pe_by, "pe_px": pe_px}


def run(groups: list[str], focus: str, out_dir: Path,
        eps_col: str = "eps_act") -> dict[str, pd.DataFrame]:
    out_dir.mkdir(parents=True, exist_ok=True)
    con = universe.connect()
    try:
        resolved = universe.load_groups(con, groups)
        for name, tks in resolved.items():
            print(f"[study]   {name}: {len(tks)} tickers")
        panels = build(con, resolved, eps_col=eps_col)
    finally:
        con.close()

    ret_ticker, ret_agg = analyze.returns_by_group(panels["px"])
    spreads   = analyze.return_spreads(ret_agg, focus)
    delivery  = analyze.delivery_by_group(panels["earn"])
    xsec      = analyze.cross_section(panels["px"], panels["earn"])
    reaction  = analyze.reaction_slopes(panels["earn"])
    pe_ticker, pe_agg = analyze.pe_decomposition(panels["pe"])

    tables = {
        "returns_by_ticker":   ret_ticker,
        "returns_by_group":    ret_agg,
        "return_spreads":      spreads,
        "delivery_by_group":   delivery,
        "cross_section_corr":  xsec,
        "reaction_slopes":     reaction,
        "pe_decomp_by_ticker": pe_ticker,
        "pe_decomp_by_group":  pe_agg,
    }
    for name, df in tables.items():
        path = out_dir / f"{name}.csv"
        df.to_csv(path, index=False)
        print(f"[study] wrote {path} ({len(df)} rows)")

    # A daily group-median price index, for the report's rotation chart.
    idx_rows = []
    for group, px in panels["px"].items():
        w = px[px["date"] >= pd.Timestamp(STUDY_START)].dropna(subset=["px_total"])
        base = (
            w.sort_values("date")
            .groupby("ticker")["px_total"]
            .transform("first")
        )
        w = w.assign(rebased=w["px_total"] / base * 100.0)
        s = w.groupby("date")["rebased"].median().rename("index_level")
        idx_rows.append(s.reset_index().assign(group=group))
    if idx_rows:
        idx = pd.concat(idx_rows, ignore_index=True)
        idx.to_csv(out_dir / "group_index.csv", index=False)
        print(f"[study] wrote {out_dir/'group_index.csv'} ({len(idx)} rows)")
        tables["group_index"] = idx

    return tables


def main() -> None:
    ap = argparse.ArgumentParser(description="Sector rotation fundamentals study")
    ap.add_argument("--groups", nargs="+", default=DEFAULT_GROUPS,
                    help="Group names (a name not in GROUP_SPECS is read as a sector)")
    ap.add_argument("--focus", default="Semiconductors",
                    help="Group the return spreads are measured against")
    ap.add_argument("--sector", help="Ad-hoc single group: exact sector name")
    ap.add_argument("--industry", help="Ad-hoc single group: industry substring")
    ap.add_argument("--out", default="study/out", help="Output directory")
    ap.add_argument("--eps-basis", choices=["nongaap", "gaap"], default="nongaap",
                    help="Earnings basis for TTM/YoY/P-E (default: non-GAAP, "
                         "matching the basis estimates and surprises use)")
    args = ap.parse_args()

    if args.sector or args.industry:
        name = args.sector or f"{args.industry} (industry)"
        universe.GROUP_SPECS[name] = (
            {"sector": args.sector} if args.sector
            else {"industry_contains": args.industry}
        )
        groups, focus = [name], name
    else:
        groups, focus = args.groups, args.focus

    run(groups, focus, Path(args.out),
        eps_col="eps_act" if args.eps_basis == "nongaap" else "eps_gaap_act")


if __name__ == "__main__":
    main()
