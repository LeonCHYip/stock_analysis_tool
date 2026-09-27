"""CLI entry point for the cross-sectional fundamentals-vs-technicals study.

    uv run python -m study.xsec.run
    uv run python -m study.xsec.run --eps-basis gaap --out study/out_gaap
    STUDY_DB=/path/to/copy.duckdb uv run python -m study.xsec.run

The DuckDB store allows one process at a time, so `STUDY_DB` (or `--db`) points
the study at a file copy when the Streamlit app is holding the live database.
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

import pandas as pd

from . import analyses, backfill_2022 as backfill, config, data, features, technicals


def _log(msg: str, t0: float) -> None:
    print(f"[{time.time() - t0:6.1f}s] {msg}", flush=True)


def build_panels(db_path: str | None, eps_basis: str, t0: float) -> dict:
    """Load and assemble everything the analyses run on."""
    con = data.connect(db_path)
    try:
        requested = data.read_universe_file()
        tickers = data.resolve_universe(con, requested, config.STUDY_START)
        _log(f"universe: {len(tickers)} of {len(requested)} tickers in "
             f"tickers.txt have price history from {config.STUDY_START}", t0)

        tags = data.load_tags(con, tickers)
        # A handful of tickers have price history but no fundamentals row.
        # They stay in the sample as their own group rather than dropping out
        # of every sector-controlled regression as a silent NaN.
        missing = sorted(set(tickers) - set(tags["ticker"]))
        if missing:
            tags = pd.concat(
                [tags, pd.DataFrame({"ticker": missing, "sector": "Unknown",
                                     "industry": "", "group": "Unknown",
                                     "market_cap": pd.NA})],
                ignore_index=True,
            )
        _log(f"sector tags: {len(tags)} tickers, {tags['group'].nunique()} groups "
             f"({len(missing)} without fundamentals)", t0)

        px = data.load_prices(con, tickers, config.WARMUP_START, config.STUDY_END)
        _log(f"price rows: {len(px):,}", t0)

        earn = data.load_earnings(con, tickers, config.WARMUP_START,
                                  config.STUDY_END)
        _log(f"earnings rows from DuckDB: {len(earn):,} "
             f"({earn['ticker'].nunique()} tickers)", t0)
    finally:
        con.close()

    # `earnings_history` starts in 2023, which leaves the 2024H1 window with no
    # year-on-year figure to condition on. The 2022 backfill closes that gap;
    # it is read from the staging file so the database stays read-only.
    staged = data.load_staged_earnings(backfill.STAGING, tickers)
    if not staged.empty:
        before = len(earn)
        earn = data.merge_earnings(earn, staged)
        _log(f"staged 2022 backfill: {len(staged):,} rows available, "
             f"{len(earn) - before:,} new to the panel", t0)
    else:
        _log("staged 2022 backfill: not present — 2024H1 ex-ante growth will "
             "be unavailable (run `-m study.xsec.backfill_2022 --scrape`)", t0)

    mats = technicals.build_matrices(px)
    _log(f"price matrix: {mats['px_total'].shape[0]} sessions x "
         f"{mats['px_total'].shape[1]} tickers", t0)

    feats_daily = technicals.daily_features(mats)
    _log(f"daily technical features: {len(feats_daily)} matrices", t0)

    eps_col = "eps_act" if eps_basis == "nongaap" else "eps_gaap_act"
    feat = features.build(earn, mats, eps_col=eps_col)
    _log(f"fundamental features on {eps_basis} basis: "
         f"{feat['rev_yoy'].notna().sum():,} rev-YoY, "
         f"{feat['eps_yoy'].notna().sum():,} eps-YoY, "
         f"{feat['sue'].notna().sum():,} SUE", t0)

    events = technicals.forward_returns(mats, feat, config.DRIFT_HORIZONS)
    _log(f"earnings events with forward returns: {len(events):,}", t0)

    win_stats = {}
    for w in config.ALL_WINDOWS:
        st = technicals.window_stats(mats, w)
        win_stats[w.name] = st
        _log(f"window {w.name}: {len(st)} tickers with usable coverage", t0)

    return {
        "tags": tags, "mats": mats, "feats_daily": feats_daily,
        "feat": feat, "events": events, "win_stats": win_stats,
    }


def run(db_path: str | None, out_dir: Path, eps_basis: str) -> dict[str, pd.DataFrame]:
    t0 = time.time()
    out_dir.mkdir(parents=True, exist_ok=True)
    p = build_panels(db_path, eps_basis, t0)
    tags, feat, events = p["tags"], p["feat"], p["events"]

    tables: dict[str, pd.DataFrame] = {}

    ret_ticker, ret_group = analyses.group_returns(p["win_stats"], tags)
    tables["returns_by_ticker"] = ret_ticker
    tables["returns_by_group"] = ret_group
    _log("1/10 returns by group", t0)

    tables["delivery_by_group"] = analyses.group_delivery(feat, tags)
    _log("2/10 fundamental delivery by group", t0)

    fits, coefs = analyses.explanatory_power(p["win_stats"], p["feats_daily"],
                                             feat, tags)
    tables["explanatory_power"] = fits
    tables["regression_coefficients"] = coefs
    _log("3/10 nested explanatory-power regressions", t0)

    tables["rank_correlations"] = analyses.rank_correlations(p["win_stats"], feat,
                                                             tags)
    _log("4/10 rank correlations", t0)

    tables["reaction_slopes"] = analyses.reaction_and_drift(events, tags)
    _log("5/10 announcement reaction and drift slopes", t0)

    tables["drift_portfolios"] = analyses.drift_portfolios(events, tags)
    _log("6/10 surprise-quintile drift portfolios", t0)

    slopes, fm = analyses.fama_macbeth_quarterly(events, tags)
    tables["fm_quarterly_slopes"] = slopes
    tables["fm_summary"] = fm
    _log("7/10 quarterly Fama-MacBeth", t0)

    pe_ticker, pe_group = analyses.pe_decomposition(p["mats"], feat, tags)
    tables["pe_decomp_by_ticker"] = pe_ticker
    tables["pe_decomp_by_group"] = pe_group
    tables["group_index"] = analyses.group_index(p["mats"], tags)
    _log("8/10 P/E decomposition and group index", t0)

    tables["sector_level_link"] = analyses.sector_level_link(
        ret_group, tables["delivery_by_group"]
    )
    _log("9/10 sector-level delivery-vs-return link", t0)

    tables["delivery_vs_price"] = analyses.delivery_vs_price(
        ret_group, tables["delivery_by_group"], pe_group
    )
    _log("10/10 combined delivery-vs-price table", t0)

    for name, df in tables.items():
        path = out_dir / f"{name}.csv"
        df.to_csv(path, index=False)
        print(f"[study] wrote {path.name:32s} {len(df):>7,} rows")

    return tables


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Cross-sectional study: do fundamentals explain price action?"
    )
    ap.add_argument("--db", help="DuckDB path (default: the project store, or $STUDY_DB)")
    ap.add_argument("--out", default=str(config.OUT_DIR), help="Output directory")
    ap.add_argument("--eps-basis", choices=["nongaap", "gaap"], default="nongaap",
                    help="Earnings basis for growth and P/E. Default non-GAAP, "
                         "which is the basis the estimates and surprises use.")
    args = ap.parse_args()
    run(args.db, Path(args.out), args.eps_basis)


if __name__ == "__main__":
    main()
