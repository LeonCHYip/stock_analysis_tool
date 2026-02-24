"""
main.py
-------
CLI entrypoint for the stock analysis tool.

Usage:
    uv run python main.py
    uv run python main.py --tickers AAPL,TSLA,MU
"""

import argparse
import sys
from datetime import datetime

from data_fetcher import fetch_technical, fetch_fundamental
from peers_fetcher import get_peer_valuations
from indicators import evaluate_all
from db import init_db, save_results
from reporter import print_summary_table, print_detail_table


def parse_tickers(raw: str) -> list[str]:
    return [t.strip().upper() for t in raw.split(",") if t.strip()]


def run(tickers: list[str], collapsed_tickers: set | None = None, collapsed_indicators: set | None = None):
    init_db()
    analysis_dt = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    all_results: dict[str, dict] = {}

    print(f"\n{'='*60}")
    print(f"  Stock Analysis Tool  |  {analysis_dt} UTC")
    print(f"  Tickers: {', '.join(tickers)}")
    print(f"{'='*60}\n")

    for ticker in tickers:
        print(f"[{ticker}] Fetching technical data...")
        tech = fetch_technical(ticker)
        if "error" in tech:
            print(f"  WARNING: Technical error: {tech['error']}")
            tech = {}

        print(f"[{ticker}] Fetching fundamental data...")
        fund = fetch_fundamental(ticker)
        if "error" in fund:
            print(f"  WARNING: Fundamental error: {fund['error']}")
            fund = {}

        print(f"[{ticker}] Fetching peer valuations (FMP + Yahoo)...")
        peer_data = get_peer_valuations(ticker)
        n_peers = len(peer_data.get("peers", []))
        print(f"  -> Found {n_peers} peers | PE data: {len(peer_data.get('peer_forward_pe_values',[]))} | P/B data: {len(peer_data.get('peer_pb_values',[]))}")

        print(f"[{ticker}] Evaluating indicators...")
        indicators = evaluate_all(ticker, tech, fund, peer_data)

        save_results(ticker, indicators, analysis_dt)
        all_results[ticker] = indicators
        print(f"[{ticker}] Done\n")

    # Output
    print_summary_table(all_results)
    print_detail_table(all_results, collapsed_tickers=collapsed_tickers, collapsed_indicators=collapsed_indicators)

    from pathlib import Path
    from db import DB_PATH
    print(f"\nResults saved to: {DB_PATH.resolve()}")


def main():
    parser = argparse.ArgumentParser(description="Stock Analysis Tool")
    parser.add_argument(
        "--tickers", "-t",
        type=str,
        default=None,
        help="Comma-separated list of tickers, e.g. AAPL,TSLA,MU"
    )
    parser.add_argument(
        "--collapse-tickers",
        type=str,
        default=None,
        help="Tickers to collapse in detail view (header only), e.g. AAPL,TSLA"
    )
    parser.add_argument(
        "--collapse-indicators",
        type=str,
        default=None,
        help="Indicator codes to collapse in detail view (header only), e.g. T1,T2,F5,F6"
    )
    args = parser.parse_args()

    if args.tickers:
        tickers = parse_tickers(args.tickers)
    else:
        raw = input("Enter tickers (comma-separated), e.g. AAPL, TSLA, MU: ").strip()
        tickers = parse_tickers(raw)

    if not tickers:
        print("No valid tickers provided. Exiting.")
        sys.exit(1)

    collapsed_tickers = (
        {t.strip().upper() for t in args.collapse_tickers.split(",") if t.strip()}
        if args.collapse_tickers else set()
    )
    collapsed_indicators = (
        {i.strip().upper() for i in args.collapse_indicators.split(",") if i.strip()}
        if args.collapse_indicators else set()
    )

    run(tickers, collapsed_tickers=collapsed_tickers, collapsed_indicators=collapsed_indicators)


if __name__ == "__main__":
    main()
