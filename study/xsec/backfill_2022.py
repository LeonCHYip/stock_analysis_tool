"""Backfill 2022 earnings from Finviz so year-on-year growth exists in 2023.

Why this is needed: `earnings_history` starts on 2023-01-03. Year-on-year
growth is a lag-4 comparison, so the earliest report that can carry a YoY
figure is one filed in 2024 — and the *acceleration* of that growth, a lag-5
quantity, only appears in mid-2024. That leaves the 2024H1 window with no
ex-ante fundamentals at all. Adding calendar 2022 pushes YoY back to 2023 and
acceleration to late 2023, which is what the earliest window needs.

Two phases, deliberately separate:

    # 1. scrape to a staging file — no database access at all
    uv run python -m study.xsec.backfill_2022 --scrape

    # 2. insert the staged rows (needs the DuckDB write lock, so the
    #    Streamlit app has to be closed first)
    uv run python -m study.xsec.backfill_2022 --load

Splitting them means the long network job never holds the database open, and
the load step is a few seconds of exclusive access rather than half an hour.

The load only ever INSERTs rows whose (ticker, earnings_date) is absent. It
adds no columns and alters no schema; existing rows are never touched.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
from datetime import date
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import earnings_fetcher as ef  # noqa: E402  (path set above)

STAGING = Path(__file__).resolve().parent.parent / "out_xsec" / "finviz_2022.json"

DEFAULT_START = "2022-01-01"
DEFAULT_END = "2022-12-31"

# Finviz tolerates the daily fetcher's pacing; this is the same courtesy delay
# applied between dates rather than between pages.
DELAY_BETWEEN_DATES = (1.0, 2.5)


def scrape(start: str, end: str, out_path: Path) -> int:
    """Fetch every trading day in the range into one staging JSON file.

    Resumable: dates already present in the staging file are skipped, so an
    interrupted run can simply be restarted.
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    staged: dict[str, dict] = {}
    if out_path.exists():
        staged = json.loads(out_path.read_text())
        print(f"[backfill] resuming — {len(staged)} dates already staged")

    days = ef._get_trading_days(date.fromisoformat(start), date.fromisoformat(end))
    todo = [d for d in days if d not in staged]
    print(f"[backfill] {len(days)} trading days in range, {len(todo)} to fetch")

    session = requests.Session()
    for i, day in enumerate(todo, 1):
        try:
            staged[day] = ef.fetch_earnings_for_date(session, day)
        except Exception as exc:  # a single bad day must not lose the run
            print(f"  {day}: {exc}")
            continue
        if i % 10 == 0 or i == len(todo):
            out_path.write_text(json.dumps(staged))
            print(f"[backfill] {i}/{len(todo)} days — staged to {out_path.name}")
        time.sleep(random.uniform(*DELAY_BETWEEN_DATES))

    out_path.write_text(json.dumps(staged))
    total = sum(len(v) for v in staged.values())
    print(f"[backfill] done: {len(staged)} dates, {total:,} report rows staged")
    return total


def load(in_path: Path, dry_run: bool = False, db_path: str | None = None) -> int:
    """Insert staged rows that are not already in `earnings_history`.

    Needs the DuckDB write lock. Nothing existing is modified: a report the
    table already holds is left exactly as it is.
    """
    import duckdb

    from study.xsec import config

    staged = json.loads(in_path.read_text())
    rows = []
    for day, per_ticker in staged.items():
        for ticker, fields in per_ticker.items():
            rows.append({"ticker": ticker, **fields})
    print(f"[backfill] {len(rows):,} staged rows from {len(staged)} dates")
    if not rows:
        return 0

    cols = [
        "ticker", "earnings_date", "earnings_time",
        "eps_est", "eps_act", "eps_sur",
        "eps_gaap_est", "eps_gaap_act", "eps_gaap_sur",
        "rev_est_m", "rev_act_m", "rev_sur", "one_day_change",
    ]
    target = db_path or os.environ.get("STUDY_DB") or str(config.DB_PATH)
    print(f"[backfill] target: {target}{' (dry run)' if dry_run else ''}")
    con = duckdb.connect(target, read_only=dry_run)
    try:
        con.execute(
            "CREATE TEMP TABLE staged_2022 (" +
            ", ".join(
                f"{c} " + ("VARCHAR" if c in ("ticker", "earnings_date",
                                              "earnings_time") else "DOUBLE")
                for c in cols
            ) + ")"
        )
        con.executemany(
            f"INSERT INTO staged_2022 VALUES ({', '.join('?' * len(cols))})",
            [[r.get(c) for c in cols] for r in rows],
        )
        new = con.execute(
            """
            SELECT count(*) FROM staged_2022 s
            WHERE NOT EXISTS (
                SELECT 1 FROM earnings_history e
                WHERE e.ticker = s.ticker AND e.earnings_date = s.earnings_date
            )
            """
        ).fetchone()[0]
        print(f"[backfill] {new:,} rows are new to earnings_history")
        if dry_run:
            print("[backfill] dry run — nothing written")
            return new

        con.execute(
            f"""
            INSERT INTO earnings_history ({', '.join(cols)}, fetch_date)
            SELECT {', '.join('s.' + c for c in cols)}, ?
            FROM staged_2022 s
            WHERE NOT EXISTS (
                SELECT 1 FROM earnings_history e
                WHERE e.ticker = s.ticker AND e.earnings_date = s.earnings_date
            )
            """,
            [date.today().isoformat()],
        )
        print(f"[backfill] inserted {new:,} rows")
        return new
    finally:
        con.close()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--scrape", action="store_true", help="Fetch Finviz to staging")
    ap.add_argument("--load", action="store_true", help="Insert staged rows")
    ap.add_argument("--dry-run", action="store_true",
                    help="With --load: count new rows without writing")
    ap.add_argument("--start", default=DEFAULT_START)
    ap.add_argument("--end", default=DEFAULT_END)
    ap.add_argument("--file", default=str(STAGING))
    ap.add_argument("--db", help="DuckDB path (default: the project store, or $STUDY_DB)")
    args = ap.parse_args()

    path = Path(args.file)
    if args.scrape:
        scrape(args.start, args.end, path)
    if args.load:
        load(path, dry_run=args.dry_run, db_path=args.db)
    if not (args.scrape or args.load):
        ap.error("choose --scrape and/or --load")


if __name__ == "__main__":
    main()
