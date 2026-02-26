"""
migrate.py — One-time migration of SQLite data → DuckDB.

MIGRATION ALREADY COMPLETED (Feb 2026): 9486 runs, 94860 detail rows migrated.
This script is intentionally disabled. Do not re-run.
"""

import sys
print("Migration already completed. This script is disabled.")
sys.exit(0)

from __future__ import annotations
import json
import sqlite3
from pathlib import Path

import storage

SQLITE_PATH = Path(__file__).parent / "stock_analysis.db"


def migrate() -> None:
    if not SQLITE_PATH.exists():
        print(f"SQLite DB not found: {SQLITE_PATH}")
        return

    print(f"Migrating {SQLITE_PATH} → {storage.DB_PATH}")

    # ── Ensure DuckDB schema exists ───────────────────────────────────────────
    storage.init_db()

    # ── Connect to SQLite ─────────────────────────────────────────────────────
    sqlite_con = sqlite3.connect(SQLITE_PATH)
    sqlite_con.row_factory = sqlite3.Row

    # ── Migrate indicator_summary → analysis_runs ─────────────────────────────
    try:
        rows = sqlite_con.execute(
            "SELECT * FROM indicator_summary ORDER BY analysis_datetime, ticker"
        ).fetchall()
    except sqlite3.OperationalError as e:
        print(f"Could not read indicator_summary: {e}")
        rows = []

    duck_con = storage._conn()

    summary_cols = (
        storage.MAIN_IND_COLS + storage.ALL_SUB_COLS + ["market_cap", "comments"]
    )

    migrated_runs = 0
    for row in rows:
        row_dict = dict(row)
        # Map analysis_datetime → run_dt
        run_dt = row_dict.get("analysis_datetime", "")
        ticker = row_dict.get("ticker", "")
        if not run_dt or not ticker:
            continue

        vals = [run_dt, ticker]
        for col in summary_cols:
            vals.append(row_dict.get(col))

        col_str = "run_dt, ticker, " + ", ".join(summary_cols)
        placeholders = ", ".join(["?" for _ in vals])

        try:
            duck_con.execute(
                f"INSERT OR IGNORE INTO analysis_runs ({col_str}) VALUES ({placeholders})",
                vals,
            )
            migrated_runs += 1
        except Exception as e:
            print(f"  Skipping run row {run_dt}/{ticker}: {e}")

    print(f"  Migrated {migrated_runs} analysis_runs rows.")

    # ── Migrate indicator_detail → analysis_details ───────────────────────────
    try:
        detail_rows = sqlite_con.execute(
            "SELECT * FROM indicator_detail ORDER BY analysis_datetime, ticker, indicator_id"
        ).fetchall()
    except sqlite3.OperationalError as e:
        print(f"Could not read indicator_detail: {e}")
        detail_rows = []

    migrated_details = 0
    for row in detail_rows:
        row_dict = dict(row)
        run_dt       = row_dict.get("analysis_datetime", "")
        ticker       = row_dict.get("ticker", "")
        indicator_id = row_dict.get("indicator_id", "")
        detail_json  = row_dict.get("detail_json", "{}")

        if not run_dt or not ticker or not indicator_id:
            continue

        try:
            duck_con.execute(
                "INSERT OR REPLACE INTO analysis_details VALUES (?, ?, ?, ?)",
                (run_dt, ticker, indicator_id, detail_json),
            )
            migrated_details += 1
        except Exception as e:
            print(f"  Skipping detail row {run_dt}/{ticker}/{indicator_id}: {e}")

    print(f"  Migrated {migrated_details} analysis_details rows.")

    duck_con.close()
    sqlite_con.close()
    print("Migration complete.")


if __name__ == "__main__":
    migrate()
