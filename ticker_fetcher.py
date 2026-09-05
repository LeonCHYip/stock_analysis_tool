"""
ticker_fetcher.py — Fetch the current US-listed symbol universe from the
official NASDAQ Trader symbol directory and split it into two lists:

  * tickers_common.txt  — common stocks / ADRs (the scan-universe superset)
  * etfs.txt            — ETFs, ETNs, and closed-end funds / BDCs

Source files (Nasdaq refreshes them nightly):
  https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt
  https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt

Why a separate common-stock file (not tickers.txt): the goal is to never miss
a >=$2B name, so we capture the FULL common-stock universe. It is written to
tickers_common.txt — deliberately kept apart from the in-use tickers.txt so the
two can be diffed/compared before adoption. A market-cap >=$2B daily-scan
subset is a separate, later step (the raw files carry no market-cap data).

Classification (case-insensitive on the security name; symbol shape as backup):
  drop   — warrants / units / rights, preferred ($ symbol or "Preferred"),
           exchange-traded debt (senior/subordinated notes, debentures)
  fund   — ETF flag = Y, or ETN, or "Closed End Fund" (CEFs / BDCs)
  common — everything else (common stock, ordinary shares, ADRs, class shares)

Symbols are normalised to yfinance form: class-share dots become dashes
(BRK.B -> BRK-B), matching the existing tickers.txt convention (e.g. AKO-B).

CLI:
  uv run python ticker_fetcher.py            # fetch + write both lists (+ diff)
  uv run python ticker_fetcher.py --dry-run  # fetch + print counts/diff only
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import requests

# ── Source URLs ──────────────────────────────────────────────────────────────
NASDAQ_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
OTHER_LISTED_URL  = "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"

# ── Output files (kept separate from the in-use tickers.txt) ─────────────────
_DIR             = Path(__file__).parent
COMMON_FILE      = _DIR / "tickers_common.txt"
ETF_FILE         = _DIR / "etfs.txt"

_HTTP_TIMEOUT = 30
_HEADERS = {"User-Agent": "Mozilla/5.0 (stock_analysis_tool ticker_fetcher)"}

# ── Classification patterns ──────────────────────────────────────────────────
# Warrants and rights are always derivatives. "Units" is ambiguous — a SPAC
# unit (a bundle) is a derivative to drop, but an MLP/LP "Common Unit" is the
# actual tradable >=$2B security to KEEP (BEP, ET, MPLX, PAA, IEP, ...), so it
# is handled separately via _LP_UNIT below.
_WARR_RIGHT = re.compile(r"\b(warrants?|rights?)\b", re.I)
_UNIT       = re.compile(r"\bunits?\b", re.I)
# LP/MLP tradable units (keep as common): distinguished from SPAC "- Units".
# The decisive signal is the L.P./LP legal entity — SPAC units are always
# "Corp/Inc/Co", never "L.P." — so an "...L.P. ... Units" name (e.g. AB,
# AllianceBernstein Holding L.P. Units) is kept while "Acquisition Corp. -
# Units" is dropped. \bl\.?p\.?\b matches "L.P."/"LP" as a word, not the "lp"
# inside "Alpex".
_LP_UNIT = re.compile(
    r"common units|limited partnership units|limited partner|depositary units"
    r"|\bl\.?p\.?\b",
    re.I,
)
# Foreign-equity ADR/ADS lines. These trade like common stock, so they are
# kept as common regardless of the underlying-share wording — e.g. AMX ("...the
# RIGHT to receive twenty Series B Shares"), CIB/AVAL ("...representing N
# PREFERRED shares") — which would otherwise trip the right/preferred filters.
# Note "American Depositary" specifically: a US preferred line says only
# "Depositary Shares ... Preferred" (no "American"), so it still drops.
_ADR = re.compile(
    r"american deposit[ao]ry|\bADRs?\b|\bADSs?\b|new york registry",
    re.I,
)
_PFD = re.compile(r"\bpreferred\b", re.I)
_ETN = re.compile(r"\b(etn|exchange[- ]traded note)\b", re.I)
_CEF = re.compile(r"closed[- ]end fund", re.I)
# Exchange-traded debt (baby bonds): "... Notes due 20xx", senior/subordinated
# notes, debentures. These are debt instruments, not scannable common stock.
_DEBT = re.compile(
    r"\bnotes?\s+due\b"
    r"|\b(senior|subordinated|junior)\b.*\b(notes?|debentures?)\b"
    r"|\bdebentures?\b",
    re.I,
)
# Dotted symbol suffixes that denote a warrant/right (e.g. ACHR.W). ".U"/".UN"
# units are handled in classify() so LP common units aren't wrongly dropped.
_DOT_WARR_RIGHT = {"W", "R", "WS", "RT", "WD", "RW"}
_DOT_UNIT       = {"U", "UN"}


def classify(symbol: str, name: str, is_etf: str) -> str:
    """Return 'common', 'fund', or 'drop' for one directory row.

    is_etf is the raw ETF-column value ('Y'/'N'). Order matters:
      1. symbol-suffix derivatives (unambiguous) are dropped first;
      2. fund-like (ETF flag / ETN / CEF) wins over the ADR heuristic, so an
         ETF merely named "...ADR ETF" (e.g. AADR) is a fund, not common;
      3. foreign ADR/ADS lines are then treated as common, bypassing the
         underlying-share wording that would trip the preferred/right filters;
      4. remaining name-based derivatives / preferred / debt are dropped.
    """
    dot_suffix = symbol.rsplit(".", 1)[-1].upper() if "." in symbol else ""
    # 1. Symbol-suffix derivatives (dotted warrant/right/unit). LP common units
    #    never use these suffixes, so an "".U"" unit here is a SPAC bundle.
    if dot_suffix in _DOT_WARR_RIGHT:
        return "drop"
    if dot_suffix in _DOT_UNIT and not _LP_UNIT.search(name):
        return "drop"
    # 2. Fund-like takes precedence over the ADR heuristic below.
    if is_etf.strip().upper() == "Y" or _ETN.search(name) or _CEF.search(name):
        return "fund"
    # 3. Foreign-equity ADR/ADS: common regardless of underlying-share wording.
    if _ADR.search(name):
        return "common"
    # 4. Name-based derivatives / preferred / debt.
    if _WARR_RIGHT.search(name):
        return "drop"
    if _UNIT.search(name) and not _LP_UNIT.search(name):
        return "drop"
    if "$" in symbol or _PFD.search(name):
        return "drop"
    if _DEBT.search(name):
        return "drop"
    return "common"


def _to_yf(symbol: str) -> str:
    """Normalise a directory symbol to yfinance form (class-share dot -> dash)."""
    return symbol.strip().replace(".", "-")


def _download(url: str) -> list[str]:
    """Download a pipe-delimited directory file and return its data lines
    (header and the trailing 'File Creation Time' footer removed)."""
    resp = requests.get(url, headers=_HEADERS, timeout=_HTTP_TIMEOUT)
    resp.raise_for_status()
    lines = resp.text.splitlines()
    if not lines:
        raise ValueError(f"Empty response from {url}")
    # Drop header (first line) and the footer creation-time line.
    return [ln for ln in lines[1:] if ln and not ln.startswith("File Creation Time")]


def _parse(lines: list[str], sym_i: int, name_i: int, etf_i: int, test_i: int):
    """Yield (symbol, name, etf_flag) for non-test rows of a directory file."""
    need = max(sym_i, name_i, etf_i, test_i)
    for ln in lines:
        parts = ln.split("|")
        if len(parts) <= need:
            continue
        if parts[test_i].strip().upper() == "Y":   # skip test issues
            continue
        yield parts[sym_i].strip(), parts[name_i].strip(), parts[etf_i].strip()


def fetch_symbol_directory() -> tuple[list[str], list[str], dict]:
    """Fetch and classify the full US-listed symbol universe.

    Returns (common_symbols, fund_symbols, meta) where both lists are sorted,
    de-duplicated, and in yfinance form. meta carries per-bucket counts for
    logging / UI display. Raises on any download or parse failure.
    """
    # Column layouts differ between the two files:
    #  nasdaqlisted: Symbol|Name|MktCat|Test|FinStatus|RoundLot|ETF|NextShares
    #  otherlisted:  ACTSym|Name|Exch|CQSSym|ETF|RoundLot|Test|NASDAQSym
    nq = list(_parse(_download(NASDAQ_LISTED_URL), sym_i=0, name_i=1, etf_i=6, test_i=3))
    ot = list(_parse(_download(OTHER_LISTED_URL),  sym_i=0, name_i=1, etf_i=4, test_i=6))

    common: set[str] = set()
    funds: set[str] = set()
    dropped = 0
    for symbol, name, etf in nq + ot:
        if not symbol:
            continue
        kind = classify(symbol, name, etf)
        if kind == "common":
            common.add(_to_yf(symbol))
        elif kind == "fund":
            funds.add(_to_yf(symbol))
        else:
            dropped += 1

    # A symbol should never land in both buckets, but if it does, common wins
    # (a genuine equity mislabelled as a fund is the safer error for a screener).
    funds -= common

    meta = {
        "n_common": len(common),
        "n_funds": len(funds),
        "n_dropped": dropped,
        "n_raw": len(nq) + len(ot),
    }
    return sorted(common), sorted(funds), meta


def _read_existing(path: Path) -> set[str]:
    """Read a comma-separated ticker file into a set (empty set if absent)."""
    if not path.exists():
        return set()
    return {t.strip() for t in path.read_text().split(",") if t.strip()}


def _diff(new: list[str], path: Path) -> dict:
    """Compare a new symbol list against the file currently at `path`."""
    old = _read_existing(path)
    new_set = set(new)
    return {
        "added": sorted(new_set - old),
        "removed": sorted(old - new_set),
        "n_old": len(old),
        "n_new": len(new_set),
    }


def _write(symbols: list[str], path: Path) -> None:
    """Write symbols as a single comma-separated line (matches tickers.txt)."""
    path.write_text(",".join(symbols))


def refresh_ticker_universe(
    common_file: Path = COMMON_FILE,
    etf_file: Path = ETF_FILE,
    write: bool = True,
) -> dict:
    """Fetch the universe and (optionally) write the two list files.

    Callable from the CLI and from the Streamlit button. Returns a result dict
    with per-list diffs and counts (also computed in dry-run mode so the UI can
    preview changes before writing).
    """
    common, funds, meta = fetch_symbol_directory()
    result = {
        "meta": meta,
        "common": {"file": str(common_file), **_diff(common, common_file)},
        "etf": {"file": str(etf_file), **_diff(funds, etf_file)},
        "written": write,
    }
    if write:
        _write(common, common_file)
        _write(funds, etf_file)
    return result


def _print_report(result: dict) -> None:
    m = result["meta"]
    print(f"Fetched {m['n_raw']} rows -> "
          f"{m['n_common']} common, {m['n_funds']} funds, {m['n_dropped']} dropped")
    for key, label in (("common", "Common stocks"), ("etf", "ETFs/ETNs/CEFs")):
        d = result[key]
        print(f"\n{label} -> {d['file']}")
        print(f"  current {d['n_old']}  ->  new {d['n_new']}  "
              f"(+{len(d['added'])} / -{len(d['removed'])})")
        if d["added"]:
            print(f"  added:   {', '.join(d['added'][:20])}"
                  f"{' …' if len(d['added']) > 20 else ''}")
        if d["removed"]:
            print(f"  removed: {', '.join(d['removed'][:20])}"
                  f"{' …' if len(d['removed']) > 20 else ''}")
    print("\n" + ("Wrote files." if result["written"] else "Dry run — no files written."))


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true",
                    help="Fetch and print counts/diff without writing files.")
    ap.add_argument("--common-file", type=Path, default=COMMON_FILE,
                    help=f"Output path for common stocks (default: {COMMON_FILE.name}).")
    ap.add_argument("--etf-file", type=Path, default=ETF_FILE,
                    help=f"Output path for ETFs/ETNs/CEFs (default: {ETF_FILE.name}).")
    args = ap.parse_args(argv)

    try:
        result = refresh_ticker_universe(
            common_file=args.common_file, etf_file=args.etf_file,
            write=not args.dry_run,
        )
    except Exception as e:  # network / parse failure
        print(f"ERROR: ticker fetch failed: {e}", file=sys.stderr)
        return 1
    _print_report(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
