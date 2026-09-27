"""Windows, universe definition and tuning constants for the cross-sectional study.

Kept as data rather than literals inside the analyses so a re-cut of the
regimes (or a different universe file) needs no change to the statistics.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DB_PATH = ROOT / "stock_analysis_v2.duckdb"
UNIVERSE_FILE = ROOT / "tickers.txt"
OUT_DIR = ROOT / "study" / "out_xsec"

# Trailing sums need four quarters behind the first study date and the 12-1
# momentum control needs a year of prices, so both panels start well before
# the study window. It reaches back to the start of 2022 so that the prices
# cover the 2022 earnings backfill: a report with no price behind it has no
# share basis to check against.
WARMUP_START = "2021-12-01"
STUDY_START = "2024-01-02"
STUDY_END = "2026-09-03"


@dataclass(frozen=True)
class Regime:
    name: str
    start: str  # inclusive
    end: str    # inclusive
    label: str


# The user's framing is one long "rotation" phase running from 2024H2 to
# Liberation Day. It is split at the turn of the year because the drivers were
# different either side of it (rate-cut rotation, then the tariff run-up), and
# a single window would average the two into something neither of them was.
REGIMES: list[Regime] = [
    Regime("R0_AI_MELTUP", "2024-01-02", "2024-06-28", "2024H1 AI melt-up"),
    Regime("R1_ROTATION",  "2024-07-01", "2024-12-31", "2024H2 rotation out of semis"),
    Regime("R2_PRE_LIB",   "2025-01-02", "2025-04-02", "2025 run-up to Liberation Day"),
    Regime("R3_SHOCK",     "2025-04-03", "2025-04-08", "Liberation Day drawdown"),
    Regime("R4_RECOVERY",  "2025-04-09", "2026-09-03", "Post-tariff recovery"),
]

# The composite window the user asked about, kept separately so it can be
# reported alongside the pieces it decomposes into.
SPAN_ROTATION = Regime(
    "S_ROTATION", "2024-07-01", "2025-04-02", "2024H2 → Liberation Day (composite)"
)

ALL_WINDOWS: list[Regime] = REGIMES + [SPAN_ROTATION]

# R3_SHOCK is four sessions long, so the per-window minimum has to be shorter
# than a trading week.
MIN_WINDOW_DAYS = 3

# Cross-sectional statistics are run only where the sample can support them.
MIN_XSEC_OBS = 30
MIN_GROUP_OBS = 8

# Surprise percentages have unbounded tails (a cent against a zero estimate
# reads as a four-digit beat), so every regression input is winsorised.
WINSOR_LO, WINSOR_HI = 0.01, 0.99

# Forward horizons for the post-earnings drift work, in trading days.
DRIFT_HORIZONS = (1, 5, 21, 63)

# "Semiconductors" is an industry, not a sector, and it is the group the study
# is really about — so it is promoted to a group of its own and removed from
# Technology, which would otherwise mask it.
SEMI_INDUSTRY_MATCH = "semiconductor"
SEMI_GROUP = "Semiconductors"
