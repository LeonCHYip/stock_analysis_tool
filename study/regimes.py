"""Regime windows for the 2024–2026 rotation study.

Defined as data rather than literals scattered through the analyses so the
same windows can be re-used when the study is extended to other sectors.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Regime:
    name: str
    start: str          # inclusive, YYYY-MM-DD
    end: str            # inclusive, YYYY-MM-DD
    label: str


REGIMES: list[Regime] = [
    Regime("R0_BASE",   "2024-01-02", "2024-06-28", "Pre-rotation baseline"),
    Regime("R1_ROTATE", "2024-07-01", "2025-04-02", "Rotation out of semis"),
    Regime("R2_SHOCK",  "2025-04-03", "2025-04-08", "Liberation Day drawdown"),
    Regime("R3_AFTER",  "2025-04-09", "2026-09-02", "Post-shock recovery"),
]

BY_NAME = {r.name: r for r in REGIMES}

STUDY_START = REGIMES[0].start
STUDY_END   = REGIMES[-1].end
