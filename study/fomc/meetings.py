"""Build meetings.csv: every FOMC decision 2016 → 2026 with the policy move derived from FRED.

Scheduled dates come from the Fed's FOMC calendar (decision = second day of the meeting;
'*' meetings carry a Summary of Economic Projections / dot plot). The rate change is read
from DFEDTARU (upper bound), which moves on the effective date — the day after the decision.
"""
from pathlib import Path
import pandas as pd

HERE = Path(__file__).parent
SCHEDULED = """
2016-01-27 2016-03-16* 2016-04-27 2016-06-15* 2016-07-27 2016-09-21* 2016-11-02 2016-12-14*
2017-02-01 2017-03-15* 2017-05-03 2017-06-14* 2017-07-26 2017-09-20* 2017-11-01 2017-12-13*
2018-01-31 2018-03-21* 2018-05-02 2018-06-13* 2018-08-01 2018-09-26* 2018-11-08 2018-12-19*
2019-01-30 2019-03-20* 2019-05-01 2019-06-19* 2019-07-31 2019-09-18* 2019-10-30 2019-12-11*
2020-01-29 2020-04-29 2020-06-10* 2020-07-29 2020-09-16* 2020-11-05 2020-12-16*
2021-01-27 2021-03-17* 2021-04-28 2021-06-16* 2021-07-28 2021-09-22* 2021-11-03 2021-12-15*
2022-01-26 2022-03-16* 2022-05-04 2022-06-15* 2022-07-27 2022-09-21* 2022-11-02 2022-12-14*
2023-02-01 2023-03-22* 2023-05-03 2023-06-14* 2023-07-26 2023-09-20* 2023-11-01 2023-12-13*
2024-01-31 2024-03-20* 2024-05-01 2024-06-12* 2024-07-31 2024-09-18* 2024-11-07 2024-12-18*
2025-01-29 2025-03-19* 2025-05-07 2025-06-18* 2025-07-30 2025-09-17* 2025-10-29 2025-12-10*
2026-01-28 2026-03-18* 2026-04-29 2026-06-17* 2026-07-29
"""
# Intermeeting emergency cuts; the Sunday 15 Mar 2020 cut is first traded on Monday 16 Mar.
UNSCHEDULED = {"2020-03-03": "2020-03-03", "2020-03-15": "2020-03-16"}
UPCOMING = "2026-09-16"


def build():
    fred = pd.read_csv(HERE / "fred.csv", parse_dates=["date"])
    up = fred[fred.series == "DFEDTARU"].set_index("date").value.dropna().sort_index()
    rows = []
    for tok in SCHEDULED.split():
        rows.append(dict(announce=tok.rstrip("*"), trade_date=tok.rstrip("*"), sep=tok.endswith("*"), scheduled=True))
    for a, t in UNSCHEDULED.items():
        rows.append(dict(announce=a, trade_date=t, sep=False, scheduled=False))
    m = pd.DataFrame(rows)
    m["announce"] = pd.to_datetime(m.announce)
    m["trade_date"] = pd.to_datetime(m.trade_date)
    m = m.sort_values("announce").reset_index(drop=True)
    before = up.asof(m.announce - pd.Timedelta(days=1))
    after = up.asof(m.announce + pd.Timedelta(days=3))
    m["upper_before"] = before.values
    m["upper_after"] = after.values
    m["move_bp"] = ((m.upper_after - m.upper_before) * 100).round().astype(int)
    m["decision"] = m.move_bp.map(lambda b: "hike" if b > 0 else "cut" if b < 0 else "hold")
    phase, last = [], 0
    for b, u in zip(m.move_bp, m.upper_before):
        if b > 0:
            phase.append("Hiking")
        elif b < 0:
            phase.append("Cutting")
        elif u <= 0.25:
            phase.append("Zero rates")
        else:
            phase.append("Hold after hikes" if last > 0 else "Hold after cuts")
        if b != 0:
            last = b
    m["phase"] = phase
    m.to_csv(HERE / "meetings.csv", index=False)
    return m


if __name__ == "__main__":
    m = build()
    pd.set_option("display.width", 200, "display.max_rows", 200)
    print(m[m.decision != "hold"].to_string(index=False))
    print(m.groupby([m.announce.dt.year, "decision"]).size().unstack(fill_value=0))
    print(m.tail(26)[["announce", "sep", "upper_after", "move_bp", "phase"]].to_string(index=False))
