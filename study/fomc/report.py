"""Build fomc_study.html by embedding out/*.csv results into report_template.html."""
import json
from pathlib import Path

import pandas as pd

HERE = Path(__file__).parent
O = HERE / "out"
START10, START3, END = pd.Timestamp("2016-01-01"), pd.Timestamp("2023-09-15"), pd.Timestamp("2026-09-14")
WINS = {"10y": START10, "3y": START3}


def rec(df, digits=4):
    df = df.copy()
    for c in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[c]):
            df[c] = df[c].dt.strftime("%Y-%m-%d")
    return json.loads(df.to_json(orient="records", double_precision=digits))


F = pd.read_csv(O / "meeting_features.csv", parse_dates=["date"])
S = F[F.scheduled]

# average / median path around the meeting, by decision type, plus an any-day baseline
P = pd.read_csv(O / "paths.csv", parse_dates=["date"]).merge(F[["date", "decision", "scheduled"]], on="date")
P = P[P.scheduled]
parts = []
for wl, ws in WINS.items():
    pw = P[P.date >= ws]
    for grp in ["all", "hike", "cut", "hold"]:
        g = pw if grp == "all" else pw[pw.decision == grp]
        if g.empty:
            continue
        a = g.groupby(["asset", "k"]).ret.agg(["mean", "median", "count"]).reset_index()
        a["window"], a["group"] = wl, grp
        parts.append(a)
close = pd.read_csv(HERE / "prices_daily.csv", parse_dates=["date"]).pivot(index="date", columns="ticker", values="close")
close = close[close["MU"].notna()]
for wl, ws in WINS.items():
    days = (close.index >= ws) & (close.index <= END)
    for t in ["SOXL", "MU", "QQQ"]:
        for k in range(-10, 22):
            v = ((close[t].shift(-k) / close[t].shift(1) - 1) * 100)[days].dropna()
            parts.append(pd.DataFrame([dict(asset=t, k=k, mean=v.mean(), median=v.median(), count=len(v),
                                            window=wl, group="baseline")]))
paths = pd.concat(parts, ignore_index=True)

MCOLS = ["date", "scheduled", "decision", "move_bp", "upper_after", "sep", "phase", "d2y_bp", "d10y_bp", "tone_mkt",
         "tone_hand", "priced", "decision_surprise", "summary", "same_day_events", "SOXL_pre5", "SOXL_pre10",
         "SOXL_day_before", "SOXL_morning", "SOXL_reaction", "SOXL_fomc_day", "SOXL_post1", "SOXL_post5", "SOXL_post21",
         "MU_pre5", "MU_morning", "MU_reaction", "MU_fomc_day", "MU_post1", "MU_post5", "MU_post21", "SOXL_vs200",
         "SOXL_dd252", "vix_prev", "MU_earn_gap_days", "core_pce_yoy", "window3y"]

data = dict(
    meetings=rec(F[MCOLS]),
    paths=rec(paths),
    drift=rec(pd.read_csv(O / "drift.csv")),
    groups=rec(pd.read_csv(O / "group_stats.csv")),
    corr=rec(pd.read_csv(O / "factor_corr.csv")),
    hikes=rec(pd.read_csv(O / "analog_hikes.csv", parse_dates=["date"])),
    buckets=rec(pd.read_csv(O / "scenario_buckets.csv")),
    intraday=rec(pd.read_csv(O / "intraday_summary.csv").rename(columns={"Unnamed: 0": "sample"})),
    current=json.loads((O / "current_state.json").read_text()),
)
tpl = (HERE / "report_template.html").read_text()
out = HERE / "fomc_study.html"
out.write_text(tpl.replace("/*__DATA__*/null", json.dumps(data, allow_nan=False)))
print(f"wrote {out} ({out.stat().st_size / 1024:.0f} KB)")
