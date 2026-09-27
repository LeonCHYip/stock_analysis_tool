"""Build soxl_drop_study.html by embedding the out/*.csv results into report_template.html."""
import json

import pandas as pd

from common import HERE, START, END, load

O = HERE / "out"
WIN = {"3y (2023-09 → 2026-09)": "3y", "pre-window (2010 → 2023-09)": "2010-2023"}


def rec(df, digits=4):
    return json.loads(df.to_json(orient="records", double_precision=digits))


es = pd.read_csv(O / "event_study.csv")
es["window"] = es.window.map(WIN)
es = es[es.h.isin([1, 5, 21, 63])][["window", "asset", "threshold", "sample", "h", "n", "mean", "median",
                                     "hit", "base_mean", "base_median", "base_hit", "boot_pct"]]
ev = pd.read_csv(O / "events_10pct_context.csv")[[
    "date", "SOXL_1d", "MU_1d", "QQQ_1d", "VIX_1d", "SOXL_f1", "SOXL_f5", "SOXL_f21", "MU_f5", "MU_f21",
    "SOXL_mae21", "MU_mae21", "above200", "first_in_cluster", "dd252", "earn_names", "category", "catalyst",
    "confidence"]]
eq = pd.read_csv(O / "equity_3y.csv", index_col=0)
close, _ = load()
px = close.loc[START:END, ["SOXL", "MU"]]

data = dict(
    fwd=rec(es),
    backtest=rec(pd.read_csv(O / "backtest.csv")),
    placebo=rec(pd.read_csv(O / "placebo.csv")),
    events=rec(ev),
    buckets=rec(pd.read_csv(O / "buckets.csv")),
    risk=rec(pd.read_csv(O / "risk.csv")),
    years=rec(pd.read_csv(O / "by_year.csv")),
    equity=dict(dates=[str(d)[:10] for d in eq.index],
                series={c: [round(float(v), 4) for v in eq[c]] for c in eq.columns}),
    price=dict(dates=[d.strftime("%Y-%m-%d") for d in px.index],
               SOXL=px.SOXL.round(3).tolist(), MU=px.MU.round(3).tolist()),
)
tpl = (HERE / "report_template.html").read_text()
out = HERE / "soxl_drop_study.html"
out.write_text(tpl.replace("/*__DATA__*/null", json.dumps(data, allow_nan=False)))
print(f"wrote {out} ({out.stat().st_size / 1024:.0f} KB)")
