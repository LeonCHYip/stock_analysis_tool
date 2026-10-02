"""Correlation scan: every technical feature x every post-earnings outcome.

Spearman rho with a permutation p-value, Benjamini-Hochberg q across features per target,
and a stability check: the rho in the early half (2001-2013) and late half (2014-2026)
must share a sign for a feature to be called anything but noise.
"""
import numpy as np, pandas as pd
from features import HERE

rng = np.random.default_rng(7)
df = pd.read_csv(f"{HERE}/panel.csv", parse_dates=["date"])

TARGETS = ["react_1d", "react_ex", "abs_react_atr", "post_5d", "post_5d_ex", "post_21d", "post_21d_ex", "full_5d"]
NON_FEAT = set(TARGETS) | {"date", "report_ts", "eps_est", "eps_act", "eps_surp", "gap", "sec_react",
                           "sec_post_5d", "sec_post_21d", "react_atr"}
FEATS = [c for c in df.columns if c not in NON_FEAT]


def spear(a, b):
    m = ~(np.isnan(a) | np.isnan(b))
    a, b = a[m], b[m]
    if len(a) < 8:
        return np.nan, np.nan, len(a)
    ra, rb = pd.Series(a).rank().values, pd.Series(b).rank().values
    ra, rb = (ra - ra.mean()) / ra.std(), (rb - rb.mean()) / rb.std()
    rho = np.mean(ra * rb) * len(ra) / (len(ra) - 1)
    perm = np.array([rng.permutation(rb) for _ in range(4000)])
    null = (perm * ra).sum(1) / (len(ra) - 1)
    return rho, (np.sum(np.abs(null) >= abs(rho)) + 1) / 4001, len(a)


def bh(p):
    p = np.asarray(p); n = np.sum(~np.isnan(p)); o = np.argsort(np.where(np.isnan(p), 9, p))
    q = np.full(len(p), np.nan); prev = 1.0
    for k in range(n - 1, -1, -1):
        prev = min(prev, p[o[k]] * n / (k + 1)); q[o[k]] = prev
    return q


early, late = df.date < "2014-01-01", df.date >= "2014-01-01"
recent = df.index >= len(df) - 20
out = []
for t in TARGETS:
    rows = []
    for f in FEATS:
        a, b = df[f].values.astype(float), df[t].values.astype(float)
        rho, p, n = spear(a, b)
        rows.append({"target": t, "feature": f, "rho": rho, "p": p, "n": n,
                     "rho_early": spear(a[early], b[early])[0],
                     "rho_late": spear(a[late], b[late])[0],
                     "rho_last20": spear(a[recent], b[recent])[0]})
    r = pd.DataFrame(rows)
    r["q"] = bh(r.p.values)
    r["stable"] = np.sign(r.rho_early) == np.sign(r.rho_late)
    out.append(r)
res = pd.concat(out)
res.to_csv(f"{HERE}/scan.csv", index=False)

pd.set_option("display.width", 220)
for t in TARGETS:
    r = res[res.target == t].sort_values("p")
    print(f"\n=== {t}   (n~{int(r.n.median())}; expected false hits at p<.05: {0.05*len(r):.1f})")
    print(r[r.p < 0.10][["feature", "rho", "p", "q", "rho_early", "rho_late", "rho_last20", "stable"]]
          .round(3).to_string(index=False))
