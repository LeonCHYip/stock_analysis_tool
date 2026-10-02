"""Post-2014 regime scan + rolling stability of the key relationships."""
import numpy as np, pandas as pd
from scan import spear, bh, FEATS, df, HERE
late = df[df.date >= "2014-01-01"].reset_index(drop=True)
pd.set_option("display.width", 220)
rows = []
for t in ["react_1d", "post_5d", "post_5d_ex", "full_5d", "post_21d"]:
    for f in FEATS:
        a, b = late[f].values.astype(float), late[t].values.astype(float)
        rho, p, n = spear(a, b)
        h1, h2 = late.index < len(late) // 2, late.index >= len(late) // 2
        rows.append({"target": t, "feature": f, "rho": rho, "p": p, "n": n,
                     "rho_14_19": spear(a[h1], b[h1])[0], "rho_20_26": spear(a[h2], b[h2])[0]})
r = pd.DataFrame(rows)
r["q"] = r.groupby("target").p.transform(lambda s: bh(s.values))
r.to_csv(f"{HERE}/scan_late.csv", index=False)
for t, g in r.groupby("target", sort=False):
    g = g.sort_values("p"); g = g[g.p < 0.05]
    print(f"\n=== 2014+ {t}  n={int(late[t].notna().sum())}  (expected false hits {0.05*len(FEATS):.1f})")
    print(g[["feature", "rho", "p", "q", "rho_14_19", "rho_20_26"]].round(3).to_string(index=False))

print("\n=== rolling 24-print Spearman: pre-5d run-in vs post-5d drift, and prev_react vs react")
for end in range(24, len(df) + 1, 4):
    w = df.iloc[end - 24:end]
    a = spear(w.ret_5d.values, w.post_5d.values)[0]
    b = spear(w.prev_react.values, w.react_1d.values)[0]
    print(f"{w.date.iloc[0].date()} -> {w.date.iloc[-1].date()}   runin->drift {a:+.2f}   prev_react->react {b:+.2f}")
