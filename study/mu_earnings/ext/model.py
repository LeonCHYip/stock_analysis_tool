"""Walk-forward test of three small, pre-registered models (no feature search inside the loop).

  A  react_1d  ~ prev_react                                   (all eras: stable since 2001)
  B  post_5d   ~ overextension composite                      (2014+ regime only)
  C  full_5d   ~ prev_react + overextension composite         (2014+ regime only)

Overextension composite = mean of in-sample z-scores of ret_5d, ret_10d, vs_sma10, stoch14, rsi5.
Each event is predicted by an OLS fit on strictly earlier events. Reported: OOS Spearman,
directional hit rate, and the tercile spread (avg outcome in top third of predictions minus bottom).
"""
import numpy as np, pandas as pd
from features import HERE

OVX = ["ret_5d", "ret_10d", "vs_sma10", "stoch14", "rsi5"]
MODELS = {
    "A": dict(target="react_1d", feats=["prev_react"], start="2001-01-01", min_train=16),
    "B": dict(target="post_5d", feats=["ovx"], start="2014-01-01", min_train=16),
    "C": dict(target="full_5d", feats=["prev_react", "ovx"], start="2014-01-01", min_train=16),
}


def add_ovx(train, test):
    mu, sd = train[OVX].mean(), train[OVX].std()
    return (((train[OVX] - mu) / sd).mean(1), ((test[OVX] - mu) / sd).mean(1))


def fit_predict(train, test, feats, target):
    train, test = train.copy(), test.copy()
    if "ovx" in feats:
        train["ovx"], test["ovx"] = add_ovx(train, test)
    tr = train.dropna(subset=feats + [target])
    A = np.column_stack([np.ones(len(tr))] + [tr[f] for f in feats])
    beta, *_ = np.linalg.lstsq(A, tr[target].values, rcond=None)
    B = np.column_stack([np.ones(len(test))] + [test[f] for f in feats])
    resid_sd = np.std(tr[target].values - A @ beta, ddof=len(beta))
    return B @ beta, beta, resid_sd, test


def walk_forward(df, m):
    d = df[df.date >= m["start"]].reset_index(drop=True)
    preds = []
    for k in range(m["min_train"], len(d)):
        p, *_ = fit_predict(d.iloc[:k], d.iloc[[k]], m["feats"], m["target"])
        preds.append((d.date.iloc[k], p[0], d[m["target"]].iloc[k]))
    return pd.DataFrame(preds, columns=["date", "pred", "actual"]).dropna()


if __name__ == "__main__":
    df = pd.read_csv(f"{HERE}/panel.csv", parse_dates=["date"])
    summary = []
    for name, m in MODELS.items():
        w = walk_forward(df, m)
        rho = w.pred.rank().corr(w.actual.rank())
        hit = np.mean(np.sign(w.pred) == np.sign(w.actual))
        base = max(np.mean(w.actual > 0), np.mean(w.actual < 0))
        q = pd.qcut(w.pred, 3, labels=["low", "mid", "high"])
        g = w.groupby(q, observed=True).actual.mean()
        summary.append({"model": name, "target": m["target"], "n_oos": len(w), "oos_rho": rho,
                        "hit": hit, "base_rate": base, "low3": g["low"], "high3": g["high"],
                        "spread": g["high"] - g["low"]})
        w.to_csv(f"{HERE}/wf_{name}.csv", index=False)
    print(pd.DataFrame(summary).round(3).to_string(index=False))
