"""Estimation helpers, written against numpy because the project has neither
scipy nor statsmodels as a dependency and this study is not a reason to add
them.

What is here: winsorisation, cross-sectional standardisation, OLS with
heteroskedasticity-consistent (HC1) standard errors, Spearman correlation,
Fama-MacBeth averaging, and quantile portfolio sorts.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from .config import WINSOR_HI, WINSOR_LO


def winsorise(s: pd.Series, lo: float = WINSOR_LO, hi: float = WINSOR_HI) -> pd.Series:
    """Clip to the given quantiles. Tail-robustness, not outlier deletion."""
    s = pd.to_numeric(s, errors="coerce")
    if s.notna().sum() < 5:
        return s
    return s.clip(s.quantile(lo), s.quantile(hi))


def zscore(s: pd.Series) -> pd.Series:
    """Cross-sectional standardisation, after winsorising.

    Regression coefficients then read as "return per one cross-sectional
    standard deviation of the signal", which is comparable across metrics that
    are measured on wildly different scales (a percentage surprise and a
    standardised one, say).
    """
    w = winsorise(s)
    sd = w.std()
    if not np.isfinite(sd) or sd == 0:
        return pd.Series(np.nan, index=s.index)
    return (w - w.mean()) / sd


def rank_normalise(s: pd.Series) -> pd.Series:
    """Map to [-0.5, 0.5] by rank. Immune to the fat tails entirely."""
    r = s.rank(pct=True)
    return r - 0.5


# ── Regression ────────────────────────────────────────────────────────────────

def _t_to_p(t: np.ndarray, df: int) -> np.ndarray:
    """Two-sided p-value for a t statistic, without scipy.

    Uses the normal approximation, which is accurate to well under a
    percentage point at the sample sizes here (hundreds to thousands of
    stocks); it is only loose for the smallest per-sector cross-sections, and
    those are flagged by their `n` in the output anyway.
    """
    z = np.abs(np.asarray(t, dtype="float64"))
    # Abramowitz & Stegun 7.1.26 approximation to the error function.
    a1, a2, a3, a4, a5, p = (
        0.254829592, -0.284496736, 1.421413741, -1.453152027, 1.061405429, 0.3275911
    )
    x = z / np.sqrt(2.0)
    t_ = 1.0 / (1.0 + p * x)
    erf = 1.0 - (((((a5 * t_ + a4) * t_) + a3) * t_ + a2) * t_ + a1) * t_ * np.exp(-x * x)
    return np.clip(1.0 - erf, 0.0, 1.0)


def ols(y: pd.Series, X: pd.DataFrame, add_const: bool = True) -> dict:
    """OLS with HC1 robust standard errors.

    Cross-sectional return regressions are badly heteroskedastic — small,
    volatile stocks have far larger residuals than mega caps — so classical
    standard errors would overstate significance. HC1 is the small-sample
    correction of White's estimator.

    Returns the fit statistics plus a per-coefficient frame.
    """
    d = pd.concat([y.rename("_y"), X], axis=1).dropna()
    n = len(d)
    cols = list(X.columns)
    if n < max(20, 3 * (len(cols) + 1)):
        return {"n": n, "r2": np.nan, "adj_r2": np.nan, "coefs": pd.DataFrame()}

    yv = d["_y"].to_numpy(dtype="float64")
    Xv = d[cols].to_numpy(dtype="float64")
    names = list(cols)
    if add_const:
        Xv = np.column_stack([np.ones(n), Xv])
        names = ["const", *names]

    # Drop columns that became collinear after the dropna (an all-zero sector
    # dummy, most often). A pseudo-inverse would silently return an arbitrary
    # split of the coefficient between the collinear columns instead.
    keep = _independent_columns(Xv)
    Xv, names = Xv[:, keep], [names[i] for i in keep]
    k = Xv.shape[1]
    if k == 0 or n <= k:
        return {"n": n, "r2": np.nan, "adj_r2": np.nan, "coefs": pd.DataFrame()}

    XtX_inv = np.linalg.pinv(Xv.T @ Xv)
    beta = XtX_inv @ Xv.T @ yv
    resid = yv - Xv @ beta

    # HC1: White's sandwich with the n/(n-k) small-sample scaling.
    meat = (Xv * (resid**2)[:, None]).T @ Xv
    cov = XtX_inv @ meat @ XtX_inv * (n / (n - k))
    se = np.sqrt(np.clip(np.diag(cov), 0, None))

    ss_res = float(resid @ resid)
    ss_tot = float(((yv - yv.mean()) ** 2).sum())
    r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else np.nan
    adj_r2 = 1.0 - (1.0 - r2) * (n - 1) / (n - k) if np.isfinite(r2) else np.nan

    with np.errstate(divide="ignore", invalid="ignore"):
        tstat = np.where(se > 0, beta / se, np.nan)

    coefs = pd.DataFrame(
        {
            "term": names,
            "coef": beta,
            "se": se,
            "t_stat": tstat,
            "p_value": _t_to_p(tstat, n - k),
        }
    )
    return {"n": n, "k": k, "r2": r2, "adj_r2": adj_r2, "coefs": coefs}


def _independent_columns(X: np.ndarray, tol: float = 1e-10) -> list[int]:
    """Indices of a maximal linearly independent set of columns, left to right."""
    keep: list[int] = []
    for j in range(X.shape[1]):
        trial = X[:, keep + [j]]
        if np.linalg.matrix_rank(trial, tol=tol) == len(keep) + 1:
            keep.append(j)
    return keep


def spearman(x: pd.Series, y: pd.Series) -> dict:
    """Rank correlation with its t statistic. Spearman is Pearson on ranks."""
    d = pd.DataFrame({"x": x, "y": y}).dropna()
    n = len(d)
    if n < 5:
        return {"n": n, "rho": np.nan, "t_stat": np.nan, "p_value": np.nan}
    rho = d["x"].rank().corr(d["y"].rank())
    if pd.isna(rho) or abs(rho) >= 1:
        return {"n": n, "rho": float(rho) if pd.notna(rho) else np.nan,
                "t_stat": np.nan, "p_value": np.nan}
    t = rho * np.sqrt((n - 2) / (1 - rho**2))
    return {"n": n, "rho": float(rho), "t_stat": float(t),
            "p_value": float(_t_to_p(np.array([t]), n - 2)[0])}


def dummies(s: pd.Series, prefix: str, drop_first: bool = True) -> pd.DataFrame:
    """Indicator columns for a categorical, with one level dropped as the base."""
    d = pd.get_dummies(s.astype("string").fillna("Unknown"), prefix=prefix,
                       dtype="float64")
    if drop_first and d.shape[1] > 1:
        d = d.drop(columns=d.columns[0])
    return d


def fama_macbeth(per_period: pd.DataFrame, coef_col: str = "coef",
                 by: str = "term") -> pd.DataFrame:
    """Average a series of cross-sectional slopes into one estimate per term.

    The Fama-MacBeth t statistic is the mean slope over the standard error of
    the slopes *across periods*, so it prices in how unstable the relationship
    was over time rather than only how precisely each period was estimated.
    That instability is the whole subject here.
    """
    recs = []
    for term, g in per_period.groupby(by):
        v = g[coef_col].dropna()
        n = len(v)
        if n < 3:
            continue
        mean, sd = v.mean(), v.std(ddof=1)
        se = sd / np.sqrt(n) if n > 1 and sd > 0 else np.nan
        t = mean / se if se and np.isfinite(se) and se > 0 else np.nan
        recs.append(
            {
                "term": term,
                "n_periods": n,
                "mean_coef": mean,
                "sd_coef": sd,
                "t_stat": t,
                "p_value": float(_t_to_p(np.array([t]), n - 1)[0])
                if np.isfinite(t) else np.nan,
                "pct_positive": float((v > 0).mean() * 100.0),
            }
        )
    return pd.DataFrame(recs)


def quantile_sort(df: pd.DataFrame, signal: str, outcomes: list[str],
                  q: int = 5, group_col: str | None = None) -> pd.DataFrame:
    """Sort on `signal` into `q` buckets and report mean/median outcomes.

    Includes the top-minus-bottom spread with a two-sample t statistic, which
    is the direct answer to "was this signal paid for in this window".
    """
    recs = []
    keys = [None] if group_col is None else sorted(df[group_col].dropna().unique())
    for key in keys:
        d = df if key is None else df[df[group_col] == key]
        d = d.dropna(subset=[signal])
        if len(d) < q * 5:
            continue
        try:
            bucket = pd.qcut(d[signal].rank(method="first"), q, labels=False)
        except ValueError:
            continue
        for outcome in outcomes:
            stats = []
            for b in range(q):
                v = d.loc[bucket == b, outcome].dropna()
                stats.append(v)
                recs.append(
                    {
                        group_col or "group": key or "ALL",
                        "signal": signal,
                        "outcome": outcome,
                        "bucket": f"Q{b + 1}",
                        "n": len(v),
                        "mean": v.mean(),
                        "median": v.median(),
                    }
                )
            lo, hi = stats[0], stats[-1]
            if len(lo) > 2 and len(hi) > 2:
                diff = hi.mean() - lo.mean()
                se = np.sqrt(hi.var(ddof=1) / len(hi) + lo.var(ddof=1) / len(lo))
                t = diff / se if se > 0 else np.nan
                recs.append(
                    {
                        group_col or "group": key or "ALL",
                        "signal": signal,
                        "outcome": outcome,
                        "bucket": "Q5-Q1",
                        "n": len(lo) + len(hi),
                        "mean": diff,
                        "median": hi.median() - lo.median(),
                        "t_stat": t,
                        "p_value": float(
                            _t_to_p(np.array([t]), len(lo) + len(hi) - 2)[0]
                        ) if np.isfinite(t) else np.nan,
                    }
                )
    return pd.DataFrame(recs)
