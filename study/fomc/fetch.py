"""Fetch data for the FOMC study.

- Yahoo daily bars (auto-adjusted) for SOXL, MU, semis/market ETFs, rates and dollar proxies
- Yahoo 60-minute bars (last ~730 days) to isolate the 2:00pm ET announcement window
- FRED series: fed funds target, Treasury yields, CPI/core PCE, unemployment, HY spread
- MU / NVDA earnings dates with EPS surprise (yfinance)
"""
from pathlib import Path
import io

import pandas as pd
import requests
import yfinance as yf

HERE = Path(__file__).parent
DAILY = ["SOXL", "MU", "SOXX", "SMH", "NVDA", "QQQ", "SPY", "TLT", "SHY", "^VIX", "^TNX", "^IRX", "DX-Y.NYB"]
HOURLY = ["SOXL", "MU", "QQQ", "SHY", "TLT"]
FRED = ["DFEDTARU", "DFEDTARL", "DGS2", "DGS10", "DGS3MO", "T10Y2Y", "CPIAUCSL", "CPILFESL",
        "PCEPILFE", "UNRATE", "BAMLH0A0HYM2"]
NAME = {"^VIX": "VIX", "^TNX": "TNX", "^IRX": "IRX", "DX-Y.NYB": "DXY"}


def daily():
    raw = yf.download(DAILY, start="2015-06-01", auto_adjust=True, group_by="ticker", progress=False, threads=True)
    rows = []
    for t in DAILY:
        df = raw[t].dropna(subset=["Close"]).reset_index()
        df.columns = [c.lower() for c in df.columns]
        df["ticker"] = NAME.get(t, t)
        rows.append(df[["ticker", "date", "open", "high", "low", "close", "volume"]])
    px = pd.concat(rows)
    px.to_csv(HERE / "prices_daily.csv", index=False)
    print(px.groupby("ticker").date.agg(["min", "max", "count"]))


def hourly():
    raw = yf.download(HOURLY, period="730d", interval="60m", auto_adjust=True, group_by="ticker",
                      progress=False, threads=True)
    rows = []
    for t in HOURLY:
        df = raw[t].dropna(subset=["Close"]).reset_index()
        df.columns = [c.lower() for c in df.columns]
        df = df.rename(columns={"datetime": "ts"})
        df["ts"] = pd.to_datetime(df["ts"]).dt.tz_convert("America/New_York").dt.tz_localize(None)
        df["ticker"] = t
        rows.append(df[["ticker", "ts", "open", "high", "low", "close", "volume"]])
    h = pd.concat(rows)
    h.to_csv(HERE / "prices_hourly.csv", index=False)
    print(h.groupby("ticker").ts.agg(["min", "max", "count"]))


def fred():
    out = []
    for sid in FRED:
        r = requests.get(f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={sid}", timeout=60)
        r.raise_for_status()
        df = pd.read_csv(io.StringIO(r.text))
        df.columns = ["date", "value"]
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df["series"] = sid
        out.append(df[df.date >= "2015-01-01"])
    f = pd.concat(out)
    f.to_csv(HERE / "fred.csv", index=False)
    print(f.dropna().groupby("series").date.agg(["min", "max", "count"]))


def earnings():
    rows = []
    for t in ["MU", "NVDA"]:
        try:
            e = yf.Ticker(t).get_earnings_dates(limit=60).reset_index()
            e.columns = ["ts", "eps_est", "eps_act", "surprise_pct"] + list(e.columns[4:])
            e["ticker"] = t
            rows.append(e[["ticker", "ts", "eps_est", "eps_act", "surprise_pct"]])
        except Exception as exc:  # scraping endpoint is flaky; the DB copy covers 2023+
            print(t, "earnings fetch failed:", exc)
    if rows:
        e = pd.concat(rows)
        e.to_csv(HERE / "earnings_dates.csv", index=False)
        print(e.groupby("ticker").ts.agg(["min", "max", "count"]))


if __name__ == "__main__":
    daily()
    hourly()
    fred()
    earnings()
