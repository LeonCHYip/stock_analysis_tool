"""Fetch daily OHLCV for the SOXL drop study from Yahoo Finance.

Full SOXL history (2010 launch) is pulled so the 3-year primary window can be
checked against a longer out-of-sample record. Writes prices.csv (long format).
"""
from pathlib import Path
import pandas as pd
import yfinance as yf

OUT = Path(__file__).parent / "prices.csv"
TICKERS = ["SOXL", "MU", "SOXX", "SMH", "NVDA", "QQQ", "SPY", "^VIX"]

raw = yf.download(TICKERS, start="2010-01-01", auto_adjust=True, group_by="ticker",
                  progress=False, threads=True)
rows = []
for t in TICKERS:
    df = raw[t].dropna(subset=["Close"]).reset_index()
    df.columns = [c.lower() for c in df.columns]
    df["ticker"] = t.replace("^", "")
    rows.append(df[["ticker", "date", "open", "high", "low", "close", "volume"]])
px = pd.concat(rows)
px.to_csv(OUT, index=False)
print(px.groupby("ticker").date.agg(["min", "max", "count"]))
