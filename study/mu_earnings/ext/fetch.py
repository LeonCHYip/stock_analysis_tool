"""Pull long daily history for the extended MU earnings study (2000 -> today)."""
import yfinance as yf, pandas as pd
T = ["MU", "SOXX", "SMH", "QQQ", "SPY", "^VIX"]
df = yf.download(T, start="1999-01-01", auto_adjust=True, group_by="ticker", progress=False)
rows = []
for t in T:
    s = df[t].dropna(subset=["Close"]).reset_index()
    s.columns = [c.lower() for c in s.columns]
    s["ticker"] = t.replace("^", "")
    rows.append(s[["date", "open", "high", "low", "close", "volume", "ticker"]])
out = pd.concat(rows)
out.to_csv(__file__.rsplit("/", 1)[0] + "/prices_long.csv", index=False)
print(out.groupby("ticker").date.agg(["min", "max", "count"]))
