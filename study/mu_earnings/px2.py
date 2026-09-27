import yfinance as yf, pandas as pd
tk=["MU","SOXX","SOXL","QQQ","SPY"]
df=yf.download(tk,start="2022-09-01",end="2026-09-13",auto_adjust=False,group_by="ticker",progress=False)
o={}
for t in tk:
    s=df[t][["Open","High","Low","Close","Adj Close","Volume"]].copy()
    s.columns=["open","high","low","close","adj_close","volume"]; s["ticker"]=t; o[t]=s
pd.concat(o.values()).reset_index().rename(columns={"Date":"date"}).to_csv("prices.csv",index=False)
print("ok")
