import pandas as pd, numpy as np
PX=pd.read_csv("prices.csv",parse_dates=["date"])
def rsi(c,n=14):
    d=c.diff(); u=d.clip(lower=0).ewm(alpha=1/n,adjust=False).mean(); dn=(-d.clip(upper=0)).ewm(alpha=1/n,adjust=False).mean()
    return 100-100/(1+u/dn)
for t in ["MU","SOXX","QQQ","SPY"]:
    s=PX[PX.ticker==t].sort_values("date").reset_index(drop=True)
    c=s.close; i=len(s)-1
    print(f"{t:5s} {s.date.iloc[i].date()}  px={c.iloc[i]:8.2f}  RSI={rsi(c).iloc[i]:5.1f} "
          f" vs50DMA={(c.iloc[i]/c.rolling(50).mean().iloc[i]-1)*100:+6.1f}%  vs200DMA={(c.iloc[i]/c.rolling(200).mean().iloc[i]-1)*100:+6.1f}%"
          f"  vs52wHi={(c.iloc[i]/c.rolling(252).max().iloc[i]-1)*100:+6.1f}%  21d={(c.iloc[i]/c.iloc[i-21]-1)*100:+6.1f}%  63d={(c.iloc[i]/c.iloc[i-63]-1)*100:+6.1f}%")
