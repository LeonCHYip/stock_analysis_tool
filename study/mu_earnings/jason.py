import pandas as pd, numpy as np
p=pd.read_csv("prices.csv",parse_dates=["date"])
for t in ["MU","SOXX","QQQ","SPY"]:
    s=p[p.ticker==t].sort_values("date").set_index("date")
    c=s.close
    w=c[c.index>=pd.Timestamp("2024-03-20")]
    yrs=(w.index[-1]-w.index[0]).days/365.25
    cagr=((w.iloc[-1]/w.iloc[0])**(1/yrs)-1)*100
    dv=c.pct_change().dropna(); vol=dv.std()*np.sqrt(252)*100
    wk=c.resample("W-FRI").last().pct_change().dropna()
    big=(wk.abs()>=0.10).mean()*100
    mdd=((c/c.cummax())-1).min()*100
    print(f"{t:5s} CAGR {cagr:6.1f}%/yr over {yrs:.1f}y | ann.vol {vol:5.1f}% | ret/vol {cagr/vol:4.2f} "
          f"| weeks >=10%: {big:4.1f}% | maxDD {mdd:6.1f}%")
