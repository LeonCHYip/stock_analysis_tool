import pandas as pd, numpy as np
pd.set_option("display.width",250,"display.max_columns",80)
df = pd.read_csv("mu_earnings_study.csv")

def sec(t): print("\n" + "="*90 + f"\n{t}\n" + "="*90)

sec("1. BEAT HISTORY  (all 10 = double beat; only the MAGNITUDE varies)")
print(df[["fq","date","eps_est","eps_act","eps_surp_pct","rev_est","rev_act","rev_surp_pct","react_1d","mu_post_5d","mu_post_21d"]].round(2).to_string(index=False))
print(f"\nEPS beats {df.eps_beat.sum()}/10   Revenue beats {df.rev_beat.sum()}/10")
print(f"1-day reaction POSITIVE in {int((df.react_1d>0).sum())}/10 despite 10/10 double beats")
print(f"mean react_1d {df.react_1d.mean():.2f}%  median {df.react_1d.median():.2f}%  stdev {df.react_1d.std():.2f}%")
print(f"mean |react_1d| {df.react_1d.abs().mean():.2f}%   avg implied-ish move vs ATR at print: {(df.react_1d.abs()/df.atrp).mean():.1f}x ATR")

sec("2. CORRELATIONS  (Pearson r / Spearman rho, n=10)")
targets = ["react_1d","mu_post_5d","mu_post_21d","excess_21d"]
preds = ["eps_surp_pct","rev_surp_pct","mu_pre_5d","mu_pre_10d","mu_pre_21d","mu_pre_63d",
         "rsi14","vs_sma50","vs_sma200","vs_52wh","vol20","rs_pre_63d","rs_pre_21d",
         "SOXX_pre_21d","SOXX_pre_63d","SOXX_post_5d","SOXX_post_21d","QQQ_post_21d","SPY_post_21d"]
out=[]
for p in preds:
    row={"predictor":p}
    for t in targets:
        s=df[[p,t]].dropna()
        row[t+"_r"]=s[p].corr(s[t]) if len(s)>3 else np.nan
        row[t+"_rho"]=s[p].corr(s[t],method="spearman") if len(s)>3 else np.nan
    out.append(row)
c=pd.DataFrame(out).set_index("predictor")
print(c[[t+"_r" for t in targets]].round(2).to_string())
print("\nSpearman (rank) — more robust with n=10:")
print(c[[t+"_rho" for t in targets]].round(2).to_string())

sec("3. DOES BEAT SIZE DRIVE THE REACTION?  revenue-surprise buckets")
df["rev_bucket"]=np.where(df.rev_surp_pct>=5,"BIG beat (>=5%)","small beat (<5%)")
print(df.groupby("rev_bucket")[["rev_surp_pct","eps_surp_pct","react_1d","mu_post_5d","mu_post_21d"]].agg(["mean","count"]).round(2).to_string())
print("\nper event:")
print(df.sort_values("rev_surp_pct")[["fq","rev_surp_pct","eps_surp_pct","react_1d","mu_post_21d"]].round(2).to_string(index=False))

sec("4. DOES THE SECTOR/INDEX TREND MATTER?")
for col,lab in [("SOXX_above50","SOXX > 50DMA at print"),("SOXX_above200","SOXX > 200DMA"),("QQQ_above50","QQQ > 50DMA")]:
    g=df.groupby(col)[["react_1d","mu_post_5d","mu_post_21d"]].agg(["mean","count"]).round(2)
    print(f"\n--- {lab} ---"); print(g.to_string())
print("\nMU 21d post vs SOXX 21d post, event by event:")
print(df[["fq","mu_post_21d","SOXX_post_21d","SOXL_post_21d","QQQ_post_21d","SPY_post_21d","excess_21d"]].round(1).to_string(index=False))

sec("5. PRE-EARNINGS RUN-UP → 'BUY RUMOUR, SELL NEWS'?")
df["runup_bucket"]=np.where(df.mu_pre_21d>=15,"hot into print (>=+15% 21d)","cool/flat (<+15%)")
print(df.groupby("runup_bucket")[["mu_pre_21d","rsi14","react_1d","mu_post_5d","mu_post_21d"]].agg(["mean","count"]).round(2).to_string())
print("\nRSI at print vs reaction:")
print(df.sort_values("rsi14")[["fq","rsi14","vs_sma50","mu_pre_21d","react_1d","mu_post_5d","mu_post_21d"]].round(1).to_string(index=False))

sec("6. GAP BEHAVIOUR — does the open hold?")
df["gap_fade"]=df.react_1d-df.gap_pct
print(df[["fq","gap_pct","react_1d","gap_fade","mu_post_5d"]].round(2).to_string(index=False))
up=df[df.gap_pct>0]; dn=df[df.gap_pct<0]
print(f"\nUp gaps  n={len(up)}: avg gap {up.gap_pct.mean():+.2f}% -> avg close {up.react_1d.mean():+.2f}% (faded {up.gap_fade.mean():+.2f}pp); next 5d {up.mu_post_5d.mean():+.2f}%, next 21d {up.mu_post_21d.mean():+.2f}%")
print(f"Down gaps n={len(dn)}: avg gap {dn.gap_pct.mean():+.2f}% -> avg close {dn.react_1d.mean():+.2f}% (extended {dn.gap_fade.mean():+.2f}pp); next 5d {dn.mu_post_5d.mean():+.2f}%, next 21d {dn.mu_post_21d.mean():+.2f}%")

sec("7. DRIFT: does the 1-day reaction predict the next month?")
print(df[["fq","react_1d","mu_post_3d","mu_post_5d","mu_post_10d","mu_post_21d"]].round(2).to_string(index=False))
pos=df[df.react_1d>0]; neg=df[df.react_1d<0]
print(f"\nAfter POSITIVE reaction (n={len(pos)}): next 5d {pos.mu_post_5d.mean():+.2f}%  next 21d {pos.mu_post_21d.mean():+.2f}%")
print(f"After NEGATIVE reaction (n={len(neg)}): next 5d {neg.mu_post_5d.mean():+.2f}%  next 21d {neg.mu_post_21d.mean():+.2f}%")
print(f"corr(react_1d, mu_post_21d) = {df.react_1d.corr(df.mu_post_21d):.2f}")

sec("8. TECHNICAL STATE AT EACH PRINT")
print(df[["fq","px_T","rsi14","vs_sma50","vs_sma200","vs_52wh","atrp","vol20","ma_stack","rs_pre_63d"]].round(1).to_string(index=False))
df.to_csv("mu_earnings_study.csv",index=False)
