import pandas as pd, numpy as np
pd.set_option("display.width",250,"display.max_columns",80)
df = pd.read_csv("mu_earnings_study.csv")

# Next-quarter revenue guidance midpoint vs street consensus at the time.
# verified = from Micron press release / contemporaneous coverage; approx = widely-reported consensus
GUIDE = {  # fq: (guide_mid_$B, street_cons_$B, confidence)
 "FQ2'24": (6.60,  6.00, "approx"),
 "FQ3'24": (7.60,  7.58, "approx"),
 "FQ4'24": (8.70,  8.30, "verified-guide"),
 "FQ1'25": (7.90,  9.00, "verified"),
 "FQ2'25": (8.80,  8.50, "approx"),
 "FQ3'25": (10.70, 9.90, "verified-guide"),
 "FQ4'25": (12.50, 11.90,"verified"),
 "FQ1'26": (18.70, 14.80,"verified-guide"),
 "FQ2'26": (33.50, 24.30,"verified"),
 "FQ3'26": (50.00, 41.00,"verified-guide"),
}
df["guide_mid"]  = df.fq.map(lambda f: GUIDE[f][0])
df["guide_cons"] = df.fq.map(lambda f: GUIDE[f][1])
df["guide_conf"] = df.fq.map(lambda f: GUIDE[f][2])
df["guide_surp_pct"] = (df.guide_mid/df.guide_cons - 1)*100

def sec(t): print("\n"+"="*88+f"\n{t}\n"+"="*88)

sec("9. GUIDANCE — the variable that actually moves MU")
print(df[["fq","date","rev_surp_pct","eps_surp_pct","guide_mid","guide_cons","guide_surp_pct","react_1d","mu_post_21d","guide_conf"]].round(2).to_string(index=False))
print("\ncorr(guide_surp_pct, react_1d)  Pearson %.2f  Spearman %.2f" % (df.guide_surp_pct.corr(df.react_1d), df.guide_surp_pct.corr(df.react_1d,method="spearman")))
print("corr(rev_surp_pct,   react_1d)  Pearson %.2f  Spearman %.2f" % (df.rev_surp_pct.corr(df.react_1d), df.rev_surp_pct.corr(df.react_1d,method="spearman")))
print("corr(eps_surp_pct,   react_1d)  Pearson %.2f  Spearman %.2f" % (df.eps_surp_pct.corr(df.react_1d), df.eps_surp_pct.corr(df.react_1d,method="spearman")))

df["g_bucket"]=pd.cut(df.guide_surp_pct,[-99,2,15,99],labels=["guide in-line/below (<+2%)","guide modestly above (+2..15%)","guide hugely above (>+15%)"])
print()
print(df.groupby("g_bucket",observed=True)[["guide_surp_pct","react_1d","mu_post_5d","mu_post_21d"]].agg(["mean","count"]).round(2).to_string())

sec("10. TWO-FACTOR: guidance surprise  x  sector trend (SOXX vs 50DMA)")
df["g_up"]=np.where(df.guide_surp_pct>=2,"guide ABOVE","guide in-line/below")
df["sox"]=np.where(df.SOXX_above50==1,"SOXX>50DMA","SOXX<50DMA")
print(df.pivot_table(index="g_up",columns="sox",values="react_1d",aggfunc=["mean","count"]).round(2).to_string())
print("\n21-day post return:")
print(df.pivot_table(index="g_up",columns="sox",values="mu_post_21d",aggfunc=["mean","count"]).round(2).to_string())

sec("11. OVERBOUGHT FILTER: RSI at the print")
df["rsi_b"]=np.where(df.rsi14>=70,"RSI>=70 (overbought)",np.where(df.rsi14>=58,"RSI 58-70","RSI<58"))
print(df.groupby("rsi_b")[["rsi14","guide_surp_pct","react_1d","mu_post_5d","mu_post_21d"]].agg(["mean","count"]).round(2).to_string())

sec("12. SUMMARY SCORECARD")
print(f"Double beats:                 10/10")
print(f"Guide above consensus:        {(df.guide_surp_pct>=2).sum()}/10")
print(f"Positive 1-day reaction:       {(df.react_1d>0).sum()}/10")
print(f"Positive 5-day post reaction:  {(df.mu_post_5d>0).sum()}/10")
print(f"Positive 21-day post reaction: {(df.mu_post_21d>0).sum()}/10")
print(f"Avg |1-day move|:             {df.react_1d.abs().mean():.1f}%  (range {df.react_1d.min():.1f}% to {df.react_1d.max():.1f}%)")
print(f"Avg 21d post:                 {df.mu_post_21d.mean():+.1f}%   vs SOXX {df.SOXX_post_21d.mean():+.1f}%   QQQ {df.QQQ_post_21d.mean():+.1f}%   SPY {df.SPY_post_21d.mean():+.1f}%")
print(f"corr(MU 21d post, SOXX 21d post) = {df.mu_post_21d.corr(df.SOXX_post_21d):.2f}")
print(f"corr(MU 21d post, QQQ 21d post)  = {df.mu_post_21d.corr(df.QQQ_post_21d):.2f}")
print(f"corr(MU 21d post, SPY 21d post)  = {df.mu_post_21d.corr(df.SPY_post_21d):.2f}")
print(f"Beta-ish: MU 21d / SOXX 21d slope = {np.polyfit(df.SOXX_post_21d,df.mu_post_21d,1)[0]:.2f}")
df.to_csv("mu_earnings_study.csv",index=False)
print("\nsaved -> study/mu_earnings/mu_earnings_study.csv")
