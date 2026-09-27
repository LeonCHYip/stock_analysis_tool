# -*- coding: utf-8 -*-
import pandas as pd, json, html
d = pd.read_csv("mu_earnings_study.csv")
E = d.to_dict("records")
def f(v, dp=1, sign=True):
    if pd.isna(v): return "—"
    s = f"{v:+.{dp}f}" if sign else f"{v:.{dp}f}"
    return s

# ─────────────────────────────────────────── chart 1: 1-day reaction columns
def chart_reaction():
    W,H=920,404; x0,x1=64,900; yt,yb=34,304
    lo,hi=-19.0,19.0
    def Y(v): return yt+(hi-v)*(yb-yt)/(hi-lo)
    n=len(E); slot=(x1-x0)/n; bw=46
    p=[f'<svg viewBox="0 0 {W} {H}" role="img" aria-label="Micron one-day price reaction to each of the last ten earnings reports" class="chart">']
    p.append(f'<title>MU one-day reaction by quarter</title>')
    for g in (-15,-10,-5,5,10,15):
        p.append(f'<line x1="{x0}" y1="{Y(g):.1f}" x2="{x1}" y2="{Y(g):.1f}" class="grid"/>')
        p.append(f'<text x="{x0-10}" y="{Y(g)+4:.1f}" class="tick" text-anchor="end">{g:+d}%</text>')
    p.append(f'<line x1="{x0}" y1="{Y(0):.1f}" x2="{x1}" y2="{Y(0):.1f}" class="zero"/>')
    p.append(f'<text x="{x0-10}" y="{Y(0)+4:.1f}" class="tick" text-anchor="end">0%</text>')
    for i,e in enumerate(E):
        v=e["react_1d"]; cx=x0+slot*i+slot/2
        top=Y(max(v,0)); h=abs(Y(v)-Y(0))
        cls="pos" if v>0 else "neg"
        p.append(f'<rect x="{cx-bw/2:.1f}" y="{top:.1f}" width="{bw}" height="{max(h,1.5):.1f}" rx="4" class="bar {cls}"/>')
        ly = top-9 if v>0 else top+h+18
        p.append(f'<text x="{cx:.1f}" y="{ly:.1f}" class="vlab {cls}" text-anchor="middle">{v:+.1f}</text>')
        p.append(f'<text x="{cx:.1f}" y="{yb+40:.1f}" class="xlab" text-anchor="middle">{html.escape(e["fq"])}</text>')
        p.append(f'<text x="{cx:.1f}" y="{yb+56:.1f}" class="xsub" text-anchor="middle">{e["date"][2:7]}</text>')
        gs=e["guide_surp_pct"]
        gc = "gpos" if gs>=2 else "gneg"
        p.append(f'<rect x="{cx-bw/2:.1f}" y="{yb+66:.1f}" width="{bw}" height="7" rx="3.5" class="gbar {gc}"/>')
    p.append(f'<text x="{x0}" y="{yb+86:.1f}" class="xsub">▬ guidance vs consensus: blue = above, red = at/below</text>')
    p.append("</svg>")
    return "\n".join(p)

# ─────────────────────────────────────────── chart 2: guidance surprise vs reaction
def chart_scatter():
    W,H=920,430; x0,x1=78,884; yt,yb=34,330
    xlo,xhi=-18.0,44.0; ylo,yhi=-20.0,20.0
    def X(v): return x0+(v-xlo)*(x1-x0)/(xhi-xlo)
    def Y(v): return yt+(yhi-v)*(yb-yt)/(yhi-ylo)
    p=[f'<svg viewBox="0 0 {W} {H}" role="img" aria-label="Scatter of next-quarter guidance surprise against Micron one-day price reaction" class="chart">']
    for g in (-15,-10,-5,5,10,15):
        p.append(f'<line x1="{x0}" y1="{Y(g):.1f}" x2="{x1}" y2="{Y(g):.1f}" class="grid"/>')
        p.append(f'<text x="{x0-10}" y="{Y(g)+4:.1f}" class="tick" text-anchor="end">{g:+d}%</text>')
    for g in (-10,0,10,20,30,40):
        p.append(f'<line x1="{X(g):.1f}" y1="{yt}" x2="{X(g):.1f}" y2="{yb}" class="grid"/>')
        p.append(f'<text x="{X(g):.1f}" y="{yb+20:.1f}" class="tick" text-anchor="middle">{g:+d}%</text>')
    p.append(f'<line x1="{x0}" y1="{Y(0):.1f}" x2="{x1}" y2="{Y(0):.1f}" class="zero"/>')
    p.append(f'<line x1="{X(0):.1f}" y1="{yt}" x2="{X(0):.1f}" y2="{yb}" class="zero"/>')
    lab_off={"FQ2'25":(0,20),"FQ4'24":(0,-16),"FQ3'24":(-4,20),"FQ3'25":(4,20),"FQ4'25":(0,-15),
             "FQ1'25":(0,20),"FQ2'24":(0,-16),"FQ1'26":(0,-16),"FQ2'26":(0,20),"FQ3'26":(0,-16)}
    for e in E:
        cx,cy=X(e["guide_surp_pct"]),Y(e["react_1d"])
        cls="pos" if e["react_1d"]>0 else "neg"
        solid = e["SOXX_above50"]==1
        p.append(f'<circle cx="{cx:.1f}" cy="{cy:.1f}" r="8" class="dot {cls} {"solid" if solid else "hollow"}"/>')
        dx,dy=lab_off[e["fq"]]
        p.append(f'<text x="{cx+dx:.1f}" y="{cy+dy:.1f}" class="plab" text-anchor="middle">{html.escape(e["fq"])}</text>')
    p.append(f'<text x="{(x0+x1)/2:.1f}" y="{yb+44:.1f}" class="axtitle" text-anchor="middle">Next-quarter revenue guidance vs street consensus →</text>')
    p.append(f'<text transform="translate(22,{(yt+yb)/2:.1f}) rotate(-90)" class="axtitle" text-anchor="middle">MU next-day close →</text>')
    p.append(f'<g transform="translate({x0},{yb+64})">'
             f'<circle cx="8" cy="-4" r="7" class="dot pos solid"/><text x="22" y="0" class="xsub">filled = SOXX above its 50-day at the print</text>'
             f'<circle cx="330" cy="-4" r="7" class="dot neg hollow"/><text x="344" y="0" class="xsub">hollow = SOXX below its 50-day</text></g>')
    p.append("</svg>")
    return "\n".join(p)

# ─────────────────────────────────────────── chart 3: MU vs SOXX 21-day after
def chart_after():
    W,H=920,400; x0,x1=70,900; yt,yb=30,312
    lo,hi=-32.0,62.0
    def Y(v): return yt+(hi-v)*(yb-yt)/(hi-lo)
    n=len(E); slot=(x1-x0)/n; bw=31; gap=4
    p=[f'<svg viewBox="0 0 {W} {H}" role="img" aria-label="Micron versus SOXX return over the 21 trading days after each earnings report" class="chart">']
    for g in (-30,-15,15,30,45,60):
        p.append(f'<line x1="{x0}" y1="{Y(g):.1f}" x2="{x1}" y2="{Y(g):.1f}" class="grid"/>')
        p.append(f'<text x="{x0-10}" y="{Y(g)+4:.1f}" class="tick" text-anchor="end">{g:+d}%</text>')
    p.append(f'<line x1="{x0}" y1="{Y(0):.1f}" x2="{x1}" y2="{Y(0):.1f}" class="zero"/>')
    p.append(f'<text x="{x0-10}" y="{Y(0)+4:.1f}" class="tick" text-anchor="end">0%</text>')
    for i,e in enumerate(E):
        base=x0+slot*i+slot/2
        for j,(key,cls) in enumerate([("mu_post_21d","s1"),("SOXX_post_21d","s2")]):
            v=e[key]; bx=base-bw-gap/2+j*(bw+gap)
            top=Y(max(v,0)); h=max(abs(Y(v)-Y(0)),1.5)
            p.append(f'<rect x="{bx:.1f}" y="{top:.1f}" width="{bw}" height="{h:.1f}" rx="4" class="bar {cls}"/>')
            ly= top-7 if v>0 else top+h+15
            p.append(f'<text x="{bx+bw/2:.1f}" y="{ly:.1f}" class="vsm" text-anchor="middle">{v:+.0f}</text>')
        p.append(f'<text x="{base:.1f}" y="{yb+42:.1f}" class="xlab" text-anchor="middle">{html.escape(e["fq"])}</text>')
    p.append(f'<g transform="translate({x0},{yb+66})">'
             f'<rect x="0" y="-10" width="14" height="12" rx="3" class="bar s1"/><text x="22" y="0" class="xsub">MU</text>'
             f'<rect x="70" y="-10" width="14" height="12" rx="3" class="bar s2"/><text x="92" y="0" class="xsub">SOXX</text></g>')
    p.append("</svg>")
    return "\n".join(p)

# ─────────────────────────────────────────── ledger table
def ledger():
    head = ["Quarter","Reported","EPS est → act","EPS surp","Rev est → act","Rev surp","Next-Q guide vs street","21d run-in","RSI","SOXX trend","Gap","Next-day close","+5d","+21d"]
    rows=[]
    for e in E:
        gs=e["guide_surp_pct"]
        gcls = "up" if gs>=2 else ("flat" if gs>-2 else "dn")
        trend = "above 50d" if e["SOXX_above50"]==1 else ("below 50d" if e["SOXX_above200"]==1 else "below 200d")
        tcls  = "up" if e["SOXX_above50"]==1 else "dn"
        def n(v,dp=1): 
            return f'<td class="num {"up" if v>0 else "dn"}">{v:+.{dp}f}%</td>'
        rows.append(f"""<tr>
<th scope="row">{e['fq']}</th>
<td class="mono dim">{e['date']}</td>
<td class="mono">{e['eps_est']:.2f} → <b>{e['eps_act']:.2f}</b></td>
<td class="num up">+{e['eps_surp_pct']:.0f}%</td>
<td class="mono">{e['rev_est']:.2f} → <b>{e['rev_act']:.2f}</b></td>
<td class="num up">+{e['rev_surp_pct']:.1f}%</td>
<td class="mono">{e['guide_mid']:.1f} vs {e['guide_cons']:.1f}&nbsp;<span class="chip {gcls}">{gs:+.0f}%</span></td>
{n(e['mu_pre_21d'])}
<td class="num {'hot' if e['rsi14']>=70 else ''}">{e['rsi14']:.0f}</td>
<td><span class="chip {tcls}">{trend}</span></td>
{n(e['gap_pct'])}
<td class="num big {'up' if e['react_1d']>0 else 'dn'}">{e['react_1d']:+.1f}%</td>
{n(e['mu_post_5d'])}
{n(e['mu_post_21d'])}
</tr>""")
    th="".join(f"<th>{h}</th>" for h in head)
    return f'<div class="scroll"><table class="ledger"><thead><tr>{th}</tr></thead><tbody>{"".join(rows)}</tbody></table></div>'

CORR = [
 ("Sector trend after the print — SOXX +21d", "MU +21d", 0.75, "strong", "The single best explainer of where MU is a month later."),
 ("Sector trend after the print — SOXX +5d", "MU +5d", 0.79, "strong", "Same story on a one-week horizon."),
 ("Next-quarter guidance vs consensus", "Next-day close", 0.56, "moderate", "Spearman rank. The only fundamental input with real signal."),
 ("Nasdaq — QQQ +21d", "MU +21d", 0.45, "moderate", "Weaker than SOXX: this is a sector trade, not a beta trade."),
 ("EPS surprise %", "Next-day close", 0.30, "weak", "Rank correlation. Mostly noise once guidance is known."),
 ("Revenue surprise %", "Next-day close", 0.44, "weak", "Some signal, but driven by two of ten events."),
 ("S&P 500 — SPY +21d", "MU +21d", 0.31, "weak", "Broad market barely matters."),
 ("5-day run-in before the print", "+5 days after", -0.72, "moderate", "Inverse. A hot tape into the print is repaid in the week after."),
 ("Next-day close", "+21 days after", 0.02, "none", "The reaction day tells you nothing about the month ahead."),
]
def corrtable():
    r=[]
    for a,b,v,s,note in CORR:
        w=abs(v)*100
        cls = "pos" if v>0 else "neg"
        r.append(f"""<tr><th scope="row">{a}</th><td class="mono dim">{b}</td>
<td class="num">{v:+.2f}</td>
<td class="barcell"><span class="minibar {cls}" style="width:{w:.0f}%"></span></td>
<td><span class="chip s-{s}">{s}</span></td><td class="note">{note}</td></tr>""")
    return f'<div class="scroll"><table class="corr"><thead><tr><th>Input</th><th>Against</th><th>r</th><th></th><th>Strength</th><th>Reading</th></tr></thead><tbody>{"".join(r)}</tbody></table></div>'

open("charts.json","w").write(json.dumps({"c1":chart_reaction(),"c2":chart_scatter(),"c3":chart_after(),"ledger":ledger(),"corr":corrtable()}))
print("generated")
