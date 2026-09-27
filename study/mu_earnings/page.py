# -*- coding: utf-8 -*-
import json
C = json.load(open("charts.json"))

HTML = f"""<title>Micron Earnings Playbook</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&family=IBM+Plex+Sans+Condensed:wght@500;600;700&family=IBM+Plex+Serif:ital,wght@0,400;0,500;1,400&display=swap">
<style>
:root {{
  color-scheme: light;
  --ground:#E7ECF1; --surface:#FBFCFD; --surface-2:#F1F5F9; --raise:#FFFFFF;
  --ink:#0F161F; --ink-2:#505D6B; --ink-3:#7B8794;
  --rule:#D1DAE3; --rule-soft:#E3E9EF;
  --accent:#2F4B7C;
  --pos:#2a78d6; --neg:#e34948; --s1:#2a78d6; --s2:#eb6834;
  --hot:#b45309;
  --shadow:0 1px 2px rgba(15,22,31,.05), 0 8px 24px -14px rgba(15,22,31,.22);
}}
@media (prefers-color-scheme: dark) {{
  :root:not([data-theme="light"]) {{
    color-scheme: dark;
    --ground:#0A0E13; --surface:#161B22; --surface-2:#1B222B; --raise:#1E262F;
    --ink:#E3EAF2; --ink-2:#96A2B1; --ink-3:#6C7988;
    --rule:#242D38; --rule-soft:#1C242D;
    --accent:#8FB0DE;
    --pos:#3987e5; --neg:#e66767; --s1:#3987e5; --s2:#d95926;
    --hot:#d99a2b;
    --shadow:0 1px 2px rgba(0,0,0,.4), 0 8px 24px -14px rgba(0,0,0,.7);
  }}
}}
:root[data-theme="dark"] {{
  color-scheme: dark;
  --ground:#0A0E13; --surface:#161B22; --surface-2:#1B222B; --raise:#1E262F;
  --ink:#E3EAF2; --ink-2:#96A2B1; --ink-3:#6C7988;
  --rule:#242D38; --rule-soft:#1C242D;
  --accent:#8FB0DE;
  --pos:#3987e5; --neg:#e66767; --s1:#3987e5; --s2:#d95926;
  --hot:#d99a2b;
  --shadow:0 1px 2px rgba(0,0,0,.4), 0 8px 24px -14px rgba(0,0,0,.7);
}}

*,*::before,*::after {{ box-sizing:border-box; }}
body {{
  background:var(--ground); color:var(--ink);
  font-family:"IBM Plex Serif",Georgia,serif; font-size:16px; line-height:1.62;
  -webkit-font-smoothing:antialiased;
}}
.wrap {{ max-width:1096px; margin:0 auto; padding:0 24px 88px; }}
h1,h2,h3,.cond {{ font-family:"IBM Plex Sans Condensed","Helvetica Neue",Arial,sans-serif; text-wrap:balance; }}
.mono,.num,table,.tick,.chip,.eyebrow {{ font-family:"IBM Plex Mono",ui-monospace,SFMono-Regular,Menlo,monospace; }}
.num,table {{ font-variant-numeric:tabular-nums; }}
a {{ color:var(--accent); }}
:focus-visible {{ outline:2px solid var(--accent); outline-offset:3px; border-radius:3px; }}

/* ── masthead ─────────────────────────────── */
header.mast {{ padding:56px 0 30px; border-bottom:2px solid var(--ink); }}
.eyebrow {{ font-size:11.5px; letter-spacing:.16em; text-transform:uppercase; color:var(--ink-3); margin:0 0 14px; }}
h1 {{ font-size:clamp(40px,6.4vw,68px); line-height:.98; font-weight:700; letter-spacing:-.022em; margin:0 0 18px; }}
.standfirst {{ font-size:19px; line-height:1.56; color:var(--ink-2); max-width:63ch; margin:0; }}
.standfirst b {{ color:var(--ink); font-weight:500; }}

/* ── stat strip ───────────────────────────── */
.stats {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(178px,1fr)); gap:0; border-bottom:1px solid var(--rule); }}
.stat {{ padding:24px 22px 22px; border-right:1px solid var(--rule-soft); }}
.stat:last-child {{ border-right:0; }}
.stat .k {{ font-family:"IBM Plex Mono",monospace; font-size:34px; font-weight:500; letter-spacing:-.03em; line-height:1; color:var(--ink); }}
.stat .k.neg {{ color:var(--neg); }}
.stat .l {{ font-family:"IBM Plex Sans Condensed",sans-serif; font-size:13px; line-height:1.35; color:var(--ink-2); margin-top:9px; }}

/* ── sections ─────────────────────────────── */
section {{ padding-top:54px; }}
h2 {{ font-size:13px; font-weight:600; letter-spacing:.15em; text-transform:uppercase; color:var(--ink-3);
     margin:0 0 6px; padding-bottom:9px; border-bottom:1px solid var(--rule); }}
.lede {{ font-size:17px; color:var(--ink-2); max-width:66ch; margin:20px 0 26px; }}
p {{ max-width:66ch; }}

/* ── charts ───────────────────────────────── */
figure {{ margin:0 0 12px; background:var(--surface); border:1px solid var(--rule); border-radius:10px; padding:22px 20px 16px; box-shadow:var(--shadow); }}
figcaption {{ font-family:"IBM Plex Sans Condensed",sans-serif; font-size:14px; color:var(--ink-2); margin-top:14px; padding-top:13px; border-top:1px solid var(--rule-soft); max-width:none; }}
figcaption b {{ color:var(--ink); font-weight:600; }}
.chart {{ display:block; width:100%; height:auto; overflow:visible; }}
.grid {{ stroke:var(--rule-soft); stroke-width:1; }}
.zero {{ stroke:var(--ink-3); stroke-width:1.25; }}
.tick {{ font-family:"IBM Plex Mono",monospace; font-size:11px; fill:var(--ink-3); }}
.xlab {{ font-family:"IBM Plex Sans Condensed",sans-serif; font-size:13px; font-weight:600; fill:var(--ink); }}
.xsub {{ font-family:"IBM Plex Mono",monospace; font-size:11px; fill:var(--ink-3); }}
.axtitle {{ font-family:"IBM Plex Sans Condensed",sans-serif; font-size:12px; font-weight:600; letter-spacing:.05em; fill:var(--ink-3); }}
.vlab {{ font-family:"IBM Plex Mono",monospace; font-size:13px; font-weight:600; }}
.vsm {{ font-family:"IBM Plex Mono",monospace; font-size:10.5px; fill:var(--ink-2); }}
.plab {{ font-family:"IBM Plex Sans Condensed",sans-serif; font-size:11.5px; font-weight:600; fill:var(--ink-2); }}
.bar.pos, .dot.pos.solid {{ fill:var(--pos); }}
.bar.neg, .dot.neg.solid {{ fill:var(--neg); }}
.bar.s1 {{ fill:var(--s1); }}
.bar.s2 {{ fill:var(--s2); }}
text.vlab.pos {{ fill:var(--pos); }}
text.vlab.neg {{ fill:var(--neg); }}
.gbar.gpos {{ fill:var(--pos); opacity:.45; }}
.gbar.gneg {{ fill:var(--neg); opacity:.45; }}
.dot {{ stroke:var(--surface); stroke-width:2; }}
.dot.hollow {{ fill:var(--surface); stroke-width:2.75; }}
.dot.pos.hollow {{ stroke:var(--pos); }}
.dot.neg.hollow {{ stroke:var(--neg); }}

/* ── tables ───────────────────────────────── */
.scroll {{ overflow-x:auto; border:1px solid var(--rule); border-radius:10px; background:var(--surface); box-shadow:var(--shadow); }}
table {{ border-collapse:collapse; width:100%; font-size:13px; }}
thead th {{ font-family:"IBM Plex Sans Condensed",sans-serif; font-size:11px; font-weight:600; letter-spacing:.07em; text-transform:uppercase;
  color:var(--ink-3); text-align:left; padding:13px 12px; border-bottom:1px solid var(--rule); white-space:nowrap; background:var(--surface-2); }}
tbody td, tbody th {{ padding:11px 12px; border-bottom:1px solid var(--rule-soft); white-space:nowrap; text-align:left; font-weight:400; }}
tbody tr:last-child td, tbody tr:last-child th {{ border-bottom:0; }}
tbody th[scope=row] {{ font-family:"IBM Plex Sans Condensed",sans-serif; font-size:14px; font-weight:700; color:var(--ink); }}
td.num {{ text-align:right; }}
td.num.up {{ color:var(--pos); }}
td.num.dn {{ color:var(--neg); }}
td.num.big {{ font-size:15px; font-weight:600; }}
td.num.hot {{ color:var(--hot); font-weight:600; }}
td.dim {{ color:var(--ink-3); }}
td.mono b {{ font-weight:600; color:var(--ink); }}
td.note {{ font-family:"IBM Plex Serif",serif; color:var(--ink-2); white-space:normal; min-width:290px; font-size:14px; }}
.chip {{ display:inline-block; font-size:10.5px; letter-spacing:.04em; padding:2.5px 7px; border-radius:4px; border:1px solid; white-space:nowrap; }}
.chip.up {{ color:var(--pos); border-color:color-mix(in srgb, var(--pos) 40%, transparent); background:color-mix(in srgb, var(--pos) 9%, transparent); }}
.chip.dn {{ color:var(--neg); border-color:color-mix(in srgb, var(--neg) 40%, transparent); background:color-mix(in srgb, var(--neg) 9%, transparent); }}
.chip.flat {{ color:var(--ink-3); border-color:var(--rule); }}
.chip.s-strong {{ color:var(--pos); border-color:color-mix(in srgb,var(--pos) 45%,transparent); background:color-mix(in srgb,var(--pos) 11%,transparent); }}
.chip.s-moderate {{ color:var(--ink-2); border-color:var(--rule); background:var(--surface-2); }}
.chip.s-weak, .chip.s-none {{ color:var(--ink-3); border-color:var(--rule-soft); }}
.barcell {{ width:96px; }}
.minibar {{ display:block; height:7px; border-radius:3.5px; }}
.minibar.pos {{ background:var(--pos); }}
.minibar.neg {{ background:var(--neg); }}

/* ── findings ─────────────────────────────── */
.findings {{ display:grid; gap:1px; background:var(--rule); border:1px solid var(--rule); border-radius:10px; overflow:hidden; box-shadow:var(--shadow); }}
.finding {{ background:var(--surface); padding:26px 26px 24px; display:grid; grid-template-columns:minmax(0,1fr); gap:10px; }}
@media (min-width:760px) {{ .finding {{ grid-template-columns:212px minmax(0,1fr); gap:28px; }} }}
.finding h3 {{ font-size:19px; font-weight:600; line-height:1.22; letter-spacing:-.01em; margin:0; }}
.finding .body {{ margin:0; font-size:15.5px; color:var(--ink-2); max-width:62ch; }}
.finding .body b {{ color:var(--ink); font-weight:500; }}
.finding .body + .body {{ margin-top:11px; }}
.tag {{ display:inline-block; margin-top:11px; }}
.ev {{ font-family:"IBM Plex Mono",monospace; font-size:12px; color:var(--ink); background:var(--surface-2);
      border-left:2px solid var(--accent); padding:9px 13px; margin-top:14px; border-radius:0 5px 5px 0; max-width:62ch; }}

/* ── setup panel ──────────────────────────── */
.setup {{ background:var(--raise); border:1px solid var(--rule); border-radius:10px; padding:0; overflow:hidden; box-shadow:var(--shadow); }}
.setup .hd {{ padding:22px 26px 18px; border-bottom:1px solid var(--rule); display:flex; flex-wrap:wrap; gap:12px; align-items:baseline; justify-content:space-between; }}
.setup .hd h3 {{ margin:0; font-size:21px; font-weight:600; }}
.setup .grid2 {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(150px,1fr)); }}
.cell {{ padding:18px 26px; border-right:1px solid var(--rule-soft); border-bottom:1px solid var(--rule-soft); }}
.cell .l {{ font-family:"IBM Plex Sans Condensed",sans-serif; font-size:11.5px; letter-spacing:.08em; text-transform:uppercase; color:var(--ink-3); }}
.cell .v {{ font-family:"IBM Plex Mono",monospace; font-size:20px; margin-top:5px; }}
.cell .v small {{ font-size:12px; color:var(--ink-3); }}
.setup .ft {{ padding:20px 26px 24px; }}
.setup .ft p {{ margin:0 0 12px; font-size:15.5px; color:var(--ink-2); }}
.setup .ft p:last-child {{ margin-bottom:0; }}

.caveats {{ font-size:14.5px; color:var(--ink-2); }}
.caveats li {{ margin-bottom:9px; max-width:70ch; }}
footer {{ margin-top:56px; padding-top:22px; border-top:1px solid var(--rule); font-size:13px; color:var(--ink-3); }}
footer a {{ color:var(--ink-2); }}
@media (prefers-reduced-motion:reduce) {{ *{{animation:none!important;transition:none!important}} }}
</style>

<div class="wrap">

<header class="mast">
  <p class="eyebrow">Micron Technology · NASDAQ: MU · ten prints, Mar 2024 – Jun 2026</p>
  <h1>Micron Earnings Playbook</h1>
  <p class="standfirst">Micron beat both the revenue and the EPS consensus in <b>all ten</b> of its last ten reports. The stock closed higher the next day in <b>four</b> of them. This is what actually moved it instead.</p>
</header>

<div class="stats">
  <div class="stat"><div class="k">10 / 10</div><div class="l">Double beats — revenue <em>and</em> EPS, every quarter</div></div>
  <div class="stat"><div class="k neg">4 / 10</div><div class="l">Next-day closes that were green</div></div>
  <div class="stat"><div class="k">9.4%</div><div class="l">Average absolute next-day move — about 2× the ATR going in</div></div>
  <div class="stat"><div class="k">0.75</div><div class="l">Correlation of MU's next month with SOXX's next month</div></div>
  <div class="stat"><div class="k">0.02</div><div class="l">Correlation of the reaction day with the next month</div></div>
</div>

<section>
  <h2>The ten prints</h2>
  <p class="lede">Every report is after the close, so the last pre-news price is the report-day close and the reaction lands the next session. Revenue in $bn; guidance is the next-quarter revenue midpoint Micron gave on the call, against the street number going in.</p>
  {C['ledger']}
</section>

<section>
  <h2>Reaction, print by print</h2>
  <figure>
    {C['c1']}
    <figcaption>MU's next-day close after each report, with the guidance bar beneath each column. <b>Two of the three worst days followed guidance that was at or below consensus</b> — and the only two prints where guidance disappointed produced −16.2% and −7.1%.</figcaption>
  </figure>
</section>

<section>
  <h2>What we found</h2>
  <div class="findings">

    <div class="finding">
      <div><h3>The beat is not the trade</h3><span class="chip s-strong tag">strong</span></div>
      <div>
        <p class="body">Ten prints, ten double beats. Average EPS surprise <b>+41%</b>, average revenue surprise <b>+6.8%</b> — and a median next-day move of <b>−1.9%</b>. Beating consensus carries no information here because it is fully priced: Micron has not missed an EPS number since March 2024, and the market has learned that.</p>
        <p class="body">The rank correlation between EPS surprise and the next-day close is +0.30; for revenue surprise, +0.44. Both are inside the noise at n=10.</p>
      </div>
    </div>

    <div class="finding">
      <div><h3>Guidance is the print</h3><span class="chip s-strong tag">strong</span></div>
      <div>
        <p class="body">The forward number is the highest-signal fundamental input in the sample — rank correlation <b>+0.56</b> with the next-day close, against +0.30 for EPS surprise.</p>
        <p class="body">The two cleanest cases sit at opposite ends. In December 2024 Micron beat on both lines and then guided next-quarter revenue to <b>$7.9bn against a $9.0bn street number</b>; the stock lost 16.2%, the worst day in the sample. In September 2024 the revenue beat was a thin +1.3%, but the guide came in about 5% above street and the stock gained 14.7%.</p>
        <div class="ev">Guide ≥ 2% above street (n=8) → next-day avg <b>+4.9%</b><br>Guide at or below street (n=2) → next-day avg <b>−11.7%</b></div>
      </div>
    </div>

    <div class="finding">
      <div><h3>…but guidance stopped being enough</h3><span class="chip s-moderate tag">moderate</span></div>
      <div>
        <p class="body">On the last four reports Micron guided 5%, 26%, 38% and 22% above consensus. It fell on two of them. March 2026 is the sharpest example: a guide of <b>$33.5bn against a $24.3bn street number</b> — the largest raise in the sample — and the stock still closed down 3.8%.</p>
        <p class="body">Once a huge raise is the consensus expectation, delivering one is neutral. The bar the stock trades against is not the published estimate, it is the whisper.</p>
      </div>
    </div>

    <div class="finding">
      <div><h3>The sector decides the month</h3><span class="chip s-strong tag">strong</span></div>
      <div>
        <p class="body">A month after the print, where MU sits is explained far better by what the semis did than by anything in the report. Correlation with SOXX over the following 21 sessions is <b>0.75</b>, with a slope of about <b>1.39</b> — MU moves roughly 1.4× the sector. Against QQQ it is 0.45; against SPY, 0.31.</p>
        <p class="body">That ordering matters: this is a <b>sector</b> trade, not a market-beta trade. The broad index is close to irrelevant.</p>
      </div>
    </div>

    <div class="finding">
      <div><h3>The reaction day tells you nothing about the month</h3><span class="chip s-strong tag">strong</span></div>
      <div>
        <p class="body">Correlation between the next-day close and the following 21 sessions is <b>0.02</b> — no relationship at all.</p>
        <p class="body">The two extremes make the point. December 2024 fell 16.2% on the day and then rose <b>20.4%</b> over the next month. June 2026 rose 15.7% — the best reaction in the sample — and then fell <b>25.8%</b>, as the whole memory complex went into a bear market on profit-taking rather than on any company news.</p>
      </div>
    </div>

    <div class="finding">
      <div><h3>Up-gaps fade intraday, 5 out of 5</h3><span class="chip s-moderate tag">moderate</span></div>
      <div>
        <p class="body">Every one of the five up-gaps closed below where it opened: average open <b>+13.8%</b>, average close <b>+10.8%</b> — a 3.0-point give-back, with no exceptions. Down-gaps did the opposite, extending from −6.3% at the open to −7.6% at the close.</p>
        <p class="body">Two prints reversed an after-hours move entirely. In March 2025 the stock rose about 6% after hours on a beat and an above-consensus guide, then closed the next session <b>−8.0%</b>. In June 2024 it came in above the high end of its own guidance and closed <b>−7.1%</b>.</p>
      </div>
    </div>

    <div class="finding">
      <div><h3>A hot run-in gets repaid</h3><span class="chip s-moderate tag">moderate</span></div>
      <div>
        <p class="body">Rank correlation between the 5-day run into the print and the 5 days after it is <b>−0.72</b>. Split at the month: the five prints where MU came in up 15%+ over the prior 21 sessions averaged <b>−4.5%</b> in the following week; the cooler five averaged +0.4%.</p>
        <p class="body">Both prints with RSI above 70 going in (June and September 2025) closed red on the day despite guides 8% and 5% above street.</p>
      </div>
    </div>

    <div class="finding">
      <div><h3>Size it for ±9%</h3><span class="chip s-strong tag">strong</span></div>
      <div>
        <p class="body">Mean absolute next-day move <b>9.4%</b>, range <b>−16.2% to +15.7%</b>, and it has been at least 2.8% every single time. That is roughly <b>2× the ATR</b> Micron carries into the print. The distribution is wide and close to two-sided — there is no version of this event that is small.</p>
      </div>
    </div>

  </div>
</section>

<section>
  <h2>Guidance against reaction</h2>
  <p class="lede">Each point is one report: how far the next-quarter revenue guide landed above or below the street, against what the stock did the next day. Filled points are prints where SOXX was above its 50-day moving average.</p>
  <figure>
    {C['c2']}
    <figcaption>The upward tilt is real but loose. <b>The two points left of zero are the two worst days in the sample</b> — guidance below consensus is reliably punished. Above consensus, the outcome scatters from −3.8% to +15.7%, which is where positioning and the sector tape take over.</figcaption>
  </figure>
</section>

<section>
  <h2>MU against the sector, one month on</h2>
  <figure>
    {C['c3']}
    <figcaption>The 21 sessions after each print. The two move together in <b>nine of ten</b> cases — same sign, MU amplified. The exception is March 2026, when MU went sideways (+0.9%) while SOXX ran +22.7%, after a print that had already carried the stock 94% in the prior quarter.</figcaption>
  </figure>
</section>

<section>
  <h2>Correlations</h2>
  <p class="lede">Pearson unless noted. With ten observations these are directional, not statistical proof — read the ordering, not the decimals.</p>
  {C['corr']}
</section>

<section>
  <h2>How the next print sets up</h2>
  <div class="setup">
    <div class="hd">
      <h3>FQ4 2026 · expected 30 September 2026</h3>
      <span class="chip flat">prices as of 11 Sep 2026</span>
    </div>
    <div class="grid2">
      <div class="cell"><div class="l">MU last</div><div class="v">$975.26</div></div>
      <div class="cell"><div class="l">RSI (14)</div><div class="v">53 <small>mid-range</small></div></div>
      <div class="cell"><div class="l">vs 50-day</div><div class="v">+5.0%</div></div>
      <div class="cell"><div class="l">vs 200-day</div><div class="v">+56.8%</div></div>
      <div class="cell"><div class="l">From 52w high</div><div class="v">−19.6%</div></div>
      <div class="cell"><div class="l">21-day run-in</div><div class="v">+7.0%</div></div>
      <div class="cell"><div class="l">SOXX vs 50-day</div><div class="v">−0.8%</div></div>
      <div class="cell"><div class="l">Guide on the table</div><div class="v">$50.0bn</div></div>
    </div>
    <div class="ft">
      <p>Mapped onto the patterns above: the run-in is <b>cool</b> by this sample's standards (+7.0% over 21 sessions against a +30% average for the hot half) and RSI is mid-range at 53 — both of which have sat on the better side of the record. The stock is 19.6% off its high rather than making one.</p>
      <p>The sector is the open question. SOXX is fractionally below its 50-day and 19.5% off its own high after the July–August memory drawdown. That is the condition with the strongest measured link to where MU trades a month later — and it is the one variable the earnings report itself does not control.</p>
      <p>The company guided FQ4 revenue to $50bn on 24 June. Consensus has moved since; what matters on the day is the FQ1 2027 number that comes with it, not the quarter being reported.</p>
    </div>
  </div>
</section>

<section>
  <h2>Reading the limits</h2>
  <ul class="caveats">
    <li><b>Ten observations, one regime.</b> The whole sample sits inside the AI memory upcycle. Every bucket split here is 2–6 events per side; treat the cross-tabs as description, not as an edge with a t-statistic.</li>
    <li><b>Guidance consensus is the soft number.</b> Micron's guide midpoints come from its own releases. The street number it is measured against is verified for FQ1'25, FQ4'25 and FQ2'26, and approximate for FQ2'24, FQ3'24 and FQ2'25 — consensus is not a single published figure and vendors differ by a few percent.</li>
    <li><b>Estimates vary by source.</b> The EPS and revenue consensus used here is investing.com's. Yahoo and others differ slightly (for June 2026, a $20.49 estimate here against $20.69 at Yahoo and $20.28 at another vendor) — enough to move a surprise percentage, not enough to move a conclusion.</li>
    <li><b>Guidance was not scored blind.</b> The guide-versus-street classification was assembled after the fact from contemporaneous coverage, so it carries some hindsight in how "above" and "below" were read.</li>
    <li><b>No options data.</b> Realised moves are compared to ATR, not to what the straddle was pricing. Whether a 9.4% average move was cheap or expensive is a question this study cannot answer.</li>
  </ul>
</section>

<footer>
  <p>Prices from Yahoo Finance via yfinance (daily, unadjusted closes; returns on close-to-close). EPS and revenue estimates against actuals from <a href="https://www.investing.com/equities/micron-tech-earnings">investing.com</a>, cross-checked against Yahoo's earnings history. Guidance midpoints from Micron's quarterly releases at <a href="https://investors.micron.com">investors.micron.com</a>; street context from CNBC, Barchart, Futurum and Motley Fool coverage at each print. Indicators computed locally — RSI Wilder-smoothed, ATR 14-day, moving averages simple. Not investment advice.</p>
</footer>

</div>
"""
open("mu_earnings_playbook.html","w").write(HTML)
print("wrote mu_earnings_playbook.html", len(HTML), "bytes")
