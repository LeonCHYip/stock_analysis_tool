"""Render the study's CSVs into a self-contained HTML report.

Regenerating the page is part of the study rather than a one-off write-up:
every figure in the prose is interpolated from the CSVs, so a rerun after a
data refresh produces a page that is still true.

    uv run python -m study.xsec.publish
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from . import charts, config

GROUPS = ["Semiconductors", "Financial Services", "Energy", "Utilities"]
TABLE_GROUPS = ["Semiconductors", "Technology", "Financial Services", "Utilities",
                "Energy", "Healthcare", "Industrials", "ALL"]

BANDS = [
    {"start": "2024-01-02", "end": "2024-06-28", "label": "AI melt-up", "cls": ""},
    {"start": "2024-07-01", "end": "2024-12-31", "label": "Rotation", "cls": "band-hi"},
    {"start": "2025-01-02", "end": "2025-04-02", "label": "Run-up", "cls": "band-hi"},
    {"start": "2025-04-03", "end": "2025-04-08", "label": "", "cls": "band-shock"},
    {"start": "2025-04-09", "end": config.STUDY_END, "label": "Recovery", "cls": ""},
]


def _esc(s) -> str:
    return (str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))


def _fmt(v, nd=1, plus=False, dash="—") -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return dash
    return f"{v:+.{nd}f}" if plus else f"{v:.{nd}f}"


def table(rows: list[list], head: list[str], cls: str = "",
          highlight: str | None = None) -> str:
    """A data table. Numeric cells get tabular figures via CSS."""
    th = "".join(f"<th>{_esc(h)}</th>" for h in head)
    body = []
    for r in rows:
        mark = ' class="row-focus"' if highlight and r[0] == highlight else ""
        tds = "".join(
            f'<td class="{"lab" if i == 0 else "num"}">{c}</td>'
            for i, c in enumerate(r)
        )
        body.append(f"<tr{mark}>{tds}</tr>")
    return (f'<div class="scroll"><table class="{cls}">'
            f"<thead><tr>{th}</tr></thead><tbody>{''.join(body)}</tbody>"
            f"</table></div>")


def sign(v, nd=1, unit="%") -> str:
    """A signed number wearing a good/bad class, for return-style figures."""
    if v is None or pd.isna(v):
        return "—"
    cls = "pos" if v > 0 else ("neg" if v < 0 else "flat")
    return f'<span class="{cls}">{v:+.{nd}f}{unit}</span>'


def build(out_dir: Path) -> str:
    t = {p.stem: pd.read_csv(p) for p in out_dir.glob("*.csv")}
    ret, dvp = t["returns_by_group"], t["delivery_vs_price"]
    ep, rs, dp = t["explanatory_power"], t["reaction_slopes"], t["drift_portfolios"]
    fm, sl, gi = t["fm_summary"], t["sector_level_link"], t["group_index"]

    n_stocks = int(ret[ret["group"] == "ALL"]["n_stocks"].max())
    n_reports = int(dvp[(dvp["window"] == "R4_RECOVERY")
                        & (dvp["group"] == "ALL")]["n_reports"].iloc[0])

    def row(window, group, frame=dvp):
        d = frame[(frame["window"] == window) & (frame["group"] == group)]
        return d.iloc[0] if len(d) else None

    sm, fn, en, al = (row("S_ROTATION", g) for g in
                      ["Semiconductors", "Financial Services", "Energy", "ALL"])
    sm0 = row("R0_AI_MELTUP", "Semiconductors")

    # ── hero chart: rebased group index ──────────────────────────────────────
    gi["date"] = pd.to_datetime(gi["date"])
    series = {}
    for g in GROUPS + ["ALL"]:
        d = gi[gi["group"] == g].sort_values("date").iloc[::5]
        if d.empty:
            continue
        # Short names: these are drawn as end-of-line labels inside a fixed
        # right gutter, and the full sector names do not fit there.
        name = {"ALL": "Universe", "Financial Services": "Financials",
                "Semiconductors": "Semis"}.get(g, g)
        series[name] = [(x.strftime("%Y-%m-%d"), float(y))
                        for x, y in zip(d["date"], d["index_level"])]
    # Universe last so the dashed reference line draws on top of the bands but
    # takes the neutral colour, not a categorical slot.
    series = {k: series[k] for k in list(series) if k != "Universe"} | \
             {"Universe": series["Universe"]}
    hero = charts.line_chart(series, BANDS, height=420)

    # ── P/E decomposition ────────────────────────────────────────────────────
    pe_rows = []
    for g in TABLE_GROUPS:
        r = row("S_ROTATION", g)
        if r is None or pd.isna(r.median_pe_chg_pct):
            continue
        pe_rows.append({"label": g if g != "ALL" else "Universe",
                        "eps": float(r.median_eps_growth_pct),
                        "pe": float(r.median_pe_chg_pct)})
    pe_series = [("eps", "Trailing EPS growth"), ("pe", "Multiple change")]
    pe_chart = charts.grouped_hbar(pe_rows, pe_series)

    # ── drift decay ──────────────────────────────────────────────────────────
    d = dp[(dp["scope"] == "universe") & (dp["signal"] == "sue")
           & (dp["bucket"] == "Q5-Q1") & (dp["window"] == "S_ROTATION")]
    names = {"one_day_change": "Report day", "fwd_1d_pct": "+1d",
             "fwd_5d_pct": "+5d", "fwd_21d_pct": "+21d", "fwd_63d_pct": "+63d"}
    drift_rows = []
    for k, lab in names.items():
        r = d[d["outcome"] == k]
        if r.empty:
            continue
        r = r.iloc[0]
        drift_rows.append({"label": lab, "v": float(r["mean"]),
                           "sub": f"t = {r['t_stat']:.1f}"
                           if pd.notna(r["t_stat"]) else ""})
    drift_chart = charts.vbar_chart(drift_rows, [("v", "Q5 − Q1 spread")],
                                    height=300, unit="pp")

    # ── reaction slope by window ─────────────────────────────────────────────
    slope = rs[(rs["signal"] == "sue") & (rs["outcome"] == "one_day_change")]
    wins = [("R0_AI_MELTUP", "2024 H1"), ("R1_ROTATION", "2024 H2"),
            ("R2_PRE_LIB", "2025 Q1"), ("R4_RECOVERY", "Post-tariff")]
    slope_rows = []
    for w, lab in wins:
        rec = {"label": lab}
        for key, g in (("semi", "Semiconductors"), ("fin", "Financial Services"),
                       ("all", "ALL")):
            s = slope[(slope["window"] == w) & (slope["group"] == g)]
            rec[key] = float(s.iloc[0]["slope_per_sd"]) if len(s) else None
        slope_rows.append(rec)
    slope_series = [("semi", "Semiconductors"), ("fin", "Financials"),
                    ("all", "Universe")]
    slope_chart = charts.vbar_chart(slope_rows, slope_series, height=320,
                                    unit="pp")

    # ── incremental R² ───────────────────────────────────────────────────────
    inc = ep[(ep["spec"] == "INCREMENTAL_fund_core") & (ep["outcome"] == "ret_pct")]
    inc_rows = []
    for w, lab in [("R0_AI_MELTUP", "2024 H1"), ("R1_ROTATION", "2024 H2"),
                   ("R2_PRE_LIB", "2025 Q1"), ("R3_SHOCK", "Tariff shock"),
                   ("R4_RECOVERY", "Post-tariff")]:
        rec = {"label": lab}
        for key, basis in (("ex", "exante"), ("co", "contemp")):
            s = inc[(inc["window"] == w) & (inc["basis"] == basis)]
            rec[key] = float(s.iloc[0]["r2"]) * 100 if len(s) else None
        inc_rows.append(rec)
    inc_series = [("ex", "Known before the window"),
                  ("co", "Reported during the window")]
    inc_chart = charts.vbar_chart(inc_rows, inc_series, height=300, unit="%")

    ex_vals = inc[inc["basis"] == "exante"]["r2"] * 100
    co_vals = inc[inc["basis"] == "contemp"]["r2"] * 100
    sc_full = ep[(ep["spec"] == "sector+controls") & (ep["outcome"] == "ret_pct")
                 ]["r2"] * 100

    # ── tables ───────────────────────────────────────────────────────────────
    rot_rows = []
    for g in TABLE_GROUPS:
        r = row("S_ROTATION", g)
        if r is None:
            continue
        rot_rows.append([
            g if g != "ALL" else "Universe median",
            f"{int(r.n_stocks)}", sign(r.median_ret_pct),
            sign(r.excess_vs_universe_pct, unit="pp"),
            _fmt(r.eps_beat_rate_pct, 0) + "%", _fmt(r.rev_beat_rate_pct, 0) + "%",
            sign(r.median_rev_yoy_pct), sign(r.median_eps_yoy_pct),
            sign(r.median_1d_reaction_pct, 2),
        ])
    rot_table = table(
        rot_rows,
        ["Group", "n", "Median return", "vs universe", "EPS beat", "Rev beat",
         "Rev YoY", "EPS YoY", "Median report-day move"],
        highlight="Semiconductors",
    )

    fm_rows = []
    labels = {"z_sue": "Standardised surprise (SUE)", "z_rev_sur": "Revenue surprise",
              "z_rev_yoy": "Revenue YoY growth", "z_eps_yoy": "EPS YoY growth",
              "z_rev_yoy_accel": "Revenue growth acceleration",
              "z_beat_streak": "Consecutive beats", "z_log_mcap": "Size (log market cap)"}
    for r in fm.sort_values("t_stat", ascending=False).itertuples():
        fm_rows.append([
            labels.get(r.term, r.term), f"{int(r.n_periods)}",
            _fmt(r.mean_coef, 2, plus=True), _fmt(r.sd_coef, 2),
            f'<span class="{"strong" if abs(r.t_stat) >= 2 else ""}">'
            f'{r.t_stat:+.2f}</span>',
            _fmt(r.pct_positive, 0) + "%",
        ])
    fm_table = table(fm_rows, ["Signal", "Quarters", "Mean coef (pp per σ)",
                               "SD across quarters", "t", "% quarters positive"])

    q1 = sl[(sl["window"] == "R2_PRE_LIB")
            & (sl["metric"] == "rev_beat_rate_pct")].iloc[0]
    q4 = sl[(sl["window"] == "R4_RECOVERY")
            & (sl["metric"] == "median_rev_yoy_accel_pp")].iloc[0]

    semi_slopes = slope[slope["group"] == "Semiconductors"].set_index("window")
    fin_q1 = slope[(slope["group"] == "Financial Services")
                   & (slope["window"] == "R2_PRE_LIB")].iloc[0]

    day = dp[(dp["scope"] == "universe") & (dp["signal"] == "sue")
             & (dp["bucket"] == "Q5-Q1") & (dp["outcome"] == "one_day_change")]

    return HTML.format(
        css=CSS, script=SCRIPT,
        n_stocks=f"{n_stocks:,}",
        n_reports=f"{n_reports:,}",
        study_start=config.STUDY_START, study_end=config.STUDY_END,
        hero=hero,
        hero_legend=charts.legend([(g, g) for g in GROUPS]),
        rot_table=rot_table,
        semi_ret=f"{sm.median_ret_pct:.0f}",
        semi_excess=f"{abs(sm.median_ret_pct - fn.median_ret_pct):.0f}",
        fin_ret=f"{fn.median_ret_pct:.0f}",
        eng_ret=f"{en.median_ret_pct:.1f}",
        eng_excess=f"{abs(en.excess_vs_universe_pct):.1f}",
        eng_eps=f"{en.median_eps_yoy_pct:.1f}",
        semi_epsbeat=f"{sm.eps_beat_rate_pct:.0f}",
        semi_revbeat=f"{sm.rev_beat_rate_pct:.0f}",
        semi_accel=f"{sm.median_rev_yoy_accel_pp:+.1f}",
        semi_react=f"{sm.median_1d_reaction_pct:.2f}",
        semi_r0_ret=f"{sm0.median_ret_pct:+.0f}",
        semi_r0_rev=f"{sm0.median_rev_yoy_pct:+.1f}",
        semi_r0_eps=f"{sm0.median_eps_yoy_pct:+.1f}",
        semi_rot_rev=f"{sm.median_rev_yoy_pct:+.1f}",
        semi_rot_eps=f"{sm.median_eps_yoy_pct:+.1f}",
        all_react=f"{al.median_1d_reaction_pct:+.2f}",
        pe_chart=pe_chart, pe_legend=charts.legend(pe_series),
        semi_pe_start=f"{sm.median_pe_start:.0f}",
        semi_pe_end=f"{sm.median_pe_end:.0f}",
        semi_pe_chg=f"{sm.median_pe_chg_pct:.0f}",
        semi_eps_growth=f"{sm.median_eps_growth_pct:+.1f}",
        fin_eps_growth=f"{fn.median_eps_growth_pct:+.1f}",
        fin_pe_chg=f"{fn.median_pe_chg_pct:+.1f}",
        inc_chart=inc_chart, inc_legend=charts.legend(inc_series),
        ex_lo=f"{ex_vals.min():.2f}", ex_hi=f"{ex_vals.max():.2f}",
        co_lo=f"{co_vals.min():.2f}", co_hi=f"{co_vals.max():.2f}",
        sc_lo=f"{sc_full.min():.0f}", sc_hi=f"{sc_full.max():.0f}",
        drift_chart=drift_chart,
        day_lo=f"{day['mean'].min():.1f}", day_hi=f"{day['mean'].max():.1f}",
        day_t_lo=f"{day['t_stat'].min():.0f}", day_t_hi=f"{day['t_stat'].max():.0f}",
        slope_chart=slope_chart, slope_legend=charts.legend(slope_series),
        semi_s_h1=f"{semi_slopes.loc['R0_AI_MELTUP', 'slope_per_sd']:.1f}",
        semi_s_h2=f"{semi_slopes.loc['R1_ROTATION', 'slope_per_sd']:.1f}",
        semi_s_q1=f"{semi_slopes.loc['R2_PRE_LIB', 'slope_per_sd']:+.2f}",
        semi_t_q1=f"{semi_slopes.loc['R2_PRE_LIB', 't_stat']:.2f}",
        fin_s_q1=f"{fin_q1.slope_per_sd:.1f}", fin_t_q1=f"{fin_q1.t_stat:.1f}",
        fm_table=fm_table,
        q1_rho=f"{q1.rho:.2f}", q1_p=f"{q1.p_value:.3f}",
        q4_rho=f"{q4.rho:.2f}", q4_p=f"{q4.p_value:.3f}",
    )


SCRIPT = """
(function(){
  var tip = document.getElementById('tip');
  document.addEventListener('mouseover', function(e){
    var m = e.target.closest('[data-tip]');
    if(!m){ tip.classList.remove('on'); return; }
    tip.textContent = m.getAttribute('data-tip');
    tip.classList.add('on');
  });
  document.addEventListener('mousemove', function(e){
    if(!tip.classList.contains('on')) return;
    var x = e.clientX + 14, y = e.clientY + 16;
    var r = tip.getBoundingClientRect();
    if(x + r.width > window.innerWidth - 8) x = e.clientX - r.width - 14;
    if(y + r.height > window.innerHeight - 8) y = e.clientY - r.height - 16;
    tip.style.left = x + 'px'; tip.style.top = y + 'px';
  });
})();
"""


CSS = """
:root{
  color-scheme: light;
  --ground:#f4f6f8; --surface:#ffffff; --surface-2:#eaeef3; --raise:#ffffff;
  --ink:#0d151e; --ink-2:#4a5765; --ink-3:#7d8a99;
  --rule:#d9e0e8; --rule-2:#bcc7d4;
  --accent:#1f4e8c;
  --pos:#0f7a55; --neg:#c0392f;
  --s1:#eb6834; --s2:#2a78d6; --s3:#1baf7a; --s4:#4a3aa7;
  --band:#e7ecf2; --band-hi:#dde5ef; --band-shock:#f2d9d5;
}
@media (prefers-color-scheme: dark){
  :root:not([data-theme="light"]){
    color-scheme: dark;
    --ground:#0e1319; --surface:#161c24; --surface-2:#1c242e; --raise:#1a222c;
    --ink:#e6ecf3; --ink-2:#a3b0bf; --ink-3:#76838f;
    --rule:#252e39; --rule-2:#36414e;
    --accent:#7aa9e8;
    --pos:#2fb383; --neg:#e2645a;
    --s1:#d95926; --s2:#3987e5; --s3:#199e70; --s4:#9085e9;
    --band:#161d26; --band-hi:#1a2330; --band-shock:#3a1f1c;
  }
}
:root[data-theme="dark"]{
  color-scheme: dark;
  --ground:#0e1319; --surface:#161c24; --surface-2:#1c242e; --raise:#1a222c;
  --ink:#e6ecf3; --ink-2:#a3b0bf; --ink-3:#76838f;
  --rule:#252e39; --rule-2:#36414e;
  --accent:#7aa9e8;
  --pos:#2fb383; --neg:#e2645a;
  --s1:#d95926; --s2:#3987e5; --s3:#199e70; --s4:#9085e9;
  --band:#161d26; --band-hi:#1a2330; --band-shock:#3a1f1c;
}

*{box-sizing:border-box}
body{
  margin:0; background:var(--ground); color:var(--ink);
  font-family:"IBM Plex Sans","Helvetica Neue",Arial,sans-serif;
  font-size:16px; line-height:1.62; -webkit-font-smoothing:antialiased;
}
.wrap{max-width:1000px; margin:0 auto; padding:0 28px 96px}
.col{max-width:68ch}

/* ── masthead ─────────────────────────────────────────── */
header.top{
  border-bottom:1px solid var(--rule-2); padding:56px 0 32px; margin-bottom:8px;
}
.eyebrow{
  font-family:"IBM Plex Mono",ui-monospace,monospace; font-size:11.5px;
  letter-spacing:.14em; text-transform:uppercase; color:var(--ink-3);
  display:flex; flex-wrap:wrap; gap:6px 18px; margin-bottom:22px;
}
h1{
  font-family:Newsreader,Georgia,serif; font-weight:500; font-size:clamp(34px,5.2vw,54px);
  line-height:1.06; letter-spacing:-.015em; margin:0 0 20px; text-wrap:balance;
}
h1 em{font-style:italic; color:var(--accent)}
.standfirst{font-size:19px; line-height:1.55; color:var(--ink-2); max-width:60ch; margin:0}
.standfirst strong{color:var(--ink); font-weight:600}

.tiles{
  display:grid; grid-template-columns:repeat(auto-fit,minmax(190px,1fr));
  gap:1px; background:var(--rule); border:1px solid var(--rule);
  margin:38px 0 0;
}
.tile{background:var(--surface); padding:18px 20px 16px}
.tile .k{
  font-family:"IBM Plex Mono",monospace; font-size:11px; letter-spacing:.11em;
  text-transform:uppercase; color:var(--ink-3); display:block; margin-bottom:8px;
}
.tile .v{
  font-family:Newsreader,Georgia,serif; font-size:34px; line-height:1;
  font-variant-numeric:tabular-nums; display:block;
}
.tile .n{font-size:13px; color:var(--ink-2); display:block; margin-top:7px; line-height:1.4}

/* ── sections ─────────────────────────────────────────── */
section{padding-top:52px}
.slug{
  font-family:"IBM Plex Mono",monospace; font-size:11px; letter-spacing:.13em;
  text-transform:uppercase; color:var(--accent); margin-bottom:10px;
}
h2{
  font-family:Newsreader,Georgia,serif; font-weight:500; font-size:clamp(25px,3.2vw,33px);
  line-height:1.15; letter-spacing:-.01em; margin:0 0 6px; text-wrap:balance;
}
.deck{color:var(--ink-2); margin:0 0 24px; max-width:66ch; font-size:15.5px}
p{margin:0 0 17px; max-width:68ch}
p strong{font-weight:600}
.pull{
  border-left:3px solid var(--accent); padding:2px 0 2px 20px; margin:26px 0;
  font-family:Newsreader,Georgia,serif; font-size:20px; line-height:1.45;
  color:var(--ink); max-width:56ch;
}

/* ── figures ──────────────────────────────────────────── */
figure{margin:26px 0 30px}
.chart{width:100%; height:auto; display:block; overflow:visible}
.grid{stroke:var(--rule); stroke-width:1}
.baseline{stroke:var(--rule-2); stroke-width:1.5}
.band{fill:var(--band)}
.band-hi{fill:var(--band-hi)}
.band-shock{fill:var(--band-shock)}
.band-label{
  font-family:"IBM Plex Mono",monospace; font-size:10px; letter-spacing:.1em;
  text-transform:uppercase; fill:var(--ink-3); text-anchor:middle;
}
.tick{font-family:"IBM Plex Mono",monospace; font-size:11px; fill:var(--ink-3);
      font-variant-numeric:tabular-nums}
.tick-y{text-anchor:end}
.tick-x{text-anchor:start}
.tick-sub{font-size:10px}
.mid{text-anchor:middle}
.cat-label{font-size:13px; fill:var(--ink-2); text-anchor:end}
.end-label{font-size:12.5px; font-weight:600}
.end-value{font-family:"IBM Plex Mono",monospace; font-size:11.5px; opacity:.75}
.bar-value{font-family:"IBM Plex Mono",monospace; font-size:11px; fill:var(--ink-2);
           font-variant-numeric:tabular-nums}
.mark{cursor:default}
.mark:hover{opacity:.78}
figcaption{
  font-size:13px; color:var(--ink-3); margin-top:10px; max-width:66ch;
  line-height:1.5;
}
.legend{display:flex; flex-wrap:wrap; gap:8px 20px; margin:14px 0 2px;
        font-size:13px; color:var(--ink-2)}
.lg-item{display:inline-flex; align-items:center; gap:7px}
.lg-item i{width:11px; height:11px; border-radius:2px; display:inline-block}

/* ── tables ───────────────────────────────────────────── */
.scroll{overflow-x:auto; margin:22px 0 8px; border-block:1px solid var(--rule-2)}
table{border-collapse:collapse; width:100%; font-size:14px}
th{
  font-family:"IBM Plex Mono",monospace; font-size:10.5px; letter-spacing:.08em;
  text-transform:uppercase; color:var(--ink-3); font-weight:500;
  text-align:right; padding:11px 14px; white-space:nowrap;
  border-bottom:1px solid var(--rule-2);
}
th:first-child{text-align:left}
td{padding:9px 14px; border-bottom:1px solid var(--rule); white-space:nowrap}
tbody tr:last-child td{border-bottom:none}
td.num{text-align:right; font-family:"IBM Plex Mono",monospace; font-size:13px;
       font-variant-numeric:tabular-nums}
td.lab{font-weight:500}
tr.row-focus{background:var(--surface-2)}
tr.row-focus td.lab{font-weight:650}
.pos{color:var(--pos)} .neg{color:var(--neg)} .flat{color:var(--ink-3)}
.strong{font-weight:700}

/* ── findings ─────────────────────────────────────────── */
.findings{list-style:none; padding:0; margin:26px 0 0; counter-reset:f}
.findings li{
  counter-increment:f; padding:20px 0 20px 52px; position:relative;
  border-top:1px solid var(--rule); max-width:70ch;
}
.findings li::before{
  content:counter(f,decimal-leading-zero); position:absolute; left:0; top:20px;
  font-family:"IBM Plex Mono",monospace; font-size:12px; color:var(--accent);
  letter-spacing:.06em;
}
.findings b{display:block; font-size:17px; margin-bottom:5px; font-weight:600}
.findings span{color:var(--ink-2); font-size:15px}

footer{
  margin-top:64px; padding-top:26px; border-top:1px solid var(--rule-2);
  font-size:13.5px; color:var(--ink-3);
}
footer h3{
  font-family:"IBM Plex Mono",monospace; font-size:11px; letter-spacing:.13em;
  text-transform:uppercase; color:var(--ink-2); margin:0 0 12px; font-weight:500;
}
footer ul{margin:0 0 22px; padding-left:18px; max-width:70ch}
footer li{margin-bottom:7px}
code{font-family:"IBM Plex Mono",monospace; font-size:.9em;
     background:var(--surface-2); padding:1px 5px; border-radius:3px}

#tip{
  position:fixed; pointer-events:none; opacity:0; transition:opacity .12s;
  background:var(--ink); color:var(--ground); font-size:12.5px;
  font-family:"IBM Plex Mono",monospace; padding:6px 9px; border-radius:4px;
  z-index:10; white-space:nowrap;
}
#tip.on{opacity:1}
@media (prefers-reduced-motion:reduce){*{transition:none!important}}
@media (max-width:640px){
  .wrap{padding:0 18px 64px}
  .tile .v{font-size:28px}
}
"""


HTML = """<title>Delivery Without Reward</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Newsreader:ital,opsz,wght@0,6..72,400;0,6..72,500;1,6..72,400&family=IBM+Plex+Mono:wght@400;500&family=IBM+Plex+Sans:wght@400;500;600;700&display=swap">
<style>{css}</style>

<div class="wrap">
<header class="top">
  <div class="eyebrow">
    <span>Cross-sectional equity study</span><span>{n_stocks} US equities</span>
    <span>{n_reports} quarterly reports</span><span>{study_start} → {study_end}</span>
  </div>
  <h1>Semiconductors delivered the best earnings in the market<br><em>and were sold anyway</em></h1>
  <p class="standfirst">Testing whether revenue and EPS growth, and surprise against
  estimate, explain how {n_stocks} US stocks actually traded from 2024 through the
  tariff shock and out the other side. <strong>They explain the day a company
  reports, and almost nothing else.</strong></p>

  <div class="tiles">
    <div class="tile"><span class="k">Semis, 2024H2 → Apr 2025</span>
      <span class="v neg">{semi_ret}%</span>
      <span class="n">Median return, worst of 13 groups</span></div>
    <div class="tile"><span class="k">Their trailing EPS</span>
      <span class="v pos">{semi_eps_growth}%</span>
      <span class="n">Earnings grew through the fall</span></div>
    <div class="tile"><span class="k">Their multiple</span>
      <span class="v neg">{semi_pe_chg}%</span>
      <span class="n">{semi_pe_start}× to {semi_pe_end}× trailing</span></div>
    <div class="tile"><span class="k">Fundamentals' added R²</span>
      <span class="v">{ex_lo}–{ex_hi}%</span>
      <span class="n">Over sector, size and momentum, ex ante</span></div>
  </div>
</header>

<section>
  <div class="slug">Median of rebased prices · base 100 at 2 Jan 2024</div>
  <h2>The rotation, measured</h2>
  <p class="deck">Each line is the median stock in its group, rebased to 100. The
  shaded blocks are the study's five windows; the narrow red band is the four
  sessions after the 2 April 2025 tariff announcement.</p>
  <figure>
    {hero_legend}
    {hero}
    <figcaption>Semiconductors led the market into mid-2024, gave up more than a
    third from July 2024 to the tariff shock, and then out-ran everything in the
    recovery. Financials climbed straight through the window semis fell in.</figcaption>
  </figure>
  {rot_table}
  <p style="margin-top:18px">Over the rotation window — 1 July 2024 to 2 April
  2025 — semiconductors returned <strong>{semi_ret}%</strong> at the median
  against <strong>+{fin_ret}%</strong> for financials, a spread of
  {semi_excess} percentage points. Only 12% of semiconductor stocks finished the
  window up.</p>
  <p>Energy did not benefit. Its median stock returned <strong>{eng_ret}%</strong>,
  {eng_excess}pp <em>below</em> the universe and the second-worst group after
  semis, on genuinely deteriorating earnings — trailing EPS at the median
  fell {eng_eps}%. Money leaving semiconductors in 2024H2 went to financials and
  utilities. Energy's turn came in the post-tariff recovery, a year later.</p>
</section>

<section>
  <div class="slug">Price = P/E × trailing EPS</div>
  <h2>The business, or the price of the business?</h2>
  <p class="deck">A window's price move splits exactly into earnings growth and
  re-rating. That split separates <em>the company got worse</em> from <em>the
  market decided to pay less for the same company</em>.</p>
  <figure>
    {pe_legend}
    {pe_chart}
    <figcaption>Median change over 1 July 2024 – 2 April 2025, by group. Trailing
    EPS is the sum of the last four reported quarters; the multiple is the
    split-adjusted price over that sum.</figcaption>
  </figure>
  <p>The median semiconductor grew trailing EPS <strong>{semi_eps_growth}%</strong>
  while its multiple fell <strong>{semi_pe_chg}%</strong>, from {semi_pe_start}× to
  {semi_pe_end}×. The median financial grew trailing EPS
  <strong>{fin_eps_growth}%</strong> — barely different — and was re-rated
  <strong>{fin_pe_chg}%</strong>.</p>
  <div class="pull">Two groups delivered near-identical earnings growth. One lost a
  third of its multiple; the other gained. Essentially none of the gap between
  them was fundamental.</div>
  <p>The earnings record was not merely adequate. Semiconductors had the
  <strong>highest</strong> beat rates of any group in the window —
  {semi_epsbeat}% of reports beat on EPS, {semi_revbeat}% on revenue — with revenue
  growth accelerating {semi_accel}pp, again the highest. The median reaction to a
  semiconductor earnings report was <strong>{semi_react}%</strong>, against
  {all_react}% for the universe. Good numbers were being sold.</p>
  <p>The timing inverts the story you would expect. Through the first half of
  2024, while semiconductors returned <strong>{semi_r0_ret}%</strong>, their
  <em>trailing</em> numbers were still falling — median revenue
  {semi_r0_rev}% year on year and EPS {semi_r0_eps}%, coming off the 2023 trough.
  By the rotation window trailing growth had turned positive
  ({semi_rot_rev}% revenue, {semi_rot_eps}% EPS) and the group lost a quarter of
  its value. The market bought the sector when its reported fundamentals were at
  their worst and sold it as they recovered.</p>
</section>

<section>
  <div class="slug">Nested cross-sectional OLS · HC1 robust errors</div>
  <h2>How much do fundamentals explain?</h2>
  <p class="deck">Each stock's window return regressed on its earnings record,
  after sector, size, 12-1 momentum, volatility and trend are already in the
  model. The bars are what the earnings block adds on top.</p>
  <figure>
    {inc_legend}
    {inc_chart}
    <figcaption>Incremental R² (%) of standardised surprise, revenue and EPS
    growth over a baseline of sector dummies plus four price-and-size controls
    measured at each window's open.</figcaption>
  </figure>
  <p>Known in advance, the earnings record adds between <strong>{ex_lo}%</strong>
  and <strong>{ex_hi}%</strong> of explained variance. That is nothing. Scoring
  each stock instead on the reports it filed <em>during</em> the window — which is
  not a forecast, only a description — lifts it to {co_lo}–{co_hi}%. Still small.
  Sector membership plus size, momentum and volatility carry
  <strong>{sc_lo}–{sc_hi}%</strong>, an order of magnitude more.</p>
  <p>Extending the same test to the risk side changes nothing: fundamentals
  explain even less of a stock's drawdown or realised volatility over a window
  than they do of its direction.</p>
</section>

<section>
  <div class="slug">Quintile portfolios on standardised surprise</div>
  <h2>Where fundamentals do work: one day</h2>
  <p class="deck">Stocks sorted into five buckets by how far their EPS beat their
  own history of surprises, then the return of the top bucket minus the bottom,
  at each horizon after the report.</p>
  <figure>
    {drift_chart}
    <figcaption>Q5 − Q1 spread in percentage points, 1 July 2024 – 2 April 2025.
    The report-day figure is close-to-close over the session containing the
    release; later horizons run from that close.</figcaption>
  </figure>
  <p>On the day, the biggest beats beat the biggest misses by
  <strong>{day_lo}–{day_hi} percentage points</strong>, with t-statistics of
  {day_t_lo} to {day_t_hi}, in every window of the study. This is the strongest
  and most reliable relationship in the whole exercise.</p>
  <p>Then it stops. At one, five, twenty-one and sixty-three trading days after
  the report the spread is indistinguishable from zero, and where it reaches
  significance it is <em>negative</em>. There is no post-earnings drift to trade
  here: the market prices the surprise in a single session and does not keep
  paying for it.</p>
</section>

<section>
  <div class="slug">Announcement-day slope per 1σ of surprise</div>
  <h2>When the market stopped listening</h2>
  <p class="deck">How far a stock moved on results day for each standard
  deviation of earnings surprise. A slope near zero means beats were no longer
  being paid for at all.</p>
  <figure>
    {slope_legend}
    {slope_chart}
    <figcaption>OLS slope with HC1 robust standard errors, by group and window.
    The 2025 Q1 semiconductor bar is not significantly different from zero.</figcaption>
  </figure>
  <p>Semiconductor earnings were the <em>most</em> keenly priced of any group
  through 2024: a one-sigma surprise moved the stock {semi_s_h1}pp in the first
  half and {semi_s_h2}pp in the second. In the first quarter of 2025, into
  Liberation Day, that sensitivity vanished — a slope of
  <strong>{semi_s_q1}pp</strong> with a t-statistic of {semi_t_q1}, statistically
  indistinguishable from the market not reading the release. Financials over the
  same quarter sat at {fin_s_q1}pp, t = {fin_t_q1}.</p>
  <div class="pull">The numbers kept coming in ahead of estimates. The market
  simply stopped trading on them.</div>
</section>

<section>
  <div class="slug">Fama-MacBeth · quarterly cross-sections, sector-neutral</div>
  <h2>Averaged over every reporting quarter</h2>
  <p class="deck">The 63-day return after each report regressed on that quarter's
  fundamentals, quarter by quarter, then averaged. The t-statistic prices in how
  much the relationship moved between quarters — which is what a rotation is.</p>
  {fm_table}
  <p style="margin-top:18px">Only two effects survive the averaging, and neither
  is a growth rate. Size is the strongest single factor in the sample. Standardised
  surprise is weakly positive and borderline. Revenue and EPS growth have standard
  deviations across quarters three to five times their means: the sign flips from
  quarter to quarter.</p>
  <p>The negative loading on a run of consecutive beats is worth its own line — a
  long beat streak was mildly <em>bad</em> for the following quarter, which reads
  as a crowded expectations bar rather than a fundamental signal.</p>
  <p>At sector level the picture is the same, with one inversion. Through the
  run-up to Liberation Day the sectors with the <em>highest</em> revenue beat
  rates had the <em>lowest</em> returns (ρ = {q1_rho}, p = {q1_p}). Only in the
  post-tariff recovery do delivery and returns line up positively across sectors
  (ρ = {q4_rho} on revenue growth acceleration, p = {q4_p}) — the market went back
  to paying for growth once the macro shock cleared.</p>
</section>

<section>
  <div class="slug">Conclusions</div>
  <h2>What this says</h2>
  <ol class="findings">
    <li><b>Fundamentals did not cause the rotation.</b><span>Semiconductors posted
    the best earnings record of any sector through 2024H2 and Q1 2025 and the worst
    return. The fall was multiple compression against growing earnings.</span></li>
    <li><b>Earnings explain the day, not the period.</b><span>Surprise is priced
    hard and immediately, then contributes essentially nothing over the following
    quarter.</span></li>
    <li><b>Ex-ante fundamentals are close to useless cross-sectionally.</b><span>Under
    1% of added explained variance over sector and price-based controls. For
    forecasting six-month relative performance, the last earnings report is not
    where the information is.</span></li>
    <li><b>Sector and momentum dominate.</b><span>Sector plus size, momentum and
    volatility explain {sc_lo}–{sc_hi}% of the cross-section. That is where the
    rotation lived.</span></li>
    <li><b>Energy was not a 2024H2 destination.</b><span>It underperformed the
    universe by {eng_excess}pp over the rotation window on falling earnings. Its
    outperformance is a 2025–26 recovery story; financials and utilities were the
    2024H2 destinations.</span></li>
  </ol>
</section>

<footer>
  <h3>Method</h3>
  <ul>
    <li>Universe is <code>tickers.txt</code> filtered to names already trading in
    January 2024 — {n_stocks} of 2,530. Names that stopped trading are kept for the
    windows they cover, so the sample is not survivorship-filtered, but it
    under-represents 2024–26 listings.</li>
    <li>Returns are total returns from split- and dividend-adjusted closes in
    <code>price_history</code>. Technical measures are recomputed from daily bars;
    the stored <code>tech_indicators</code> table only reaches back to February 2026.</li>
    <li>Fundamentals are Finviz quarterly reports from <code>earnings_history</code>,
    on the non-GAAP basis the estimates are stated on. Year-on-year growth is
    recomputed as a lag-4 change — the stored <code>q_rev_yoy</code> and
    <code>q_eps_yoy</code> columns are only populated for rows fetched from 2026.</li>
    <li>Every regression input is winsorised at the 1st and 99th percentiles and
    standardised cross-sectionally, so a coefficient reads as return per one
    standard deviation of signal. Standard errors are HC1.</li>
    <li>Sector tags come from the latest fundamentals snapshot, so a company that
    changed classification is tagged by what it is now. Semiconductors are lifted
    out of Technology into their own group.</li>
    <li>Windows are calendar cuts chosen to match the question, not estimated
    breakpoints. They are hindsight cuts.</li>
    <li>Read-only study: no table was altered and no schema changed.</li>
  </ul>
</footer>
</div>

<div id="tip" role="status"></div>
<script>{script}</script>
"""


def main() -> None:
    ap = argparse.ArgumentParser(description="Render the study report as HTML")
    ap.add_argument("--out", default=str(config.OUT_DIR))
    ap.add_argument("--file", default="report.html")
    args = ap.parse_args()
    out_dir = Path(args.out)
    html = build(out_dir)
    path = out_dir / args.file
    path.write_text(html)
    print(f"[publish] wrote {path} ({len(html):,} bytes)")


if __name__ == "__main__":
    main()
