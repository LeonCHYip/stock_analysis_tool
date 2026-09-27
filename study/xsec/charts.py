"""Inline-SVG chart builders for the published report.

Hand-built SVG rather than a charting library: the page needs four chart forms,
all static apart from hover, and a pinned CDN library would be more bytes and
more failure modes than the ~200 lines here. Every chart draws to one scale,
labels values the scale actually reaches, and takes its text colour from the
page's theme tokens so it reads in light and dark alike.

Marks carry `data-tip` for the hover layer in the page's own script.
"""

from __future__ import annotations

import math
from datetime import date

# Series colours are theme tokens defined in the page, so a chart never hard
# codes a hex value that would only work in one theme.
SERIES_VARS = ["var(--s1)", "var(--s2)", "var(--s3)", "var(--s4)"]


def _esc(s: str) -> str:
    return (str(s).replace("&", "&amp;").replace("<", "&lt;")
            .replace(">", "&gt;").replace('"', "&quot;"))


def nice_ticks(lo: float, hi: float, target: int = 5) -> list[float]:
    """Round tick values spanning [lo, hi], on a 1/2/2.5/5 x 10^n step."""
    if hi <= lo:
        hi = lo + 1
    raw = (hi - lo) / max(target, 2)
    mag = 10 ** math.floor(math.log10(raw)) if raw > 0 else 1
    step = min((s * mag for s in (1, 2, 2.5, 5, 10) if s * mag >= raw),
               default=raw)
    start = math.floor(lo / step) * step
    out, v = [], start
    while v <= hi + step * 0.5:
        out.append(round(v, 10))
        v += step
    return out


def _axis_y(ticks, y_of, x0, x1, fmt) -> str:
    parts = []
    for t in ticks:
        y = y_of(t)
        parts.append(
            f'<line x1="{x0}" x2="{x1}" y1="{y:.1f}" y2="{y:.1f}" '
            f'class="grid"/>'
            f'<text x="{x0 - 8}" y="{y + 3.5:.1f}" class="tick tick-y">'
            f'{_esc(fmt(t))}</text>'
        )
    return "".join(parts)


# ── 1. Rebased index lines, with regime bands ─────────────────────────────────

def line_chart(series: dict[str, list[tuple[str, float]]], bands: list[dict],
               width: int = 860, height: int = 400,
               y_fmt=lambda v: f"{v:.0f}") -> str:
    """Multi-series time line with shaded regime bands and end-of-line labels.

    Every line is directly labelled at its right end, which is also the relief
    the palette check asks for: identity never rests on hue alone.
    """
    # The right gutter has to hold the longest end-label plus its value; the
    # caller keeps those labels short for exactly this reason.
    pad_l, pad_r, pad_t, pad_b = 52, 118, 26, 36
    iw, ih = width - pad_l - pad_r, height - pad_t - pad_b

    all_pts = [p for pts in series.values() for p in pts]
    xs = sorted({date.fromisoformat(d) for d, _ in all_pts})
    x_lo, x_hi = xs[0].toordinal(), xs[-1].toordinal()
    ys = [v for _, v in all_pts]
    y_lo, y_hi = min(ys), max(ys)
    ticks = nice_ticks(y_lo, y_hi)
    y_lo, y_hi = min(ticks[0], y_lo), max(ticks[-1], y_hi)

    def X(d: str) -> float:
        return pad_l + (date.fromisoformat(d).toordinal() - x_lo) / (x_hi - x_lo) * iw

    def Y(v: float) -> float:
        return pad_t + ih - (v - y_lo) / (y_hi - y_lo) * ih

    out = [f'<svg viewBox="0 0 {width} {height}" class="chart" '
           f'role="img" aria-label="Rebased group index, base 100">']

    for b in bands:
        x1, x2 = X(b["start"]), X(b["end"])
        out.append(
            f'<rect x="{x1:.1f}" y="{pad_t}" width="{max(x2 - x1, 1):.1f}" '
            f'height="{ih}" class="band {b.get("cls", "")}"/>'
        )
        out.append(
            f'<text x="{(x1 + x2) / 2:.1f}" y="{pad_t - 10}" '
            f'class="band-label">{_esc(b["label"])}</text>'
        )

    out.append(_axis_y(ticks, Y, pad_l, pad_l + iw, y_fmt))
    out.append(f'<line x1="{pad_l}" x2="{pad_l + iw}" y1="{Y(100):.1f}" '
               f'y2="{Y(100):.1f}" class="baseline"/>')

    for i, (name, pts) in enumerate(series.items()):
        colour = "var(--ink-3)" if name.startswith("Universe") else SERIES_VARS[i % 4]
        dash = ' stroke-dasharray="4 3"' if name.startswith("Universe") else ""
        d = " ".join(
            ("M" if j == 0 else "L") + f"{X(dt):.1f} {Y(v):.1f}"
            for j, (dt, v) in enumerate(pts)
        )
        out.append(f'<path d="{d}" fill="none" stroke="{colour}" '
                   f'stroke-width="2" stroke-linejoin="round"{dash}/>')
        lx, ly = X(pts[-1][0]), Y(pts[-1][1])
        out.append(
            f'<text x="{lx + 8:.1f}" y="{ly + 4:.1f}" class="end-label" '
            f'fill="{colour}">{_esc(name)} '
            f'<tspan class="end-value">{pts[-1][1]:.0f}</tspan></text>'
        )

    dates = [p[0] for p in series[next(iter(series))]]
    step = max(1, len(dates) // 6)
    for i in range(0, len(dates), step):
        d = dates[i]
        # Anchor the last label at its right edge so it cannot run past the plot.
        anchor = "end" if i + step >= len(dates) else "start"
        out.append(f'<text x="{X(d):.1f}" y="{pad_t + ih + 20}" '
                   f'class="tick" text-anchor="{anchor}">{d[:7]}</text>')

    out.append("</svg>")
    return "".join(out)


# ── 2. Horizontal grouped bars (P/E decomposition) ────────────────────────────

def grouped_hbar(rows: list[dict], series: list[tuple[str, str]],
                 width: int = 860, row_h: int = 46,
                 unit: str = "%") -> str:
    """Grouped horizontal bars around a zero line, one group per row.

    Horizontal because the categories are sector names, which do not fit under
    a vertical axis without turning the labels.
    """
    pad_l, pad_r, pad_t, pad_b = 148, 24, 30, 30
    ih = row_h * len(rows)
    height = ih + pad_t + pad_b
    iw = width - pad_l - pad_r

    vals = [r[k] for r in rows for k, _ in series if r.get(k) is not None]
    ticks = nice_ticks(min(min(vals), 0), max(max(vals), 0), 6)
    lo, hi = ticks[0], ticks[-1]

    def X(v: float) -> float:
        return pad_l + (v - lo) / (hi - lo) * iw

    out = [f'<svg viewBox="0 0 {width} {height}" class="chart" role="img" '
           f'aria-label="Grouped bar chart">']
    for t in ticks:
        x = X(t)
        out.append(f'<line x1="{x:.1f}" x2="{x:.1f}" y1="{pad_t}" '
                   f'y2="{pad_t + ih}" class="grid"/>')
        out.append(f'<text x="{x:.1f}" y="{pad_t + ih + 18}" '
                   f'class="tick tick-x mid">{t:.0f}{unit}</text>')
    x0 = X(0)
    out.append(f'<line x1="{x0:.1f}" x2="{x0:.1f}" y1="{pad_t}" '
               f'y2="{pad_t + ih}" class="baseline"/>')

    bh = (row_h - 14) / len(series)
    for i, r in enumerate(rows):
        top = pad_t + i * row_h
        out.append(f'<text x="{pad_l - 12}" y="{top + row_h / 2 + 4:.1f}" '
                   f'class="cat-label">{_esc(r["label"])}</text>')
        for j, (key, name) in enumerate(series):
            v = r.get(key)
            if v is None:
                continue
            y = top + 7 + j * bh
            x1, x2 = min(X(0), X(v)), max(X(0), X(v))
            # 2px gap between the paired bars keeps the two fills separate.
            out.append(
                f'<rect x="{x1:.1f}" y="{y:.1f}" width="{max(x2 - x1, 1.5):.1f}" '
                f'height="{bh - 2:.1f}" rx="2" fill="{SERIES_VARS[j]}" '
                f'class="mark" data-tip="{_esc(r["label"])} · {_esc(name)}: '
                f'{v:+.1f}{unit}"/>'
            )
    out.append("</svg>")
    return "".join(out)


# ── 3. Vertical bars (drift decay, incremental R²) ────────────────────────────

def vbar_chart(rows: list[dict], series: list[tuple[str, str]],
               width: int = 860, height: int = 300, unit: str = "",
               y_fmt=None, label_bars: bool = True) -> str:
    """Grouped vertical bars around a zero line."""
    pad_l, pad_r, pad_t, pad_b = 56, 20, 26, 46
    iw, ih = width - pad_l - pad_r, height - pad_t - pad_b
    y_fmt = y_fmt or (lambda v: f"{v:g}{unit}")

    vals = [r[k] for r in rows for k, _ in series if r.get(k) is not None]
    ticks = nice_ticks(min(min(vals), 0), max(max(vals), 0), 5)
    lo, hi = ticks[0], ticks[-1]

    def Y(v: float) -> float:
        return pad_t + ih - (v - lo) / (hi - lo) * ih

    gw = iw / len(rows)
    bw = min((gw - 18) / len(series), 54)

    out = [f'<svg viewBox="0 0 {width} {height}" class="chart" role="img" '
           f'aria-label="Bar chart">']
    out.append(_axis_y(ticks, Y, pad_l, pad_l + iw, y_fmt))
    out.append(f'<line x1="{pad_l}" x2="{pad_l + iw}" y1="{Y(0):.1f}" '
               f'y2="{Y(0):.1f}" class="baseline"/>')

    for i, r in enumerate(rows):
        cx = pad_l + gw * (i + 0.5)
        span = bw * len(series) + 2 * (len(series) - 1)
        for j, (key, name) in enumerate(series):
            v = r.get(key)
            if v is None:
                continue
            x = cx - span / 2 + j * (bw + 2)
            y1, y2 = min(Y(0), Y(v)), max(Y(0), Y(v))
            out.append(
                f'<rect x="{x:.1f}" y="{y1:.1f}" width="{bw:.1f}" '
                f'height="{max(y2 - y1, 1.5):.1f}" rx="3" '
                f'fill="{SERIES_VARS[j]}" class="mark" '
                f'data-tip="{_esc(r["label"])} · {_esc(name)}: '
                f'{v:+.2f}{unit}"/>'
            )
            if label_bars:
                above = v >= 0
                out.append(
                    f'<text x="{x + bw / 2:.1f}" '
                    f'y="{(y1 - 6) if above else (y2 + 14):.1f}" '
                    f'class="bar-value mid">{v:+.2f}</text>'
                )
        out.append(f'<text x="{cx:.1f}" y="{pad_t + ih + 20}" '
                   f'class="tick tick-x mid">{_esc(r["label"])}</text>')
        if r.get("sub"):
            out.append(f'<text x="{cx:.1f}" y="{pad_t + ih + 34}" '
                       f'class="tick tick-sub mid">{_esc(r["sub"])}</text>')
    out.append("</svg>")
    return "".join(out)


def legend(series: list[tuple[str, str]]) -> str:
    """Legend swatches. Always present for two or more series."""
    items = "".join(
        f'<span class="lg-item"><i style="background:{SERIES_VARS[j]}"></i>'
        f'{_esc(name)}</span>'
        for j, (_, name) in enumerate(series)
    )
    return f'<div class="legend">{items}</div>'
