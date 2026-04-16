"""
screener.py — Screener page for emerging-edge.

V1 feature: a Floebertus-style bubble chart — P/E on X, ROE on Y,
bubble size = growth %. User picks a country (display exchange label)
from a dropdown, and the chart renders every stock in that country
that has fundamentals entered in the DB.

Fundamentals (P/E, ROE %, Growth %) are stored in the
`stock_fundamentals` table and can be edited per stock via an
inline table below the chart.

Styled to match the rest of the app (toast/confirm helpers already
in dashboard.py are assumed, but the page is self-contained).
"""

from __future__ import annotations

import html as html_mod
import os
from typing import Optional

from db import Database
from fetchers import get_active_stocks


def _esc(s) -> str:
    return html_mod.escape(str(s)) if s is not None else ""


EXCHANGE_DISPLAY = {
    "NASDAQ": "US", "NYSE": "US", "AMEX": "US", "OTC": "US", "PNK": "US",
    "KLSE": "Malaysia", "NGX": "Nigeria", "BRVM": "Ivory Coast/BRVM",
    "UZSE": "Uzbekistan", "SGX": "Singapore", "KSE": "Kyrgyzstan",
    "KASE": "Kazakhstan", "NSEK": "Kenya", "GSE": "Ghana",
    "BWSE": "Botswana", "LUSE": "Zambia", "DSET": "Tanzania",
    "DSEB": "Bangladesh", "PSX": "Pakistan", "CSEM": "Morocco",
    "ZSE": "Croatia", "BELEX": "Serbia", "BSSE": "Slovakia",
    "PNGX": "Papua New Guinea", "BVMT": "Tunisia", "CSEL": "Sri Lanka",
    "UX": "Ukraine", "USE": "Uganda", "RSE": "Rwanda", "SEM": "Mauritius",
    "ISX": "Iraq", "ESX": "Ethiopia", "JSE": "South Africa",
    "LSE": "UK", "IOB": "UK", "HKSE": "Hong Kong", "ASX": "Australia",
    "FRA": "Germany", "TSX": "Canada", "BMV": "Mexico",
    "EURONEXT": "Europe", "EUR_FR": "France", "EUR_NL": "Netherlands",
    "EUR_BE": "Belgium", "EUR_PT": "Portugal", "EUR_IE": "Ireland",
    "BIT": "Italy", "OMX": "Sweden", "HSE": "Finland", "ICEX": "Iceland",
    "OSE": "Norway", "CSE": "Denmark", "SWX": "Switzerland",
    "B3": "Brazil", "BCBA": "Argentina", "NSE": "India", "BSE": "India",
    "KRX": "South Korea", "TWSE": "Taiwan", "IDX": "Indonesia",
    "SET": "Thailand", "PSE": "Philippines", "HOSE": "Vietnam",
    "TASE": "Israel", "TADAWUL": "Saudi Arabia", "DFM": "UAE (Dubai)",
    "ADX": "UAE (Abu Dhabi)", "QSE": "Qatar", "BIST": "Turkey",
    "WSE": "Poland", "PSE_CZ": "Czech Republic", "BET": "Hungary",
    "ATHEX": "Greece", "BVB": "Romania", "NZX": "New Zealand",
    "SSE": "China (Shanghai)", "SZSE": "China (Shenzhen)",
    "JPX": "Japan", "BME": "Spain", "WBAG": "Austria", "BVS": "Chile",
}


def _display_ex(code: str) -> str:
    return EXCHANGE_DISPLAY.get((code or "").upper(), code or "")


# ---------------------------------------------------------------------------
# Floebertus-style SVG bubble chart
# ---------------------------------------------------------------------------

# Style constants — cream background, muted blue-grey border,
# gold bubbles, to match the matplotlib-generated Floebertus look.
FB_CREAM = "#F4E8C8"
FB_BORDER = "#5F7A8C"
FB_BORDER_DARK = "#4a5f6f"
FB_TEXT = "#1F1F1F"
# Radial gradient for the gold "sphere" bubbles (approximates the
# Malaysia_Bubble_Chart.png look).
FB_GOLD_OUTER = "#b88f2a"
FB_GOLD_MID   = "#e9c668"
FB_GOLD_INNER = "#fff0b8"


def _render_floebertus_svg(country_label: str, stocks: list[dict]) -> str:
    """
    Render the floebertus-style bubble chart as inline SVG.

    Each stock needs pe (float), roe_pct (float), growth_pct (float), name.
    Stocks missing any of these are skipped.
    """
    # Viewport — keep ratio similar to the matplotlib version (13 × 7.8).
    W, H = 1300, 780
    # Plot area padding
    PAD_L, PAD_R, PAD_T, PAD_B = 90, 40, 110, 70

    # Filter to stocks with all three fundamentals
    pts = []
    for s in stocks:
        if s.get("pe") is None or s.get("roe_pct") is None or s.get("growth_pct") is None:
            continue
        try:
            pe = float(s["pe"]); roe = float(s["roe_pct"]); gr = float(s["growth_pct"])
        except (TypeError, ValueError):
            continue
        pts.append({
            "name":   s.get("name") or s.get("ticker", ""),
            "ticker": s.get("ticker", ""),
            "pe":     pe, "roe": roe, "growth": gr,
        })

    if not pts:
        return (
            '<div class="floeb-empty">No fundamentals entered for <b>'
            + _esc(country_label)
            + '</b> yet. Add P/E, ROE, and Growth below to see the chart.</div>'
        )

    # Auto axis bounds with a bit of padding
    x_vals = [p["pe"] for p in pts]
    y_vals = [p["roe"] for p in pts]
    x_min = max(0.0, min(x_vals) - 2)
    x_max = max(x_vals) + 3
    y_min = min(min(y_vals) - 5, 0)
    y_max = max(y_vals) + 5

    # Round bounds to nice ticks
    def _round_down(x, step):  return int(x // step) * step
    def _round_up(x, step):    return int((x + step - 1) // step) * step
    x_min = _round_down(x_min, 2); x_max = _round_up(x_max, 2)
    y_min = _round_down(y_min, 10); y_max = _round_up(y_max, 10)
    if x_max == x_min: x_max = x_min + 2
    if y_max == y_min: y_max = y_min + 10

    def sx(pe):  return PAD_L + (pe - x_min) / (x_max - x_min) * (W - PAD_L - PAD_R)
    def sy(roe): return H - PAD_B - (roe - y_min) / (y_max - y_min) * (H - PAD_T - PAD_B)

    # Bubble radius scales with growth %. Reference: growth=20 → r≈28px.
    def br(growth): return max(8.0, min(70.0, growth * 1.6))

    out = []
    out.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" '
        f'class="floeb-svg" preserveAspectRatio="xMidYMid meet">'
    )
    # Defs — radial gradient for gold spheres + filter for plot-box shadow
    out.append('<defs>')
    out.append(
        f'<radialGradient id="fbGold" cx="35%" cy="32%" r="65%">'
        f'  <stop offset="0%"  stop-color="{FB_GOLD_INNER}"/>'
        f'  <stop offset="55%" stop-color="{FB_GOLD_MID}"/>'
        f'  <stop offset="100%" stop-color="{FB_GOLD_OUTER}"/>'
        f'</radialGradient>'
    )
    out.append(
        '<filter id="fbShadow" x="-5%" y="-5%" width="115%" height="115%">'
        '  <feGaussianBlur in="SourceAlpha" stdDeviation="3"/>'
        '  <feOffset dx="4" dy="4" result="offsetblur"/>'
        '  <feComponentTransfer><feFuncA type="linear" slope="0.35"/></feComponentTransfer>'
        '  <feMerge><feMergeNode/><feMergeNode in="SourceGraphic"/></feMerge>'
        '</filter>'
    )
    out.append('</defs>')
    # Outer rounded cream frame
    out.append(
        f'<rect x="10" y="10" width="{W-20}" height="{H-20}" '
        f'rx="18" ry="18" fill="{FB_CREAM}" stroke="{FB_BORDER}" stroke-width="1.5"/>'
    )
    # Plot box
    out.append(
        f'<rect x="{PAD_L}" y="{PAD_T}" '
        f'width="{W - PAD_L - PAD_R}" height="{H - PAD_T - PAD_B}" '
        f'fill="{FB_CREAM}" stroke="{FB_BORDER}" stroke-width="2.2" '
        f'filter="url(#fbShadow)"/>'
    )

    # Axis ticks
    x_step = 2 if (x_max - x_min) <= 20 else 4
    y_step = 10
    for x in range(int(x_min), int(x_max) + 1, x_step):
        px = sx(x)
        out.append(
            f'<line x1="{px}" y1="{H - PAD_B}" x2="{px}" y2="{H - PAD_B + 6}" '
            f'stroke="{FB_BORDER_DARK}" stroke-width="1.2"/>'
        )
        out.append(
            f'<text x="{px}" y="{H - PAD_B + 22}" text-anchor="middle" '
            f'font-family="Inter, system-ui, sans-serif" font-size="14" '
            f'fill="{FB_TEXT}">{x}</text>'
        )
    for y in range(int(y_min), int(y_max) + 1, y_step):
        py = sy(y)
        out.append(
            f'<line x1="{PAD_L}" y1="{py}" x2="{PAD_L - 6}" y2="{py}" '
            f'stroke="{FB_BORDER_DARK}" stroke-width="1.2"/>'
        )
        out.append(
            f'<text x="{PAD_L - 12}" y="{py + 5}" text-anchor="end" '
            f'font-family="Inter, system-ui, sans-serif" font-size="14" '
            f'fill="{FB_TEXT}">{y}%</text>'
        )

    # Title + legend
    out.append(
        f'<text x="30" y="50" font-family="Inter, system-ui, sans-serif" '
        f'font-size="22" font-weight="700" fill="{FB_TEXT}">'
        f'ROE vs P/E vs Growth — {_esc(country_label)} stocks</text>'
    )
    out.append(
        f'<text x="30" y="76" font-family="Inter, system-ui, sans-serif" '
        f'font-size="13" fill="{FB_TEXT}" opacity="0.7">'
        f'Floebertus-style screener · {len(pts)} stock'
        f'{"s" if len(pts) != 1 else ""}</text>'
    )
    # Legend bubble (growth=20 reference)
    leg_r = br(20)
    leg_cx = W - PAD_R - leg_r - 140
    leg_cy = 55
    out.append(
        f'<circle cx="{leg_cx + leg_r + 5}" cy="{leg_cy}" r="{leg_r}" '
        f'fill="url(#fbGold)" opacity="0.95"/>'
    )
    out.append(
        f'<text x="{leg_cx - 5}" y="{leg_cy + 5}" text-anchor="end" '
        f'font-family="Inter, system-ui, sans-serif" font-size="13" '
        f'font-weight="600" fill="{FB_TEXT}">bubble = growth (20% ref)</text>'
    )

    # Axis labels
    out.append(
        f'<text x="{(W - PAD_L - PAD_R)/2 + PAD_L}" y="{H - 20}" '
        f'text-anchor="middle" font-family="Inter, system-ui, sans-serif" '
        f'font-size="15" font-weight="700" fill="{FB_TEXT}">P/E ratio</text>'
    )
    out.append(
        f'<text x="{PAD_L - 55}" y="{(H - PAD_T - PAD_B)/2 + PAD_T}" '
        f'text-anchor="middle" font-family="Inter, system-ui, sans-serif" '
        f'font-size="15" font-weight="700" fill="{FB_TEXT}" '
        f'transform="rotate(-90 {PAD_L - 55} {(H - PAD_T - PAD_B)/2 + PAD_T})">'
        f'Return on Equity</text>'
    )

    # Bubbles (largest first so labels on smaller ones stay readable)
    for p in sorted(pts, key=lambda x: -x["growth"]):
        cx = sx(p["pe"]); cy = sy(p["roe"]); r = br(p["growth"])
        out.append(
            f'<circle cx="{cx:.1f}" cy="{cy:.1f}" r="{r:.1f}" '
            f'fill="url(#fbGold)" opacity="0.92" '
            f'stroke="{FB_GOLD_OUTER}" stroke-width="0.5">'
            f'<title>{_esc(p["name"])} ({_esc(p["ticker"])})'
            f'&#10;P/E {p["pe"]:.1f} · ROE {p["roe"]:.1f}% · Growth {p["growth"]:.1f}%'
            f'</title>'
            f'</circle>'
        )
        # Name label above the bubble, shifted down into the bubble
        # if there's no room above.
        label = p["name"][:22]
        ly = cy - r - 6 if cy - r - 6 > PAD_T + 10 else cy + r + 14
        out.append(
            f'<text x="{cx:.1f}" y="{ly:.1f}" text-anchor="middle" '
            f'font-family="Inter, system-ui, sans-serif" font-size="12" '
            f'font-weight="700" fill="{FB_TEXT}">{_esc(label)}</text>'
        )

    out.append('</svg>')
    return "".join(out)


# ---------------------------------------------------------------------------
# Page builder
# ---------------------------------------------------------------------------

def generate_html(db: Database, config: dict, country: str = None) -> str:
    """Generate the Screener page HTML."""
    active_stocks = get_active_stocks(db, config)

    # Unique display-exchange labels present in the watchlist, sorted.
    countries_in_use = sorted({
        _display_ex(s.get("exchange", "")) for s in active_stocks
    })
    if not country and countries_in_use:
        country = countries_in_use[0]

    # Stocks in the chosen country, with fundamentals joined in.
    country_stocks = [
        s for s in active_stocks
        if _display_ex(s.get("exchange", "")) == country
    ]
    # Pull fundamentals in one go (indexed by (ticker, exchange))
    fund_rows = db.get_fundamentals()
    fund_ix = {(r["ticker"], r["exchange"]): r for r in fund_rows}

    # Merge fundamentals into stock rows
    for s in country_stocks:
        key = (s.get("ticker", "").upper(), s.get("exchange", "").upper())
        f = fund_ix.get(key, {})
        s["pe"] = f.get("pe")
        s["roe_pct"] = f.get("roe_pct")
        s["growth_pct"] = f.get("growth_pct")
        s["fund_notes"] = f.get("notes", "")

    svg = _render_floebertus_svg(country or "", country_stocks)

    # Country dropdown options
    option_html = "".join(
        f'<option value="{_esc(c)}"{" selected" if c == country else ""}>'
        f'{_esc(c)}</option>'
        for c in countries_in_use
    )

    # Fundamentals-entry table
    rows_html = []
    for s in sorted(country_stocks, key=lambda x: x.get("ticker", "")):
        tk = s.get("ticker", "")
        ex = s.get("exchange", "")
        name = s.get("name", tk)
        pe = "" if s.get("pe") is None else f"{s['pe']:.2f}"
        roe = "" if s.get("roe_pct") is None else f"{s['roe_pct']:.1f}"
        gr = "" if s.get("growth_pct") is None else f"{s['growth_pct']:.1f}"
        notes = s.get("fund_notes") or ""
        rows_html.append(
            f'<tr data-ticker="{_esc(tk)}" data-exchange="{_esc(ex)}">'
            f'  <td><strong>{_esc(name)}</strong>'
            f'      <span class="muted"> · {_esc(tk)}</span></td>'
            f'  <td><input type="number" step="0.01" class="fund-input" '
            f'             data-field="pe" value="{_esc(pe)}" placeholder="–"></td>'
            f'  <td><input type="number" step="0.1" class="fund-input" '
            f'             data-field="roe_pct" value="{_esc(roe)}" placeholder="–"></td>'
            f'  <td><input type="number" step="0.1" class="fund-input" '
            f'             data-field="growth_pct" value="{_esc(gr)}" placeholder="–"></td>'
            f'  <td><input type="text" class="fund-input fund-notes" '
            f'             data-field="notes" value="{_esc(notes)}" placeholder="notes"></td>'
            f'  <td><button class="fund-save" onclick="saveFundamentalsRow(this)">Save</button></td>'
            f'</tr>'
        )
    table_html = "".join(rows_html) if rows_html else (
        '<tr><td colspan="6" class="muted" style="text-align:center;padding:1.5rem">'
        'No stocks in this country yet. Add some via the Add Stock modal on Monitor.'
        '</td></tr>'
    )

    # Load favicon as data URL via the same pattern as other pages — but
    # keep lightweight: link to existing /favicon.png / inline SVG.
    return _PAGE_TEMPLATE.format(
        option_html=option_html,
        country=_esc(country or ""),
        svg=svg,
        table_html=table_html,
        stock_count=len(country_stocks),
    )


_PAGE_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta name="theme-color" content="#0f1117">
<link rel="icon" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'%3E%3Ctext y='.9em' font-size='90'%3E🌍%3C/text%3E%3C/svg%3E">
<title>Emerging Edge — Screener</title>
<style>
:root {{
    --bg: #0f1117; --surface: #1a1d26; --surface2: #252935;
    --border: #2e3342; --text: #e6e8ec; --text-muted: #9aa0a8;
    --accent: #ffb800; --accent-dim: rgba(255,184,0,0.14);
    --green: #4ade80; --green-dim: rgba(74,222,128,0.14);
    --red: #f87171; --red-dim: rgba(248,113,113,0.14);
}}
* {{ box-sizing: border-box; }}
body {{
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Inter, sans-serif;
    margin: 0; background: var(--bg); color: var(--text); font-size: 14px;
}}
a {{ color: var(--accent); text-decoration: none; }}
a:hover {{ text-decoration: underline; }}
.header {{
    background: var(--surface); border-bottom: 1px solid var(--border);
    padding: 0.65rem 2rem 0.55rem;
    position: sticky; top: 0; z-index: 100;
}}
.header-inner {{
    max-width: 1400px; margin: 0 auto;
    display: flex; align-items: center; gap: 1.2rem; flex-wrap: wrap;
}}
.header-brand {{ display: flex; align-items: center; gap: 0.6rem; }}
.header-brand h1 {{
    font-size: 1.15rem; font-weight: 700; margin: 0; color: var(--text);
}}
.header-brand h1 span {{ color: var(--accent); }}
.header-nav {{ display: flex; gap: 0.6rem; margin-left: auto; flex-wrap: wrap; }}
.header-nav a {{
    font-size: 0.82rem; font-weight: 600;
    padding: 0.45rem 0.9rem; border-radius: 8px;
    background: var(--surface2); color: var(--text);
    border: 1px solid var(--border);
}}
.header-nav a.active {{ background: var(--accent); color: var(--bg); border-color: var(--accent); }}
.header-nav a:hover {{ text-decoration: none; border-color: var(--accent); }}

.container {{
    max-width: 1400px; margin: 1rem auto 3rem; padding: 0 2rem;
}}
.toolbar {{
    display: flex; align-items: center; gap: 1rem; flex-wrap: wrap;
    margin-bottom: 1rem;
}}
.toolbar label {{ font-size: 0.8rem; color: var(--text-muted); }}
.toolbar select {{
    background: var(--surface); color: var(--text);
    border: 1px solid var(--border); border-radius: 8px;
    padding: 0.4rem 0.75rem; font-size: 0.85rem;
    min-width: 180px;
}}
.chart-card {{
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 12px; padding: 1rem; margin-bottom: 1.2rem;
    overflow: hidden;
}}
.floeb-svg {{
    display: block; width: 100%; height: auto; border-radius: 8px;
}}
.floeb-empty {{
    padding: 3rem 1rem; text-align: center;
    color: var(--text-muted); font-size: 0.9rem;
    background: #F4E8C8; color: #1F1F1F;
    border-radius: 8px;
}}

.fund-card {{
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 12px; padding: 1.2rem;
}}
.fund-card h2 {{
    font-size: 0.95rem; font-weight: 700; margin: 0 0 0.8rem;
    color: var(--text);
}}
.fund-table {{
    width: 100%; border-collapse: collapse; font-size: 0.82rem;
}}
.fund-table th, .fund-table td {{
    padding: 0.55rem 0.6rem; text-align: left;
    border-bottom: 1px solid var(--border);
}}
.fund-table th {{
    font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.04em;
    color: var(--text-muted); font-weight: 600;
}}
.muted {{ color: var(--text-muted); }}
.fund-input {{
    background: var(--surface2); color: var(--text);
    border: 1px solid var(--border); border-radius: 6px;
    padding: 0.3rem 0.5rem; font-size: 0.82rem;
    width: 100%; max-width: 110px;
    font-variant-numeric: tabular-nums;
}}
.fund-input.fund-notes {{ max-width: none; width: 100%; }}
.fund-input:focus {{ border-color: var(--accent); outline: none; }}
.fund-save {{
    background: var(--accent-dim); color: var(--accent);
    border: 1px solid var(--accent); border-radius: 6px;
    padding: 0.3rem 0.85rem; font-size: 0.78rem; font-weight: 700;
    cursor: pointer; transition: all 0.15s;
}}
.fund-save:hover {{ background: var(--accent); color: var(--bg); }}

/* Toast (minimal — matches dashboard) */
#toast-container {{
    position: fixed; top: 1rem; right: 1rem; z-index: 9999;
    display: flex; flex-direction: column; gap: 0.5rem;
    pointer-events: none; max-width: min(380px, calc(100vw - 2rem));
}}
.toast {{
    pointer-events: auto; background: var(--surface);
    border: 1px solid var(--border); border-left: 3px solid var(--text-muted);
    border-radius: 8px; padding: 0.7rem 1rem; font-size: 0.82rem;
    color: var(--text); box-shadow: 0 8px 24px rgba(0,0,0,0.35);
    animation: toast-in 0.22s ease-out;
}}
.toast.toast-success {{ border-left-color: var(--green); }}
.toast.toast-error   {{ border-left-color: var(--red); }}
.toast.toast-info    {{ border-left-color: var(--accent); }}
.toast.toast-out {{ animation: toast-out 0.18s ease-in forwards; }}
@keyframes toast-in {{
    from {{ transform: translateX(120%); opacity: 0; }}
    to   {{ transform: translateX(0); opacity: 1; }}
}}
@keyframes toast-out {{
    from {{ transform: translateX(0); opacity: 1; }}
    to   {{ transform: translateX(120%); opacity: 0; }}
}}
</style>
</head>
<body>

<div class="header">
<div class="header-inner">
    <div class="header-brand">
        <h1><span>Emerging Edge</span> Screener</h1>
    </div>
    <nav class="header-nav">
        <a href="/monitor">📊 Monitor</a>
        <a href="/portfolio">💼 Portfolio</a>
        <a href="/screener" class="active">🔍 Screener</a>
        <a href="/engine-room">⚙️ Engine Room</a>
    </nav>
</div>
</div>

<div class="container">
    <div class="toolbar">
        <label>Country:
            <select id="country-select" onchange="onCountryChange(this.value)">
                {option_html}
            </select>
        </label>
        <span class="muted">{stock_count} stocks in <b>{country}</b></span>
    </div>

    <div class="chart-card">
        {svg}
    </div>

    <div class="fund-card">
        <h2>Fundamentals · {country}</h2>
        <table class="fund-table">
            <thead>
                <tr>
                    <th>Stock</th><th>P/E</th><th>ROE %</th>
                    <th>Growth %</th><th>Notes</th><th></th>
                </tr>
            </thead>
            <tbody>{table_html}</tbody>
        </table>
    </div>
</div>

<script>
function showToast(msg, type) {{
    type = type || 'info';
    let c = document.getElementById('toast-container');
    if (!c) {{ c = document.createElement('div'); c.id = 'toast-container'; document.body.appendChild(c); }}
    const t = document.createElement('div');
    t.className = 'toast toast-' + type;
    t.textContent = msg;
    const dismiss = () => {{ t.classList.add('toast-out'); setTimeout(() => t.remove(), 200); }};
    t.addEventListener('click', dismiss);
    c.appendChild(t);
    setTimeout(dismiss, 4000);
}}

function onCountryChange(c) {{
    window.location.href = '/screener?country=' + encodeURIComponent(c);
}}

function saveFundamentalsRow(btn) {{
    const row = btn.closest('tr');
    const payload = {{
        ticker: row.dataset.ticker,
        exchange: row.dataset.exchange,
    }};
    row.querySelectorAll('.fund-input').forEach(i => {{
        const f = i.dataset.field;
        if (f === 'notes') payload[f] = i.value.trim();
        else {{
            const v = i.value.trim();
            payload[f] = v === '' ? null : parseFloat(v);
        }}
    }});
    fetch('/api/fundamentals/save', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify(payload),
    }})
    .then(r => r.json())
    .then(resp => {{
        if (resp.status === 'ok') {{
            showToast('Saved ' + payload.ticker, 'success');
            // Reload chart to reflect new data
            setTimeout(() => window.location.reload(), 500);
        }} else {{
            showToast(resp.message || 'Failed to save', 'error');
        }}
    }})
    .catch(err => showToast('Network error: ' + err, 'error'));
}}
</script>
</body>
</html>
"""


def save_screener_html(db: Database, config: dict, country: str = None) -> str:
    """Generate and write the Screener page. Returns file path."""
    digest_dir = config.get("digest_dir", "./digests")
    os.makedirs(digest_dir, exist_ok=True)
    content = generate_html(db, config, country)
    fp = os.path.join(digest_dir, "screener.html")
    with open(fp, "w", encoding="utf-8") as f:
        f.write(content)
    return fp
