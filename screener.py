"""
screener.py — Screener page for emerging-edge.

Pick a country, see a Floebertus-style bubble chart (P/E on X,
ROE on Y, bubble size = growth %) of the *full universe* of
stocks in that country.

Universe = frontier_stocks.json catalog (~20k stocks across 44
countries) merged with the user's watchlist.

Fundamentals (P/E, ROE, Revenue growth) come from
stockanalysis.com and are cached in the `stock_fundamentals`
table. Because the universe is large, fetching is batched:
- On page load, every stock with cached (non-stale) fundamentals
  is plotted immediately.
- Click "↻ Fetch up to N more" to fetch the next batch of
  un-cached stocks. This blocks the request for up to ~45s
  (or until N stocks are processed), then re-renders.
"""

from __future__ import annotations

import datetime
import html as html_mod
import json
import logging
import os
import re
import ssl
import time
import urllib.error
import urllib.request

import catalog_updaters as _cu
from db import Database
from fetchers import _SA_SLUG, _sa_ticker, get_active_stocks

logger = logging.getLogger("emerging-edge.screener")


def _esc(s) -> str:
    return html_mod.escape(str(s)) if s is not None else ""


# ---------------------------------------------------------------------------
# stockanalysis.com-supported exchanges
# ---------------------------------------------------------------------------
_SA_SUPPORTED_EX = set(_SA_SLUG.keys()) | {"NASDAQ", "NYSE", "AMEX"}


# ---------------------------------------------------------------------------
# Universe: catalog + watchlist
# ---------------------------------------------------------------------------

def _load_universe(db: Database, config: dict) -> list[dict]:
    """
    Catalog (frontier_stocks.json) + watchlist (config + user_stocks),
    deduped on (ticker, exchange). Catalog entries already include a
    `country` field; watchlist entries don't, so we derive it.
    """
    try:
        catalog = _cu.load_catalog() or []
    except Exception:
        logger.warning("load_catalog failed", exc_info=True)
        catalog = []

    seen: set[tuple[str, str]] = set()
    merged: list[dict] = []
    for s in catalog:
        t = (s.get("ticker") or "").upper()
        e = (s.get("exchange") or "").upper()
        if not t or not e:
            continue
        key = (t, e)
        if key in seen:
            continue
        seen.add(key)
        merged.append({
            "ticker": t,
            "exchange": e,
            "name": s.get("name") or t,
            "country": s.get("country") or e,
        })

    # Merge in watchlist stocks (may add some the catalog misses)
    try:
        watchlist = get_active_stocks(db, config)
    except Exception:
        watchlist = []
    # Build a quick ex→country lookup from catalog for watchlist entries
    ex_to_country: dict[str, str] = {}
    for s in merged:
        ex_to_country.setdefault(s["exchange"], s["country"])
    for s in watchlist:
        t = (s.get("ticker") or "").upper()
        e = (s.get("exchange") or "").upper()
        if not t or not e:
            continue
        key = (t, e)
        if key in seen:
            continue
        seen.add(key)
        merged.append({
            "ticker": t,
            "exchange": e,
            "name": s.get("name") or t,
            "country": ex_to_country.get(e, e),
        })
    return merged


# ---------------------------------------------------------------------------
# stockanalysis.com fundamentals fetching
# ---------------------------------------------------------------------------

_SA_CACHE_HOURS = 24 * 7  # a week — fundamentals don't change fast


def _parse_number(s) -> float | None:
    if s is None:
        return None
    txt = str(s).strip()
    if not txt or txt.lower() in ("n/a", "-", "--"):
        return None
    txt = txt.replace(",", "").replace("%", "").strip()
    if txt and txt[-1].isalpha():
        return None
    try:
        return float(txt)
    except ValueError:
        return None


def _ssl_ctx() -> ssl.SSLContext:
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


def _http_get(url: str, timeout: int = 6) -> str | None:
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0 Safari/537.36",
            "Accept-Encoding": "identity",
            "Accept": "text/html,application/json",
        })
        with urllib.request.urlopen(req, timeout=timeout, context=_ssl_ctx()) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        logger.info("stockanalysis.com %s → HTTP %d", url, e.code)
        return None
    except Exception as e:
        logger.info("stockanalysis.com %s → %s", url, e)
        return None


def _fetch_us_fundamentals(ticker: str) -> tuple[float | None, float | None, float | None]:
    url = f"https://stockanalysis.com/api/symbol/s/{ticker.lower()}/statistics"
    raw = _http_get(url)
    if not raw:
        return None, None, None
    try:
        payload = json.loads(raw)
    except Exception:
        return None, None, None
    data = (payload or {}).get("data") or {}

    def _lookup(section: str, key: str):
        items = ((data.get(section) or {}).get("data")) or []
        for row in items:
            if row.get("id") == key:
                return row.get("value")
        return None

    pe = _parse_number(_lookup("ratios", "pe"))
    roe = _parse_number(_lookup("financialEfficiency", "roe"))
    growth = _parse_number(_lookup("analystForecasts", "revenue5y"))
    if growth is None:
        growth = _parse_number(_lookup("growth", "revenue_growth"))
    return pe, roe, growth


def _fetch_nonus_fundamentals(slug: str, ticker: str) -> tuple[float | None, float | None, float | None]:
    url = f"https://stockanalysis.com/quote/{slug}/{ticker}/statistics/"
    html = _http_get(url)
    if not html:
        return None, None, None

    def _grab(label_re: str) -> str | None:
        m = re.search(label_re + r'",\s*value:\s*"([^"]+)"', html)
        return m.group(1) if m else None

    pe = _parse_number(_grab(r'"PE Ratio'))
    roe = _parse_number(_grab(r'"Return on Equity \(ROE\)'))
    growth = _parse_number(
        _grab(r'"Revenue Growth Forecast \(5Y\)')
        or _grab(r'"Revenue Growth \(YoY\)')
        or _grab(r'"Revenue Growth')
    )
    return pe, roe, growth


def _fetch_fundamentals(ticker: str, exchange: str) -> tuple[float | None, float | None, float | None]:
    exU = (exchange or "").upper()
    if exU not in _SA_SUPPORTED_EX:
        return None, None, None
    slug = _SA_SLUG.get(exU)
    t = _sa_ticker(exU, ticker)
    if slug is None:
        return _fetch_us_fundamentals(t)
    return _fetch_nonus_fundamentals(slug, t)


def _is_stale(updated_at: str | None) -> bool:
    if not updated_at:
        return True
    try:
        dt = datetime.datetime.fromisoformat(updated_at.replace("Z", "+00:00"))
    except Exception:
        return True
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    age = datetime.datetime.now(datetime.timezone.utc) - dt
    return age.total_seconds() > _SA_CACHE_HOURS * 3600


# ---------------------------------------------------------------------------
# Batched fetch
# ---------------------------------------------------------------------------

DEFAULT_BATCH = 25
DEFAULT_BUDGET_S = 40


def _batch_fetch(db: Database, stocks: list[dict],
                 batch_size: int, budget_s: float) -> int:
    """Fetch fundamentals for up to `batch_size` uncached stocks, under
    a wall-clock time budget. Returns number fetched successfully."""
    # Cached keys (anything in stock_fundamentals table) — so we skip
    # both fresh AND stale+missing rows ONLY on cache-check path.
    cached_rows = db.get_fundamentals()
    cached_by_key = {(r["ticker"].upper(), r["exchange"].upper()): r for r in cached_rows}

    # Candidates: stocks with no fundamentals row yet, OR stale.
    todo: list[dict] = []
    for s in stocks:
        if s["exchange"] not in _SA_SUPPORTED_EX:
            continue
        key = (s["ticker"], s["exchange"])
        row = cached_by_key.get(key)
        if row is None or _is_stale(row.get("updated_at")):
            todo.append(s)

    start = time.monotonic()
    done = 0
    for s in todo[:batch_size * 3]:  # generous cap — budget still dominates
        if done >= batch_size:
            break
        if time.monotonic() - start > budget_s:
            break
        pe, roe, growth = _fetch_fundamentals(s["ticker"], s["exchange"])
        # Always upsert so we record "attempted", avoiding infinite retries
        db.upsert_fundamentals(
            s["ticker"], s["exchange"],
            pe=pe, roe_pct=roe, growth_pct=growth,
            notes="auto" if (pe or roe or growth) else "no-data",
        )
        if pe is not None or roe is not None or growth is not None:
            done += 1
    return done


def _collect_rows(db: Database, stocks: list[dict]) -> list[dict]:
    """Return one row per stock with cached fundamentals attached (may be None)."""
    cached_rows = db.get_fundamentals()
    cached_by_key = {(r["ticker"].upper(), r["exchange"].upper()): r for r in cached_rows}
    out: list[dict] = []
    for s in stocks:
        row = cached_by_key.get((s["ticker"], s["exchange"])) or {}
        out.append({
            "name": s["name"], "ticker": s["ticker"], "exchange": s["exchange"],
            "country": s["country"],
            "pe": row.get("pe"), "roe": row.get("roe_pct"),
            "growth": row.get("growth_pct"),
            "updated_at": row.get("updated_at"),
            "notes": row.get("notes") or "",
        })
    return out


# ---------------------------------------------------------------------------
# Floebertus SVG (unchanged styling)
# ---------------------------------------------------------------------------

FB_CREAM = "#F4E8C8"
FB_BORDER = "#5F7A8C"
FB_BORDER_DARK = "#4a5f6f"
FB_TEXT = "#1F1F1F"
FB_GOLD_OUTER = "#b88f2a"
FB_GOLD_MID   = "#e9c668"
FB_GOLD_INNER = "#fff0b8"


def _render_floebertus_svg(country_label: str, rows: list[dict]) -> str:
    W, H = 1300, 780
    PAD_L, PAD_R, PAD_T, PAD_B = 100, 40, 120, 70

    pts = []
    for r in rows:
        if r.get("pe") is None or r.get("roe") is None:
            continue
        try:
            pe = float(r["pe"]); roe = float(r["roe"])
        except (TypeError, ValueError):
            continue
        # Filter extreme outliers that would crush the axis
        if pe < 0 or pe > 200 or roe < -100 or roe > 200:
            continue
        pts.append({
            "name": r["name"],
            "pe": pe, "roe": roe,
            "growth": float(r["growth"]) if r.get("growth") is not None else 5.0,
            "has_growth": r.get("growth") is not None,
        })

    if not pts:
        return (
            f'<div class="floeb-empty">No fundamentals cached yet for '
            f'<b>{_esc(country_label)}</b>. Click <b>↻ Fetch more</b> to '
            f'pull data from stockanalysis.com.</div>'
        )

    x_vals = [p["pe"] for p in pts]
    y_vals = [p["roe"] for p in pts]
    x_min = max(0.0, min(x_vals) - 2)
    x_max = max(x_vals) + 3
    y_min = min(min(y_vals) - 5, 0)
    y_max = max(y_vals) + 5

    def _rd(x, step):  return int(x // step) * step
    def _ru(x, step):  return int((x + step - 1) // step) * step
    x_min = _rd(x_min, 2); x_max = _ru(x_max, 2)
    y_min = _rd(y_min, 10); y_max = _ru(y_max, 10)
    if x_max == x_min: x_max = x_min + 2
    if y_max == y_min: y_max = y_min + 10

    def sx(pe):  return PAD_L + (pe - x_min) / (x_max - x_min) * (W - PAD_L - PAD_R)
    def sy(roe): return H - PAD_B - (roe - y_min) / (y_max - y_min) * (H - PAD_T - PAD_B)
    def br(growth): return max(6.0, min(55.0, growth * 1.4))

    out = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" '
        f'class="floeb-svg" preserveAspectRatio="xMidYMid meet">',
        '<defs>',
        f'<radialGradient id="fbGold" cx="35%" cy="32%" r="65%">'
        f'  <stop offset="0%"  stop-color="{FB_GOLD_INNER}"/>'
        f'  <stop offset="55%" stop-color="{FB_GOLD_MID}"/>'
        f'  <stop offset="100%" stop-color="{FB_GOLD_OUTER}"/>'
        f'</radialGradient>',
        '<filter id="fbShadow" x="-5%" y="-5%" width="115%" height="115%">'
        '  <feGaussianBlur in="SourceAlpha" stdDeviation="3"/>'
        '  <feOffset dx="4" dy="4" result="offsetblur"/>'
        '  <feComponentTransfer><feFuncA type="linear" slope="0.35"/></feComponentTransfer>'
        '  <feMerge><feMergeNode/><feMergeNode in="SourceGraphic"/></feMerge>'
        '</filter>',
        '</defs>',
        f'<rect x="10" y="10" width="{W-20}" height="{H-20}" '
        f'rx="18" ry="18" fill="{FB_CREAM}" stroke="{FB_BORDER}" stroke-width="1.5"/>',
        f'<rect x="{PAD_L}" y="{PAD_T}" '
        f'width="{W - PAD_L - PAD_R}" height="{H - PAD_T - PAD_B}" '
        f'fill="{FB_CREAM}" stroke="{FB_BORDER}" stroke-width="2.2" '
        f'filter="url(#fbShadow)"/>',
    ]

    x_step = 2 if (x_max - x_min) <= 20 else (5 if (x_max - x_min) <= 50 else 10)
    y_step = 10 if (y_max - y_min) <= 100 else 20
    for x in range(int(x_min), int(x_max) + 1, x_step):
        px = sx(x)
        out.append(
            f'<line x1="{px}" y1="{H - PAD_B}" x2="{px}" y2="{H - PAD_B + 6}" '
            f'stroke="{FB_BORDER_DARK}" stroke-width="1.2"/>'
            f'<text x="{px}" y="{H - PAD_B + 22}" text-anchor="middle" '
            f'font-family="Inter, system-ui, sans-serif" font-size="14" '
            f'fill="{FB_TEXT}">{x}</text>'
        )
    for y in range(int(y_min), int(y_max) + 1, y_step):
        py = sy(y)
        out.append(
            f'<line x1="{PAD_L}" y1="{py}" x2="{PAD_L - 6}" y2="{py}" '
            f'stroke="{FB_BORDER_DARK}" stroke-width="1.2"/>'
            f'<text x="{PAD_L - 12}" y="{py + 5}" text-anchor="end" '
            f'font-family="Inter, system-ui, sans-serif" font-size="14" '
            f'fill="{FB_TEXT}">{y}%</text>'
        )

    out.append(
        f'<text x="30" y="52" font-family="Inter, system-ui, sans-serif" '
        f'font-size="22" font-weight="700" fill="{FB_TEXT}">'
        f'ROE vs P/E vs Growth — {_esc(country_label)}</text>'
    )
    out.append(
        f'<text x="30" y="78" font-family="Inter, system-ui, sans-serif" '
        f'font-size="13" fill="{FB_TEXT}" opacity="0.7">'
        f'Floebertus-style screener · full catalog · '
        f'{len(pts)} stock{"s" if len(pts) != 1 else ""} plotted</text>'
    )
    leg_r = br(20)
    leg_cx = W - PAD_R - leg_r - 140
    leg_cy = 55
    out.append(
        f'<circle cx="{leg_cx + leg_r + 5}" cy="{leg_cy}" r="{leg_r}" '
        f'fill="url(#fbGold)" opacity="0.95"/>'
        f'<text x="{leg_cx - 5}" y="{leg_cy + 5}" text-anchor="end" '
        f'font-family="Inter, system-ui, sans-serif" font-size="13" '
        f'font-weight="600" fill="{FB_TEXT}">bubble = growth (20% ref)</text>'
    )

    out.append(
        f'<text x="{(W - PAD_L - PAD_R)/2 + PAD_L}" y="{H - 20}" '
        f'text-anchor="middle" font-family="Inter, system-ui, sans-serif" '
        f'font-size="15" font-weight="700" fill="{FB_TEXT}">P/E ratio</text>'
        f'<text x="{PAD_L - 65}" y="{(H - PAD_T - PAD_B)/2 + PAD_T}" '
        f'text-anchor="middle" font-family="Inter, system-ui, sans-serif" '
        f'font-size="15" font-weight="700" fill="{FB_TEXT}" '
        f'transform="rotate(-90 {PAD_L - 65} {(H - PAD_T - PAD_B)/2 + PAD_T})">'
        f'Return on Equity</text>'
    )

    for p in sorted(pts, key=lambda x: -x["growth"]):
        cx = sx(p["pe"]); cy = sy(p["roe"]); r = br(p["growth"])
        growth_note = f'{p["growth"]:.1f}%' if p["has_growth"] else "n/a"
        out.append(
            f'<circle cx="{cx:.1f}" cy="{cy:.1f}" r="{r:.1f}" '
            f'fill="url(#fbGold)" opacity="0.88" '
            f'stroke="{FB_GOLD_OUTER}" stroke-width="0.5">'
            f'<title>{_esc(p["name"])}'
            f'&#10;P/E {p["pe"]:.1f} · ROE {p["roe"]:.1f}% · Growth {growth_note}'
            f'</title>'
            f'</circle>'
        )
        # Only label bubbles large enough to read a name on
        if r >= 14:
            label = p["name"][:22]
            ly = cy - r - 6 if cy - r - 6 > PAD_T + 10 else cy + r + 14
            out.append(
                f'<text x="{cx:.1f}" y="{ly:.1f}" text-anchor="middle" '
                f'font-family="Inter, system-ui, sans-serif" font-size="11" '
                f'font-weight="700" fill="{FB_TEXT}">{_esc(label)}</text>'
            )

    out.append('</svg>')
    return "".join(out)


# ---------------------------------------------------------------------------
# Page builder
# ---------------------------------------------------------------------------

def _country_counts(universe: list[dict]) -> list[tuple[str, int, int]]:
    """Return (country, total_in_country, fetchable_in_country) sorted by fetchable."""
    from collections import Counter
    total = Counter(s["country"] for s in universe)
    fetchable = Counter(
        s["country"] for s in universe if s["exchange"] in _SA_SUPPORTED_EX
    )
    rows = []
    for c, t in total.items():
        rows.append((c, t, fetchable.get(c, 0)))
    rows.sort(key=lambda r: (-r[2], r[0]))
    return rows


def _render_missing_table(rows: list[dict], max_show: int = 30) -> str:
    missing = [r for r in rows if r.get("pe") is None or r.get("roe") is None]
    if not missing:
        return ""
    tried = [r for r in missing if r.get("notes")]
    unfetched = [r for r in missing if not r.get("notes")]

    body = []
    shown = 0
    for r in tried[:max_show]:
        body.append(
            f'<tr><td>{_esc(r["name"])}</td>'
            f'<td class="mono">{_esc(r["ticker"])}</td>'
            f'<td class="mono">{_esc(r["exchange"])}</td>'
            f'<td class="muted">stockanalysis.com returned no usable values</td></tr>'
        )
        shown += 1
    extra_tried = max(0, len(tried) - shown)

    info_line = []
    if unfetched:
        info_line.append(f'<b>{len(unfetched)}</b> stocks not yet fetched')
    if tried:
        info_line.append(f'<b>{len(tried)}</b> fetched but returned no P/E or ROE')
    if extra_tried:
        info_line.append(f'showing first {shown} of {len(tried)}')

    return (
        '<div class="missing-card">'
        '<h3>Stocks without fundamentals</h3>'
        f'<p class="muted">{" · ".join(info_line)}</p>'
        + (('<table><thead><tr>'
            '<th>Name</th><th>Ticker</th><th>Exchange</th><th>Status</th>'
            '</tr></thead><tbody>' + "".join(body) + '</tbody></table>')
           if body else '')
        + '</div>'
    )


def generate_html(db: Database, config: dict, country: str | None = None,
                  refresh: bool = False,
                  batch_size: int = DEFAULT_BATCH,
                  budget_s: float = DEFAULT_BUDGET_S) -> str:
    universe = _load_universe(db, config)
    country_rows = _country_counts(universe)
    countries = [c for c, _, _ in country_rows]

    if not country or country not in {c for c, _, _ in country_rows}:
        country = countries[0] if countries else ""

    in_country = [s for s in universe if s["country"] == country]

    # Batched fetch on refresh
    fetched = 0
    if refresh and in_country:
        fetched = _batch_fetch(db, in_country, batch_size, budget_s)

    rows = _collect_rows(db, in_country)

    svg = _render_floebertus_svg(country, rows)
    missing_tbl = _render_missing_table(rows)

    def _opt_label(c: str, total: int, fetchable: int) -> str:
        if fetchable == 0:
            return f"{c} ({total} · stockanalysis ✗)"
        if fetchable == total:
            return f"{c} ({total})"
        return f"{c} ({fetchable}/{total})"

    option_html = "".join(
        f'<option value="{_esc(c)}"{" selected" if c == country else ""}>'
        f'{_esc(_opt_label(c, t, fch))}</option>'
        for c, t, fch in country_rows
    )

    total = len(in_country)
    fetchable = sum(1 for s in in_country if s["exchange"] in _SA_SUPPORTED_EX)
    plotted = sum(1 for r in rows if r.get("pe") is not None and r.get("roe") is not None)
    has_row = sum(1 for r in rows if r.get("updated_at"))
    remaining_to_fetch = max(0, fetchable - has_row)

    toast = ""
    if refresh:
        toast = (
            f'<div class="toast">Fetched fundamentals for '
            f'<b>{fetched}</b> more stock{"s" if fetched != 1 else ""}. '
            f'{remaining_to_fetch} fetchable remaining.</div>'
        )

    return _PAGE_TEMPLATE.format(
        option_html=option_html,
        country=_esc(country),
        svg=svg,
        plotted=plotted,
        total=total,
        fetchable=fetchable,
        remaining=remaining_to_fetch,
        missing_table=missing_tbl,
        toast=toast,
        batch_size=batch_size,
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
    max-width: 1400px; margin: 1.5rem auto 3rem; padding: 0 2rem;
}}
.toast {{
    background: var(--accent-dim); border: 1px solid var(--accent);
    color: var(--text); padding: 0.6rem 1rem; border-radius: 8px;
    margin-bottom: 1rem; font-size: 0.88rem;
}}
.toolbar {{
    display: flex; align-items: center; gap: 1rem; flex-wrap: wrap;
    margin-bottom: 1.2rem;
}}
.toolbar label {{ font-size: 0.85rem; color: var(--text-muted); }}
.toolbar select {{
    background: var(--surface); color: var(--text);
    border: 1px solid var(--border); border-radius: 8px;
    padding: 0.5rem 0.85rem; font-size: 0.9rem;
    min-width: 260px;
}}
.toolbar select:focus {{ border-color: var(--accent); outline: none; }}
.toolbar .summary {{
    font-size: 0.82rem; color: var(--text-muted);
    margin-left: 0.3rem;
}}
.toolbar button {{
    background: var(--accent); color: var(--bg);
    border: 1px solid var(--accent); border-radius: 8px;
    padding: 0.5rem 0.9rem; font-size: 0.85rem; font-weight: 700;
    cursor: pointer;
}}
.toolbar button:hover {{ filter: brightness(1.08); }}
.toolbar button:disabled {{
    background: var(--surface2); color: var(--text-muted);
    border-color: var(--border); cursor: not-allowed;
}}
.toolbar .hint {{ font-size: 0.78rem; color: var(--text-muted); }}

.chart-card {{
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 12px; padding: 1rem;
    overflow: hidden;
}}
.floeb-svg {{
    display: block; width: 100%; height: auto; border-radius: 8px;
}}
.floeb-empty {{
    padding: 3rem 1rem; text-align: center;
    background: #F4E8C8; color: #1F1F1F;
    border-radius: 8px; font-size: 0.95rem;
}}

.missing-card {{
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 12px; padding: 1rem 1.25rem; margin-top: 1.5rem;
}}
.missing-card h3 {{ margin: 0 0 0.3rem 0; font-size: 1rem; }}
.missing-card .muted {{ color: var(--text-muted); font-size: 0.82rem; margin: 0 0 0.8rem 0; }}
.missing-card table {{ width: 100%; border-collapse: collapse; font-size: 0.85rem; }}
.missing-card th, .missing-card td {{
    text-align: left; padding: 0.45rem 0.6rem;
    border-bottom: 1px solid var(--border);
}}
.missing-card th {{ color: var(--text-muted); font-weight: 600; }}
.mono {{ font-family: ui-monospace, Menlo, monospace; }}
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
        <a href="/engine-room">⚙ Engine Room</a>
    </nav>
</div>
</div>

<div class="container">
    {toast}
    <div class="toolbar">
        <label>Country:
            <select id="country-select" onchange="onCountryChange(this.value)">
                {option_html}
            </select>
        </label>
        <span class="summary">
            <b>{plotted}</b> plotted · <b>{fetchable}</b> fetchable · <b>{total}</b> in catalog
        </span>
        <button id="refresh-btn" onclick="onRefresh()">↻ Fetch up to {batch_size} more</button>
        <span class="hint">{remaining} stockanalysis-supported stocks not yet fetched</span>
    </div>

    <div class="chart-card">
        {svg}
    </div>

    {missing_table}
</div>

<script>
function onCountryChange(c) {{
    window.location.href = '/screener?country=' + encodeURIComponent(c);
}}
function onRefresh() {{
    var c = document.getElementById('country-select').value;
    var btn = document.getElementById('refresh-btn');
    btn.disabled = true;
    btn.textContent = 'Fetching… (up to ~45s)';
    window.location.href = '/screener?country=' + encodeURIComponent(c) + '&refresh=1';
}}
</script>
</body>
</html>
"""


def save_screener_html(db: Database, config: dict,
                       country: str | None = None,
                       refresh: bool = False) -> str:
    digest_dir = config.get("digest_dir", "./digests")
    os.makedirs(digest_dir, exist_ok=True)
    content = generate_html(db, config, country, refresh=refresh)
    fp = os.path.join(digest_dir, "screener.html")
    with open(fp, "w", encoding="utf-8") as f:
        f.write(content)
    return fp
