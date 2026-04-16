"""
dashboard.py — Self-contained HTML dashboard generator for emerging-edge.

Produces a single .html file with all CSS/JS inline (no external
dependencies). Designed as a dark-themed financial dashboard.

Sections mirror the markdown digest:
  🔴 Urgent       — red-bordered alert cards
  📰 News         — articles grouped by exchange, collapsible
  📅 Upcoming     — earnings table with countdown badges
  💬 Forum Buzz   — grouped by forum source

French content is preserved as-is and tagged with 🇫🇷.
"""

from __future__ import annotations

import base64
import html as html_mod
import os
import webbrowser
from datetime import datetime, timedelta

from db import Database
from stock_search import has_price_source

# ---------------------------------------------------------------------------
# Embedded logo (vikingship.jpeg, base64-encoded for self-contained HTML)
# ---------------------------------------------------------------------------

def _load_logo_b64() -> str:
    """Load vikingship.jpeg from the project dir and return as base64 data URI."""
    logo_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "vikingship.jpeg")
    if os.path.exists(logo_path):
        with open(logo_path, "rb") as f:
            b64 = base64.b64encode(f.read()).decode()
        return f"data:image/jpeg;base64,{b64}"
    return ""  # graceful fallback — no logo


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def _esc(text: str) -> str:
    """HTML-escape user content."""
    return html_mod.escape(str(text)) if text else ""


def _strip_html(text: str) -> str:
    """Strip HTML tags, unescape entities, and normalize whitespace."""
    import re as _re
    if not text:
        return ""
    # Unescape HTML entities first (&lt; → <, &amp; → &, &nbsp; → space)
    cleaned = html_mod.unescape(str(text))
    # Strip any HTML tags
    cleaned = _re.sub(r"<[^>]+>", "", cleaned)
    # Normalize non-breaking spaces and zero-width chars to regular space
    cleaned = _re.sub(r"[\xa0\u200b]+", " ", cleaned)
    return cleaned.strip()


def _has_unsupported_language(text: str) -> bool:
    """
    Check if text is in an unsupported language.
    Allows: English, French, Swedish, Italian.
    Blocks: Cyrillic scripts, CJK, Arabic, Korean,
    and Latin-script languages like Polish, Spanish, Portuguese, German,
    Turkish, Malay (detected by common marker words).
    """
    import re as _re
    if not text:
        return False
    # Block non-Latin scripts
    if _re.search(
        r'[\u0400-\u04FF'   # Cyrillic
        r'\u0600-\u06FF'    # Arabic
        r'\u4E00-\u9FFF'    # CJK
        r'\u3040-\u30FF'    # Japanese
        r'\uAC00-\uD7AF]',  # Korean
        text):
        return True
    # Block other Latin-script languages by detecting common marker words
    tl = text.lower()
    _OTHER_LANG_MARKERS = [
        # Polish
        "możecie", "dzięki", "dziś", "przez", "będzie", "również", "spółka",
        # Spanish
        "también", "después", "según", "además", "está", "deber esta",
        # Portuguese
        "também", "através", "então", "após", "resultados financeiros",
        # German
        "über", "können", "geschäft", "unternehmen", "ergebnis",
        # Turkish
        "hakkında", "şirket", "yatırım", "sonuçları",
        # Malay/Indonesian
        "adalah", "dengan", "untuk", "dalam", "keputusan",
    ]
    return any(marker in tl for marker in _OTHER_LANG_MARKERS)


def _fmt_price(price: float) -> str:
    """
    Smart price formatting:
      >= 100     → no decimals    (e.g. "7600", "1855")
      >= 10      → 2 decimals     (e.g. "26.20", "45.95")
      >= 0.1     → 3 decimals     (e.g. "0.595", "1.300")
      < 0.1      → 3 decimals     (e.g. "0.084")

    Uses 3 decimals for prices under 10 to capture sub-cent
    moves on exchanges like KLSE (sen) and SGX (cents).
    """
    if price >= 100:
        return f"{price:.0f}"
    elif price >= 10:
        return f"{price:.2f}"
    else:
        return f"{price:.3f}"


def _normalize_date(date_str: str) -> str:
    """
    Convert various date formats to ISO YYYY-MM-DD for consistent sorting.
    Returns original string if parsing fails (sorts to bottom).
    """
    if not date_str:
        return ""
    s = date_str.strip()
    # Already ISO
    if len(s) >= 10 and s[4] == '-' and s[7] == '-':
        return s[:10]
    for fmt in ("%b %d, %Y", "%d %b %Y", "%d %B %Y", "%B %d, %Y",
                "%d/%m/%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(s[:20].strip().rstrip('.'), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    # Handle relative dates like "5 days ago", "3 weeks ago"
    import re as _re
    m = _re.match(r"(\d+)\s+(day|week|month|year)s?\s+ago", s, _re.I)
    if m:
        n = int(m.group(1))
        unit = m.group(2).lower()
        days_back = {"day": 1, "week": 7, "month": 30, "year": 365}.get(unit, 1) * n
        return (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    return date_str  # fallback — won't sort perfectly but won't crash


def _parse_news_epoch(date_str: str) -> int:
    """
    Parse a published date string and return Unix epoch seconds (UTC).
    Returns 0 if unparseable. Handles Yahoo RSS, ISO, common Serper formats.
    """
    if not date_str:
        return 0
    s = date_str.strip()
    # Yahoo RSS format: "Tue, 14 Apr 2026 21:00:00 +0000"
    for fmt in ("%a, %d %b %Y %H:%M:%S %z", "%a, %d %b %Y %H:%M:%S %Z"):
        try:
            return int(datetime.strptime(s, fmt).timestamp())
        except (ValueError, TypeError):
            pass
    # ISO timestamp with T separator
    try:
        clean = s.replace("Z", "+00:00")
        if "T" in clean:
            return int(datetime.fromisoformat(clean).timestamp())
    except (ValueError, TypeError):
        pass
    # ISO date only
    if len(s) >= 10 and s[4] == '-' and s[7] == '-':
        try:
            return int(datetime.strptime(s[:10], "%Y-%m-%d").timestamp())
        except ValueError:
            pass
    # Common natural-language formats from Serper
    for fmt in ("%b %d, %Y", "%d %b %Y", "%d %B %Y", "%B %d, %Y",
                "%d/%m/%Y", "%m/%d/%Y"):
        try:
            return int(datetime.strptime(s[:20].strip().rstrip('.'), fmt).timestamp())
        except ValueError:
            continue
    # Relative dates including sub-day: "9 hours ago", "5 minutes ago",
    # "3 days ago", "2 weeks ago", etc. Capture hour/minute resolution
    # so news items from earlier today sort ABOVE items from yesterday.
    import re as _re
    m = _re.match(
        r"(\d+)\s+(second|minute|hour|day|week|month|year)s?\s+ago", s, _re.I)
    if m:
        n = int(m.group(1))
        unit = m.group(2).lower()
        secs_back = {
            "second": 1, "minute": 60, "hour": 3600,
            "day": 86400, "week": 604800,
            "month": 2592000, "year": 31536000,
        }.get(unit, 1) * n
        return int(datetime.now().timestamp()) - secs_back
    # Short forms like "9h ago", "12m ago", "3d ago"
    m = _re.match(r"(\d+)\s*([smhdw])\s*ago", s, _re.I)
    if m:
        n = int(m.group(1))
        unit = m.group(2).lower()
        secs_back = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}.get(unit, 1) * n
        return int(datetime.now().timestamp()) - secs_back
    return 0


def _fmt_date_compact(iso_date: str) -> str:
    """Format ISO date as compact '9APR26'. Returns original if unparseable."""
    try:
        dt = datetime.strptime(iso_date[:10], "%Y-%m-%d")
        return f"{dt.day}{dt.strftime('%b').upper()}{dt.strftime('%y')}"
    except (ValueError, TypeError):
        return iso_date


def _countdown_class(days: int) -> str:
    """Return CSS class name for an earnings countdown badge."""
    if days < 0:
        return "badge-past"
    if days <= 7:
        return "badge-urgent"
    if days <= 14:
        return "badge-soon"
    return "badge-ok"


# ---------------------------------------------------------------------------
# CSS — dark financial dashboard theme
# ---------------------------------------------------------------------------

CSS = """
:root {
    --bg:          #0f1117;
    --surface:     #1a1d27;
    --surface2:    #232733;
    --border:      #2d3040;
    --text:        #e2e4ea;
    --text-muted:  #8b8fa3;
    --accent:      #6c8cff;
    --accent-dim:  #3d5199;
    --red:         #ff4d6a;
    --red-dim:     rgba(255,77,106,0.12);
    --amber:       #ffb84d;
    --amber-dim:   rgba(255,184,77,0.12);
    --green:       #4ddb8a;
    --green-dim:   rgba(77,219,138,0.12);
    --blue-dim:    rgba(108,140,255,0.12);
}

* { margin: 0; padding: 0; box-sizing: border-box; }

body {
    font-family: -apple-system, BlinkMacSystemFont, 'Inter', 'Segoe UI', system-ui, sans-serif;
    background: var(--bg);
    color: var(--text);
    line-height: 1.6;
    min-height: 100vh;
}

/* ── Header ── */
.header {
    background: var(--surface);
    border-bottom: 1px solid var(--border);
    padding: 0.65rem 2rem 0.55rem;
    position: sticky; top: 0; z-index: 100;
}
.header-inner {
    max-width: 1400px; margin: 0 auto;
    display: flex; align-items: center; gap: 0.75rem 1.25rem;
    flex-wrap: wrap;
}
.header-brand {
    display: flex; align-items: center; gap: 0.6rem;
    min-width: 0;
}
.header-logo {
    height: 32px; width: auto; border-radius: 6px;
    object-fit: contain;
}
.header h1 {
    font-size: 1.1rem; font-weight: 700; letter-spacing: -0.02em;
    color: var(--text); margin: 0; white-space: nowrap;
}
.header h1 span { color: var(--accent); }
.header-nav {
    display: flex; align-items: center; gap: 0.35rem;
    flex-wrap: wrap;
}
/* All header-nav chips share the same pill shape; solid vs ghost is a
 * fill variant. Grouping every variant in one selector ensures no
 * missing property (padding/radius/border) regardless of element type. */
.header-nav a,
.header-nav .ghost-btn,
.header-nav .solid-btn {
    display: inline-flex; align-items: center;
    font-size: 0.72rem; font-weight: 600;
    padding: 0.28rem 0.75rem;
    border: 1px solid var(--accent);
    border-radius: 999px;
    color: var(--accent);
    text-decoration: none; cursor: pointer;
    white-space: nowrap;
    transition: background 0.12s ease, color 0.12s ease, transform 0.08s ease;
}
.header-nav a:hover,
.header-nav .ghost-btn:hover {
    background: var(--accent-dim);
}
.header-nav .solid-btn {
    color: #fff; background: var(--accent);
    box-shadow: 0 1px 3px rgba(108, 140, 255, 0.25);
}
.header-nav .solid-btn:hover {
    background: #5a7ae6; transform: translateY(-1px);
}
.header-nav .solid-btn:active { transform: translateY(0); }

/* KPI row: compact inline stats */
.header-kpis {
    display: flex; align-items: center; flex-wrap: wrap;
    gap: 0 0.85rem; margin-left: auto;
    font-size: 0.75rem;
}
.header-kpis .kpi {
    display: inline-flex; align-items: baseline; gap: 0.3rem;
    text-decoration: none; color: var(--text-muted);
    padding: 0.15rem 0;
    white-space: nowrap;
}
.header-kpis .kpi:hover { color: var(--accent); }
.header-kpis .kpi-val {
    font-weight: 700; font-size: 0.95rem; color: var(--text);
}
.header-kpis .kpi:hover .kpi-val { color: var(--accent); }
.header-kpis .kpi-sep {
    color: var(--border); padding: 0 0.1rem;
}

/* ── Price refresh button (in header, inline with KPIs) ── */
.price-refresh-btn {
    padding: 0.2rem 0.55rem; border-radius: 999px;
    font-size: 0.68rem; font-weight: 700;
    background: var(--surface2); color: var(--text-muted);
    border: 1px solid var(--border); cursor: pointer;
    transition: none;
    display: inline-flex; align-items: center; gap: 0.3rem;
    margin-left: 0.5rem;
    white-space: nowrap;
}
.price-refresh-btn:hover {
    background: var(--accent-dim); color: var(--accent);
    border-color: var(--accent-dim);
}
.price-refresh-btn.busy {
    background: var(--surface2); color: var(--text-muted);
    cursor: wait; pointer-events: none;
}
.price-refresh-btn .mini-spinner {
    display: none; width: 10px; height: 10px;
    border: 2px solid var(--text-muted); border-top-color: var(--accent);
    border-radius: 50%; animation: spin 0.8s linear infinite;
}
.price-refresh-btn.busy .mini-spinner { display: inline-block; }

/* ── Filter row (exchanges + stocks on one line) ── */
.filter-row {
    max-width: 1400px; margin: 0.5rem auto 0;
    padding: 0 2rem;
    display: flex; gap: 0.75rem 1rem; flex-wrap: wrap;
    align-items: center;
}
.filter-group {
    display: flex; gap: 0.35rem; flex-wrap: wrap; align-items: center;
    min-width: 0;
}
.filter-group-label {
    font-size: 0.62rem; text-transform: uppercase;
    letter-spacing: 0.06em; color: var(--text-muted);
    font-weight: 700; margin-right: 0.2rem;
}
.filter-group.stocks {
    border-left: 1px solid var(--border);
    padding-left: 1rem;
}

/* Legacy .filters — kept for any residual uses, made inert */
.filters {
    max-width: 1400px; margin: 0.5rem auto 0;
    padding: 0 2rem;
    display: flex; gap: 0.5rem; flex-wrap: wrap;
}
.filter-pill {
    padding: 0.3rem 0.85rem; border-radius: 999px;
    font-size: 0.78rem; font-weight: 600;
    background: var(--surface2); color: var(--text-muted);
    border: 1px solid var(--border); cursor: pointer;
    transition: none;
}
.filter-pill:hover {
    border-color: var(--accent-dim); color: var(--text);
}
.filter-pill.active {
    background: var(--accent); color: #fff; border-color: var(--accent);
}

/* ── Stock panel: one flat flex-wrap grid of chips for all exchanges ── */
.stock-panel {
    /* Base panel — layout is controlled by #stock-panels-wrapper (flex).
     * Expanded: flex: 1 1 100% → full row. Collapsed: flex: 0 0 auto → pill. */
    min-width: 0;
}
.exchange-status-bar {
    display: flex; flex-wrap: wrap; gap: 0.4rem 1rem;
}
.exchange-status-bar .exchange-status:empty { display: none; }
.stock-panel-header {
    display: flex; align-items: baseline; flex-wrap: wrap;
    gap: 0.4rem; margin: 0.5rem 0 0.2rem;
    font-size: 0.78rem;
    cursor: pointer;
    user-select: none;
    padding: 0.2rem 0.35rem;
    border-radius: 6px;
    transition: background 0.12s;
}
.stock-panel-header:hover { background: var(--surface2); }
.stock-panel-header .panel-chevron {
    display: inline-block;
    color: var(--text-muted);
    font-size: 0.7rem;
    transition: transform 0.15s;
    width: 0.9rem;
}
.stock-panel.panel-collapsed .stock-panel-header .panel-chevron {
    transform: rotate(-90deg);
}
.stock-panel.panel-collapsed .stock-panel-inner {
    display: none;
}
.stock-panel-country {
    font-weight: 700; color: var(--text);
    letter-spacing: 0.01em;
}
.stock-panel-sep { color: var(--text-muted); opacity: 0.6; }
.stock-panel-exchanges {
    color: var(--text-muted); font-size: 0.72rem; font-weight: 500;
}
.stock-panel-count {
    color: var(--text-muted); font-size: 0.7rem;
    background: var(--surface2); padding: 0.06rem 0.45rem;
    border-radius: 999px;
}
.stock-panel-header .exchange-status {
    margin-left: auto; font-size: 0.7rem;
}
.stock-panel-inner {
    display: flex; gap: 0.6rem; flex-wrap: wrap;
    padding: 0.5rem 0 0.75rem;
}
.stock-chip {
    position: relative;
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 0.55rem 0.85rem;
    min-width: 170px;
    transition: border-color 0.15s;
}
.stock-chip { cursor: pointer; }
/* Use !important so density-line/mini `display: flex` rules don't
 * override the filter-hidden state. Exchange filter correctness
 * trumps density layout. */
.stock-chip.filtered-out { display: none !important; }
.stock-chip:hover { border-color: var(--accent-dim); }
.stock-chip.chip-active {
    border-color: var(--accent);
    box-shadow: 0 0 0 1px var(--accent);
    background: var(--accent-dim);
}
.stock-chip:hover .stock-chip-remove { opacity: 1; }
.stock-chip-remove {
    position: absolute; top: 4px; right: 6px;
    width: 18px; height: 18px;
    display: flex; align-items: center; justify-content: center;
    font-size: 0.85rem; line-height: 1; cursor: pointer;
    color: var(--text-muted); border-radius: 4px;
    opacity: 0; transition: opacity 0.15s, background 0.15s, color 0.15s;
    user-select: none;
}
.stock-chip-remove:hover {
    background: var(--red-dim); color: var(--red); opacity: 1;
}
.stock-chip-name {
    font-size: 0.78rem; font-weight: 700; color: var(--text);
}
.stock-chip-ticker {
    font-size: 0.68rem; color: var(--text-muted);
}
.stock-chip-price {
    font-size: 1rem; font-weight: 700; color: var(--text);
    margin-top: 0.2rem;
}
.stock-chip-change {
    display: inline-block; font-size: 0.72rem; font-weight: 700;
    padding: 0.1rem 0.45rem; border-radius: 4px;
    margin-left: 0.3rem;
}
.stock-chip-change.up   { background: var(--green-dim); color: var(--green); }
.stock-chip-change.down { background: var(--red-dim); color: var(--red); }
.stock-chip-change.flat { background: var(--surface2); color: var(--text-muted); }
.stock-chip-nodata {
    font-size: 0.75rem; color: var(--text-muted); margin-top: 0.2rem;
}
/* ── Density variants for the stock-chip grid ──
 * Chip mode (default): boxy cards, ~170px wide, 3 lines tall.
 * Line mode: single-row horizontal cards, flex-wrap, all info on one line.
 * Mini mode: compact ticker + change%, 7-8 per row.
 * The body class .density-line / .density-mini swaps which rules apply. */
body.density-line .stock-panel-inner {
    gap: 0.35rem 0.6rem;
}
body.density-line .stock-chip {
    min-width: 0;
    flex: 1 1 calc(50% - 0.6rem);
    max-width: calc(50% - 0.6rem);
    padding: 0.4rem 0.7rem;
    display: flex; align-items: center; gap: 0.5rem;
}
@media (min-width: 900px) {
    body.density-line .stock-chip {
        flex-basis: calc(33.33% - 0.6rem);
        max-width: calc(33.33% - 0.6rem);
    }
}
@media (min-width: 1200px) {
    body.density-line .stock-chip {
        flex-basis: calc(25% - 0.6rem);
        max-width: calc(25% - 0.6rem);
    }
}
/* Line mode layout:
 *   [TICKER] Name (truncates)       PRICE ±X.X%  ✕
 *   The ticker is the anchor (bold, never truncated).
 *   The name degrades gracefully with ellipsis — hover title shows full name.
 *   Price/change are right-aligned and pinned (tabular-nums). */
body.density-line .stock-chip-ticker {
    font-size: 0.72rem; font-weight: 700; color: var(--text);
    flex: 0 0 auto;
    white-space: nowrap; opacity: 1;
}
/* In line/mini modes the code suffix (e.g. "· 5236") just noise; hide it. */
body.density-line .stock-chip-ticker .tk-sep,
body.density-line .stock-chip-ticker .tk-code,
body.density-mini .stock-chip-ticker .tk-sep,
body.density-mini .stock-chip-ticker .tk-code { display: none; }
body.density-line .stock-chip-name {
    font-size: 0.7rem; font-weight: 400; color: var(--text-muted);
    flex: 1 1 auto; min-width: 0;
    white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
    order: 2;
}
body.density-line .stock-chip-ticker { order: 1; }
body.density-line .stock-chip-price {
    font-size: 0.76rem; font-weight: 600; margin: 0;
    flex: 0 0 auto;
    white-space: nowrap;
    font-variant-numeric: tabular-nums;
    order: 3;
}
body.density-line .stock-chip-change {
    font-size: 0.66rem; padding: 0.05rem 0.35rem;
    font-variant-numeric: tabular-nums;
    margin-left: 0.25rem;
}
body.density-line .stock-chip-nodata {
    font-size: 0.66rem; margin: 0;
    flex: 0 0 auto;
    white-space: nowrap;
    order: 3;
}
body.density-line .stock-chip-remove {
    position: static; width: 14px; height: 14px; font-size: 0.7rem;
    flex: 0 0 auto; opacity: 0.4;
    order: 99;
}
body.density-line .stock-chip:hover .stock-chip-remove { opacity: 1; }

/* Mini: just ticker + change, 7 across */
body.density-mini .stock-panel-inner {
    gap: 0.3rem;
}
body.density-mini .stock-chip {
    min-width: 0;
    flex: 0 0 calc(14.28% - 0.3rem);
    max-width: calc(14.28% - 0.3rem);
    padding: 0.35rem 0.55rem;
    display: flex; flex-direction: column; gap: 0.1rem;
}
@media (max-width: 1000px) {
    body.density-mini .stock-chip {
        flex-basis: calc(25% - 0.3rem);
        max-width: calc(25% - 0.3rem);
    }
}
body.density-mini .stock-chip-name { display: none; }
body.density-mini .stock-chip-ticker {
    font-size: 0.72rem; font-weight: 700; color: var(--text);
    white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
}
body.density-mini .stock-chip-price {
    font-size: 0.7rem; margin: 0;
    display: flex; justify-content: space-between; align-items: center; gap: 0.3rem;
    font-variant-numeric: tabular-nums;
}
body.density-mini .stock-chip-price > :first-child {
    font-weight: 500; opacity: 0.75;
    white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
    min-width: 0;
}
body.density-mini .stock-chip-change {
    font-size: 0.62rem; padding: 0.04rem 0.3rem;
}
body.density-mini .stock-chip-nodata {
    font-size: 0.6rem; margin: 0;
    white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
}
body.density-mini .stock-chip-remove { display: none; }

/* ── Sticky sub-header with the collapse button + mover summary ──
 * Sits just below the main .header (which is sticky at top:0) so both
 * the KPIs and the stock reference follow the user as they scroll
 * into news/earnings/forums. Works even when the main header is
 * collapsed — stays attached to the viewport at top: ~header height. */
.stock-layout-toggle {
    position: sticky; top: 0; z-index: 90;
    background: var(--bg);
    border-bottom: 1px solid var(--border);
    margin: 0; padding: 0;
}
.stock-layout-toggle-inner {
    max-width: 1400px; margin: 0 auto;
    padding: 0.4rem 2rem;
    display: flex; align-items: center; gap: 0.8rem;
    flex-wrap: wrap;
}
.stock-layout-toggle-spacer { flex: 1 1 auto; }
.stl-label {
    font-size: 0.72rem; color: var(--text-muted);
    cursor: pointer; display: inline-flex; align-items: center;
    gap: 0.3rem;
}

/* The density/collapse toggle docks beneath the main header. Give the
 * main header a specific height anchor so scroll-to-anchor behavior
 * (e.g. section hash jumps) clears both bars properly. */
.section { scroll-margin-top: 9rem; }

/* Stock filter pill row was a parallel filter UI to the chip grid.
 * Now that every chip is click-to-filter at any density, the pill
 * row is pure redundancy — hide it unconditionally. Freed header
 * space goes to the actual content below. */
.filter-group.stocks {
    display: none;
}

/* Collapse/expand button for the stock-panels section */
.stocks-collapse-btn {
    background: var(--surface2); border: 1px solid var(--border);
    color: var(--text-muted); font-size: 0.72rem; font-weight: 600;
    padding: 0.3rem 0.75rem; border-radius: 999px;
    cursor: pointer; display: inline-flex; align-items: center;
    gap: 0.35rem; transition: all 0.15s;
}
.density-count-hint {
    font-size: 0.66rem; font-weight: 500; color: var(--text-muted);
    opacity: 0.7; margin-left: 0.2rem;
}
.panels-bulk-btn {
    background: transparent; border: 1px solid var(--border);
    color: var(--text-muted); font-size: 0.68rem; font-weight: 600;
    padding: 0.2rem 0.55rem; border-radius: 999px;
    cursor: pointer; transition: all 0.15s;
}
.panels-bulk-btn:hover { border-color: var(--accent); color: var(--text); }
.stocks-collapse-btn:hover {
    border-color: var(--accent); color: var(--text);
}
.stocks-collapse-btn #stocks-collapse-icon {
    display: inline-block;
    transition: transform 0.15s;
    font-size: 0.6rem;
}
body.stocks-collapsed .stocks-collapse-btn #stocks-collapse-icon {
    transform: rotate(-90deg);
}
body.stocks-collapsed #stock-panels-wrapper {
    display: none;
}

/* When a mix of expanded / collapsed panels exists, let the
 * collapsed ones flow horizontally so 25 collapsed countries
 * don't eat 25 vertical rows. Expanded panels still take full
 * width (natural block). */
#stock-panels-wrapper {
    max-width: 1400px; margin: 0 auto;
    padding: 0 2rem;
    display: flex; flex-wrap: wrap;
    gap: 0.35rem 0.6rem;
    align-items: flex-start;
}
#stock-panels-wrapper > .stock-panel {
    flex: 1 1 100%;   /* expanded: full-row */
    min-width: 0;
    padding: 0;  /* wrapper handles outer spacing */
}
#stock-panels-wrapper > .stock-panel.panel-collapsed {
    /* collapsed → auto-width pill that wraps horizontally */
    flex: 0 0 auto;
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 999px;
    padding: 0;
    margin: 0;
}
/* Tighten the header inside a collapsed pill — no margin, smaller gap */
.stock-panel.panel-collapsed .stock-panel-header {
    margin: 0;
    padding: 0.2rem 0.55rem;
    font-size: 0.74rem;
    flex-wrap: nowrap;
    white-space: nowrap;
}
.stock-panel.panel-collapsed .stock-panel-header .stock-panel-exchanges,
.stock-panel.panel-collapsed .stock-panel-header .stock-panel-sep {
    display: none;  /* keep pills compact — just country + count */
}
.stock-panel.panel-collapsed .stock-panel-header .exchange-status {
    display: none;  /* too noisy inside a pill */
}
/* When collapsed, show a compact summary strip: "77 stocks · 4 up today · 6 down · Top +5.2% TIGO · Bottom -3.1% CARB" */
.stocks-summary-strip {
    display: inline-flex; align-items: center; gap: 0.6rem;
    font-size: 0.7rem; color: var(--text-muted);
    flex-wrap: wrap;
}
.stocks-summary-strip .summary-mover {
    display: inline-flex; align-items: center; gap: 0.25rem;
    padding: 0.08rem 0.4rem; border-radius: 999px;
    background: var(--surface2); border: 1px solid var(--border);
    cursor: pointer;
}
.stocks-summary-strip .summary-mover.up   { color: var(--green); border-color: var(--green-dim); }
.stocks-summary-strip .summary-mover.down { color: var(--red);   border-color: var(--red-dim); }
.stocks-summary-strip .summary-mover:hover { background: var(--surface); }
.stocks-summary-strip .summary-sep {
    opacity: 0.4;
}

/* Sticky selected-stock chip: shows above the freeze pane when a
 * single stock is selected, so the user always knows what they're
 * looking at while scrolling through news/earnings/forums. */
.selected-stock-chip {
    display: inline-flex; align-items: center; gap: 0.5rem;
    padding: 0.25rem 0.7rem 0.25rem 0.55rem;
    background: var(--accent-dim);
    border: 1px solid var(--accent);
    border-radius: 999px;
    font-size: 0.75rem;
    color: var(--text);
}
.selected-stock-chip .ssc-ticker {
    font-weight: 700; color: var(--accent);
}
.selected-stock-chip .ssc-name {
    color: var(--text-muted); font-weight: 500;
    max-width: 10rem;
    white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
}
.selected-stock-chip .ssc-price {
    font-variant-numeric: tabular-nums; font-weight: 600;
}
.selected-stock-chip .ssc-change {
    font-variant-numeric: tabular-nums; font-size: 0.7rem;
    padding: 0.06rem 0.38rem; border-radius: 4px; font-weight: 700;
}
.selected-stock-chip .ssc-change.up   { background: var(--green-dim); color: var(--green); }
.selected-stock-chip .ssc-change.down { background: var(--red-dim);   color: var(--red); }
.selected-stock-chip .ssc-change.flat { background: var(--surface2);  color: var(--text-muted); }
.selected-stock-chip .ssc-clear {
    cursor: pointer; color: var(--text-muted);
    font-size: 0.9rem; line-height: 1; padding: 0 0.2rem;
    border-radius: 4px;
}
.selected-stock-chip .ssc-clear:hover { color: var(--text); background: var(--surface2); }

/* Density pill toggle */
.density-pills {
    display: inline-flex; gap: 0;
    background: var(--surface2); border: 1px solid var(--border);
    border-radius: 999px; padding: 2px;
}
.density-pill {
    background: transparent; border: none; color: var(--text-muted);
    font-size: 0.7rem; font-weight: 600;
    padding: 0.2rem 0.7rem; border-radius: 999px;
    cursor: pointer; transition: all 0.15s;
}
.density-pill:hover { color: var(--text); }
.density-pill.active {
    background: var(--accent); color: var(--bg);
}

.stock-chip-nodata.nosource {
    font-style: italic; opacity: 0.65;
}
.stock-chip-nodata.awaiting::before {
    content: "⟳ "; opacity: 0.7;
}

/* ── FX rates box (inline with the header KPIs) ── */
.fx-box {
    background: var(--surface2); border: 1px solid var(--border);
    border-radius: 8px; padding: 0.3rem 0.6rem;
    display: flex; flex-wrap: wrap;
    gap: 0.2rem 0.75rem;
    font-size: 0.68rem;
    flex-shrink: 1; min-width: 0;
    flex-basis: 100%;
    max-width: 1400px; margin: 0.35rem 0 0;
}
@media (max-width: 600px) {
    .fx-box {
        font-size: 0.62rem; padding: 0.25rem 0.5rem;
        gap: 0.15rem 0.55rem;
    }
}
.fx-pair {
    display: flex; align-items: center; gap: 0.25rem;
    white-space: nowrap;
}
.fx-label { font-weight: 600; color: var(--text-muted); }
.fx-rate { color: var(--text); font-weight: 600; }
.fx-up { color: var(--green); font-weight: 600; font-size: 0.62rem; }
.fx-down { color: var(--red); font-weight: 600; font-size: 0.62rem; }
.fx-flat { color: var(--text-muted); font-size: 0.62rem; }

/* ── Exchange open/closed status ── */
.exchange-status {
    padding: 0 2rem 0.4rem;
    font-size: 0.78rem; font-weight: 600;
    display: flex; align-items: center; gap: 0.5rem;
}
.exchange-status .status-dot {
    width: 8px; height: 8px; border-radius: 50%;
    display: inline-block;
}
.exchange-status .status-dot.open { background: var(--green); box-shadow: 0 0 6px var(--green); }
.exchange-status .status-dot.closed { background: var(--red); opacity: 0.6; }
.exchange-status .status-text { color: var(--text-muted); }
.exchange-status .status-label-open { color: var(--green); }
.exchange-status .status-label-closed { color: var(--text-muted); }

/* ── Main grid ── */
.container {
    max-width: 1400px; margin: 0 auto;
    padding: 0.75rem 2rem 5rem;
    display: grid;
    grid-template-columns: minmax(0, 2fr) minmax(320px, 1fr);
    grid-template-areas:
        "alerts    alerts"
        "news      earnings"
        "forum     forum"
        "insider   insider";
    gap: 1rem;
    align-items: start;
}
.section {
    min-width: 0; scroll-margin-top: 11rem;
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 0.9rem 1.1rem 1rem;
    transition: border-color 0.15s;
}
.section:hover { border-color: var(--border); }
.section.empty { opacity: 0.55; }
#alerts-section   { grid-area: alerts; }
#news-section     { grid-area: news; }
#earnings-section { grid-area: earnings; }
#insider-section  { grid-area: insider; }
#forum-section    { grid-area: forum; }

/* Alerts renders as a horizontal strip — cards sit side-by-side,
 * the bar scrolls horizontally when there are more than fit the
 * viewport. Height is dictated by content. */
#alerts-section {
    padding: 0.75rem 0.9rem 0.85rem;
}
.alerts-strip {
    display: flex;
    gap: 0.6rem;
    overflow-x: auto;
    padding-bottom: 0.2rem;
    scrollbar-width: thin;
    scroll-snap-type: x proximity;
}
.alerts-strip::-webkit-scrollbar { height: 6px; }
.alerts-strip::-webkit-scrollbar-thumb {
    background: var(--border); border-radius: 3px;
}
.alerts-strip .alert-card {
    flex: 0 0 auto;
    min-width: 260px; max-width: 340px;
    margin-bottom: 0;
    padding: 0.6rem 0.8rem;
    scroll-snap-align: start;
}
.alerts-strip .alert-card .alert-stock { font-size: 0.78rem; }
.alerts-strip .alert-card .alert-title { font-size: 0.78rem; }
.alerts-strip .empty {
    flex: 1 1 auto; text-align: center; padding: 0.4rem 0;
    font-size: 0.82rem; color: var(--text-muted);
}

/* News and Forum truncate after the first N items and expand on click.
 * Cards beyond the threshold get `.collapsed-hidden` added by Python,
 * and the JS toggle removes the class + hides the "Show more" button. */
.news-card.collapsed-hidden, .forum-card.collapsed-hidden { display: none; }

/* News section: scroll internally once expanded so it doesn't push
 * the Insider row at the bottom way off-screen. */
/* News section scrolls at the page level — no internal overflow.
 * Item count is already capped to 10 visible via _applyCollapsedState,
 * with "Show more" to expand. Nested scroll containers caused
 * rubber-band/scroll-capture issues on trackpads. */
#news-section {
    display: flex; flex-direction: column;
}
.show-more-btn {
    display: block; margin: 0.6rem auto 0;
    padding: 0.45rem 1.1rem;
    background: var(--surface2); color: var(--accent);
    border: 1px solid var(--border); border-radius: 999px;
    font-size: 0.75rem; font-weight: 600; cursor: pointer;
    transition: all 0.15s;
}
.show-more-btn:hover {
    border-color: var(--accent); background: var(--accent-dim);
}

/* Earnings sits in the right rail alongside News — short, compact.
 * Tabs stay pinned; the two tab bodies share a single scroll region
 * so tall tables can't overflow the section when all exchanges are
 * selected. */
#earnings-section {
    display: flex; flex-direction: column;
    max-height: 40rem;
}
#earnings-section .stock-filters {
    flex: 0 0 auto; margin-bottom: 0.5rem;
}
#earnings-section .earnings-body {
    flex: 1 1 auto; min-height: 0; overflow-y: auto;
    padding-right: 0.25rem; margin-right: -0.25rem;
}

/* Forum is now full-width below News. With more space available,
 * exchange-group cards lay out in a 2-column grid on wide screens
 * and single-column on narrow ones. Tall ceiling since Forum is
 * expected to hold dozens of mentions. */
#forum-section {
    display: flex; flex-direction: column;
}
#forum-section .exchange-body {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(340px, 1fr));
    gap: 0.5rem 1rem;
}
#forum-section .forum-card { margin-bottom: 0.5rem; }

/* Insider is at the bottom full-width — let it grow as tall as needed
 * but stop runaway lists at 30rem with internal scroll. */
#insider-section {
    display: flex; flex-direction: column;
    max-height: 30rem;
}
#insider-section > :not(.section-title) {
    min-height: 0; overflow-y: auto;
    padding-right: 0.3rem; margin-right: -0.3rem;
}

.section > .section-title { flex: 0 0 auto; }

/* Legacy .section-full is no-op now — grid-area handles placement */
.section-full {}

/* ── Section titles with inline count pill ── */
.section-title {
    font-size: 0.95rem; font-weight: 700;
    margin-bottom: 0.7rem;
    padding-bottom: 0.55rem;
    border-bottom: 1px solid var(--border);
    display: flex; align-items: center; gap: 0.5rem;
    color: var(--text);
}
.section-title .icon { font-size: 1.1rem; }
.section-count {
    background: var(--surface2); color: var(--text-muted);
    border: 1px solid var(--border); border-radius: 999px;
    font-size: 0.7rem; font-weight: 700;
    padding: 0.1rem 0.55rem;
    font-variant-numeric: tabular-nums;
}
.section-hint {
    color: var(--text-muted); font-weight: 400; font-size: 0.72rem;
    margin-left: auto;
}

@media (max-width: 1000px) {
    .container {
        grid-template-columns: 1fr;
        grid-template-areas:
            "alerts"
            "news"
            "earnings"
            "forum"
            "insider";
    }
    /* News spanned multiple rows on desktop; release the span on mobile */
    #news-section { grid-row: auto; max-height: none; }
    #earnings-section, #insider-section, #forum-section { max-height: none; }
    #news-section > :not(.section-title),
    #earnings-section .earnings-body,
    #insider-section > :not(.section-title),
    #forum-section > :not(.section-title) { overflow-y: visible; }
    /* Alerts strip stays horizontal even on mobile (natural for a ticker) */
}

/* ── Alert cards (Urgent) ── */
.alert-card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-left: 3px solid var(--red);
    border-radius: 8px;
    padding: 0.85rem 1rem;
    margin-bottom: 0.6rem;
    transition: border-color 0.15s;
}
.alert-card:hover { border-left-color: #ff7a8f; }
.alert-card.price-up { border-left-color: var(--green); }
.alert-card.price-down { border-left-color: var(--red); }
.alert-stock {
    font-weight: 700; font-size: 0.85rem; color: var(--accent);
    margin-bottom: 0.2rem;
}
.alert-title a {
    color: var(--text); text-decoration: none; font-size: 0.82rem;
}
.alert-title a:hover { color: var(--accent); text-decoration: underline; }
.alert-meta {
    font-size: 0.72rem; color: var(--text-muted); margin-top: 0.25rem;
}
.alert-date {
    display: inline-block; font-size: 0.68rem; font-weight: 600;
    padding: 0.1rem 0.5rem; border-radius: 4px;
    background: var(--surface2); color: var(--text-muted);
    margin-top: 0.3rem;
}
.urgent-toggle {
    font-size: 0.78rem; color: var(--accent); cursor: pointer;
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 8px; padding: 0.6rem; text-align: center;
    margin-top: 0.4rem;
}
.urgent-toggle:hover { background: var(--surface2); }

/* ── News cards ── */
.exchange-group {
    margin-bottom: 1rem;
}
.exchange-header {
    font-size: 0.82rem; font-weight: 700;
    color: var(--text-muted); text-transform: uppercase;
    letter-spacing: 0.06em;
    padding: 0.4rem 0;
    border-bottom: 1px solid var(--border);
    margin-bottom: 0.5rem;
    cursor: pointer;
    display: flex; align-items: center; justify-content: space-between;
}
.exchange-header .chevron {
    transition: transform 0.2s;
    font-size: 0.7rem;
}
.exchange-header.collapsed .chevron { transform: rotate(-90deg); }
.exchange-body.hidden { display: none; }

.news-card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 0.75rem 1rem;
    margin-bottom: 0.5rem;
    transition: border-color 0.15s;
}
.news-card:hover { border-color: var(--accent-dim); }
.news-stock {
    font-weight: 700; font-size: 0.8rem; color: var(--accent);
    display: flex; align-items: center; gap: 0.4rem; flex-wrap: wrap;
}
/* Inline exchange badge on cards (shown in flat/chronological layout).
 * Color-coded by region so the eye can scan by geography. */
.ex-badge {
    display: inline-block;
    font-size: 0.62rem; font-weight: 600; letter-spacing: 0.02em;
    padding: 0.08rem 0.42rem; border-radius: 999px;
    background: var(--surface2); color: var(--text-muted);
    border: 1px solid var(--border);
    text-transform: uppercase;
}
.ex-badge.r-africa    { background: rgba(46,139,87,0.12);  color: #4caf80; border-color: rgba(46,139,87,0.30); }
.ex-badge.r-asia      { background: rgba(54,128,214,0.12); color: #6aa3e8; border-color: rgba(54,128,214,0.30); }
.ex-badge.r-europe    { background: rgba(214,161,54,0.12); color: #d6a136; border-color: rgba(214,161,54,0.30); }
.ex-badge.r-americas  { background: rgba(170,92,204,0.12); color: #b884d9; border-color: rgba(170,92,204,0.30); }
.ex-badge.r-me        { background: rgba(204,116,92,0.12); color: #d89077; border-color: rgba(204,116,92,0.30); }
.ex-badge.r-pacific   { background: rgba(92,170,204,0.12); color: #77b5d9; border-color: rgba(92,170,204,0.30); }
.news-title a {
    color: var(--text); text-decoration: none; font-size: 0.82rem;
    line-height: 1.4;
}
.news-title a:hover { color: var(--accent); }
.news-snippet {
    font-size: 0.76rem; color: var(--text-muted);
    margin-top: 0.3rem;
    display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden;
}
.news-meta {
    font-size: 0.7rem; color: var(--text-muted); margin-top: 0.3rem;
}

.lang-badge {
    display: inline-block; font-size: 0.65rem;
    padding: 0.1rem 0.4rem; border-radius: 4px;
    background: var(--blue-dim); color: var(--accent);
    font-weight: 600;
}

/* ── Earnings table ── */
.earnings-table {
    width: 100%; border-collapse: collapse;
    font-size: 0.8rem;
}
.earnings-table th {
    text-align: left; padding: 0.6rem 0.75rem;
    font-size: 0.7rem; text-transform: uppercase;
    letter-spacing: 0.06em; color: var(--text-muted);
    border-bottom: 1px solid var(--border);
    font-weight: 600;
}
.earnings-table td {
    padding: 0.6rem 0.75rem;
    border-bottom: 1px solid var(--border);
}
.earnings-table tr:hover td { background: var(--surface2); }

.badge {
    display: inline-block; padding: 0.15rem 0.55rem;
    border-radius: 999px; font-size: 0.7rem; font-weight: 700;
}
.badge-urgent { background: var(--red-dim); color: var(--red); }
.badge-soon   { background: var(--amber-dim); color: var(--amber); }
.badge-ok     { background: var(--green-dim); color: var(--green); }
.badge-past   { background: var(--surface2); color: var(--text-muted); }

/* ── Forum cards ── */
.forum-card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 0.75rem 1rem;
    margin-bottom: 0.5rem;
}
.forum-card:hover { border-color: var(--accent-dim); }
.forum-header {
    display: flex; justify-content: space-between; align-items: center;
    margin-bottom: 0.3rem;
}
.forum-stock {
    font-weight: 700; font-size: 0.8rem; color: var(--accent);
}
.forum-author {
    font-size: 0.72rem; color: var(--text-muted); font-style: italic;
}
.forum-text {
    font-size: 0.82rem; color: var(--text); line-height: 1.45;
    display: -webkit-box; -webkit-line-clamp: 6; -webkit-box-orient: vertical; overflow: hidden;
    margin: 0.3rem 0;
}
.forum-source {
    font-size: 0.68rem; color: var(--text-muted); margin-top: 0.3rem;
}
.forum-source a { color: var(--accent-dim); text-decoration: none; }
.forum-source a:hover { color: var(--accent); }

/* ── Stock filter pills (inside sections and the top global bar) ── */
.stock-filters {
    display: flex; gap: 0.4rem; flex-wrap: wrap;
    margin-bottom: 0.75rem;
}
/* Stock pills inside the top filter row — remove bottom margin and
 * let the parent .filter-row handle spacing. */
.filter-group.stocks .stock-filters,
.filter-group.stocks { margin-bottom: 0; }
.stock-pill {
    padding: 0.2rem 0.65rem; border-radius: 999px;
    font-size: 0.72rem; font-weight: 600;
    background: var(--surface2); color: var(--text-muted);
    border: 1px solid var(--border); cursor: pointer;
    transition: all 0.15s;
}
.stock-pill:hover { border-color: var(--accent-dim); color: var(--text); }
.stock-pill.active {
    background: var(--accent-dim); color: var(--accent); border-color: var(--accent-dim);
}
.stock-pill.hidden-pill { display: none; }

/* ── Stock selection: hide non-matching cards/rows ── */
.news-card.stock-hidden, .forum-card.stock-hidden, tr.stock-hidden { display: none; }
/* ── News age filter: hide items older than the active window ── */
.news-card.news-old { display: none; }
.news-extend-btn {
    display: inline-block; padding: 0.2rem 0.7rem; border-radius: 999px;
    font-size: 0.7rem; font-weight: 600; cursor: pointer;
    background: var(--surface2); color: var(--accent);
    border: 1px solid var(--accent); margin-left: 0.4rem;
    transition: all 0.15s;
}
.news-extend-btn:hover { background: var(--accent-dim); }
.news-extend-btn.active { background: var(--accent); color: #fff; }

/* ── Add Stock modal ── */
/* ── Toast notifications (top-right slide-in) ── */
#toast-container {
    position: fixed; top: 1rem; right: 1rem; z-index: 9999;
    display: flex; flex-direction: column; gap: 0.5rem;
    pointer-events: none;
    max-width: min(380px, calc(100vw - 2rem));
}
.toast {
    pointer-events: auto;
    background: var(--surface);
    border: 1px solid var(--border);
    border-left: 3px solid var(--text-muted);
    border-radius: 8px;
    padding: 0.75rem 1rem;
    font-size: 0.85rem;
    color: var(--text);
    box-shadow: 0 8px 24px rgba(0,0,0,0.35);
    display: flex; align-items: flex-start; gap: 0.6rem;
    animation: toast-in 0.22s ease-out;
}
.toast.toast-success { border-left-color: var(--green); }
.toast.toast-info    { border-left-color: var(--accent); }
.toast.toast-warning { border-left-color: #d6a136; }
.toast.toast-error   { border-left-color: var(--red); }
.toast.toast-out { animation: toast-out 0.18s ease-in forwards; }
.toast-icon { flex: 0 0 auto; font-size: 1rem; line-height: 1.2; }
.toast-body { flex: 1 1 auto; min-width: 0; word-wrap: break-word; }
.toast-close {
    flex: 0 0 auto; cursor: pointer; color: var(--text-muted);
    font-size: 1rem; line-height: 1; padding: 0 0.2rem;
}
.toast-close:hover { color: var(--text); }
@keyframes toast-in {
    from { transform: translateX(120%); opacity: 0; }
    to   { transform: translateX(0);    opacity: 1; }
}
@keyframes toast-out {
    from { transform: translateX(0);    opacity: 1; }
    to   { transform: translateX(120%); opacity: 0; }
}

/* ── Confirm dialog (themed replacement for native confirm) ── */
.confirm-overlay {
    position: fixed; inset: 0; background: rgba(0,0,0,0.6);
    z-index: 600; display: flex; align-items: center;
    justify-content: center; backdrop-filter: blur(4px);
}
.confirm-dialog {
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 12px; padding: 1.4rem 1.5rem;
    width: min(440px, 92vw);
    box-shadow: 0 20px 60px rgba(0,0,0,0.5);
}
.confirm-title {
    font-size: 1rem; font-weight: 700; color: var(--text);
    margin-bottom: 0.6rem;
}
.confirm-message {
    font-size: 0.85rem; color: var(--text-muted);
    line-height: 1.5; margin-bottom: 1.2rem;
}
.confirm-actions {
    display: flex; justify-content: flex-end; gap: 0.6rem;
}
.confirm-btn {
    padding: 0.5rem 1rem; border-radius: 8px; cursor: pointer;
    font-size: 0.82rem; font-weight: 600;
    border: 1px solid var(--border); background: var(--surface2);
    color: var(--text); transition: all 0.15s;
}
.confirm-btn:hover { border-color: var(--accent); }
.confirm-btn.confirm-btn-danger {
    background: rgba(220,70,70,0.14); color: #ff7b7b;
    border-color: rgba(220,70,70,0.40);
}
.confirm-btn.confirm-btn-danger:hover {
    background: rgba(220,70,70,0.22); border-color: #ff7b7b;
}

.add-stock-overlay {
    position: fixed; inset: 0; background: rgba(0,0,0,0.6);
    z-index: 500; display: flex; align-items: flex-start;
    justify-content: center; padding-top: 10vh;
    backdrop-filter: blur(4px);
}
.add-stock-card {
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 12px; padding: 1.5rem;
    width: min(640px, 92vw); box-shadow: 0 20px 60px rgba(0,0,0,0.5);
}
.add-stock-header {
    display: flex; justify-content: space-between; align-items: center;
    margin-bottom: 1rem;
}
.add-stock-close {
    cursor: pointer; font-size: 1.3rem; color: var(--text-muted);
    width: 28px; height: 28px; display: flex;
    align-items: center; justify-content: center;
    border-radius: 6px;
}
.add-stock-close:hover { background: var(--surface2); color: var(--text); }
#add-stock-search {
    width: 100%; padding: 0.7rem 1rem; font-size: 0.95rem;
    background: var(--bg); color: var(--text);
    border: 1px solid var(--border); border-radius: 8px;
    box-sizing: border-box;
}
#add-stock-search:focus { border-color: var(--accent); outline: none; }
.add-stock-results {
    margin-top: 0.8rem; max-height: 40vh; overflow-y: auto;
}
.add-stock-result {
    padding: 0.6rem 0.8rem; border-radius: 6px; cursor: pointer;
    border-bottom: 1px solid var(--border);
    display: flex; justify-content: space-between; align-items: center;
}
.add-stock-result:hover { background: var(--surface2); }
.add-stock-result-name { font-weight: 600; color: var(--text); font-size: 0.88rem; }
.add-stock-result-meta { font-size: 0.72rem; color: var(--text-muted); margin-top: 0.15rem; }
.add-stock-result-badge {
    font-size: 0.65rem; padding: 0.15rem 0.45rem; border-radius: 3px;
    background: var(--surface2); color: var(--text-muted);
    border: 1px solid var(--border); margin-left: 0.5rem;
}
/* Empty-state welcome for when watchlist is zero */
.welcome-card {
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 12px; padding: 2.5rem 2rem; text-align: center;
    margin: 2rem auto; max-width: 640px;
}
.welcome-card h2 { margin: 0 0 0.5rem; font-size: 1.3rem; }
.welcome-card p { color: var(--text-muted); font-size: 0.9rem; margin: 0.5rem 0 1.2rem; }
.welcome-card button {
    padding: 0.7rem 1.4rem; background: var(--accent); color: #fff;
    border: none; border-radius: 999px; cursor: pointer;
    font-weight: 700; font-size: 0.9rem;
}
.welcome-card button:hover { opacity: 0.9; }

/* ── Refresh button (floating, bottom-right) ── */
.refresh-bar {
    position: fixed; bottom: 1.5rem; right: 1.5rem;
    display: flex; align-items: center; gap: 0.6rem;
    z-index: 200;
}
.refresh-btn {
    padding: 0.6rem 1.2rem; border-radius: 999px;
    font-size: 0.82rem; font-weight: 700;
    background: var(--accent); color: #fff;
    border: none; cursor: pointer;
    box-shadow: 0 4px 16px rgba(0,0,0,0.4);
    transition: background 0.15s, transform 0.1s;
    display: flex; align-items: center; gap: 0.4rem;
}
.refresh-btn:hover { background: #5a7ae6; transform: translateY(-1px); }
.refresh-btn:active { transform: translateY(0); }
.refresh-btn.busy {
    background: var(--surface2); color: var(--text-muted);
    cursor: wait; pointer-events: none;
}
.refresh-btn .spinner {
    display: none; width: 14px; height: 14px;
    border: 2px solid var(--text-muted); border-top-color: #fff;
    border-radius: 50%; animation: spin 0.8s linear infinite;
}
.refresh-btn.busy .spinner { display: inline-block; }
.refresh-btn-free { background: #2a8a5f; }
.refresh-btn-free:hover:not(:disabled) { background: #2f9e6c; }
.refresh-btn-full { background: #c96a2d; }
.refresh-btn-full:hover:not(:disabled) { background: #de7632; }
.refresh-btn:disabled {
    background: var(--surface2); color: var(--text-muted);
    cursor: not-allowed; box-shadow: none; opacity: 0.7;
}
.refresh-btn:disabled:hover { transform: none; }
@keyframes spin { to { transform: rotate(360deg); } }
.refresh-status {
    font-size: 0.7rem; color: var(--text-muted);
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 6px; padding: 0.3rem 0.6rem;
    white-space: nowrap;
}
.refresh-progress {
    display: none;
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 8px; padding: 0.5rem 0.8rem;
    min-width: 260px;
}
.refresh-progress.visible { display: block; }
.refresh-progress-bar {
    height: 4px; background: var(--surface2); border-radius: 2px;
    overflow: hidden; margin-bottom: 0.4rem;
}
.refresh-progress-fill {
    height: 100%; background: var(--accent); border-radius: 2px;
    transition: width 0.3s ease;
    width: 0%;
}
.refresh-progress-text {
    font-size: 0.7rem; color: var(--text-muted);
    display: flex; justify-content: space-between;
}
.refresh-progress-step {
    color: var(--text); font-weight: 600;
}
.refresh-progress-error {
    font-size: 0.7rem; color: var(--red); margin-top: 0.3rem;
}

/* ── Empty state ── */
.empty {
    text-align: center; padding: 0.8rem 1rem;
    color: var(--text-muted); font-size: 0.8rem;
    background: var(--surface); border-radius: 8px;
    border: 1px dashed var(--border);
}

/* ── Responsive ── */
@media (max-width: 900px) {
    .header-inner { flex-direction: column; align-items: flex-start; }
}

/* ── Mobile compact mode ── */
@media (max-width: 600px) {
    body { font-size: 13px; }
    .header { padding: 0.6rem 0.8rem; }
    .header h1 { font-size: 1rem; }
    .header-logo { height: 28px; }
    .header h1 { font-size: 0.95rem; }
    .header-kpis { font-size: 0.7rem; gap: 0 0.55rem; width: 100%; margin-left: 0; }
    .header-kpis .kpi-val { font-size: 0.8rem; }
    .filters, .filter-row { padding: 0 0.8rem; margin-top: 0.5rem; }
    .filter-group.stocks { border-left: none; padding-left: 0; }
    .filter-pill { padding: 0.2rem 0.6rem; font-size: 0.68rem; }
    .container { padding: 0.5rem 0.8rem 2rem; gap: 1rem; }
    .section-title { font-size: 0.85rem; margin-bottom: 0.5rem; }
    .stock-filters { gap: 0.3rem; margin-bottom: 0.5rem; }
    .stock-pill { padding: 0.15rem 0.5rem; font-size: 0.65rem; }

    /* Stock chips */
    .stock-panel-inner { gap: 0.4rem; padding: 0.4rem 0; }
    .stock-chip { padding: 0.4rem 0.6rem; min-width: 140px; }
    .stock-chip-name { font-size: 0.7rem; }
    .stock-chip-ticker { font-size: 0.6rem; }
    .stock-chip-price { font-size: 0.85rem; }
    .exchange-status { padding: 0 0.8rem 0.3rem; font-size: 0.68rem; }

    /* Alert cards */
    .alert-card { padding: 0.6rem 0.7rem; margin-bottom: 0.4rem; }
    .alert-stock { font-size: 0.75rem; }
    .alert-title a { font-size: 0.72rem; }
    .alert-date { font-size: 0.6rem; }

    /* News cards */
    .news-card { padding: 0.5rem 0.7rem; margin-bottom: 0.35rem; }
    .news-stock { font-size: 0.7rem; }
    .news-title a { font-size: 0.72rem; }
    .news-snippet { font-size: 0.66rem; -webkit-line-clamp: 1; }
    .news-meta { font-size: 0.6rem; }

    /* Earnings table */
    .earnings-table { font-size: 0.68rem; }
    .earnings-table th { padding: 0.4rem; font-size: 0.6rem; }
    .earnings-table td { padding: 0.4rem; }
    .badge { font-size: 0.6rem; padding: 0.1rem 0.4rem; }

    /* Forum cards */
    .forum-card { padding: 0.5rem 0.7rem; margin-bottom: 0.35rem; }
    .forum-stock { font-size: 0.7rem; }
    .forum-text { font-size: 0.68rem; -webkit-line-clamp: 2; }
    .forum-author { font-size: 0.62rem; }
    .forum-source { font-size: 0.6rem; }

    /* Exchange headers */
    .exchange-header { font-size: 0.72rem; padding: 0.3rem 0; }

    /* Refresh bar */
    .refresh-bar { bottom: 0.8rem; right: 0.8rem; }
    .refresh-btn { font-size: 0.7rem; padding: 0.4rem 0.8rem; }
    .refresh-status { font-size: 0.6rem; }
    .price-refresh-btn { font-size: 0.6rem; padding: 0.15rem 0.45rem; }

    /* Gen time */
    .gen-time { font-size: 0.6rem; }
}

/* ── Timestamp ── */
.gen-time {
    text-align: center; padding: 0.25rem;
    font-size: 0.7rem; color: var(--text-muted);
}
"""

# ---------------------------------------------------------------------------
# JavaScript — collapsible sections + exchange filter
# ---------------------------------------------------------------------------

JS = """
// Toggle collapsible exchange sections
document.querySelectorAll('.exchange-header').forEach(h => {
    h.addEventListener('click', () => {
        h.classList.toggle('collapsed');
        h.nextElementSibling.classList.toggle('hidden');
    });
});

// ── News age filtering ──
// Default: hide news older than 3 months (90 days).
// When a single stock is selected, show a "📅 Show 10y" toggle that
// extends the window for that one stock to 10 years.
const NEWS_DEFAULT_WINDOW_S = 90 * 24 * 3600;          // 3 months
const NEWS_EXTENDED_WINDOW_S = 10 * 365 * 24 * 3600;   // 10 years
let newsExtendedMode = false;

// Track whether the user has explicitly clicked "Show more" on a section.
// If they did, we keep the section expanded even after filter changes
// clear and re-collapse normally wouldn't apply.
// Per-section show-more counter. Instead of a boolean "expand
// everything", each click on "Show more" grants +_SECTION_VISIBLE_LIMIT
// more items — a progressive reveal that scales to huge feeds without
// dumping 300 cards at once.
const _sectionShowCount = { news: 0, forum: 0 };

function expandSection(btn, sectionSelector) {
    const key = sectionSelector.indexOf('news') >= 0 ? 'news'
              : sectionSelector.indexOf('forum') >= 0 ? 'forum' : null;
    if (key !== null) _sectionShowCount[key] += 1;
    _applyCollapsedState();
}

// Any filter active = at least one non-ALL exchange pill OR one stock pill
function _filtersActive() {
    if (typeof activeTickers !== 'undefined' && activeTickers.size > 0) return true;
    const allPill = document.querySelector('.filter-pill[data-exchange="ALL"]');
    if (allPill && !allPill.classList.contains('active')) return true;
    return false;
}

// Reconcile the collapsed state of News/Forum with the current filter.
// Always show at most 10 VISIBLE items per section — items hidden by
// the exchange filter don't count toward the limit. The "Show more"
// button appears with the count of remaining hidden-but-matching items.
const _SECTION_VISIBLE_LIMIT = 10;

function _applyCollapsedState() {
    [
        { sel: '#news-section',  key: 'news',  btn: 'news-show-more',  card: '.news-card' },
        { sel: '#forum-section', key: 'forum', btn: 'forum-show-more', card: '.forum-card' },
    ].forEach(({ sel, key, btn: btnId, card: cardSel }) => {
        // Total limit = initial 10 + (_SECTION_VISIBLE_LIMIT * clicks).
        // Each "Show more" click reveals 10 more items.
        const extraClicks = _sectionShowCount[key] || 0;
        const limit = _SECTION_VISIBLE_LIMIT * (1 + extraClicks);
        const cards = document.querySelectorAll(sel + ' ' + cardSel);
        let visibleCount = 0;
        let hiddenByCollapse = 0;

        cards.forEach(el => {
            // Is this card hidden by the exchange/stock filter?
            const filteredOut = el.style.display === 'none'
                || el.classList.contains('stock-hidden');
            if (filteredOut) {
                return;
            }
            visibleCount++;
            if (visibleCount > limit) {
                el.classList.add('collapsed-hidden');
                hiddenByCollapse++;
            } else {
                el.classList.remove('collapsed-hidden');
            }
        });

        const btn = document.getElementById(btnId);
        if (btn) {
            if (hiddenByCollapse > 0) {
                btn.style.display = '';
                // Reveal in chunks: next click will show the next 10
                // (or all remaining if fewer). Tell the user what they'll get.
                const nextChunk = Math.min(_SECTION_VISIBLE_LIMIT, hiddenByCollapse);
                btn.textContent = nextChunk === hiddenByCollapse
                    ? '\u25BC Show ' + hiddenByCollapse + ' more'
                    : '\u25BC Show ' + nextChunk + ' more (of '
                      + hiddenByCollapse + ')';
            } else {
                btn.style.display = 'none';
            }
        }
    });
}

// Minimum number of news items we want visible before giving up on
// the preferred age window. If the strict window yields fewer than
// this, we fall back to "newest N overall" so the section never
// looks empty when data exists.
const NEWS_MIN_VISIBLE = 10;

function applyNewsAgeFilter() {
    const newsSection = document.getElementById('news-section');
    if (!newsSection) return;
    const allCards = [...newsSection.querySelectorAll('.news-card')];
    const nowSec = Math.floor(Date.now() / 1000);

    const singleStock = (typeof activeTickers !== 'undefined' && activeTickers.size === 1)
        ? [...activeTickers][0]
        : null;
    const anyFilter = (typeof _filtersActive === 'function') && _filtersActive();

    // Reset all cards — we'll reapply the age filter below.
    allCards.forEach(c => c.classList.remove('news-old'));

    // When any filter is active, show everything in-scope. No age filter.
    if (anyFilter) {
        _updateNewsSubtitle(singleStock, anyFilter, 'filter');
        return;
    }

    // Candidates = cards not hidden by other filters (we only have
    // the age filter active here; stock-hidden / filtered-out aren't
    // in play when anyFilter is false).
    const candidates = allCards;

    // First pass: apply the strict 3-month window (or 10y if extended).
    const pref = singleStock && newsExtendedMode
        ? NEWS_EXTENDED_WINDOW_S
        : NEWS_DEFAULT_WINDOW_S;
    let visibleCount = 0;
    candidates.forEach(c => {
        const epoch = parseInt(c.dataset.pubEpoch || '0', 10);
        if (epoch === 0) { visibleCount++; return; }  // no-date items always show
        const ageS = nowSec - epoch;
        if (ageS > pref) c.classList.add('news-old');
        else visibleCount++;
    });

    // Second pass: if the strict window left too few items, progressively
    // un-hide the newest ones (by pub date) until we have ≥ NEWS_MIN_VISIBLE
    // or we run out. This ensures the section never looks empty when
    // the DB has news — it just shows older items with a note.
    let relaxed = false;
    if (visibleCount < NEWS_MIN_VISIBLE) {
        const hidden = candidates
            .filter(c => c.classList.contains('news-old'))
            .map(c => ({ c, epoch: parseInt(c.dataset.pubEpoch || '0', 10) }))
            .filter(x => x.epoch > 0)
            .sort((a, b) => b.epoch - a.epoch);   // newest first
        for (const x of hidden) {
            if (visibleCount >= NEWS_MIN_VISIBLE) break;
            x.c.classList.remove('news-old');
            visibleCount++;
            relaxed = true;
        }
    }

    _updateNewsSubtitle(singleStock, false, relaxed ? 'relaxed' : 'window');
}

function _updateNewsSubtitle(singleStock, anyFilter, mode) {
    const toggleBtn = document.getElementById('news-extend-toggle');
    if (toggleBtn) {
        toggleBtn.style.display = singleStock ? 'inline-block' : 'none';
        toggleBtn.textContent = newsExtendedMode ? '📅 Last 3 months' : '📅 Show 10y';
        toggleBtn.classList.toggle('active', newsExtendedMode);
    }
    const subtitle = document.getElementById('news-subtitle');
    if (!subtitle) return;
    if (singleStock && newsExtendedMode) {
        subtitle.textContent = '(last 10 years)';
    } else if (anyFilter) {
        subtitle.textContent = '(all dates for current filter)';
    } else if (mode === 'relaxed') {
        subtitle.textContent = '(newest items — most are older than 3 months)';
    } else {
        subtitle.textContent = '(last 3 months — select a stock to see older items)';
    }
}

function toggleNewsExtended() {
    newsExtendedMode = !newsExtendedMode;
    applyNewsAgeFilter();
}

// ── Global stock-level filter ──
// Selecting a stock anywhere on the page filters every section that
// supports stock-level filtering (news, earnings, forum, insiders, etc).
// activeTickers is a Set; empty means "show all".
const activeTickers = new Set();

// ── Selected-stock sticky chip: shows above the freeze pane whenever
// exactly one stock is selected, so the user knows what they're
// viewing while scrolling through news/earnings/forums. ──
function _renderSelectedStockChip() {
    const wrap = document.getElementById('selected-stock-chip');
    if (!wrap) return;
    if (activeTickers.size !== 1) {
        wrap.style.display = 'none';
        wrap.innerHTML = '';
        return;
    }
    const tk = [...activeTickers][0];
    const chip = document.querySelector('.stock-chip[data-ticker="' + tk + '"]');
    if (!chip) { wrap.style.display = 'none'; return; }
    const name = (chip.getAttribute('title') || tk).replace(/"/g, '&quot;');
    const priceEl = chip.querySelector('.stock-chip-price');
    const changeEl = chip.querySelector('.stock-chip-change');
    let priceHtml = '';
    if (priceEl) {
        // Clone just the price text (strip the nested change pill)
        const clone = priceEl.cloneNode(true);
        const pill = clone.querySelector('.stock-chip-change');
        if (pill) pill.remove();
        priceHtml = '<span class="ssc-price">' + clone.textContent.trim() + '</span>';
    }
    let changeHtml = '';
    if (changeEl) {
        const cls = changeEl.classList.contains('up') ? 'up'
                  : changeEl.classList.contains('down') ? 'down' : 'flat';
        changeHtml = '<span class="ssc-change ' + cls + '">' + changeEl.textContent.trim() + '</span>';
    }
    wrap.innerHTML =
        '<span class="ssc-ticker">' + tk + '</span>' +
        '<span class="ssc-name" title="' + name + '">' + name + '</span>' +
        priceHtml +
        changeHtml +
        '<span class="ssc-clear" title="Clear selection" onclick="clearStockSelection()">×</span>';
    wrap.style.display = 'inline-flex';
}
function clearStockSelection() {
    activeTickers.clear();
    applyGlobalStockFilter();
    document.querySelectorAll('.stock-chip[data-ticker]').forEach(c => c.classList.remove('chip-active'));
    _renderSelectedStockChip();
}

// ── Click a stock chip to toggle filter on that ticker ──
//   Click        → replace selection with this ticker (or clear if same)
//   Shift/Cmd/Ctrl+click → toggle additive
//   Click ✕      → remove from watchlist (existing behavior)
document.addEventListener('click', (e) => {
    if (e.target.closest('.stock-chip-remove')) return;
    const chip = e.target.closest('.stock-chip[data-ticker]');
    if (!chip) return;
    const tk = chip.dataset.ticker;
    if (!tk) return;
    const additive = e.shiftKey || e.metaKey || e.ctrlKey;
    if (additive) {
        if (activeTickers.has(tk)) activeTickers.delete(tk);
        else activeTickers.add(tk);
    } else {
        if (activeTickers.size === 1 && activeTickers.has(tk)) {
            activeTickers.clear();
        } else {
            activeTickers.clear();
            activeTickers.add(tk);
        }
    }
    applyGlobalStockFilter();
    document.querySelectorAll('.stock-chip[data-ticker]').forEach(c => {
        c.classList.toggle('chip-active', activeTickers.has(c.dataset.ticker));
    });
    _renderSelectedStockChip();
});

function applyGlobalStockFilter() {
    // Sync the top ticker bar with activeTickers
    document.querySelectorAll('.filter-group.stocks .stock-pill').forEach(p => {
        const tk = p.dataset.ticker;
        if (tk === 'ALL') {
            p.classList.toggle('active', activeTickers.size === 0);
        } else {
            p.classList.toggle('active', activeTickers.has(tk));
        }
    });

    // Hide/show all filterable cards/rows across every section
    document.querySelectorAll('.section [data-ticker]').forEach(card => {
        const tk = card.dataset.ticker;
        if (activeTickers.size === 0) {
            card.classList.remove('stock-hidden');
        } else {
            card.classList.toggle('stock-hidden', !activeTickers.has(tk));
        }
    });

    // Collapsed sections need to expand when filters are active —
    // otherwise a filter could match only hidden cards.
    _applyCollapsedState();

    // News age filter depends on filter state — re-apply first so
    // _updateEmptyGroups and _updateSectionCounts see the final state
    applyNewsAgeFilter();

    // Forum / insider groups whose children are all filtered out
    _updateEmptyGroups();

    // Section count pills reflect currently visible items only
    _updateSectionCounts();
}

// Recompute and update each section's count pill based on what is
// currently VISIBLE after filters, collapse, and age-filter classes
// have been applied. Call last in the filter pipeline so classes are
// already settled.
function _isRowVisible(el) {
    if (el.style.display === 'none') return false;
    if (el.classList.contains('stock-hidden')) return false;
    if (el.classList.contains('news-old')) return false;
    const group = el.closest('.exchange-group');
    if (group && group.style.display === 'none') return false;
    return true;
}

function _updateSectionCounts() {
    const sections = [
        { sel: '#news-section',     item: '.news-card' },
        { sel: '#forum-section',    item: '.forum-card' },
        { sel: '#insider-section',  item: '.news-card' },
        { sel: '#earnings-section', item: 'tr[data-ticker]' },
        { sel: '#alerts-section',   item: '.alert-card' },
    ];
    sections.forEach(({ sel, item }) => {
        const section = document.querySelector(sel);
        if (!section) return;
        let n = 0;
        section.querySelectorAll(item).forEach(el => {
            if (_isRowVisible(el)) n++;
        });
        const pill = section.querySelector('.section-count');
        if (pill) pill.textContent = n;
    });

    // Update earnings tab counts (Upcoming / Past Reports)
    const upDiv = document.getElementById('earnings-upcoming');
    const pastDiv = document.getElementById('earnings-past');
    const tabUp = document.getElementById('earnings-upcoming-tab');
    const tabPast = document.getElementById('earnings-past-tab');
    if (upDiv && tabUp) {
        let n = 0;
        upDiv.querySelectorAll('tr[data-ticker]').forEach(r => { if (_isRowVisible(r)) n++; });
        tabUp.textContent = '📅 Upcoming (' + n + ')';
    }
    if (pastDiv && tabPast) {
        let n = 0;
        pastDiv.querySelectorAll('tr[data-ticker]').forEach(r => { if (_isRowVisible(r)) n++; });
        tabPast.textContent = '📋 Past Reports (' + n + ')';
    }
}

// Hide exchange-group containers whose children are all filtered out.
// Needed especially for Forum: groups there are labelled by source
// name (richbourse / i3investor / twitter), not exchange code, so the
// exchange-filter inline display:none doesn't reach the wrapping group
// even when every card inside disappears.
function _updateEmptyGroups() {
    document.querySelectorAll('.section .exchange-group').forEach(g => {
        const cards = g.querySelectorAll('[data-ticker]');
        if (cards.length === 0) { g.style.display = ''; return; }
        let anyVisible = false;
        cards.forEach(c => {
            if (c.style.display === 'none') return;          // exchange filter
            if (c.classList.contains('stock-hidden')) return; // stock filter
            anyVisible = true;
        });
        g.style.display = anyVisible ? '' : 'none';
    });
}

function setActiveTicker(ticker, additive) {
    if (ticker === 'ALL') {
        activeTickers.clear();
    } else if (additive) {
        // Multi-select with cmd/ctrl-click — toggle this one
        if (activeTickers.has(ticker)) {
            activeTickers.delete(ticker);
        } else {
            activeTickers.add(ticker);
        }
    } else {
        // Single-select: clicking the same active ticker clears the filter
        if (activeTickers.size === 1 && activeTickers.has(ticker)) {
            activeTickers.clear();
        } else {
            activeTickers.clear();
            activeTickers.add(ticker);
        }
    }
    // Reset news extended mode when changing selection
    newsExtendedMode = false;
    applyGlobalStockFilter();
}

document.querySelectorAll('.stock-pill').forEach(pill => {
    pill.addEventListener('click', (e) => {
        const ticker = pill.dataset.ticker;
        if (!ticker) return;
        // Cmd-click (Mac) or Ctrl-click for multi-select
        const additive = e.metaKey || e.ctrlKey;
        setActiveTicker(ticker, additive);
    });
});

// Apply news age filter on page load
applyNewsAgeFilter();
// Recompute section counts after the age filter has hidden old items
_updateSectionCounts();

// ── Add Stock modal ──
let addStockSearchTimer = null;

function openAddStockModal() {
    const m = document.getElementById('add-stock-modal');
    if (!m) return;
    m.style.display = 'flex';
    setTimeout(() => {
        const s = document.getElementById('add-stock-search');
        if (s) s.focus();
    }, 50);
}

function closeAddStockModal() {
    const m = document.getElementById('add-stock-modal');
    if (!m) return;
    m.style.display = 'none';
    const s = document.getElementById('add-stock-search');
    if (s) s.value = '';
    const r = document.getElementById('add-stock-results');
    if (r) r.innerHTML = '';
}

function onAddStockSearch(query) {
    if (addStockSearchTimer) clearTimeout(addStockSearchTimer);
    const results = document.getElementById('add-stock-results');
    if (query.trim().length < 2) {
        results.innerHTML = '';
        return;
    }
    addStockSearchTimer = setTimeout(() => {
        fetch('/api/stock-search?q=' + encodeURIComponent(query))
            .then(r => r.json())
            .then(data => {
                renderAddStockResults(data.results || []);
            })
            .catch(err => {
                results.innerHTML = '<div class="muted" style="padding:0.5rem">Search failed: ' + err + '</div>';
            });
    }, 300);
}

function renderAddStockResults(results) {
    const container = document.getElementById('add-stock-results');
    if (!results.length) {
        container.innerHTML = '<div class="muted" style="padding:0.5rem">No matches. Try a longer or more specific search term.</div>';
        return;
    }
    // True-frontier exchanges (illiquid, Yahoo-indexed poorly). Everything
    // else from the catalog is a small/mid-cap on a developed or emerging
    // exchange — no FRONTIER badge.
    const FRONTIER_EX = new Set([
        'UZSE','KSE','KASE','BRVM','NGX','NSEK','GSE','BWSE','LUSE',
        'DSET','DSEB','CSEL','BVMT','CSEM','USE','RSE','SEM','ISX','ESX',
        'BELEX','PNGX','UX','PSX'
    ]);
    let html = '';
    for (const r of results) {
        let source_badge = '';
        if (r.source === 'catalog' && FRONTIER_EX.has((r.exchange || '').toUpperCase())) {
            source_badge = '<span class="add-stock-result-badge" style="color:var(--green);border-color:var(--green)">FRONTIER</span>';
        }
        const data = JSON.stringify(r).replace(/"/g, '&quot;');
        html += `<div class="add-stock-result" data-stock="${data}" onclick="addStockFromResult(this)">
            <div>
                <div class="add-stock-result-name">${escapeHtml(r.name)}</div>
                <div class="add-stock-result-meta">${escapeHtml(r.ticker)} · ${escapeHtml(r.exchDisp || r.exchange)} · ${escapeHtml(r.currency)}</div>
            </div>
            <div>${source_badge}</div>
        </div>`;
    }
    container.innerHTML = html;
}

function escapeHtml(s) {
    const div = document.createElement('div');
    div.textContent = s || '';
    return div.innerHTML;
}

function addStockFromResult(el) {
    try {
        const data = JSON.parse(el.dataset.stock.replace(/&quot;/g, '"'));
        postAddStock(data);
    } catch (e) {
        showToast('Failed to parse result: ' + e, 'error');
    }
}

// ── In-site toast + confirm helpers (themed replacements for alert/confirm) ──
function showToast(message, type) {
    type = type || 'info';
    let container = document.getElementById('toast-container');
    if (!container) {
        container = document.createElement('div');
        container.id = 'toast-container';
        document.body.appendChild(container);
    }
    const icons = { success: '✓', info: 'ℹ', warning: '⚠', error: '✕' };
    const toast = document.createElement('div');
    toast.className = 'toast toast-' + type;
    toast.innerHTML = '<span class="toast-icon">' + (icons[type] || 'ℹ') + '</span>' +
                      '<span class="toast-body"></span>' +
                      '<span class="toast-close">×</span>';
    toast.querySelector('.toast-body').textContent = message;
    const dismiss = () => {
        toast.classList.add('toast-out');
        setTimeout(() => toast.remove(), 200);
    };
    toast.querySelector('.toast-close').addEventListener('click', dismiss);
    container.appendChild(toast);
    setTimeout(dismiss, 4500);
}

function showConfirm(title, message, opts) {
    opts = opts || {};
    return new Promise(resolve => {
        const overlay = document.createElement('div');
        overlay.className = 'confirm-overlay';
        overlay.innerHTML =
            '<div class="confirm-dialog">' +
                '<div class="confirm-title"></div>' +
                '<div class="confirm-message"></div>' +
                '<div class="confirm-actions">' +
                    '<button class="confirm-btn" data-role="cancel"></button>' +
                    '<button class="confirm-btn confirm-btn-danger" data-role="ok"></button>' +
                '</div>' +
            '</div>';
        overlay.querySelector('.confirm-title').textContent = title;
        overlay.querySelector('.confirm-message').textContent = message;
        overlay.querySelector('[data-role="cancel"]').textContent = opts.cancelLabel || 'Cancel';
        overlay.querySelector('[data-role="ok"]').textContent = opts.okLabel || 'Confirm';
        const close = (ok) => { overlay.remove(); resolve(ok); };
        overlay.querySelector('[data-role="cancel"]').addEventListener('click', () => close(false));
        overlay.querySelector('[data-role="ok"]').addEventListener('click', () => close(true));
        overlay.addEventListener('click', (e) => { if (e.target === overlay) close(false); });
        document.body.appendChild(overlay);
        overlay.querySelector('[data-role="ok"]').focus();
    });
}

function _preserveFilterHashForReload() {
    const actives = [...document.querySelectorAll('.filter-pill.active:not([data-exchange="ALL"])')]
        .map(p => p.dataset.exchange);
    if (actives.length) {
        window.location.hash = 'ex=' + actives.join(',');
    }
}

function postAddStock(data) {
    fetch('/api/watchlist/add', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(data),
    })
    .then(r => r.json())
    .then(resp => {
        if (resp.status === 'ok') {
            if (resp.added === false) {
                // Stock already on the watchlist — no reload needed.
                showToast(
                    (data.ticker || '') + ' is already on your watchlist',
                    'info'
                );
                closeAddStockModal();
                return;
            }
            closeAddStockModal();
            showToast('Added ' + (data.name || data.ticker) + ' to watchlist', 'success');
            _preserveFilterHashForReload();
            // Brief delay so the toast is visible before the reload
            setTimeout(() => location.reload(), 500);
        } else {
            showToast(resp.message || 'Failed to add stock', 'error');
        }
    })
    .catch(err => showToast('Network error: ' + err, 'error'));
}

// ── Remove a stock from the watchlist (called from the chip ✕ button) ──
function removeStockFromWatchlist(ticker, exchange, name) {
    showConfirm(
        'Remove from watchlist?',
        'Remove ' + (name || ticker) + ' from your watchlist. Existing portfolio ' +
        'transactions will not be deleted, but this stock will no longer appear ' +
        'on the monitor unless you re-add it.',
        { okLabel: 'Remove', cancelLabel: 'Keep' }
    ).then(ok => {
        if (!ok) return;
        fetch('/api/watchlist/remove', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ ticker: ticker, exchange: exchange }),
        })
        .then(r => r.json())
        .then(resp => {
            if (resp.status === 'ok') {
                showToast('Removed ' + (name || ticker), 'success');
                _preserveFilterHashForReload();
                setTimeout(() => location.reload(), 500);
            } else {
                showToast(resp.message || 'Failed to remove', 'error');
            }
        })
        .catch(err => showToast('Network error: ' + err, 'error'));
    });
}

// ── Exchange trading hours (IANA timezone, open/close in local exchange time) ──
// Keys are the user-facing display names that match data-exchange attributes
// on stock panels and filter pills. 'US' covers NASDAQ + NYSE + AMEX.
const EXCHANGE_HOURS = {
    'Malaysia':         { tz: 'Asia/Kuala_Lumpur',   open: '09:00', close: '17:00', days: [1,2,3,4,5], name: 'Bursa Malaysia' },
    'Nigeria':          { tz: 'Africa/Lagos',        open: '09:30', close: '14:30', days: [1,2,3,4,5], name: 'Nigerian Exchange' },
    'Ivory Coast/BRVM':      { tz: 'Africa/Abidjan',      open: '09:00', close: '15:30', days: [1,2,3,4,5], name: "BRVM (8-country West African regional exchange)" },
    'Uzbekistan':       { tz: 'Asia/Tashkent',       open: '10:00', close: '15:00', days: [1,2,3,4,5], name: 'Tashkent Stock Exchange' },
    'Singapore':        { tz: 'Asia/Singapore',      open: '09:00', close: '17:00', days: [1,2,3,4,5], name: 'Singapore Exchange' },
    'Kyrgyzstan':       { tz: 'Asia/Bishkek',        open: '10:00', close: '15:00', days: [1,2,3,4,5], name: 'Kyrgyz Stock Exchange' },
    'Kazakhstan':       { tz: 'Asia/Almaty',         open: '11:30', close: '17:00', days: [1,2,3,4,5], name: 'Kazakhstan Stock Exchange (KASE)' },
    'Kenya':            { tz: 'Africa/Nairobi',      open: '09:00', close: '15:00', days: [1,2,3,4,5], name: 'Nairobi Securities Exchange (NSE)' },
    'Ghana':            { tz: 'Africa/Accra',        open: '10:00', close: '15:00', days: [1,2,3,4,5], name: 'Ghana Stock Exchange (GSE)' },
    'Botswana':         { tz: 'Africa/Gaborone',     open: '09:30', close: '15:00', days: [1,2,3,4,5], name: 'Botswana Stock Exchange (BSE)' },
    'Zambia':           { tz: 'Africa/Lusaka',       open: '10:00', close: '15:45', days: [1,2,3,4,5], name: 'Lusaka Securities Exchange (LuSE)' },
    'Tanzania':         { tz: 'Africa/Dar_es_Salaam',open: '10:00', close: '15:00', days: [1,2,3,4,5], name: 'Dar es Salaam Stock Exchange (DSE)' },
    'Bangladesh':       { tz: 'Asia/Dhaka',          open: '10:00', close: '14:30', days: [0,1,2,3,4], name: 'Dhaka Stock Exchange (DSE)' },
    'Pakistan':         { tz: 'Asia/Karachi',        open: '09:30', close: '15:30', days: [1,2,3,4,5], name: 'Pakistan Stock Exchange (PSX)' },
    'Morocco':          { tz: 'Africa/Casablanca',   open: '09:30', close: '15:30', days: [1,2,3,4,5], name: 'Casablanca Stock Exchange' },
    'Croatia':          { tz: 'Europe/Zagreb',       open: '09:00', close: '16:00', days: [1,2,3,4,5], name: 'Zagreb Stock Exchange (ZSE)' },
    'Serbia':           { tz: 'Europe/Belgrade',     open: '09:30', close: '14:00', days: [1,2,3,4,5], name: 'Belgrade Stock Exchange (BELEX)' },
    'Slovakia':         { tz: 'Europe/Bratislava',   open: '09:30', close: '16:00', days: [1,2,3,4,5], name: 'Bratislava Stock Exchange (BSSE)' },
    'Papua New Guinea': { tz: 'Pacific/Port_Moresby',open: '10:00', close: '12:00', days: [1,2,3,4,5], name: 'Port Moresby Stock Exchange (PNGX)' },
    'Tunisia':          { tz: 'Africa/Tunis',        open: '09:00', close: '14:10', days: [1,2,3,4,5], name: 'Bourse de Tunis (BVMT)' },
    'Sri Lanka':        { tz: 'Asia/Colombo',        open: '09:30', close: '14:30', days: [1,2,3,4,5], name: 'Colombo Stock Exchange (CSE)' },
    'Ukraine':          { tz: 'Europe/Kyiv',         open: '10:00', close: '17:30', days: [1,2,3,4,5], name: 'Ukrainian Exchange (UX)' },
    'Uganda':           { tz: 'Africa/Kampala',      open: '10:00', close: '12:00', days: [1,2,3,4,5], name: 'Uganda Securities Exchange (USE)' },
    'Rwanda':           { tz: 'Africa/Kigali',       open: '09:00', close: '12:00', days: [1,2,3,4,5], name: 'Rwanda Stock Exchange (RSE)' },
    'Mauritius':        { tz: 'Indian/Mauritius',    open: '09:00', close: '13:30', days: [1,2,3,4,5], name: 'Stock Exchange of Mauritius (SEM)' },
    'Iraq':             { tz: 'Asia/Baghdad',        open: '09:30', close: '12:00', days: [0,1,2,3,4], name: 'Iraq Stock Exchange (ISX)' },
    'Ethiopia':         { tz: 'Africa/Addis_Ababa',  open: '09:00', close: '15:00', days: [1,2,3,4,5], name: 'Ethiopian Securities Exchange (ESX)' },
    'South Korea':      { tz: 'Asia/Seoul',          open: '09:00', close: '15:30', days: [1,2,3,4,5], name: 'Korea Exchange (KRX)' },
    'Taiwan':           { tz: 'Asia/Taipei',         open: '09:00', close: '13:30', days: [1,2,3,4,5], name: 'Taiwan Stock Exchange (TWSE)' },
    'Indonesia':        { tz: 'Asia/Jakarta',        open: '09:00', close: '16:15', days: [1,2,3,4,5], name: 'Indonesia Stock Exchange (IDX)' },
    'Thailand':         { tz: 'Asia/Bangkok',        open: '10:00', close: '16:30', days: [1,2,3,4,5], name: 'Stock Exchange of Thailand (SET)' },
    'Philippines':      { tz: 'Asia/Manila',         open: '09:30', close: '15:30', days: [1,2,3,4,5], name: 'Philippine Stock Exchange (PSE)' },
    'Vietnam':          { tz: 'Asia/Ho_Chi_Minh',    open: '09:00', close: '15:00', days: [1,2,3,4,5], name: 'Ho Chi Minh Stock Exchange (HOSE)' },
    'Israel':           { tz: 'Asia/Jerusalem',      open: '09:59', close: '17:14', days: [0,1,2,3,4], name: 'Tel Aviv Stock Exchange (TASE)' },
    'Saudi Arabia':     { tz: 'Asia/Riyadh',         open: '10:00', close: '15:00', days: [0,1,2,3,4], name: 'Saudi Stock Exchange (Tadawul)' },
    'UAE (Dubai)':      { tz: 'Asia/Dubai',          open: '10:00', close: '14:00', days: [1,2,3,4,5], name: 'Dubai Financial Market (DFM)' },
    'UAE (Abu Dhabi)':  { tz: 'Asia/Dubai',          open: '10:00', close: '14:00', days: [1,2,3,4,5], name: 'Abu Dhabi Securities Exchange (ADX)' },
    'Qatar':            { tz: 'Asia/Qatar',          open: '09:30', close: '13:15', days: [0,1,2,3,4], name: 'Qatar Stock Exchange (QSE)' },
    'Turkey':           { tz: 'Europe/Istanbul',     open: '10:00', close: '18:00', days: [1,2,3,4,5], name: 'Borsa Istanbul (BIST)' },
    'Poland':           { tz: 'Europe/Warsaw',       open: '09:00', close: '17:05', days: [1,2,3,4,5], name: 'Warsaw Stock Exchange (WSE)' },
    'Czech Republic':   { tz: 'Europe/Prague',       open: '09:00', close: '16:30', days: [1,2,3,4,5], name: 'Prague Stock Exchange (PSE)' },
    'Hungary':          { tz: 'Europe/Budapest',     open: '09:00', close: '17:00', days: [1,2,3,4,5], name: 'Budapest Stock Exchange (BET)' },
    'Greece':           { tz: 'Europe/Athens',       open: '10:00', close: '17:20', days: [1,2,3,4,5], name: 'Athens Stock Exchange (ATHEX)' },
    'Romania':          { tz: 'Europe/Bucharest',    open: '10:00', close: '17:45', days: [1,2,3,4,5], name: 'Bucharest Stock Exchange (BVB)' },
    'New Zealand':      { tz: 'Pacific/Auckland',    open: '10:00', close: '16:45', days: [1,2,3,4,5], name: 'New Zealand Exchange (NZX)' },
    'China (Shanghai)': { tz: 'Asia/Shanghai',       open: '09:30', close: '15:00', days: [1,2,3,4,5], name: 'Shanghai Stock Exchange (SSE)' },
    'China (Shenzhen)': { tz: 'Asia/Shanghai',       open: '09:30', close: '15:00', days: [1,2,3,4,5], name: 'Shenzhen Stock Exchange (SZSE)' },
    'US':               { tz: 'America/New_York',    open: '09:30', close: '16:00', days: [1,2,3,4,5], name: 'New York (NASDAQ + NYSE)' },
    'South Africa':     { tz: 'Africa/Johannesburg', open: '09:00', close: '17:00', days: [1,2,3,4,5], name: 'Johannesburg Stock Exchange' },
    'UK':               { tz: 'Europe/London',       open: '08:00', close: '16:30', days: [1,2,3,4,5], name: 'London Stock Exchange' },
    'Hong Kong':        { tz: 'Asia/Hong_Kong',      open: '09:30', close: '16:00', days: [1,2,3,4,5], name: 'Hong Kong Exchange' },
    'Australia':        { tz: 'Australia/Sydney',    open: '10:00', close: '16:00', days: [1,2,3,4,5], name: 'Australian Securities Exchange' },
    'Germany':          { tz: 'Europe/Berlin',       open: '09:00', close: '17:30', days: [1,2,3,4,5], name: 'Frankfurt Stock Exchange' },
    'Canada':           { tz: 'America/Toronto',     open: '09:30', close: '16:00', days: [1,2,3,4,5], name: 'Toronto Stock Exchange' },
    // Nordics split by country (previously lumped as "Nordic")
    'Sweden':           { tz: 'Europe/Stockholm',    open: '09:00', close: '17:30', days: [1,2,3,4,5], name: 'Nasdaq Stockholm (OMX)' },
    'Finland':          { tz: 'Europe/Helsinki',     open: '10:00', close: '18:30', days: [1,2,3,4,5], name: 'Nasdaq Helsinki (OMX)' },
    'Iceland':          { tz: 'Atlantic/Reykjavik',  open: '09:30', close: '15:30', days: [1,2,3,4,5], name: 'Nasdaq Iceland (OMX)' },
    // Euronext split by country
    'France':           { tz: 'Europe/Paris',        open: '09:00', close: '17:30', days: [1,2,3,4,5], name: 'Euronext Paris' },
    'Netherlands':      { tz: 'Europe/Amsterdam',    open: '09:00', close: '17:30', days: [1,2,3,4,5], name: 'Euronext Amsterdam' },
    'Belgium':          { tz: 'Europe/Brussels',     open: '09:00', close: '17:30', days: [1,2,3,4,5], name: 'Euronext Brussels' },
    'Portugal':         { tz: 'Europe/Lisbon',       open: '08:00', close: '16:30', days: [1,2,3,4,5], name: 'Euronext Lisbon' },
    'Ireland':          { tz: 'Europe/Dublin',       open: '08:00', close: '16:30', days: [1,2,3,4,5], name: 'Euronext Dublin' },
    'Japan':            { tz: 'Asia/Tokyo',          open: '09:00', close: '15:00', days: [1,2,3,4,5], name: 'Tokyo Stock Exchange (JPX)' },
    'Spain':            { tz: 'Europe/Madrid',       open: '09:00', close: '17:30', days: [1,2,3,4,5], name: 'Bolsa de Madrid (BME)' },
    'Austria':          { tz: 'Europe/Vienna',       open: '09:00', close: '17:30', days: [1,2,3,4,5], name: 'Wiener Börse' },
    'Chile':            { tz: 'America/Santiago',    open: '09:30', close: '16:00', days: [1,2,3,4,5], name: 'Bolsa de Santiago' },
};

// Slugify exchange display names for use in HTML IDs (CSS-safe).
function exSlug(s) {
    return (s || '').toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');
}

function getExchangeStatus(exCode) {
    const info = EXCHANGE_HOURS[exCode];
    if (!info) return null;

    const now = new Date();

    // Current time in exchange timezone
    const exTime = new Date(now.toLocaleString('en-US', { timeZone: info.tz }));
    const day = exTime.getDay();
    const exMins = exTime.getHours() * 60 + exTime.getMinutes();
    const [oh, om] = info.open.split(':').map(Number);
    const [ch, cm] = info.close.split(':').map(Number);
    const openMins = oh * 60 + om;
    const closeMins = ch * 60 + cm;
    const isTradeDay = info.days.includes(day);
    const isOpen = isTradeDay && exMins >= openMins && exMins < closeMins;

    // Convert exchange open/close times to user's local time.
    // Method: build a Date for "today at HH:MM in exchange tz",
    // then format it in the user's local tz.
    // We use the exchange's "today" date string to anchor the times.
    const exDateStr = exTime.getFullYear() + '-' +
        String(exTime.getMonth()+1).padStart(2,'0') + '-' +
        String(exTime.getDate()).padStart(2,'0');

    function exTimeToLocal(hh, mm) {
        // Create a date string interpreted in the exchange timezone
        // by computing the UTC equivalent
        const exFull = new Date(exDateStr + 'T' + String(hh).padStart(2,'0') + ':' + String(mm).padStart(2,'0') + ':00');
        // Get the offset: difference between "now as local in ex tz" and real now
        const nowInEx = new Date(now.toLocaleString('en-US', { timeZone: info.tz }));
        const offsetMs = nowInEx.getTime() - now.getTime();
        // The actual UTC time of the exchange event
        const utcTime = new Date(exFull.getTime() - offsetMs);
        // Format in user's local timezone
        return utcTime.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', hour12: false });
    }

    const localOpenStr = exTimeToLocal(oh, om);
    const localCloseStr = exTimeToLocal(ch, cm);

    if (isOpen) {
        const minsLeft = closeMins - exMins;
        const hrsLeft = Math.floor(minsLeft / 60);
        const mLeft = minsLeft % 60;
        const countdown = hrsLeft > 0 ? hrsLeft + 'h ' + mLeft + 'm' : mLeft + 'm';
        return {
            isOpen: true,
            label: 'OPEN',
            detail: info.name + ' · closes in ' + countdown + ' (at ' + localCloseStr + ' local)'
        };
    } else {
        let nextInfo = '';
        if (!isTradeDay || exMins >= closeMins) {
            // Find the next trading day name (e.g. "Monday")
            const dayNames = ['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'];
            let nextDay = day;
            for (let i = 1; i <= 7; i++) {
                const d = (day + i) % 7;
                if (info.days.includes(d)) { nextDay = d; break; }
            }
            nextInfo = 'opens ' + localOpenStr + ' local (' + dayNames[nextDay] + ')';
        } else {
            nextInfo = 'opens at ' + localOpenStr + ' local';
        }
        return {
            isOpen: false,
            label: 'CLOSED',
            detail: info.name + ' · ' + nextInfo
        };
    }
}

// Update exchange status displays
function updateExchangeStatuses(activeExchanges) {
    // Clear all
    Object.keys(EXCHANGE_HOURS).forEach(ex => {
        const el = document.getElementById('exstatus-' + exSlug(ex));
        if (el) el.innerHTML = '';
    });
    // Show status only when exactly one exchange is selected
    if (activeExchanges.length !== 1) return;
    const ex = activeExchanges[0];
    const el = document.getElementById('exstatus-' + exSlug(ex));
    if (!el) return;
    const st = getExchangeStatus(ex);
    if (!st) return;
    const dotCls = st.isOpen ? 'open' : 'closed';
    const lblCls = st.isOpen ? 'status-label-open' : 'status-label-closed';
    el.innerHTML = '<span class="status-dot ' + dotCls + '"></span>' +
        '<span class="' + lblCls + '">' + st.label + '</span>' +
        '<span class="status-text">' + st.detail + '</span>';
}

// Earnings toggle: upcoming vs past reports
function toggleEarnings(view) {
    const upcoming = document.getElementById('earnings-upcoming');
    const past = document.getElementById('earnings-past');
    const tabUp = document.getElementById('earnings-upcoming-tab');
    const tabPast = document.getElementById('earnings-past-tab');
    if (view === 'upcoming') {
        upcoming.style.display = '';
        past.style.display = 'none';
        tabUp.classList.add('active');
        tabPast.classList.remove('active');
    } else {
        upcoming.style.display = 'none';
        past.style.display = '';
        tabUp.classList.remove('active');
        tabPast.classList.add('active');
    }
}

// Stock panel: show/hide individual chips by their data-exchange.
// Chips are all rendered in one flat flex grid, so hiding happens per
// chip rather than per exchange container.
function updateStockPanel(activeExchanges) {
    // Hide/show individual chips
    document.querySelectorAll('.stock-chip[data-exchange]').forEach(chip => {
        if (activeExchanges.length === 0) {
            chip.classList.remove('filtered-out');
        } else {
            chip.classList.toggle('filtered-out', !activeExchanges.includes(chip.dataset.exchange));
        }
    });
    // Hide entire stock-panel containers when their exchange is filtered out
    // so no blank space remains from empty panels. Skip in flat mode — only
    // the first panel is visible and the rest are already hidden.
    if (!_stockLayoutFlat) {
        document.querySelectorAll('.stock-panel[data-exchange]').forEach(panel => {
            if (activeExchanges.length === 0) {
                panel.style.display = '';
            } else {
                panel.style.display = activeExchanges.includes(panel.dataset.exchange) ? '' : 'none';
            }
        });
    }
    // Refresh the "Stocks (N of TOTAL)" count + movers strip so the
    // sticky bar reflects the exchange-filtered subset.
    if (typeof _updateDensityHint === 'function') _updateDensityHint();
    if (typeof _renderStocksSummary === 'function'
        && document.body.classList.contains('stocks-collapsed')) {
        _renderStocksSummary();
    }
}

// ── Density modes: chip (default), line (one-row-per-stock), mini ──
// User choice persists in localStorage. First-time visitors auto-default
// to "line" when the watchlist is >30 stocks so 77 stocks stay visible
// without a wall of boxes.
const _DENSITY_AUTO_THRESHOLD = 30;
function setDensity(mode, skipSave) {
    if (mode !== 'chip' && mode !== 'line' && mode !== 'mini') mode = 'chip';
    document.body.classList.remove('density-chip', 'density-line', 'density-mini');
    document.body.classList.add('density-' + mode);
    document.querySelectorAll('.density-pill').forEach(p => {
        p.classList.toggle('active', p.dataset.density === mode);
    });
    if (!skipSave) localStorage.setItem('ee-stock-density', mode);
}
function _updateDensityHint() {
    const total = document.querySelectorAll('.stock-chip').length;
    // Count chips currently visible (not hidden by exchange/stock filter).
    // When a filter is active, show "N of TOTAL" so the user sees both.
    const visible = document.querySelectorAll(
        '.stock-chip:not(.filtered-out):not(.stock-hidden)'
    ).length;
    const hint = document.getElementById('density-count-hint');
    if (hint) {
        hint.textContent = (visible !== total)
            ? '(' + visible + ' of ' + total + ')'
            : '(' + total + ')';
    }
    return total;
}

// ── Stock panels: collapsible, with a summary strip when collapsed ──
// Auto-default: if >40 stocks, start collapsed so the user sees
// news/earnings/forums without scrolling past a wall of chips.
const _STOCKS_AUTO_COLLAPSE_THRESHOLD = 40;
function _computeStocksSummary() {
    // Pull every VISIBLE chip's ticker + change % — respect the
    // active exchange/stock filter so the summary reflects what
    // the user has selected.
    const chips = [...document.querySelectorAll(
        '.stock-chip[data-ticker]:not(.filtered-out):not(.stock-hidden)'
    )];
    const entries = [];
    chips.forEach(c => {
        const change = c.querySelector('.stock-chip-change');
        if (!change) return;
        const m = change.textContent.match(/(-?\\+?[\\d.]+)%/);
        if (!m) return;
        entries.push({
            ticker: c.dataset.ticker,
            pct: parseFloat(m[1]),
            up: change.classList.contains('up'),
            down: change.classList.contains('down'),
        });
    });
    const ups = entries.filter(e => e.up);
    const downs = entries.filter(e => e.down);
    const topUp = [...ups].sort((a,b) => b.pct - a.pct)[0];
    const topDown = [...downs].sort((a,b) => a.pct - b.pct)[0];
    return { visible: chips.length, ups: ups.length, downs: downs.length, topUp, topDown };
}
function _renderStocksSummary() {
    const strip = document.getElementById('stocks-summary-strip');
    if (!strip) return;
    const s = _computeStocksSummary();
    const parts = [
        '<span class="summary-sep">·</span>',
        '<span>' + s.ups + ' up</span>',
        '<span class="summary-sep">·</span>',
        '<span>' + s.downs + ' down</span>',
    ];
    if (s.topUp) {
        parts.push('<span class="summary-sep">·</span>');
        parts.push('<span class="summary-mover up" title="Biggest gainer today">' +
                   '▲ ' + s.topUp.ticker + ' +' + s.topUp.pct.toFixed(1) + '%</span>');
    }
    if (s.topDown) {
        parts.push('<span class="summary-mover down" title="Biggest loser today">' +
                   '▼ ' + s.topDown.ticker + ' ' + s.topDown.pct.toFixed(1) + '%</span>');
    }
    strip.innerHTML = parts.join(' ');
}
function toggleStocksCollapsed(skipSave) {
    const collapsed = !document.body.classList.contains('stocks-collapsed');
    document.body.classList.toggle('stocks-collapsed', collapsed);
    const strip = document.getElementById('stocks-summary-strip');
    if (strip) strip.style.display = collapsed ? 'inline-flex' : 'none';
    if (collapsed) _renderStocksSummary();
    if (!skipSave) localStorage.setItem('ee-stocks-collapsed', collapsed ? '1' : '0');
}
function _initStocksCollapsed() {
    const saved = localStorage.getItem('ee-stocks-collapsed');
    const count = document.querySelectorAll('.stock-chip').length;
    const shouldCollapse = saved !== null
        ? saved === '1'
        : count > _STOCKS_AUTO_COLLAPSE_THRESHOLD;
    if (shouldCollapse) {
        document.body.classList.add('stocks-collapsed');
        const strip = document.getElementById('stocks-summary-strip');
        if (strip) strip.style.display = 'inline-flex';
        _renderStocksSummary();
    }
    _setupGridVisibilityObserver();
}

// Per-panel collapse — click any country header to toggle just that
// exchange's chip grid. Useful at 77 stocks × 30 countries where
// only a handful of panels matter on a given day. State persists
// in localStorage as a comma-separated list of collapsed display
// exchange labels.
function _loadCollapsedPanels() {
    const raw = localStorage.getItem('ee-panels-collapsed') || '';
    return new Set(raw.split(',').filter(Boolean));
}
function _saveCollapsedPanels(set) {
    localStorage.setItem('ee-panels-collapsed', [...set].join(','));
}
function togglePanelCollapsed(headerEl) {
    const panel = headerEl.closest('.stock-panel');
    if (!panel) return;
    panel.classList.toggle('panel-collapsed');
    const ex = panel.dataset.exchange;
    const set = _loadCollapsedPanels();
    if (panel.classList.contains('panel-collapsed')) set.add(ex);
    else set.delete(ex);
    _saveCollapsedPanels(set);
}
function setAllPanelsCollapsed(collapsed) {
    const panels = document.querySelectorAll('.stock-panel[data-exchange]');
    const set = new Set();
    panels.forEach(p => {
        p.classList.toggle('panel-collapsed', collapsed);
        if (collapsed) set.add(p.dataset.exchange);
    });
    _saveCollapsedPanels(set);
}
// On page load, restore any panels the user had previously collapsed.
function _restoreCollapsedPanels() {
    const set = _loadCollapsedPanels();
    if (!set.size) return;
    document.querySelectorAll('.stock-panel[data-exchange]').forEach(p => {
        if (set.has(p.dataset.exchange)) p.classList.add('panel-collapsed');
    });
}

// When the chip grid is scrolled OUT of view but the user is still
// on the page (reading news, earnings, etc), reveal the mover
// summary in the sticky bar so they always have a stock reference.
// When the grid is visible, hide the summary — it'd be redundant
// with the grid itself. Collapsed state always shows the summary
// (that's its dedicated purpose).
function _setupGridVisibilityObserver() {
    if (typeof IntersectionObserver === 'undefined') return;
    const wrapper = document.getElementById('stock-panels-wrapper');
    const strip = document.getElementById('stocks-summary-strip');
    if (!wrapper || !strip) return;
    const observer = new IntersectionObserver((entries) => {
        // Don't fight the collapsed state — when collapsed the summary
        // is permanently visible via its own logic.
        if (document.body.classList.contains('stocks-collapsed')) return;
        entries.forEach(entry => {
            if (entry.isIntersecting) {
                // Grid is on-screen → summary would be noise, hide it.
                strip.style.display = 'none';
            } else {
                // Grid scrolled off → show the summary as persistent reference.
                _renderStocksSummary();
                strip.style.display = 'inline-flex';
            }
        });
    }, {
        // Trigger slightly before the grid fully exits so there's
        // no flicker as the user scrolls past the last row.
        rootMargin: '-50px 0px 0px 0px',
    });
    observer.observe(wrapper);
}
function _initDensity() {
    const count = _updateDensityHint();
    const saved = localStorage.getItem('ee-stock-density');
    if (saved) { setDensity(saved, true); }
    else { setDensity(count > _DENSITY_AUTO_THRESHOLD ? 'line' : 'chip', true); }
    _initStocksCollapsed();
    _restoreCollapsedPanels();
    _updateStickyOffset();
}

// Main header (KPIs / filter pills) is sticky at top:0. The stock
// layout bar below it also uses position:sticky, but both can't
// sit at top:0 or they overlap — the bar would hide under the
// header. Measure the header height and push the bar just below it.
function _updateStickyOffset() {
    const header = document.querySelector('.header');
    const bar = document.querySelector('.stock-layout-toggle');
    if (!header || !bar) return;
    const h = header.offsetHeight;
    bar.style.top = h + 'px';
}
window.addEventListener('resize', _updateStickyOffset);
// Header height can change when filter pills wrap on viewport resize;
// re-measure after any layout-affecting event.
if (typeof ResizeObserver !== 'undefined') {
    const ro = new ResizeObserver(_updateStickyOffset);
    const header = document.querySelector('.header');
    if (header) ro.observe(header);
}
// Run now and also when DOM finishes parsing (the script tag lives in
// the middle of the body, so some chips may not be in the DOM yet).
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', _initDensity);
} else {
    _initDensity();
}

// Toggle between grouped-by-exchange and flat layout
let _stockLayoutFlat = false;
// Save original parent for each chip so we can restore grouping
const _chipOriginalParent = new Map();

function toggleStockLayout(grouped, skipSave) {
    if (!skipSave) localStorage.setItem('ee-stock-grouped', grouped ? '1' : '0');
    const panels = [...document.querySelectorAll('.stock-panel[data-exchange]')];
    if (grouped) {
        _stockLayoutFlat = false;
        // Move chips back to their original panels
        _chipOriginalParent.forEach((parent, chip) => {
            parent.appendChild(chip);
        });
        _chipOriginalParent.clear();
        // Restore panels (header + status bar)
        panels.forEach(p => {
            p.style.display = '';
            const h = p.querySelector('.stock-panel-header');
            if (h) h.style.display = '';
        });
        // Re-apply current filter
        const actives = [...document.querySelectorAll('.filter-pill.active:not([data-exchange="ALL"])')]
            .map(p => p.dataset.exchange);
        updateStockPanel(actives);
    } else {
        _stockLayoutFlat = true;
        const first = panels[0];
        if (!first) return;
        const inner = first.querySelector('.stock-panel-inner');
        // Remember each chip's original parent, then move to first panel
        panels.forEach((p, i) => {
            if (i === 0) return;
            const pInner = p.querySelector('.stock-panel-inner');
            if (pInner) {
                [...pInner.children].forEach(chip => {
                    _chipOriginalParent.set(chip, pInner);
                    inner.appendChild(chip);
                });
            }
            p.style.display = 'none';
        });
        // In flat mode the first panel's country header (e.g. "Australia")
        // makes no sense since it now holds every country's chips.
        const fh = first.querySelector('.stock-panel-header');
        if (fh) fh.style.display = 'none';
        first.style.display = '';
        // Re-apply filter on chips only
        const actives = [...document.querySelectorAll('.filter-pill.active:not([data-exchange="ALL"])')]
            .map(p => p.dataset.exchange);
        updateStockPanel(actives);
    }
}

// Update stock pills visibility based on active exchanges
function updateStockPills(activeExchanges) {
    document.querySelectorAll('.stock-pill').forEach(pill => {
        const pillEx = pill.dataset.exchange;
        if (!pillEx || pill.dataset.ticker === 'ALL') {
            // "All" pill (and earnings upcoming/past tabs) — always visible
            pill.classList.remove('hidden-pill');
            return;
        }
        if (activeExchanges.length === 0) {
            // No exchange filter — show all stock pills
            pill.classList.remove('hidden-pill');
        } else {
            pill.classList.toggle('hidden-pill', !activeExchanges.includes(pillEx));
        }
    });

    // Exchange change always clears the global stock selection — otherwise
    // a ticker from a now-hidden exchange would stay "active" invisibly.
    if (typeof activeTickers !== 'undefined') {
        activeTickers.clear();
    }
    // Reset visible stock-hidden cards/rows to the unfiltered state
    document.querySelectorAll('.section [data-ticker]').forEach(el => {
        el.classList.remove('stock-hidden');
    });
    // Reset ticker pills to "All" active
    document.querySelectorAll('.filter-group.stocks .stock-pill').forEach(p => {
        p.classList.toggle('active', p.dataset.ticker === 'ALL');
    });
    if (typeof applyNewsAgeFilter === 'function') applyNewsAgeFilter();
}

// Classes the filter handler must NOT touch with inline display —
// these are managed by their own dedicated updater functions.
function _isDedicatedManaged(el) {
    return el.classList.contains('filter-pill')
        || el.classList.contains('stock-panel')
        || el.classList.contains('stock-chip')
        || el.classList.contains('exchange-status');
}

// Exchange filter pills
// Default behaviour: single-select — clicking an exchange replaces the
// current selection. Shift/Cmd/Ctrl-click toggles additive multi-select
// (same convention used by the stock pill bar). Clicking an already-
// active exchange clears the filter back to "All".
function _applyExchangeFilter(actives) {
    if (actives.length === 0) {
        document.querySelectorAll('.filter-pill').forEach(p => p.classList.remove('active'));
        document.querySelector('.filter-pill[data-exchange="ALL"]').classList.add('active');
        document.querySelectorAll('[data-exchange]').forEach(el => {
            if (!_isDedicatedManaged(el)) el.style.display = '';
        });
        updateStockPanel([]);
        updateStockPills([]);
        updateExchangeStatuses([]);
        _applyCollapsedState();
        applyNewsAgeFilter();
        _updateEmptyGroups();
        _updateSectionCounts();
        return;
    }
    document.querySelectorAll('.filter-pill').forEach(p => {
        const ex = p.dataset.exchange;
        p.classList.toggle('active', ex !== 'ALL' && actives.includes(ex));
    });
    document.querySelectorAll('[data-exchange]').forEach(el => {
        if (_isDedicatedManaged(el)) return;
        el.style.display = actives.includes(el.dataset.exchange) ? '' : 'none';
    });
    updateStockPanel(actives);
    updateStockPills(actives);
    updateExchangeStatuses(actives);
    _applyCollapsedState();
    applyNewsAgeFilter();
    _updateEmptyGroups();
    _updateSectionCounts();
}

document.querySelectorAll('.filter-pill').forEach(pill => {
    pill.addEventListener('click', (e) => {
        const ex = pill.dataset.exchange;
        if (ex === 'ALL') {
            _applyExchangeFilter([]);
            return;
        }
        // Shift / Cmd / Ctrl click = additive multi-select toggle
        const additive = e.shiftKey || e.metaKey || e.ctrlKey;
        const current = [...document.querySelectorAll('.filter-pill.active:not([data-exchange="ALL"])')]
            .map(p => p.dataset.exchange);
        let next;
        if (additive) {
            if (current.includes(ex)) {
                next = current.filter(x => x !== ex);
            } else {
                next = [...current, ex];
            }
        } else {
            // Single-select: clicking the sole active pill clears the filter,
            // otherwise replace whatever was selected with this one.
            if (current.length === 1 && current[0] === ex) {
                next = [];
            } else {
                next = [ex];
            }
        }
        _applyExchangeFilter(next);
    });
});
"""


# ---------------------------------------------------------------------------
# HTML builder
# ---------------------------------------------------------------------------

def generate_html(db: Database, config: dict, target_date: str = None) -> str:
    """Build a self-contained HTML dashboard string."""

    if target_date is None:
        target_date = datetime.utcnow().strftime("%Y-%m-%d")

    since = f"{target_date}T00:00:00Z"
    # News and contracts: look back 1 year to show historical items
    since_1y = (datetime.strptime(target_date, "%Y-%m-%d") - timedelta(days=365)).strftime("%Y-%m-%dT00:00:00Z")

    # ── Fetch data ──
    # Fetch data and filter out non-Latin script entries
    # (Cyrillic, CJK, Arabic etc.) — keep English, French, Swedish, Italian
    def _filter_latin(items, text_keys):
        return [i for i in items
                if not any(_has_unsupported_language(i.get(k, "")) for k in text_keys)]

    news = _filter_latin(db.get_news_since(since_1y), ["title", "snippet"])
    contracts = _filter_latin(db.get_contracts_since(since_1y), ["title", "snippet"])
    earnings = db.get_upcoming_earnings(within_days=365)   # 12-month forward look
    forum = _filter_latin(db.get_forum_since(since_1y), ["text"])
    insiders = _filter_latin(db.get_insiders_since(since_1y), ["title", "snippet"])

    from fetchers import get_active_stocks
    active_stocks = get_active_stocks(db, config)

    # ── Display-name groups for exchanges ─────────────────────────────
    # Internal exchange codes (KLSE, NGX, BRVM, NASDAQ, NYSE, ...) are
    # confusing for users. We display country-based labels instead.
    # Multiple internal codes can map to the same display group:
    # NASDAQ + NYSE + AMEX + OTC all become "US".
    EXCHANGE_DISPLAY = {
        "NASDAQ":   "US",
        "NYSE":     "US",
        "AMEX":     "US",
        "OTC":      "US",
        "PNK":      "US",
        "KLSE":     "Malaysia",
        "NGX":      "Nigeria",
        "BRVM":     "Ivory Coast/BRVM",
        "UZSE":     "Uzbekistan",
        "SGX":      "Singapore",
        "KSE":      "Kyrgyzstan",
        "KASE":     "Kazakhstan",
        "NSEK":     "Kenya",
        "GSE":      "Ghana",
        "BWSE":     "Botswana",
        "LUSE":     "Zambia",
        "DSET":     "Tanzania",
        "DSEB":     "Bangladesh",
        "PSX":      "Pakistan",
        "CSEM":     "Morocco",
        "ZSE":      "Croatia",
        "BELEX":    "Serbia",
        "BSSE":     "Slovakia",
        "PNGX":     "Papua New Guinea",
        "BVMT":     "Tunisia",
        "CSEL":     "Sri Lanka",
        "UX":       "Ukraine",
        "USE":      "Uganda",
        "RSE":      "Rwanda",
        "SEM":      "Mauritius",
        "ISX":      "Iraq",
        "ESX":      "Ethiopia",
        "JSE":      "South Africa",
        "LSE":      "UK",
        "IOB":      "UK",           # LSE International Orderbook
        "HKSE":     "Hong Kong",
        "ASX":      "Australia",
        "FRA":      "Germany",
        "TSX":      "Canada",
        "BMV":      "Mexico",
        # Euronext split by country
        "EURONEXT": "Europe",
        "EUR_FR":   "France",
        "EUR_NL":   "Netherlands",
        "EUR_BE":   "Belgium",
        "EUR_PT":   "Portugal",
        "EUR_IE":   "Ireland",
        "BIT":      "Italy",
        # Nordics by country (was "Nordic" for the combined OMX bucket)
        "OMX":      "Sweden",
        "HSE":      "Finland",
        "ICEX":     "Iceland",
        "OSE":      "Norway",
        "CSE":      "Denmark",
        "SWX":      "Switzerland",
        "B3":       "Brazil",
        "BCBA":     "Argentina",
        "KRX":      "South Korea",
        "TWSE":     "Taiwan",
        "IDX":      "Indonesia",
        "SET":      "Thailand",
        "PSE":      "Philippines",
        "HOSE":     "Vietnam",
        "TASE":     "Israel",
        "TADAWUL":  "Saudi Arabia",
        "DFM":      "UAE (Dubai)",
        "ADX":      "UAE (Abu Dhabi)",
        "QSE":      "Qatar",
        "BIST":     "Turkey",
        "WSE":      "Poland",
        "PSE_CZ":   "Czech Republic",
        "BET":      "Hungary",
        "ATHEX":    "Greece",
        "BVB":      "Romania",
        "NZX":      "New Zealand",
        "SSE":      "China (Shanghai)",
        "SZSE":     "China (Shenzhen)",
        "JPX":      "Japan",
        "BME":      "Spain",
        "WBAG":     "Austria",
        "BVS":      "Chile",
        "AMEX":     "US",
        "OTC":      "US",
    }
    def display_ex(code: str) -> str:
        return EXCHANGE_DISPLAY.get((code or "").upper(), code or "")

    # Map internal exchange code → region class for the inline exchange
    # badge shown on flat news/forum cards. Region colors help the eye
    # scan across a mixed chronological stream.
    _EX_REGION = {
        # Africa
        "NGX":"africa","BRVM":"africa","JSE":"africa","NSEK":"africa",
        "GSE":"africa","BWSE":"africa","LUSE":"africa","DSET":"africa",
        "USE":"africa","RSE":"africa","SEM":"africa","CSEM":"africa",
        "BVMT":"africa","ESX":"africa",
        # Asia
        "KLSE":"asia","SGX":"asia","HKSE":"asia","NSE":"asia","BSE":"asia",
        "UZSE":"asia","KSE":"asia","KASE":"asia","DSEB":"asia","PSX":"asia",
        "CSEL":"asia","KRX":"asia","TWSE":"asia","IDX":"asia","SET":"asia",
        "PSE":"asia","HOSE":"asia","SSE":"asia","SZSE":"asia","JPX":"asia",
        # Europe
        "LSE":"europe","IOB":"europe","FRA":"europe","BIT":"europe",
        "OMX":"europe","HSE":"europe","ICEX":"europe",
        "OSE":"europe","CSE":"europe","SWX":"europe",
        "EURONEXT":"europe","EUR_FR":"europe","EUR_NL":"europe",
        "EUR_BE":"europe","EUR_PT":"europe","EUR_IE":"europe",
        "ZSE":"europe","BELEX":"europe","BSSE":"europe","UX":"europe",
        "WSE":"europe","PSE_CZ":"europe","BET":"europe","ATHEX":"europe",
        "BVB":"europe","BIST":"europe","BME":"europe","WBAG":"europe",
        # Middle East
        "ISX":"me","TASE":"me","TADAWUL":"me","DFM":"me","ADX":"me","QSE":"me",
        # Americas
        "NASDAQ":"americas","NYSE":"americas","AMEX":"americas","OTC":"americas",
        "PNK":"americas","TSX":"americas","BMV":"americas","B3":"americas",
        "BCBA":"americas","BVS":"americas",
        # Pacific
        "ASX":"pacific","NZX":"pacific","PNGX":"pacific",
    }
    def ex_region(code: str) -> str:
        return _EX_REGION.get((code or "").upper(), "")

    def ex_badge_html(internal_code: str, display_label: str) -> str:
        region = ex_region(internal_code)
        cls = f"ex-badge r-{region}" if region else "ex-badge"
        return f'<span class="{cls}">{_esc(display_label)}</span>'

    def ex_slug(label: str) -> str:
        """Slugify a display label for use in HTML IDs (must match the JS exSlug)."""
        import re as _re
        return _re.sub(r'[^a-z0-9]+', '-',
                       (label or '').lower()).strip('-')

    # Annotate each active stock with its display group (mutates in place;
    # the original 'exchange' field stays for DB lookups and price scrapers).
    for s in active_stocks:
        s["_display_ex"] = display_ex(s.get("exchange", ""))

    stock_map = {s["ticker"]: s for s in active_stocks}
    exchanges = sorted({s["_display_ex"] for s in active_stocks})

    # ── Stats ──
    total_stocks = len(active_stocks)
    gen_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    # Does the user have a Serper key configured? Used to enable/disable
    # the Full refresh button in the bottom bar.
    try:
        from fetchers import get_serper_api_key as _get_serper_key
        _serper_key_set = bool(_get_serper_key())
    except Exception:
        _serper_key_set = bool(os.environ.get("SERPER_API_KEY", ""))

    # ── FX rates (from Yahoo Finance, free, no Serper) ──
    # Yahoo can hang for 15+ seconds on exotic currency pairs. We do
    # our own short-timeout fetch and cache the result in-process so
    # the monitor page stays snappy. Pair list is DYNAMIC — we show
    # any non-USD currency present in the current watchlist.
    _FX_CACHE_TTL_SEC = 300   # 5 min on successful fetches
    _FX_FAIL_TTL_SEC  = 120   # 2 min on failures — retry later but don't hang
    _FX_TIMEOUT_SEC   = 4     # short so slow pairs don't block the render

    _fx_cache = getattr(generate_html, "_fx_cache", None)
    if _fx_cache is None:
        _fx_cache = {}
        generate_html._fx_cache = _fx_cache  # type: ignore[attr-defined]

    def _fx_fetch(pair: str):
        import urllib.request as _ureq, urllib.parse as _upar, json as _json
        url = ("https://query1.finance.yahoo.com/v8/finance/chart/"
               + _upar.quote(pair))
        req = _ureq.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        try:
            with _ureq.urlopen(req, timeout=_FX_TIMEOUT_SEC) as resp:
                data = _json.loads(resp.read().decode("utf-8"))
            meta = (data.get("chart") or {}).get("result", [{}])[0].get("meta", {})
            price = meta.get("regularMarketPrice")
            prev = meta.get("chartPreviousClose") or meta.get("previousClose") or 0
            if price is None:
                return None
            chg = ((price - prev) / prev * 100) if prev else 0.0
            return (float(price), round(chg, 2), meta.get("currency", ""))
        except Exception:
            return None

    def _fx_get(pair: str):
        """Return cached FX or kick off a background fetch and return None.
        The page never blocks on Yahoo — exotic pairs can take 15+s. The
        next render picks up whatever the background thread has stored."""
        import time as _t
        import threading as _th
        entry = _fx_cache.get(pair)
        now = _t.time()
        if entry is not None:
            ts, val = entry
            ttl = _FX_CACHE_TTL_SEC if val is not None else _FX_FAIL_TTL_SEC
            if now - ts < ttl:
                return val
        # Mark as "in flight" so repeated renders don't spawn threads.
        _fx_cache[pair] = (now, None)

        def _worker(p):
            val = _fx_fetch(p)
            _fx_cache[p] = (_t.time(), val)

        _th.Thread(target=_worker, args=(pair,), daemon=True).start()
        return None

    portfolio_currencies = {s.get("currency", "") for s in active_stocks}
    # Skip USD (implicit) and the Johannesburg cents-pence pseudo-code.
    _portfolio_fx = sorted(c for c in portfolio_currencies
                           if c and c.upper() not in ("USD", "ZAC"))
    fx_html_parts = []
    for label in _portfolio_fx:
        pair = f"{label.upper()}=X"
        r = _fx_get(pair)
        if r:
            rate, chg_pct, _ = r
            display_label = label
            if rate >= 100:
                rate_str = f"{rate:,.0f}"
            elif rate >= 1:
                rate_str = f"{rate:.2f}"
            else:
                rate_str = f"{rate:.4f}"
            # Color the change
            if chg_pct > 0:
                chg_cls = "fx-up"
                chg_str = f"+{chg_pct:.1f}%"
            elif chg_pct < 0:
                chg_cls = "fx-down"
                chg_str = f"{chg_pct:.1f}%"
            else:
                chg_cls = "fx-flat"
                chg_str = "0.0%"
            fx_html_parts.append(
                f'<span class="fx-pair"><span class="fx-label">USD/{display_label}</span>'
                f'<span class="fx-rate">{rate_str}</span>'
                f'<span class="{chg_cls}">{chg_str}</span></span>')

    fx_bar_html = "".join(fx_html_parts) if fx_html_parts else ""

    # ── Build stock panel (single flat grid so chips flow horizontally) ──
    # All chips are rendered side-by-side in one wrapping flex container.
    # Each chip carries its display-exchange as a data attribute so the
    # exchange filter can hide/show chips individually. Exchange status
    # indicators live in a parallel container and are toggled per-exchange.
    # Price lookup needs the INTERNAL exchange code (NGX/NASDAQ/...), not
    # the country display label. Fetch once per real exchange across all
    # active stocks, then look up by ticker below.
    price_map: dict[str, dict] = {}
    for _internal_ex in {s.get("exchange", "") for s in active_stocks}:
        if not _internal_ex:
            continue
        for _p in db.get_latest_prices_by_exchange(_internal_ex):
            price_map[_p["ticker"]] = _p

    # Readable full names for internal exchange codes (shown in panel
    # headers when grouped by exchange). Short so the header doesn't
    # consume too much space; one line per country.
    EXCHANGE_FULL_NAME = {
        "NASDAQ": "NASDAQ", "NYSE": "NYSE", "AMEX": "NYSE American", "OTC": "OTC",
        "PNK": "OTC Pink",
        "LSE": "London Stock Exchange", "IOB": "LSE International Orderbook",
        "FRA": "Frankfurt", "BIT": "Borsa Italiana", "SWX": "SIX Swiss",
        "OMX": "Nasdaq Stockholm", "HSE": "Nasdaq Helsinki",
        "ICEX": "Nasdaq Iceland", "OSE": "Oslo Børs", "CSE": "Nasdaq Copenhagen",
        "EUR_FR": "Euronext Paris", "EUR_NL": "Euronext Amsterdam",
        "EUR_BE": "Euronext Brussels", "EUR_PT": "Euronext Lisbon",
        "EUR_IE": "Euronext Dublin", "EURONEXT": "Euronext",
        "BME": "Bolsa de Madrid", "WBAG": "Wiener Börse",
        "TSX": "Toronto Stock Exchange", "BMV": "Bolsa Mexicana",
        "B3": "B3 São Paulo", "BCBA": "BYMA Buenos Aires", "BVS": "Bolsa de Santiago",
        "JSE": "Johannesburg", "NGX": "Nigerian Exchange",
        "BRVM": "BRVM (Abidjan)", "UZSE": "Tashkent", "KSE": "Kyrgyz SE",
        "KASE": "KASE", "NSEK": "Nairobi", "GSE": "Ghana SE",
        "BWSE": "Botswana SE", "LUSE": "Lusaka SE", "DSET": "Dar es Salaam",
        "USE": "Uganda SE", "RSE": "Rwanda SE", "SEM": "Mauritius SE",
        "CSEM": "Casablanca", "BVMT": "Tunis", "ESX": "Ethiopia SE",
        "DSEB": "Dhaka SE", "PSX": "Pakistan SE", "CSEL": "Colombo SE",
        "ISX": "Iraq SE", "TASE": "Tel Aviv", "TADAWUL": "Tadawul",
        "DFM": "DFM Dubai", "ADX": "ADX Abu Dhabi", "QSE": "Qatar SE",
        "KLSE": "Bursa Malaysia", "SGX": "Singapore Exchange",
        "HKSE": "Hong Kong Exchange", "NSE": "NSE India", "BSE": "BSE Mumbai",
        "KRX": "Korea Exchange", "TWSE": "Taiwan SE", "IDX": "Indonesia SE",
        "SET": "SET Thailand", "PSE": "Philippine SE", "HOSE": "HOSE Vietnam",
        "SSE": "Shanghai SE", "SZSE": "Shenzhen SE", "JPX": "Tokyo Stock Exchange",
        "ASX": "ASX", "NZX": "NZX", "PNGX": "PNGX",
        "ZSE": "Zagreb SE", "BELEX": "Belgrade SE", "BSSE": "Bratislava SE",
        "WSE": "Warsaw SE", "PSE_CZ": "Prague SE", "BET": "Budapest SE",
        "ATHEX": "Athens SE", "BVB": "Bucharest SE", "BIST": "Borsa Istanbul",
        "UX": "Ukrainian Exchange",
    }

    stock_panels_html = []
    for ex in exchanges:
        ex_stocks = [s for s in active_stocks if s["_display_ex"] == ex]

        chips = []
        # Track which internal exchange codes are represented in this
        # country group, so the header can show "US — NASDAQ, NYSE"
        # when multiple exchanges share a country display label.
        internal_codes_in_group = []
        for s in ex_stocks:
            ic = (s.get("exchange") or "").upper()
            if ic and ic not in internal_codes_in_group:
                internal_codes_in_group.append(ic)
            pd = price_map.get(s["ticker"])
            if pd and pd.get("price") is not None:
                pct = pd.get("change_pct", 0) or 0
                if pct > 0:
                    chg_cls, chg_prefix = "up", "+"
                elif pct < 0:
                    chg_cls, chg_prefix = "down", ""
                else:
                    chg_cls, chg_prefix = "flat", ""
                price_line = f"""<div class="stock-chip-price">{_esc(pd.get('currency',''))} {_fmt_price(pd['price'])}
                    <span class="stock-chip-change {chg_cls}">{chg_prefix}{pct:.1f}%</span></div>"""
            elif has_price_source(s):
                price_line = ('<div class="stock-chip-nodata awaiting" '
                              'title="This stock has a live price source — '
                              'click Free refresh to populate">'
                              'Awaiting refresh</div>')
            else:
                price_line = ('<div class="stock-chip-nodata nosource" '
                              'title="No free price source exists for this '
                              'exchange yet — add a yahoo_ticker if you have one">'
                              'No price source</div>')

            # Build compact display: use ticker as visual anchor and the
            # name as secondary so the layout degrades gracefully in
            # line/mini density. Full name is always in the `title`
            # attribute so hovering reveals it.
            chips.append(f"""
            <div class="stock-chip" data-exchange="{_esc(ex)}" data-ticker="{_esc(s['ticker'])}" title="{_esc(s['name'])}">
                <span class="stock-chip-remove" title="Remove from watchlist"
                      onclick="removeStockFromWatchlist('{_esc(s['ticker'])}', '{_esc(s['exchange'])}', '{_esc(s['name'])}')">✕</span>
                <div class="stock-chip-name">{_esc(s['name'])}</div>
                <div class="stock-chip-ticker"><span class="tk-sym">{_esc(s['ticker'])}</span>{(' <span class="tk-sep">·</span> <span class="tk-code">' + _esc(s.get('code','')) + '</span>') if s.get('code') and s.get('code') != s.get('ticker') else ''}</div>
                {price_line}
            </div>""")

        # Header: "Country — ExchangeA, ExchangeB" with exchange names
        # only for codes that have stocks in this group.
        ex_names = [EXCHANGE_FULL_NAME.get(ic, ic) for ic in internal_codes_in_group]
        ex_names_str = ", ".join(ex_names) if ex_names else ""
        stock_panels_html.append(f"""
        <div class="stock-panel" data-exchange="{_esc(ex)}">
            <div class="stock-panel-header" onclick="togglePanelCollapsed(this)" title="Click to collapse / expand">
                <span class="panel-chevron">▼</span>
                <span class="stock-panel-country">{_esc(ex)}</span>
                {'<span class="stock-panel-sep">—</span>' if ex_names_str else ''}
                <span class="stock-panel-exchanges">{_esc(ex_names_str)}</span>
                <span class="stock-panel-count">({len(ex_stocks)})</span>
                <div class="exchange-status" id="exstatus-{ex_slug(ex)}"></div>
            </div>
            <div class="stock-panel-inner">{''.join(chips)}</div>
        </div>""")

    # If the watchlist is empty, replace the stock panels with a welcome CTA
    if not active_stocks:
        stock_panels_html = ['''
        <div class="welcome-card">
            <h2>👋 Welcome to Emerging Edge</h2>
            <p>Your watchlist is empty. Add your first stock to start tracking news,
            earnings, insider transactions, forum buzz, and price action.</p>
            <button onclick="openAddStockModal()">➕ Add your first stock</button>
            <p style="font-size:0.75rem;margin-top:1rem">
            Type a company name or ticker in any language — we'll resolve it to
            the right exchange automatically.
            </p>
        </div>''']

    # ── Build alerts cards ──
    # Alerts are for TODAY only — breaking news, not a 30-day feed.
    # A contract win or big price move from a month ago belongs in News,
    # not in the banner strip at the top. Window is 2 days to be safe
    # with timezones (a story published late UTC may look like yesterday
    # to the server but be today in the user's local time).
    ALERT_LIMIT = 10
    ALERT_MAX_AGE_DAYS = 2

    # Signal keywords — title must contain at least one to be included
    _SIGNAL_EN = ["win", "award", "secur", "acqui", "bag", "land",
                  "sign", "deal", "partner", "venture", "invest",
                  "merger", "takeover", "buyout", "stake", "joint"]
    _SIGNAL_FR = ["remport", "attribu", "acqui", "partenariat",
                  "fusion", "investiss", "contrat"]

    def _is_important(title_lower: str) -> bool:
        """Does the title contain a signal keyword?"""
        for kw in _SIGNAL_EN + _SIGNAL_FR:
            if kw in title_lower:
                return True
        return False

    def _is_relevant(title: str, snippet: str, stock: dict) -> bool:
        """
        Verify the result actually refers to OUR company, not a
        similarly-named one (e.g. CEMATRIX vs Matrix Concepts).

        Uses word-boundary matching to avoid substring false positives.
        """
        import re as _re
        text = (title + " " + snippet).lower()
        name = stock.get("name", "").lower()
        ticker = stock.get("ticker", "").lower()
        code = stock.get("code", "").lower()

        # Direct ticker/code as whole word.
        # For tickers that are common English words (e.g. MATRIX, FOCUS),
        # require the ticker to appear alongside at least one name word.
        _common_words = {"matrix", "focus", "critical", "bank", "group"}
        if ticker and _re.search(r'\b' + _re.escape(ticker) + r'\b', text):
            if ticker not in _common_words:
                return True
            # Common-word ticker: also need a name word nearby
            name_words_check = [w for w in name.split() if len(w) >= 4 and w != ticker]
            if any(_re.search(r'\b' + _re.escape(w) + r'\b', text) for w in name_words_check):
                return True
        if code and len(code) >= 3 and _re.search(r'\b' + _re.escape(code) + r'\b', text):
            return True

        # Check multi-word company name — require at least 2 significant
        # words appearing as whole words (not as substrings of other words)
        name_words = [w for w in name.split() if len(w) >= 4]
        if len(name_words) >= 2:
            matches = sum(1 for w in name_words
                          if _re.search(r'\b' + _re.escape(w) + r'\b', text))
            if matches >= 2:
                return True
        elif len(name_words) == 1:
            if _re.search(r'\b' + _re.escape(name_words[0]) + r'\b', text):
                return True

        # Full name match
        if name and name in text:
            return True

        return False

    def _is_recent(pub_str: str, max_days: int) -> bool:
        """Is the published date within max_days of today?"""
        if not pub_str:
            return False
        for fmt in ("%Y-%m-%d", "%b %d, %Y", "%d %b %Y", "%d %B %Y",
                    "%B %d, %Y", "%Y-%m-%dT%H:%M:%S"):
            try:
                dt = datetime.strptime(pub_str.strip()[:19], fmt)
                return (datetime.now() - dt).days <= max_days
            except ValueError:
                continue
        return False

    alert_all = []

    # Price moves >5% (always top priority)
    for s in active_stocks:
        price_data = db.get_latest_price(s["ticker"], s["exchange"])
        if price_data and price_data.get("change_pct") is not None:
            pct = price_data["change_pct"]
            if abs(pct) >= 5.0:
                cls = "price-up" if pct > 0 else "price-down"
                icon = "📈" if pct > 0 else "📉"
                alert_all.append(f"""
        <div class="alert-card {cls}" data-exchange="{_esc(display_ex(s['exchange']))}">
            <div class="alert-stock">{icon} {_esc(s['name'])} ({_esc(s['ticker'])})</div>
            <div class="alert-title" style="font-size:0.9rem;font-weight:600;">
                Price move <span style="color:var({'--green' if pct > 0 else '--red'})">{pct:+.1f}%</span> — {_esc(price_data.get('currency',''))} {price_data.get('price','N/A')}
            </div>
            <div class="alert-date">{_esc(price_data.get('snapshot_at', ''))}</div>
        </div>""")

    # Contracts — filtered: recent + important + relevant to our company
    sorted_contracts = sorted(contracts, key=lambda c: c.get("published", ""), reverse=True)
    for c in sorted_contracts:
        title_raw = c.get("title", "")
        pub_raw = c.get("published", "")
        if not _is_recent(pub_raw, ALERT_MAX_AGE_DAYS):
            continue
        if not _is_important(title_raw.lower()):
            continue
        tk = c.get("ticker", "")
        stock_info = stock_map.get(tk, {})
        snippet_raw = c.get("snippet", "")
        if not _is_relevant(title_raw, snippet_raw, stock_info):
            continue
        sname = stock_map.get(tk, {}).get("name", tk)
        ex = display_ex(c.get("exchange", ""))
        title = _esc(_strip_html(title_raw))
        url = _esc(c.get("url", "#"))
        pub_date = _esc(pub_raw)
        alert_all.append(f"""
        <div class="alert-card" data-exchange="{_esc(ex)}">
            <div class="alert-stock">{_esc(sname)} ({_esc(tk)}) <span style="color:var(--text-muted);font-weight:400;font-size:0.72rem">· {_esc(ex)}</span></div>
            <div class="alert-title"><a href="{url}" target="_blank">{title}</a></div>
            {"<div class='alert-date'>📅 " + pub_date + "</div>" if pub_date else ""}
        </div>""")

    if not alert_all:
        alert_html_str = '<div class="alerts-strip"><div class="empty">No alerts — nothing significant in the last 2 days</div></div>'
    else:
        # Horizontal strip — no limit, scroll handles overflow
        alert_html_str = f'<div class="alerts-strip">{"".join(alert_all)}</div>'

    # ── Build news section (most recent first) ──
    # Stock filtering happens via the global ticker bar at the top of the
    # page, so we no longer need per-section pills.
    # Sort by epoch (second resolution) so recent items like "9 hours
    # ago" sort correctly above items from yesterday. Items with no
    # parseable date get epoch 0 and fall to the bottom.
    news_sorted = sorted(news,
                         key=lambda n: _parse_news_epoch(n.get("published", "")),
                         reverse=True)

    # Group by display exchange (e.g. NASDAQ + NYSE both → "US")
    news_by_ex: dict[str, list] = {}
    for n in news_sorted:
        ex = display_ex(n.get("exchange", "Other"))
        news_by_ex.setdefault(ex, []).append(n)

    # Layout decision: when there are more than FLAT_THRESHOLD distinct
    # exchanges with news, render a flat chronological stream with an
    # inline exchange badge on each card. Below the threshold, keep the
    # grouped-by-exchange layout — useful when filtering to 1-3
    # exchanges where grouping actually helps navigation.
    FLAT_THRESHOLD = 3
    flat_news = len(news_by_ex) > FLAT_THRESHOLD

    # Initial render shows only the first N news items; the rest are
    # marked .collapsed-hidden and a "Show more" button reveals them.
    NEWS_INITIAL_LIMIT = 10
    news_total = sum(len(v) for v in news_by_ex.values())
    news_cards_html = []

    def _news_card_html(n, idx_ref) -> str:
        tk = n.get("ticker", "")
        internal_ex = n.get("exchange", "")
        display_label = display_ex(internal_ex)
        sname = stock_map.get(tk, {}).get("name", tk)
        title = _esc(_strip_html(n.get("title", "No title")))
        url = _esc(n.get("url", "#"))
        snippet = _esc(_strip_html(n.get("snippet", "")))[:200]
        source = _esc(n.get("source", ""))
        pub = _esc(n.get("published", ""))
        pub_epoch = _parse_news_epoch(n.get("published", ""))
        lang_badge = '<span class="lang-badge">🇫🇷 FR</span>' if n.get("lang") == "fr" else ""
        ex_badge = ex_badge_html(internal_ex, display_label)

        is_collapsed = idx_ref[0] >= NEWS_INITIAL_LIMIT
        hidden_cls = " collapsed-hidden" if is_collapsed else ""
        collapsed_attr = ' data-collapsed="1"' if is_collapsed else ""
        idx_ref[0] += 1

        return f"""
        <div class="news-card{hidden_cls}"{collapsed_attr} data-exchange="{_esc(display_label)}" data-ticker="{_esc(tk)}" data-pub-epoch="{pub_epoch}">
            <div class="news-stock">{ex_badge} {_esc(sname)} ({_esc(tk)}) {lang_badge}</div>
            <div class="news-title"><a href="{url}" target="_blank">{title}</a></div>
            {"<div class='news-snippet'>" + snippet + "</div>" if snippet else ""}
            <div class="news-meta">{source}{(' · ' + pub) if pub else ''}</div>
        </div>"""

    idx_ref = [0]
    if flat_news:
        # Flat chronological stream across all exchanges — already sorted
        # newest-first by `news_sorted`.
        for n in news_sorted:
            news_cards_html.append(_news_card_html(n, idx_ref))
    else:
        # Grouped by exchange — use when filter is narrow (1-3 exchanges).
        for ex in sorted(news_by_ex.keys()):
            items = news_by_ex[ex]
            cards = [_news_card_html(n, idx_ref) for n in items]
            news_cards_html.append(f"""
            <div class="exchange-group" data-exchange="{_esc(ex)}">
                <div class="exchange-header">
                    {_esc(ex)} <span style="font-weight:400;color:var(--text-muted)">({len(items)})</span>
                    <span class="chevron">▼</span>
                </div>
                <div class="exchange-body">{''.join(cards)}</div>
            </div>""")

    if news_total > NEWS_INITIAL_LIMIT:
        news_cards_html.append(
            f'<button class="show-more-btn" id="news-show-more" '
            f'onclick="expandSection(this, \'#news-section\')">'
            f'▼ Show more</button>')

    if not news_cards_html:
        news_cards_html.append('<div class="empty">No new articles today</div>')

    # ── Build earnings tables (upcoming + past) ──
    past_earnings = db.get_past_earnings(within_days=365)

    def _build_earnings_rows(items, is_past=False):
        rows = []
        for e in items:
            tk = e.get("ticker", "")
            sname = stock_map.get(tk, {}).get("name", tk)
            ex = display_ex(e.get("exchange", ""))
            rdate = e.get("report_date", "TBD")
            period = _esc(e.get("fiscal_period", ""))
            src = _esc(e.get("source_url", ""))
            try:
                dt = datetime.strptime(rdate, "%Y-%m-%d").date()
                days = (dt - datetime.now().date()).days
                badge_cls = _countdown_class(days)
                if days < 0:
                    badge_text = f"{abs(days)}d ago"
                elif days == 0:
                    badge_text = "TODAY"
                else:
                    badge_text = f"in {days}d"
            except ValueError:
                badge_cls = "badge-past"
                badge_text = "TBD"

            # For past reports, show period as a link if source_url exists
            if is_past and src:
                period_cell = f"<a href='{src}' target='_blank' style='color:var(--accent);text-decoration:none'>{period or 'View report'} ↗</a>"
            else:
                period_cell = period

            rows.append(f"""
            <tr data-exchange="{_esc(ex)}" data-ticker="{_esc(tk)}">
                <td><strong>{_esc(sname)}</strong> <span style="color:var(--text-muted)">({_esc(tk)})</span></td>
                <td>{_esc(ex)}</td>
                <td style="white-space:nowrap">{_esc(_fmt_date_compact(rdate))}</td>
                <td><span class="badge {badge_cls}">{badge_text}</span></td>
                <td>{period_cell}</td>
                {"<td><a href='" + src + "' target='_blank' style='color:var(--accent);text-decoration:none'>↗</a></td>" if src and not is_past else "<td>—</td>" if not is_past else ""}
            </tr>""")
        return rows

    upcoming_rows = _build_earnings_rows(earnings, is_past=False)
    past_rows = _build_earnings_rows(past_earnings, is_past=True)

    if not upcoming_rows:
        upcoming_table = '<div class="empty">No upcoming earnings dates found.</div>'
    else:
        upcoming_table = f"""
        <table class="earnings-table">
            <thead><tr><th>Stock</th><th>Exchange</th><th>Report Date</th><th>Countdown</th><th>Period</th><th>Source</th></tr></thead>
            <tbody>{''.join(upcoming_rows)}</tbody>
        </table>"""

    if not past_rows:
        past_table = '<div class="empty">No past earnings reports found.</div>'
    else:
        past_table = f"""
        <table class="earnings-table">
            <thead><tr><th>Stock</th><th>Exchange</th><th>Report Date</th><th>When</th><th>Report</th></tr></thead>
            <tbody>{''.join(past_rows)}</tbody>
        </table>"""

    earnings_section = f"""
        <div class="stock-filters">
            <span class="stock-pill active" id="earnings-upcoming-tab" onclick="toggleEarnings('upcoming')">📅 Upcoming ({len(upcoming_rows)})</span>
            <span class="stock-pill" id="earnings-past-tab" onclick="toggleEarnings('past')">📋 Past Reports ({len(past_rows)})</span>
        </div>
        <div class="earnings-body">
            <div id="earnings-upcoming">{upcoming_table}</div>
            <div id="earnings-past" style="display:none">{past_table}</div>
        </div>"""

    # ── Build insider transactions section (most recent first) ──
    # Filter to only genuine insider/director transaction items.
    # Use multi-word phrases to avoid matching general business articles.
    # These must appear in the TITLE (not snippet) to be strict.
    _INSIDER_TITLE_SIGNALS = [
        # Director / insider trade terminology
        "director's interest", "director interest", "director dealing",
        "insider trad", "insider buy", "insider sell", "insider transaction",
        "insider report", "insider move", "insider acqui", "insider activity",
        "insider ups holding", "insider trading",
        # SEC filings
        "form 4", "form 3", "form 144", "sec filing",
        # Shareholding / ownership changes
        "substantial shareholder", "shareholding change",
        "share acquisition", "share disposal", "share buyback",
        "disclosure of interest", "changes in interest",  # SGX
        "s-hldr", "person ceasing", "section 138", "section 139",  # KLSE Bursa
        "director acqui", "director purchase", "director report",
        # Ownership / stake moves
        "raises stock holding", "stock holding", "management holds",
        "boosts ownership", "ups holding", "stake",
        "proposed sale of", "purchase of share",
        # French
        "opération d'initié", "transaction directeur",
    ]

    # Sources that are regulatory feeds — every row is definitively
    # an insider transaction, so skip the keyword whitelist.
    _TRUSTED_INSIDER_SOURCES = {
        "sec edgar", "finansinspektionen", "klse screener",
    }

    def _is_insider_item(item: dict) -> bool:
        pub = item.get("published", "").strip()
        if not pub:
            return False
        source_lower = (item.get("source", "") or "").lower()
        if source_lower in _TRUSTED_INSIDER_SOURCES:
            return True
        title_lower = item.get("title", "").lower()
        return any(kw in title_lower for kw in _INSIDER_TITLE_SIGNALS)

    insiders_filtered = [i for i in insiders if _is_insider_item(i)]
    insiders_sorted = sorted(insiders_filtered,
                             key=lambda i: _normalize_date(i.get("published", "")),
                             reverse=True)

    # Group by display exchange for collapsible sections
    insider_by_ex: dict[str, list] = {}
    for ins in insiders_sorted:
        ex = display_ex(stock_map.get(ins.get("ticker", ""), {}).get("exchange", "Other"))
        insider_by_ex.setdefault(ex, []).append(ins)

    insider_groups_html = []
    for ex in sorted(insider_by_ex.keys()):
        items = insider_by_ex[ex]
        cards = []
        for ins in items:
            tk = ins.get("ticker", "")
            sname = stock_map.get(tk, {}).get("name", tk)
            title = _esc(_strip_html(ins.get("title", "")))
            url = _esc(ins.get("url", "#"))
            snippet = _esc(_strip_html(ins.get("snippet", "")))[:200]
            source = _esc(ins.get("source", ""))
            pub = _esc(ins.get("published", ""))

            cards.append(f"""
            <div class="news-card" data-exchange="{_esc(ex)}" data-ticker="{_esc(tk)}">
                <div class="news-stock">{_esc(sname)} ({_esc(tk)})</div>
                <div class="news-title"><a href="{url}" target="_blank">{title}</a></div>
                {"<div class='news-snippet'>" + snippet + "</div>" if snippet else ""}
                <div class="news-meta">{source}{(' · ' + pub) if pub else ''}</div>
            </div>""")

        insider_groups_html.append(f"""
        <div class="exchange-group" data-exchange="{_esc(ex)}">
            <div class="exchange-header">
                {_esc(ex)} <span style="font-weight:400;color:var(--text-muted)">({len(items)})</span>
                <span class="chevron">▼</span>
            </div>
            <div class="exchange-body">{''.join(cards)}</div>
        </div>""")

    if not insider_groups_html:
        insider_groups_html.append('<div class="empty">No insider transactions found</div>')

    # ── Build forum section (most recent first, all entries) ──
    forum_sorted = sorted(forum, key=lambda f: _normalize_date(f.get("posted_at", "")), reverse=True)

    # Group by forum source
    forum_by_src: dict[str, list] = {}
    for f in forum_sorted:
        fname = f.get("forum", "other")
        forum_by_src.setdefault(fname, []).append(f)

    # Forum is full-width under News, so it can show more cards at
    # once — bumped from 8 to 16.
    FORUM_INITIAL_LIMIT = 16
    # Count distinct display exchanges represented in the forum feed
    # (not distinct forum sources) to match the news threshold semantic.
    forum_exchanges = set()
    for f in forum_sorted:
        tk = f.get("ticker", "")
        forum_exchanges.add(display_ex(stock_map.get(tk, {}).get("exchange", "")))
    flat_forum = len(forum_exchanges) > FLAT_THRESHOLD

    forum_global_idx = 0
    forum_total = sum(len(v) for v in forum_by_src.values())
    forum_cards_html = []

    def _forum_card_html(f, fname, idx_ref) -> str:
        tk = f.get("ticker", "")
        internal_ex = stock_map.get(tk, {}).get("exchange", "")
        display_label = display_ex(internal_ex)
        sname = stock_map.get(tk, {}).get("name", tk)
        author = _esc(f.get("author", "")) or "Anonymous"
        text = _esc(_strip_html(f.get("text", "")))[:300]
        post_url = _esc(f.get("post_url", ""))
        posted_at = _esc(f.get("posted_at", ""))
        lang_badge = '<span class="lang-badge">🇫🇷 FR</span>' if f.get("lang") == "fr" else ""
        ex_badge = ex_badge_html(internal_ex, display_label) if display_label else ""

        is_collapsed = idx_ref[0] >= FORUM_INITIAL_LIMIT
        hidden_cls = " collapsed-hidden" if is_collapsed else ""
        collapsed_attr = ' data-collapsed="1"' if is_collapsed else ""
        idx_ref[0] += 1

        return f"""
        <div class="forum-card{hidden_cls}"{collapsed_attr} data-exchange="{_esc(display_label)}" data-ticker="{_esc(tk)}">
            <div class="forum-header">
                <div class="forum-stock">{ex_badge} {_esc(sname)} ({_esc(tk)}) {lang_badge}</div>
                <div class="forum-author">{author}</div>
            </div>
            <div class="forum-text">{text}</div>
            <div class="forum-source">
                {"<span class='alert-date'>📅 " + posted_at + "</span> " if posted_at else ""}
                {"<a href='" + post_url + "' target='_blank'>View on " + _esc(fname) + " ↗</a>" if post_url else ""}
            </div>
        </div>"""

    idx_ref_f = [0]
    if flat_forum:
        # Flat chronological stream, newest first
        for f in forum_sorted:
            fname = f.get("forum", "other")
            forum_cards_html.append(_forum_card_html(f, fname, idx_ref_f))
    else:
        # Grouped by forum source (original behavior — useful for narrow filters)
        for fname in sorted(forum_by_src.keys()):
            items = forum_by_src[fname]  # already sorted newest first
            cards = [_forum_card_html(f, fname, idx_ref_f) for f in items]
            forum_cards_html.append(f"""
            <div class="exchange-group">
                <div class="exchange-header">
                    {_esc(fname)} <span style="font-weight:400;color:var(--text-muted)">({len(items)})</span>
                    <span class="chevron">▼</span>
                </div>
                <div class="exchange-body">{''.join(cards)}</div>
            </div>""")

    if forum_total > FORUM_INITIAL_LIMIT:
        forum_cards_html.append(
            f'<button class="show-more-btn" id="forum-show-more" '
            f'onclick="expandSection(this, \'#forum-section\')">'
            f'▼ Show more</button>')

    if not forum_cards_html:
        forum_cards_html.append('<div class="empty">No forum mentions today</div>')

    # ── Filter pills ──
    # Top bar 1: exchange filter (country labels)
    pills = [f'<span class="filter-pill active" data-exchange="ALL">All</span>']
    for ex in exchanges:
        pills.append(f'<span class="filter-pill" data-exchange="{_esc(ex)}">{_esc(ex)}</span>')
    pills_html = "".join(pills)

    # Top bar 2: global stock selector — one unified control that filters
    # News / Earnings / Forum / Insider sections at once. Each pill carries
    # its display exchange so the exchange filter above can hide non-matching
    # tickers. Sorted by display-exchange then name for a stable order.
    _sorted_stocks = sorted(
        active_stocks,
        key=lambda s: (s.get("_display_ex", ""), s.get("name", "")),
    )
    stock_pill_items = ['<span class="stock-pill active" data-ticker="ALL">All stocks</span>']
    for _s in _sorted_stocks:
        _tk = _s.get("ticker", "")
        _tex = _s.get("_display_ex", "")
        _sname = _s.get("name", _tk)
        stock_pill_items.append(
            f'<span class="stock-pill" data-ticker="{_esc(_tk)}" '
            f'data-exchange="{_esc(_tex)}" title="{_esc(_sname)}">{_esc(_tk)}</span>'
        )
    stock_pills_html = "".join(stock_pill_items)

    # ── Logo ──
    logo_uri = _load_logo_b64()
    logo_img = f'<img src="{logo_uri}" alt="Emerging Edge" class="header-logo">' if logo_uri else ""

    # ── Assemble full HTML ──
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta name="theme-color" content="#0f1117">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
<link rel="manifest" href="/manifest.json">
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><text y='.9em' font-size='90'>🌍</text></svg>">
<title>Emerging Edge — {_esc(target_date)}</title>
<style>{CSS}</style>
</head>
<body>

<!-- ═══════════ Header ═══════════ -->
<div class="header">
    <div class="header-inner">
        <div class="header-brand">
            {logo_img}
            <h1><span>Emerging Edge</span> Monitor</h1>
        </div>
        <div class="header-nav">
            <span class="solid-btn" onclick="openAddStockModal()">➕ Add Stock</span>
            <a href="/portfolio">Portfolio</a>
            <a href="/engine-room">⚙ Engine Room</a>
        </div>
        <div class="header-kpis">
            <span class="kpi"><span class="kpi-val">{total_stocks}</span>Stocks</span>
            <span class="kpi-sep">·</span>
            <a class="kpi" href="#news-section"><span class="kpi-val">{len(news)}</span>News</a>
            <span class="kpi-sep">·</span>
            <a class="kpi" href="#alerts-section"><span class="kpi-val">{len(alert_all)}</span>Alerts</a>
            <span class="kpi-sep">·</span>
            <a class="kpi" href="#earnings-section"><span class="kpi-val">{len(earnings)}</span>Earnings</a>
            <span class="kpi-sep">·</span>
            <a class="kpi" href="#insider-section"><span class="kpi-val">{len(insiders_sorted)}</span>Insider</a>
            <span class="kpi-sep">·</span>
            <a class="kpi" href="#forum-section"><span class="kpi-val">{len(forum)}</span>Forum</a>
            <button class="price-refresh-btn" id="price-refresh-btn" onclick="refreshPrices()" title="Refresh stock prices">
                <span class="mini-spinner"></span> ↻ Prices
            </button>
        </div>
        <div class="fx-box">{fx_bar_html}</div>
    </div>
    <div class="filter-row">
        <div class="filter-group">
            <span class="filter-group-label">Exchange</span>
            {pills_html}
        </div>
        <div class="filter-group stocks">
            <span class="filter-group-label">Stock</span>
            {stock_pills_html}
        </div>
    </div>
</div>

<div class="stock-layout-toggle">
    <div class="stock-layout-toggle-inner">
        <button type="button" id="stocks-collapse-btn" class="stocks-collapse-btn"
                onclick="toggleStocksCollapsed()" title="Collapse / expand the stock grid">
            <span id="stocks-collapse-icon">▼</span>
            <span id="stocks-collapse-label">Stocks</span>
            <span id="density-count-hint" class="density-count-hint"></span>
        </button>
        <span id="stocks-summary-strip" class="stocks-summary-strip" style="display:none;"></span>
        <span id="selected-stock-chip" class="selected-stock-chip" style="display:none;"></span>
        <span class="stock-layout-toggle-spacer"></span>
        <button type="button" class="panels-bulk-btn" id="panels-collapse-all"
                onclick="setAllPanelsCollapsed(true)" title="Collapse all country panels">
            ▲ Collapse all
        </button>
        <button type="button" class="panels-bulk-btn" id="panels-expand-all"
                onclick="setAllPanelsCollapsed(false)" title="Expand all country panels">
            ▼ Expand all
        </button>
        <label class="stl-label">
            <input type="checkbox" id="group-by-exchange" checked onchange="toggleStockLayout(this.checked)">
            Group by exchange
        </label>
        <span class="stl-label">
            Density:
            <span class="density-pills" role="tablist">
                <button type="button" class="density-pill" data-density="chip"  onclick="setDensity('chip')">Chips</button>
                <button type="button" class="density-pill" data-density="line"  onclick="setDensity('line')">Lines</button>
                <button type="button" class="density-pill" data-density="mini"  onclick="setDensity('mini')">Mini</button>
            </span>
        </span>
    </div>
</div>
<div id="stock-panels-wrapper">
{''.join(stock_panels_html)}
</div>

<div class="gen-time">Generated {_esc(gen_time)} · Date: {_esc(target_date)}</div>

<!-- ═══════════ Dashboard Grid ═══════════ -->
<div class="container">

    <!-- 🚨 Alerts -->
    <div class="section{' empty' if len(alert_all) == 0 else ''}" id="alerts-section">
        <div class="section-title">
            <span class="icon">🚨</span> Alerts
            <span class="section-count">{len(alert_all)}</span>
        </div>
        {alert_html_str}
    </div>

    <!-- 📰 News -->
    <div class="section{' empty' if len(news) == 0 else ''}" id="news-section">
        <div class="section-title">
            <span class="icon">📰</span> News
            <span class="section-count">{len(news)}</span>
            <span id="news-subtitle" class="section-hint">(last 3 months — select a stock to see older items)</span>
            <span id="news-extend-toggle" class="news-extend-btn" style="display:none;margin-left:0.5rem" onclick="toggleNewsExtended()">📅 Show 10y</span>
        </div>
        {''.join(news_cards_html)}
    </div>

    <!-- 📅 Earnings Calendar -->
    <div class="section{' empty' if len(earnings) == 0 and len(past_earnings) == 0 else ''}" id="earnings-section">
        <div class="section-title">
            <span class="icon">📅</span> Earnings Calendar
            <span class="section-count">{len(earnings)}</span>
        </div>
        {earnings_section}
    </div>

    <!-- 🔔 Insider Transactions -->
    <div class="section{' empty' if len(insiders_sorted) == 0 else ''}" id="insider-section">
        <div class="section-title">
            <span class="icon">🔔</span> Insider Transactions
            <span class="section-count">{len(insiders_sorted)}</span>
            <span class="section-hint">(12 months)</span>
        </div>
        {''.join(insider_groups_html)}
    </div>

    <!-- 💬 Forum Buzz -->
    <div class="section{' empty' if len(forum) == 0 else ''}" id="forum-section">
        <div class="section-title">
            <span class="icon">💬</span> Forum Buzz
            <span class="section-count">{len(forum)}</span>
            <span class="section-hint">(12 months)</span>
        </div>
        {''.join(forum_cards_html)}
    </div>

</div>

<div class="gen-time">Emerging Edge v1.0 · {total_stocks} stocks tracked across {len(exchanges)} exchanges</div>

<!-- Refresh button (works when served via 'python monitor.py serve') -->
<div class="refresh-bar">
    <div class="refresh-progress" id="refresh-progress">
        <div class="refresh-progress-bar"><div class="refresh-progress-fill" id="progress-fill"></div></div>
        <div class="refresh-progress-text">
            <span class="refresh-progress-step" id="progress-step"></span>
            <span id="progress-count"></span>
        </div>
        <div class="refresh-progress-error" id="progress-error"></div>
    </div>
    <span class="refresh-status" id="refresh-status">Last: {_esc(gen_time)}</span>
    <button class="refresh-btn refresh-btn-free" id="refresh-btn-free"
            onclick="doRefresh('free')"
            title="Refresh prices, SEC insiders, Yahoo news and page scrapes. Free — no Serper credits used.">
        <span class="spinner"></span>
        🆓 Free refresh
    </button>
    <button class="refresh-btn refresh-btn-full" id="refresh-btn-full"
            onclick="doRefresh('full')"
            {'' if _serper_key_set else 'disabled data-needs-key="1" title="Add a Serper API key in the Engine Room to enable full refresh"'}
            {'title="Refresh everything above + Serper news, forums, contracts. Uses Serper API credits."' if _serper_key_set else ''}>
        <span class="spinner"></span>
        💳 Full refresh
    </button>
</div>

<!-- Add Stock modal -->
<div id="add-stock-modal" class="add-stock-overlay" style="display:none" onclick="if (event.target===this) closeAddStockModal()">
    <div class="add-stock-card">
        <div class="add-stock-header">
            <h3 style="margin:0">Add Stock to Watchlist</h3>
            <span class="add-stock-close" onclick="closeAddStockModal()">✕</span>
        </div>
        <input type="text" id="add-stock-search" placeholder="Type a company name or ticker (e.g. 'matrix', 'millicom', 'wema bank')" autocomplete="off" oninput="onAddStockSearch(this.value)">
        <div id="add-stock-results" class="add-stock-results"></div>
    </div>
</div>

<script>
{JS}

// ── Refresh button logic with progress tracking ──
const STEP_LABELS = {{
    'starting': 'Starting…',
    'news': '📰 News',
    'contracts': '📋 Contracts',
    'earnings': '📅 Earnings',
    'forums': '💬 Forums',
    'prices': '💰 Prices',
    'insiders': '🔔 Insiders',
    'generating': '📝 Generating dashboard',
    'done': '✅ Complete',
}};
const STEP_FREE_HINT = {{
    'news':     '(Yahoo RSS only)',
    'earnings': '(page scrape only)',
    'forums':   '(i3investor / richbourse only)',
    'prices':   '',
    'insiders': '(SEC EDGAR / KLSE Screener only)',
    'generating': '',
    'starting': '',
    'done': '',
}};

let refreshTimeout = null;

function showProgress(visible) {{
    document.getElementById('refresh-progress').classList.toggle('visible', visible);
}}

function updateProgress(prog, mode) {{
    if (!prog) return;
    const fill = document.getElementById('progress-fill');
    const step = document.getElementById('progress-step');
    const count = document.getElementById('progress-count');
    const error = document.getElementById('progress-error');

    const pct = prog.total > 0 ? Math.round((prog.done / prog.total) * 100) : 0;
    fill.style.width = pct + '%';

    const label = STEP_LABELS[prog.step] || prog.step;
    const modeBadge = mode === 'free'
        ? '🆓 FREE — '
        : (mode === 'full' ? '💳 FULL — ' : '');
    const hint = mode === 'free' ? (STEP_FREE_HINT[prog.step] || '') : '';
    let text = modeBadge + label;
    if (prog.ticker) text += ' · ' + prog.ticker;
    if (hint) text += ' ' + hint;
    step.textContent = text;
    count.textContent = prog.done + ' / ' + prog.total + ' stocks';

    if (prog.error) {{
        error.textContent = '❌ ' + prog.error;
        error.style.display = 'block';
    }} else {{
        error.style.display = 'none';
    }}
}}

function _refreshButtons() {{
    return [
        document.getElementById('refresh-btn-free'),
        document.getElementById('refresh-btn-full'),
    ].filter(Boolean);
}}

// Current refresh mode (set when doRefresh is called, read by pollRefresh)
let _currentRefreshMode = '';

function doRefresh(mode) {{
    mode = mode || 'free';
    _currentRefreshMode = mode;
    const btns = _refreshButtons();
    const status = document.getElementById('refresh-status');
    const activeId = mode === 'full' ? 'refresh-btn-full' : 'refresh-btn-free';
    const activeBtn = document.getElementById(activeId);
    // Disable BOTH buttons so the user can't fire a second refresh, but
    // only the clicked one gets the `.busy` class (which drives the
    // spinner + "Refreshing…" label). The other just goes greyed-out.
    btns.forEach(b => {{
        b.disabled = true;
        b.classList.remove('busy');
    }});
    if (activeBtn) {{
        activeBtn.classList.add('busy');
        const label = mode === 'full' ? '💳 Refreshing (Serper)…' : '🆓 Refreshing (no Serper)…';
        activeBtn.innerHTML = '<span class="spinner"></span> ' + label;
    }}
    status.textContent = '';
    showProgress(true);
    updateProgress({{ step: 'starting', ticker: '', done: 0, total: 0, error: '' }}, mode);

    fetch('/api/refresh', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify({{ mode: mode }})
    }})
        .then(r => r.json())
        .then(data => {{
            if (data.status === 'started' || data.status === 'busy') {{
                pollRefresh();
            }}
        }})
        .catch(err => {{
            btns.forEach(b => b.classList.remove('busy'));
            const free = document.getElementById('refresh-btn-free');
            const full = document.getElementById('refresh-btn-full');
            if (free) free.innerHTML = '<span class="spinner"></span> 🆓 Free refresh';
            if (full) full.innerHTML = '<span class="spinner"></span> 💳 Full refresh';
            showProgress(false);
            status.textContent = 'Refresh unavailable (use: python monitor.py serve)';
        }});
}}

function _resetRefreshButtons(finalLabelFree, finalLabelFull) {{
    const btns = _refreshButtons();
    btns.forEach(b => {{
        b.classList.remove('busy');
        b.disabled = false;
    }});
    const free = document.getElementById('refresh-btn-free');
    const full = document.getElementById('refresh-btn-full');
    if (free) free.innerHTML = '<span class="spinner"></span> ' + (finalLabelFree || '🆓 Free refresh');
    if (full) full.innerHTML = '<span class="spinner"></span> ' + (finalLabelFull || '💳 Full refresh');
    // Full refresh button should remain disabled if no Serper key is set.
    if (full && full.hasAttribute('data-needs-key')) full.disabled = true;
}}

function pollRefresh() {{
    const status = document.getElementById('refresh-status');
    let stuckCount = 0;
    let lastDone = -1;

    // Timeout: if no progress for 60s, show error
    const STUCK_LIMIT = 30;  // 30 polls × 2s = 60s

    const poll = setInterval(() => {{
        fetch('/api/status')
            .then(r => r.json())
            .then(data => {{
                const mode = data.refresh_mode || _currentRefreshMode;
                updateProgress(data.progress, mode);

                // Check for stuck state
                if (data.progress && data.progress.done === lastDone && data.refreshing) {{
                    stuckCount++;
                    if (stuckCount >= STUCK_LIMIT) {{
                        clearInterval(poll);
                        document.getElementById('progress-error').textContent =
                            '⚠️ Refresh appears stuck on ' + (data.progress.ticker || data.progress.step) + '. Try reloading the page.';
                        document.getElementById('progress-error').style.display = 'block';
                        _resetRefreshButtons();
                        return;
                    }}
                }} else {{
                    stuckCount = 0;
                    lastDone = data.progress ? data.progress.done : -1;
                }}

                // Check for server-side error
                if (data.progress && data.progress.error) {{
                    clearInterval(poll);
                    _resetRefreshButtons();
                    return;
                }}

                if (!data.refreshing) {{
                    clearInterval(poll);
                    status.textContent = 'Last: ' + data.last_refresh;
                    _resetRefreshButtons('✅ Done! Reloading…', '✅ Done! Reloading…');
                    setTimeout(() => {{
                        showProgress(false);
                        location.reload();
                    }}, 800);
                }}
            }})
            .catch(() => {{
                clearInterval(poll);
                document.getElementById('progress-error').textContent = '❌ Lost connection to server';
                document.getElementById('progress-error').style.display = 'block';
                _resetRefreshButtons();
            }});
    }}, 2000);
}}

// ── Price-only refresh (header button) ──
// Sends the active exchange filter so only those stocks are refreshed.
// Preserves exchange selection across reload via URL hash.
function getActiveExchanges() {{
    const pills = document.querySelectorAll('.filter-pill.active:not([data-exchange="ALL"])');
    return [...pills].map(p => p.dataset.exchange);
}}

function refreshPrices() {{
    const btn = document.getElementById('price-refresh-btn');
    const status = document.getElementById('refresh-status');
    btn.classList.add('busy');
    btn.innerHTML = '<span class="mini-spinner"></span> Updating...';
    showProgress(true);

    const actives = getActiveExchanges();
    const body = actives.length === 1 ? JSON.stringify({{ exchange: actives[0] }}) : '{{}}';

    fetch('/api/refresh-prices', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: body
    }})
        .then(r => r.json())
        .then(data => {{
            if (data.status === 'started' || data.status === 'busy') {{
                status.textContent = '';
                const poll = setInterval(() => {{
                    fetch('/api/status')
                        .then(r => r.json())
                        .then(d => {{
                            updateProgress(d.progress);
                            if (!d.refreshing) {{
                                clearInterval(poll);
                                btn.classList.remove('busy');
                                btn.innerHTML = '✅ Done';
                                status.textContent = 'Last: ' + d.last_refresh;
                                if (actives.length > 0) {{
                                    window.location.hash = 'ex=' + actives.join(',');
                                }}
                                setTimeout(() => {{ showProgress(false); location.reload(); }}, 600);
                            }}
                        }})
                        .catch(() => {{
                            clearInterval(poll);
                            document.getElementById('progress-error').textContent = '❌ Lost connection';
                            document.getElementById('progress-error').style.display = 'block';
                            btn.classList.remove('busy');
                            btn.innerHTML = '<span class="mini-spinner"></span> Refresh Prices';
                        }});
                }}, 1500);
            }}
        }})
        .catch(() => {{
            btn.classList.remove('busy');
            btn.innerHTML = '<span class="mini-spinner"></span> Refresh Prices';
            showProgress(false);
        }});
}}

// ── Restore exchange selection from URL hash on page load ──
(function restoreExchange() {{
    const hash = window.location.hash;
    if (!hash.startsWith('#ex=')) return;
    const exchanges = hash.slice(4).split(',');
    if (!exchanges.length) return;
    // Click the matching exchange pills
    document.querySelector('.filter-pill[data-exchange="ALL"]').classList.remove('active');
    exchanges.forEach(ex => {{
        const pill = document.querySelector('.filter-pill[data-exchange="' + ex + '"]');
        if (pill) pill.click();
    }});
    // Clear hash so it doesn't persist on manual navigation
    history.replaceState(null, '', window.location.pathname);
}})();

// Restore stock layout preference from localStorage
(function() {{
    const saved = localStorage.getItem('ee-stock-grouped');
    if (saved === '0') {{
        const cb = document.getElementById('group-by-exchange');
        if (cb) {{ cb.checked = false; toggleStockLayout(false, true); }}
    }}
}})();
</script>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def save_html(db: Database, config: dict, target_date: str = None) -> str:
    """Generate and write the HTML dashboard. Returns the file path."""
    if target_date is None:
        target_date = datetime.utcnow().strftime("%Y-%m-%d")

    digest_dir = config.get("digest_dir", "./digests")
    os.makedirs(digest_dir, exist_ok=True)

    content = generate_html(db, config, target_date)
    filepath = os.path.join(digest_dir, f"daily_{target_date}.html")

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content)

    return filepath


def open_html(filepath: str):
    """Open the HTML file in the default browser."""
    webbrowser.open(f"file://{os.path.abspath(filepath)}")
