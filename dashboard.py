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
    # Relative dates like "5 days ago"
    import re as _re
    m = _re.match(r"(\d+)\s+(day|week|month|year)s?\s+ago", s, _re.I)
    if m:
        n = int(m.group(1))
        unit = m.group(2).lower()
        days_back = {"day": 1, "week": 7, "month": 30, "year": 365}.get(unit, 1) * n
        return int((datetime.now() - timedelta(days=days_back)).timestamp())
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
    max-width: 1400px; margin: 0.75rem auto 0;
    padding: 0 2rem;
    display: block;
}
.exchange-status-bar {
    display: flex; flex-wrap: wrap; gap: 0.4rem 1rem;
}
.exchange-status-bar .exchange-status:empty { display: none; }
.stock-panel-inner {
    display: flex; gap: 0.6rem; flex-wrap: wrap;
    padding: 0.75rem 0;
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
.stock-chip.filtered-out { display: none; }
.stock-chip:hover { border-color: var(--accent-dim); }
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
#news-section {
    display: flex; flex-direction: column;
    max-height: 54rem;
}
#news-section > :not(.section-title) {
    min-height: 0; overflow-y: auto;
    padding-right: 0.3rem; margin-right: -0.3rem;
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
    max-height: 48rem;
}
#forum-section > :not(.section-title) {
    min-height: 0; overflow-y: auto;
    padding-right: 0.3rem; margin-right: -0.3rem;
}
#forum-section .exchange-body {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(340px, 1fr));
    gap: 0.5rem 1rem;
}
#forum-section .forum-card { margin-bottom: 0; }

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
    display: flex; align-items: center; gap: 0.4rem;
}
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
    font-size: 0.78rem; color: var(--text);
    display: -webkit-box; -webkit-line-clamp: 3; -webkit-box-orient: vertical; overflow: hidden;
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
const _sectionUserExpanded = { news: false, forum: false };

// "Show more" toggle for collapsed sections (News, Forum). Reveals all
// cards that were initially truncated and remembers the intent so
// filter changes won't re-collapse them.
function expandSection(btn, sectionSelector) {
    if (sectionSelector.indexOf('news') >= 0)  _sectionUserExpanded.news = true;
    if (sectionSelector.indexOf('forum') >= 0) _sectionUserExpanded.forum = true;
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
    const filtersActive = _filtersActive();
    [
        { sel: '#news-section',  key: 'news',  btn: 'news-show-more',  card: '.news-card' },
        { sel: '#forum-section', key: 'forum', btn: 'forum-show-more', card: '.forum-card' },
    ].forEach(({ sel, key, btn: btnId, card: cardSel }) => {
        const userExpanded = _sectionUserExpanded[key];
        const cards = document.querySelectorAll(sel + ' ' + cardSel);
        let visibleCount = 0;
        let hiddenByCollapse = 0;

        cards.forEach(el => {
            // Is this card hidden by the exchange/stock filter?
            const filteredOut = el.style.display === 'none'
                || el.classList.contains('stock-hidden');
            if (filteredOut) {
                // Don't touch collapsed state — card is already invisible
                return;
            }
            visibleCount++;
            if (userExpanded) {
                el.classList.remove('collapsed-hidden');
            } else if (visibleCount > _SECTION_VISIBLE_LIMIT) {
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
                btn.textContent = '\u25BC Show more';
            } else {
                btn.style.display = 'none';
            }
        }
    });
}

function applyNewsAgeFilter() {
    const newsSection = document.getElementById('news-section');
    if (!newsSection) return;
    const cards = newsSection.querySelectorAll('.news-card');
    const nowSec = Math.floor(Date.now() / 1000);

    // Single stock selected globally? (if so, allow 10y extension)
    const singleStock = (typeof activeTickers !== 'undefined' && activeTickers.size === 1)
        ? [...activeTickers][0]
        : null;

    // Any filter active? If so, don't apply the default 3-month window
    // — the user has explicitly narrowed the scope and wants to see
    // everything that matches, no matter how old.
    const anyFilter = (typeof _filtersActive === 'function') && _filtersActive();

    cards.forEach(c => {
        const epoch = parseInt(c.dataset.pubEpoch || '0', 10);
        // Items with no published date (epoch=0): always show — they're
        // usually fresh items where the publisher didn't include a date.
        if (epoch === 0) {
            c.classList.remove('news-old');
            return;
        }
        if (anyFilter) {
            // Any filter active: show everything in-scope regardless of age
            c.classList.remove('news-old');
            return;
        }
        const ageS = nowSec - epoch;
        const ext = newsExtendedMode && singleStock && c.dataset.ticker === singleStock;
        const limit = ext ? NEWS_EXTENDED_WINDOW_S : NEWS_DEFAULT_WINDOW_S;
        c.classList.toggle('news-old', ageS > limit);
    });

    // Show/hide the 10y toggle button based on whether a single stock is filtered
    const toggleBtn = document.getElementById('news-extend-toggle');
    if (toggleBtn) {
        toggleBtn.style.display = singleStock ? 'inline-block' : 'none';
        toggleBtn.textContent = newsExtendedMode ? '📅 Last 3 months' : '📅 Show 10y';
        toggleBtn.classList.toggle('active', newsExtendedMode);
    }

    // Update the subtitle to reflect what window is ACTUALLY being
    // shown. With no filter active we default to the last 3 months.
    // Any filter active (exchange pill, stock pill, multi-select)
    // relaxes the age filter and shows everything in-scope — label
    // it as "all dates".
    const subtitle = document.getElementById('news-subtitle');
    if (subtitle) {
        if (singleStock && newsExtendedMode) {
            subtitle.textContent = '(last 10 years)';
        } else if (anyFilter) {
            subtitle.textContent = '(all dates for current filter)';
        } else {
            subtitle.textContent = '(last 3 months — select a stock above to extend)';
        }
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
    let html = '';
    for (const r of results) {
        const source_badge = r.source === 'catalog'
            ? '<span class="add-stock-result-badge" style="color:var(--green);border-color:var(--green)">FRONTIER</span>'
            : '';
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
        alert('Failed to parse result: ' + e);
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
            closeAddStockModal();
            location.reload();
        } else {
            alert('Error: ' + (resp.message || 'failed'));
        }
    })
    .catch(err => alert('Failed: ' + err));
}

// ── Remove a stock from the watchlist (called from the chip ✕ button) ──
function removeStockFromWatchlist(ticker, exchange, name) {
    if (!confirm('Remove ' + (name || ticker) + ' from your watchlist?\\n\\n' +
                 'Existing portfolio transactions will NOT be deleted, but the ' +
                 'stock will no longer appear on the monitor unless you re-add it.')) {
        return;
    }
    fetch('/api/watchlist/remove', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ticker: ticker, exchange: exchange }),
    })
    .then(r => r.json())
    .then(resp => {
        if (resp.status === 'ok') {
            location.reload();
        } else {
            alert('Error: ' + (resp.message || 'failed'));
        }
    })
    .catch(err => alert('Failed: ' + err));
}

// ── Exchange trading hours (IANA timezone, open/close in local exchange time) ──
// Keys are the user-facing display names that match data-exchange attributes
// on stock panels and filter pills. 'US' covers NASDAQ + NYSE + AMEX.
const EXCHANGE_HOURS = {
    'Malaysia':         { tz: 'Asia/Kuala_Lumpur',   open: '09:00', close: '17:00', days: [1,2,3,4,5], name: 'Bursa Malaysia' },
    'Nigeria':          { tz: 'Africa/Lagos',        open: '09:30', close: '14:30', days: [1,2,3,4,5], name: 'Nigerian Exchange' },
    'BRVM/Ivory Coast': { tz: 'Africa/Abidjan',      open: '09:00', close: '15:30', days: [1,2,3,4,5], name: "BRVM" },
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
        // Restore panels and status bars
        panels.forEach(p => {
            p.style.display = '';
            const st = p.querySelector('.exchange-status');
            if (st) st.style.display = '';
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
        // Hide exchange-status in flat mode
        first.querySelectorAll('.exchange-status').forEach(s => s.style.display = 'none');
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
        "BRVM":     "BRVM/Ivory Coast",
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
        "HKSE":     "Hong Kong",
        "ASX":      "Australia",
        "FRA":      "Germany",
        "TSX":      "Canada",
        "BMV":      "Mexico",
        "EURONEXT": "Euronext",
        "BIT":      "Italy",
        "OMX":      "Nordic",
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
    }
    def display_ex(code: str) -> str:
        return EXCHANGE_DISPLAY.get((code or "").upper(), code or "")

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

    stock_panels_html = []
    for ex in exchanges:
        ex_stocks = [s for s in active_stocks if s["_display_ex"] == ex]

        chips = []
        for s in ex_stocks:
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

            chips.append(f"""
            <div class="stock-chip" data-exchange="{_esc(ex)}">
                <span class="stock-chip-remove" title="Remove from watchlist"
                      onclick="removeStockFromWatchlist('{_esc(s['ticker'])}', '{_esc(s['exchange'])}', '{_esc(s['name'])}')">✕</span>
                <div class="stock-chip-name">{_esc(s['name'])}</div>
                <div class="stock-chip-ticker">{_esc(s['ticker'])} · {_esc(s.get('code',''))}</div>
                {price_line}
            </div>""")

        stock_panels_html.append(f"""
        <div class="stock-panel" data-exchange="{_esc(ex)}">
            <div class="exchange-status" id="exstatus-{ex_slug(ex)}"></div>
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
    news_sorted = sorted(news, key=lambda n: _normalize_date(n.get("published", "")), reverse=True)

    # Group by display exchange (e.g. NASDAQ + NYSE both → "US")
    news_by_ex: dict[str, list] = {}
    for n in news_sorted:
        ex = display_ex(n.get("exchange", "Other"))
        news_by_ex.setdefault(ex, []).append(n)

    # Initial render shows only the first N news items; the rest are
    # marked .collapsed-hidden and a "Show more" button reveals them.
    NEWS_INITIAL_LIMIT = 10
    news_global_idx = 0
    news_total = sum(len(v) for v in news_by_ex.values())
    news_cards_html = []
    for ex in sorted(news_by_ex.keys()):
        items = news_by_ex[ex]
        cards = []
        for n in items:
            tk = n.get("ticker", "")
            sname = stock_map.get(tk, {}).get("name", tk)
            title = _esc(_strip_html(n.get("title", "No title")))
            url = _esc(n.get("url", "#"))
            snippet = _esc(_strip_html(n.get("snippet", "")))[:200]
            source = _esc(n.get("source", ""))
            pub = _esc(n.get("published", ""))
            pub_epoch = _parse_news_epoch(n.get("published", ""))
            lang_badge = '<span class="lang-badge">🇫🇷 FR</span>' if n.get("lang") == "fr" else ""

            is_collapsed = news_global_idx >= NEWS_INITIAL_LIMIT
            hidden_cls = " collapsed-hidden" if is_collapsed else ""
            collapsed_attr = ' data-collapsed="1"' if is_collapsed else ""
            news_global_idx += 1

            cards.append(f"""
            <div class="news-card{hidden_cls}"{collapsed_attr} data-exchange="{_esc(ex)}" data-ticker="{_esc(tk)}" data-pub-epoch="{pub_epoch}">
                <div class="news-stock">{_esc(sname)} ({_esc(tk)}) {lang_badge}</div>
                <div class="news-title"><a href="{url}" target="_blank">{title}</a></div>
                {"<div class='news-snippet'>" + snippet + "</div>" if snippet else ""}
                <div class="news-meta">{source}{(' · ' + pub) if pub else ''}</div>
            </div>""")

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
    forum_global_idx = 0
    forum_total = sum(len(v) for v in forum_by_src.values())
    forum_cards_html = []
    for fname in sorted(forum_by_src.keys()):
        items = forum_by_src[fname]  # already sorted newest first
        cards = []
        for f in items:
            tk = f.get("ticker", "")
            sname = stock_map.get(tk, {}).get("name", tk)
            ex = display_ex(stock_map.get(tk, {}).get("exchange", ""))
            author = _esc(f.get("author", "")) or "Anonymous"
            text = _esc(f.get("text", ""))[:300]
            post_url = _esc(f.get("post_url", ""))
            posted_at = _esc(f.get("posted_at", ""))
            lang_badge = '<span class="lang-badge">🇫🇷 FR</span>' if f.get("lang") == "fr" else ""

            is_collapsed = forum_global_idx >= FORUM_INITIAL_LIMIT
            hidden_cls = " collapsed-hidden" if is_collapsed else ""
            collapsed_attr = ' data-collapsed="1"' if is_collapsed else ""
            forum_global_idx += 1

            cards.append(f"""
            <div class="forum-card{hidden_cls}"{collapsed_attr} data-exchange="{_esc(ex)}" data-ticker="{_esc(tk)}">
                <div class="forum-header">
                    <div class="forum-stock">{_esc(sname)} ({_esc(tk)}) {lang_badge}</div>
                    <div class="forum-author">{author}</div>
                </div>
                <div class="forum-text">{text}</div>
                <div class="forum-source">
                    {"<span class='alert-date'>📅 " + posted_at + "</span> " if posted_at else ""}
                    {"<a href='" + post_url + "' target='_blank'>View on " + _esc(fname) + " ↗</a>" if post_url else ""}
                </div>
            </div>""")

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

<div class="stock-layout-toggle" style="max-width:1400px;margin:0.5rem auto 0;padding:0 2rem;">
    <label style="font-size:0.72rem;color:var(--text-muted);cursor:pointer;display:inline-flex;align-items:center;gap:0.3rem;">
        <input type="checkbox" id="group-by-exchange" checked onchange="toggleStockLayout(this.checked)">
        Group by exchange
    </label>
</div>
{''.join(stock_panels_html)}

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
            <span id="news-subtitle" class="section-hint">(last 3 months — select a stock above to extend)</span>
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
