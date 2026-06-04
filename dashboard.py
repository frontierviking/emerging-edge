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
import json
import os
import webbrowser
from datetime import datetime, timedelta

from db import Database
from stock_search import has_price_source
from translate import (
    translate_to_english,
    cached_translation as _cached_translation,
    lang_flag as _lang_flag,
    get_skip_langs as _translate_skip_langs,
    detect_language as _detect_language,
)


def _translate_items_inplace(db, items: list[dict], fields: tuple[str, ...],
                              budget_s: float = 0.8,
                              max_workers: int = 12) -> None:
    """Mutate items in-place: translate each named field from item['lang']
    to English. Original values are preserved under '<field>_orig'.

    Cached items are resolved synchronously (instant). Items that need a
    fresh network call are dispatched to a thread pool, capped by a
    wall-clock ``budget_s`` so a cold cache never blocks page render
    indefinitely. Items that don't fit in the budget are simply left
    untranslated this round — the cache they wrote will speed up the
    next render until everything is hot.
    """
    if not items:
        return
    import time as _time
    from concurrent.futures import ThreadPoolExecutor, as_completed
    skip = _translate_skip_langs(db)

    # First pass: cache lookups only — fast, no network. Anything not in
    # the cache queues a background translation.
    #
    # Language is determined per-FIELD by content detection. Stored
    # `item.lang` is only a hint (it often reflects the stock's locale,
    # not the actual post body — Indonesian/French/Italian tweets get
    # filed under en because Yahoo metadata says so). We auto-detect
    # the language of the actual text and translate when it isn't en.
    pending: list[tuple[dict, str, str, str]] = []  # (item, field, src, lang)
    stored_lang_default = lambda item: (item.get("lang") or "").strip().lower()
    for item in items:
        for f in fields:
            src = item.get(f) or ""
            if not src or len(src.strip()) < 2:
                continue
            # Detect language from the actual text. If detection says
            # English, trust it (no translation). Otherwise the detected
            # language wins over any incorrect stored hint.
            detected = _detect_language(src)
            lang = detected if detected and detected != "en" else stored_lang_default(item)
            if not lang or lang.startswith("en") or lang in skip:
                continue
            cached = _cached_translation(db, src, lang)
            if cached is not None and cached != src:
                # Cache hit — apply immediately, no network call.
                item[f + "_orig"] = src
                item[f] = cached
                # Also reflect the actual content language so the flag
                # chip and tooltip show the right country, not 'en'.
                item.setdefault("lang", lang)
            elif cached is None:
                # No cached translation; defer to background worker.
                pending.append((item, f, src, lang))
            # cached == src means we previously decided not to translate; skip.

    if not pending:
        return

    def _worker(args):
        item, field, src, lang = args
        try:
            return (item, field, src, lang, translate_to_english(db, src, lang))
        except Exception:
            return (item, field, src, lang, src)

    deadline = _time.monotonic() + budget_s
    # Important: do NOT use `with ThreadPoolExecutor(...) as pool:` here —
    # the context manager's __exit__ calls shutdown(wait=True), which
    # blocks until every in-flight HTTP call to Google Translate finishes
    # regardless of our budget. We want the render path to return as
    # soon as the budget elapses; remaining workers can finish in the
    # background and populate the cache for the next render.
    pool = ThreadPoolExecutor(max_workers=max_workers)
    try:
        futures = {pool.submit(_worker, a): a for a in pending}
        for fut in as_completed(futures, timeout=budget_s):
            if _time.monotonic() > deadline:
                break
            try:
                item, field, src, lang, tgt = fut.result(timeout=0.05)
            except Exception:
                continue
            if tgt and tgt != src:
                item[field + "_orig"] = src
                item[field] = tgt
                # Override stored hint so the flag chip is accurate
                item["lang"] = lang
    except Exception:
        # as_completed raises TimeoutError when the deadline hits and
        # some futures are still running — that's expected, just exit.
        pass
    finally:
        # Don't block render on background HTTP. Workers keep running
        # (the pool isn't garbage-collected until they finish) and
        # write to the translation cache for next time.
        pool.shutdown(wait=False, cancel_futures=True)

# ---------------------------------------------------------------------------
# Background self-refresh for stale chips
# ---------------------------------------------------------------------------
# The scheduled refresh sometimes lags (sleep, network blip, weekend
# rollover, watchdog miss) and the dashboard would otherwise render
# yesterday-or-older prices indefinitely. On each /monitor render we
# look at the price snapshots used by the chips and, for any stock
# older than today, fire a background fetch_prices() call. The page
# returns immediately with whatever's in the DB; the *next* render
# (typically seconds later when the user reloads) shows the fresh
# value. Per-ticker cooldown prevents pounding APIs on rapid reloads.

import threading as _threading
from concurrent.futures import ThreadPoolExecutor as _ThreadPoolExecutor
import time as _time_mod

_STALE_REFRESH_TS: dict[tuple, float] = {}
_STALE_REFRESH_LOCK = _threading.Lock()
_STALE_REFRESH_POOL = _ThreadPoolExecutor(
    max_workers=4, thread_name_prefix="stale-refresh")
_STALE_REFRESH_COOLDOWN_S = 1800  # 30 min — don't re-attempt the same
                                  # stock more than twice an hour.


def _safe_refetch(stock: dict, db, config: dict) -> None:
    """Best-effort price refresh from a background thread."""
    try:
        import fetchers as _f
        _f.fetch_prices(stock, db, config)
    except Exception:
        pass  # background; never raise back to the render path


def _kick_stale_refresh(db, config: dict, stale_stocks: list[dict]) -> None:
    """Dispatch background fetch_prices() calls for stale stocks.

    Each (ticker, exchange) is rate-limited so successive page renders
    don't pile up duplicate refetches. Fire-and-forget — never waits."""
    if not stale_stocks:
        return
    # Back off entirely while a manual / scheduled price refresh is
    # running — otherwise both pools hammer the one SQLite connection
    # and the manual refresh wedges at 0/N on lock contention.
    try:
        import fetchers as _f
        if _f.price_refresh_active():
            return
    except Exception:
        pass
    now = _time_mod.monotonic()
    targets: list[dict] = []
    with _STALE_REFRESH_LOCK:
        for s in stale_stocks:
            key = (s.get("ticker", ""), s.get("exchange", ""))
            last = _STALE_REFRESH_TS.get(key, 0.0)
            if now - last < _STALE_REFRESH_COOLDOWN_S:
                continue
            _STALE_REFRESH_TS[key] = now
            targets.append(s)
    for s in targets:
        try:
            _STALE_REFRESH_POOL.submit(_safe_refetch, s, db, config)
        except RuntimeError:
            pass  # pool shut down at process exit


# ---------------------------------------------------------------------------
# Background self-refresh for stale EARNINGS rows
# ---------------------------------------------------------------------------
# Same idea as the price self-heal above. A new past-earnings row (e.g.
# Plenitude's Q3 release on 2026-05-29) would otherwise only appear on
# the next scheduled refresh — sometimes days away. On each /monitor
# render we kick fetch_earnings() for any watched stock whose last
# earnings refresh is older than ~6 hours, so new releases land within
# a single page reload.

_STALE_EARN_TS: dict[tuple, float] = {}
_STALE_EARN_LOCK = _threading.Lock()
_STALE_EARN_POOL = _ThreadPoolExecutor(
    max_workers=2, thread_name_prefix="stale-earn")
_STALE_EARN_COOLDOWN_S = 21600  # 6h between background re-attempts per stock


def _safe_refetch_earnings(stock: dict, db, config: dict) -> None:
    try:
        import fetchers as _f
        _f.fetch_earnings(stock, db, config)
    except Exception:
        pass


def _kick_stale_earnings(db, config: dict,
                         stocks: list[dict]) -> None:
    """Dispatch background fetch_earnings() calls for stocks whose
    earnings rows haven't been re-checked recently.

    Cheap per-render scan: pulls max(fetched_at) per ticker from
    earnings_dates and only re-fires when older than the cooldown."""
    if not stocks:
        return
    # Back off while a real price/earnings refresh is running.
    try:
        import fetchers as _f
        if _f.price_refresh_active():
            return
    except Exception:
        pass
    try:
        rows = db.conn.execute(
            "SELECT ticker, exchange, MAX(fetched_at) AS f "
            "FROM earnings_dates GROUP BY ticker, exchange"
        ).fetchall()
        last_fetch = {(r["ticker"], r["exchange"]): r["f"]
                      for r in rows}
    except Exception:
        last_fetch = {}
    now = _time_mod.monotonic()
    from datetime import datetime as _dt, timedelta as _td
    cutoff_iso = (_dt.utcnow() - _td(hours=6)).isoformat()
    targets: list[dict] = []
    with _STALE_EARN_LOCK:
        for s in stocks:
            key = (s.get("ticker", ""), s.get("exchange", ""))
            f = last_fetch.get(key) or ""
            # In-process cooldown so successive reloads don't pile up
            if now - _STALE_EARN_TS.get(key, 0.0) < _STALE_EARN_COOLDOWN_S:
                continue
            # Skip if a fresh fetch happened within the last 6h
            if f and f >= cutoff_iso:
                continue
            _STALE_EARN_TS[key] = now
            targets.append(s)
    for s in targets:
        try:
            _STALE_EARN_POOL.submit(_safe_refetch_earnings, s, db, config)
        except RuntimeError:
            pass


# ---------------------------------------------------------------------------
# Lazy-loaded chart history endpoint
# ---------------------------------------------------------------------------

def get_chart_history_json(db, config: dict, days: int = 365) -> str:
    """Return per-ticker daily price history for the last ``days`` days
    as a compact JSON string. Shape: ``{ticker: [[YYYY-MM-DD, price], ...]}``.

    Served by /api/history. Moved out of the inline monitor.html payload
    so the first paint doesn't ship hundreds of KB of price points users
    rarely look at (only when they switch to Graphs mode or pull the
    1Y/ALL timescale)."""
    from datetime import datetime, timedelta
    cutoff = (datetime.utcnow() - timedelta(days=int(days))).strftime("%Y-%m-%d")
    out: dict[str, list[list]] = {}
    rows = db.conn.execute(
        "SELECT ticker, snapshot_at, price FROM price_snapshots "
        "WHERE snapshot_at >= ? ORDER BY ticker ASC, snapshot_at ASC",
        (cutoff,),
    ).fetchall()
    for r in rows:
        out.setdefault(r["ticker"], []).append([r["snapshot_at"][:10], r["price"]])
    return json.dumps(out, separators=(",", ":"))


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


def _spark_svg(prices: list, width: int = 90, height: int = 22) -> str:
    """Tiny inline-SVG sparkline with a *fixed* price scale (±20% from
    the first price = full chip height). Stocks that moved more than
    ±20% break visually out of the rectangle — that's the whole point:
    a big winner shoots above the chip, a big loser dives below.

    Auto-fitting to each stock's own min/max is what the live fetcher
    used to do; it made every chip look equally jagged regardless of
    actual return. Anchoring on the first price gives an honest
    visual sense of magnitude.
    """
    pts_in = [p for p in prices if p is not None]
    if len(pts_in) < 2:
        return ""
    first = pts_in[0]
    if first <= 0:
        return ""
    n = len(pts_in)
    # ±20% return = full chip height. Returns beyond that escape the
    # viewBox (overflow="visible" below lets them render outside).
    SCALE = 0.20
    half = (height - 2) / 2
    mid  = height / 2
    coords = []
    for i, p in enumerate(pts_in):
        x = i * (width - 2) / (n - 1) + 1
        ret = (p - first) / first
        y = mid - (ret / SCALE) * half
        coords.append(f"{x:.1f},{y:.1f}")
    color = "var(--green)" if pts_in[-1] >= first else "var(--red)"
    return (f'<svg class="chip-spark" viewBox="0 0 {width} {height}" '
            f'preserveAspectRatio="none" overflow="visible" '
            f'aria-hidden="true">'
            f'<polyline points="{" ".join(coords)}" fill="none" '
            f'stroke="{color}" stroke-width="1.4" '
            f'stroke-linecap="round" stroke-linejoin="round"/>'
            f'</svg>')


def _fmt_price_compact(p: float) -> str:
    """Format a price for axis labels — keeps it short (3-4 chars)."""
    if p is None:
        return ""
    if p >= 1000:
        return f"{p:,.0f}"
    if p >= 100:
        return f"{p:.0f}"
    if p >= 10:
        return f"{p:.1f}"
    if p >= 1:
        return f"{p:.2f}"
    return f"{p:.3f}"


def _fmt_date_short(iso: str, include_year: bool = False) -> str:
    """ISO date → '7 May' (or '7 May 25' when include_year)."""
    try:
        dt = datetime.strptime(iso[:10], "%Y-%m-%d")
        base = dt.strftime("%d %b").lstrip("0")
        if include_year:
            base += " " + dt.strftime("%y")
        return base
    except (ValueError, TypeError):
        return iso[:10] or ""


def _chart_svg(history: list, currency: str = "",
               window_start: str = "", window_end: str = "",
               width: int = 220, height: int = 120) -> str:
    """Render a labeled chart for the Graph density mode.

    Axis style:
      • Y-axis (right margin, right-aligned): max / mid / min, with
        the currency suffixed only on the max label so the column
        doesn't repeat the currency three times.
      • X-axis (bottom): first / mid / last date. Year is appended
        when the window spans a calendar boundary.
      • Three faint horizontal gridlines at min/mid/max so the eye
        can read price levels without back-checking the labels.
    """
    pts = [(d, p) for (d, p) in history if p is not None]
    if len(pts) < 2:
        return ""
    prices = [p for _, p in pts]
    pmin = min(prices)
    pmax = max(prices)
    rng = (pmax - pmin) if pmax > pmin else max(pmax * 0.01, 0.01)
    pad_top    = 8
    pad_right  = 46
    pad_left   = 6
    pad_bottom = 16
    plot_w = width - pad_left - pad_right
    plot_h = height - pad_top - pad_bottom
    d_start = window_start or pts[0][0]
    d_end   = window_end   or pts[-1][0]
    try:
        t_start = datetime.strptime(d_start, "%Y-%m-%d").timestamp()
        t_end   = datetime.strptime(d_end,   "%Y-%m-%d").timestamp()
    except ValueError:
        t_start, t_end = 0.0, 1.0
    t_span = (t_end - t_start) if t_end > t_start else 1.0
    coords = []
    for d, p in pts:
        try:
            t = datetime.strptime(d, "%Y-%m-%d").timestamp()
        except ValueError:
            t = t_start
        x = pad_left + (t - t_start) / t_span * plot_w
        y = pad_top + plot_h - (p - pmin) / rng * plot_h
        coords.append(f"{x:.1f},{y:.1f}")
    color = "var(--green)" if prices[-1] >= prices[0] else "var(--red)"
    cur = (currency or "").strip()
    pmid = (pmin + pmax) / 2.0
    pmax_label = ((cur + " ") if cur else "") + _fmt_price_compact(pmax)
    pmid_label = _fmt_price_compact(pmid)
    pmin_label = _fmt_price_compact(pmin)
    cross_year = d_start[:4] != d_end[:4]
    # Mid date = midpoint of the time window (not data, so labels stay
    # evenly spaced).
    try:
        t_mid = (t_start + t_end) / 2.0
        d_mid = datetime.utcfromtimestamp(t_mid).strftime("%Y-%m-%d")
    except (OverflowError, OSError, ValueError):
        d_mid = pts[len(pts) // 2][0]
    date_first = _fmt_date_short(d_start, include_year=cross_year)
    date_mid   = _fmt_date_short(d_mid,   include_year=cross_year)
    date_last  = _fmt_date_short(d_end,   include_year=cross_year)
    # Geometry helpers
    y_top    = pad_top
    y_mid    = pad_top + plot_h / 2.0
    y_bot    = pad_top + plot_h
    x_left   = pad_left
    x_mid    = pad_left + plot_w / 2.0
    x_right  = pad_left + plot_w
    label_x  = width - 4   # right edge of the SVG, with text-anchor=end
    return (
        f'<svg class="chip-chart" viewBox="0 0 {width} {height}" '
        f'aria-hidden="true">'
        # Three horizontal gridlines: max / mid / min.
        f'<line x1="{x_left}" y1="{y_top:.1f}" '
        f'x2="{x_right:.1f}" y2="{y_top:.1f}" '
        f'stroke="var(--border)" stroke-width="0.5" '
        f'stroke-dasharray="2,3" opacity="0.55"/>'
        f'<line x1="{x_left}" y1="{y_mid:.1f}" '
        f'x2="{x_right:.1f}" y2="{y_mid:.1f}" '
        f'stroke="var(--border)" stroke-width="0.5" '
        f'stroke-dasharray="2,3" opacity="0.4"/>'
        f'<line x1="{x_left}" y1="{y_bot:.1f}" '
        f'x2="{x_right:.1f}" y2="{y_bot:.1f}" '
        f'stroke="var(--border)" stroke-width="0.6"/>'
        # Polyline
        f'<polyline points="{" ".join(coords)}" fill="none" '
        f'stroke="{color}" stroke-width="1.6" '
        f'stroke-linecap="round" stroke-linejoin="round"/>'
        # Y-axis labels — right-aligned at the SVG edge.
        f'<text x="{label_x}" y="{y_top + 3.5:.1f}" '
        f'font-size="9.5" font-weight="600" '
        f'fill="var(--text-muted)" text-anchor="end">'
        f'{_esc_chart(pmax_label)}</text>'
        f'<text x="{label_x}" y="{y_mid + 3.5:.1f}" '
        f'font-size="9.5" fill="var(--text-muted)" text-anchor="end" '
        f'opacity="0.75">{_esc_chart(pmid_label)}</text>'
        f'<text x="{label_x}" y="{y_bot + 3.5:.1f}" '
        f'font-size="9.5" font-weight="600" '
        f'fill="var(--text-muted)" text-anchor="end">'
        f'{_esc_chart(pmin_label)}</text>'
        # X-axis labels (bottom).
        f'<text x="{x_left}" y="{height - 3}" '
        f'font-size="9.5" fill="var(--text-muted)">'
        f'{_esc_chart(date_first)}</text>'
        f'<text x="{x_mid:.1f}" y="{height - 3}" '
        f'font-size="9.5" fill="var(--text-muted)" '
        f'text-anchor="middle" opacity="0.75">'
        f'{_esc_chart(date_mid)}</text>'
        f'<text x="{x_right:.1f}" y="{height - 3}" '
        f'font-size="9.5" fill="var(--text-muted)" text-anchor="end">'
        f'{_esc_chart(date_last)}</text>'
        f'</svg>'
    )


def _esc_chart(s: str) -> str:
    """Minimal XML escape for SVG <text> nodes."""
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _fmt_date_compact(iso_date: str) -> str:
    """Format ISO date as compact '9APR26'. Returns original if unparseable."""
    try:
        dt = datetime.strptime(iso_date[:10], "%Y-%m-%d")
        return f"{dt.day}{dt.strftime('%b').upper()}{dt.strftime('%y')}"
    except (ValueError, TypeError):
        return iso_date


def _humanize_pub_date(date_str: str) -> str:
    """Render a news/forum publication timestamp consistently across feeds.

    Source feeds emit a wild mix:  RFC 2822 ("Fri, 24 Apr 2026 …"),
    ISO 8601, "9 hours ago", "Apr 25, 2026", "2 weeks ago", … . This
    helper normalizes all of them to:

        Today HH:MM    — published in the last <24h, today (local)
        Yesterday      — published yesterday
        DD MMM         — same year, more than yesterday
        DD MMM YYYY    — different year

    Returns '' when input is empty, or the original string when it
    can't be parsed (graceful fallback so we never hide real data).
    """
    if not date_str:
        return ""
    epoch = _parse_news_epoch(date_str)
    if epoch <= 0:
        # Fall through to the raw string — better than nothing
        return date_str
    try:
        dt = datetime.fromtimestamp(epoch)
    except (OSError, ValueError, OverflowError):
        return date_str
    now = datetime.now()
    today = now.date()
    pub_date = dt.date()
    if pub_date == today:
        # Hide a fake 00:00 timestamp (date-only feeds parse to midnight)
        if dt.hour == 0 and dt.minute == 0:
            return "Today"
        return "Today " + dt.strftime("%H:%M")
    if (today - pub_date).days == 1:
        return "Yesterday"
    if pub_date.year == today.year:
        return dt.strftime("%d %b").lstrip("0")
    return dt.strftime("%d %b %Y").lstrip("0")


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
/* ── Light-mode palette ──────────────────────────────────────────────
 * Toggle via ☀️/🌙 button in the header. The class lives on <html>
 * so an inline pre-paint script can apply it before <body> exists,
 * preventing a flash of dark UI on every load. */
html.light-mode {
    --bg:          #f7f8fb;
    --surface:     #ffffff;
    --surface2:    #eef0f5;
    --border:      #d6d9e0;
    --text:        #1c1f2c;
    --text-muted:  #5a6075;
    --accent:      #3b5bdb;
    --accent-dim:  #c9d3f6;
    --red:         #d12d4a;
    --red-dim:     rgba(209,45,74,0.10);
    --amber:       #c98014;
    --amber-dim:   rgba(201,128,20,0.12);
    --green:       #1e9560;
    --green-dim:   rgba(30,149,96,0.10);
    --blue-dim:    rgba(59,91,219,0.08);
    color-scheme: light;
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

/* Theme toggle (☀️/🌙) — small, ghost-styled, in the header nav. */
.theme-toggle {
    background: transparent; border: 1px solid var(--border);
    color: var(--text-muted);
    font-size: 0.95rem;
    padding: 0.18rem 0.42rem;
    border-radius: 6px;
    cursor: pointer;
    line-height: 1;
    transition: all 0.15s;
}
.theme-toggle:hover { color: var(--text); background: var(--surface2); }

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
/* Use !important so density-line/graph `display: flex` rules don't
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
/* Stale price marker — shown when the last successful price fetch was
   on an earlier day (refresh failed, source 429d). The price + change
   render dimmed so the eye doesn't take the number as current. */
.stock-chip-price[data-stale="1"] {
    color: var(--text-muted);
}
.stock-chip-price[data-stale="1"] .stock-chip-change {
    opacity: 0.55;
}
.stock-chip-stale {
    display: inline-block; margin-left: 0.4rem;
    font-size: 0.65rem; font-weight: 600;
    color: #ffb74d; background: rgba(255,183,77,0.12);
    padding: 0.05rem 0.35rem; border-radius: 4px;
    cursor: help; vertical-align: middle;
}
.stock-chip-nodata {
    font-size: 0.75rem; color: var(--text-muted); margin-top: 0.2rem;
}
/* ── Sparklines + elaborate charts ────────────────────────────────
 * Two distinct visualizations:
 *   .chip-spark  — tiny axis-less SVG line. Used by the "📈 Charts"
 *                  toggle on Chips and Lines density modes.
 *   .chip-chart  — bigger, axis-labeled SVG (price min/max + date
 *                  range). Used by the Graph density mode.
 * Each is hidden by default and turned on selectively.
 */
.chip-spark {
    width: 100%; height: 22px; margin-top: 4px;
    display: none;
    /* The SVG itself uses overflow="visible" so big movers spill out
     * above/below the rectangle. Match that here so no parent clips. */
    overflow: visible;
}
body.show-charts .stock-chip .chip-spark { display: block; }
body.density-line  .stock-chip .chip-spark { width: 90px; margin: 0 0 0 0.5rem; flex-shrink: 0; }
/* The chip itself must not clip the spillover line. Chips already
 * have padding, so the visual breakout is bounded by the panel. */
.stock-chip { overflow: visible; }
/* In Graph mode the elaborate chart replaces the sparkline. */
body.density-graph .stock-chip .chip-spark { display: none; }

.chip-chart {
    width: 100%; height: 120px; margin-top: 6px;
    display: none;
    overflow: visible;
}
body.density-graph .stock-chip .chip-chart { display: block; }
/* When indexed mode is on, outlier polylines escape the chart's
 * viewBox. The chip card already has overflow:visible (set higher up
 * for sparkline breakout). Add a touch more breathing room above the
 * chart so the escape doesn't crash into the price line. */
body.density-graph.chart-indexed .stock-chip .chip-chart { margin-top: 12px; }

/* ── Density variants for the stock-chip grid ──
 * Chip mode (default): boxy cards, ~170px wide, 3 lines tall.
 * Line mode: single-row horizontal cards, flex-wrap, all info on one line.
 * Mini mode: compact ticker + change%, 7-8 per row.
 * The body class .density-line / .density-graph swaps which rules apply. */
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
body.density-graph .stock-chip-ticker .tk-sep,
body.density-graph .stock-chip-ticker .tk-code { display: none; }
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

/* Graph: ticker + current price + an axis-labeled chart, 4 across.
 * Each card stays compact (~24% of row width) but shows enough chart
 * area to make the trend readable. Falls to 2-up on tablet, 1-up on
 * phone. */
body.density-graph .stock-panel-inner {
    gap: 0.6rem;
}
body.density-graph .stock-chip {
    min-width: 0;
    flex: 0 0 calc(25% - 0.6rem);
    max-width: calc(25% - 0.6rem);
    padding: 0.5rem 0.6rem 0.4rem;
    display: flex; flex-direction: column; gap: 0.15rem;
}
@media (max-width: 1100px) {
    body.density-graph .stock-chip {
        flex-basis: calc(33.33% - 0.6rem);
        max-width: calc(33.33% - 0.6rem);
    }
}
@media (max-width: 800px) {
    body.density-graph .stock-chip {
        flex-basis: calc(50% - 0.6rem);
        max-width: calc(50% - 0.6rem);
    }
}
@media (max-width: 500px) {
    body.density-graph .stock-chip {
        flex-basis: 100%;
        max-width: 100%;
    }
}
/* Show the company name in graph mode — there's room. */
body.density-graph .stock-chip-name {
    display: block;
    font-size: 0.72rem; color: var(--text-muted);
    white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
    margin-bottom: 0.05rem;
}
body.density-graph .stock-chip-ticker {
    font-size: 0.85rem; font-weight: 700; color: var(--text);
    white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
}
body.density-graph .stock-chip-price {
    font-size: 0.95rem; margin: 0.05rem 0 0;
    display: flex; justify-content: space-between; align-items: center; gap: 0.3rem;
    font-variant-numeric: tabular-nums;
}
body.density-graph .stock-chip-price > :first-child {
    font-weight: 700;
    white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
    min-width: 0;
}
body.density-graph .stock-chip-change {
    font-size: 0.72rem; padding: 0.1rem 0.4rem;
}
body.density-graph .stock-chip-nodata {
    font-size: 0.7rem; margin: 0.2rem 0 0;
}
body.density-graph .stock-chip-remove { opacity: 0.4; }
body.density-graph .stock-chip:hover .stock-chip-remove { opacity: 1; }

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

.density-count-hint {
    font-size: 0.66rem; font-weight: 500; color: var(--text-muted);
    opacity: 0.7; margin-left: 0.2rem;
}
.stocks-label {
    font-size: 0.78rem; font-weight: 700; color: var(--text);
    letter-spacing: 0.01em;
}
.panels-bulk-btn {
    background: transparent; border: 1px solid var(--border);
    color: var(--text-muted); font-size: 0.68rem; font-weight: 600;
    padding: 0.2rem 0.55rem; border-radius: 999px;
    cursor: pointer; transition: all 0.15s;
}
.panels-bulk-btn:hover { border-color: var(--accent); color: var(--text); }

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

/* Selected-exchange sticky chip — shown when exchange(s) are filtered
 * but no specific stock is picked. Same role as the stock chip:
 * persistent reference while scrolling through news/earnings. */
.selected-exchange-chip {
    display: inline-flex; align-items: center; gap: 0.5rem;
    padding: 0.25rem 0.7rem 0.25rem 0.55rem;
    background: var(--surface2);
    border: 1px solid var(--border);
    border-radius: 999px;
    font-size: 0.75rem;
    color: var(--text);
}
.selected-exchange-chip .sec-icon { opacity: 0.75; }
.selected-exchange-chip .sec-country { font-weight: 700; }
.selected-exchange-chip .sec-exchanges { color: var(--text-muted); font-weight: 500; }
.selected-exchange-chip .sec-count {
    color: var(--text-muted); font-size: 0.7rem;
    padding: 0.04rem 0.38rem; border-radius: 999px;
    background: var(--surface);
    font-variant-numeric: tabular-nums;
}
.selected-exchange-chip .sec-clear {
    cursor: pointer; color: var(--text-muted);
    font-size: 0.9rem; line-height: 1; padding: 0 0.2rem;
    border-radius: 4px;
}
.selected-exchange-chip .sec-clear:hover { color: var(--text); background: var(--surface); }

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

/* Graph-mode timescale picker + 1y-history backfill — both only
 * visible while Graph density is on. The Sparklines toggle is the
 * inverse: useful in Chips/Lines, redundant in Graphs (the elaborate
 * chart replaces the sparkline anyway). */
.graph-range-bar { display: none; }
body.density-graph .graph-range-bar { display: inline-flex; align-items: center; gap: 0.4rem; }
body.density-graph .sparklines-toggle-bar { display: none; }

/* Backfill button sits at the right of the range bar. Hairline border
 * so it reads as an action rather than a toggle state. */
.graph-backfill-btn,
.graph-indexed-btn {
    border: 1px solid var(--border) !important;
    margin-left: 0.4rem;
}
.graph-backfill-btn:hover,
.graph-indexed-btn:hover {
    color: var(--text);
    background: var(--surface2);
}
.graph-backfill-btn.busy {
    opacity: 0.7; cursor: wait;
}
/* Active indexed toggle gets the same fill as the active range pill. */
.graph-indexed-btn.active {
    background: var(--accent);
    color: var(--bg);
    border-color: var(--accent) !important;
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
.exchange-status .status-dot.break  { background: #f5a623; box-shadow: 0 0 6px #f5a623; }
.exchange-status .status-text { color: var(--text-muted); }
.exchange-status .status-label-open { color: var(--green); }
.exchange-status .status-label-closed { color: var(--text-muted); }
.exchange-status .status-label-break { color: #f5a623; }

/* ── Main grid ── */
.container {
    max-width: 1400px; margin: 0 auto;
    padding: 0.75rem 2rem 5rem;
    display: grid;
    grid-template-columns: minmax(0, 1.25fr) minmax(420px, 1fr);
    grid-template-areas:
        "alerts    alerts"
        "news      earnings"
        "forum     forum"
        "insider   funds";
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
#funds-section    { grid-area: funds; }
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
.serper-info {
    display: inline-flex; align-items: center; justify-content: center;
    width: 22px; height: 22px; border-radius: 50%;
    background: var(--surface); border: 1px solid var(--border);
    color: var(--text-muted); font-size: 0.75rem; font-weight: 700;
    cursor: help; user-select: none; position: relative;
    transition: background 0.15s, color 0.15s;
}
.serper-info:hover, .serper-info:focus {
    background: var(--accent); color: #fff; outline: none;
}
.serper-popover {
    display: none; position: absolute; bottom: calc(100% + 10px); right: 0;
    width: 300px; padding: 0.85rem 1rem;
    background: var(--surface); border: 1px solid var(--border);
    border-left: 3px solid #c96a2d; border-radius: 8px;
    box-shadow: 0 6px 24px rgba(0,0,0,0.5);
    font-size: 0.78rem; font-weight: 400; line-height: 1.5;
    color: var(--text-muted); text-align: left;
    z-index: 300;
}
.serper-popover strong { color: var(--text); }
.serper-popover em { font-style: italic; color: var(--text); }
.serper-popover a { color: var(--accent); text-decoration: underline; }
.serper-info:hover .serper-popover,
.serper-info:focus .serper-popover { display: block; }
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
    _renderSelectedExchangeChip();
    if (typeof _syncScopeFromChips === 'function') _syncScopeFromChips();
}

// Sticky chip for the currently-selected exchange(s). Shown when a
// user has picked one or more exchanges but no specific stock — so
// scrolling through news/earnings/forums always shows which region
// the page is filtered to.
function _renderSelectedExchangeChip() {
    const wrap = document.getElementById('selected-exchange-chip');
    if (!wrap) return;
    // Gather active display-exchange labels (country names like "Japan").
    const actives = [...document.querySelectorAll(
        '.filter-pill.active:not([data-exchange="ALL"])'
    )].map(p => p.dataset.exchange).filter(Boolean);
    // Hide when no exchange is selected, or when the stock chip is
    // showing (it's more specific, don't clutter with both).
    if (actives.length === 0 || activeTickers.size > 0) {
        wrap.style.display = 'none';
        wrap.innerHTML = '';
        return;
    }
    // Count visible chips per exchange, and pull any exchange-name
    // sub-labels from the corresponding stock-panel header.
    const chipsVisible = document.querySelectorAll(
        '.stock-chip:not(.filtered-out):not(.stock-hidden)'
    ).length;
    // For single-exchange selection, include the exchange-name detail.
    let country, exNames = '';
    if (actives.length === 1) {
        country = actives[0];
        const panel = document.querySelector(
            '.stock-panel[data-exchange="' + country.replace(/"/g, '\\"') + '"]'
        );
        if (panel) {
            const ex = panel.querySelector('.stock-panel-exchanges');
            if (ex) exNames = ex.textContent.trim();
        }
    } else {
        country = actives.join(' + ');
    }
    const safeCountry = country.replace(/</g, '&lt;');
    const safeEx = exNames.replace(/</g, '&lt;');
    wrap.innerHTML =
        '<span class="sec-icon">🌍</span>' +
        '<span class="sec-country">' + safeCountry + '</span>' +
        (safeEx ? ' <span class="sec-exchanges">— ' + safeEx + '</span>' : '') +
        ' <span class="sec-count">' + chipsVisible + ' stock' + (chipsVisible === 1 ? '' : 's') + '</span>' +
        ' <span class="sec-clear" title="Clear exchange filter" onclick="clearExchangeSelection()">×</span>';
    wrap.style.display = 'inline-flex';
}

function clearExchangeSelection() {
    _applyExchangeFilter([]);
    _renderSelectedExchangeChip();
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
    _renderSelectedExchangeChip();  // hide exchange chip when stock is selected
    if (typeof _syncScopeFromChips === 'function') _syncScopeFromChips();
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

// Refresh-button labels depend on the current chip + exchange filter
// state. Call this after any change that affects activeTickers or the
// exchange filter so the buttons read e.g. "Free refresh: BXN only"
// or "Full refresh: 3 stocks" or "Free refresh: KRX only".
function _syncScopeFromChips() {
    if (typeof _updateRefreshScopeLabels === 'function') {
        _updateRefreshScopeLabels();
    }
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
    if (typeof _syncScopeFromChips === 'function') _syncScopeFromChips();
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
    // MSCI Market Classification (June 2024). Anything not listed in
    // DEVELOPED or EMERGING is treated as Frontier/Standalone (gets the
    // amber FRONTIER badge). Catalog source only — Yahoo-source results
    // already cover developed-market large/mid caps and don't need a
    // tier badge.
    const MSCI_DEVELOPED = new Set([
        'NASDAQ','NYSE','AMEX','OTC','PNK',          // United States
        'TSX',                                       // Canada
        'LSE','IOB',                                 // United Kingdom
        'FRA',                                       // Germany
        'BIT',                                       // Italy
        'BME',                                       // Spain
        'SWX',                                       // Switzerland
        'OMX','STO',                                 // Sweden
        'HSE',                                       // Finland
        'OSE',                                       // Norway
        'CSE',                                       // Denmark (Copenhagen)
        'WBAG',                                      // Austria
        'TASE',                                      // Israel
        'JPX',                                       // Japan
        'HKSE',                                      // Hong Kong
        'SGX',                                       // Singapore
        'ASX','NZX',                                 // Australia, New Zealand
        'KRX',                                       // South Korea (MSCI says EM, but treated as DM here — high-income, deep liquidity)
        'WSE',                                       // Poland (MSCI says EM, but treated as DM here — EU member, high-income)
    ]);
    const MSCI_EMERGING = new Set([
        'SSE','SZSE',                                // China A
        'TWSE',                                      // Taiwan
        'NSE','BSE',                                 // India
        'JSE',                                       // South Africa
        'ATHEX',                                     // Greece
        'BIST',                                      // Turkey
        'ADX','DFM',                                 // UAE
        'QSE',                                       // Qatar
        'KWSE',                                      // Kuwait
        'EGX',                                       // Egypt
        'IDX',                                       // Indonesia
        'KLSE',                                      // Malaysia
        'PSE',                                       // Philippines
        'SET',                                       // Thailand
        'BMV',                                       // Mexico
        'BVL',                                       // Peru
        'BCBA',                                      // Argentina (was EM, now Standalone — keep here)
    ]);
    let html = '';
    for (const r of results) {
        let source_badge = '';
        {
            const ex = (r.exchange || '').toUpperCase();
            // EMERGING badge applies regardless of source: emerging
            // markets like Thailand (SET), Malaysia (KLSE) and Indonesia
            // (IDX) are Yahoo-covered, so they'd otherwise show no tier.
            // FRONTIER stays catalog-only — Yahoo's long tail includes
            // many obscure exchange codes we don't want to mislabel.
            if (MSCI_EMERGING.has(ex)) {
                source_badge = '<span class="add-stock-result-badge" style="color:#ffb74d;border-color:#ffb74d">EMERGING</span>';
            } else if (r.source === 'catalog' && !MSCI_DEVELOPED.has(ex)) {
                source_badge = '<span class="add-stock-result-badge" style="color:var(--green);border-color:var(--green)">FRONTIER</span>';
            }
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
        .map(p => encodeURIComponent(p.dataset.exchange));
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
                // Stock already on the dashboard — no reload needed.
                showToast(
                    (data.ticker || '') + ' is already on your monitor',
                    'info'
                );
                closeAddStockModal();
                return;
            }
            closeAddStockModal();
            showToast('Added ' + (data.name || data.ticker) + ' to your monitor', 'success');
            _preserveFilterHashForReload();
            // Brief delay so the toast is visible before the reload
            setTimeout(() => location.reload(), 500);
        } else {
            showToast(resp.message || 'Failed to add stock', 'error');
        }
    })
    .catch(err => showToast('Network error: ' + err, 'error'));
}

// ── Remove a stock from the monitor (called from the chip ✕ button) ──
function removeStockFromWatchlist(ticker, exchange, name) {
    showConfirm(
        'Remove from monitor?',
        'Remove ' + (name || ticker) + ' from your monitor. Existing portfolio ' +
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
    'Malaysia':         { tz: 'Asia/Kuala_Lumpur',   open: '09:00', close: '17:00', lunch: ['12:30','14:30'], days: [1,2,3,4,5], name: 'Bursa Malaysia' },
    'Nigeria':          { tz: 'Africa/Lagos',        open: '09:30', close: '14:30', days: [1,2,3,4,5], name: 'Nigerian Exchange' },
    'Ivory Coast/BRVM':      { tz: 'Africa/Abidjan',      open: '09:00', close: '15:30', days: [1,2,3,4,5], name: "BRVM (8-country West African regional exchange)" },
    'Uzbekistan':       { tz: 'Asia/Tashkent',       open: '10:00', close: '15:00', days: [1,2,3,4,5], name: 'Tashkent Stock Exchange' },
    'Singapore':        { tz: 'Asia/Singapore',      open: '09:00', close: '17:00', lunch: ['12:00','13:00'], days: [1,2,3,4,5], name: 'Singapore Exchange' },
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
    'Lithuania':        { tz: 'Europe/Vilnius',      open: '10:00', close: '16:00', days: [1,2,3,4,5], name: 'Nasdaq Baltic Vilnius' },
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
    'Indonesia':        { tz: 'Asia/Jakarta',        open: '09:00', close: '16:15', lunch: ['12:00','13:30'], days: [1,2,3,4,5], name: 'Indonesia Stock Exchange (IDX)' },
    'Thailand':         { tz: 'Asia/Bangkok',        open: '10:00', close: '16:30', lunch: ['12:30','14:30'], days: [1,2,3,4,5], name: 'Stock Exchange of Thailand (SET)' },
    'Philippines':      { tz: 'Asia/Manila',         open: '09:30', close: '15:30', lunch: ['12:00','13:30'], days: [1,2,3,4,5], name: 'Philippine Stock Exchange (PSE)' },
    'Vietnam':          { tz: 'Asia/Ho_Chi_Minh',    open: '09:00', close: '15:00', lunch: ['11:30','13:00'], days: [1,2,3,4,5], name: 'Ho Chi Minh Stock Exchange (HOSE)' },
    'Israel':           { tz: 'Asia/Jerusalem',      open: '09:59', close: '17:14', days: [0,1,2,3,4], name: 'Tel Aviv Stock Exchange (TASE)' },
    'Saudi Arabia':     { tz: 'Asia/Riyadh',         open: '10:00', close: '15:00', days: [0,1,2,3,4], name: 'Saudi Stock Exchange (Tadawul)' },
    'UAE':              { tz: 'Asia/Dubai',          open: '10:00', close: '14:00', days: [1,2,3,4,5], name: 'UAE (DFM Dubai + ADX Abu Dhabi)' },
    'Qatar':            { tz: 'Asia/Qatar',          open: '09:30', close: '13:15', days: [0,1,2,3,4], name: 'Qatar Stock Exchange (QSE)' },
    'Turkey':           { tz: 'Europe/Istanbul',     open: '10:00', close: '18:00', days: [1,2,3,4,5], name: 'Borsa Istanbul (BIST)' },
    'Poland':           { tz: 'Europe/Warsaw',       open: '09:00', close: '17:05', days: [1,2,3,4,5], name: 'Warsaw Stock Exchange (WSE)' },
    'Czech Republic':   { tz: 'Europe/Prague',       open: '09:00', close: '16:30', days: [1,2,3,4,5], name: 'Prague Stock Exchange (PSE)' },
    'Hungary':          { tz: 'Europe/Budapest',     open: '09:00', close: '17:00', days: [1,2,3,4,5], name: 'Budapest Stock Exchange (BET)' },
    'Greece':           { tz: 'Europe/Athens',       open: '10:00', close: '17:20', days: [1,2,3,4,5], name: 'Athens Stock Exchange (ATHEX)' },
    'Romania':          { tz: 'Europe/Bucharest',    open: '10:00', close: '17:45', days: [1,2,3,4,5], name: 'Bucharest Stock Exchange (BVB)' },
    'New Zealand':      { tz: 'Pacific/Auckland',    open: '10:00', close: '16:45', days: [1,2,3,4,5], name: 'New Zealand Exchange (NZX)' },
    'China (Shanghai)': { tz: 'Asia/Shanghai',       open: '09:30', close: '15:00', lunch: ['11:30','13:00'], days: [1,2,3,4,5], name: 'Shanghai Stock Exchange (SSE)' },
    'China (Shenzhen)': { tz: 'Asia/Shanghai',       open: '09:30', close: '15:00', lunch: ['11:30','13:00'], days: [1,2,3,4,5], name: 'Shenzhen Stock Exchange (SZSE)' },
    'US':               { tz: 'America/New_York',    open: '09:30', close: '16:00', days: [1,2,3,4,5], name: 'New York (NASDAQ + NYSE)' },
    'South Africa':     { tz: 'Africa/Johannesburg', open: '09:00', close: '17:00', days: [1,2,3,4,5], name: 'Johannesburg Stock Exchange' },
    'UK':               { tz: 'Europe/London',       open: '08:00', close: '16:30', days: [1,2,3,4,5], name: 'London Stock Exchange' },
    'Hong Kong':        { tz: 'Asia/Hong_Kong',      open: '09:30', close: '16:00', lunch: ['12:00','13:00'], days: [1,2,3,4,5], name: 'Hong Kong Exchange' },
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
    'Japan':            { tz: 'Asia/Tokyo',          open: '09:00', close: '15:00', lunch: ['11:30','12:30'], days: [1,2,3,4,5], name: 'Tokyo Stock Exchange (JPX)' },
    'Spain':            { tz: 'Europe/Madrid',       open: '09:00', close: '17:30', days: [1,2,3,4,5], name: 'Bolsa de Madrid (BME)' },
    'Austria':          { tz: 'Europe/Vienna',       open: '09:00', close: '17:30', days: [1,2,3,4,5], name: 'Wiener Börse' },
    'Chile':            { tz: 'America/Santiago',    open: '09:30', close: '16:00', days: [1,2,3,4,5], name: 'Bolsa de Santiago' },
    'Brazil':           { tz: 'America/Sao_Paulo',   open: '10:00', close: '17:00', days: [1,2,3,4,5], name: 'B3 — Brasil, Bolsa, Balcão' },
    'Mexico':           { tz: 'America/Mexico_City', open: '08:30', close: '15:00', days: [1,2,3,4,5], name: 'Bolsa Mexicana de Valores (BMV)' },
    'Argentina':        { tz: 'America/Argentina/Buenos_Aires', open: '11:00', close: '17:00', days: [1,2,3,4,5], name: 'Bolsa Argentina (BYMA / BCBA)' },
    'Italy':            { tz: 'Europe/Rome',         open: '09:00', close: '17:30', days: [1,2,3,4,5], name: 'Borsa Italiana (Euronext Milan)' },
    'Egypt':            { tz: 'Africa/Cairo',        open: '10:00', close: '14:30', days: [0,1,2,3,4], name: 'Egyptian Exchange (EGX)' },
    'India':            { tz: 'Asia/Kolkata',        open: '09:15', close: '15:30', days: [1,2,3,4,5], name: 'NSE / BSE India' },
    'Slovenia':         { tz: 'Europe/Ljubljana',    open: '09:30', close: '13:30', days: [1,2,3,4,5], name: 'Ljubljana Stock Exchange (LJSE)' },
    'Bahrain':          { tz: 'Asia/Bahrain',        open: '09:30', close: '12:30', days: [0,1,2,3,4], name: 'Bahrain Bourse (BHB)' },
    'Oman':             { tz: 'Asia/Muscat',         open: '10:00', close: '13:00', days: [0,1,2,3,4], name: 'Muscat Stock Exchange (MSM)' },
    'Jordan':           { tz: 'Asia/Amman',          open: '10:00', close: '14:00', days: [0,1,2,3,4], name: 'Amman Stock Exchange (ASE)' },
    'Cambodia':         { tz: 'Asia/Phnom_Penh',     open: '08:30', close: '15:00', days: [1,2,3,4,5], name: 'Cambodia Securities Exchange (CSX)' },
};

// ── Exchange holidays — closures with names so we can show "CLOSED ·
// Labor Day" in the badge. Format: "MM-DD:Name" (annual) or
// "YYYY-MM-DD:Name" (movable holidays for 2026). Best-effort coverage.
const EXCHANGE_HOLIDAYS = {
    'Brazil':           ['01-01:New Year','04-21:Tiradentes','05-01:Labor Day','06-19:Corpus Christi','09-07:Independence','10-12:Lady of Aparecida','11-02:All Souls','11-15:Republic Day','11-20:Black Awareness','12-24:Christmas Eve','12-25:Christmas','12-31:New Year Eve','2026-02-16:Carnival','2026-02-17:Carnival','2026-04-03:Good Friday'],
    'Mexico':           ['01-01:New Year','02-02:Constitution Day','03-16:Benito Juarez','05-01:Labor Day','09-16:Independence','11-02:Day of the Dead','11-16:Revolution Day','12-12:Lady of Guadalupe','12-25:Christmas','2026-04-02:Maundy Thursday','2026-04-03:Good Friday'],
    'Argentina':        ['01-01:New Year','02-16:Carnival','02-17:Carnival','03-24:Memory Day','04-02:Malvinas','05-01:Labor Day','05-25:May Revolution','06-15:Güemes','06-20:Flag Day','07-09:Independence','08-17:San Martín','10-12:Diversity','11-23:Sovereignty','12-08:Immaculate Conception','12-25:Christmas','2026-04-02:Maundy Thursday','2026-04-03:Good Friday'],
    'Italy':            ['01-01:New Year','01-06:Epiphany','05-01:Labor Day','06-02:Republic Day','08-15:Assumption','12-08:Immaculate Conception','12-24:Christmas Eve','12-25:Christmas','12-26:St Stephen','12-31:New Year Eve','2026-04-03:Good Friday','2026-04-06:Easter Monday'],
    'France':           ['01-01:New Year','05-01:Labor Day','05-08:Victory Day','07-14:Bastille Day','08-15:Assumption','11-01:All Saints','11-11:Armistice','12-25:Christmas','12-26:St Stephen','2026-04-03:Good Friday','2026-04-06:Easter Monday','2026-05-14:Ascension','2026-05-25:Whit Monday'],
    'Germany':          ['01-01:New Year','05-01:Labor Day','10-03:Unity Day','12-24:Christmas Eve','12-25:Christmas','12-26:St Stephen','12-31:New Year Eve','2026-04-03:Good Friday','2026-04-06:Easter Monday','2026-05-14:Ascension','2026-05-25:Whit Monday'],
    'Spain':            ['01-01:New Year','01-06:Epiphany','05-01:Labor Day','08-15:Assumption','10-12:National Day','11-01:All Saints','12-06:Constitution','12-08:Immaculate Conception','12-24:Christmas Eve','12-25:Christmas','12-31:New Year Eve','2026-04-02:Maundy Thursday','2026-04-03:Good Friday'],
    'Portugal':         ['01-01:New Year','04-25:Freedom Day','05-01:Labor Day','06-10:Portugal Day','08-15:Assumption','10-05:Republic Day','11-01:All Saints','12-01:Restoration','12-08:Immaculate Conception','12-24:Christmas Eve','12-25:Christmas','12-31:New Year Eve','2026-04-03:Good Friday'],
    'Netherlands':      ['01-01:New Year','05-01:Labor Day','12-24:Christmas Eve','12-25:Christmas','12-26:St Stephen','12-31:New Year Eve','2026-04-03:Good Friday','2026-04-06:Easter Monday','2026-04-27:King Day','2026-05-14:Ascension','2026-05-25:Whit Monday'],
    'Belgium':          ['01-01:New Year','05-01:Labor Day','07-21:National Day','08-15:Assumption','11-01:All Saints','11-11:Armistice','12-24:Christmas Eve','12-25:Christmas','12-26:St Stephen','12-31:New Year Eve','2026-04-03:Good Friday','2026-04-06:Easter Monday','2026-05-14:Ascension','2026-05-25:Whit Monday'],
    'Ireland':          ['01-01:New Year','03-17:St Patrick','05-01:May Bank','12-24:Christmas Eve','12-25:Christmas','12-26:St Stephen','12-31:New Year Eve','2026-04-03:Good Friday','2026-04-06:Easter Monday'],
    'Switzerland':      ['01-01:New Year','01-02:Berchtold','05-01:Labor Day','08-01:National Day','12-24:Christmas Eve','12-25:Christmas','12-26:St Stephen','12-31:New Year Eve','2026-04-03:Good Friday','2026-04-06:Easter Monday','2026-05-14:Ascension'],
    'Austria':          ['01-01:New Year','01-06:Epiphany','05-01:Labor Day','08-15:Assumption','10-26:National Day','11-01:All Saints','12-08:Immaculate Conception','12-24:Christmas Eve','12-25:Christmas','12-26:St Stephen','12-31:New Year Eve','2026-04-03:Good Friday','2026-04-06:Easter Monday','2026-05-14:Ascension'],
    'Greece':           ['01-01:New Year','01-06:Epiphany','03-25:Independence','05-01:Labor Day','08-15:Assumption','10-28:Ohi Day','12-24:Christmas Eve','12-25:Christmas','12-26:St Stephen','12-31:New Year Eve','2026-02-23:Clean Monday','2026-04-10:Orthodox Good Friday','2026-04-13:Orthodox Easter Monday'],
    'Poland':           ['01-01:New Year','01-06:Epiphany','05-01:Labor Day','05-03:Constitution Day','08-15:Assumption','11-01:All Saints','11-11:Independence','12-24:Christmas Eve','12-25:Christmas','12-26:St Stephen','2026-04-06:Easter Monday'],
    'Czech Republic':   ['01-01:New Year','05-01:Labor Day','05-08:Liberation','07-05:St Cyril & Methodius','07-06:Jan Hus','09-28:St Wenceslaus','10-28:Independence','11-17:Freedom & Democracy','12-24:Christmas Eve','12-25:Christmas','12-26:St Stephen','2026-04-03:Good Friday','2026-04-06:Easter Monday'],
    'Hungary':          ['01-01:New Year','03-15:Revolution','05-01:Labor Day','08-20:St Stephen','10-23:Republic Day','11-01:All Saints','12-24:Christmas Eve','12-25:Christmas','12-26:St Stephen','2026-04-03:Good Friday','2026-04-06:Easter Monday','2026-05-25:Whit Monday'],
    'Romania':          ['01-01:New Year','01-02:New Year','01-24:Union Day','05-01:Labor Day','06-01:Children Day','08-15:Assumption','11-30:St Andrew','12-01:National Day','12-25:Christmas','12-26:St Stephen','2026-04-10:Orthodox Good Friday','2026-04-13:Orthodox Easter Monday','2026-06-01:Whit Monday'],
    'Croatia':          ['01-01:New Year','01-06:Epiphany','05-01:Labor Day','06-22:Antifascist Struggle','08-05:Victory Day','08-15:Assumption','11-01:All Saints','12-25:Christmas','12-26:St Stephen','2026-04-06:Easter Monday'],
    'Serbia':           ['01-01:New Year','01-02:New Year','01-07:Orthodox Christmas','02-15:Statehood','02-16:Statehood','05-01:Labor Day','05-02:Labor Day','11-11:Armistice','2026-04-10:Orthodox Good Friday','2026-04-13:Orthodox Easter Monday'],
    'Slovakia':         ['01-01:New Year','01-06:Epiphany','05-01:Labor Day','05-08:Liberation','07-05:St Cyril & Methodius','08-29:Uprising','09-01:Constitution','09-15:Lady of Sorrows','11-01:All Saints','11-17:Freedom Day','12-24:Christmas Eve','12-25:Christmas','12-26:St Stephen','2026-04-03:Good Friday','2026-04-06:Easter Monday'],
    'Slovenia':         ['01-01:New Year','01-02:New Year','02-08:Culture Day','04-27:Resistance','05-01:Labor Day','05-02:Labor Day','06-25:Statehood','08-15:Assumption','10-31:Reformation','11-01:All Saints','12-25:Christmas','12-26:Independence','2026-04-06:Easter Monday'],
    'Sweden':           ['01-01:New Year','01-06:Epiphany','05-01:Labor Day','06-06:National Day','12-24:Christmas Eve','12-25:Christmas','12-26:St Stephen','12-31:New Year Eve','2026-04-03:Good Friday','2026-04-06:Easter Monday','2026-05-14:Ascension'],
    'Finland':          ['01-01:New Year','01-06:Epiphany','05-01:Labor Day','12-06:Independence','12-24:Christmas Eve','12-25:Christmas','12-26:St Stephen','2026-04-03:Good Friday','2026-04-06:Easter Monday','2026-05-14:Ascension','2026-06-19:Midsummer Eve'],
    'Norway':           ['01-01:New Year','05-01:Labor Day','05-17:Constitution Day','12-24:Christmas Eve','12-25:Christmas','12-26:St Stephen','12-31:New Year Eve','2026-04-02:Maundy Thursday','2026-04-03:Good Friday','2026-04-06:Easter Monday','2026-05-14:Ascension','2026-05-25:Whit Monday'],
    'Denmark':          ['01-01:New Year','05-01:Labor Day','06-05:Constitution Day','12-24:Christmas Eve','12-25:Christmas','12-26:St Stephen','12-31:New Year Eve','2026-04-02:Maundy Thursday','2026-04-03:Good Friday','2026-04-06:Easter Monday','2026-05-01:General Prayer','2026-05-14:Ascension','2026-05-25:Whit Monday'],
    'Iceland':          ['01-01:New Year','12-24:Christmas Eve','12-25:Christmas','12-26:St Stephen','12-31:New Year Eve','2026-04-02:Maundy Thursday','2026-04-03:Good Friday','2026-04-06:Easter Monday','2026-04-23:First Day of Summer','2026-05-01:Labor Day','2026-05-14:Ascension','2026-05-25:Whit Monday'],
    'UK':               ['01-01:New Year','05-01:May Bank','12-25:Christmas','12-26:Boxing Day','2026-04-03:Good Friday','2026-04-06:Easter Monday','2026-05-04:Early May Bank','2026-05-25:Spring Bank','2026-08-31:Summer Bank'],
    'US':               ['01-01:New Year','06-19:Juneteenth','07-04:Independence','12-25:Christmas','2026-01-19:MLK Day','2026-02-16:Presidents Day','2026-04-03:Good Friday','2026-05-25:Memorial Day','2026-09-07:Labor Day','2026-11-26:Thanksgiving','2026-11-27:Day after Thanksgiving'],
    'Canada':           ['01-01:New Year','07-01:Canada Day','12-25:Christmas','12-26:Boxing Day','2026-02-16:Family Day','2026-04-03:Good Friday','2026-05-18:Victoria Day','2026-08-03:Civic Holiday','2026-09-07:Labor Day','2026-10-12:Thanksgiving'],
    'Chile':            ['01-01:New Year','05-01:Labor Day','05-21:Naval Glories','06-29:St Peter & St Paul','07-16:Lady of Carmen','08-15:Assumption','09-18:Independence','09-19:Army Day','10-12:Discovery','11-01:All Saints','12-08:Immaculate Conception','12-25:Christmas','12-31:Bank Holiday','2026-04-03:Good Friday'],
    'Australia':        ['01-01:New Year','01-26:Australia Day','12-25:Christmas','12-26:Boxing Day','12-28:Boxing Day Obs','2026-04-03:Good Friday','2026-04-06:Easter Monday','2026-04-25:ANZAC Day','2026-06-08:King Birthday'],
    'New Zealand':      ['01-01:New Year','01-02:Day after New Year','02-06:Waitangi Day','04-25:ANZAC Day','12-25:Christmas','12-26:Boxing Day','12-28:Boxing Day Obs','2026-04-03:Good Friday','2026-04-06:Easter Monday','2026-06-01:King Birthday','2026-10-26:Labor Day'],
    'Japan':            ['01-01:New Year','01-02:New Year','01-03:New Year','02-11:National Foundation','02-23:Emperor Birthday','04-29:Showa Day','05-03:Constitution Day','05-04:Greenery Day','05-05:Children Day','08-11:Mountain Day','11-03:Culture Day','11-23:Labor Thanksgiving','12-31:New Year Eve','2026-01-12:Coming of Age','2026-03-21:Vernal Equinox','2026-05-06:Children Day Obs','2026-07-20:Marine Day','2026-09-21:Respect for the Aged','2026-09-22:Autumnal Equinox','2026-10-12:Sports Day'],
    'South Korea':      ['01-01:New Year','03-01:Independence','05-05:Children Day','06-06:Memorial Day','08-15:Liberation','10-03:National Foundation','10-09:Hangul Day','12-25:Christmas','2026-02-16:Lunar New Year','2026-02-17:Lunar New Year','2026-02-18:Lunar New Year','2026-05-01:Labor Day','2026-05-25:Buddha Birthday','2026-09-24:Chuseok','2026-09-25:Chuseok'],
    'China (Shanghai)': ['01-01:New Year','05-01:Labor Day','2026-02-16:Lunar New Year','2026-02-17:Lunar New Year','2026-02-18:Lunar New Year','2026-04-06:Qingming','2026-05-04:Labor Day','2026-06-22:Dragon Boat','2026-09-25:Mid-Autumn','2026-09-28:National Day','2026-09-29:National Day','2026-09-30:National Day','2026-10-01:National Day','2026-10-02:National Day','2026-10-05:National Day','2026-10-06:National Day','2026-10-07:National Day','2026-10-08:National Day'],
    'China (Shenzhen)': ['01-01:New Year','05-01:Labor Day','2026-02-16:Lunar New Year','2026-02-17:Lunar New Year','2026-02-18:Lunar New Year','2026-04-06:Qingming','2026-05-04:Labor Day','2026-06-22:Dragon Boat','2026-09-25:Mid-Autumn','2026-09-28:National Day','2026-09-29:National Day','2026-09-30:National Day','2026-10-01:National Day','2026-10-02:National Day','2026-10-05:National Day','2026-10-06:National Day','2026-10-07:National Day','2026-10-08:National Day'],
    'Hong Kong':        ['01-01:New Year','05-01:Labor Day','07-01:HKSAR Day','10-01:National Day','12-25:Christmas','12-26:Boxing Day','2026-02-17:Lunar New Year','2026-02-18:Lunar New Year','2026-02-19:Lunar New Year','2026-04-03:Good Friday','2026-04-06:Easter Monday','2026-04-07:Ching Ming','2026-05-25:Buddha Birthday','2026-06-19:Dragon Boat','2026-09-26:Mid-Autumn'],
    'Taiwan':           ['01-01:New Year','02-28:Peace Memorial','04-04:Tomb Sweeping','12-25:Christmas','2026-02-16:Lunar New Year','2026-02-17:Lunar New Year','2026-02-18:Lunar New Year','2026-02-19:Lunar New Year','2026-02-20:Lunar New Year','2026-04-06:Tomb Sweeping','2026-09-25:Mid-Autumn','2026-10-09:National Day'],
    'India':            ['01-26:Republic Day','03-31:Eid al-Fitr','05-01:May Day','08-15:Independence','10-02:Gandhi Jayanti','12-25:Christmas','2026-03-04:Holi','2026-04-03:Good Friday','2026-04-14:Ambedkar Jayanti','2026-04-21:Mahavir Jayanti','2026-09-23:Eid al-Adha','2026-10-21:Diwali','2026-11-04:Diwali Padwa'],
    'Indonesia':        ['01-01:New Year','05-01:Labor Day','06-01:Pancasila Day','08-17:Independence','12-25:Christmas','2026-02-17:Lunar New Year','2026-03-22:Eid al-Fitr','2026-03-23:Eid al-Fitr','2026-04-03:Good Friday','2026-05-14:Ascension','2026-06-01:Vesak','2026-06-01:Eid al-Adha','2026-09-25:Prophet Birthday'],
    'Thailand':         ['01-01:New Year','01-02:New Year','04-06:Chakri Memorial','04-13:Songkran','04-14:Songkran','04-15:Songkran','05-01:Labor Day','05-04:Coronation','07-28:King Birthday','08-12:Mother Day','10-13:King Bhumibol Memorial','10-23:Chulalongkorn','12-07:King Father Birthday','12-10:Constitution Day','12-25:Christmas','12-31:New Year Eve','2026-06-01:Visakha Bucha','2026-07-29:Asarnha Bucha','2026-07-30:Buddhist Lent'],
    'Philippines':      ['01-01:New Year','02-25:EDSA Revolution','04-09:Day of Valor','05-01:Labor Day','06-12:Independence','08-21:Ninoy Aquino','08-31:National Heroes','11-01:All Saints','11-30:Bonifacio Day','12-08:Immaculate Conception','12-25:Christmas','12-30:Rizal Day','12-31:New Year Eve','2026-04-02:Maundy Thursday','2026-04-03:Good Friday'],
    'Vietnam':          ['01-01:New Year','04-30:Reunification','05-01:Labor Day','09-02:National Day','2026-02-16:Lunar New Year','2026-02-17:Lunar New Year','2026-02-18:Lunar New Year','2026-02-19:Lunar New Year','2026-02-20:Lunar New Year','2026-04-26:Hung Kings','2026-04-29:Reunification Obs'],
    'Malaysia':         ['01-01:New Year','02-01:Federal Territory Day','05-01:Labor Day','06-02:Agong Birthday','08-31:National Day','09-16:Malaysia Day','12-25:Christmas','2026-02-16:Lunar New Year','2026-02-17:Lunar New Year','2026-03-21:Eid al-Fitr','2026-03-31:Hari Raya','2026-06-01:Wesak','2026-08-29:Maulidur Rasul','2026-11-09:Deepavali'],
    'Singapore':        ['01-01:New Year','05-01:Labor Day','08-09:National Day','12-25:Christmas','2026-02-16:Lunar New Year','2026-02-17:Lunar New Year','2026-04-03:Good Friday','2026-05-01:Labor Day','2026-06-01:Vesak','2026-08-09:National Day','2026-09-25:Hari Raya Haji','2026-11-08:Deepavali'],
    'South Africa':     ['01-01:New Year','03-21:Human Rights','04-27:Freedom Day','05-01:Workers Day','06-16:Youth Day','08-09:Womens Day','09-24:Heritage Day','12-16:Reconciliation','12-25:Christmas','12-26:Day of Goodwill','2026-04-03:Good Friday','2026-04-06:Family Day'],
    'Nigeria':          ['01-01:New Year','05-01:Workers Day','05-29:Democracy Day','06-12:Democracy Day','10-01:Independence','12-25:Christmas','12-26:Boxing Day','2026-04-03:Good Friday','2026-04-06:Easter Monday','2026-03-21:Eid al-Fitr','2026-05-31:Eid al-Adha'],
    'Kenya':            ['01-01:New Year','05-01:Labor Day','06-01:Madaraka Day','10-10:Huduma Day','10-20:Mashujaa','12-12:Jamhuri Day','12-25:Christmas','12-26:Boxing Day','2026-04-03:Good Friday','2026-04-06:Easter Monday','2026-03-21:Eid al-Fitr'],
    'Egypt':            ['01-07:Coptic Christmas','01-25:Revolution Day','04-25:Sinai Liberation','05-01:Labor Day','07-23:Revolution Day','10-06:Armed Forces','2026-04-13:Sham El-Nessim','2026-04-19:Eid al-Fitr','2026-04-20:Eid al-Fitr','2026-03-22:Eid al-Fitr','2026-03-23:Eid al-Fitr','2026-05-31:Eid al-Adha'],
    'Israel':           ['2026-04-02:Passover Eve','2026-04-03:Passover','2026-04-08:Passover','2026-04-09:Passover','2026-04-22:Independence Day','2026-05-22:Shavuot','2026-09-12:Rosh Hashanah','2026-09-13:Rosh Hashanah','2026-09-21:Yom Kippur','2026-09-22:Yom Kippur','2026-09-26:Sukkot','2026-10-03:Simchat Torah'],
    'Turkey':           ['01-01:New Year','04-23:National Sovereignty','05-01:Labor Day','05-19:Atatürk Memorial','07-15:Democracy Day','08-30:Victory Day','10-29:Republic Day','2026-03-21:Eid al-Fitr','2026-03-22:Eid al-Fitr','2026-03-23:Eid al-Fitr','2026-05-31:Eid al-Adha','2026-06-01:Eid al-Adha'],
    'Saudi Arabia':     ['09-23:National Day','2026-03-21:Eid al-Fitr','2026-03-22:Eid al-Fitr','2026-03-23:Eid al-Fitr','2026-03-24:Eid al-Fitr','2026-05-31:Eid al-Adha','2026-06-01:Eid al-Adha','2026-06-02:Eid al-Adha','2026-06-03:Eid al-Adha'],
    'UAE':              ['01-01:New Year','12-02:National Day','12-03:National Day','2026-03-22:Eid al-Fitr','2026-03-23:Eid al-Fitr','2026-03-24:Eid al-Fitr','2026-05-31:Eid al-Adha','2026-06-01:Eid al-Adha','2026-06-02:Eid al-Adha','2026-06-19:Hijri New Year','2026-12-12:Prophet Birthday'],
    'Lithuania':        ['01-01:New Year','02-16:Restoration of State','03-11:Restoration of Independence','05-01:Labor Day','06-24:Midsummer','07-06:Statehood','08-15:Assumption','11-01:All Saints','11-02:All Souls','12-24:Christmas Eve','12-25:Christmas','12-26:St Stephen','2026-04-05:Easter','2026-04-06:Easter Monday','2026-05-03:Mother Day'],
    'Qatar':            ['12-18:National Day','2026-03-22:Eid al-Fitr','2026-03-23:Eid al-Fitr','2026-03-24:Eid al-Fitr','2026-05-31:Eid al-Adha','2026-06-01:Eid al-Adha','2026-06-02:Eid al-Adha','2026-06-03:Eid al-Adha'],
    'Pakistan':         ['02-05:Kashmir Day','03-23:Pakistan Day','05-01:Labor Day','08-14:Independence','11-09:Iqbal Day','12-25:Quaid-e-Azam','2026-03-22:Eid al-Fitr','2026-05-31:Eid al-Adha','2026-06-01:Eid al-Adha','2026-06-02:Eid al-Adha'],
    'Bangladesh':       ['02-21:Language Movement','03-17:Mujib Birthday','03-26:Independence','04-14:Bengali New Year','05-01:May Day','08-15:National Mourning','12-16:Victory Day','12-25:Christmas','2026-03-22:Eid al-Fitr','2026-04-13:Bengali New Year','2026-05-31:Eid al-Adha','2026-06-01:Eid al-Adha'],
    'Sri Lanka':        ['02-04:Independence','05-01:Labor Day','05-22:Vesak','12-25:Christmas','2026-04-13:Sinhala New Year','2026-04-14:Sinhala New Year','2026-05-23:Vesak','2026-08-08:Esala Poya'],
    'Iraq':             ['01-01:New Year','01-06:Army Day','05-01:Labor Day','07-14:Republic Day','10-03:National Day','2026-03-22:Eid al-Fitr','2026-05-31:Eid al-Adha','2026-06-01:Eid al-Adha'],
};

// Easter-derived (Christian) movable holidays are fully computable for
// any year via the Anonymous Gregorian algorithm — so we never have to
// hand-type Good Friday / Easter Monday / Ascension / Whit Monday /
// Corpus Christi / Maundy Thursday again, and they can't drift or get a
// typo. A market "observes" one only if its curated list already names
// it (so we don't invent holidays for markets that don't take them);
// the computed date then applies for every year, not just the hand-
// entered one. Lunar/Islamic holidays (Vesak, Eid, Lunar New Year,
// Buddha's Birthday, Diwali …) need an ephemeris and stay curated —
// _checkHolidayDataFreshness() warns when that table runs out.
function _easterSunday(year) {
    const a = year % 19, b = Math.floor(year / 100), c = year % 100;
    const d = Math.floor(b / 4), e = b % 4, f = Math.floor((b + 8) / 25);
    const g = Math.floor((b - f + 1) / 3);
    const h = (19 * a + b - d - g + 15) % 30;
    const i = Math.floor(c / 4), k = c % 4;
    const l = (32 + 2 * e + 2 * i - h - k) % 7;
    const m = Math.floor((a + 11 * h + 22 * l) / 451);
    const month = Math.floor((h + l - 7 * m + 114) / 31);
    const day = ((h + l - 7 * m + 114) % 31) + 1;
    return new Date(Date.UTC(year, month - 1, day));
}
// name → offset in days from Easter Sunday
const _EASTER_OFFSETS = {
    'Maundy Thursday': -3, 'Good Friday': -2, 'Easter Saturday': -1,
    'Easter Monday': 1, 'Ascension': 39, 'Whit Monday': 50,
    'Pentecost Monday': 50, 'Corpus Christi': 60,
};
const _easterCache = {};
function _easterMMDDForYear(year, name) {
    const off = _EASTER_OFFSETS[name];
    if (off === undefined) return null;
    const key = year + '|' + name;
    if (_easterCache[key] !== undefined) return _easterCache[key];
    const es = _easterSunday(year);
    const d = new Date(es); d.setUTCDate(es.getUTCDate() + off);
    const mmdd = String(d.getUTCMonth() + 1).padStart(2, '0') + '-'
        + String(d.getUTCDate()).padStart(2, '0');
    _easterCache[key] = mmdd;
    return mmdd;
}

function _exchangeHolidayName(exDate, holidayList) {
    if (!holidayList) return null;
    const mmdd = String(exDate.getMonth() + 1).padStart(2, '0') + '-'
        + String(exDate.getDate()).padStart(2, '0');
    const yyyymmdd = exDate.getFullYear() + '-' + mmdd;
    for (const h of holidayList) {
        const colonIdx = h.indexOf(':');
        const datePart = colonIdx >= 0 ? h.slice(0, colonIdx) : h;
        const namePart = colonIdx >= 0 ? h.slice(colonIdx + 1) : '';
        // 1. Exact match against a curated entry (fixed MM-DD or a
        //    specific YYYY-MM-DD lunar/Islamic date).
        if (datePart === mmdd || datePart === yyyymmdd) {
            return namePart || 'holiday';
        }
        // 2. Easter-derived: if this market lists the holiday by name,
        //    honour the *computed* date for exDate's year — works for
        //    every year regardless of which year was hand-entered.
        if (namePart && _EASTER_OFFSETS[namePart] !== undefined) {
            if (_easterMMDDForYear(exDate.getFullYear(), namePart) === mmdd) {
                return namePart;
            }
        }
    }
    return null;
}
function _isExchangeHoliday(exDate, holidayList) {
    return _exchangeHolidayName(exDate, holidayList) !== null;
}

// Guard rail for the holidays that CAN'T be computed (lunar/Islamic):
// scan the curated table for the latest year that has explicit
// YYYY-MM-DD entries. If we're already in (or past) that year, the
// movable dates for the next year are missing and exchange status will
// silently go wrong — surface a visible banner + console warning so the
// table gets topped up before that happens.
function _checkHolidayDataFreshness() {
    try {
        let maxYear = 0;
        for (const k in EXCHANGE_HOLIDAYS) {
            for (const h of EXCHANGE_HOLIDAYS[k]) {
                const m = /^(\\d{4})-/.exec(h);
                if (m) { const y = +m[1]; if (y > maxYear) maxYear = y; }
            }
        }
        if (!maxYear) return;
        const now = new Date();
        const curY = now.getFullYear(), curM = now.getMonth() + 1;
        let msg = null;
        if (curY > maxYear) {
            msg = 'Lunar / Islamic holiday dates (Vesak, Eid, Lunar New Year, '
                + "Buddha's Birthday, Diwali) only go through " + maxYear
                + '. ' + curY + ' is missing — those markets may show the wrong '
                + 'open/closed status on holidays. Update EXCHANGE_HOLIDAYS in '
                + 'dashboard.py. (Easter-based holidays are computed automatically.)';
        } else if (curY === maxYear && curM >= 10) {
            msg = 'Lunar / Islamic holiday dates only go through ' + maxYear
                + '. Add ' + (curY + 1) + ' dates before year-end so early-'
                + (curY + 1) + ' exchange status stays correct.';
        }
        if (!msg) return;
        console.warn('[holiday-data] ' + msg);
        if (document.getElementById('holiday-stale-banner')) return;
        const b = document.createElement('div');
        b.id = 'holiday-stale-banner';
        b.style.cssText = 'margin:0.6rem 1rem;padding:0.6rem 0.9rem;'
            + 'background:var(--amber-dim,#3a2f12);border:1px solid #f5a623;'
            + 'border-radius:8px;color:var(--text,#e2e4ea);font-size:0.8rem;'
            + 'display:flex;gap:0.6rem;align-items:flex-start';
        b.innerHTML = '<span>⚠</span><span style="flex:1">' + msg + '</span>'
            + '<span style="cursor:pointer;color:var(--text-muted)" '
            + 'onclick="this.parentElement.remove()">✕</span>';
        const c = document.querySelector('.container') || document.body;
        c.insertBefore(b, c.firstChild);
    } catch (e) { /* never break render on the guard rail */ }
}
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', _checkHolidayDataFreshness);
} else {
    _checkHolidayDataFreshness();
}

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
    const holidayName = _exchangeHolidayName(exTime, EXCHANGE_HOLIDAYS[exCode]);
    const isHoliday = holidayName !== null;
    // Lunch break (mostly Asian markets). When defined, the trading
    // day is split into AM (open → lunchStart) and PM (lunchEnd → close).
    let lunchStartMins = null, lunchEndMins = null;
    if (info.lunch) {
        const [lsh, lsm] = info.lunch[0].split(':').map(Number);
        const [leh, lem] = info.lunch[1].split(':').map(Number);
        lunchStartMins = lsh * 60 + lsm;
        lunchEndMins = leh * 60 + lem;
    }
    const inLunch = lunchStartMins !== null
        && exMins >= lunchStartMins && exMins < lunchEndMins;
    const isOpen = isTradeDay && !isHoliday
        && exMins >= openMins && exMins < closeMins && !inLunch;
    const isOnBreak = isTradeDay && !isHoliday && inLunch;

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
    const localLunchStartStr = info.lunch ? exTimeToLocal(
        Number(info.lunch[0].split(':')[0]),
        Number(info.lunch[0].split(':')[1])) : null;
    const localLunchEndStr = info.lunch ? exTimeToLocal(
        Number(info.lunch[1].split(':')[0]),
        Number(info.lunch[1].split(':')[1])) : null;

    function fmtCountdown(mins) {
        const h = Math.floor(mins / 60);
        const m = mins % 60;
        return h > 0 ? h + 'h ' + m + 'm' : m + 'm';
    }

    if (isOnBreak) {
        const minsToResume = lunchEndMins - exMins;
        return {
            isOpen: false,
            onBreak: true,
            label: 'LUNCH BREAK',
            detail: info.name + ' · resumes in ' + fmtCountdown(minsToResume)
                + ' (at ' + localLunchEndStr + ' local)'
        };
    }
    if (isOpen) {
        // If a lunch break is coming up before close, show that as the
        // next event; otherwise show the close countdown.
        if (lunchStartMins !== null && exMins < lunchStartMins) {
            const minsToLunch = lunchStartMins - exMins;
            return {
                isOpen: true,
                label: 'OPEN',
                detail: info.name + ' · lunch break in ' + fmtCountdown(minsToLunch)
                    + ' (' + localLunchStartStr + '–' + localLunchEndStr + ' local)'
            };
        }
        const minsLeft = closeMins - exMins;
        return {
            isOpen: true,
            label: 'OPEN',
            detail: info.name + ' · closes in ' + fmtCountdown(minsLeft)
                + ' (at ' + localCloseStr + ' local)'
        };
    } else {
        let nextInfo = '';
        const dayNames = ['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'];
        if (isHoliday) {
            let nextDate = new Date(exTime);
            for (let i = 1; i <= 14; i++) {
                nextDate = new Date(exTime);
                nextDate.setDate(exTime.getDate() + i);
                if (info.days.includes(nextDate.getDay())
                    && !_isExchangeHoliday(nextDate, EXCHANGE_HOLIDAYS[exCode])) {
                    break;
                }
            }
            nextInfo = 'closed for ' + holidayName + ' · opens ' + localOpenStr
                + ' local (' + dayNames[nextDate.getDay()] + ')';
        } else if (!isTradeDay || exMins >= closeMins) {
            let nextDate = new Date(exTime);
            for (let i = 1; i <= 14; i++) {
                nextDate = new Date(exTime);
                nextDate.setDate(exTime.getDate() + i);
                if (info.days.includes(nextDate.getDay())
                    && !_isExchangeHoliday(nextDate, EXCHANGE_HOLIDAYS[exCode])) {
                    break;
                }
            }
            nextInfo = 'opens ' + localOpenStr + ' local (' + dayNames[nextDate.getDay()] + ')';
        } else {
            nextInfo = 'opens at ' + localOpenStr + ' local';
        }
        return {
            isOpen: false,
            label: isHoliday ? 'CLOSED · ' + holidayName : 'CLOSED',
            detail: info.name + ' · ' + nextInfo
        };
    }
}

// Update exchange status displays.
// Populates the status slot on EVERY exchange panel that's currently
// in the DOM, not just the one selected exchange — so when stocks
// are grouped by exchange the user sees at-a-glance which markets
// are open / on lunch / closed without filtering down. The
// `activeExchanges` argument is preserved for back-compat (callers
// still pass it) but no longer affects which panels get a status.
function updateExchangeStatuses(activeExchanges) {
    Object.keys(EXCHANGE_HOURS).forEach(ex => {
        const el = document.getElementById('exstatus-' + exSlug(ex));
        if (!el) return;   // panel not rendered (e.g. ungrouped layout)
        const st = getExchangeStatus(ex);
        if (!st) { el.innerHTML = ''; return; }
        const dotCls = st.isOpen ? 'open' : (st.onBreak ? 'break' : 'closed');
        const lblCls = st.isOpen ? 'status-label-open'
            : (st.onBreak ? 'status-label-break' : 'status-label-closed');
        el.innerHTML = '<span class="status-dot ' + dotCls + '"></span>' +
            '<span class="' + lblCls + '">' + st.label + '</span>' +
            '<span class="status-text">' + st.detail + '</span>';
    });
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
    // Summary strip refreshes when active (IntersectionObserver
    // controls visibility; we just keep content fresh).
    if (typeof _renderStocksSummary === 'function') {
        const strip = document.getElementById('stocks-summary-strip');
        if (strip && strip.style.display !== 'none') _renderStocksSummary();
    }
}

// ── Density modes: chip (default), line (one-row-per-stock), graph ──
// "graph" used to be called "mini" — kept as legacy alias in stored
// preferences so existing users don't lose their setting.
// First-time visitors auto-default to "line" when the watchlist is
// >30 stocks so big watchlists stay scannable.
const _DENSITY_AUTO_THRESHOLD = 30;
function setDensity(mode, skipSave) {
    if (mode === 'mini') mode = 'graph';   // back-compat alias
    if (mode !== 'chip' && mode !== 'line' && mode !== 'graph') mode = 'chip';
    const wasGraph = document.body.classList.contains('density-graph');
    document.body.classList.remove('density-chip', 'density-line', 'density-graph');
    document.body.classList.add('density-' + mode);
    document.querySelectorAll('.density-pill').forEach(p => {
        p.classList.toggle('active', p.dataset.density === mode);
    });
    if (!skipSave) localStorage.setItem('ee-stock-density', mode);
    // Density-aware chip-change pill: in Graph mode it shows the
    // cumulative % move from the start of the selected timescale;
    // elsewhere it shows the day's % move (the server-rendered value).
    if (mode === 'graph') {
        // setGraphRange will overwrite the pill with cumulative %.
        const saved = localStorage.getItem('ee-graph-range') || '90';
        if (typeof setGraphRange === 'function') setGraphRange(saved, true);
    } else if (wasGraph) {
        document.querySelectorAll('.stock-chip').forEach(_restoreChipChange);
    }
}

// Stash original (server-rendered, daily-%) chip-change values so we
// can restore them when the user leaves Graph mode.
function _restoreChipChange(chip) {
    const el = chip.querySelector('.stock-chip-change');
    if (!el) return;
    if (el.dataset.dailyText != null) {
        el.textContent = el.dataset.dailyText;
        el.className   = el.dataset.dailyClass || el.className;
        delete el.dataset.dailyText;
        delete el.dataset.dailyClass;
    }
}
function _setCumulativeChipChange(chip, hist) {
    if (!hist || hist.length < 2) return;
    const first = hist[0][1];
    const last  = hist[hist.length - 1][1];
    if (!(first > 0)) return;
    const pct = (last - first) / first * 100;
    const el = chip.querySelector('.stock-chip-change');
    if (!el) return;
    if (el.dataset.dailyText == null) {
        el.dataset.dailyText  = el.textContent;
        el.dataset.dailyClass = el.className;
    }
    el.textContent = (pct >= 0 ? '+' : '') + pct.toFixed(1) + '%';
    const trend = (pct >  0.05) ? 'up'
                : (pct < -0.05) ? 'down' : 'flat';
    el.className = 'stock-chip-change ' + trend;
}

// ── Sparkline toggle (📈 Charts pill) ─────────────────────────────
// Adds body.show-charts which lights up the per-chip SVG sparkline
// in Chips and Lines modes. Mini mode always shows them (CSS), so
// the toggle has no visible effect there. Choice persists.
function toggleCharts(skipSave) {
    const on = !document.body.classList.contains('show-charts');
    document.body.classList.toggle('show-charts', on);
    const btn = document.getElementById('charts-toggle');
    if (btn) btn.classList.toggle('active', on);
    if (!skipSave) localStorage.setItem('ee-stock-charts', on ? '1' : '0');
}
// Restore on load
(function _restoreCharts() {
    if (localStorage.getItem('ee-stock-charts') === '1') {
        document.body.classList.add('show-charts');
        const btn = document.getElementById('charts-toggle');
        if (btn) btn.classList.add('active');
    }
})();

// ── Light / dark mode toggle ─────────────────────────────────────
// Class lives on <html> so an inline <head> script can apply it
// before <body> paints (no FOUC). Choice persists in localStorage.
function _applyThemeIcon() {
    const btn = document.getElementById('theme-toggle');
    if (!btn) return;
    btn.textContent = document.documentElement.classList.contains('light-mode') ? '☀️' : '🌙';
}
function toggleTheme(skipSave) {
    const light = !document.documentElement.classList.contains('light-mode');
    document.documentElement.classList.toggle('light-mode', light);
    if (!skipSave) localStorage.setItem('ee-theme', light ? 'light' : 'dark');
    _applyThemeIcon();
    // Chart.js doesn't auto-pick up CSS variable changes — repaint
    // the donut + portfolio chart so axis labels and lines refresh.
    try { if (window._donutChart) window._donutChart.update(); } catch (_) {}
    try { if (window.chart && typeof window.chart.update === 'function') window.chart.update(); } catch (_) {}
}
(function _restoreTheme() {
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', _applyThemeIcon);
    } else {
        _applyThemeIcon();
    }
})();

// ── Per-chip chart timescale switcher ─────────────────────────────
// History payload is lazy-loaded from /api/history on first need
// (Graphs mode, timescale switch, Index 100 toggle). Currency map
// is small (~1 KB) so it stays inline.
let _STOCK_HISTORY = {};
let _STOCK_CURRENCY = {};
let _STOCK_HISTORY_FETCH = null;   // Promise — coalesces concurrent calls
let _STOCK_HISTORY_LOADED = false;
(function _loadCurrency() {
    try {
        const curEl = document.getElementById('chart-currency');
        if (curEl) _STOCK_CURRENCY = JSON.parse(curEl.textContent || '{}');
    } catch (e) { console.warn('chart-currency parse failed', e); }
})();
function _ensureStockHistory() {
    if (_STOCK_HISTORY_LOADED) return Promise.resolve(_STOCK_HISTORY);
    if (_STOCK_HISTORY_FETCH)  return _STOCK_HISTORY_FETCH;
    _STOCK_HISTORY_FETCH = fetch('/api/history?days=365')
        .then(r => r.ok ? r.json() : {})
        .then(data => {
            _STOCK_HISTORY = data || {};
            _STOCK_HISTORY_LOADED = true;
            return _STOCK_HISTORY;
        })
        .catch(err => {
            console.warn('history fetch failed', err);
            _STOCK_HISTORY = {};
            _STOCK_HISTORY_LOADED = true;
            return _STOCK_HISTORY;
        })
        .finally(() => { _STOCK_HISTORY_FETCH = null; });
    return _STOCK_HISTORY_FETCH;
}
if (typeof window !== 'undefined') {
    const _prewarm = () => { setTimeout(_ensureStockHistory, 50); };
    if (document.readyState === 'complete') _prewarm();
    else window.addEventListener('load', _prewarm, { once: true });
}

function _fmtPriceCompact(p) {
    if (p == null) return '';
    if (p >= 1000) return p.toLocaleString('en-US', {maximumFractionDigits: 0});
    if (p >= 100)  return p.toFixed(0);
    if (p >= 10)   return p.toFixed(1);
    if (p >= 1)    return p.toFixed(2);
    return p.toFixed(3);
}
function _fmtDateShort(iso, includeYear) {
    const dt = new Date(iso + 'T00:00:00Z');
    if (isNaN(dt)) return iso || '';
    const months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
    const base = dt.getUTCDate() + ' ' + months[dt.getUTCMonth()];
    return includeYear ? base + ' ' + (dt.getUTCFullYear() % 100).toString().padStart(2, '0') : base;
}
function _renderChartSVG(history, currency, windowStartIso, windowEndIso,
                          opts) {
    // Layout / labels mirror the server-rendered _chart_svg in
    // dashboard.py — keep both in sync. Three gridlines at min/mid/max,
    // currency only on the max label, midpoint date on X-axis.
    //
    // `opts` (object, optional):
    //   indexed:     true → rebase to start = 100; Y-axis becomes index
    //                values, no currency.
    //   sharedMin/sharedMax: when set (typical with indexed=true),
    //                draw the Y-axis using these GLOBAL bounds so every
    //                chart in the grid shares a scale.
    opts = opts || {};
    const indexed = !!opts.indexed;
    const pts = history.filter(([d, p]) => p != null);
    if (pts.length < 2) return '<svg class="chip-chart" aria-hidden="true"></svg>';
    let series = pts.map(([d, p]) => [d, p]);
    if (indexed) {
        const base = pts[0][1];
        if (!(base > 0)) return '<svg class="chip-chart" aria-hidden="true"></svg>';
        series = pts.map(([d, p]) => [d, p / base * 100]);
    }
    const values = series.map(([, v]) => v);
    let pmin = Math.min(...values);
    let pmax = Math.max(...values);
    if (indexed && opts.sharedMin != null && opts.sharedMax != null) {
        pmin = opts.sharedMin;
        pmax = opts.sharedMax;
    }
    const pmid = (pmin + pmax) / 2;
    const rng = (pmax > pmin) ? (pmax - pmin) : Math.max(pmax * 0.01, 0.01);
    const width = 220, height = 120;
    const padTop = 8, padRight = 46, padLeft = 6, padBottom = 16;
    const plotW = width - padLeft - padRight;
    const plotH = height - padTop - padBottom;
    const dStart = windowStartIso || pts[0][0];
    const dEnd   = windowEndIso   || pts[pts.length - 1][0];
    const tStart = Date.parse(dStart + 'T00:00:00Z');
    const tEnd   = Date.parse(dEnd   + 'T00:00:00Z');
    const tSpan  = (tEnd > tStart) ? (tEnd - tStart) : 1;
    const tMid   = (tStart + tEnd) / 2;
    const dMid   = new Date(tMid).toISOString().slice(0, 10);
    const coords = series.map(([d, v]) => {
        const t = Date.parse(d + 'T00:00:00Z');
        const x = padLeft + (t - tStart) / tSpan * plotW;
        const y = padTop + plotH - (v - pmin) / rng * plotH;
        return x.toFixed(1) + ',' + y.toFixed(1);
    }).join(' ');
    const color = (values[values.length - 1] >= values[0])
        ? 'var(--green)' : 'var(--red)';
    // Currency prefix only in price mode; indexed mode is unitless.
    const cur = (currency || '').trim();
    const curPre = (!indexed && cur) ? cur + ' ' : '';
    // In indexed mode, label values without decimals (whole index pts).
    const fmtVal = indexed
        ? (v) => Math.round(v).toString()
        : (v) => _fmtPriceCompact(v);
    const yTop = padTop;
    const yMid = padTop + plotH / 2;
    const yBot = padTop + plotH;
    const xLeft = padLeft;
    const xMid = padLeft + plotW / 2;
    const xRight = padLeft + plotW;
    const labelX = width - 4;
    const crossYear = dStart.slice(0,4) !== dEnd.slice(0,4);
    // Optional baseline at 100 in indexed mode — the "no change" line.
    let baselineSvg = '';
    if (indexed && pmin <= 100 && pmax >= 100) {
        const yBase = padTop + plotH - (100 - pmin) / rng * plotH;
        baselineSvg = '<line x1="' + xLeft + '" y1="' + yBase.toFixed(1) +
            '" x2="' + xRight.toFixed(1) + '" y2="' + yBase.toFixed(1) +
            '" stroke="var(--text-muted)" stroke-width="0.6" ' +
            'stroke-dasharray="3,3" opacity="0.55"/>' +
            '<text x="' + (xLeft + 2) + '" y="' + (yBase - 2).toFixed(1) +
            '" font-size="8" fill="var(--text-muted)" opacity="0.7">100</text>';
    }
    // In indexed mode, allow outlier polylines to escape the SVG so
    // a single 5×-bagger doesn't have to be clamped (which would just
    // glue a flat line to the top edge). In price mode, default clip.
    const overflowAttr = indexed ? ' overflow="visible"' : '';
    return (
        '<svg class="chip-chart" viewBox="0 0 ' + width + ' ' + height + '"' +
        overflowAttr + ' aria-hidden="true">' +
        // Gridlines (max / mid / min)
        '<line x1="' + xLeft + '" y1="' + yTop +
        '" x2="' + xRight.toFixed(1) + '" y2="' + yTop +
        '" stroke="var(--border)" stroke-width="0.5" stroke-dasharray="2,3" opacity="0.55"/>' +
        '<line x1="' + xLeft + '" y1="' + yMid.toFixed(1) +
        '" x2="' + xRight.toFixed(1) + '" y2="' + yMid.toFixed(1) +
        '" stroke="var(--border)" stroke-width="0.5" stroke-dasharray="2,3" opacity="0.4"/>' +
        '<line x1="' + xLeft + '" y1="' + yBot.toFixed(1) +
        '" x2="' + xRight.toFixed(1) + '" y2="' + yBot.toFixed(1) +
        '" stroke="var(--border)" stroke-width="0.6"/>' +
        baselineSvg +
        // Polyline
        '<polyline points="' + coords + '" fill="none" stroke="' + color +
        '" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"/>' +
        // Y-axis labels (right-aligned)
        '<text x="' + labelX + '" y="' + (yTop + 3.5).toFixed(1) +
        '" font-size="9.5" font-weight="600" fill="var(--text-muted)" text-anchor="end">' +
        curPre + fmtVal(pmax) + '</text>' +
        '<text x="' + labelX + '" y="' + (yMid + 3.5).toFixed(1) +
        '" font-size="9.5" fill="var(--text-muted)" text-anchor="end" opacity="0.75">' +
        fmtVal(pmid) + '</text>' +
        '<text x="' + labelX + '" y="' + (yBot + 3.5).toFixed(1) +
        '" font-size="9.5" font-weight="600" fill="var(--text-muted)" text-anchor="end">' +
        fmtVal(pmin) + '</text>' +
        // X-axis labels (bottom)
        '<text x="' + xLeft + '" y="' + (height - 3) +
        '" font-size="9.5" fill="var(--text-muted)">' +
        _fmtDateShort(dStart, crossYear) + '</text>' +
        '<text x="' + xMid.toFixed(1) + '" y="' + (height - 3) +
        '" font-size="9.5" fill="var(--text-muted)" text-anchor="middle" opacity="0.75">' +
        _fmtDateShort(dMid, crossYear) + '</text>' +
        '<text x="' + xRight.toFixed(1) + '" y="' + (height - 3) +
        '" font-size="9.5" fill="var(--text-muted)" text-anchor="end">' +
        _fmtDateShort(dEnd, crossYear) + '</text>' +
        '</svg>'
    );
}
// Per-ticker cache of the data CURRENTLY rendered in each chip-chart,
// plus the time-axis bounds. Drives the hover tooltip — needs the
// same filtering as the visible polyline so the snapped point lines
// up with what the user sees.
let _CHART_HOVER_DATA = {};

function _setChartHoverData(tk, hist, winStart, winEnd, sharedMin, sharedMax) {
    if (!hist || hist.length < 2) {
        delete _CHART_HOVER_DATA[tk];
        return;
    }
    _CHART_HOVER_DATA[tk] = {
        hist: hist, winStart: winStart, winEnd: winEnd,
        sharedMin: (sharedMin != null) ? sharedMin : null,
        sharedMax: (sharedMax != null) ? sharedMax : null,
    };
}

function setGraphRange(range, skipSave) {
    // History is fetched on demand from /api/history. If it hasn't
    // arrived yet, defer the render — the active timescale pill is
    // still updated immediately so the UI feels responsive.
    if (!_STOCK_HISTORY_LOADED) {
        document.querySelectorAll('.graph-range-pill').forEach(p => {
            p.classList.toggle('active', String(p.dataset.range) === String(range));
        });
        _ensureStockHistory().then(() => setGraphRange(range, skipSave));
        return;
    }
    const isAll = (range === 'all' || range === 'ALL');
    const todayIso = new Date().toISOString().slice(0, 10);
    let cutoffIso = null;
    if (!isAll) {
        const days = parseInt(range, 10);
        cutoffIso = new Date(Date.now() - days * 86400000)
            .toISOString().slice(0, 10);
    }
    const indexed = document.body.classList.contains('chart-indexed');

    // First pass: collect filtered history per chip and (when indexed)
    // compute the global indexed Y-range so every chart in the grid
    // shares a Y scale. Without this, each chart would auto-fit and
    // a 5%-mover would look as dramatic as a 5×-bagger.
    const chipsData = [];
    document.querySelectorAll('.stock-chip').forEach(chip => {
        const tk = chip.dataset.ticker;
        const full = _STOCK_HISTORY[tk] || [];
        const filtered = isAll ? full : full.filter(([d]) => d >= cutoffIso);
        if (filtered.length < 2) return;
        const winStart = isAll
            ? (full.length ? full[0][0] : null)
            : cutoffIso;
        chipsData.push({ chip, tk, filtered, winStart });
    });
    let sharedMin = null, sharedMax = null;
    if (indexed && chipsData.length) {
        // Percentile-based shared bounds: a single 5×-bagger would
        // otherwise stretch the Y-axis to the moon and flatten every
        // other chart into a horizontal line near 100. The 5th–95th
        // percentile across (stock, date) pairs is robust to that
        // outlier — typically 90% of values fit comfortably inside,
        // and the few outliers escape the SVG (overflow:visible).
        const allIdx = [];
        for (const cd of chipsData) {
            const base = cd.filtered[0][1];
            if (!(base > 0)) continue;
            for (const [, p] of cd.filtered) {
                allIdx.push(p / base * 100);
            }
        }
        if (allIdx.length >= 4) {
            allIdx.sort((a, b) => a - b);
            const n = allIdx.length;
            const p5  = allIdx[Math.floor(n * 0.05)];
            const p95 = allIdx[Math.min(n - 1, Math.floor(n * 0.95))];
            // Always keep at least ±10 around the 100 baseline so
            // the reference is visible even on a calm grid.
            sharedMin = Math.min(p5,  90);
            sharedMax = Math.max(p95, 110);
            // If the spread is tiny (very calm grid), pad symmetrically
            // around 100 so the chart isn't a degenerate horizontal.
            if (sharedMax - sharedMin < 10) {
                sharedMin = 100 - 5;
                sharedMax = 100 + 5;
            }
        }
    }

    // Second pass: render each chart with the (possibly shared) bounds.
    // Cumulative-from-start % is a Graph-mode affordance only — in
    // Chips / Lines we keep the server-rendered daily %.
    const inGraph = document.body.classList.contains('density-graph');
    chipsData.forEach(cd => {
        const slot = cd.chip.querySelector('.chip-chart');
        if (!slot) return;
        slot.outerHTML = _renderChartSVG(
            cd.filtered,
            _STOCK_CURRENCY[cd.tk] || '',
            cd.winStart,
            todayIso,
            { indexed, sharedMin, sharedMax }
        );
        _setChartHoverData(cd.tk, cd.filtered, cd.winStart, todayIso,
                           sharedMin, sharedMax);
        if (inGraph) _setCumulativeChipChange(cd.chip, cd.filtered);
    });
    document.querySelectorAll('.graph-range-pill').forEach(p => {
        p.classList.toggle('active', String(p.dataset.range) === String(range));
    });
    if (!skipSave) localStorage.setItem('ee-graph-range', String(range));
}

// Toggle indexed mode (rebase to 100, shared Y across all charts).
function toggleChartIndexed(skipSave) {
    const on = !document.body.classList.contains('chart-indexed');
    document.body.classList.toggle('chart-indexed', on);
    const btn = document.getElementById('chart-indexed-btn');
    if (btn) btn.classList.toggle('active', on);
    if (!skipSave) localStorage.setItem('ee-graph-indexed', on ? '1' : '0');
    // Re-render at the current range.
    const saved = localStorage.getItem('ee-graph-range') || '90';
    setGraphRange(saved, true);
}
(function _restoreChartIndexed() {
    if (localStorage.getItem('ee-graph-indexed') === '1') {
        if (document.readyState === 'loading') {
            document.addEventListener('DOMContentLoaded',
                () => toggleChartIndexed(true));
        } else {
            toggleChartIndexed(true);
        }
    }
})();
(function _restoreGraphRange() {
    const saved = localStorage.getItem('ee-graph-range');
    if (saved && saved !== '90') {
        if (document.readyState === 'loading') {
            document.addEventListener('DOMContentLoaded',
                () => setGraphRange(saved, true));
        } else {
            setGraphRange(saved, true);
        }
    }
})();

// Seed the hover cache for the SERVER-rendered charts (90-day default
// window). setGraphRange repopulates this whenever the range changes.
// History is lazy-loaded now, so we run this once the fetch resolves.
function _seedHoverCacheFromDefault() {
    const todayIso = new Date().toISOString().slice(0, 10);
    const cutoff90 = new Date(Date.now() - 90 * 86400000)
        .toISOString().slice(0, 10);
    Object.keys(_STOCK_HISTORY).forEach(tk => {
        const full = _STOCK_HISTORY[tk] || [];
        const filtered = full.filter(([d]) => d >= cutoff90);
        _setChartHoverData(tk, filtered, cutoff90, todayIso);
    });
}
_ensureStockHistory().then(_seedHoverCacheFromDefault);

// ── Hover tooltip + dot marker for chip-charts ─────────────────────
// Single shared tooltip <div> is appended to body. Mousemove on any
// .chip-chart finds the nearest data point by date and pops the tip
// + a small dot inside the SVG.
let _chartTip = null;
let _activeChartSvg = null;
function _ensureChartTip() {
    if (_chartTip) return _chartTip;
    _chartTip = document.createElement('div');
    _chartTip.className = 'chip-chart-tip';
    _chartTip.style.cssText = (
        'position:fixed;display:none;pointer-events:none;'
        + 'background:var(--surface,#181b22);color:var(--text,#e8e9ec);'
        + 'border:1px solid var(--border,#262932);padding:3px 7px;'
        + 'border-radius:4px;font-size:0.72rem;'
        + 'font-variant-numeric:tabular-nums;z-index:1000;'
        + 'box-shadow:0 4px 10px rgba(0,0,0,0.35);white-space:nowrap;'
    );
    document.body.appendChild(_chartTip);
    return _chartTip;
}
function _clearActiveDot() {
    if (_activeChartSvg) {
        const dot = _activeChartSvg.querySelector('.chart-hover-dot');
        if (dot) dot.remove();
        _activeChartSvg = null;
    }
}
function _hideChartTip() {
    if (_chartTip) _chartTip.style.display = 'none';
    _clearActiveDot();
}
function _onChartMousemove(e) {
    const svg = e.target.closest('.chip-chart');
    if (!svg) { _hideChartTip(); return; }
    const chip = svg.closest('.stock-chip');
    if (!chip) return;
    const tk = chip.dataset.ticker;
    const data = _CHART_HOVER_DATA[tk];
    if (!data || !data.hist || data.hist.length < 2) {
        _hideChartTip();
        return;
    }
    const rect = svg.getBoundingClientRect();
    if (rect.width === 0 || rect.height === 0) return;
    // viewBox = 220×120; padLeft=4, padRight=38, padTop=6, padBottom=14.
    const VBW = 220, VBH = 120;
    // Keep these in sync with the layout constants in
    // _renderChartSVG / _chart_svg above.
    const padLeft = 6, padRight = 46, padTop = 8, padBottom = 16;
    const plotW_vb = VBW - padLeft - padRight;
    const plotH_vb = VBH - padTop - padBottom;
    const x_pct = (e.clientX - rect.left) / rect.width;
    const t_pct = Math.max(0, Math.min(1,
        (x_pct - padLeft / VBW) / (plotW_vb / VBW)));
    const tStart = Date.parse(data.winStart + 'T00:00:00Z');
    const tEnd = Date.parse(data.winEnd + 'T00:00:00Z');
    const target = tStart + t_pct * (tEnd - tStart);
    let best = data.hist[0], bestDiff = Infinity;
    for (const row of data.hist) {
        const diff = Math.abs(Date.parse(row[0] + 'T00:00:00Z') - target);
        if (diff < bestDiff) { best = row; bestDiff = diff; }
    }
    // Position the in-SVG dot at the snapped point. Y math must match
    // the chart's actual mode (price vs indexed) so the dot lands on
    // the visible polyline.
    const indexedMode = document.body.classList.contains('chart-indexed');
    let series = data.hist;
    let bestVal = best[1];
    if (indexedMode && data.hist[0][1] > 0) {
        const base = data.hist[0][1];
        series = data.hist.map(([d, p]) => [d, p / base * 100]);
        bestVal = best[1] / base * 100;
    }
    let pmin, pmax;
    if (indexedMode && data.sharedMin != null && data.sharedMax != null) {
        pmin = data.sharedMin; pmax = data.sharedMax;
    } else {
        const vals = series.map(r => r[1]);
        pmin = Math.min(...vals);
        pmax = Math.max(...vals);
    }
    const rng = (pmax > pmin) ? (pmax - pmin) : Math.max(pmax * 0.01, 0.01);
    const tBest = Date.parse(best[0] + 'T00:00:00Z');
    const tSpan = (tEnd > tStart) ? (tEnd - tStart) : 1;
    const cx = padLeft + (tBest - tStart) / tSpan * plotW_vb;
    const cy = padTop + plotH_vb - (bestVal - pmin) / rng * plotH_vb;
    if (_activeChartSvg && _activeChartSvg !== svg) _clearActiveDot();
    let dot = svg.querySelector('.chart-hover-dot');
    if (!dot) {
        dot = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
        dot.setAttribute('class', 'chart-hover-dot');
        dot.setAttribute('r', '2.8');
        dot.setAttribute('fill', 'var(--text,#e8e9ec)');
        dot.setAttribute('stroke', 'var(--bg,#0f1117)');
        dot.setAttribute('stroke-width', '1');
        svg.appendChild(dot);
    }
    dot.setAttribute('cx', cx.toFixed(1));
    dot.setAttribute('cy', cy.toFixed(1));
    _activeChartSvg = svg;
    // Tooltip text: "5 May 2026 · USD 542.21"
    const dt = new Date(best[0] + 'T00:00:00Z');
    const months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
    const dateStr = dt.getUTCDate() + ' ' + months[dt.getUTCMonth()]
        + ' ' + dt.getUTCFullYear();
    const cur = (_STOCK_CURRENCY[tk] || '').trim();
    const indexed = document.body.classList.contains('chart-indexed');
    const tip = _ensureChartTip();
    if (indexed && data.hist.length && data.hist[0][1] > 0) {
        const base = data.hist[0][1];
        const idx = best[1] / base * 100;
        tip.textContent = dateStr + '  ·  Idx ' + idx.toFixed(1)
            + '  ·  ' + (cur ? cur + ' ' : '') + _fmtPriceCompact(best[1]);
    } else {
        tip.textContent = dateStr + '  ·  '
            + (cur ? cur + ' ' : '') + _fmtPriceCompact(best[1]);
    }
    tip.style.display = 'block';
    // Position above-right of cursor; flip to left if it would clip.
    let tx = e.clientX + 12;
    let ty = e.clientY - 30;
    const tipW = tip.offsetWidth || 120;
    if (tx + tipW > window.innerWidth - 8) tx = e.clientX - tipW - 12;
    if (ty < 4) ty = e.clientY + 16;
    tip.style.left = tx + 'px';
    tip.style.top  = ty + 'px';
}
document.addEventListener('mousemove', _onChartMousemove);
document.addEventListener('mouseleave', _hideChartTip, true);
// Hide on scroll so the tooltip doesn't drift away from the cursor.
window.addEventListener('scroll', _hideChartTip, true);
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

// ── Stock panels: per-country collapse (headers clickable → pills) ──
// The old top-level "▼ Stocks" toggle was removed; its purpose is
// now served by "Collapse all" which folds every country into a
// compact pill row — a strictly better state than fully hiding
// the grid. A summary strip still appears above the fold when
// the grid is scrolled out of view (IntersectionObserver below).
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
function _initStocksCollapsed() {
    // On first visit (no saved per-panel state) AND large watchlist,
    // collapse every country panel into the pill row so the user
    // lands on a compact view and can cherry-pick which countries
    // to expand. Small watchlists stay expanded by default.
    const savedPanels = localStorage.getItem('ee-panels-collapsed');
    const count = document.querySelectorAll('.stock-chip').length;
    if (savedPanels === null && count > _STOCKS_AUTO_COLLAPSE_THRESHOLD) {
        setAllPanelsCollapsed(true);
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

// Populate the per-panel "exchange-status" badges on initial load and
// then keep them current. The original updateExchangeStatuses call
// site only fired when the user changed a filter — leaving every
// panel header blank on first paint. Refresh every 60s so countdowns
// ("closes in 1h 22m") tick down without a page reload.
function _refreshAllExchangeStatuses() {
    if (typeof updateExchangeStatuses !== 'function') return;
    try {
        const actives = (typeof getActiveExchanges === 'function')
            ? getActiveExchanges() : [];
        updateExchangeStatuses(actives);
    } catch (_) { /* swallow — bad clock state, etc. */ }
}
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', _refreshAllExchangeStatuses);
} else {
    _refreshAllExchangeStatuses();
}
setInterval(_refreshAllExchangeStatuses, 60 * 1000);

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
    // Clearing activeTickers here means the refresh-scope dropdown
    // should also reset (it was tracking the chip filter).
    if (typeof _syncScopeFromChips === 'function') _syncScopeFromChips();
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
        if (typeof _renderSelectedExchangeChip === 'function') _renderSelectedExchangeChip();
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
    // If the user had a stock selected and the new exchange filter
    // doesn't include that stock's exchange, auto-clear the selection —
    // the sticky selected-stock pill would otherwise point at a
    // filtered-out ticker.
    if (typeof activeTickers !== 'undefined' && activeTickers.size > 0) {
        let changed = false;
        activeTickers.forEach(tk => {
            const chip = document.querySelector('.stock-chip[data-ticker="' + tk + '"]');
            const ex = chip && chip.dataset.exchange;
            if (!ex || !actives.includes(ex)) {
                activeTickers.delete(tk);
                changed = true;
            }
        });
        if (changed) {
            document.querySelectorAll('.stock-chip[data-ticker]').forEach(c => {
                c.classList.toggle('chip-active', activeTickers.has(c.dataset.ticker));
            });
            if (typeof _renderSelectedStockChip === 'function') _renderSelectedStockChip();
            if (typeof applyGlobalStockFilter === 'function') applyGlobalStockFilter();
        }
    }
    updateStockPanel(actives);
    updateStockPills(actives);
    updateExchangeStatuses(actives);
    _applyCollapsedState();
    applyNewsAgeFilter();
    _updateEmptyGroups();
    _updateSectionCounts();
    if (typeof _renderSelectedExchangeChip === 'function') _renderSelectedExchangeChip();
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

def generate_html(db: Database, config: dict, target_date: str = None,
                  view_only: bool = False) -> str:
    """Build a self-contained HTML dashboard string.

    ``view_only=True`` produces a snapshot suitable for sharing publicly
    (e.g. via Vercel deploy + password gate). All interactive controls
    that would call /api/* endpoints are hidden in CSS — Add Stock,
    Refresh Prices, the per-stock remove ✕, the bottom refresh bar,
    and links to /portfolio and /engine-room. The data on the page is
    still real; just no one can mutate or refresh it.
    """

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

    # Filter all data feeds to the current watchlist. Without this, stocks
    # the user removed from `user_stocks` keep showing up in News, Forum,
    # Earnings, and Insider sections because their historical rows in
    # news_items / forum_mentions / earnings_dates / insider_transactions
    # are not deleted when the watchlist entry goes away.
    _active_keys = {(s["ticker"].upper(), s["exchange"].upper())
                    for s in active_stocks}
    def _on_watchlist(item: dict) -> bool:
        return (
            (item.get("ticker") or "").upper(),
            (item.get("exchange") or "").upper(),
        ) in _active_keys
    news      = [n for n in news      if _on_watchlist(n)]
    contracts = [c for c in contracts if _on_watchlist(c)]
    earnings  = [e for e in earnings  if _on_watchlist(e)]
    forum     = [f for f in forum     if _on_watchlist(f)]
    insiders  = [i for i in insiders  if _on_watchlist(i)]

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
        "MSE":      "Mongolia",
        "SGX":      "Singapore",
        "KSE":      "Kyrgyzstan",
        "KASE":     "Kazakhstan",
        "AIX":      "Kazakhstan",
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
        "TSXV":     "Canada",
        "NEO":      "Canada",
        "CNSX":     "Canada",
        "CSE_CA":   "Canada",     # Canadian Securities Exchange (vs Copenhagen "CSE")
        "VAN":      "Canada",     # Vancouver — merged into TSX Venture in 1999
        "VSE":      "Canada",     # Vancouver Stock Exchange (legacy code)
        "LIT":      "Lithuania",
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
        "DFM":      "UAE",
        "ADX":      "UAE",
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
        "BVG":      "Ecuador",
        "BVC":      "Colombia",
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
        "UZSE":"asia","MSE":"asia","KSE":"asia","KASE":"asia","AIX":"asia","DSEB":"asia","PSX":"asia",
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
        "BCBA":"americas","BVS":"americas","BVG":"americas","BVC":"americas",
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

    # Geographic ordering — roughly west → east by region (Americas →
    # Europe/Africa → Middle East / Central Asia → South & East Asia →
    # Pacific). Anything not listed falls to the end alphabetically so
    # newly-added exchanges still show up.
    _GEO_ORDER = [
        # Order follows the global trading day as seen from Europe/Africa:
        # markets that open first in our morning come first; the Americas
        # come last because they're still open after we've gone to bed.
        # Pacific (opens earliest)
        'New Zealand', 'Australia', 'Papua New Guinea',
        # East Asia
        'Japan', 'South Korea', 'Taiwan', 'Hong Kong',
        'China (Shanghai)', 'China (Shenzhen)',
        # South-East Asia
        'Philippines', 'Singapore', 'Malaysia', 'Indonesia',
        'Vietnam', 'Thailand', 'Cambodia',
        # South / Central Asia
        'Bangladesh', 'Sri Lanka', 'India', 'Pakistan',
        'Mongolia', 'Kyrgyzstan', 'Kazakhstan', 'Uzbekistan',
        # Middle East
        'Oman', 'UAE', 'Qatar', 'Bahrain', 'Saudi Arabia',
        'Iraq', 'Jordan', 'Israel',
        # Africa (east → west)
        'Mauritius', 'Ethiopia', 'Kenya', 'Tanzania', 'Uganda', 'Rwanda',
        'South Africa', 'Botswana', 'Zambia', 'Egypt',
        'Nigeria', 'Ghana', 'Ivory Coast/BRVM', 'Morocco', 'Tunisia',
        # Europe (east → west)
        'Turkey', 'Greece', 'Romania', 'Ukraine', 'Finland', 'Lithuania',
        'Bulgaria', 'Serbia', 'Hungary', 'Slovakia', 'Croatia',
        'Slovenia', 'Czech Republic', 'Poland', 'Sweden', 'Denmark',
        'Norway', 'Austria', 'Italy', 'Germany', 'Switzerland',
        'Netherlands', 'Belgium', 'France', 'Spain', 'Portugal',
        'Iceland', 'Ireland', 'UK',
        # Americas (east → west — opens last from our perspective)
        'Brazil', 'Argentina', 'Chile', 'Colombia', 'Ecuador', 'Peru',
        'Mexico', 'US', 'Canada',
    ]
    _geo_idx = {name: i for i, name in enumerate(_GEO_ORDER)}
    _present = {s["_display_ex"] for s in active_stocks}
    exchanges = sorted(
        _present,
        key=lambda x: (_geo_idx.get(x, len(_GEO_ORDER)), x))

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
    # ZAc/ZAC (Johannesburg cents) → display as ZAR. JSE quotes prices in
    # cents but the meaningful FX rate for users is USD/ZAR.
    _fx_labels: set[str] = set()
    for c in portfolio_currencies:
        if not c or c.upper() == "USD":
            continue
        if c.upper() in ("ZAC", "ZAR"):
            _fx_labels.add("ZAR")
        else:
            _fx_labels.add(c)
    _portfolio_fx = sorted(_fx_labels)
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
            # A ticker can have snapshots under more than one exchange
            # code (e.g. WSTL lives under OTC with fresh stockanalysis
            # data plus a stale PNK/OID row). price_map is keyed by
            # ticker only, so without this guard a later exchange's
            # older row clobbers a fresher one. Keep the freshest.
            _prev = price_map.get(_p["ticker"])
            if _prev and (_prev.get("snapshot_at") or "") > (_p.get("snapshot_at") or ""):
                continue
            price_map[_p["ticker"]] = _p

    def _resolve_pd(s: dict) -> dict | None:
        """Resolve a chip's price across the stock's alias keys.

        KLSE names live in price_snapshots under both the alpha ticker
        (stockanalysis source) and the numeric Bursa code (klsescreener
        source); the alpha row can go stale while the code row stays
        fresh. Prefer the code-keyed row, then the yahoo-root, then the
        ticker — and within that, the freshest snapshot_at wins."""
        best = None
        for k in (s.get("code"),
                  (s.get("yahoo_ticker") or "").split(".")[0],
                  s.get("ticker")):
            if not k:
                continue
            p = price_map.get(k)
            if not p:
                continue
            if best is None or (p.get("snapshot_at") or "") > (best.get("snapshot_at") or ""):
                best = p
        return best

    # Self-heal stale prices: any stock whose freshest snapshot is older
    # than today (UTC) gets a background fetch_prices() so the next page
    # render shows up-to-date data. The cooldown inside _kick_stale_refresh
    # stops rapid reloads from re-firing the same fetches.
    try:
        from datetime import datetime as _dt_now
        _today_iso = _dt_now.utcnow().strftime("%Y-%m-%d")
        _stale_stocks: list[dict] = []
        for _s in active_stocks:
            _pd = _resolve_pd(_s)
            _snap = ((_pd or {}).get("snapshot_at") or "")[:10]
            if not _snap or _snap < _today_iso:
                _stale_stocks.append(_s)
        if _stale_stocks:
            _kick_stale_refresh(db, config, _stale_stocks)
    except Exception:
        pass  # never block page render on the self-heal path

    # Same self-heal pattern for earnings: kick fetch_earnings() in the
    # background for any watched stock whose last earnings refresh is
    # older than the cooldown. Catches new past-quarter announcements
    # (e.g. Plenitude Q3 released on a Friday) on the next page render
    # instead of waiting for the scheduled overnight run.
    try:
        _kick_stale_earnings(db, config, active_stocks)
    except Exception:
        pass

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
        "BVG": "Bolsa de Guayaquil", "BVC": "Bolsa de Colombia",
        "JSE": "Johannesburg", "NGX": "Nigerian Exchange",
        "BRVM": "BRVM (Abidjan)", "UZSE": "Tashkent",
        "MSE": "Mongolian SE", "KSE": "Kyrgyz SE",
        "KASE": "KASE", "AIX": "Astana Intl Exchange",
        "NSEK": "Nairobi", "GSE": "Ghana SE",
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

    # Batch-load up to 365 days of price history for every watched
    # ticker so each chip can carry: (a) a tiny inline sparkline used
    # by the Charts toggle, (b) an axis-labeled graph in Graph mode,
    # and (c) a JS-side timescale switcher (1M / 3M / 6M / 1Y / ALL)
    # that re-renders the graph client-side without a round trip.
    # 1Y × ~100 tickers × ~30 bytes JSON ≈ 300 KB — acceptable.
    chart_history: dict = {}   # ticker → [(date,price),...] full 1Y window
    if active_stocks:
        history_cutoff = (datetime.utcnow() - timedelta(days=365)).strftime("%Y-%m-%d")
        ticker_list = list({s["ticker"] for s in active_stocks})
        for i in range(0, len(ticker_list), 500):
            chunk = ticker_list[i:i + 500]
            placeholders = ",".join("?" * len(chunk))
            rows = db.conn.execute(
                f"SELECT ticker, snapshot_at, price FROM price_snapshots "
                f"WHERE ticker IN ({placeholders}) AND snapshot_at >= ? "
                f"ORDER BY ticker ASC, snapshot_at ASC",
                (*chunk, history_cutoff),
            ).fetchall()
            for r in rows:
                chart_history.setdefault(r["ticker"], []).append(
                    (r["snapshot_at"][:10], r["price"]))

    # Pre-slice the 90-day window for the initial server-rendered SVG.
    # The client can switch ranges later without a reload.
    spark_cutoff_90d = (datetime.utcnow() - timedelta(days=90)).strftime("%Y-%m-%d")
    def _slice_to_90d(hist):
        return [(d, p) for (d, p) in hist if d >= spark_cutoff_90d]
    spark_history = {tk: [p for _, p in _slice_to_90d(h)]
                     for tk, h in chart_history.items()}

    # chart_history is still used to render the inline per-chip SVG
    # (last 90 days) below. The 365-day JSON is no longer embedded —
    # it's served by /api/history on demand. See get_chart_history_json.
    chart_currency_json = json.dumps(
        {s["ticker"]: (_resolve_pd(s) or {}).get("currency", "")
         for s in active_stocks},
        separators=(",", ":"),
    )

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
            pd = _resolve_pd(s)
            if pd and pd.get("price") is not None:
                pct = pd.get("change_pct", 0) or 0
                if pct > 0:
                    chg_cls, chg_prefix = "up", "+"
                elif pct < 0:
                    chg_cls, chg_prefix = "down", ""
                else:
                    chg_cls, chg_prefix = "flat", ""
                # Staleness: when the latest snapshot is from a previous
                # day, show a small "as of YYYY-MM-DD" tag and dim the
                # change %. Yahoo throttles us intermittently per-IP, so
                # a refresh can fail silently and leave yesterday's
                # number on the chip — without this tag the user has no
                # way to know the price isn't today's.
                today_str = datetime.utcnow().strftime("%Y-%m-%d")
                snap = (pd.get("snapshot_at") or "")[:10]
                stale = bool(snap) and snap < today_str
                stale_attr = ' data-stale="1"' if stale else ''
                stale_tag = (f'<span class="stock-chip-stale" '
                             f'title="Price refresh failed today — showing last successful '
                             f'value from {snap}. Try Free refresh again.">⚠ {snap}</span>'
                             if stale else '')
                price_line = f"""<div class="stock-chip-price"{stale_attr}>{_esc(pd.get('currency',''))} {_fmt_price(pd['price'])}
                    <span class="stock-chip-change {chg_cls}">{chg_prefix}{pct:.1f}%</span>{stale_tag}</div>"""
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
            spark_html = _spark_svg(spark_history.get(s["ticker"], []))
            chart_currency = (_resolve_pd(s) or {}).get("currency", "")
            # Initial server render uses the 90-day window. JS can
            # rebuild from the embedded full-1Y history when the user
            # picks a different timescale (see setGraphRange below).
            today_iso = datetime.utcnow().strftime("%Y-%m-%d")
            chart_html = _chart_svg(_slice_to_90d(chart_history.get(s["ticker"], [])),
                                    currency=chart_currency,
                                    window_start=spark_cutoff_90d,
                                    window_end=today_iso)
            chips.append(f"""
            <div class="stock-chip" data-exchange="{_esc(ex)}" data-ticker="{_esc(s['ticker'])}" title="{_esc(s['name'])}">
                <span class="stock-chip-remove" title="Remove from watchlist"
                      onclick="removeStockFromWatchlist('{_esc(s['ticker'])}', '{_esc(s['exchange'])}', '{_esc(s['name'])}')">✕</span>
                <div class="stock-chip-name">{_esc(s['name'])}</div>
                <div class="stock-chip-ticker"><span class="tk-sym">{_esc(s['ticker'])}</span>{(' <span class="tk-sep">·</span> <span class="tk-code">' + _esc(s.get('code','')) + '</span>') if s.get('code') and s.get('code') != s.get('ticker') else ''}</div>
                {price_line}
                {spark_html}
                {chart_html}
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
            <p>Your monitor is empty. Add your first stock to start tracking news,
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

    # ── Earnings-report alerts ─────────────────────────────────────────
    # Fires when a watched stock has a past-earnings row whose date is
    # within ALERT_MAX_AGE_DAYS of today. We also do a quick sentiment
    # pass: look at news items within ±2 days of the report date, scan
    # for beat/miss/surge/drop keywords, and tag the alert accordingly.
    # Skip rows labelled "(est)" / "(proj)" — those are projections, not
    # confirmed announcements, so an alert would be misleading.
    _today_date = datetime.now().date()
    _POS_KW = (
        "beat", "beats", "tops", "surge", "surges", "soar", "soars",
        "rises", "jumps", "above expectations", "above estimate",
        "above estimates", "raises guidance", "record profit",
        "strong results", "earnings beat",
    )
    _NEG_KW = (
        "miss", "misses", "below expectations", "below estimate",
        "below estimates", "fall", "falls", "drop", "drops", "plunges",
        "tumbles", "cuts guidance", "lower guidance", "loss widens",
        "earnings miss", "disappoint", "disappoints", "warning",
        "weaker than", "slumps",
    )

    def _sentiment_for(tk: str, rd: str) -> tuple[str, str]:
        """Return (label, css_class) for a recent earnings announcement.
        Scans `news` (already in scope) for any item published within
        ±2 days of the report_date that mentions a beat/miss keyword."""
        try:
            r_dt = datetime.strptime(rd, "%Y-%m-%d").date()
        except (TypeError, ValueError):
            return ("", "")
        pos = neg = 0
        sample_pos = sample_neg = ""
        for n in news or []:
            if (n.get("ticker") or "").upper() != tk.upper():
                continue
            p = (n.get("published") or "").strip()
            n_dt = None
            for fmt in ("%a, %d %b %Y %H:%M:%S %Z",
                        "%a, %d %b %Y %H:%M:%S GMT",
                        "%Y-%m-%d", "%b %d, %Y", "%d %b %Y", "%d %B %Y"):
                try:
                    n_dt = datetime.strptime(p[:30].strip(), fmt).date()
                    break
                except ValueError:
                    continue
            if n_dt is None:
                continue
            if abs((n_dt - r_dt).days) > 3:
                continue
            text = ((n.get("title") or "") + " "
                    + (n.get("snippet") or "")).lower()
            if any(k in text for k in _POS_KW):
                pos += 1
                if not sample_pos:
                    sample_pos = _strip_html(n.get("title") or "")[:80]
            if any(k in text for k in _NEG_KW):
                neg += 1
                if not sample_neg:
                    sample_neg = _strip_html(n.get("title") or "")[:80]
        if pos and not neg:
            return (f"📈 Positive — {sample_pos}" if sample_pos
                    else "📈 Positive reception", "price-up")
        if neg and not pos:
            return (f"📉 Negative — {sample_neg}" if sample_neg
                    else "📉 Negative reception", "price-down")
        if pos and neg:
            return ("Mixed news reception", "")
        return ("", "")

    earnings_alerts_seen = set()
    for s in active_stocks:
        tk = s.get("ticker", "")
        for er in db.get_past_earnings(within_days=ALERT_MAX_AGE_DAYS + 1):
            if (er.get("ticker") or "").upper() != tk.upper():
                continue
            if (er.get("exchange") or "") != s.get("exchange", ""):
                continue
            rd = er.get("report_date", "")
            period = (er.get("fiscal_period") or "").strip()
            # Skip projected/estimated rows — only alert on confirmed
            # actual announcement dates.
            if "(proj)" in period.lower() or "(est)" in period.lower():
                continue
            key = (tk, rd)
            if key in earnings_alerts_seen:
                continue
            earnings_alerts_seen.add(key)
            try:
                r_dt = datetime.strptime(rd, "%Y-%m-%d").date()
            except ValueError:
                continue
            days_ago = (_today_date - r_dt).days
            if days_ago < 0 or days_ago > ALERT_MAX_AGE_DAYS:
                continue
            when = ("today" if days_ago == 0
                    else "yesterday" if days_ago == 1
                    else f"{days_ago}d ago")
            ex = display_ex(s.get("exchange", ""))
            sent_label, sent_cls = _sentiment_for(tk, rd)
            sent_html = (f"<div class='alert-date' style='color:var(--text-muted)'>{_esc(sent_label)}</div>"
                         if sent_label else "")
            src_url = _esc(er.get("source_url", "") or "#")
            alert_all.append(f"""
        <div class="alert-card {sent_cls}" data-exchange="{_esc(ex)}" data-ticker="{_esc(tk)}">
            <div class="alert-stock">📊 {_esc(s.get('name', tk))} ({_esc(tk)}) <span style="color:var(--text-muted);font-weight:400;font-size:0.72rem">· {_esc(ex)}</span></div>
            <div class="alert-title" style="font-size:0.9rem;font-weight:600;">
                <a href="{src_url}" target="_blank" style="color:inherit;text-decoration:none">Reported {_esc(period or 'results')} · {when}</a>
            </div>
            {sent_html}
            <div class="alert-date">📅 {_esc(rd)}</div>
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

    # Translate non-English titles/snippets to English (cached per
    # phrase in the `translations` table). Originals are kept under
    # *_orig keys so the rendered card can show a tooltip with the
    # source-language version. Languages the user has opted to keep
    # native (Engine Room → Translations) are left alone.
    _translate_items_inplace(db, news_sorted, ("title", "snippet"))

    # Group by display exchange (e.g. NASDAQ + NYSE both → "US")
    news_by_ex: dict[str, list] = {}
    for n in news_sorted:
        ex = display_ex(n.get("exchange", "Other"))
        news_by_ex.setdefault(ex, []).append(n)

    # Layout: always render a flat chronological stream across all
    # exchanges. Each card has an inline exchange badge so country origin
    # is still visible at a glance, but a user with stocks across 2-3
    # countries doesn't have to click "show more" inside each country
    # group to see the latest headlines. Set FLAT_THRESHOLD > 0 to
    # restore the per-exchange grouping below that many exchanges.
    FLAT_THRESHOLD = 0
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
        pub = _esc(_humanize_pub_date(n.get("published", "")))
        pub_epoch = _parse_news_epoch(n.get("published", ""))
        # Translation badge — show a flag chip if we translated this item
        # from its native language. Original title is kept under title_orig.
        nlang = (n.get("lang") or "").lower()
        flag = _lang_flag(nlang)
        if n.get("title_orig") and flag:
            orig = _esc(_strip_html(n.get("title_orig", "")))[:240]
            lang_badge = (f'<span class="lang-badge" title="Original: {orig}">'
                          f'{flag} translated</span>')
        elif n.get("title_orig"):
            lang_badge = '<span class="lang-badge">translated</span>'
        elif flag:
            lang_badge = f'<span class="lang-badge">{flag}</span>'
        else:
            lang_badge = ""
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
        # Route each row's "View report" link to the place where the
        # actual quarterly report PDF lives, per exchange:
        #   - KLSE: klsescreener already stores the per-quarter URL.
        #   - HKSE: send to HKEX News (the official disclosure portal)
        #     filtered to that stock's Financial Statements — clicking
        #     a result gives the company's released PDF.
        #   - SGX: per-stock announcements page on sgx.com.
        #   - Fallback: stockanalysis /financials/ subpage (still better
        #     than the bare profile page).
        def _better_report_url(stock_url: str, e: dict) -> str:
            exch = (e.get("exchange") or "").upper()
            ticker = (e.get("ticker") or "").strip()
            # HKEX News — its own search portal is a JSF form whose URL
            # params don't actually filter without a real session (the
            # naive titlesearch.xhtml URL just shows "No matches"). The
            # robust workaround is a Google search scoped to
            # hkexnews.hk PDFs — the stock code + filetype:pdf surface
            # the company's released reports as the first results.
            if exch == "HKSE" and ticker:
                try:
                    cd = str(int(ticker))   # strip leading zeros
                except ValueError:
                    cd = ticker
                import urllib.parse as _up
                q = f"site:hkexnews.hk {cd} filetype:pdf"
                return ("https://www.google.com/search?q="
                        + _up.quote_plus(q))
            if exch == "SGX" and ticker:
                return ("https://www.sgx.com/securities/equities/"
                        f"{ticker}/announcements")
            # KLSE entries already point at klsescreener's per-quarter
            # financial-report page — leave them alone.
            if "klsescreener.com" in (stock_url or ""):
                return stock_url
            if not stock_url:
                return stock_url
            url = stock_url.rstrip("/")
            if "stockanalysis.com/" in url and not url.endswith(
                ("/financials", "/statistics", "/earnings")):
                return url + "/financials/"
            return stock_url

        # Friendly label for the period column. Stockanalysis stores
        # 'Next report' / 'Report' which is meaningless on past rows;
        # turn it into something context-appropriate.
        def _period_label(raw: str, is_past: bool) -> str:
            r = (raw or "").strip()
            if not r or r.lower() in ("next report", "report"):
                return "View report" if is_past else "Next report"
            if r.startswith("("):  # e.g. '(from web search)'
                return "View report" if is_past else r
            return r

        for e in items:
            tk = e.get("ticker", "")
            sname = stock_map.get(tk, {}).get("name", tk)
            ex = display_ex(e.get("exchange", ""))
            rdate = e.get("report_date", "TBD")
            raw_period = e.get("fiscal_period", "")
            src_raw = e.get("source_url", "")
            src = _esc(_better_report_url(src_raw, e) if is_past else src_raw)
            period_label = _period_label(raw_period, is_past)
            period = _esc(period_label)
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
                period_cell = f"<a href='{src}' target='_blank' style='color:var(--accent);text-decoration:none'>{period} ↗</a>"
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
            pub = _esc(_humanize_pub_date(ins.get("published", "")))

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

    # Translate forum mention text to English (same caching/skip-list as news).
    _translate_items_inplace(db, forum_sorted, ("text",))

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
        posted_at = _esc(_humanize_pub_date(f.get("posted_at", "")))
        flang = (f.get("lang") or "").lower()
        flag = _lang_flag(flang)
        if f.get("text_orig") and flag:
            orig = _esc(_strip_html(f.get("text_orig", "")))[:300]
            lang_badge = (f'<span class="lang-badge" title="Original: {orig}">'
                          f'{flag} translated</span>')
        elif f.get("text_orig"):
            lang_badge = '<span class="lang-badge">translated</span>'
        elif flag:
            lang_badge = f'<span class="lang-badge">{flag}</span>'
        else:
            lang_badge = ""
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

    # ── Build Funds section (fund-newsletter mentions of watchlist stocks) ──
    fund_lookback_iso = (datetime.utcnow() - timedelta(days=730)).strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        fund_rows = db.get_fund_mentions_since(fund_lookback_iso)
    except Exception:
        fund_rows = []
    fund_cards_html: list[str] = []
    if fund_rows:
        # Group by fund_name → list of mentions, newest report first
        fund_groups: dict[str, list[dict]] = {}
        for fr in fund_rows:
            fund_groups.setdefault(fr.get("fund_name", "Funds"), []).append(fr)
        for fund_name in sorted(fund_groups.keys()):
            items = fund_groups[fund_name][:60]
            cards = []
            for fr in items:
                tk = fr.get("ticker", "")
                ex_internal = fr.get("exchange", "")
                ex_display = display_ex(ex_internal)
                sname = stock_map.get(tk, {}).get("name", tk)
                ex_badge = ex_badge_html(ex_internal, ex_display) if ex_display else ""
                report_date = _esc(fr.get("report_date", ""))
                report_url = _esc(fr.get("report_url", "#"))
                snippet = _esc(_strip_html(fr.get("snippet", "")))[:600]
                cards.append(f"""
                <div class="news-card fund-card" data-exchange="{_esc(ex_display)}" data-ticker="{_esc(tk)}">
                    <div class="news-stock">{ex_badge} {_esc(sname)} ({_esc(tk)})
                        <span class="lang-badge">📅 {report_date}</span>
                    </div>
                    <div class="news-snippet">{snippet}</div>
                    <div class="news-meta">
                        <a href="{report_url}" target="_blank" rel="noreferrer">View report ↗</a>
                    </div>
                </div>""")
            fund_cards_html.append(f"""
            <div class="exchange-group">
                <div class="exchange-header">
                    💼 {_esc(fund_name)}
                    <span style="font-weight:400;color:var(--text-muted)">({len(items)})</span>
                    <span class="chevron">▼</span>
                </div>
                <div class="exchange-body">{''.join(cards)}</div>
            </div>""")
    if not fund_cards_html:
        fund_cards_html.append(
            '<div class="empty">No watchlist stocks mentioned in tracked '
            'fund newsletters yet. Add aliases in the '
            '<a href="/engine-room#funds-card" style="color:var(--accent)">'
            'Engine Room → Fund mentions</a> if you expect a hit that '
            "isn't appearing.</div>")

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

    # ── view-only mode: shared snapshots add a body class so CSS hides
    # all interactive controls that would call /api/* endpoints. ──
    view_only_class = ' class="view-only"' if view_only else ''

    # ── Sign-out link only appears in multi-user mode (Fly.io deploy).
    # In single-user local dev there's no concept of a session.
    logout_link = (
        '<a href="/logout" title="Sign out">Sign out</a>'
        if os.environ.get("MULTI_USER", "").lower() in ("1", "true", "yes")
        else ''
    )

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
<script>
// Apply saved theme BEFORE any styles paint so a light-mode user
// doesn't get a flash of dark UI on every page load.
try {{ if (localStorage.getItem('ee-theme') === 'light')
       document.documentElement.classList.add('light-mode'); }} catch (_) {{}}
</script>
<style>{CSS}</style>
<style>
/* View-only mode (used for shared snapshots): hide every control that
 * would attempt to mutate state via /api/* on a deploy where there's
 * no live server. Everything stays visually consistent — data renders
 * exactly the same — but Add Stock / Remove / Refresh / Engine Room
 * / Portfolio links disappear so beta testers can't try to use them. */
body.view-only .solid-btn,
body.view-only .stock-chip-remove,
body.view-only .price-refresh-btn,
body.view-only #refresh-progress,
body.view-only .refresh-bar,
body.view-only .header-nav a[href="/portfolio"],
body.view-only .header-nav a[href="/engine-room"],
body.view-only .header-nav a[href="/screener"]    {{ display: none !important; }}
body.view-only .news-extend-btn                    {{ display: none !important; }}
</style>
</head>
<body{view_only_class}>

<!-- ═══════════ Header ═══════════ -->
<div class="header">
    <div class="header-inner">
        <div class="header-brand">
            {logo_img}
            <h1><span>Emerging Edge</span> Monitor</h1>
        </div>
        <div class="header-nav">
            <button type="button" class="theme-toggle" id="theme-toggle"
                    onclick="toggleTheme()" title="Toggle light / dark mode">🌙</button>
            <span class="solid-btn" onclick="openAddStockModal()">➕ Add Stock</span>
            <a href="/portfolio">Portfolio</a>
            <a href="/engine-room">⚙ Engine Room</a>
            {logout_link}
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
            <span class="kpi-sep">·</span>
            <a class="kpi" href="#funds-section"><span class="kpi-val">{len(fund_rows)}</span>Funds</a>
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
        <span class="stocks-label">
            Stocks <span id="density-count-hint" class="density-count-hint"></span>
        </span>
        <span id="stocks-summary-strip" class="stocks-summary-strip" style="display:none;"></span>
        <span id="selected-exchange-chip" class="selected-exchange-chip" style="display:none;"></span>
        <span id="selected-stock-chip" class="selected-stock-chip" style="display:none;"></span>
        <span class="stock-layout-toggle-spacer"></span>
        <label class="stl-label">
            <input type="checkbox" id="group-by-exchange" checked onchange="toggleStockLayout(this.checked)">
            Group by exchange
        </label>
        <span class="stl-label">
            <span class="density-pills" role="tablist">
                <button type="button" class="density-pill" data-density="chip"  onclick="setDensity('chip')">Chips</button>
                <button type="button" class="density-pill" data-density="line"  onclick="setDensity('line')">Lines</button>
                <button type="button" class="density-pill" data-density="graph" onclick="setDensity('graph')">Graphs</button>
            </span>
        </span>
        <span class="stl-label sparklines-toggle-bar" style="margin-left:0.4rem">
            <button type="button" id="charts-toggle" class="density-pill"
                    onclick="toggleCharts()" title="Show 90-day sparklines on every stock chip">
                📈 Sparklines
            </button>
        </span>
        <span class="stl-label graph-range-bar" id="graph-range-bar"
              title="Pick the time window for the per-stock charts">
            Range:
            <span class="density-pills" role="tablist">
                <button type="button" class="density-pill graph-range-pill" data-range="30"  onclick="setGraphRange(30)">1M</button>
                <button type="button" class="density-pill graph-range-pill active" data-range="90"  onclick="setGraphRange(90)">3M</button>
                <button type="button" class="density-pill graph-range-pill" data-range="180" onclick="setGraphRange(180)">6M</button>
                <button type="button" class="density-pill graph-range-pill" data-range="365" onclick="setGraphRange(365)">1Y</button>
                <button type="button" class="density-pill graph-range-pill" data-range="all" onclick="setGraphRange('all')">ALL</button>
            </span>
            <button type="button" class="density-pill graph-indexed-btn" id="chart-indexed-btn"
                    onclick="toggleChartIndexed()"
                    title="Rebase every chart to start at 100. Y-axis becomes a shared index scale across the grid — line height directly compares relative performance.">
                📊 Index 100
            </button>
            <button type="button" class="density-pill graph-backfill-btn" id="price-backfill-btn"
                    onclick="backfillPrices()"
                    title="Pull 1 year of daily price history for every watched stock from stockanalysis.com / Yahoo / Naver / TMX. Runs once in the background; charts update on completion.">
                <span class="mini-spinner"></span> ⟳ Update 1y history
            </button>
        </span>
    </div>
</div>
<!-- chart-data is lazy-loaded from /api/history on first interaction
     (Graphs mode, timescale switch, Index 100 toggle). Removing it from
     the inline page payload trims ~100 KB on fv / ~500 KB on ee and
     speeds up the first paint. -->
<script id="chart-currency" type="application/json">{chart_currency_json}</script>
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

    <!-- 💼 Funds — fund-newsletter mentions -->
    <div class="section{' empty' if not fund_rows else ''}" id="funds-section">
        <div class="section-title">
            <span class="icon">💼</span> Funds
            <span class="section-count">{len(fund_rows)}</span>
            <span class="section-hint">(when AFC &amp; tracked funds mention a watchlist stock)</span>
        </div>
        {''.join(fund_cards_html)}
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
        <span class="refresh-btn-label" data-base="🆓 Free refresh">🆓 Free refresh</span>
    </button>
    <button class="refresh-btn refresh-btn-full" id="refresh-btn-full"
            onclick="doRefresh('full')"
            {'' if _serper_key_set else 'disabled data-needs-key="1"'}
            title="{'Refresh everything above + Serper news, forums, contracts. Uses Serper API credits.' if _serper_key_set else 'Disabled — add a Serper API key in the Engine Room to enable.'}">
        <span class="spinner"></span>
        <span class="refresh-btn-label" data-base="💳 Full refresh">💳 Full refresh</span>
    </button>
    <span class="serper-info" tabindex="0" aria-label="What is Serper?">ⓘ
        <span class="serper-popover">
            {'<strong>Full refresh is disabled</strong> — add a Serper API key to enable.<br><br>' if not _serper_key_set else ''}
            <strong>What is Serper?</strong><br>
            A Google-search API. <em>Free refresh</em> already covers most news (Google News,
            Yahoo RSS, regional feeds), forums (i3investor, Telegram, KLSE, …), earnings
            (stockanalysis.com), insiders (SEC) and prices.<br><br>
            <em>Full refresh</em> adds a paid layer on top: Twitter/web forum sweeps,
            Serper-news search, and contract scans. See the
            <a href="/engine-room">Engine Room</a> for the exact <strong>free vs paid</strong>
            breakdown per source.<br><br>
            <strong>Sign up:</strong> <a href="https://serper.dev" target="_blank" rel="noopener">serper.dev</a> —
            the free tier gives <strong>2,500 searches/month</strong>, plenty for a small portfolio.
        </span>
    </span>
</div>

<!-- Add Stock modal -->
<div id="add-stock-modal" class="add-stock-overlay" style="display:none" onclick="if (event.target===this) closeAddStockModal()">
    <div class="add-stock-card">
        <div class="add-stock-header">
            <h3 style="margin:0">Add Stock to Monitor</h3>
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
    // Compute the actual refresh target from the unified function so
    // exchange filters / multi-chip selections / scope dropdown all
    // narrow the refresh consistently.
    const targets = _getRefreshTargets();
    if (activeBtn) {{
        activeBtn.classList.add('busy');
        let label;
        if (targets.short) {{
            label = mode === 'full'
                ? '💳 Refreshing ' + targets.short + ' (Serper)…'
                : '🆓 Refreshing ' + targets.short + '…';
        }} else {{
            label = mode === 'full' ? '💳 Refreshing (Serper)…' : '🆓 Refreshing (no Serper)…';
        }}
        activeBtn.innerHTML = '<span class="spinner"></span> ' + label;
    }}
    status.textContent = '';
    showProgress(true);
    updateProgress({{ step: 'starting', ticker: '', done: 0, total: 0, error: '' }}, mode);

    const reqBody = targets.tickers.length
        ? {{ mode: mode, tickers: targets.tickers }}
        : {{ mode: mode }};
    fetch('/api/refresh', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify(reqBody)
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
    // Re-apply scope label after reset
    _updateRefreshScopeLabels();
}}

// Compute the current refresh target — i.e. what set of tickers the
// refresh buttons would actually fetch right now, given the user's
// current view. Priority:
//   1. Chip filter active (1 or more stocks selected) → those tickers
//   2. Exchange filter narrowed to one or more exchanges → all
//      stocks in those exchanges
//   3. Otherwise → all stocks (empty list, backend treats as no filter)
// Returns: {{ tickers: [...], label: '...', short: '...' }}
//   tickers: array sent in the API body. Empty = refresh all.
//   label: long human-readable suffix for the tooltip.
//   short: short suffix for the button label, e.g. "BXN only",
//          "3 stocks", "ASX only", "" (no scope).
function _getRefreshTargets() {{
    if (typeof activeTickers !== 'undefined' && activeTickers && activeTickers.size > 0) {{
        const tks = Array.from(activeTickers);
        if (tks.length === 1) {{
            return {{ tickers: tks, label: tks[0] + ' only', short: tks[0] + ' only' }};
        }}
        return {{ tickers: tks, label: tks.length + ' selected stocks',
                 short: tks.length + ' stocks' }};
    }}
    // No chip filter — fall through to exchange-based scope
    const activeEx = (typeof getActiveExchanges === 'function')
        ? getActiveExchanges() : [];
    if (activeEx.length) {{
        // Resolve exchanges to tickers via the chip grid.
        const tks = [];
        document.querySelectorAll('.stock-chip[data-ticker][data-exchange]').forEach(c => {{
            if (activeEx.includes(c.dataset.exchange)) {{
                tks.push(c.dataset.ticker);
            }}
        }});
        const exLabel = activeEx.length === 1
            ? activeEx[0] + ' only'
            : activeEx.length + ' exchanges';
        return {{ tickers: tks, label: exLabel + ' (' + tks.length + ' stocks)',
                 short: exLabel }};
    }}
    return {{ tickers: [], label: '', short: '' }};
}}

// Reflect the selected scope on the idle refresh buttons so the user
// sees what would happen BEFORE clicking. Also updates tooltips.
function _updateRefreshScopeLabels() {{
    const free = document.getElementById('refresh-btn-free');
    const full = document.getElementById('refresh-btn-full');
    const targets = _getRefreshTargets();
    [free, full].forEach(btn => {{
        if (!btn || btn.classList.contains('busy')) return;
        const labelEl = btn.querySelector('.refresh-btn-label');
        if (!labelEl) return;
        const base = labelEl.dataset.base;
        const isFull = btn.id === 'refresh-btn-full';
        if (targets.short) {{
            labelEl.textContent = base + ': ' + targets.short;
            btn.setAttribute('title',
                isFull
                    ? 'Refresh ' + targets.label + ' — news, forums, contracts, prices, etc. (Uses Serper credits, scoped to this selection only.)'
                    : 'Refresh ' + targets.label + ' — prices, free news, forums, insiders, earnings. No Serper credits.');
        }} else {{
            labelEl.textContent = base;
            btn.setAttribute('title',
                isFull
                    ? 'Refresh everything above + Serper news, forums, contracts. Uses Serper API credits.'
                    : 'Refresh prices, SEC insiders, Yahoo news and page scrapes. Free — no Serper credits used.');
        }}
    }});
    // Backfill button (Graph-mode toolbar) scope label.
    const bf = document.getElementById('price-backfill-btn');
    if (bf && !bf.classList.contains('busy')) {{
        if (targets.short) {{
            bf.innerHTML = '<span class="mini-spinner"></span> ⟳ 1y history: ' + targets.short;
            bf.setAttribute('title',
                'Pull 1 year of daily price history for ' + targets.label
                + '. Runs in the background; charts update on completion.');
        }} else {{
            bf.innerHTML = '<span class="mini-spinner"></span> ⟳ Update 1y history';
            bf.setAttribute('title',
                'Pull 1 year of daily price history for every watched stock from stockanalysis.com / Yahoo / Naver / TMX. Runs once in the background; charts update on completion.');
        }}
    }}
}}

// Initialize button labels once on page load. The chip + exchange
// filter state is restored later (by restoreFilterState) before any
// chip-driven update runs, so we just need to reflect whatever ends
// up in those filters after restore.
_updateRefreshScopeLabels();

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
                        // Preserve the FULL filter state across reload
                        // via URL hash. Refresh updates data, not view.
                        //   #ex=NASDAQ,NYSE&tk=AAPL,MSFT
                        const _activeEx = (typeof getActiveExchanges === 'function')
                            ? getActiveExchanges() : [];
                        const _activeTk = (typeof activeTickers !== 'undefined' && activeTickers && activeTickers.size)
                            ? Array.from(activeTickers) : [];
                        const _parts = [];
                        if (_activeEx.length) _parts.push('ex=' + _activeEx.map(encodeURIComponent).join(','));
                        if (_activeTk.length) _parts.push('tk=' + _activeTk.map(encodeURIComponent).join(','));
                        const _newHash = _parts.join('&');
                        if (_newHash) window.location.hash = _newHash;
                        else history.replaceState(null, '', window.location.pathname);
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
    // Use the unified target resolver so the price-refresh button
    // honors scope dropdown / multi-chip / exchange filter the same
    // way the Free/Full buttons do.
    const _priceTargets = _getRefreshTargets();
    const _priceBody = {{}};
    if (_priceTargets.tickers.length) {{
        // Explicit ticker set (from scope dropdown, chip selection,
        // or exchange-resolved-to-tickers) — send as-is.
        _priceBody.tickers = _priceTargets.tickers;
    }} else if (actives.length === 1) {{
        // No chip/scope and exactly one exchange filter — pass it
        // directly. (Multiple exchanges fall through to refresh-all.)
        _priceBody.exchange = actives[0];
    }}
    const body = JSON.stringify(_priceBody);

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
                                // Preserve filter state — same as pollRefresh.
                                const _activeTk = (typeof activeTickers !== 'undefined' && activeTickers && activeTickers.size)
                                    ? Array.from(activeTickers) : [];
                                const _hashParts2 = [];
                                if (actives.length) _hashParts2.push('ex=' + actives.map(encodeURIComponent).join(','));
                                if (_activeTk.length) _hashParts2.push('tk=' + _activeTk.map(encodeURIComponent).join(','));
                                if (_hashParts2.length) {{
                                    window.location.hash = _hashParts2.join('&');
                                }} else {{
                                    history.replaceState(null, '', window.location.pathname);
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

// ── 1-year backfill ────────────────────────────────────────────────
// Renders an at-completion summary of the backfill run. Pops a
// dismissible block under the progress bar listing per-stock failures
// with the actual reason returned by the helper:
//   • no-source     — exchange not in any historical-price source map
//   • source-failed — sources tried but all returned no data (429, etc.)
//   • exception     — unexpected error during fetch
function _showBackfillSummary(p) {{
    if (!p) return;
    const failures = p.failures || [];
    const wrapId = 'backfill-summary';
    let wrap = document.getElementById(wrapId);
    if (!wrap) {{
        wrap = document.createElement('div');
        wrap.id = wrapId;
        wrap.style.cssText =
            'position:fixed;right:1rem;bottom:1rem;max-width:520px;'
            + 'background:var(--surface);border:1px solid var(--border);'
            + 'border-radius:8px;padding:0.85rem 1rem;'
            + 'font-size:0.82rem;color:var(--text);'
            + 'box-shadow:0 8px 20px rgba(0,0,0,0.35);z-index:9999;';
        document.body.appendChild(wrap);
    }}
    let body = '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:0.4rem">'
             + '<strong>1y history — done</strong>'
             + '<span style="cursor:pointer;color:var(--text-muted)" onclick="this.parentElement.parentElement.remove()">✕</span>'
             + '</div>';
    body += '<div style="color:var(--text-muted);font-size:0.78rem;margin-bottom:0.4rem">'
          + '+' + (p.inserted || 0) + ' rows · '
          + 'ok ' + (p.ok || 0) + ' · '
          + 'already covered ' + (p.covered || 0);
    if (p.no_source) body += ' · no source ' + p.no_source;
    if (p.failed)    body += ' · failed ' + p.failed;
    body += '</div>';
    if (failures.length) {{
        body += '<div style="max-height:240px;overflow-y:auto">';
        body += '<table style="width:100%;border-collapse:collapse;font-size:0.78rem">';
        body += '<thead><tr style="text-align:left;color:var(--text-muted)">'
              + '<th style="padding:0.2rem 0.3rem">Ticker</th>'
              + '<th>Why</th></tr></thead><tbody>';
        for (const f of failures.slice(0, 30)) {{
            const reason = (f.reason || f.status || '').replace(/&/g,'&amp;').replace(/</g,'&lt;');
            body += '<tr style="border-top:1px solid var(--border)">'
                  + '<td style="padding:0.25rem 0.3rem;white-space:nowrap;font-family:monospace">'
                  + (f.ticker || '?') + '<span style="color:var(--text-muted)">/' + (f.exchange || '?') + '</span></td>'
                  + '<td style="padding:0.25rem 0.3rem;color:var(--text-muted)">' + reason + '</td>'
                  + '</tr>';
        }}
        if (failures.length > 30) {{
            body += '<tr><td colspan="2" style="padding:0.25rem 0.3rem;color:var(--text-muted)">…and '
                  + (failures.length - 30) + ' more</td></tr>';
        }}
        body += '</tbody></table></div>';
    }} else {{
        body += '<div style="color:var(--text-muted)">All stocks updated cleanly.</div>';
    }}
    wrap.innerHTML = body;
}}

// Pulls daily price history for every watched ticker so Graph mode
// has data to chart. Yahoo + Stooq + TMX (Canada) cover the bulk;
// stocks on exotic frontier exchanges (where no historical source
// exists) just get whatever the live fetcher has accumulated.
function backfillPrices() {{
    const btn = document.getElementById('price-backfill-btn');
    if (!btn) return;
    if (btn.classList.contains('busy')) return;
    // Resolve scope the same way Refresh Prices does: chip selection
    // and/or exchange filter narrow the scope; otherwise fall back to
    // every watched stock.
    const targets = (typeof _getRefreshTargets === 'function')
        ? _getRefreshTargets() : {{ tickers: [], label: '', short: '' }};
    const body = {{ days: 365 }};
    if (targets.tickers && targets.tickers.length) {{
        body.tickers = targets.tickers;
    }}
    btn.classList.add('busy');
    const scopeBit = targets.short ? ' (' + targets.short + ')' : '';
    btn.innerHTML = '<span class="mini-spinner"></span> Backfilling' + scopeBit + '…';
    showProgress(true);

    fetch('/api/backfill-prices', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify(body),
    }})
        .then(r => r.json())
        .then(data => {{
            if (data.status === 'started' || data.status === 'busy') {{
                const poll = setInterval(() => {{
                    fetch('/api/status')
                        .then(r => r.json())
                        .then(d => {{
                            // Reuse the existing progress UI; the status
                            // text gives the user a live counter.
                            updateProgress(d.progress);
                            const p = d.progress || {{}};
                            if (p.step === 'backfill') {{
                                btn.innerHTML = '<span class="mini-spinner"></span>'
                                    + ' ' + (p.done || 0) + '/' + (p.total || '?')
                                    + ' (' + (p.inserted || 0) + ' new)';
                            }}
                            if (!d.refreshing) {{
                                clearInterval(poll);
                                btn.classList.remove('busy');
                                const failed = (p.failed || 0) + (p.no_source || 0);
                                if (failed > 0) {{
                                    btn.innerHTML = '⚠ ' + failed + ' failed';
                                }} else {{
                                    btn.innerHTML = '✅ +' + (p.inserted || 0) + ' days';
                                }}
                                _showBackfillSummary(p);
                                // Preserve filter state across reload.
                                const _activeEx = (typeof getActiveExchanges === 'function')
                                    ? getActiveExchanges() : [];
                                const _activeTk = (typeof activeTickers !== 'undefined' && activeTickers && activeTickers.size)
                                    ? Array.from(activeTickers) : [];
                                const _parts = [];
                                if (_activeEx.length) _parts.push('ex=' + _activeEx.map(encodeURIComponent).join(','));
                                if (_activeTk.length) _parts.push('tk=' + _activeTk.map(encodeURIComponent).join(','));
                                if (_parts.length) window.location.hash = _parts.join('&');
                                else history.replaceState(null, '', window.location.pathname);
                                // Hold the summary visible longer when
                                // there's something to read.
                                setTimeout(() => {{
                                    showProgress(false);
                                    location.reload();
                                }}, failed > 0 ? 5000 : 800);
                            }}
                        }})
                        .catch(() => {{
                            clearInterval(poll);
                            btn.classList.remove('busy');
                            btn.innerHTML = '⟳ Update 1y history';
                        }});
                }}, 1500);
            }} else {{
                btn.classList.remove('busy');
                btn.innerHTML = '⟳ Update 1y history';
                showProgress(false);
            }}
        }})
        .catch(() => {{
            btn.classList.remove('busy');
            btn.innerHTML = '⟳ Update 1y history';
            showProgress(false);
        }});
}}

// ── Restore filter state from URL hash on page load ──
// Hash format set by pollRefresh / refreshPrices just before reload:
//   #ex=NASDAQ,NYSE&tk=AAPL,MSFT  (either part optional)
//
// Goal: refreshing data must not change the view. Whatever exchange
// pill and chip selection the user had, they should still see after
// the refresh-induced reload.
(function restoreFilterState() {{
    const hash = (window.location.hash || '').replace(/^#/, '');
    if (!hash) {{
        // Even with no hash, sync button labels in case there's any
        // pre-applied filter state from earlier in the script.
        if (typeof _updateRefreshScopeLabels === 'function') {{
            _updateRefreshScopeLabels();
        }}
        return;
    }}
    const parts = hash.split('&');
    let exList = [];
    let tkList = [];
    for (const p of parts) {{
        if (p.startsWith('ex=')) {{
            exList = p.slice(3).split(',').map(decodeURIComponent).filter(Boolean);
        }} else if (p.startsWith('tk=')) {{
            tkList = p.slice(3).split(',').map(decodeURIComponent).filter(Boolean);
        }}
    }}
    // Apply exchange filter first (it can clear chip selection as a
    // side effect when activeTickers don't match the new exchange,
    // so chip restore needs to happen AFTER).
    if (exList.length) {{
        const valid = exList.filter(ex =>
            document.querySelector('.filter-pill[data-exchange="' + ex.replace(/"/g, '\\\\"') + '"]'));
        if (valid.length && typeof _applyExchangeFilter === 'function') {{
            _applyExchangeFilter(valid);
        }}
    }}
    // Restore chip / ticker selection. We apply them one at a time
    // additively so the existing setActiveTicker logic (single vs multi)
    // doesn't collapse a multi-select to a single click.
    if (tkList.length && typeof activeTickers !== 'undefined') {{
        activeTickers.clear();
        tkList.forEach(tk => activeTickers.add(tk));
        if (typeof applyGlobalStockFilter === 'function') {{
            applyGlobalStockFilter();
        }}
        document.querySelectorAll('.stock-chip[data-ticker]').forEach(c => {{
            c.classList.toggle('chip-active', activeTickers.has(c.dataset.ticker));
        }});
        if (typeof _renderSelectedStockChip === 'function') {{
            _renderSelectedStockChip();
        }}
    }}
    // Refresh button labels now that the chip/exchange state is in place.
    if (typeof _updateRefreshScopeLabels === 'function') {{
        _updateRefreshScopeLabels();
    }}
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
