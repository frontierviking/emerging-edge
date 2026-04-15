"""
fetchers.py — Data collection layer for emerging-edge.

Each public function takes a stock dict (from config.json) and a Database
instance, fetches data from the appropriate source, and stores new items.

Data sources:
  • Serper REST API (https://serper.dev)  — direct HTTPS calls, no Node.js
  • Direct HTTP (earnings pages, forum pages)   — via urllib

No external Python dependencies required — uses only the standard library.
"""

from __future__ import annotations

import json
import os
import re
import sys
import urllib.request
import urllib.error
import urllib.parse
import logging
from datetime import datetime, timedelta
from html.parser import HTMLParser
from typing import Optional

from db import Database

logger = logging.getLogger("emerging-edge.fetchers")

# ---------------------------------------------------------------------------
# Configuration helpers
# ---------------------------------------------------------------------------

def load_config(path: str = "config.json") -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def get_active_stocks(db, config: dict) -> list[dict]:
    """
    Return the merged list of stocks from config['stocks'] (which may be
    empty in the public/shareable version) plus any user-added stocks
    stored in the user_stocks DB table. Dedupes by (ticker, exchange).

    Callers that previously read config['stocks'] should use this helper
    instead so user-added stocks appear in the dashboard, portfolio form,
    and data fetching pipeline.
    """
    seen: set = set()
    merged: list = []
    for s in config.get("stocks", []) or []:
        key = (s.get("ticker", "").upper(), s.get("exchange", "").upper())
        if key in seen:
            continue
        seen.add(key)
        merged.append(s)
    try:
        user_stocks = db.get_user_stocks() if db is not None else []
    except Exception:
        user_stocks = []
    for s in user_stocks:
        key = (s.get("ticker", "").upper(), s.get("exchange", "").upper())
        if key in seen:
            continue
        seen.add(key)
        merged.append(s)
    return merged


# ---------------------------------------------------------------------------
# Serper REST API  (replaces the old MCP subprocess approach)
#
# Docs: https://serper.dev/docs
# Endpoints:
#   POST https://google.serper.dev/search  — web search
#   POST https://google.serper.dev/news    — news search
#
# Auth: "X-API-KEY" header with your SERPER_API_KEY.
# ---------------------------------------------------------------------------

SERPER_BASE = "https://google.serper.dev"

# Module-level DB path for logging Serper calls. Set by run_all/cmd_serve
# via set_serper_db_path(). If not set, calls are logged to a default
# location alongside the repo.
_SERPER_DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "emerging_edge.db")


def set_serper_db_path(path: str):
    global _SERPER_DB_PATH
    _SERPER_DB_PATH = path


# Runtime controls for Serper:
#  - _SERPER_KEY_OVERRIDE: if set (by monitor.py after reading the
#    DB-stored user key), takes precedence over the SERPER_API_KEY env
#    var. This lets the user paste their key in the UI.
#  - _SERPER_ENABLED: if False, _call_serper returns None without
#    making a network call — used for "free refresh" mode.
_SERPER_KEY_OVERRIDE: str = ""
_SERPER_ENABLED: bool = True


def set_serper_api_key(key: str):
    global _SERPER_KEY_OVERRIDE
    _SERPER_KEY_OVERRIDE = (key or "").strip()


def set_serper_enabled(enabled: bool):
    global _SERPER_ENABLED
    _SERPER_ENABLED = bool(enabled)


def get_serper_api_key() -> str:
    """Resolve the active Serper key — DB override wins over env var."""
    return _SERPER_KEY_OVERRIDE or os.environ.get("SERPER_API_KEY", "")


def _log_serper_call(endpoint: str, caller: str, ticker: str,
                      query: str, ok: bool):
    """Append a row to the serper_calls table (best-effort, never raises)."""
    try:
        import sqlite3
        from datetime import datetime as _dt
        conn = sqlite3.connect(_SERPER_DB_PATH, timeout=5)
        conn.execute(
            "INSERT INTO serper_calls (called_at, endpoint, caller, ticker, query, ok) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (_dt.utcnow().isoformat() + "Z", endpoint, caller,
             ticker or "", (query or "")[:200], 1 if ok else 0))
        conn.commit()
        conn.close()
    except Exception:
        pass  # logging must never break a search


def _call_serper(endpoint: str, payload: dict, caller: str = "other",
                 ticker: str = "") -> dict | None:
    """
    POST to the Serper REST API and return the parsed JSON response.

    endpoint: "/search" or "/news"
    payload:  {"q": "...", "num": 10, ...}
    caller:   category for usage tracking ('news', 'contracts', etc.)
    ticker:   stock ticker for usage attribution
    """
    if not _SERPER_ENABLED:
        # "Free refresh" mode — skip Serper entirely without consuming credits.
        return None
    api_key = get_serper_api_key()
    if not api_key:
        logger.warning("SERPER_API_KEY not set — skipping Serper call")
        return None

    url = SERPER_BASE + endpoint
    body = json.dumps(payload).encode("utf-8")
    headers = {
        "X-API-KEY": api_key,
        "Content-Type": "application/json",
    }

    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    query = payload.get("q", "")

    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            _log_serper_call(endpoint, caller, ticker, query, True)
            return data
    except urllib.error.HTTPError as e:
        logger.error("Serper HTTP %d for %s: %s", e.code, endpoint, e.read().decode()[:300])
        # HTTP errors still consumed a credit if it was a 4xx (rate limit etc.)
        # but typically billing only counts successful calls. Log as failed.
        _log_serper_call(endpoint, caller, ticker, query, False)
        return None
    except Exception as e:
        logger.error("Serper call failed (%s): %s", endpoint, e)
        _log_serper_call(endpoint, caller, ticker, query, False)
        return None


def serper_news_search(query: str, config: dict, caller: str = "news",
                        ticker: str = "") -> list[dict]:
    """
    Run a Serper news search.
    Returns a list of result dicts with keys: title, link, snippet, date, source.
    """
    data = _call_serper("/news", {"q": query, "num": 10}, caller=caller, ticker=ticker)
    if not data:
        return []
    return data.get("news", [])


def serper_web_search(query: str, config: dict, caller: str = "other",
                       ticker: str = "") -> list[dict]:
    """
    Run a Serper web (organic) search.
    Returns a list of result dicts with keys: title, link, snippet.
    """
    data = _call_serper("/search", {"q": query, "num": 10}, caller=caller, ticker=ticker)
    if not data:
        return []
    return data.get("organic", [])


# ---------------------------------------------------------------------------
# Simple HTML text extractor (no external dependency)
# ---------------------------------------------------------------------------

class _TextExtractor(HTMLParser):
    """Minimal HTML-to-text extractor."""
    def __init__(self):
        super().__init__()
        self._pieces = []
        self._skip = False

    def handle_starttag(self, tag, attrs):
        if tag in ("script", "style", "noscript"):
            self._skip = True

    def handle_endtag(self, tag):
        if tag in ("script", "style", "noscript"):
            self._skip = False

    def handle_data(self, data):
        if not self._skip:
            self._pieces.append(data)

    def get_text(self) -> str:
        return " ".join(self._pieces)


def _fetch_page_text(url: str, timeout: int = 15) -> str:
    """Fetch a URL and return stripped text content.
    Uses a tolerant SSL context because several frontier exchange sites
    (brvm.org, uzse.uz, etc.) ship certificates that the bundled Python
    trust store can't verify on macOS."""
    import ssl as _ssl
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
    }
    req = urllib.request.Request(url, headers=headers)
    ctx = _ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = _ssl.CERT_NONE
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
            raw = resp.read()
        try:
            html = raw.decode("utf-8")
        except UnicodeDecodeError:
            html = raw.decode("latin-1", errors="replace")
        parser = _TextExtractor()
        parser.feed(html)
        return parser.get_text()
    except Exception as e:
        logger.warning("Failed to fetch %s: %s", url, e)
        return ""


# ---------------------------------------------------------------------------
# Staleness check — skip Serper calls if data is fresh
# ---------------------------------------------------------------------------

# Staleness thresholds — how many hours before re-fetching from Serper.
# Higher = fewer API calls. The DB deduplicates by URL anyway, so
# re-fetching mostly just finds the same results.
STALE_NEWS_HOURS = 48       # News changes fastest — check every 2 days
STALE_CONTRACTS_HOURS = 168  # Contracts/tenders — weekly is enough
STALE_INSIDER_HOURS = 168    # Insider transactions — weekly
STALE_FORUM_HOURS = 168      # Forum web/twitter — weekly


def _is_fresh(db: Database, table: str, ticker: str, max_hours: int = 168) -> bool:
    """
    Check if we already have recent data for this ticker in this table.
    Returns True if data is fresh (less than max_hours old) → skip fetch.
    Returns False if data is stale or missing → should fetch.
    """
    last = db.last_fetched(table, ticker)
    if not last:
        return False  # no data at all — fetch
    try:
        # Strip timezone info and compare as naive UTC datetimes
        last_str = last.replace("Z", "").replace("+00:00", "")
        # Handle microseconds: "2026-04-06T14:57:10.459851"
        if "." in last_str:
            last_dt = datetime.strptime(last_str[:26], "%Y-%m-%dT%H:%M:%S.%f")
        else:
            last_dt = datetime.strptime(last_str[:19], "%Y-%m-%dT%H:%M:%S")
        age_hours = (datetime.utcnow() - last_dt).total_seconds() / 3600
        return age_hours < max_hours
    except (ValueError, TypeError):
        return False


# ---------------------------------------------------------------------------
# A) NEWS FETCHER
# ---------------------------------------------------------------------------

def _fetch_news_yahoo_rss(stock: dict, db: Database) -> int:
    """
    Fetch recent news from Yahoo Finance's free RSS feed for the stock's
    yahoo_ticker. No API key, no Serper credits.

    Yahoo coverage: NASDAQ, KLSE, SGX, JSE — but NOT NGX, BRVM, UZSE, KSE.
    Returns the number of NEW items stored.
    """
    ticker = stock["ticker"]
    exchange = stock["exchange"]
    lang = stock.get("lang", "en")
    yahoo_tk = stock.get("yahoo_ticker", "")
    if not yahoo_tk:
        return 0

    url = (f"https://feeds.finance.yahoo.com/rss/2.0/headline?"
           f"s={urllib.parse.quote(yahoo_tk)}&region=US&lang=en-US")
    logger.info("NEWS Yahoo RSS: %s → %s", ticker, url)

    headers = {"User-Agent": "Mozilla/5.0 (emerging-edge)"}
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            xml_text = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        logger.warning("Yahoo RSS fetch failed for %s: %s", ticker, e)
        return 0

    new_count = 0
    items = re.findall(r"<item>(.*?)</item>", xml_text, re.DOTALL)
    for item_xml in items:
        title_m = re.search(r"<title>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</title>",
                            item_xml, re.DOTALL)
        link_m = re.search(r"<link>(.*?)</link>", item_xml, re.DOTALL)
        desc_m = re.search(r"<description>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</description>",
                           item_xml, re.DOTALL)
        date_m = re.search(r"<pubDate>(.*?)</pubDate>", item_xml, re.DOTALL)

        if not (title_m and link_m):
            continue

        title = title_m.group(1).strip()
        link_url = link_m.group(1).strip()
        desc = desc_m.group(1).strip() if desc_m else ""
        # Strip any HTML tags from description
        desc = re.sub(r"<[^>]+>", "", desc)[:500]
        pub = date_m.group(1).strip() if date_m else ""

        stored = db.insert_news(
            ticker=ticker, exchange=exchange, url=link_url,
            title=title, snippet=desc,
            source="Yahoo Finance",
            published=pub,
            search_type="news", lang=lang)
        if stored:
            new_count += 1

    logger.info("  → %d new Yahoo RSS items for %s", new_count, ticker)
    return new_count


# Exchange → Google News RSS locale hint (hl / gl / ceid). For markets
# where Google News covers a dominant local-language press (Nordics,
# Nigeria, Malaysia, France-West-Africa, etc.) this picks up far more
# relevant items than Yahoo RSS.
_GNEWS_LOCALE = {
    "NASDAQ":  ("en", "US"),
    "NYSE":    ("en", "US"),
    "LSE":     ("en", "GB"),
    "ASX":     ("en", "AU"),
    "TSX":     ("en", "CA"),
    "JSE":     ("en", "ZA"),
    "NGX":     ("en", "NG"),
    "KLSE":    ("en", "MY"),
    "SGX":     ("en", "SG"),
    "HKSE":    ("en", "HK"),
    "NSE":     ("en", "IN"),
    "OMX":     ("sv", "SE"),   # Stockholm
    "OSE":     ("no", "NO"),   # Oslo
    "CSE":     ("da", "DK"),   # Copenhagen
    "HEL":     ("fi", "FI"),   # Helsinki
    "FRA":     ("de", "DE"),   # Frankfurt
    "BIT":     ("it", "IT"),   # Milan
    "BRVM":    ("fr", "CI"),   # Côte d'Ivoire French
    "KASE":    ("ru", "KZ"),   # Kazakhstan — most financial press is Russian-language
}


def _fetch_news_google_rss(stock: dict, db: Database) -> int:
    """
    Google News search RSS feed — free, no key, works for any stock in
    any language. Search query is the company name (quoted). Locale is
    chosen per exchange so Nordic stocks fetch Swedish/Norwegian press,
    Ivorian stocks get French press, etc.
    """
    ticker = stock["ticker"]
    exchange = stock["exchange"]
    name = stock.get("name", "").strip()
    if not name:
        return 0

    hl, gl = _GNEWS_LOCALE.get(exchange, ("en", "US"))
    # Strip parenthetical suffixes so "Investor AB (publ)" becomes a
    # clean query, and wrap in quotes so we only match the exact name.
    clean_name = re.sub(r"\s*\(publ\)\s*$", "", name, flags=re.I).strip()
    query = f'"{clean_name}"'
    url = ("https://news.google.com/rss/search?"
           f"q={urllib.parse.quote(query)}"
           f"&hl={hl}&gl={gl}&ceid={gl}:{hl}")
    logger.info("NEWS Google RSS (%s/%s): %s", hl, gl, clean_name)

    try:
        import ssl as _ssl
        ctx = _ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = _ssl.CERT_NONE
        req = urllib.request.Request(
            url, headers={"User-Agent": "Mozilla/5.0 (emerging-edge)"})
        with urllib.request.urlopen(req, timeout=10, context=ctx) as resp:
            xml_text = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        logger.warning("Google News RSS failed for %s: %s", ticker, e)
        return 0

    new_count = 0
    items = re.findall(r"<item>(.*?)</item>", xml_text, re.DOTALL)
    # Google News wraps content in CDATA blocks; simpler regex needed
    for item_xml in items[:20]:  # cap at 20 per stock
        title_m = re.search(
            r"<title>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</title>",
            item_xml, re.DOTALL)
        link_m = re.search(r"<link>(.*?)</link>", item_xml, re.DOTALL)
        desc_m = re.search(
            r"<description>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</description>",
            item_xml, re.DOTALL)
        date_m = re.search(r"<pubDate>(.*?)</pubDate>", item_xml, re.DOTALL)
        source_m = re.search(r"<source[^>]*>(.*?)</source>", item_xml, re.DOTALL)

        if not (title_m and link_m):
            continue
        title = title_m.group(1).strip()
        link_url = link_m.group(1).strip()
        desc = re.sub(r"<[^>]+>", "", desc_m.group(1)).strip()[:500] if desc_m else ""
        pub = date_m.group(1).strip() if date_m else ""
        source = source_m.group(1).strip() if source_m else "Google News"

        stored = db.insert_news(
            ticker=ticker, exchange=exchange, url=link_url,
            title=title, snippet=desc,
            source=source,
            published=pub,
            search_type="news", lang=hl)
        if stored:
            new_count += 1

    logger.info("  → %d new Google News items for %s", new_count, ticker)
    return new_count


def fetch_news(stock: dict, db: Database, config: dict) -> int:
    """
    Fetch news for a stock. Uses free Yahoo Finance RSS first (covers
    NASDAQ/KLSE/SGX/JSE), then falls back to Serper for stocks Yahoo
    doesn't cover or for non-English secondary searches.
    Skips if data was fetched within the last STALE_NEWS_HOURS.
    """
    ticker = stock["ticker"]
    exchange = stock["exchange"]
    lang = stock.get("lang", "en")
    name = stock["name"]
    new_count = 0

    if _is_fresh(db, "news_items", ticker, STALE_NEWS_HOURS):
        logger.info("NEWS skip %s — data is fresh", ticker)
        return 0

    # ── 1) FREE: Yahoo Finance RSS (no Serper credit) ──
    # Yahoo covers most major exchanges but not NGX, BRVM, UZSE, KSE.
    _YAHOO_COVERED = {"NASDAQ", "KLSE", "SGX", "JSE"}
    yahoo_count = 0
    if exchange in _YAHOO_COVERED and stock.get("yahoo_ticker"):
        yahoo_count = _fetch_news_yahoo_rss(stock, db)
        new_count += yahoo_count

    # ── 2) FREE: Google News search RSS ──
    # Covers everything — any exchange, any language. Runs for every
    # stock as a baseline, in addition to Yahoo where applicable.
    google_count = _fetch_news_google_rss(stock, db)
    new_count += google_count

    # ── 3) PAID fallback: Serper news search ──
    # Skip Serper entirely if Yahoo returned items (covers our needs for
    # English-speaking exchanges). For non-yahoo exchanges and french
    # stocks, Serper is the only realistic option. Also skip if the
    # runtime has disabled Serper (free-refresh mode).
    use_serper = (_SERPER_ENABLED
                  and (exchange not in _YAHOO_COVERED or yahoo_count == 0))
    if use_serper:
        query = f"{name} {ticker}"
        logger.info("NEWS Serper search: %s", query)
        results = serper_news_search(query, config, caller="news", ticker=ticker)
        for item in results:
            url = item.get("link") or item.get("url", "")
            if not url:
                continue
            stored = db.insert_news(
                ticker=ticker, exchange=exchange, url=url,
                title=item.get("title", ""),
                snippet=item.get("snippet", item.get("description", "")),
                source=item.get("source", ""),
                published=item.get("date", ""),
                search_type="news", lang=lang)
            if stored:
                new_count += 1

    # ── 3) French-language secondary search (Serper) ──
    if lang == "fr" and _SERPER_ENABLED:
        query_fr = f"{name} résultats"
        logger.info("NEWS search (FR): %s", query_fr)
        results_fr = serper_news_search(query_fr, config, caller="news", ticker=ticker)
        for item in results_fr:
            url = item.get("link") or item.get("url", "")
            if not url:
                continue
            stored = db.insert_news(
                ticker=ticker, exchange=exchange, url=url,
                title=item.get("title", ""),
                snippet=item.get("snippet", item.get("description", "")),
                source=item.get("source", ""),
                published=item.get("date", ""),
                search_type="resultats", lang="fr")
            if stored:
                new_count += 1

    logger.info("  → %d new news items for %s", new_count, ticker)
    return new_count


# ---------------------------------------------------------------------------
# B) CONTRACTS / TENDERS FETCHER
# ---------------------------------------------------------------------------

def fetch_contracts(stock: dict, db: Database, config: dict) -> int:
    """
    Search for contract awards and tenders.
    Skips if data was fetched within the last 24 hours.
    """
    ticker = stock["ticker"]
    exchange = stock["exchange"]
    lang = stock.get("lang", "en")
    name = stock["name"]
    new_count = 0

    if _is_fresh(db, "contract_items", ticker, STALE_CONTRACTS_HOURS):
        logger.info("CONTRACT skip %s — data is fresh", ticker)
        return 0

    if not _SERPER_ENABLED:
        # Contracts is 100% Serper-sourced; nothing to do in free mode.
        return 0

    if lang == "fr":
        query = f"{name} contrat OR attribution OR appel d'offres"
    else:
        query = f"{name} contract award OR tender"

    logger.info("CONTRACT search: %s", query)
    results = serper_web_search(query, config, caller="contracts", ticker=ticker)

    for item in results:
        url = item.get("link") or item.get("url", "")
        if not url:
            continue
        stored = db.insert_contract(
            ticker=ticker, exchange=exchange, url=url,
            title=item.get("title", ""),
            snippet=item.get("snippet", item.get("description", "")),
            source=item.get("source", ""),
            published=item.get("date", ""),
            lang=lang)
        if stored:
            new_count += 1

    logger.info("  → %d new contract items for %s", new_count, ticker)
    return new_count


# ---------------------------------------------------------------------------
# C) EARNINGS DATE FETCHER
# ---------------------------------------------------------------------------

# Regex patterns for extracting dates from page text
_DATE_PATTERNS = [
    # "28 Feb 2026", "28 February 2026"
    re.compile(r"(\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{4})", re.I),
    # "2026-02-28"
    re.compile(r"(20\d{2}-\d{2}-\d{2})"),
    # "02/28/2026" or "28/02/2026"
    re.compile(r"(\d{1,2}/\d{1,2}/20\d{2})"),
]

_EARNINGS_KEYWORDS = [
    "financial result", "quarterly report", "annual report",
    "earnings", "résultats", "rapport financier",
    "announcement date", "report date", "next report",
    "tarikh laporan", "keputusan kewangan",
]


def fetch_earnings(stock: dict, db: Database, config: dict) -> bool:
    """
    Try to extract the next earnings/report date from the stock's
    exchange-specific page.  Returns True if a date was found and stored.

    Order of attempts:
      1. stockanalysis.com — free, structured, covers ~14 exchanges with
         one HTTP call. This is the fastest and most reliable source.
      2. Exchange-specific template (klsescreener, ngx, brvm, etc.)
         from config — the legacy per-exchange scrapers.
      3. NASDAQ calendar day-by-day scan for US stocks — populates the
         Past Reports tab with 12 months of historical quarterly dates.
    """
    ticker = stock["ticker"]
    code = stock.get("code", ticker)
    exchange = stock["exchange"]
    source_key = stock.get("earnings_source", "")

    # ── 1) Try stockanalysis.com first (one call, very wide coverage) ──
    if _fetch_earnings_stockanalysis(stock, db):
        # For US stocks also run the NASDAQ calendar BACKWARD scan so the
        # Past Reports tab has a year of quarterly history. We skip the
        # forward scan because stockanalysis already provided the next
        # upcoming date — otherwise we'd end up with two "Next report"
        # rows on different dates.
        if exchange in ("NASDAQ", "NYSE"):
            _fetch_earnings_nasdaq_calendar(stock, db, config, past_only=True)
        return True

    # Build the URL from config templates
    url_templates = config.get("earnings_urls", {})
    template = url_templates.get(source_key, "")
    if not template:
        # US stocks still fall through to the NASDAQ calendar scan
        if exchange in ("NASDAQ", "NYSE"):
            return _fetch_earnings_nasdaq_calendar(stock, db, config)
        logger.info("No earnings URL template for %s (%s)", ticker, source_key)
        return False

    url = template.format(ticker=ticker, code=code, name=urllib.parse.quote(stock["name"]))
    logger.info("EARNINGS fetch: %s → %s", ticker, url)

    text = _fetch_page_text(url)
    if not text:
        return False

    # Search for date patterns near earnings keywords
    text_lower = text.lower()
    best_date = None
    best_period = ""

    for kw in _EARNINGS_KEYWORDS:
        idx = text_lower.find(kw)
        if idx == -1:
            continue
        # Look in a window around the keyword
        window = text[max(0, idx - 100):idx + 300]
        for pat in _DATE_PATTERNS:
            m = pat.search(window)
            if m:
                candidate = m.group(1)
                # Try to parse and keep only future dates
                parsed = _try_parse_date(candidate)
                if parsed and parsed >= datetime.now():
                    if best_date is None or parsed < best_date:
                        best_date = parsed
                        best_period = kw

    if best_date:
        db.upsert_earnings(
            ticker=ticker, exchange=exchange,
            report_date=best_date.strftime("%Y-%m-%d"),
            fiscal_period=best_period,
            source_url=url)
        logger.info("  → Earnings date for %s: %s", ticker, best_date.strftime("%Y-%m-%d"))
        return True

    # Fallback: also try a Serper search for earnings date (skip in free mode)
    if not _SERPER_ENABLED:
        return False
    logger.info("  → No date found on page, trying Serper fallback for %s", ticker)
    eq = f"{stock['name']} {ticker} earnings date OR report date 2025 2026"
    results = serper_web_search(eq, config, caller="earnings", ticker=ticker)
    for item in results:
        snippet = item.get("snippet", item.get("description", ""))
        for pat in _DATE_PATTERNS:
            m = pat.search(snippet)
            if m:
                parsed = _try_parse_date(m.group(1))
                if parsed and parsed >= datetime.now():
                    db.upsert_earnings(
                        ticker=ticker, exchange=exchange,
                        report_date=parsed.strftime("%Y-%m-%d"),
                        fiscal_period="(from web search)",
                        source_url=item.get("link", ""))
                    logger.info("  → Earnings date for %s (via search): %s", ticker, parsed.strftime("%Y-%m-%d"))
                    return True

    logger.info("  → No earnings date found for %s", ticker)
    return False


def _try_parse_date(s: str) -> datetime | None:
    """Attempt to parse a date string in several formats."""
    for fmt in ("%d %B %Y", "%d %b %Y", "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(s.strip(), fmt)
        except ValueError:
            continue
    return None


# ---------------------------------------------------------------------------
# stockanalysis.com earnings fetcher — the primary source.
# Single HTTP call per ticker, covers ~14 exchanges, structured HTML.
# ---------------------------------------------------------------------------

# Map our internal exchange code → stockanalysis.com URL slug.
# Values of None mean "use /stocks/{ticker}/" (US common path).
_SA_SLUG = {
    "NASDAQ":   None,
    "NYSE":     None,
    "AMEX":     None,
    "NGX":      "ngx",
    "BRVM":     "brvm",
    "JSE":      "jse",
    "LSE":      "lon",
    "ASX":      "asx",
    "KLSE":     "klse",
    "SGX":      "sgx",
    "FRA":      "fra",
    "TSX":      "tsx",
    "HKSE":     "hkg",
    "TYO":      "tyo",
    "NSE":      "nse",
    "EURONEXT": "epa",
    "BIT":      "etr",
    "OMX":      "sto",  # Stockholm (Nordic pair format uses dot: INVE.B)
    "OSE":      "osl",  # Oslo (rough guess — test before relying)
    "CSE":      "cph",  # Copenhagen
    "HEL":      "hel",  # Helsinki
    "KASE":     "kase", # Kazakhstan
}


def _sa_ticker(exchange: str, ticker: str) -> str:
    """Translate our internal ticker to stockanalysis.com's format."""
    t = ticker.upper()
    # Nordic exchanges use . between the ticker and the share class
    # letter (INVE.B), while Yahoo uses - (INVE-B).
    if exchange.upper() in ("OMX", "OSE", "CSE", "HEL"):
        t = t.replace("-", ".")
    return t


def _fetch_earnings_stockanalysis(stock: dict, db: Database) -> bool:
    """Look up next earnings date on stockanalysis.com."""
    import ssl as _ssl
    raw_ticker = stock["ticker"].upper()
    exchange = stock["exchange"].upper()
    slug = _SA_SLUG.get(exchange)
    if slug is None and exchange not in ("NASDAQ", "NYSE", "AMEX"):
        return False  # unsupported exchange

    ticker = _sa_ticker(exchange, raw_ticker)
    url = (f"https://stockanalysis.com/stocks/{ticker}/"
           if slug is None else
           f"https://stockanalysis.com/quote/{slug}/{ticker}/")

    ctx = _ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = _ssl.CERT_NONE
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
            "Accept-Encoding": "identity",
            "Accept": "text/html",
        })
        with urllib.request.urlopen(req, timeout=8, context=ctx) as resp:
            html = resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        if e.code != 404:
            logger.warning("stockanalysis.com HTTP %d for %s", e.code, ticker)
        return False
    except Exception as e:
        logger.warning("stockanalysis.com fetch failed for %s: %s", ticker, e)
        return False

    m = re.search(r'Earnings Date</td><td[^>]*>([^<]{3,60})', html)
    if not m:
        return False
    raw = m.group(1).strip()
    # stockanalysis.com format is "Apr 28, 2026" or sometimes "-" for TBA
    parsed = _try_parse_sa_date(raw)
    if parsed is None:
        return False

    # Use the original watchlist ticker for the DB key so the
    # earnings row joins back to the watchlist cleanly.
    db.upsert_earnings(
        ticker=raw_ticker, exchange=stock["exchange"],
        report_date=parsed.strftime("%Y-%m-%d"),
        fiscal_period="Next report",
        source_url=url)
    logger.info("  → %s earnings date: %s (stockanalysis.com)",
                 raw_ticker, parsed.strftime("%Y-%m-%d"))
    return True


def _try_parse_sa_date(s: str):
    """Parse 'Apr 28, 2026' style dates from stockanalysis.com."""
    s = s.strip().replace(",", "")
    for fmt in ("%b %d %Y", "%B %d %Y"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


# NASDAQ calendar response cache keyed by YYYY-MM-DD — the scanner walks
# forward ~60 days and reuses results across consecutive ticker lookups.
_NASDAQ_CAL_CACHE: dict[str, set[str]] = {}


# SQLite-backed cache for NASDAQ calendar day sets. Each day returns a
# fixed list of tickers reporting that day, so we can save it forever
# for past days (they never change) and refresh for a short window for
# upcoming days (schedules can slip). 12-hour TTL for future dates is a
# good balance — re-fetching doesn't waste credits, just a few requests.
def _nasdaq_cal_cache_init(db: Database):
    try:
        db.conn.execute("""
            CREATE TABLE IF NOT EXISTS nasdaq_cal_cache (
                day         TEXT PRIMARY KEY,
                tickers     TEXT NOT NULL,
                fetched_at  TEXT NOT NULL
            )""")
    except Exception:
        pass


def _nasdaq_cal_load_from_db(db: Database, day: str):
    try:
        row = db.conn.execute(
            "SELECT tickers, fetched_at FROM nasdaq_cal_cache WHERE day = ?",
            (day,)).fetchone()
    except Exception:
        return None
    if not row:
        return None
    tickers = set((row["tickers"] or "").split(",")) if row["tickers"] else set()
    tickers.discard("")
    # Past days are immutable — always reuse. Future days expire after 12h.
    try:
        is_future = day >= datetime.now().strftime("%Y-%m-%d")
    except Exception:
        is_future = False
    if is_future:
        try:
            age = datetime.now() - datetime.strptime(
                row["fetched_at"][:19], "%Y-%m-%dT%H:%M:%S")
            if age.total_seconds() > 12 * 3600:
                return None
        except Exception:
            pass
    return tickers


def _nasdaq_cal_save_to_db(db: Database, day: str, tickers: set[str]):
    try:
        db.conn.execute(
            """INSERT OR REPLACE INTO nasdaq_cal_cache (day, tickers, fetched_at)
               VALUES (?, ?, ?)""",
            (day, ",".join(sorted(tickers)),
             datetime.utcnow().isoformat() + "Z"))
        db.conn.commit()
    except Exception:
        pass


def _nasdaq_cal_fetch_one(day: str) -> tuple[str, set[str]]:
    """Fetch the NASDAQ calendar for a single day via HTTP. No cache."""
    url = f"https://api.nasdaq.com/api/calendar/earnings?date={day}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=8) as resp:
            data = json.loads(resp.read().decode("utf-8", errors="replace"))
        rows = (((data or {}).get("data") or {}).get("rows")) or []
        return day, {(r.get("symbol") or "").upper()
                     for r in rows if r.get("symbol")}
    except Exception as e:
        logger.debug("NASDAQ calendar fetch failed for %s: %s", day, e)
        return day, set()


def _nasdaq_cal_get_many(db: Database, days: list[str]) -> dict[str, set[str]]:
    """Batch-fetch calendar day sets with caching + 10-way parallelism."""
    _nasdaq_cal_cache_init(db)
    out: dict[str, set[str]] = {}
    missing: list[str] = []

    for day in days:
        # In-process cache (current run)
        if day in _NASDAQ_CAL_CACHE:
            out[day] = _NASDAQ_CAL_CACHE[day]
            continue
        # DB cache (persisted across runs)
        cached = _nasdaq_cal_load_from_db(db, day)
        if cached is not None:
            out[day] = cached
            _NASDAQ_CAL_CACHE[day] = cached
            continue
        missing.append(day)

    if missing:
        import concurrent.futures as _cf
        logger.info("NASDAQ calendar: fetching %d uncached days (parallel)",
                     len(missing))
        with _cf.ThreadPoolExecutor(max_workers=10) as pool:
            for day, tickers in pool.map(_nasdaq_cal_fetch_one, missing):
                out[day] = tickers
                _NASDAQ_CAL_CACHE[day] = tickers
                _nasdaq_cal_save_to_db(db, day, tickers)

    return out


def _fetch_earnings_nasdaq_calendar(stock: dict, db: Database, config: dict,
                                     past_only: bool = False) -> bool:
    """
    Free earnings date lookup for US stocks using NASDAQ's public
    calendar JSON feed. Scans forward (next 75 days) AND backward
    (last 365 days) in parallel with a persistent day-set cache.

    When past_only=True, skip the forward scan — callers use this when
    stockanalysis.com already supplied the upcoming date and we only
    want historical quarterly reports.
    """
    ticker = stock["ticker"].upper()
    exchange = stock["exchange"]
    now = datetime.now()

    forward_days = [] if past_only else [
        (now + timedelta(days=d)).strftime("%Y-%m-%d") for d in range(0, 75)
    ]
    backward_days = [(now - timedelta(days=d)).strftime("%Y-%m-%d")
                     for d in range(1, 366)]
    all_days = forward_days + backward_days

    cal = _nasdaq_cal_get_many(db, all_days)
    found_any = False

    # Upcoming — first match (skipped in past_only mode)
    for day in forward_days:
        if ticker in cal.get(day, set()):
            db.upsert_earnings(
                ticker=ticker, exchange=exchange, report_date=day,
                fiscal_period="Next report",
                source_url=f"https://www.nasdaq.com/market-activity/earnings?date={day}")
            logger.info("  → %s upcoming earnings: %s", ticker, day)
            found_any = True
            break

    # Historical — every match
    for day in backward_days:
        if ticker in cal.get(day, set()):
            db.upsert_earnings(
                ticker=ticker, exchange=exchange, report_date=day,
                fiscal_period="Report",
                source_url=f"https://www.nasdaq.com/market-activity/earnings?date={day}")
            found_any = True

    if not found_any and not past_only:
        logger.info("  → %s not found in NASDAQ calendar (±12 months)", ticker)
    return found_any


# ---------------------------------------------------------------------------
# D) FORUM FETCHER
# ---------------------------------------------------------------------------

def fetch_forums(stock: dict, db: Database, config: dict) -> int:
    """
    Fetch latest forum mentions for a stock from its configured sources.
    Returns the count of new mentions stored.
    """
    ticker = stock["ticker"]
    code = stock.get("code", ticker)
    exchange = stock["exchange"]
    lang = stock.get("lang", "en")
    forum_sources = list(stock.get("forum_sources", []) or [])
    url_templates = config.get("forum_urls", {})
    new_count = 0

    # Merge any user-configured Telegram channels for this exchange.
    # Stored in app_settings["telegram_channels"] as JSON mapping
    # EXCHANGE_CODE → [channel1, channel2, ...]. Users manage this
    # from the Engine Room settings card.
    try:
        import json as _json
        tg_setting = db.get_setting("telegram_channels", "")
        if tg_setting:
            tg_map = _json.loads(tg_setting)
            for ch in tg_map.get(exchange, []) or []:
                key = f"telegram:{ch}"
                if key not in forum_sources:
                    forum_sources.append(key)
    except Exception as _e:
        logger.debug("telegram_channels setting parse failed: %s", _e)

    # Check if Serper-based forum sources should be skipped (fresh data)
    serper_forum_fresh = _is_fresh(db, "forum_mentions", ticker, STALE_FORUM_HOURS)

    for forum_name in forum_sources:

        # Special case: Telegram group — fetch web preview and filter
        # for messages mentioning our stock's ticker or name
        if forum_name.startswith("telegram:"):
            tg_channel = forum_name.split(":", 1)[1]
            logger.info("FORUM Telegram: %s → t.me/s/%s", ticker, tg_channel)
            tg_url = f"https://t.me/s/{tg_channel}"
            tg_text = _fetch_page_text(tg_url, timeout=15)
            if tg_text:
                # Include alternate tickers (e.g. stockscope_ticker)
                alt_ticker = stock.get("stockscope_ticker", "")
                search_name = stock["name"]
                if alt_ticker and alt_ticker != ticker:
                    search_name += " " + alt_ticker
                tg_posts = _extract_telegram_posts(tg_text, ticker, search_name)
                for post in tg_posts:
                    stored = db.insert_forum(
                        ticker=ticker, exchange=exchange,
                        forum=f"telegram/{tg_channel}",
                        author=tg_channel,
                        text=post["text"][:400],
                        post_url=tg_url,
                        posted_at=post.get("date", ""),
                        lang=lang)
                    if stored:
                        new_count += 1
            continue

        # Special case: Twitter/X search via Serper
        if forum_name == "twitter":
            if not _SERPER_ENABLED:
                # Free-refresh mode: Twitter search is Serper-only.
                continue
            if serper_forum_fresh:
                logger.info("FORUM Twitter skip %s — data is fresh", ticker)
                continue
            logger.info("FORUM Twitter/X search for %s", ticker)
            name_q = stock["name"]
            yahoo_tk = stock.get("yahoo_ticker", "")

            # Build cashtags — Twitter uses $TICKER (not Yahoo format).
            # For stocks with a Yahoo ticker that differs (e.g. 5236.KL),
            # include both the plain ticker cashtag and Yahoo cashtag.
            # e.g. MATRIX → "$MATRIX" OR "$5236.KL"
            cashtags = [f"${ticker}"]
            if yahoo_tk and yahoo_tk != ticker:
                cashtags.append(f"${yahoo_tk}")
            cashtag_query = " OR ".join(f'"{ct}"' for ct in cashtags)
            query = f'site:x.com "{name_q}" OR {cashtag_query}'
            results = serper_web_search(query, config, caller="forums", ticker=ticker)

            # Build relevance check terms
            name_words = [w.lower() for w in name_q.split() if len(w) >= 4]
            cashtags_lower = [ct.lower() for ct in cashtags]

            for item in results[:10]:
                title = item.get("title", "")
                snippet = item.get("snippet", "")
                link = item.get("link", "")
                pub_date = item.get("date", "")
                if not title or not pub_date:
                    continue
                # Relevance check: the tweet must actually be about our company.
                # Strategy:
                #  1. Check if the full company name (or close variant) appears
                #     as a phrase (e.g. "Matrix Concept" adjacent words)
                #  2. Or the ticker appears alongside a name word
                #  3. Reject scattered common-word matches like
                #     "Eisenhower's matrix" + "concept" in different sentences
                text_check = (title + " " + snippet).lower()
                tk_lower = ticker.lower()
                has_ticker = bool(re.search(r'\b' + re.escape(tk_lower) + r'\b', text_check))

                # Check for company name as adjacent phrase
                # Build a phrase from the first 2-3 significant name words
                # e.g. "matrix concept" from "Matrix Concept Holdings"
                name_phrase = " ".join(name_words[:3])
                has_phrase = name_phrase in text_check if len(name_words) >= 2 else False

                # For single-word names, just check that word
                if len(name_words) == 1:
                    has_phrase = bool(re.search(r'\b' + re.escape(name_words[0]) + r'\b', text_check))

                # For phrases that are common English expressions
                # (e.g. "focus point", "critical holdings"), require
                # additional stock context nearby
                _STOCK_CONTEXT = [
                    "stock", "share", "bhd", "berhad", "holdings",
                    "klse", "bursa", "sgx", "ngx", "brvm", "nasdaq",
                    "dividend", "earnings", "ipo", "investor",
                ]
                if has_phrase and not has_ticker:
                    # Check if any stock-related word appears nearby
                    phrase_pos = text_check.find(name_phrase)
                    if phrase_pos >= 0:
                        window = text_check[max(0, phrase_pos-80):phrase_pos+len(name_phrase)+80]
                        if not any(ctx in window for ctx in _STOCK_CONTEXT + cashtags_lower):
                            has_phrase = False  # common phrase, no stock context

                # Accept if:
                #  - Company name phrase appears with stock context
                #  - Ticker appears near a name word (within 60 chars)
                relevant = False
                if has_phrase:
                    relevant = True
                elif has_ticker:
                    # Check that a non-ticker name word appears near the ticker
                    other_words = [w for w in name_words if w != tk_lower]
                    for tm in re.finditer(r'\b' + re.escape(tk_lower) + r'\b', text_check):
                        window = text_check[max(0, tm.start()-60):tm.end()+60]
                        if any(w in window for w in other_words):
                            relevant = True
                            break

                if not relevant:
                    continue
                text_combined = f"{title} — {snippet[:200]}" if snippet else title
                stored = db.insert_forum(
                    ticker=ticker, exchange=exchange,
                    forum="twitter",
                    author="X/Twitter",
                    text=text_combined[:400],
                    post_url=link,
                    posted_at=pub_date,
                    lang=lang)
                if stored:
                    new_count += 1
            continue

        # Special case: Serper-powered discussion search
        if forum_name == "serper_discuss":
            if not _SERPER_ENABLED:
                continue
            if serper_forum_fresh:
                logger.info("FORUM Serper skip %s — data is fresh", ticker)
                continue
            logger.info("FORUM Serper search for %s", ticker)
            query = f'"{stock["name"]}" {ticker} stock discussion analysis opinion'
            results = serper_web_search(query, config, caller="forums", ticker=ticker)
            for item in results[:10]:
                title = item.get("title", "")
                snippet = item.get("snippet", "")
                link = item.get("link", "")
                pub_date = item.get("date", "")
                source = item.get("source", "")
                if not title:
                    continue
                # Skip results without dates — these are evergreen pages
                # (LinkedIn profiles, PDFs, company pages), not discussions
                if not pub_date:
                    continue
                text_combined = f"{title} — {snippet[:200]}" if snippet else title
                stored = db.insert_forum(
                    ticker=ticker, exchange=exchange,
                    forum="web",
                    author=source or "Web",
                    text=text_combined[:400],
                    post_url=link,
                    posted_at=pub_date,
                    lang=lang)
                if stored:
                    new_count += 1
            continue

        template = url_templates.get(forum_name, "")
        if not template:
            logger.info("No URL template for forum '%s'", forum_name)
            continue

        url = template.format(ticker=ticker, code=code,
                              name=urllib.parse.quote(stock["name"]))
        logger.info("FORUM fetch: %s → %s", forum_name, url)
        text = _fetch_page_text(url)
        if not text:
            continue

        comments = _extract_forum_comments(text, forum_name)

        for comment in comments[:20]:  # cap at 20 per source per run
            stored = db.insert_forum(
                ticker=ticker, exchange=exchange,
                forum=forum_name,
                author=comment.get("author", ""),
                text=comment.get("text", ""),
                post_url=url,
                posted_at=comment.get("date", ""),
                lang=lang)
            if stored:
                new_count += 1

    logger.info("  → %d new forum mentions for %s", new_count, ticker)
    return new_count


def _extract_forum_comments(page_text: str, forum_name: str) -> list[dict]:
    """
    Extract forum comments/threads from raw page text.
    Dispatches to forum-specific parsers when available.

    Returns list of {"author": ..., "text": ..., "date": ...}
    """
    # Use dedicated parsers for known forums
    if forum_name == "richbourse":
        return _extract_richbourse_threads(page_text)
    if forum_name == "i3investor":
        return _extract_i3investor_comments(page_text)

    # Generic fallback for other forums
    comments = []
    paragraphs = re.split(r"\n{2,}", page_text)

    for para in paragraphs:
        para = para.strip()
        if len(para) < 30 or len(para) > 2000:
            continue
        if any(skip in para.lower() for skip in [
            "copyright", "terms of use", "privacy policy",
            "cookie", "sign up", "log in", "register",
            "advertisement", "all rights reserved"
        ]):
            continue

        author = ""
        date = ""

        author_match = re.search(r"(?:^|\n)(\w[\w\s]{2,20})\s*[-–|]\s*", para)
        if author_match:
            author = author_match.group(1).strip()

        for pat in _DATE_PATTERNS:
            m = pat.search(para)
            if m:
                date = m.group(1)
                break

        comments.append({
            "author": author,
            "text": para[:500],
            "date": date
        })

    return comments


def _extract_richbourse_threads(page_text: str) -> list[dict]:
    """
    Parse richbourse.com forum listing page.

    The page text contains thread entries in this format:
        THREAD_TITLE  AUTHOR  NUM_REPLIES  DD/MM/YY - HH:MM  VIEWS  CATEGORY

    Example:
        ETI TOGO et si on en parlait  Mig229 13 04/04/26 - 00:03 1133

    We extract: title as text, author, date, and view count.
    """
    threads = []

    # Pattern: title, then author, then count, then date DD/MM/YY
    # The text from richbourse has threads as:
    # "TITLE  AUTHOR  NUM  DD/MM/YY - HH:MM  VIEWS"
    pat = re.compile(
        r'([A-ZÀ-Ý][^\n]{10,80}?)\s+'       # thread title (starts with uppercase)
        r'([\w-]{2,25})\s+'                    # author username
        r'(\d{1,4})\s+'                        # reply count
        r'(\d{2}/\d{2}/\d{2})\s*-\s*'         # date DD/MM/YY
        r'\d{2}:\d{2}\s+'                      # time HH:MM
        r'(\d+)',                               # view count
    )

    for m in pat.finditer(page_text):
        title = m.group(1).strip()
        author = m.group(2).strip()
        date_str = m.group(3).strip()  # reply count
        date_raw = m.group(4)          # DD/MM/YY
        views = m.group(5)

        # Skip navigation/boilerplate
        if any(skip in title.lower() for skip in [
            "inscription", "connexion", "rechercher",
            "palmarès", "prévision", "analyse graphique"
        ]):
            continue

        # Convert DD/MM/YY to readable date
        try:
            dt = datetime.strptime(date_raw, "%d/%m/%y")
            date_formatted = dt.strftime("%Y-%m-%d")
        except ValueError:
            date_formatted = date_raw

        threads.append({
            "author": author,
            "text": f"{title} ({views} vues, {date_str} réponses)",
            "date": date_formatted,
        })

    return threads


def _extract_i3investor_comments(page_text: str) -> list[dict]:
    """
    Parse i3investor stock discussion page.

    The page contains comments in this repeating pattern:
        username
        comment text (one or more lines)
        YYYY-MM-DD HH:MM

    We extract the most recent comments, capped at 20.
    """
    comments = []
    lines = page_text.split("\n")

    # Find the "Showing N of M comments" marker to locate the start
    start_idx = 0
    for i, line in enumerate(lines):
        if "comments" in line.lower() and "showing" in line.lower():
            start_idx = i + 1
            break

    # Parse username / text / date triplets
    meaningful = []
    for line in lines[start_idx:]:
        stripped = line.strip()
        if stripped and len(stripped) < 500:
            meaningful.append(stripped)

    i = 0
    while i < len(meaningful) - 1:
        # Look for a date pattern: YYYY-MM-DD HH:MM
        # Walk forward to find it
        author = meaningful[i]

        # Skip if it looks like boilerplate
        if any(skip in author.lower() for skip in [
            "copyright", "cookie", "sign in", "privacy",
            "all rights", "terms of", "powered by",
            "like", "comment", "social forum", "subscribe",
        ]):
            i += 1
            continue

        # Author should be a short username (3-25 chars, no spaces typically)
        if len(author) > 30 or len(author) < 2:
            i += 1
            continue

        # Collect text lines until we hit a date
        text_parts = []
        j = i + 1
        date_found = ""
        while j < len(meaningful):
            line = meaningful[j]
            # Check if this line is a date
            date_match = re.match(r"^(20\d{2}-\d{2}-\d{2})\s+\d{2}:\d{2}$", line)
            if date_match:
                date_found = date_match.group(1)
                j += 1
                break
            text_parts.append(line)
            j += 1
            if len(text_parts) > 5:  # safety: don't collect too many lines
                break

        if date_found and text_parts:
            comment_text = " ".join(text_parts)[:300]
            comments.append({
                "author": author,
                "text": comment_text,
                "date": date_found,
            })

        i = j
        if len(comments) >= 20:
            break

    return comments


# ---------------------------------------------------------------------------
def _extract_telegram_posts(page_text: str, ticker: str,
                             company_name: str) -> list[dict]:
    """
    Parse the Telegram web preview (t.me/s/channel) and extract
    messages that mention our stock ticker or company name.

    Uses two splitting strategies:
    1. Split by "N views" markers (separates individual messages)
    2. Split by channel name (backup if views-split misses entries)

    Returns list of {"text": ..., "date": ...}
    The page structure has:
    - Date headers: "April 1, 2026" between groups of messages
    - Messages ending with: "N views [edited] HH:MM"

    We scan linearly, tracking the current date from headers,
    and extract time from each message's "views HH:MM" marker.

    Returns list of {"text": ..., "date": ...}
    """
    # Build search terms
    search_terms = [ticker.lower()]
    for word in company_name.lower().split():
        if len(word) >= 3:
            search_terms.append(word)

    # First pass: find all dates in the page and their positions.
    # Telegram uses "April 1, 2026" as day headers, but messages also
    # contain dates like "25 Mar 2026" or "01 Apr 2026" in their text.
    date_positions = []
    _DATE_PATTERNS_TG = [
        # "April 1, 2026" or "April 1 2026"
        (re.compile(r'((?:January|February|March|April|May|June|July|August|September'
                    r'|October|November|December)\s+\d{1,2},?\s+\d{4})'),
         ["%B %d %Y", "%b %d %Y"]),
        # "01 Apr 2026" or "20 March 2026"
        (re.compile(r'(\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\s+20\d{2})'),
         ["%d %b %Y", "%d %B %Y"]),
    ]
    for pat, fmts in _DATE_PATTERNS_TG:
        for m in pat.finditer(page_text):
            raw = m.group(1).strip().replace(",", "")
            for fmt in fmts:
                try:
                    dt = datetime.strptime(raw, fmt)
                    date_positions.append((m.start(), dt.strftime("%Y-%m-%d")))
                    break
                except ValueError:
                    continue
    # Sort by position so _date_at works correctly
    date_positions.sort(key=lambda x: x[0])

    def _date_at(pos):
        """Return the nearest date for a position.
        Telegram shows newest messages first with date headers between
        day groups. Try: closest date header AFTER the position first
        (same day's header), then fall back to closest BEFORE."""
        # First: find closest date AFTER this position (the day's header)
        for dp, d in date_positions:
            if dp > pos:
                return d
        # Fallback: closest before
        best = ""
        for dp, d in date_positions:
            if dp < pos:
                best = d
        return best

    # Extract channel name for splitting and cleanup
    title_match = re.search(r'^(.{10,60})\s*–\s*Telegram', page_text, re.MULTILINE)
    chan_name = title_match.group(1).strip() if title_match else None

    # Second pass: split by "N views [edited] HH:MM" markers
    msg_pattern = re.compile(r'(\d+)\s*views?\s*(?:edited\s*)?(\d{1,2}:\d{2})')
    matches = list(msg_pattern.finditer(page_text))

    posts = []
    seen = set()
    prev_end = 0

    for m in matches:
        msg_text = page_text[prev_end:m.start()]
        time_str = m.group(2)
        prev_end = m.end()

        msg_clean = re.sub(r'\s+', ' ', msg_text).strip()
        if len(msg_clean) < 20:
            continue

        # The first chunk includes page header boilerplate.
        # Strip it by taking text after the last occurrence of channel name.
        if chan_name and chan_name in msg_clean:
            last_split = msg_clean.rfind(chan_name)
            msg_clean = msg_clean[last_split + len(chan_name):].strip()

        if len(msg_clean) < 20 or len(msg_clean) > 2000:
            continue

        msg_lower = msg_clean.lower()
        if not any(term in msg_lower for term in search_terms):
            continue

        key = msg_clean[:80]
        if key in seen:
            continue
        seen.add(key)

        date_str = _date_at(m.start())
        full_date = f"{date_str} {time_str}" if date_str else ""

        posts.append({"text": msg_clean[:400], "date": full_date})

    # Backup: channel-name splitting for messages without "views" marker
    if chan_name:
        for chunk in page_text.split(chan_name):
            chunk_clean = re.sub(r'\s+', ' ', chunk).strip()
            if len(chunk_clean) < 30 or len(chunk_clean) > 2000:
                continue
            if not any(term in chunk_clean.lower() for term in search_terms):
                continue
            if any(skip in chunk_clean.lower() for skip in ["subscribers", "if you have telegram"]):
                continue
            key = chunk_clean[:80]
            if key in seen:
                continue
            seen.add(key)
            posts.append({"text": chunk_clean[:400], "date": ""})

    return posts


# E) PRICE FETCHER
# ---------------------------------------------------------------------------

# Yahoo Finance v8 JSON API — unofficial but widely used.
# Returns chart data including current price and previous close.
YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=1d&interval=1d"


def _fetch_price_yahoo(yahoo_ticker: str) -> Optional[tuple]:
    """
    Fetch price from Yahoo Finance v8 chart API.
    Returns (price, change_pct, currency) or None on failure.
    """
    if not yahoo_ticker:
        return None

    url = YAHOO_CHART_URL.format(ticker=urllib.parse.quote(yahoo_ticker))
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    }
    req = urllib.request.Request(url, headers=headers)

    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))

        result = data.get("chart", {}).get("result", [])
        if not result:
            return None

        meta = result[0].get("meta", {})
        price = meta.get("regularMarketPrice")
        prev_close = meta.get("chartPreviousClose") or meta.get("previousClose")
        currency = meta.get("currency", "")

        if price is None:
            return None

        # Calculate change %
        if prev_close and prev_close > 0:
            change_pct = ((price - prev_close) / prev_close) * 100
        else:
            change_pct = 0.0

        return (float(price), round(change_pct, 2), currency)

    except urllib.error.HTTPError as e:
        logger.warning("Yahoo Finance HTTP %d for %s", e.code, yahoo_ticker)
        return None
    except Exception as e:
        logger.warning("Yahoo Finance failed for %s: %s", yahoo_ticker, e)
        return None


def _fetch_price_scrape(stock: dict, config: dict) -> Optional[tuple]:
    """
    Fallback: scrape price from exchange-specific website.
    Each exchange has a custom parser tuned to its page format.

    Returns (price, change_pct, currency) or None.
    """
    price_url = stock.get("price_url", "")
    currency = stock.get("currency", "")
    ticker = stock["ticker"]
    exchange = stock["exchange"]

    # KASE: look up the shared shares-table cache instead of fetching
    # a per-ticker page (kase.kz is an Angular SPA so the per-ticker
    # HTML doesn't contain the price anyway — only /en/shares/ does).
    if exchange == "KASE":
        logger.info("PRICE scrape fallback: %s → kase.kz/en/shares/ (table)", ticker)
        table = _kase_shares_table()
        if table and ticker in table:
            price, chg = table[ticker]
            currency = stock.get("currency", "KZT")
            logger.info("  → KASE %s: %s %s (%+.2f%%)",
                         ticker, currency, f"{price:,.2f}", chg)
            return (price, chg, currency)
        return None

    if not price_url:
        return None

    logger.info("PRICE scrape fallback: %s → %s", ticker, price_url)
    text = _fetch_page_text(price_url, timeout=20)
    if not text:
        return None

    # ---------- Exchange-specific extraction ----------

    if exchange == "UZSE":
        # stockscope.uz — two formats:
        # Main page:    "HMKB  Company Name  54 UZS  -3.55%"
        # Listing page: "3.74 UZS -0.04 UZS  -1.06%"
        ss_ticker = stock.get("stockscope_ticker", ticker)

        # Try individual listing page format first
        indiv = _extract_stockscope_listing(text, currency)
        if indiv:
            return indiv
        # Then try main page format
        return _extract_stockscope_price(text, ss_ticker, currency)

    elif exchange == "KSE":
        # kse.kg/en/instrument/MAIR — ticker appears as "MAIR6" etc.
        # followed by price on next line
        return _extract_kse_price(text, ticker, currency)

    elif exchange == "BRVM":
        # brvm.org closing price table has format:
        #   ETIT Ecobank Transnational... 278 356 34 33 34 3,03
        # The columns are: ticker, name, vol?, prev, open, low, close, change%
        return _extract_brvm_price(text, ticker, currency)

    elif exchange == "NGX":
        # TradingView page: "current price of TICKER is 26.20 NGN"
        # and "1 day 0.38%" for daily change
        return _extract_tradingview_price(text, ticker, currency)

    else:
        return _extract_price_from_text(text, currency,
            keywords=["price", "last", "close", "current"])


# KASE shares table cache — parsed from a single HTTP call to
# kase.kz/en/shares/. TTL 5 minutes so a watchlist with 5 Kazakh
# stocks doesn't refetch the page 5 times.
_KASE_TABLE_CACHE: dict = {"ts": 0.0, "data": {}}


def _kase_shares_table() -> dict[str, tuple[float, float]]:
    """Return {ticker: (price, change_pct)} parsed from kase.kz/en/shares/."""
    import time as _t
    now = _t.time()
    if now - _KASE_TABLE_CACHE["ts"] < 300 and _KASE_TABLE_CACHE["data"]:
        return _KASE_TABLE_CACHE["data"]

    try:
        import ssl as _ssl
        ctx = _ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = _ssl.CERT_NONE
        req = urllib.request.Request(
            "https://kase.kz/en/shares/",
            headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X)"})
        with urllib.request.urlopen(req, timeout=10, context=ctx) as resp:
            html = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        logger.warning("KASE shares fetch failed: %s", e)
        return _KASE_TABLE_CACHE["data"]

    out: dict[str, tuple[float, float]] = {}
    rows = re.findall(r"<tr[^>]*>([\s\S]*?)</tr>", html)
    for row in rows:
        tm = re.search(r"/en/investors/shares/([A-Z][A-Z0-9_]{1,12})", row)
        if not tm:
            continue
        ticker = tm.group(1)
        if ticker in out:
            continue  # take the first occurrence (main table, not the sidebar)
        cells_raw = re.findall(r"<td[^>]*>([\s\S]*?)</td>", row)
        cells = [re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", c)).strip()
                 for c in cells_raw]
        if len(cells) < 2:
            continue
        # Price cell: "399,54" or "1 179,00" — comma decimal, spaces as thousand sep
        price_m = re.search(r"([\d\s]+(?:,\d+)?)", cells[1])
        if not price_m:
            continue
        try:
            price = float(price_m.group(1).replace(" ", "").replace(",", "."))
        except ValueError:
            continue
        # Change cell (3rd column) — "+0,16", "-0,18", "0,00"
        chg = 0.0
        if len(cells) >= 3:
            chg_m = re.search(r"([+-]?\s*[\d,]+)", cells[2])
            if chg_m:
                try:
                    chg = float(chg_m.group(1).replace(" ", "").replace(",", "."))
                except ValueError:
                    pass
        out[ticker] = (price, chg)

    _KASE_TABLE_CACHE["ts"] = now
    _KASE_TABLE_CACHE["data"] = out
    logger.info("KASE shares table: parsed %d tickers", len(out))
    return out


def _extract_tradingview_price(text: str, ticker: str,
                                currency: str) -> Optional[tuple]:
    """
    Parse TradingView symbol page.
    Looks for: "current price of TICKER is 26.20 NGN"
    And: "1 day 0.38%" for daily change.
    """
    # Extract current price: "price of TICKER is PRICE CURRENCY"
    pat = re.compile(
        r'(?:current\s+)?price\s+of\s+' + re.escape(ticker) +
        r'\s+is\s+([\d,]+(?:\.\d+)?)\s+' + re.escape(currency),
        re.IGNORECASE
    )
    m = pat.search(text)
    if not m:
        return None

    try:
        price = float(m.group(1).replace(",", ""))
    except ValueError:
        return None

    # Extract 1-day change %: "1 day N.NN%"  (may use − instead of -)
    change = 0.0
    chg_pat = re.compile(r'1\s+day\s+([-−]?\d+\.\d+)%')
    chg_m = chg_pat.search(text)
    if chg_m:
        try:
            change = float(chg_m.group(1).replace("−", "-"))
        except ValueError:
            pass

    logger.info("  → TradingView %s: %s %.2f (%+.2f%%)", ticker, currency, price, change)
    return (price, change, currency)


def _extract_stockscope_listing(text: str, currency: str) -> Optional[tuple]:
    """
    Parse individual stockscope.uz listing page.
    Format: "3.78 UZS + 0.09 UZS  2.44%" or "3.74 UZS -0.04 UZS  -1.06%"
    """
    pat = re.compile(
        r'([\d,]+(?:\.\d+)?)\s+UZS\s*[-+]?\s*[\d,.]+\s+UZS\s+([-+]?\d+\.\d+)%'
    )
    m = pat.search(text)
    if m:
        try:
            price = float(m.group(1).replace(",", ""))
            change = float(m.group(2))
            if 0 < price < 50_000_000:
                logger.info("  → stockscope listing: %s %.2f (%+.2f%%)",
                             currency, price, change)
                return (price, change, currency)
        except ValueError:
            pass
    return None


def _extract_stockscope_price(text: str, ss_ticker: str,
                               currency: str) -> Optional[tuple]:
    """
    Parse stockscope.uz main page.
    Format:  HMKB  Company Name  54 UZS  -3.55%
    """
    # Pattern: TICKER  ...  PRICE UZS  CHANGE%
    pat = re.compile(
        re.escape(ss_ticker) + r'\s+[\w\s\'\"\.]+?\s+'
        r'([\d,]+(?:\.\d+)?)\s+UZS\s+([-+]?\d+\.\d+)%',
        re.IGNORECASE
    )
    m = pat.search(text)
    if m:
        price_str = m.group(1).replace(",", "")
        change_str = m.group(2)
        try:
            price = float(price_str)
            change = float(change_str)
            logger.info("  → stockscope %s: %s %.2f (%+.2f%%)",
                         ss_ticker, currency, price, change)
            return (price, change, currency)
        except ValueError:
            pass
    logger.warning("  → stockscope: no match for %s", ss_ticker)
    return None


def _extract_brvm_price(text: str, ticker: str,
                         currency: str) -> Optional[tuple]:
    """
    Parse brvm.org page for a BRVM stock price.

    The page has a compact ticker list in the format:
        ETIT \xa0 34 \xa0 3,03%
        SDSC \xa0 1 855 \xa0 0,27%
    where \xa0 is non-breaking space, and prices >999 use space as
    thousands separator (e.g., "1 855" = 1855, "14 155" = 14155).

    We match: TICKER <separator> PRICE <separator> CHANGE%
    """
    # Normalize non-breaking spaces to regular spaces
    text_clean = text.replace("\xa0", " ")

    # Pattern: TICKER  PRICE  CHANGE%
    # PRICE can be "34" or "1 855" or "14 155" (space-separated thousands)
    # CHANGE is like "3,03%" or "-1,64%"
    pat = re.compile(
        re.escape(ticker) + r'\s+([\d]+(?:\s\d{3})*)\s+([-]?\d+[,]\d{2})%'
    )
    m = pat.search(text_clean)
    if m:
        price_str = m.group(1).replace(" ", "")  # "1 855" → "1855"
        change_str = m.group(2).replace(",", ".")  # "3,03" → "3.03"
        try:
            price = float(price_str)
            change = float(change_str)
            logger.info("  → BRVM %s: %s %.0f (%+.2f%%)",
                         ticker, currency, price, change)
            return (price, change, currency)
        except ValueError:
            pass

    logger.warning("  → BRVM: no price found for %s", ticker)
    return None


def _extract_kse_price(text: str, ticker: str,
                        currency: str) -> Optional[tuple]:
    """
    Parse kse.kg instrument page.
    Format: MAIR6 \\n 540 \\n 5900
    The first number after the ticker variant is the price.
    """
    # KSE lists instruments as TICKER+suffix (e.g., MAIR6, KAKB26)
    pat = re.compile(
        re.escape(ticker) + r'\d*\s+(\d[\d\s]*(?:\.\d+)?)',
        re.IGNORECASE
    )
    m = pat.search(text)
    if m:
        price_str = m.group(1).strip().split()[0].replace(" ", "")
        try:
            price = float(price_str)
            if 0 < price < 50_000_000:
                logger.info("  → KSE %s: %s %.2f", ticker, currency, price)
                return (price, 0.0, currency)
        except ValueError:
            pass

    logger.warning("  → KSE: no price found for %s", ticker)
    return None


def _extract_price_from_text(text: str, currency: str,
                              keywords: list) -> Optional[tuple]:
    """
    Generic price extractor: search for a number near any of the
    given keywords in the page text.

    Returns (price, 0.0, currency) — change_pct is 0 because
    scraped pages rarely provide previous close in a parseable way.
    """
    text_lower = text.lower()

    # Pattern: number with optional thousands separator and decimals
    # Matches: "1,234.56", "12345", "1 234.56", "1234,56" (European)
    price_pat = re.compile(r"[\d][\d\s,]*[\d](?:\.\d{1,4})?")

    for kw in keywords:
        idx = text_lower.find(kw)
        if idx == -1:
            continue

        # Search in a window after the keyword
        window = text[idx:idx + 200]
        matches = price_pat.findall(window)

        for m in matches:
            # Clean the match: remove spaces and extra commas
            cleaned = m.replace(" ", "").replace(",", "")
            try:
                val = float(cleaned)
                # Sanity check: price should be positive and not absurdly large
                # (skip things like years: 2024, 2025, 2026)
                if 0.001 < val < 50_000_000 and not (2020 <= val <= 2030):
                    logger.info("  → Scraped price: %s %s", currency, val)
                    return (val, 0.0, currency)
            except ValueError:
                continue

    return None


def _fetch_price_serper(stock: dict, config: dict) -> Optional[tuple]:
    """
    Last resort: search Serper for "[company] [ticker] stock price"
    and try to extract a price from the answer box or snippets.
    """
    name = stock["name"]
    ticker = stock["ticker"]
    currency = stock.get("currency", "")
    exchange = stock["exchange"]

    query = f"{name} {ticker} {exchange} stock price today"
    data = _call_serper("/search", {"q": query, "num": 5})
    if not data:
        return None

    # Check for answer box / knowledge graph
    answer = data.get("answerBox", {})
    if answer:
        answer_text = answer.get("answer", "") or answer.get("snippet", "")
        price_match = re.search(r'([\d,]+(?:\.\d+)?)', answer_text)
        if price_match:
            try:
                price = float(price_match.group(1).replace(",", ""))
                if 0 < price < 50_000_000:
                    logger.info("  → Serper answer box: %s %.2f", currency, price)
                    return (price, 0.0, currency)
            except ValueError:
                pass

    # Check organic snippets for price patterns
    for item in data.get("organic", []):
        snippet = item.get("snippet", "")
        # Look for currency + price pattern or just price near stock keywords
        for pat in [
            re.compile(r'(?:price|NGN|₦|naira)\s*:?\s*([\d,]+(?:\.\d+)?)', re.I),
            re.compile(r'([\d,]+(?:\.\d+)?)\s*(?:NGN|naira|' + re.escape(currency) + r')', re.I),
        ]:
            m = pat.search(snippet)
            if m:
                try:
                    price = float(m.group(1).replace(",", ""))
                    if 0 < price < 50_000_000 and not (2020 <= price <= 2030):
                        logger.info("  → Serper snippet: %s %.2f", currency, price)
                        return (price, 0.0, currency)
                except ValueError:
                    continue

    return None


def fetch_prices(stock: dict, db: Database, config: dict) -> bool:
    """
    Fetch current price for a stock.
    Strategy: try Yahoo Finance first (if yahoo_ticker is set),
    then fall back to exchange-specific scraping.
    Returns True if a price was stored.
    """
    ticker = stock["ticker"]
    exchange = stock["exchange"]
    yahoo_ticker = stock.get("yahoo_ticker", "")

    result = None
    source_url = ""

    # Try Yahoo Finance first
    if yahoo_ticker:
        logger.info("PRICE Yahoo: %s → %s", ticker, yahoo_ticker)
        result = _fetch_price_yahoo(yahoo_ticker)
        if result:
            source_url = f"https://finance.yahoo.com/quote/{yahoo_ticker}"

    # Fallback to exchange-specific scraping
    if result is None:
        logger.info("PRICE fallback scrape for %s", ticker)
        result = _fetch_price_scrape(stock, config)
        if result:
            source_url = stock.get("price_url", "")

    # Last resort: Serper search for "TICKER stock price" (skipped in free mode)
    if result is None and _SERPER_ENABLED:
        logger.info("PRICE Serper search fallback for %s", ticker)
        result = _fetch_price_serper(stock, config)
        if result:
            source_url = "serper search"

    # Store if we got a price
    if result:
        price, change_pct, currency = result
        stored = db.insert_price(
            ticker=ticker, exchange=exchange,
            price=price, change_pct=change_pct,
            currency=currency, source_url=source_url)
        logger.info("  → %s price: %s %.2f (%+.1f%%) [%s]",
                     ticker, currency, price, change_pct,
                     "new" if stored else "already stored today")
        return True
    else:
        logger.warning("  → No price found for %s", ticker)
        return False


# ---------------------------------------------------------------------------
# F) INSIDER TRANSACTIONS FETCHER
# ---------------------------------------------------------------------------

def fetch_insiders(stock: dict, db: Database, config: dict) -> int:
    """
    Fetch insider/director transactions from the best source per exchange.
    Skips Serper calls if data was fetched within the last 24 hours.
    KLSE Screener scraping is always done (it's free, no API credits).
    """
    ticker = stock["ticker"]
    exchange = stock["exchange"]
    lang = stock.get("lang", "en")
    name = stock["name"]
    code = stock.get("code", ticker)
    new_count = 0

    serper_fresh = _is_fresh(db, "insider_transactions", ticker, STALE_INSIDER_HOURS)

    # ── KLSE: scrape KLSE Screener announcements (free, always run) ──
    if exchange == "KLSE":
        new_count += _fetch_insiders_klse(ticker, exchange, code, db)

    # ── NASDAQ: SEC EDGAR Form 3/4/5 atom feed (free, always run) ──
    if exchange == "NASDAQ":
        cik = stock.get("cik", "")
        if cik:
            new_count += _fetch_insiders_sec(ticker, exchange, cik, db)
        else:
            logger.info("INSIDER SEC EDGAR skip %s — no CIK in config", ticker)

    # ── Nordics: Finansinspektionen insider register (free, always run)
    # Covers Stockholm (OMX), Oslo (OSE), Copenhagen (CSE), Helsinki (HEL)
    # issuers traded on any EU-regulated venue via the MAR regulation. ──
    if exchange in ("OMX", "OSE", "CSE", "HEL"):
        new_count += _fetch_insiders_finansinspektionen(stock, db)

    # ── All exchanges: Serper web search (skip if fresh) ──
    if serper_fresh:
        logger.info("INSIDER skip Serper for %s — data is fresh", ticker)
        return new_count

    queries = []
    if lang == "fr":
        queries.append(
            f'"{name}" opération initié OR transaction directeur OR achat actions OR cession actions'
        )
    else:
        queries.append(
            f'"{name}" {ticker} insider transaction OR director dealing OR share purchase OR share sale'
        )
    if exchange == "NASDAQ":
        queries.append(f'{ticker} insider buying OR selling shares 2025 OR 2026')
        queries.append(f'{ticker} "form 4" OR "insider transaction" OR "director" shares filed')
    elif exchange == "SGX":
        # SGXNet publishes all insider disclosures under "Disclosure of Interest"
        queries.append(f'site:sgx.com "{name}" "disclosure of interest" OR "changes in interest" director')
        queries.append(f'"{name}" SGX director interest OR substantial shareholder')

    # Build relevance check terms
    name_words = [w.lower() for w in name.split() if len(w) >= 4]
    name_phrase = " ".join(name_words[:2]) if len(name_words) >= 2 else name.lower()

    # Free-refresh mode: skip the Serper query loop entirely — SEC EDGAR
    # and KLSE Screener (above) already covered the free-source insiders.
    if not _SERPER_ENABLED:
        return new_count
    for query in queries:
        logger.info("INSIDER search: %s", query)
        results = serper_web_search(query, config, caller="insiders", ticker=ticker)
        for item in results:
            url = item.get("link") or item.get("url", "")
            title = item.get("title", "")
            snippet = item.get("snippet", item.get("description", ""))
            if not url or not title:
                continue

            # Relevance check: the TITLE must mention our company
            # (not just the snippet — snippets often have incidental mentions)
            title_lower = title.lower()
            tk_lower = ticker.lower()
            has_name = name_phrase in title_lower
            has_ticker = re.search(r'\b' + re.escape(tk_lower) + r'\b', title_lower) is not None
            if not has_name and not has_ticker:
                continue

            stored = db.insert_insider(
                ticker=ticker, exchange=exchange, url=url,
                title=title, snippet=snippet,
                source=item.get("source", ""),
                published=item.get("date", ""))
            if stored:
                new_count += 1

    logger.info("  → %d new insider items for %s", new_count, ticker)
    return new_count


def _fetch_insiders_sec(ticker: str, exchange: str, cik: str,
                         db: Database) -> int:
    """
    Fetch Form 4/3/5 (insider) filings from SEC EDGAR for a NASDAQ stock.

    SEC EDGAR exposes a public atom feed per CIK with `owner=only` filtering
    to Form 3/4/5 only. No API key required, but the User-Agent header must
    identify you per SEC's fair-access policy.

    Returns the number of NEW filings stored.
    """
    if not cik:
        return 0
    cik_padded = cik.zfill(10)
    # owner=only restricts to insider/ownership filings (Forms 3/4/5,
    # Schedule 13D/G). No type filter so we capture all of them — the
    # parser below filters to the relevant types.
    url = (
        f"https://www.sec.gov/cgi-bin/browse-edgar?"
        f"action=getcompany&CIK={cik_padded}&type=&dateb=&owner=only&count=40&output=atom"
    )
    logger.info("INSIDER SEC EDGAR: %s (CIK %s)", ticker, cik_padded)

    headers = {
        "User-Agent": "Emerging Edge martin@emergingedge.example.com",
        "Accept": "application/atom+xml",
    }
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            xml_text = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        logger.warning("SEC EDGAR fetch failed for %s: %s", ticker, e)
        return 0

    # Parse atom feed entries. SEC entries look like:
    #   <entry>
    #     <category term="4" .../>
    #     <content type="text/xml">
    #       <accession-number>...</accession-number>
    #       <filing-date>2026-04-14</filing-date>
    #       <filing-type>4</filing-type>
    #       <form-name>Statement of changes in beneficial ownership of securities</form-name>
    #       ...
    #     </content>
    #     <link href="..." />
    #     <title>4  - Statement of changes...</title>
    #   </entry>
    # Accept individual insider forms (3/4/5), Form 144 (notice of proposed
    # sale of restricted securities by affiliates), plus substantial-shareholder
    # Schedule 13D/G filings (and amendments). Foreign private issuers like
    # TIGO/VEON typically don't file Form 4 (Section 16 doesn't apply) but
    # do file Form 3, 144, or Schedule 13s.
    _ACCEPTED_TYPES = {"3", "4", "5", "144"}
    _ACCEPTED_PREFIXES = ("SC 13D", "SC 13G", "SCHEDULE 13D", "SCHEDULE 13G")

    new_count = 0
    entries = re.findall(r"<entry>(.*?)</entry>", xml_text, re.DOTALL)
    for entry in entries:
        type_m = re.search(r"<filing-type>([^<]+)</filing-type>", entry)
        if not type_m:
            continue
        ftype = type_m.group(1).strip()
        if ftype not in _ACCEPTED_TYPES and not any(
            ftype.upper().startswith(p) for p in _ACCEPTED_PREFIXES
        ):
            continue

        date_m = re.search(r"<filing-date>([^<]+)</filing-date>", entry)
        href_m = re.search(r'<link[^>]+href="([^"]+)"', entry)
        title_m = re.search(r"<title>([^<]+)</title>", entry)
        form_m = re.search(r"<form-name>([^<]+)</form-name>", entry)
        acc_m = re.search(r"<accession-number>([^<]+)</accession-number>", entry)

        if not (date_m and href_m and acc_m):
            continue

        filing_date = date_m.group(1)
        link_url = href_m.group(1).replace("&amp;", "&")
        # Build a friendly title
        title = f"Form {ftype}"
        if form_m:
            title = f"Form {ftype} — {form_m.group(1).strip()}"
        elif title_m:
            title = title_m.group(1).strip()

        stored = db.insert_insider(
            ticker=ticker, exchange=exchange, url=link_url,
            title=title, snippet="",
            source="SEC EDGAR", published=filing_date)
        if stored:
            new_count += 1

    logger.info("  → %d new SEC EDGAR filings for %s", new_count, ticker)
    return new_count


def _fetch_insiders_finansinspektionen(stock: dict, db: Database) -> int:
    """
    Scrape the Swedish FSA (Finansinspektionen) insider register for
    Nordic stocks. The register covers all MAR-regulated transactions
    by PDMRs (persons discharging managerial responsibilities) across
    EU venues, including Stockholm/Oslo/Copenhagen/Helsinki.

    Page layout is a static HTML table — each row has 8 columns:
      Publication date | Issuer | PDMR name | Position | Closely assoc.
      | Nature | Instrument name | Instrument type
    """
    import ssl as _ssl
    ticker = stock["ticker"]
    exchange = stock["exchange"]
    name = stock.get("name", "")
    # Issuer names are looked up in the official company registry, so
    # we strip the common suffixes Yahoo/stockanalysis ships and hope
    # the root matches. "Investor AB (publ)" → "Investor AB".
    short = re.sub(r"\s*\(publ\)\s*$", "", name, flags=re.I).strip()
    if not short:
        return 0

    url = ("https://marknadssok.fi.se/publiceringsklient/en-GB/Search/Search"
           f"?SearchFunctionType=Insyn&Utgivare={urllib.parse.quote(short)}")
    logger.info("INSIDER Finansinspektionen: %s", short)

    ctx = _ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = _ssl.CERT_NONE
    try:
        req = urllib.request.Request(
            url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=10, context=ctx) as resp:
            html = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        logger.warning("Finansinspektionen fetch failed for %s: %s", short, e)
        return 0

    table_m = re.search(r"<table[\s\S]*?</table>", html)
    if not table_m:
        return 0
    rows = re.findall(r"<tr[^>]*>([\s\S]*?)</tr>", table_m.group(0))
    new_count = 0
    for row in rows:
        cells = re.findall(r"<t[dh][^>]*>([\s\S]*?)</t[dh]>", row)
        if len(cells) < 8:
            continue
        cleaned = [re.sub(r"\s+", " ",
                    re.sub(r"<[^>]+>", " ", c)
                    .replace("&#160;", " ")
                    .replace("&#39;", "'")
                    .replace("&amp;", "&")).strip()
                   for c in cells]
        pub_date, issuer, pdmr, position, assoc, nature, instr_name, instr_type = cleaned[:8]
        # Skip header row
        if pub_date.lower().startswith("publication"):
            continue
        # Filter out rows where the issuer match is too loose
        if short.lower().split()[0] not in issuer.lower():
            continue
        # Convert DD/MM/YYYY → YYYY-MM-DD
        try:
            d, m, y = pub_date.split("/")
            iso_date = f"{y}-{m.zfill(2)}-{d.zfill(2)}"
        except Exception:
            iso_date = pub_date
        title = f"{nature} · {pdmr} ({position})"
        snippet = f"{instr_name} ({instr_type}) — {issuer}"
        if assoc and assoc.lower() in ("yes", "x"):
            title += " · closely associated"
        # Each row gets a unique URL suffix so upsert on (url) doesn't
        # collapse multiple transactions sharing the same query URL.
        row_url = f"{url}#{iso_date}-{pdmr.replace(' ','_')}-{nature}"
        stored = db.insert_insider(
            ticker=ticker, exchange=exchange,
            url=row_url,
            title=title,
            snippet=snippet,
            source="Finansinspektionen",
            published=iso_date)
        if stored:
            new_count += 1
    logger.info("  → %d new FI insider rows for %s", new_count, ticker)
    return new_count


def _fetch_insiders_klse(ticker: str, exchange: str, code: str,
                          db: Database) -> int:
    """
    Scrape KLSE Screener announcements page for director interest changes.

    Page structure (repeating blocks):
        TITLE LINE (e.g. "Changes in Director's Interest ... - DIRECTOR NAME")
        COMPANY NAME
        DATE - TIME

    We extract entries where title contains "Director's Interest" or
    "Substantial Shareholder".
    """
    url = f"https://www.klsescreener.com/v2/announcements/stock/{code}"
    logger.info("INSIDER KLSE Screener: %s → %s", ticker, url)
    text = _fetch_page_text(url, timeout=15)
    if not text:
        return 0

    new_count = 0
    lines = text.split("\n")
    meaningful = [l.strip() for l in lines if l.strip() and len(l.strip()) > 5]

    _INSIDER_KW = [
        "director's interest", "director interest",
        "substantial shareholder", "changes in shareholding",
        "s-hldr", "person ceasing", "section 138", "section 139",
    ]

    for i, line in enumerate(meaningful):
        line_lower = line.lower()
        if not any(kw in line_lower for kw in _INSIDER_KW):
            continue

        title = line.strip()
        # Extract date from nearby lines (usually 2 lines after)
        pub_date = ""
        for j in range(1, 4):
            if i + j < len(meaningful):
                date_match = re.match(r"^(\d{4}-\d{2}-\d{2})", meaningful[i + j])
                if date_match:
                    pub_date = date_match.group(1)
                    break

        # Build a unique URL using the announcement title hash
        ann_url = f"{url}#ann-{abs(hash(title + pub_date)) % 10**8}"

        stored = db.insert_insider(
            ticker=ticker, exchange=exchange, url=ann_url,
            title=title, snippet="",
            source="KLSE Screener", published=pub_date)
        if stored:
            new_count += 1

    logger.info("  → %d KLSE Screener insider items for %s", new_count, ticker)
    return new_count


# ---------------------------------------------------------------------------
# Master runner: run all fetchers for all stocks
# ---------------------------------------------------------------------------

def run_all(config: dict, db: Database) -> dict:
    """
    Execute all fetchers for every active stock (config + user_stocks).
    Returns a summary dict: {ticker: {news: N, contracts: N, ...}}
    """
    summary = {}

    for stock in get_active_stocks(db, config):
        ticker = stock["ticker"]
        logger.info("=" * 60)
        logger.info("Processing %s (%s / %s)", stock["name"], ticker, stock["exchange"])
        logger.info("=" * 60)

        s = {"news": 0, "contracts": 0, "earnings": False, "forum": 0, "price": False, "insider": 0}

        # a) News
        try:
            s["news"] = fetch_news(stock, db, config)
        except Exception as e:
            logger.error("News fetch failed for %s: %s", ticker, e)

        # b) Contracts / Tenders
        try:
            s["contracts"] = fetch_contracts(stock, db, config)
        except Exception as e:
            logger.error("Contract fetch failed for %s: %s", ticker, e)

        # c) Earnings date
        try:
            s["earnings"] = fetch_earnings(stock, db, config)
        except Exception as e:
            logger.error("Earnings fetch failed for %s: %s", ticker, e)

        # d) Forum
        try:
            s["forum"] = fetch_forums(stock, db, config)
        except Exception as e:
            logger.error("Forum fetch failed for %s: %s", ticker, e)

        # e) Price
        try:
            s["price"] = fetch_prices(stock, db, config)
        except Exception as e:
            logger.error("Price fetch failed for %s: %s", ticker, e)

        # f) Insider transactions
        try:
            s["insider"] = fetch_insiders(stock, db, config)
        except Exception as e:
            logger.error("Insider fetch failed for %s: %s", ticker, e)

        summary[ticker] = s

    return summary
