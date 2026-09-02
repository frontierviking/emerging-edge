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

# Resolved once at import time: the curl-subprocess fallback (used to
# dodge Yahoo Finance's urllib TLS fingerprint) needs curl on PATH.
# macOS/Linux always have it at /usr/bin/curl; Windows 10 1803+ ships
# curl.exe too, but not at that path — shutil.which() finds whichever
# curl is actually on PATH on any OS. None means "not installed" — the
# curl tier is skipped and Yahoo just falls through to the next source.
import shutil as _shutil
_CURL_BIN = _shutil.which("curl")

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
    # Stocks the user removed via the chip ✕ are tracked in
    # hidden_stocks so config-seeded entries (which can't be deleted
    # without editing config.json) are still filterable from the UI.
    try:
        hidden = db.get_hidden_stocks() if db is not None else set()
    except Exception:
        hidden = set()
    for s in config.get("stocks", []) or []:
        key = (s.get("ticker", "").upper(), s.get("exchange", "").upper())
        if key in seen or key in hidden:
            continue
        seen.add(key)
        merged.append(s)
    try:
        user_stocks = db.get_user_stocks() if db is not None else []
    except Exception:
        user_stocks = []
    for s in user_stocks:
        key = (s.get("ticker", "").upper(), s.get("exchange", "").upper())
        if key in seen or key in hidden:
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

# Set True while a manual / scheduled price refresh is running so the
# dashboard's background self-heal (_kick_stale_refresh) backs off and
# doesn't contend for the shared SQLite write lock — that contention
# was wedging the manual refresh at 0/N.
_PRICE_REFRESH_ACTIVE = False


def set_price_refresh_active(active: bool) -> None:
    global _PRICE_REFRESH_ACTIVE
    _PRICE_REFRESH_ACTIVE = bool(active)


def price_refresh_active() -> bool:
    return _PRICE_REFRESH_ACTIVE


SERPER_BASE = "https://google.serper.dev"

# Module-level DB path for logging Serper calls. Set by run_all/cmd_serve
# via set_serper_db_path(). set_serper_db_path() is currently never
# called, so this default is what's actually used in practice — keep it
# cwd-relative (matching db.py's own default) rather than __file__-based.
# __file__-based paths resolve into PyInstaller's _internal/ folder when
# frozen, a different location than the real database sitting next to
# the .exe, which would silently split Serper call-logging into a second,
# never-read database file.
_SERPER_DB_PATH = "emerging_edge.db"


def set_serper_db_path(path: str):
    global _SERPER_DB_PATH
    _SERPER_DB_PATH = path


# Runtime controls for Serper:
#  - _SERPER_KEY_OVERRIDE: if set (by monitor.py after reading the
#    DB-stored user key), takes precedence over the SERPER_API_KEY env
#    var. This lets the user paste their key in the UI.
#  - _serper_is_enabled(): if False, _call_serper returns None without
#    making a network call — used for "free refresh" mode.
# Per-thread state. In multi-user mode the request handler thread and
# the background refresh thread each run as different users — a module
# global would race (e.g. user A's refresh-status poll teardown wipes
# the key user B's bg refresh is using). threading.local gives each
# thread its own slot. _serper_is_enabled() is also per-thread for the same
# reason — "free refresh" toggles it for the bg thread without
# interfering with concurrent full refreshes from other users.
import threading as _threading
_SERPER_TLS = _threading.local()
# Process-global default. Set once at startup in single-user mode (from
# the shared DB). In multi-user mode this stays empty — every request
# sets a thread-local override instead. Resolution order is:
#   1. thread-local override (multi-user request, bg refresh thread)
#   2. process default (single-user startup, env-var override)
#   3. SERPER_API_KEY env var (CLI / Docker fallback)
_SERPER_DEFAULT_KEY: str = ""
_SERPER_DEFAULT_ENABLED: bool = True
_SERPER_TLS_SENTINEL = object()


def set_serper_api_key(key: str, *, scope: str = "default"):
    """Set the Serper key.

    scope="thread"  — set the thread-local override (multi-user request
                      handler / bg refresh thread).
    scope="default" — set the process-global default (single-user
                      startup, /api/settings/serper-key in single-user).
    """
    cleaned = (key or "").strip()
    if scope == "thread":
        _SERPER_TLS.key_override = cleaned
    else:
        global _SERPER_DEFAULT_KEY
        _SERPER_DEFAULT_KEY = cleaned


def set_serper_enabled(enabled: bool, *, scope: str = "thread"):
    """Toggle Serper. Default scope is thread because the typical caller
    is the bg refresh thread (free vs full mode); the process default
    stays True except in pathological tests."""
    val = bool(enabled)
    if scope == "thread":
        _SERPER_TLS.enabled = val
    else:
        global _SERPER_DEFAULT_ENABLED
        _SERPER_DEFAULT_ENABLED = val


def get_serper_api_key() -> str:
    """Walk the three tiers: thread-local → process default → env var."""
    override = getattr(_SERPER_TLS, "key_override", _SERPER_TLS_SENTINEL)
    if override is not _SERPER_TLS_SENTINEL and override:
        return override
    if _SERPER_DEFAULT_KEY:
        return _SERPER_DEFAULT_KEY
    return os.environ.get("SERPER_API_KEY", "")


def _serper_is_enabled() -> bool:
    val = getattr(_SERPER_TLS, "enabled", _SERPER_TLS_SENTINEL)
    if val is _SERPER_TLS_SENTINEL:
        return _SERPER_DEFAULT_ENABLED
    return bool(val)




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
    if not _serper_is_enabled():
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


def _fetch_page_text(url: str, timeout: int = 15, raw: bool = False) -> str:
    """Fetch a URL and return stripped text content (or raw HTML if ``raw=True``).
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
            raw_bytes = resp.read()
        try:
            html = raw_bytes.decode("utf-8")
        except UnicodeDecodeError:
            html = raw_bytes.decode("latin-1", errors="replace")
        if raw:
            return html
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

        title = _clean_rss_html(title_m.group(1), max_len=300)
        link_url = link_m.group(1).strip()
        desc = _clean_rss_html(desc_m.group(1) if desc_m else "", max_len=500)
        if _title_is_disambiguation_false_positive(ticker, title, desc):
            logger.info("  skip false-positive for %s: %s", ticker, title[:60])
            continue
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
    "NSEK":    ("en", "KE"),   # Kenya — English is the dominant business press language
    "GSE":     ("en", "GH"),   # Ghana
    "BWSE":    ("en", "BW"),   # Botswana
    "LUSE":    ("en", "ZM"),   # Zambia
    "DSET":    ("en", "TZ"),   # Tanzania
    "DSEB":    ("en", "BD"),   # Bangladesh — English business press is common
    "PSX":     ("en", "PK"),   # Pakistan — English business press is common
    "CSEM":    ("fr", "MA"),   # Morocco — French is the dominant business language
    "ZSE":     ("hr", "HR"),   # Croatia
    "BELEX":   ("sr", "RS"),   # Serbia
    "BSSE":    ("sk", "SK"),   # Slovakia
    "PNGX":    ("en", "AU"),   # Papua New Guinea — English press, AU locale has most coverage
    "BVMT":    ("fr", "TN"),   # Tunisia — French business press
    "CSEL":    ("en", "LK"),   # Sri Lanka — English business press dominant
    "UX":      ("uk", "UA"),   # Ukraine — Ukrainian-language press
    "USE":     ("en", "UG"),   # Uganda — English business press dominant
    "RSE":     ("en", "RW"),   # Rwanda — English business press dominant
    "SEM":     ("en", "MU"),   # Mauritius — English press alongside French
    "ISX":     ("ar", "IQ"),   # Iraq — Arabic-language financial press
    "ESX":     ("en", "ET"),   # Ethiopia — English business press
    "B3":      ("pt", "BR"),   # Brazil — Portuguese financial press
    "BCBA":    ("es", "AR"),   # Argentina — Spanish financial press
    "BMV":     ("es", "MX"),   # Mexico — Spanish financial press
    "BSE":     ("en", "IN"),   # India (Mumbai) — English business press
    "KSE":     ("ru", "KG"),   # Kyrgyzstan — Russian-language press dominates
    "UZSE":    ("ru", "UZ"),   # Uzbekistan — Russian-language press dominates
    "SWX":     ("de", "CH"),   # Switzerland — German financial press
    "EURONEXT": ("en", "NL"), # Euronext — English, NL as base
    "KRX":     ("ko", "KR"),   # South Korea
    "TWSE":    ("zh-TW", "TW"),# Taiwan — Traditional Chinese
    "EGX":     ("en", "EG"),   # Egypt (Cairo) — English business press
    "BHB":     ("en", "BH"),   # Bahrain
    "ZWZSE":   ("en", "ZW"),   # Zimbabwe
    "IDX":     ("en", "ID"),   # Indonesia — English business press
    "SET":     ("en", "TH"),   # Thailand — English business press
    "PSE":     ("en", "PH"),   # Philippines — English dominant
    "HOSE":    ("en", "VN"),   # Vietnam — English business press
    "TASE":    ("en", "IL"),   # Israel — English business press
    "TADAWUL": ("en", "SA"),   # Saudi Arabia — English press
    "DFM":     ("en", "AE"),   # UAE Dubai
    "ADX":     ("en", "AE"),   # UAE Abu Dhabi
    "QSE":     ("en", "QA"),   # Qatar
    "BIST":    ("tr", "TR"),   # Turkey — Turkish press
    "WSE":     ("pl", "PL"),   # Poland — Polish press
    "PSE_CZ":  ("cs", "CZ"),   # Czech Republic
    "BET":     ("hu", "HU"),   # Hungary — Hungarian press
    "ATHEX":   ("en", "GR"),   # Greece — English press
    "BVB":     ("ro", "RO"),   # Romania — Romanian press
    "NZX":     ("en", "NZ"),   # New Zealand
    "SSE":     ("zh-CN", "CN"),# China Shanghai
    "SZSE":    ("zh-CN", "CN"),# China Shenzhen
}


# Per-ticker disambiguation denylist — drop news items whose title or
# description contains any of these substrings (case-insensitive, with
# Unicode quote/apostrophe normalization). Use for tickers whose name
# collides with a celebrity / sports figure / unrelated entity that
# pollutes the company news feed.
_NEWS_TITLE_EXCLUDE = {
    "VEON": [
        # Le'Veon Bell (NFL running back) — most common false positive
        "le'veon bell", "le veon bell", "leveon bell", "leveon",
        # Veon Moss (2026 NFL Draft prospect) — newer collision
        "veon moss", "veon dior",
        # Broader NFL / Jets / Steelers / coaches context
        "nfl", "running back", "steelers", "new york jets", " jets ",
        "adam gase", "pittsburgh", "football player",
        "chiefs", "ravens", "buccaneers", "mike tomlin",
        # NFL Draft & college-football staples that piggyback on player names
        "nfl draft", "draft prospect", "college football",
        "heidenreich", "jaydn ott",
    ],
    # Plenitude Berhad (KLSE property) — false positives from Eni Plenitude,
    # the Italian energy brand, and Italian utility Acea. Applied under both
    # the KLSE numeric code and the short name.
    "5075": [
        "eni gas", "eni plenitude", "eni spa", "gas & power",
        "prp channel", "acea", "italian energy", "enel",
        "energia", "rome", "milan",
    ],
    "PLENITU": [
        "eni gas", "eni plenitude", "eni spa", "gas & power",
        "prp channel", "acea", "italian energy", "enel",
        "energia", "rome", "milan",
    ],
    # Pierce Group AB (publ) (OMX:PIERCE) — Swedish e-commerce. Common
    # collisions: Pierce County (Washington), Franklin Pierce, Brock
    # Pierce (crypto), Pierce Brosnan (actor), Pierce Manufacturing
    # (fire trucks), Pierce College, plus any number of people named
    # Pierce. Match the disambiguating context, not the surname.
    "PIERCE": [
        "pierce county", "tacoma", "puget sound",   # WA / county news
        "franklin pierce",                           # 14th US president
        "brock pierce",                              # crypto billionaire
        "pierce brosnan",                            # actor
        "pierce manufacturing", "pierce fire",       # fire trucks
        "pierce college",
        "tim pierce", "drew pierce",                 # surfaced racing/sports drivers
        "stryker",                                   # different exec named Pierce
        "planet fitness",                            # locker-break-in stories
        "formula 1000", "racing driver",
    ],
    # Kumpulan Fima Berhad (KLSE:6491 / KFIMA) — Malaysian conglomerate.
    # Google News surfaces Fibromat (KLSE:FIBRO) coverage on Kumpulan
    # Fima's feed (both are KLSE "M Berhad" companies). Filter out the
    # unrelated FIBRO ticker by name and code.
    "6491": [
        "fibromat", "klse:fibro", "(fibro)", " fibro ",
    ],
    "KFIMA": [
        "fibromat", "klse:fibro", "(fibro)", " fibro ",
    ],
    # Ecobank Transnational Incorporated (BRVM:ETIT) — the 4-letter
    # ticker happens to be a substring of common French words (petit,
    # petite, compétition, répétition, appétit) and the company name
    # in the watchlist was originally just "ETIT", so Google News' "ETIT"
    # query returned tons of unrelated French local-news coverage from
    # Sud Ouest etc. The real fix is the longer name on the user_stocks
    # row; this denylist mops up any residual matches that slip through.
    "ETIT": [
        " petit", " petite", " petits", " petites",   # leading-space
        "appétit", "compétition", "compétences",      # French nouns
        "répétition", "compétitif", "compétiteurs",
        "rugby", "twirling", "stade toulousain",
        "boulangerie", "épicerie",                    # local-news boilerplate
        "tournon-sur-rhône", "bordelais",
    ],
    # Add more disambiguations as they surface. Example:
    # "TIGER":  ["tiger woods", "pga"],
}


# Unicode quote / apostrophe / dash variants that web feeds sprinkle
# through titles. Normalize these to ASCII before substring matching
# so a denylist entry "le'veon bell" matches "Le'Veon Bell" too.
_UNICODE_QUOTE_NORMALIZE = str.maketrans({
    "\u2018": "'", "\u2019": "'", "\u201A": "'", "\u201B": "'",
    "\u02BC": "'", "\u02B9": "'", "\u0060": "'", "\u00B4": "'",
    "\u201C": '"', "\u201D": '"', "\u201E": '"', "\u201F": '"',
    "\u2013": "-", "\u2014": "-", "\u2212": "-",
    "\u00A0": " ",
})


def _normalize_for_match(s: str) -> str:
    return (s or "").translate(_UNICODE_QUOTE_NORMALIZE).lower()


def _title_is_disambiguation_false_positive(ticker: str, title: str,
                                            desc: str = "") -> bool:
    """Return True if the title or description matches a known false-positive
    for this ticker. Unicode quotes/apostrophes are normalized so that feeds
    using curly quotes (e.g. Google News) still hit the denylist."""
    needles = _NEWS_TITLE_EXCLUDE.get((ticker or "").upper())
    if not needles:
        return False
    haystack = _normalize_for_match(title) + " \n " + _normalize_for_match(desc)
    return any(n in haystack for n in needles)


def _clean_rss_html(s: str, max_len: int = 500) -> str:
    """
    Scrub an RSS text field (title or description) to plain text.

    Google News descriptions arrive as *double-entity-encoded* HTML
    inside a non-CDATA <description> — e.g. the raw XML contains
    `&lt;a href=...&gt;...&lt;/a&gt;&amp;nbsp;&amp;nbsp;`. A single
    html.unescape decodes `&lt;` to `<` but leaves `&amp;nbsp;` as
    `&nbsp;`, and subsequent tag stripping leaves the `&nbsp;`
    visible. So we unescape repeatedly until stable, THEN strip
    tags, THEN normalize whitespace.
    """
    import html as _html_mod
    if not s:
        return ""
    prev = None
    cur = str(s)
    # Unescape entities repeatedly — Google News RSS is often double-
    # encoded so one pass leaves visible `&amp;nbsp;` / `&lt;a`.
    for _ in range(4):  # bounded loop
        prev = cur
        cur = _html_mod.unescape(cur)
        if cur == prev:
            break
    # Strip complete HTML tags that surfaced after unescaping.
    cur = re.sub(r"<[^>]*>", "", cur)
    # Strip any dangling unterminated tag — e.g. a snippet that was
    # truncated mid-tag like `<a href="https://..."...` with no
    # closing `>`. Remove everything from that stray `<` to end.
    cur = re.sub(r"<[^<]*$", "", cur)
    # Normalize non-breaking spaces and zero-width chars to regular space.
    cur = re.sub(r"[\xa0\u200b]+", " ", cur)
    # Collapse runs of whitespace.
    cur = re.sub(r"\s+", " ", cur).strip()
    return cur[:max_len]


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
        title = _clean_rss_html(title_m.group(1), max_len=300)
        link_url = link_m.group(1).strip()
        desc = _clean_rss_html(desc_m.group(1) if desc_m else "", max_len=500)
        # Skip known disambiguation false-positives (e.g. "Le'Veon Bell"
        # for ticker VEON). Check title and description — Google News
        # often has a clean title but reveals the NFL context in the body.
        if _title_is_disambiguation_false_positive(ticker, title, desc):
            logger.info("  skip false-positive for %s: %s", ticker, title[:60])
            continue
        pub = date_m.group(1).strip() if date_m else ""
        source = _clean_rss_html(source_m.group(1), max_len=60) if source_m else "Google News"

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


# Dedicated RSS feeds for exchanges where Google News has poor coverage.
# Key is exchange code → list of (feed_url, source_label). These are
# exchange-level feeds (not per-ticker), so we only fetch once per
# exchange per refresh cycle. The module-level set tracks which
# exchanges have already been fetched this session.
_DEDICATED_RSS_FEEDS = {
    "ISX":  [("https://www.iraq-businessnews.com/feed/", "Iraq Business News")],
    "NGX":  [("https://nairametrics.com/feed/", "Nairametrics")],
    "GSE":  [("https://ghanabusinessnews.com/feed/", "Ghana Business News")],
    # Pan-African RSS covers NGX, JSE, NSEK, GSE, BWSE, LUSE, DSET, USE, RSE, SEM
    "JSE":  [("https://allafrica.com/tools/headlines/rdf/business/headlines.rdf", "AllAfrica Business")],
    "NSEK": [("https://allafrica.com/tools/headlines/rdf/business/headlines.rdf", "AllAfrica Business")],
    "BWSE": [("https://allafrica.com/tools/headlines/rdf/business/headlines.rdf", "AllAfrica Business")],
    "LUSE": [("https://allafrica.com/tools/headlines/rdf/business/headlines.rdf", "AllAfrica Business")],
    "DSET": [("https://allafrica.com/tools/headlines/rdf/business/headlines.rdf", "AllAfrica Business")],
    "USE":  [("https://allafrica.com/tools/headlines/rdf/business/headlines.rdf", "AllAfrica Business")],
    "RSE":  [("https://allafrica.com/tools/headlines/rdf/business/headlines.rdf", "AllAfrica Business")],
    "SEM":  [("https://allafrica.com/tools/headlines/rdf/business/headlines.rdf", "AllAfrica Business")],
}
_DEDICATED_RSS_DONE: set[str] = set()


def _fetch_news_dedicated_rss(stock: dict, db: Database) -> int:
    """Fetch from exchange-level RSS feeds. Runs once per exchange per
    session. Each item is matched against every watchlist stock on that
    exchange — items with no ticker or company-name match are dropped
    instead of being stored under an arbitrary fallback ticker (which
    used to produce false positives like a generic Africa article filed
    under PMV)."""
    exchange = stock["exchange"]
    if exchange in _DEDICATED_RSS_DONE:
        return 0
    feeds = _DEDICATED_RSS_FEEDS.get(exchange)
    if not feeds:
        return 0
    _DEDICATED_RSS_DONE.add(exchange)

    # Pre-compute matchable needles for every watchlist stock on this
    # exchange so we can fan out feed items to the right ticker. Skip
    # very short / generic tickers (≤2 chars) since they'd false-positive.
    try:
        import db as _dbmod  # type: ignore  # avoid name shadow
    except Exception:
        _dbmod = None  # not strictly needed
    # We don't have a clean db→config link here; build needles from the
    # watchlist passed via the stock-by-stock outer loop. Simpler: reuse
    # _build_forum_needles for each stock on this exchange.
    same_ex_stocks: list[dict] = []
    try:
        # Walk DB user_stocks + config defaults via helper
        from fetchers import get_active_stocks as _gas  # self-import alias
        _all = _gas(db, {})
    except Exception:
        _all = []
    for s in _all:
        if (s.get("exchange") or "").upper() == exchange.upper():
            same_ex_stocks.append(s)

    total = 0
    for feed_url, source_label in feeds:
        logger.info("NEWS dedicated RSS: %s → %s (%d watchlist stocks on %s)",
                    exchange, source_label, len(same_ex_stocks), exchange)
        try:
            import ssl as _ssl
            ctx = _ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = _ssl.CERT_NONE
            req = urllib.request.Request(
                feed_url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=10, context=ctx) as resp:
                xml_text = resp.read().decode("utf-8", errors="replace")
        except Exception as e:
            logger.warning("Dedicated RSS %s failed: %s", source_label, e)
            continue

        items = re.findall(r"<item>(.*?)</item>", xml_text, re.DOTALL)
        for item_xml in items[:60]:
            title_m = re.search(
                r"<title>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</title>",
                item_xml, re.DOTALL)
            link_m = re.search(r"<link>(.*?)</link>", item_xml, re.DOTALL)
            desc_m = re.search(
                r"<description>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</description>",
                item_xml, re.DOTALL)
            date_m = re.search(r"<pubDate>(.*?)</pubDate>", item_xml, re.DOTALL)

            if not (title_m and link_m):
                continue
            title = _clean_rss_html(title_m.group(1), max_len=300)
            link_url = link_m.group(1).strip()
            desc = _clean_rss_html(desc_m.group(1) if desc_m else "", max_len=500)
            pub = date_m.group(1).strip() if date_m else ""
            haystack = (title + "\n" + desc).lower()

            # Try every watchlist stock on this exchange — store under the
            # FIRST one whose ticker or name appears with a clean word
            # boundary in the title/description. Drop the item if no
            # watchlist match (avoids dumping general Africa news under
            # arbitrary tickers).
            matched_stock = None
            for s in same_ex_stocks:
                if _title_is_disambiguation_false_positive(
                        s.get("ticker", ""), title, desc):
                    continue
                needles = _build_forum_needles(s, db)
                if needles and _needle_matches(haystack, needles):
                    matched_stock = s
                    break
            if not matched_stock:
                continue

            stored = db.insert_news(
                ticker=matched_stock["ticker"], exchange=exchange,
                url=link_url, title=title, snippet=desc,
                source=source_label, published=pub,
                search_type="news", lang="en")
            if stored:
                total += 1

        logger.info("  → %d new items from %s", total, source_label)
    return total


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

    # ── 0) FREE: Dedicated exchange-level RSS feeds ──
    new_count += _fetch_news_dedicated_rss(stock, db)

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
    use_serper = (_serper_is_enabled()
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
                title=_clean_rss_html(item.get("title", ""), max_len=300),
                snippet=_clean_rss_html(item.get("snippet", item.get("description", "")), max_len=500),
                source=item.get("source", ""),
                published=item.get("date", ""),
                search_type="news", lang=lang)
            if stored:
                new_count += 1

    # ── 3) French-language secondary search (Serper) ──
    if lang == "fr" and _serper_is_enabled():
        query_fr = f"{name} résultats"
        logger.info("NEWS search (FR): %s", query_fr)
        results_fr = serper_news_search(query_fr, config, caller="news", ticker=ticker)
        for item in results_fr:
            url = item.get("link") or item.get("url", "")
            if not url:
                continue
            stored = db.insert_news(
                ticker=ticker, exchange=exchange, url=url,
                title=_clean_rss_html(item.get("title", ""), max_len=300),
                snippet=_clean_rss_html(item.get("snippet", item.get("description", "")), max_len=500),
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

    if not _serper_is_enabled():
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
    # "28 Feb 2026", "28 February 2026", "31 Mar, 2026" (KLSE Screener uses comma)
    re.compile(r"(\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*,?\s+\d{4})", re.I),
    # "2026-02-28"
    re.compile(r"(20\d{2}-\d{2}-\d{2})"),
    # "02/28/2026" or "28/02/2026"
    re.compile(r"(\d{1,2}/\d{1,2}/20\d{2})"),
]

_EARNINGS_KEYWORDS = [
    "financial result", "quarterly report", "quarter report",
    "annual report", "earnings", "résultats", "rapport financier",
    "announcement date", "report date", "next report",
    "announced", "financial year", "q date",
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
    sa_ok = _fetch_earnings_stockanalysis(stock, db)
    if sa_ok and exchange in ("NASDAQ", "NYSE"):
        # Also run the NASDAQ calendar BACKWARD scan so the Past Reports
        # tab has a year of quarterly history.
        _fetch_earnings_nasdaq_calendar(stock, db, config, past_only=True)
    # KLSE-specific: stockanalysis only gives the next date, and the
    # klsescreener template only gives the regulatory deadline. Pull the
    # real past quarterly announcement dates so Past Reports actually
    # shows Malaysian history.
    if exchange == "KLSE":
        _fetch_earnings_klsescreener_past(stock, db)
    # SA quarterly: seeds past-earnings rows for all SA-covered
    # exchanges (B3/JPX/SET/BIT/ADX/BCBA/EUR_FR/JSE/LIT/TASE/OTC/KASE
    # and supplements SGX/HKSE/IDX/OMX/etc.) AND projects the next
    # upcoming quarter when SA's "Next earnings date" field is empty
    # — fills the upcoming gap. Run for KLSE too so its 14/14 stocks
    # always have a projected next quarter even when klsescreener's
    # template scraper hasn't extracted a deadline.
    if sa_ok or exchange in _SA_SLUG:
        _fetch_earnings_stockanalysis_past(stock, db)

    # ── 2) Exchange-specific template (klsescreener, ngx, brvm, uzse, etc.)
    # Always run — may find additional dates or fiscal-period detail that
    # stockanalysis.com doesn't provide.
    url_templates = config.get("earnings_urls", {})
    template = url_templates.get(source_key, "")
    template_ok = False
    if not template:
        if not sa_ok and exchange in ("NASDAQ", "NYSE"):
            return _fetch_earnings_nasdaq_calendar(stock, db, config)
        if not sa_ok:
            logger.info("No earnings URL template for %s (%s)", ticker, source_key)

    if template:
        url = template.format(ticker=ticker, code=code, name=urllib.parse.quote(stock["name"]))
        logger.info("EARNINGS fetch: %s → %s", ticker, url)

        text = _fetch_page_text(url)
        if text:
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
                template_ok = True

    if sa_ok or template_ok:
        return True

    # Fallback: also try a Serper search for earnings date (skip in free mode)
    if not _serper_is_enabled():
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
    # Strip commas so "31 Mar, 2026" becomes "31 Mar 2026"
    cleaned = s.strip().replace(",", "")
    for fmt in ("%d %B %Y", "%d %b %Y", "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(cleaned, fmt)
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
    "JPX":      "tyo",   # alias — most of our records use JPX
    "NSE":      "nse",
    "EURONEXT": "epa",
    "BIT":      "bit",   # Borsa Italiana — was "etr" (XETRA), wrong
    "OMX":      "sto",  # Stockholm (Nordic pair format uses dot: INVE.B)
    "OSE":      "osl",  # Oslo (rough guess — test before relying)
    "CSE":      "cph",  # Copenhagen
    "HEL":      "hel",  # Helsinki
    "KASE":     "kase", # Kazakhstan
    "DSEB":     "dse",  # Dhaka SE Bangladesh (stockanalysis uses 'dse' slug)
    "PSX":      "psx",  # Pakistan Stock Exchange
    "ZSE":      "zse",  # Zagreb (Croatia)
    "BELEX":    "belex",  # Belgrade (Serbia)
    "BVMT":     "bvmt",  # Bourse de Tunis (Tunisia)
    "IDX":      "idx",  # Indonesia Stock Exchange (Jakarta)
    "SET":      "bkk",  # Stock Exchange of Thailand (Bangkok slug "bkk")
    "PSE":      "pse",  # Philippine Stock Exchange (Manila)
    "ATHEX":    "ath",  # Athens Stock Exchange (Greece)
    "WSE":      "wse",  # Warsaw Stock Exchange (Poland)
    # April 2026 additions
    "MSM":      "msm", "ASEJ": "ase", "BVL": "bvl", "ICE": "ice",
    "LJSE":     "ljse", "MSE_MT": "mse", "NMSE": "nmse", "BUL": "bul",
    "QSE":      "qse", "KWSE": "kwse", "ADX": "adx", "DFM": "dfm",
    "TASE":     "tlv",   # Tel Aviv (Israel)
    "B3":       "bvmf",  # B3 (Brazil)
    "LIT":      "vse",   # Vilnius (Lithuania, Nasdaq Baltic)
    "RIS":      "rse",   # Riga (Latvia, Nasdaq Baltic)
    "TAL":      "tse",   # Tallinn (Estonia, Nasdaq Baltic)
    "BCBA":     "bcba",  # Buenos Aires (Argentina)
    "BMV":      "bmv",   # Mexico
    "BVS":      "bvs",   # Santiago (Chile)
    "BVC":      "bvc",   # Bolsa de Valores de Colombia
    "CSEC":     "cys",   # Cyprus Stock Exchange
    "EGX":      "egx",   # Egypt
    "KRX":      "kosdaq",# Korea — `kosdaq` is the more common slug; SA
                         # serves both KOSDAQ and KOSPI under it for many
                         # tickers, and the family below covers the rest.
    "OTC":      "otc",   # OTC Markets / Pink Sheets (e.g. WSTL)
    "PNK":      "otc",   # OTC Pink alias
    "OTCMKTS":  "otc",
    # Euronext country sub-markets (this app stores them as EUR_xx).
    # stockanalysis uses a per-city slug; map each so SA is tried as a
    # fallback when Yahoo (.PA/.AS/.BR/.LS) is rate-limited.
    "EUR_FR":   "epa",   # Euronext Paris (e.g. ALLOG — Logic Instrument)
    "EUR_NL":   "ams",   # Euronext Amsterdam
    "EUR_BE":   "ebr",   # Euronext Brussels
    "EUR_PT":   "eli",   # Euronext Lisbon
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
    # Walk the slug family so e.g. Korean tickers that live under
    # `kosdaq` for some and `krx`/`kospi` for others both resolve.
    slug_variants = (_SA_SLUG_FAMILY.get(slug, [slug])
                     if slug else [None])
    ctx = _ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = _ssl.CERT_NONE
    html = ""
    url = ""
    for sv in slug_variants:
        candidate_url = (f"https://stockanalysis.com/stocks/{ticker}/"
                         if sv is None else
                         f"https://stockanalysis.com/quote/{sv}/{ticker}/")
        try:
            req = urllib.request.Request(candidate_url, headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
                "Accept-Encoding": "identity",
                "Accept": "text/html",
            })
            with urllib.request.urlopen(req, timeout=8, context=ctx) as resp:
                body = resp.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as e:
            if e.code == 404:
                continue
            logger.warning("stockanalysis.com HTTP %d for %s (%s)",
                           e.code, ticker, sv)
            continue
        except Exception as e:
            logger.warning("stockanalysis.com fetch failed for %s/%s: %s",
                           ticker, sv, e)
            continue
        if "Earnings Date" in body:
            html = body
            url = candidate_url
            break
    if not html:
        return False

    # stockanalysis.com has used a few DOM layouts over the years:
    #  · old:  <td>Earnings Date</td><td>Apr 28, 2026</td>
    #  · new:  <span>Earnings Date</span> Apr 28, 2026
    #  · idx layout:  >Earnings Date<...> Apr 29, 2026 <
    m = (
        re.search(r'Earnings Date</td><td[^>]*>([^<]{3,60})', html)
        or re.search(r'Earnings Date[</a-z>"\'\s]{0,40}([A-Z][a-z]{2}\s+\d{1,2},\s*20\d{2})', html)
        or re.search(r'"Earnings Date"[^"]*?value:\s*"([^"]+)"', html)
    )
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


# ---------------------------------------------------------------------------
# klsescreener PAST quarterly announcements (KLSE only)
# ---------------------------------------------------------------------------
# Stock-Analysis only stores the next upcoming date and the per-stock
# klsescreener fetcher returns a regulatory *deadline* (end of next
# quarter), not the actual release date. So past_reports never sees
# Malaysian Q1/Q2/… announcements. klsescreener's stock view page does
# have a structured "Quarter Reports" table with an `Announced` column
# — the real date the company released the quarter's results. We
# scrape it once and upsert each past row into earnings_dates so the
# Past Reports tab actually reflects KLSE history.
def _fetch_earnings_klsescreener_past(stock: dict, db) -> int:
    if (stock.get("exchange") or "").upper() != "KLSE":
        return 0
    code = (stock.get("code") or stock.get("ticker") or "").strip()
    if not code:
        return 0
    url = f"https://www.klsescreener.com/v2/stocks/view/{code}"
    import ssl as _ssl
    ctx = _ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = _ssl.CERT_NONE
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 Chrome/126 Safari/537"})
        with urllib.request.urlopen(req, timeout=12, context=ctx) as r:
            html = r.read().decode("utf-8", errors="replace")
    except Exception as e:
        logger.info("klsescreener past earnings fetch failed for %s: %s",
                    code, e)
        return 0

    tables = re.findall(r"<table[^>]*>(.*?)</table>", html, re.S)
    inserted = 0
    announcements: list[tuple[str, str]] = []  # (announced_iso, q_num)
    for t in tables:
        # Quarter Reports table has both 'Announced' header and a 'Q Date'
        # column — strict marker so we don't latch onto the wrong table.
        if "Announced" not in t or "Q Date" not in t:
            continue
        # Walk raw row cells so we can also extract the per-quarter
        # "Financial Report" link from the rightmost column (column 12),
        # which lives at /v2/stock/financial-report/{code}/{q_date}.
        # Using that as source_url makes the alert/Past-Reports row
        # click straight through to the actual quarterly report instead
        # of the generic stock page.
        rows = re.findall(r"<tr[^>]*>(.*?)</tr>", t, re.S)
        for row in rows:
            raw_cells = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row, re.S)
            cells = [re.sub(r"<[^>]+>", "", x).strip() for x in raw_cells]
            if len(cells) < 10:
                continue  # section divider or header row
            ann = cells[8]
            if not re.match(r"^\d{4}-\d{2}-\d{2}$", ann):
                continue
            q = cells[5] if len(cells) > 5 else ""
            q_date = cells[6] if len(cells) > 6 else ""
            period = (f"Q{q} report"
                      if q.isdigit() and 1 <= int(q) <= 4
                      else "quarter report")
            # Prefer the per-row report URL when present; fall back to
            # the stock view page if the cell doesn't expose one.
            row_url = url
            if len(raw_cells) >= 13:
                href_m = re.search(
                    r'href="(/v2/stock/financial-report/[^"]+)"',
                    raw_cells[12])
                if href_m:
                    row_url = "https://www.klsescreener.com" + href_m.group(1)
                elif re.match(r"^\d{4}-\d{2}-\d{2}$", q_date):
                    # Templated URL works even without an <a> in the
                    # cell — klsescreener uses /code/{q_date} for every
                    # past quarter.
                    row_url = (f"https://www.klsescreener.com/v2/stock/"
                               f"financial-report/{code}/{q_date}")
            ok = db.upsert_earnings(
                ticker=stock["ticker"], exchange="KLSE",
                report_date=ann, fiscal_period=period, source_url=row_url)
            if ok:
                inserted += 1
            announcements.append((ann, q))
        break  # only first matching table

    # Project next quarter: most KLSE companies report on a ~91-day
    # cadence. Take the latest announcement, advance by 91 days, and
    # upsert as a projected upcoming row if it's in the future. Lets
    # all 14 of the user's KLSE stocks have a next-report row even
    # when SA doesn't index the numeric code.
    projected = 0
    if announcements:
        from datetime import timedelta as _td
        announcements.sort()
        last_ann_iso, last_q = announcements[-1]
        try:
            last_ann = datetime.strptime(last_ann_iso, "%Y-%m-%d").date()
        except ValueError:
            last_ann = None
        if last_ann is not None:
            proj = last_ann + _td(days=91)
            if proj >= datetime.utcnow().date():
                # Project the NEXT quarter number (1→2→3→4→1).
                try:
                    qn = int(last_q)
                    nxt_q = qn % 4 + 1
                except (TypeError, ValueError):
                    nxt_q = None
                label = (f"Q{nxt_q} report (proj)"
                         if nxt_q else "quarter report (proj)")
                ok = db.upsert_earnings(
                    ticker=stock["ticker"], exchange="KLSE",
                    report_date=proj.strftime("%Y-%m-%d"),
                    fiscal_period=label, source_url=url)
                if ok:
                    projected = 1

    logger.info("klsescreener past quarterlies: %s upserted past=%d proj=%d",
                stock.get("ticker", code), inserted, projected)
    return inserted + projected


# ---------------------------------------------------------------------------
# stockanalysis PAST quarterly endings (generic, all SA-covered exchanges)
# ---------------------------------------------------------------------------
# Many exchanges (B3, JPX, SET, BIT, ADX, BCBA, EUR_FR, JSE, LIT, TASE,
# OTC, …) have *zero* past-earnings rows because the live fetcher only
# stores the next future date. SA's /financials/?p=quarterly page lists
# period-ending dates for every past quarter as <th id="YYYY-MM-DD">.
# We extract those, add a +60-day announcement-date estimate (standard
# reporting lag across most markets) and upsert into earnings_dates so
# the Past Reports tab actually has content for these markets.
# Labelled as "Qn YYYY (est)" so the user knows the date is approximate.
def _fetch_earnings_stockanalysis_past(stock: dict, db) -> int:
    import ssl as _ssl
    from datetime import timedelta as _td
    exch = (stock.get("exchange") or "").upper()
    raw_ticker = (stock.get("ticker") or "").upper()
    slug = _SA_SLUG.get(exch)
    if slug is None and exch not in ("NASDAQ", "NYSE", "AMEX"):
        return 0  # SA doesn't cover this exchange

    tk = _sa_ticker(exch, raw_ticker)
    # Walk the slug family so e.g. Korean tickers that live under
    # `kosdaq` for some and `krx` for others both resolve. The first
    # variant returning a quarterly page wins.
    slug_variants = (_SA_SLUG_FAMILY.get(slug, [slug])
                     if slug else [None])
    ctx = _ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = _ssl.CERT_NONE
    html = ""
    url = ""
    for sv in slug_variants:
        base = (f"https://stockanalysis.com/stocks/{tk}/"
                if sv is None else
                f"https://stockanalysis.com/quote/{sv}/{tk}/")
        candidate_url = base + "financials/?p=quarterly"
        try:
            req = urllib.request.Request(candidate_url, headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 Chrome/126 Safari/537"})
            with urllib.request.urlopen(req, timeout=12, context=ctx) as r:
                body = r.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as e:
            if e.code == 404:
                continue   # try the next slug in the family
            logger.info("SA past quarterly HTTP %d for %s (%s)",
                        e.code, raw_ticker, sv)
            continue
        except Exception as e:
            logger.debug("SA past quarterly fetch failed for %s/%s: %s",
                         raw_ticker, sv, e)
            continue
        # Quick sanity: page must contain quarter-end markers.
        if '<th id="' in body and "Period Ending" in body:
            html = body
            url = candidate_url
            break
    if not html:
        return 0

    dates = sorted(set(re.findall(r'<th id="(\d{4}-\d{2}-\d{2})"', html)))
    if not dates:
        return 0

    today = datetime.utcnow().date()
    inserted = 0
    # Parse valid period-end dates, keep only those plausibly in scope.
    period_ends = []
    for d in dates:
        try:
            pe = datetime.strptime(d, "%Y-%m-%d").date()
        except ValueError:
            continue
        if pe > today or (today - pe).days > 365 * 10:
            continue
        period_ends.append(pe)
    period_ends.sort()
    for pe in period_ends:
        # Announcement-date estimate: most markets file within 60 days
        # of period end (US 40-45, B3 45, JPX 45, KLSE 60, SGX 60,
        # HKSE 90, IDX 90 — 60 is a reasonable median). Skip if the
        # estimate is still in the future (handled below as upcoming
        # projection instead).
        est_ann = pe + _td(days=60)
        if est_ann >= today:
            continue
        q = ((pe.month - 1) // 3) + 1
        label = f"Q{q} {pe.year} (est)"
        ok = db.upsert_earnings(
            ticker=raw_ticker, exchange=stock["exchange"],
            report_date=est_ann.strftime("%Y-%m-%d"),
            fiscal_period=label, source_url=url)
        if ok:
            inserted += 1

    # Upcoming-projection: many exchanges (B3, JPX, OMX, ADX, BCBA, …)
    # have *zero* upcoming rows because the SA "Next earnings date"
    # field is empty between quarters. Project the next expected
    # announcement from the most recent period_end + one quarter and a
    # 60-day reporting lag. Inserted as "Q? YYYY (proj)" so the user
    # can distinguish projected from confirmed dates.
    upcoming_projected = 0
    if period_ends:
        # Step forward in 3-month increments from the latest period_end
        # until the projected announcement is in the future.
        latest = period_ends[-1]
        nxt = latest
        for _step in range(8):  # safety bound
            # advance one quarter
            month = nxt.month + 3
            year = nxt.year + (month - 1) // 12
            month = ((month - 1) % 12) + 1
            # last day of new month — pick conservative end-of-quarter
            import calendar as _cal
            day = _cal.monthrange(year, month)[1]
            nxt = nxt.__class__(year, month, day)
            proj_ann = nxt + _td(days=60)
            if proj_ann >= today:
                q = ((nxt.month - 1) // 3) + 1
                ok = db.upsert_earnings(
                    ticker=raw_ticker, exchange=stock["exchange"],
                    report_date=proj_ann.strftime("%Y-%m-%d"),
                    fiscal_period=f"Q{q} {nxt.year} (proj)",
                    source_url=url)
                if ok:
                    upcoming_projected += 1
                break
    logger.info(
        "SA quarterlies: %s/%s past=%d upcoming_proj=%d",
        raw_ticker, exch, inserted, upcoming_projected)
    return inserted + upcoming_projected


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

    # Merge Telegram channels for this exchange. The mapping is
    # ``DEFAULT_TELEGRAM_CHANNELS`` shipped in code (so a fresh install
    # gets useful defaults without manual setup) overlaid with whatever
    # the user has saved in app_settings["telegram_channels"] from the
    # Engine Room (for adding more channels or removing defaults).
    try:
        for ch in get_effective_telegram_channels(db).get(exchange, []) or []:
            key = f"telegram:{ch}"
            if key not in forum_sources:
                forum_sources.append(key)
    except Exception as _e:
        logger.debug("telegram_channels lookup failed: %s", _e)

    # Auto-attach exchange-default forum sources (e.g. WSE → bankier).
    # Lets users get sensible coverage without per-stock manual config.
    for default_src in _EXCHANGE_DEFAULT_FORUMS.get(exchange, ()):
        if default_src not in forum_sources:
            forum_sources.append(default_src)

    # Auto-attach Substack search whenever Twitter/X search is configured
    # for a stock — both share the same Serper budget + freshness gate, so
    # piggy-backing keeps cost predictable and only adds ONE extra Serper
    # call per refresh per stock.
    if "twitter" in forum_sources and "substack" not in forum_sources:
        forum_sources.append("substack")

    # Check if Serper-based forum sources should be skipped (fresh data)
    serper_forum_fresh = _is_fresh(db, "forum_mentions", ticker, STALE_FORUM_HOURS)

    for forum_name in forum_sources:

        # Special case: Bankier (Poland) — needs ticker→slug lookup, then
        # parses the threadlist HTML.
        if forum_name == "bankier":
            count = _fetch_bankier_threads(stock, db)
            new_count += count
            continue

        # Special case: Reddit subreddit — fetches /r/<sub>/new/.json and
        # filters posts for ticker / company-name mentions.
        if forum_name.startswith("reddit:"):
            sub = forum_name.split(":", 1)[1]
            count = _fetch_reddit_posts(sub, stock, db)
            new_count += count
            continue

        # Special case: capital.gr forum (Greece) — Atom feed of latest
        # messages, scanned once per refresh and filtered per-stock.
        if forum_name == "capital_gr":
            count = _fetch_capitalgr_messages(stock, db)
            new_count += count
            continue

        # Special case: Naver Finance discussion board (Korea) — per-stock
        # board page, every post is on-topic by construction.
        if forum_name == "naver_finance":
            count = _fetch_naver_threads(stock, db)
            new_count += count
            continue

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
            if not _serper_is_enabled():
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
                has_ticker = _word_boundary_match(text_check, tk_lower)

                # Check for company name as adjacent phrase
                # Build a phrase from the first 2-3 significant name words
                # e.g. "matrix concept" from "Matrix Concept Holdings"
                name_phrase = " ".join(name_words[:3])
                has_phrase = name_phrase in text_check if len(name_words) >= 2 else False

                # For single-word names, just check that word
                if len(name_words) == 1:
                    has_phrase = _word_boundary_match(text_check, name_words[0])

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
                    # Check that a non-ticker name word appears near the ticker.
                    # Stricter boundary so "arna" inside Italian "l'arna"
                    # doesn't qualify as a ticker hit.
                    other_words = [w for w in name_words if w != tk_lower]
                    boundary = "[^" + _LETTER_LIKE + "]"
                    for tm in re.finditer(r"(?:^|" + boundary + ")"
                                          + re.escape(tk_lower)
                                          + r"(?=$|" + boundary + ")", text_check):
                        # Position of the actual ticker characters within the match
                        ticker_start = tm.end() - len(tk_lower)
                        window = text_check[max(0, ticker_start-60):
                                            ticker_start+len(tk_lower)+60]
                        if any(w in window for w in other_words):
                            relevant = True
                            break

                if not relevant:
                    continue
                # Apply the same disambiguation denylist used for News
                # — a forum item satisfies the relevance check based on
                # ticker/name proximity, but a denylist hit (e.g.
                # "veon moss" / NFL context for VEON) means it's still
                # a person/place with the same surface form.
                if _title_is_disambiguation_false_positive(ticker, title, snippet):
                    logger.info("  skip Twitter false-positive for %s: %s",
                                ticker, title[:60])
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

        # Special case: Substack mentions (site:substack.com via Serper)
        if forum_name == "substack":
            if not _serper_is_enabled():
                continue
            if serper_forum_fresh:
                logger.info("FORUM Substack skip %s — data is fresh", ticker)
                continue
            logger.info("FORUM Substack search for %s", ticker)
            name_q = stock["name"]
            query = f'site:substack.com "{name_q}" OR "{ticker}"'
            results = serper_web_search(query, config, caller="forums",
                                        ticker=ticker)
            # Same relevance check as Twitter — at least one company-name
            # word must appear adjacent to the ticker, OR the multi-word
            # company name appears as a phrase, to avoid matches on
            # unrelated newsletters that mention common words.
            name_words = [w.lower() for w in name_q.split() if len(w) >= 4]
            tk_lower = ticker.lower()
            for item in results[:10]:
                title = item.get("title", "")
                snippet = item.get("snippet", "")
                link = item.get("link", "")
                pub_date = item.get("date", "")
                source = item.get("source", "Substack")
                if not title or not pub_date or not link:
                    continue
                if "substack.com" not in link.lower():
                    continue
                text_check = (title + " " + snippet).lower()
                has_ticker = _word_boundary_match(text_check, tk_lower)
                name_phrase = " ".join(name_words[:3])
                has_phrase = (
                    name_phrase in text_check
                    if len(name_words) >= 2 else
                    _word_boundary_match(text_check, name_words[0])
                    if name_words else False
                )
                # Short tickers (≤4 chars) match too many unrelated
                # acronyms (e.g. ARNA = Arkansas Nurses Association). For
                # these we require the ticker AND at least one name word
                # nearby — same logic Twitter uses. Long tickers (5+) or
                # cashtag-style queries don't need the extra check.
                if has_ticker and not has_phrase and len(tk_lower) <= 4:
                    boundary = "[^" + _LETTER_LIKE + "]"
                    other_words = [w for w in name_words if w != tk_lower]
                    nearby_match = False
                    for tm in re.finditer(r"(?:^|" + boundary + ")"
                                          + re.escape(tk_lower)
                                          + r"(?=$|" + boundary + ")",
                                          text_check):
                        ts = tm.end() - len(tk_lower)
                        window = text_check[max(0, ts-60):ts+len(tk_lower)+60]
                        if any(w in window for w in other_words):
                            nearby_match = True
                            break
                    if not nearby_match:
                        has_ticker = False  # short-ticker hit without context
                if not (has_phrase or has_ticker):
                    continue
                # Apply News-style disambiguation denylist — e.g. an
                # NFL "Veon Moss" Substack item satisfies the
                # relevance check (name word "veon" present) but it's
                # the same false-positive class we filter from news.
                if _title_is_disambiguation_false_positive(ticker, title, snippet):
                    logger.info("  skip Substack false-positive for %s: %s",
                                ticker, title[:60])
                    continue
                text_combined = f"{title} — {snippet[:200]}" if snippet else title
                stored = db.insert_forum(
                    ticker=ticker, exchange=exchange,
                    forum="substack",
                    author=source.replace(" - Substack", "") or "Substack",
                    text=text_combined[:400],
                    post_url=link,
                    posted_at=pub_date,
                    lang=lang)
                if stored:
                    new_count += 1
            continue

        # Special case: Serper-powered discussion search
        if forum_name == "serper_discuss":
            if not _serper_is_enabled():
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
                # Apply News-style disambiguation denylist — short-ticker
                # collisions (NFL Veon, Pierce County …) are the same
                # class of false positive whether they arrive via the
                # News path or the Serper-discussion forum path.
                if _title_is_disambiguation_false_positive(ticker, title, snippet):
                    logger.info("  skip Serper-discuss false-positive for %s: %s",
                                ticker, title[:60])
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


# ---------------------------------------------------------------------------
# Polish forum: bankier.pl
# ---------------------------------------------------------------------------
# Bankier exposes a per-stock thread listing at
#   /forum/forum_o_<slug>,6,21,<id>.html
# Each WSE-listed company has a unique (slug, id) pair. The mapping is
# discoverable via the dropdown on the master /forum/forum_gielda page,
# which we fetch once and cache in-process. Each <option> looks like:
#   <option value="atrem,6,21,10000000509">ATREM</option>

# Default public Telegram channels per exchange — shipped with the
# repo so any fresh install gets useful default forum-buzz sources.
# Each handle has been verified to expose public posts via
# https://t.me/s/<handle>. Users can add more (or remove these) from
# the Engine Room "Telegram forum channels" card; the user-saved list
# is layered ON TOP of these defaults via get_effective_telegram_channels().
DEFAULT_TELEGRAM_CHANNELS: dict[str, tuple[str, ...]] = {
    "UZSE":  ("avestagroupuz", "kapitalbankuz", "centralasia_news"),
    "KASE":  ("centralasia_news",),
    "KSE":   ("centralasia_news",),
    "DSEB":  ("bdstocks",),
    "HOSE":  ("cafef_vn",),
    "NGX":   ("proshareng", "ngxgroup", "businessdayng"),
    "IDX":   ("cnbcindonesia", "saham_indonesia"),
    "PSE":   ("ANCalerts", "rappler"),
    "EGX":   ("egyptstocks",),
}


def get_effective_telegram_channels(db) -> dict:
    """Return the merged exchange→[handles] mapping.

    Layered as: DEFAULT_TELEGRAM_CHANNELS (shipped in code, every install
    gets these) → app_settings["telegram_channels"] (user additions and
    overrides from the Engine Room). Within each exchange, user handles
    are appended to defaults (deduped). If the user explicitly saves an
    empty list for an exchange, that disables defaults for that exchange
    (so users can remove channels they don't like).
    """
    import json as _json
    out: dict[str, list[str]] = {
        ex: list(hs) for ex, hs in DEFAULT_TELEGRAM_CHANNELS.items()
    }
    try:
        raw = db.get_setting("telegram_channels", "")
    except Exception:
        return out
    if not raw:
        return out
    try:
        user_map = _json.loads(raw)
    except Exception:
        return out
    if not isinstance(user_map, dict):
        return out
    for ex, handles in user_map.items():
        if not isinstance(handles, list):
            continue
        ex = str(ex).strip().upper()
        if not ex:
            continue
        if not handles:
            # User explicitly cleared this exchange — drop defaults too
            out[ex] = []
            continue
        merged = list(out.get(ex, []))
        for h in handles:
            h = str(h).strip()
            if h and h not in merged:
                merged.append(h)
        out[ex] = merged
    return out


_EXCHANGE_DEFAULT_FORUMS: dict[str, tuple[str, ...]] = {
    "WSE":   ("bankier",),
    # Indonesia: r/finansial (the active Indonesian finance sub) reads
    # cleanly via Reddit's .json endpoint. Stockbit (the dominant retail
    # platform) is a JS-rendered SPA that resists scraping. Twitter +
    # Substack via Serper for paid coverage.
    "IDX":   ("reddit:finansial", "twitter", "serper_discuss"),
    # Philippines: r/phinvest is the canonical retail-investor sub —
    # very active. PSE Edge AJAX is gated; Reddit covers most chatter.
    "PSE":   ("reddit:phinvest", "twitter", "serper_discuss"),
    # Greece: capital.gr is the dominant Greek investing forum. Their
    # Atom feed exposes the latest messages across the whole site, which
    # we scan once per refresh and match against every watchlist stock.
    "ATHEX": ("capital_gr", "twitter", "serper_discuss"),
    # South Africa — r/JSE_Bets is small but the only retail-investor
    # subreddit dedicated to JSE listings; Sharenet/ShareChat/Moneyweb
    # comments resist scraping. Twitter+Substack via Serper for paid.
    "JSE":   ("reddit:JSE_Bets", "twitter", "serper_discuss"),
    # South Korea — Naver Finance is the dominant retail venue; per-stock
    # discussion boards with fresh posts every minute. English-language
    # Korean stock subreddits are dead (largest: 6 subscribers). Posts
    # are in Korean — translation pipeline renders English on render.
    "KRX":   ("naver_finance", "twitter", "serper_discuss"),
    "KOSPI": ("naver_finance", "twitter", "serper_discuss"),
    "KOSDAQ":("naver_finance", "twitter", "serper_discuss"),
    # ── More native Reddit forums per exchange ────────────────────────
    # Each sub below was probed for activity (≥10 posts in last 30d) so
    # the matcher has fresh content to scan against the watchlist. They
    # ride the cached fetch (one HTTP call per refresh per sub).
    "SGX":   ("reddit:singaporefi",),
    "HKSE":  ("reddit:HKstocks",),
    "ASX":   ("reddit:asx",),
    "TSX":   ("reddit:CanadianInvestor",),
    "OMX":   ("reddit:aktiemarknaden",),
    "LSE":   ("reddit:UKInvesting",),
    "FRA":   ("reddit:Aktien",),
    # General-country subs — broader signal, fewer hits per refresh,
    # but they pick up mentions that wouldn't appear anywhere else.
    "KSE":   ("reddit:Kyrgyzstan",),
    "UZSE":  ("reddit:Uzbekistan",),
    "PSX":   ("reddit:Pakistan",),
    "DSEB":  ("reddit:bangladesh",),
    "HOSE":  ("reddit:vietnam",),
}

_BANKIER_INDEX_URL = "https://www.bankier.pl/forum/forum_gielda,6,1.html"
_BANKIER_TICKER_MAP: dict[str, tuple[str, str]] = {}  # ticker → (slug, id)
_BANKIER_INDEX_LOADED = False


def _bankier_load_index() -> None:
    """Fetch the master ticker→(slug, id) mapping from bankier.pl once."""
    global _BANKIER_INDEX_LOADED
    if _BANKIER_INDEX_LOADED and _BANKIER_TICKER_MAP:
        return
    html = _fetch_page_text(_BANKIER_INDEX_URL, timeout=10, raw=True)
    if not html:
        # Mark as loaded anyway to avoid retry storms in this process
        _BANKIER_INDEX_LOADED = True
        return
    # <option value="atrem,6,21,10000000509">ATREM</option>
    pat = re.compile(
        r'<option\s+value="([a-z0-9\-]+),6,21,(\d+)"\s*>\s*([A-Z0-9\-]+)\s*</option>',
        re.IGNORECASE,
    )
    for slug, fid, ticker in pat.findall(html):
        _BANKIER_TICKER_MAP[ticker.upper()] = (slug, fid)
    _BANKIER_INDEX_LOADED = True
    logger.info("bankier index loaded: %d tickers", len(_BANKIER_TICKER_MAP))


def _bankier_slug_from_name(name: str) -> str:
    """Best-effort name → bankier slug. Strips legal suffixes, lowercases,
    replaces spaces with hyphens. e.g. 'Atrem S.A.' → 'atrem'."""
    if not name:
        return ""
    s = name.lower().strip()
    # Strip common Polish corporate suffixes
    for suffix in (" s.a.", " s a", " sa", " sp.z o.o.", " sp. z o.o.", " sp z oo",
                   " gk", " group", " holding", " s.k.a."):
        if s.endswith(suffix):
            s = s[: -len(suffix)].strip()
    # Normalize whitespace + non-alpha
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    return s


def _bankier_url_for(ticker: str, name: str = "") -> str | None:
    """Look up the bankier forum URL for a Polish stock.

    Bankier keys by its own shortcode (often the company-name acronym, e.g.
    ATREM), not the WSE numeric/letter ticker (e.g. ATR). We try the
    ticker first; if no match, derive a slug from the company name and
    look that up against the slug→id index.
    """
    _bankier_load_index()
    entry = _BANKIER_TICKER_MAP.get((ticker or "").upper())
    if not entry and name:
        slug = _bankier_slug_from_name(name)
        if slug:
            # Reverse lookup: find a (ticker→(slug,id)) where slug matches
            for v in _BANKIER_TICKER_MAP.values():
                if v[0] == slug:
                    entry = v
                    break
    if not entry:
        return None
    slug, fid = entry
    return f"https://www.bankier.pl/forum/forum_o_{slug},6,21,{fid}.html"


def _extract_bankier_threads(html: str, base_url: str) -> list[dict]:
    """
    Parse a bankier.pl per-stock forum threadlist.

    Each row in the table looks like:
        <td class="threadTitle"><a href="temat_X,123.html">Title</a></td>
        <td class="threadAuthor textNowrap">~Author</td>
        <td class="threadCount textAlignCenter textNowrap"><span class="icon">N</span></td>
        <td class="createDate textAlignCenter textNowrap">2026-04-24 19:24</td>

    Returns a list of dicts with author/text/date/post_url keys.
    """
    out: list[dict] = []
    # Match a complete thread block: title (with link) + author + date.
    # threadCount is optional in older rows.
    pat = re.compile(
        r'<td class="threadTitle">\s*'
        r'<a href="([^"]+)">([^<]+)</a>\s*</td>\s*'
        r'<td class="threadAuthor[^"]*">\s*([^<]+?)\s*</td>'
        r'(?:.*?<td class="createDate[^"]*">\s*(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2})\s*</td>)?',
        re.DOTALL,
    )
    for href, title, author, date_str in pat.findall(html):
        title = (title or "").strip()
        if not title:
            continue
        post_url = href if href.startswith("http") else (
            "https://www.bankier.pl/forum/" + href.lstrip("/")
        )
        out.append({
            "author": (author or "").lstrip("~").strip(),
            "text": title,
            "date": (date_str or "").strip(),
            "post_url": post_url,
        })
    return out


def _fetch_bankier_threads(stock: dict, db: Database) -> int:
    """Fetch and store the latest threads for a Polish (WSE) stock.

    Bankier's per-stock forums occasionally have very low-signal threads
    titled e.g. "hub", "ok", "pytanie" (= "question") that aren't worth
    showing. We filter those out so the dashboard doesn't list zero-info
    cards.
    """
    ticker = (stock.get("ticker") or "").upper()
    exchange = stock.get("exchange", "WSE")
    name = stock.get("name", "")
    forum_url = _bankier_url_for(ticker, name)
    if not forum_url:
        logger.info("bankier: no forum entry for %s — skipping", ticker)
        return 0
    logger.info("FORUM bankier: %s → %s", ticker, forum_url)
    html = _fetch_page_text(forum_url, timeout=12, raw=True)
    if not html:
        return 0
    threads = _extract_bankier_threads(html, forum_url)
    name_words_low = {w.lower() for w in re.split(r"\s+", name) if w}
    new_count = 0
    skipped_low = 0
    for t in threads[:25]:
        title = (t.get("text") or "").strip()
        if not title:
            continue
        # Drop low-signal one-word threads (≤4 chars OR a single word that
        # is just a substring of the company's own name — e.g. "hub" on
        # "Bridge Solutions Hub" has zero added information).
        words = title.split()
        if len(words) == 1:
            w = words[0].lower()
            if len(w) <= 4 or w in name_words_low:
                skipped_low += 1
                continue
        stored = db.insert_forum(
            ticker=ticker, exchange=exchange,
            forum="bankier",
            author=t.get("author") or "Anonymous",
            text=title[:400],
            post_url=t.get("post_url") or forum_url,
            posted_at=t.get("date", ""),
            lang="pl",
        )
        if stored:
            new_count += 1
    logger.info("  → %d new bankier threads for %s (skipped %d low-signal)",
                new_count, ticker, skipped_low)
    return new_count


# ---------------------------------------------------------------------------
# Naver Finance discussion board fetcher (KRX — South Korea)
# ---------------------------------------------------------------------------
# Korean retail investing happens almost entirely on Naver Finance
# (finance.naver.com/item/board.naver?code=XXXXXX). The English-language
# Korean-stocks subreddits are essentially dead (largest has ~6 subs);
# Naver is where the real action lives — fresh posts every minute on the
# blue-chip names. The board is per-stock so EVERY post on the page is
# already on-topic, no needle filtering required.
#
# Posts are 100% Korean — the translation pipeline downstream (cached
# via the `translations` table) renders them as English on the
# dashboard, so users without Korean still get readable buzz.
def _naver_code_for(stock: dict) -> str:
    """Resolve a 6-digit Naver Finance code from the stock entry.

    Korean tickers ARE 6-digit numerics, so the code is normally just
    `ticker.zfill(6)`. We also accept Yahoo-style suffixes like
    `005930.KS` / `005930.KQ` for tolerance.
    """
    ticker = (stock.get("ticker") or "").strip()
    if "." in ticker:
        ticker = ticker.split(".", 1)[0]
    yt = (stock.get("yahoo_ticker") or "").strip()
    if yt and "." in yt and not ticker.isdigit():
        candidate = yt.split(".", 1)[0]
        if candidate:
            ticker = candidate
    # KRX codes are 6 chars: usually all digits, but preferred-share
    # codes have a single trailing alpha (e.g. 00088K, 005935 → 005931
    # vs 005930 common). Naver accepts both.
    if not ticker:
        return ""
    if ticker.isdigit():
        return ticker.zfill(6)
    if (len(ticker) == 6 and ticker[:5].isdigit()
            and ticker[5].isalpha()):
        return ticker.upper()
    return ""


def _fetch_naver_threads(stock: dict, db: Database) -> int:
    """Fetch the latest posts from a Naver Finance discussion board and
    store them as forum_mentions. Returns count of new rows.

    Each Korean stock has its OWN board page — every post is already
    on-topic, so we skip the needle-match filter and just store the 25
    most recent posts. Spam / one-line junk is filtered by length only.
    """
    code = _naver_code_for(stock)
    ticker = (stock.get("ticker") or "").upper()
    exchange = stock.get("exchange", "KRX")
    if not code:
        logger.info("naver: no 6-digit code for %s — skipping", ticker)
        return 0

    forum_url = f"https://finance.naver.com/item/board.naver?code={code}"
    logger.info("FORUM Naver: %s → %s", ticker, forum_url)
    html = _fetch_page_text(forum_url, timeout=12, raw=True)
    if not html:
        return 0

    # Each row in the board table is a <tr> with:
    #   <a href="/item/board_read.naver?code=...&nid=...">TITLE</a>
    #   the date in YYYY.MM.DD HH:MM format
    rows = re.findall(r"<tr[^>]*>(.*?)</tr>", html, re.DOTALL)
    new_count = 0
    seen_titles = set()
    for row in rows:
        m = re.search(
            r'<a [^>]*href="(/item/board_read\.naver\?[^"]+)"[^>]*>(.*?)</a>',
            row, re.DOTALL,
        )
        if not m:
            continue
        href = m.group(1)
        title_html = m.group(2)
        title = re.sub(r"\s+", " ",
                        re.sub(r"<[^>]+>", "", title_html)).strip()
        # Decode common HTML entities Naver leaves in titles
        title = (title.replace("&quot;", '"').replace("&amp;", "&")
                       .replace("&lt;", "<").replace("&gt;", ">")
                       .replace("&#39;", "'"))
        if not title or len(title) < 4 or len(title) > 300:
            continue
        if title in seen_titles:
            continue
        seen_titles.add(title)
        date_m = re.search(r"(\d{4})\.(\d{2})\.(\d{2})\s+(\d{2}:\d{2})", row)
        posted_at = ""
        if date_m:
            y, mo, d, t = date_m.groups()
            posted_at = f"{y}-{mo}-{d} {t}"
        post_url = "https://finance.naver.com" + href

        stored = db.insert_forum(
            ticker=ticker, exchange=exchange,
            forum="naver_finance",
            author="naver",
            text=title[:400],
            post_url=post_url,
            posted_at=posted_at,
            lang="ko",
        )
        if stored:
            new_count += 1
        if new_count >= 25:  # cap per refresh
            break

    logger.info("  → %d new Naver posts for %s", new_count, ticker)
    return new_count


# ---------------------------------------------------------------------------
# Reddit subreddit fetcher (used by IDX/PSE forum sources)
# ---------------------------------------------------------------------------
# Reddit's old.reddit.com/r/{sub}/new/.json endpoint returns the latest
# 25 posts in a subreddit as JSON. We cache the response per-subreddit
# in this process so multiple stocks scanning the same sub don't re-hit
# the network. Posts are filtered per-stock by ticker / company-name
# match (same logic the Telegram handler uses).
_REDDIT_CACHE: dict[str, tuple[float, list[dict]]] = {}
_REDDIT_TTL = 30 * 60  # 30 min


def _fetch_reddit_subreddit(sub: str) -> list[dict]:
    """Return cached list of {title, selftext, url, author, created_utc}
    posts for a subreddit. Empty list on failure."""
    import time as _time, json as _json
    import ssl as _ssl
    now = _time.monotonic()
    cached = _REDDIT_CACHE.get(sub)
    if cached and (now - cached[0]) < _REDDIT_TTL:
        return cached[1]
    url = f"https://old.reddit.com/r/{sub}/new/.json?limit=50"
    ctx = _ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = _ssl.CERT_NONE
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "emerging-edge/1.0 stockmonitor",
            "Accept": "application/json",
        })
        with urllib.request.urlopen(req, timeout=10, context=ctx) as resp:
            data = _json.loads(resp.read())
    except Exception as e:
        logger.info("reddit r/%s fetch failed: %s", sub, e)
        _REDDIT_CACHE[sub] = (now, [])
        return []
    out: list[dict] = []
    for c in (data.get("data") or {}).get("children") or []:
        d = c.get("data") or {}
        out.append({
            "title": d.get("title") or "",
            "selftext": d.get("selftext") or "",
            "url": "https://www.reddit.com" + (d.get("permalink") or ""),
            "author": d.get("author") or "anon",
            "created_utc": d.get("created_utc") or 0,
        })
    _REDDIT_CACHE[sub] = (now, out)
    logger.info("reddit r/%s: cached %d posts", sub, len(out))
    return out


def _build_forum_needles(stock: dict, db: Database) -> list[str]:
    """Lowercase needles to match this stock in forum text. Pulls
    user-supplied aliases from funds.get_aliases() so a Greek-script
    or local-language alias can be added without code changes."""
    ticker = (stock.get("ticker") or "").upper()
    name = (stock.get("name") or "").strip()
    exchange = (stock.get("exchange") or "").upper()
    needles: list[str] = []
    # User-supplied aliases (TICKER:EXCHANGE → list[str]) — most
    # authoritative; works for non-Latin scripts (ΟΠΑΠ, ΠΕΙΡ, etc.).
    try:
        from funds import get_aliases as _ga
        aliases = _ga(db)
        for a in aliases.get(f"{ticker}:{exchange}", []):
            a_low = a.strip().lower()
            if a_low and a_low not in needles:
                needles.append(a_low)
    except Exception:
        pass
    if ticker and len(ticker) >= 3 and ticker.isalpha():
        needles.append(ticker.lower())
    if name:
        first_words = " ".join(name.split()[:2]).lower()
        if first_words and first_words not in needles:
            needles.append(first_words)
    return needles


# Characters that count as a "letter-like" continuation — i.e. if one of
# these appears immediately before or after a needle match, it's NOT a
# word boundary. This excludes apostrophes (straight + curly + Greek
# tonos), hyphens (regular + en/em dash), underscores, and digits — so
# "arna" inside "l'arna" or "varna-2" or "arna1" does not register as a
# match for the ticker ARNA.
_LETTER_LIKE = (
    r"A-Za-z"          # ASCII letters
    r"À-ɏ"   # Latin extended (accented Western)
    r"Ͱ-Ͽ"   # Greek
    r"Ѐ-ӿ"   # Cyrillic
    r"֐-׿"   # Hebrew
    r"؀-ۿ"   # Arabic
    r"ऀ-ॿ"   # Devanagari
    r"一-鿿"   # CJK Unified
    r"぀-ゟ"   # Hiragana
    r"゠-ヿ"   # Katakana
    r"가-힯"   # Hangul
    r"0-9"             # digits — "arna1" is not a match for ARNA
    r"'’‘ʼ"   # apostrophes (straight, curly, modifier)
    r"\-–—"  # hyphens / dashes
    r"_"               # underscore
)


def _needle_matches(haystack: str, needles: list[str]) -> bool:
    """True if any needle appears in haystack with strict word boundaries.

    Boundaries must be a non-letter, non-apostrophe, non-hyphen, non-digit
    character (or start/end of string). This prevents false positives like
    matching `arna` inside Italian `l'arna` (duck) for ticker ARNA, or
    `tigo` inside `intrigo`.
    """
    if not haystack or not needles:
        return False
    boundary = "[^" + _LETTER_LIKE + "]"
    for n in needles:
        if re.search(r"(?:^|" + boundary + ")" + re.escape(n) +
                     r"(?=$|" + boundary + ")", haystack):
            return True
    return False


def _word_boundary_match(haystack: str, needle: str) -> bool:
    """Single-needle variant of ``_needle_matches`` for callers that
    already have one specific term to look for (Twitter/Substack)."""
    return _needle_matches(haystack, [needle])


def _fetch_reddit_posts(sub: str, stock: dict, db: Database) -> int:
    """Filter cached subreddit posts for mentions of ``stock`` and
    insert matches into forum_mentions."""
    import datetime as _dt
    posts = _fetch_reddit_subreddit(sub)
    if not posts:
        return 0
    ticker = (stock.get("ticker") or "").upper()
    exchange = stock.get("exchange", "")
    lang = stock.get("lang", "en")
    needles = _build_forum_needles(stock, db)
    if not needles:
        return 0
    new_count = 0
    for p in posts:
        haystack = (p["title"] + "\n" + p["selftext"]).lower()
        if not _needle_matches(haystack, needles):
            continue
        try:
            ts = _dt.datetime.fromtimestamp(
                p["created_utc"], _dt.timezone.utc).strftime("%Y-%m-%d %H:%M")
        except Exception:
            ts = ""
        text_combined = (p["title"] + (" — " + p["selftext"][:240]
                                       if p["selftext"] else ""))[:400]
        stored = db.insert_forum(
            ticker=ticker, exchange=exchange,
            forum=f"reddit/{sub}",
            author="u/" + p["author"],
            text=text_combined,
            post_url=p["url"],
            posted_at=ts,
            lang=lang)
        if stored:
            new_count += 1
    if new_count:
        logger.info("  → %d new r/%s mentions for %s", new_count, sub, ticker)
    return new_count


# ---------------------------------------------------------------------------
# capital.gr forum fetcher (Greek ATHEX stocks)
# ---------------------------------------------------------------------------
# capital.gr exposes an Atom feed of the latest forum messages across
# the whole site. We fetch it once per refresh and filter per-stock.
_CAPITALGR_FEED_URL = ("https://www.capital.gr/forum/api/threads/"
                       "getfeedforlatestmessages/")
_CAPITALGR_CACHE: tuple[float, list[dict]] | None = None
_CAPITALGR_TTL = 30 * 60


def _fetch_capitalgr_feed() -> list[dict]:
    """Return cached capital.gr forum messages. Each entry is
    {title, content, author, link, posted_at}."""
    import time as _time, ssl as _ssl
    global _CAPITALGR_CACHE
    now = _time.monotonic()
    if _CAPITALGR_CACHE and (now - _CAPITALGR_CACHE[0]) < _CAPITALGR_TTL:
        return _CAPITALGR_CACHE[1]
    ctx = _ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = _ssl.CERT_NONE
    try:
        req = urllib.request.Request(_CAPITALGR_FEED_URL, headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36",
            "Accept": "application/atom+xml,application/xml,text/xml;q=0.9",
        })
        with urllib.request.urlopen(req, timeout=10, context=ctx) as resp:
            xml = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        logger.info("capital.gr feed fetch failed: %s", e)
        _CAPITALGR_CACHE = (now, [])
        return []
    out: list[dict] = []
    # Each <entry> has <id>, <title>, <updated>, <author>/<name>, <link>,
    # <content type="text">.
    for entry in re.findall(r"<entry[^>]*>(.*?)</entry>", xml, re.DOTALL):
        title_m = re.search(r"<title[^>]*>([^<]+)</title>", entry, re.DOTALL)
        cont_m = re.search(r"<content[^>]*>([^<]+)</content>", entry, re.DOTALL)
        author_m = re.search(r"<name>([^<]+)</name>", entry)
        link_m = re.search(r'<link[^>]+href="([^"]+)"', entry)
        upd_m = re.search(r"<updated>([^<]+)</updated>", entry)
        title = (title_m.group(1) if title_m else "").strip()
        content = (cont_m.group(1) if cont_m else "").strip()
        if not title and not content:
            continue
        # Atom dates: 2026-04-25T12:13:30+03:00 → keep first 16 chars
        ts = (upd_m.group(1) if upd_m else "").strip()[:16].replace("T", " ")
        out.append({
            "title": title,
            "content": content,
            "author": (author_m.group(1) if author_m else "").strip() or "anon",
            "link": (link_m.group(1) if link_m else "").strip(),
            "posted_at": ts,
        })
    _CAPITALGR_CACHE = (now, out)
    logger.info("capital.gr feed: cached %d messages", len(out))
    return out


def _fetch_capitalgr_messages(stock: dict, db: Database) -> int:
    """Filter cached capital.gr feed for stock mentions and store hits.
    Greek forum content is in Greek script, so users will typically
    need to add Greek-script aliases for their ATHEX stocks via the
    Engine Room (e.g. ASCO:ATHEX → ['ΑΣ ΚΟΜΠΑΝΥ', 'AS COMPANY'])."""
    msgs = _fetch_capitalgr_feed()
    if not msgs:
        return 0
    ticker = (stock.get("ticker") or "").upper()
    exchange = stock.get("exchange", "")
    needles = _build_forum_needles(stock, db)
    if not needles:
        return 0
    new_count = 0
    for m in msgs:
        haystack = (m["title"] + "\n" + m["content"]).lower()
        if not _needle_matches(haystack, needles):
            continue
        text_combined = (m["title"] + (" — " + m["content"][:240]
                                       if m["content"] else ""))[:400]
        stored = db.insert_forum(
            ticker=ticker, exchange=exchange,
            forum="capital.gr",
            author=m["author"],
            text=text_combined,
            post_url=m["link"],
            posted_at=m["posted_at"],
            lang="el")
        if stored:
            new_count += 1
    if new_count:
        logger.info("  → %d new capital.gr mentions for %s", new_count, ticker)
    return new_count


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

    i3investor renders the visible window (~50 comments) in *oldest-
    first* chronological order. The outer caller does `comments[:20]`,
    so if we returned the first 20 we'd surface 2023-era posts for any
    stock whose recent activity is sparse. Parse ALL visible comments
    and return them newest-first so the cap actually gives the newest
    20.
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
        # No early break: scan all visible (~50) comments, then sort
        # newest-first below so the caller's [:20] cap surfaces the
        # most recent posts.

    # Newest first — outer caller does comments[:20].
    comments.sort(key=lambda c: c.get("date", ""), reverse=True)
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
    # Build search terms. We DROP generic corporate suffixes — every
    # Indonesian listing ends in "Tbk", every Malaysian one in "Bhd",
    # every Vietnamese one in "JSC", etc. Keeping them as needles
    # cross-matches every story about any local listed company onto
    # every stock in the user's monitor. Also drop very short words
    # that produce false positives ("the", "and", country names like
    # "indonesia" inside the channel boilerplate).
    _CORP_STOPWORDS = {
        # Indonesia
        "tbk", "tbk.", "pt",
        # Malaysia / Singapore
        "bhd", "bhd.", "berhad", "sdn", "sendirian", "ltd", "ltd.",
        "limited", "pte",
        # Western
        "inc", "inc.", "incorporated", "corp", "corp.", "corporation",
        "co", "co.", "company", "plc", "plc.", "group", "holdings",
        "holding", "international", "intl",
        # European
        "sa", "s.a.", "ag", "nv", "n.v.", "bv", "b.v.", "spa", "s.p.a.",
        "ab", "asa", "as", "oyj", "kgaa", "se",
        # Other regional
        "psc", "qpsc", "kpsc", "jsc", "ojsc", "pjsc", "ooredoo",
        # Generic English fillers that creep in via long company names
        "the", "and", "of", "for",
    }
    search_terms = []
    if ticker and len(ticker) >= 3:
        search_terms.append(ticker.lower())
    for word in company_name.lower().split():
        # Strip trailing punctuation for the stopword test but keep the
        # cleaned form for matching.
        clean = word.strip(".,;:()[]")
        if len(clean) < 4:
            continue
        if clean in _CORP_STOPWORDS:
            continue
        search_terms.append(clean)
    if not search_terms:
        return []
    # Word-boundary matcher — substring matches let "art" match "smart",
    # "tbk" match "outback" etc. Boundary = anything that isn't a letter,
    # digit, hyphen, apostrophe.
    _boundary = r"[^A-Za-z0-9'\-]"
    _term_patterns = [
        re.compile(r"(?:^|" + _boundary + ")" + re.escape(t) +
                   r"(?=$|" + _boundary + ")")
        for t in search_terms
    ]
    def _matches(text_lower: str) -> bool:
        return any(p.search(text_lower) for p in _term_patterns)

    # Spam / pump-and-dump filter. Local-language Telegram channels are
    # heavily polluted with "registration / minimum capital / fill in the
    # data format / distribution of winnings" pyramid pitches. Match a
    # short list of unambiguous spam markers — if any hit, drop the post.
    # Lowercased; we test both the original message and the translation
    # path is irrelevant here since we run before storage.
    _SPAM_MARKERS = (
        "minimum capital", "min capital", "fill in the data",
        "fill in the format", "registration:", "please fill in",
        "distribution of winn", "join today", "investasi aman",
        "modal minimum", "daftar sekarang", "no risk",
        "no risk loss", "lose how to join", "guaranteed profit",
        "100% profit", "passive income guarantee",
        "pendaftaran:", "data format", "format pendaftaran",
        # Decorative spam frames common in Indonesian/Vietnamese pyramid pitches
        "《《《", "》》》", "▶▶▶", "◀◀◀",
    )
    def _is_spam(text_lower: str) -> bool:
        return any(marker in text_lower for marker in _SPAM_MARKERS)

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
        if not _matches(msg_lower):
            continue
        if _is_spam(msg_lower):
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
            chunk_lower = chunk_clean.lower()
            if not _matches(chunk_lower):
                continue
            if _is_spam(chunk_lower):
                continue
            if any(skip in chunk_lower for skip in ["subscribers", "if you have telegram"]):
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


# Yahoo circuit breaker. Yahoo throttles per-IP: once it starts 429-ing
# it 429s every call for a while, but each attempt still costs several
# seconds (urllib timeout + curl fallback). In a bulk refresh of 200
# stocks that's the single biggest time sink — dozens of stocks each
# waiting out a source that is definitely down. After a few consecutive
# 429s we trip the breaker and short-circuit every subsequent Yahoo
# call (return None instantly) for a cooldown, so those stocks fall
# straight through to the next tier / fail fast. A refresh past the
# cooldown retries Yahoo fresh, and any success resets the streak.
import time as _yb_time
_YAHOO_LOCK = _threading.Lock()
_YAHOO_STATE = {"streak": 0, "down_until": 0.0}
_YAHOO_TRIP_THRESHOLD = 3
_YAHOO_COOLDOWN_S = 300


def _yahoo_circuit_open() -> bool:
    with _YAHOO_LOCK:
        return _yb_time.time() < _YAHOO_STATE["down_until"]


def _yahoo_note_429() -> None:
    with _YAHOO_LOCK:
        _YAHOO_STATE["streak"] += 1
        if _YAHOO_STATE["streak"] >= _YAHOO_TRIP_THRESHOLD:
            _YAHOO_STATE["down_until"] = _yb_time.time() + _YAHOO_COOLDOWN_S


def _yahoo_note_ok() -> None:
    with _YAHOO_LOCK:
        _YAHOO_STATE["streak"] = 0
        _YAHOO_STATE["down_until"] = 0.0


def _fetch_price_yahoo(yahoo_ticker: str, bulk: bool = False) -> Optional[tuple]:
    """
    Fetch price from Yahoo Finance v8 chart API.
    Returns (price, change_pct, currency) or None on failure.

    `bulk=True` (parallel "refresh all") shortens the urllib/curl
    timeouts (opt #5): Yahoo is the last-resort tier, so when it's
    throttled we want to fail in a few seconds and free the worker
    rather than wait out a full 12-15 s timeout per stock.

    A module-level circuit breaker skips Yahoo entirely once it's
    clearly throttled (opt #6), so a whole refresh doesn't waste
    ~10s/stock on a source that is 429-ing every call.

    Yahoo Finance fingerprints Python's TLS handshake and serves 429
    to it consistently, even from residential IPs. Shelling out to
    ``curl`` (which has a real-browser-like TLS fingerprint) sidesteps
    this — verified end-to-end with same machine, same headers,
    Python urllib → 429 / curl → 200. We try urllib first (faster
    when Yahoo's mood permits) and fall back to curl on any failure.
    """
    if not yahoo_ticker:
        return None
    # Breaker tripped — Yahoo is throttled; don't waste ~10s finding out.
    if _yahoo_circuit_open():
        return None

    url = YAHOO_CHART_URL.format(ticker=urllib.parse.quote(yahoo_ticker))
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "identity",
        "Referer": "https://finance.yahoo.com/",
        "Origin": "https://finance.yahoo.com",
    }

    data = None
    _u_timeout = 4 if bulk else 10   # opt #5 — fail fast in bulk refresh

    # Path 1 — Python urllib (cheap; works when not throttled).
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=_u_timeout) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            _yahoo_note_ok()   # succeeded — reset the breaker
    except urllib.error.HTTPError as e:
        if e.code == 429:
            _yahoo_note_429()
        else:
            logger.warning("Yahoo Finance HTTP %d for %s", e.code, yahoo_ticker)
        # 429 → fall through to curl
    except Exception as e:
        logger.debug("Yahoo Finance urllib failed for %s: %s — trying curl",
                     yahoo_ticker, e)

    # Path 2 — fall back to curl-subprocess (real-browser TLS). Single
    # attempt only: Yahoo is now Tier 4 (after Google Finance,
    # stockanalysis, Naver, SGX bulk, KLSE Screener), so by the time
    # we get here the stock is already exotic enough that an additional
    # 11-sec backoff retry rarely helps and just slows the refresh.
    # The 30-min catch-up daemon handles longer-term Yahoo recovery.
    if data is None and _CURL_BIN:
        import subprocess as _sp
        import time as _time
        import tempfile as _tf
        last_err: Exception | None = None
        for attempt, delay in enumerate([0]):
            if delay:
                _time.sleep(delay)
            tmp = _tf.NamedTemporaryFile(delete=False, suffix=".json")
            tmp.close()
            try:
                cmd = [_CURL_BIN, "-sL",
                       "--max-time", ("6" if bulk else "12"),
                       "--compressed", "-o", tmp.name,
                       "-w", "%{http_code}",
                       "-A", headers["User-Agent"]]
                for k in ("Accept", "Accept-Language", "Referer", "Origin"):
                    cmd.extend(["-H", f"{k}: {headers[k]}"])
                cmd.append(url)
                code = _sp.check_output(
                    cmd, timeout=(8 if bulk else 15)).decode().strip()
                if code == "200":
                    with open(tmp.name, "rb") as f:
                        body = f.read()
                    if body.strip():
                        data = json.loads(body)
                        _yahoo_note_ok()   # recovered — reset the breaker
                        if attempt > 0:
                            logger.info("Yahoo recovered after retry #%d for %s",
                                        attempt, yahoo_ticker)
                        break
                if code == "429":
                    _yahoo_note_429()
                last_err = RuntimeError(f"HTTP {code}")
            except Exception as e:
                last_err = e
            finally:
                try:
                    os.unlink(tmp.name)
                except Exception:
                    pass
        if data is None:
            logger.warning("Yahoo Finance (curl fallback) failed for %s: %s",
                           yahoo_ticker, last_err)
            return None
    if data is None:
        return None

    try:
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


# Optimization #2 — stockanalysis.com bulk list cache. SA has no batch
# quote API, but every exchange-list page exposes a /list/<slug>/__data.json
# carrying {symbol, price, change} for the whole exchange in one ~60-100 KB
# JSON. For an exchange where we hold several stocks, one list fetch beats
# N per-stock quote pages. Only used for exchanges with a list page (the
# _SA_LIST_CONFIG set); US/KRX/etc. (huge or list-less) stay per-stock.
_SA_LIST_CACHE: dict[str, tuple] = {}
# Market caps harvested from the SAME list payload as the prices —
# it already carries marketCap, so this costs no extra request.
# list_slug -> {ticker: market_cap_in_local_currency}
_SA_MCAP_CACHE: dict[str, dict] = {}   # list_slug → (ts, {ticker: (px, pct, ccy)})
_SA_LIST_TTL = 5 * 60
# A failed or empty bulk fetch must NOT sit in the cache for the full
# TTL — one transient blip would otherwise blank a whole exchange for
# five minutes, silently pushing every stock on it down to the slower
# per-stock path (or to no price at all). Cache empties only briefly:
# long enough to avoid hammering a source that is genuinely down, short
# enough that a blip heals on the next refresh.
_BULK_EMPTY_TTL = 45
# One registry lock for handing out per-slug locks; the network fetch
# itself is guarded by a *per-slug* lock so different exchange lists
# (Philippine, Colombian, …) fetch concurrently instead of all queueing
# behind one global lock during their 15 s HTTP calls. Same-slug callers
# still dedup onto a single fetch.
_SA_LIST_LOCK = _threading.Lock()
_SA_LIST_SLUG_LOCKS: dict = {}


def _sa_list_slug_lock(slug: str):
    with _SA_LIST_LOCK:
        lk = _SA_LIST_SLUG_LOCKS.get(slug)
        if lk is None:
            lk = _threading.Lock()
            _SA_LIST_SLUG_LOCKS[slug] = lk
        return lk


def _sa_list_meta(exchange: str):
    """Return (list_slug, currency) for an exchange, or None."""
    try:
        from catalog_updaters import _SA_LIST_CONFIG
        cfg = _SA_LIST_CONFIG.get(exchange.upper())
        if cfg:
            return cfg[0], cfg[3]   # (list_slug, currency)
    except Exception:
        pass
    return None


def _fetch_sa_list_bulk(list_slug: str, currency: str) -> dict:
    """{TICKER → (price, change_pct, currency)} for a whole SA exchange list."""
    import time as _t
    # Serve a warm cache WITHOUT taking the lock, so a wedged in-flight
    # fetch can't block readers that don't need it.
    cached = _SA_LIST_CACHE.get(list_slug)
    if cached and _t.time() - cached[0] < (_SA_LIST_TTL if cached[1]
                                          else _BULK_EMPTY_TTL):
        return cached[1]
    # Bounded wait for the fetch lock. The lock exists to dedup concurrent
    # fetches of the same list, not to make callers queue indefinitely: a
    # socket hung mid-read (urlopen's timeout only bounds inactivity, not
    # total transfer) would otherwise pin every worker that needs this
    # exchange, freezing a whole refresh. If the holder hasn't finished in
    # time, give up and let the caller fall through to its per-stock
    # source — slower for that stock, but never a stall.
    lk = _sa_list_slug_lock(list_slug)
    if not lk.acquire(timeout=20):
        logger.warning("SA list bulk: lock busy for %s, skipping bulk path",
                       list_slug)
        return {}
    try:
        now = _t.time()
        cached = _SA_LIST_CACHE.get(list_slug)
        if cached and now - cached[0] < (_SA_LIST_TTL if cached[1]
                                        else _BULK_EMPTY_TTL):
            return cached[1]
        url = (f"https://stockanalysis.com/list/{list_slug}/__data.json")
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 Chrome/120 Safari/537",
            "Accept": "*/*",
            "Accept-Encoding": "identity",
            "Referer": f"https://stockanalysis.com/list/{list_slug}/",
        }
        out: dict[str, tuple] = {}
        mcaps: dict[str, float] = {}
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=15) as r:
                sk = json.loads(r.read())
            for node in (sk or {}).get("nodes") or []:
                if not (isinstance(node, dict) and isinstance(node.get("data"), list)):
                    continue
                arr = node["data"]
                for item in arr:
                    if not (isinstance(item, dict) and "s" in item and "price" in item):
                        continue
                    try:
                        sym = arr[item["s"]] if isinstance(item["s"], int) else item["s"]
                        px = arr[item["price"]] if isinstance(item["price"], int) else item["price"]
                        chg = item.get("change")
                        chg = arr[chg] if isinstance(chg, int) else chg
                        tk = str(sym).split("/")[-1].upper()
                        pxf = float(px)
                        if tk and pxf > 0:
                            out[tk] = (pxf, round(float(chg or 0), 2), currency)
                            mc = item.get("marketCap")
                            mc = arr[mc] if isinstance(mc, int) else mc
                            mc_v = _parse_market_cap(mc)
                            if mc_v:
                                mcaps[tk] = mc_v
                    except (TypeError, ValueError, IndexError):
                        continue
                if out:
                    break
        except Exception as e:
            logger.info("SA list bulk failed for %s: %s", list_slug, e)
        if out:
            logger.info("SA list bulk: cached %d quotes for %s", len(out), list_slug)
        _SA_LIST_CACHE[list_slug] = (now, out)
        if mcaps:
            _SA_MCAP_CACHE[list_slug] = mcaps
        return out
    finally:
        lk.release()


def _parse_market_cap(raw):
    """Market cap as a float, from either shape stockanalysis returns.

    The exchange LIST payload gives a plain number; the per-stock quote
    gives a formatted string like "4.67T" / "912.5M". Returns None for
    anything unrecognised rather than guessing — a wrong market cap is
    worse than a blank one.
    """
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        return float(raw) if raw > 0 else None
    s = str(raw).strip().replace(",", "")
    if not s:
        return None
    mult = 1.0
    if s[-1:].upper() in ("T", "B", "M", "K"):
        mult = {"T": 1e12, "B": 1e9, "M": 1e6, "K": 1e3}[s[-1].upper()]
        s = s[:-1]
    try:
        v = float(s) * mult
    except ValueError:
        return None
    return v if v > 0 else None


# Set by refresh_market_caps so fetch_market_cap can consult the currency
# the price feed actually reported, without threading a db handle through
# every call site.
_MCAP_PRICE_CCY: dict = {}

# Corporate actions can leave a legacy symbol in a user's watchlist after
# the exchange and StockAnalysis have moved to the successor.  Keep these
# aliases deliberately small and explicit: a wrong alias is worse than no
# market cap.  Values are the currently listed symbols used only for the
# market-cap lookup; stored watchlist/history keys remain unchanged.
_MCAP_TICKER_ALIASES: dict[tuple[str, str], str] = {
    ("ASX", "BXN"): "BLS",  # Bioxyne → BLS Pharmaceuticals (June 2026)
}

# Quote lookups need the same successor mapping as market-cap lookups.  The
# watchlist record deliberately remains BXN / “Bioxyne Limited”, so the
# dashboard preserves the investor's familiar name while every data vendor is
# queried with the currently listed BLS symbol.
_PRICE_TICKER_ALIASES: dict[tuple[str, str], str] = {
    ("ASX", "BXN"): "BLS",
}


def _mcap_price_ccy(stock: dict):
    return _MCAP_PRICE_CCY.get(
        ((stock.get("ticker") or ""), (stock.get("exchange") or "")))


def _fetch_market_cap_naver(stock: dict):
    """KRX market cap from Naver Finance, in KRW.

    Naver server-renders the Korean `시가총액` field in units of 100m KRW
    (억원), providing a dependable fallback when StockAnalysis omits its
    marketCap field for a Korean quote.
    """
    code = _naver_code_for(stock)
    if not code:
        return None
    html = _fetch_page_text(
        f"https://finance.naver.com/item/main.naver?code={code}",
        timeout=12, raw=True)
    if not html:
        return None
    text = re.sub(r"<[^>]+>", " ", html).replace("&nbsp;", " ")
    m = re.search(r"시가총액\s*([0-9][0-9,]*)\s*억", text)
    if not m:
        return None
    try:
        return float(m.group(1).replace(",", "")) * 1e8
    except ValueError:
        return None

def _fetch_market_cap_stockscope(stock: dict):
    """UZSE cap published directly by Stockscope, in billions of UZS."""
    url = stock.get("price_url") or ("https://stockscope.uz/en/listings/"
                                      f"{stock.get('ticker','')}/general")
    html = _fetch_page_text(url, timeout=15, raw=True)
    if not html:
        return None
    text = re.sub(r"<[^>]+>", " ", html).replace("&nbsp;", " ")
    m = re.search(r"Market\s*Cap\s*([0-9][0-9,.]*)\s*bn\b", text, re.I)
    if not m:
        return None
    try:
        return float(m.group(1).replace(",", "")) * 1e9
    except ValueError:
        return None


def _fetch_market_cap_yahoo(stock: dict):
    """Return Yahoo's market cap and reported currency, when available."""
    ticker = (stock.get("yahoo_ticker") or "").strip()
    if not ticker:
        try:
            from stock_search import derive_yahoo_ticker
            ticker = derive_yahoo_ticker(stock.get("ticker", ""),
                                         stock.get("exchange", "")) or ""
        except Exception:
            return None
    if not ticker:
        return None
    try:
        url = YAHOO_CHART_URL.format(ticker=urllib.parse.quote(ticker))
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=12) as r:
            meta = json.loads(r.read()).get("chart", {}).get("result", [{}])[0].get("meta", {})
        cap = _parse_market_cap(meta.get("marketCap"))
        return (cap, meta.get("currency") or stock.get("currency") or "") if cap else None
    except Exception:
        return None

def _fetch_market_cap_tradingview(stock: dict):
    """Targeted last-resort cap lookup for a watched security only."""
    prefixes = {"BME":"BME", "LSE":"LSE", "SGX":"SGX", "OMX":"OMXSTO",
                "BMV":"BMV", "TSX":"TSX", "VAN":"TSXV", "AIX":"AIX"}
    ex = (stock.get("exchange") or "").upper()
    prefix = prefixes.get(ex)
    if not prefix:
        return None
    sym = (stock.get("ticker") or "").upper()
    try:
        payload = json.dumps({"filter": [], "range": [0, 1],
            "symbols": {"tickers": [f"{prefix}:{sym}"], "query": {"types": []}},
            "columns": ["market_cap_basic"]}).encode("utf-8")
        req = urllib.request.Request("https://scanner.tradingview.com/global/scan",
            data=payload, method="POST", headers={"Content-Type":"application/json", "User-Agent":"Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=12) as r:
            rows = json.loads(r.read()).get("data") or []
        cap = _parse_market_cap((rows[0].get("d") or [None])[0]) if rows else None
        return (cap, stock.get("currency") or "") if cap else None
    except Exception:
        return None


def fetch_market_cap(stock: dict):
    """(market_cap_in_local_currency, currency) for a stock, or None.

    Prefers the per-exchange bulk list, which already carries marketCap
    alongside the price we fetch anyway — so for those exchanges this
    costs no extra request at all. Falls back to the per-stock quote
    page for exchanges without a list.
    """
    exchange = (stock.get("exchange") or "").upper()
    ticker_raw = (stock.get("ticker") or "").upper()
    ticker_lookup = _MCAP_TICKER_ALIASES.get((exchange, ticker_raw), ticker_raw)
    # Take the currency from the EXCHANGE, not the stock row. A wrong
    # per-stock currency silently turns a market cap into nonsense: Tobila
    # Systems (4441:JPX) is stored as USD, so its JPY 15.1bn cap rendered
    # as $15.10B instead of ~$94M. The exchange determines the trading
    # currency, so it's the more reliable source; the stock's own field is
    # only a fallback.
    # Currency, most trustworthy source first. The PRICE FEED is
    # empirical — it reports what the instrument actually trades in — so
    # it beats both the exchange map and the stock row. The exchange map
    # is a good second (it caught Tobila 4441:JPX, whose stock row wrongly
    # says USD and turned a JPY 15.1bn cap into "$15.10B" instead of
    # ~$94M), but it can't be trusted alone: LSE lists GBP shares AND
    # USD-denominated GDRs like Halyk Bank, which the map would force to
    # GBP.
    ccy = ""
    try:
        _row = _mcap_price_ccy(stock)
        if _row:
            ccy = _row
    except Exception:
        pass
    if not ccy:
        try:
            from stock_search import _EXCHANGE_CURRENCY
            ccy = _EXCHANGE_CURRENCY.get(exchange, "") or ""
        except Exception:
            pass
    ccy = ccy or (stock.get("currency") or "")
    # Market cap is reported in the MAJOR unit even where the price is
    # quoted in a sub-unit. stockanalysis spells this out on LSE pages:
    # "Currency is GBP - Price in GBX". Taking the price feed's GBX here
    # would divide by a 100x-too-large rate and understate every London
    # company by 100x (Lion Finance: $77M instead of $7.7B). Same for the
    # JSE's rand cents.
    ccy = {"GBX": "GBP", "GBP ": "GBP", "GBp": "GBP",
           "ZAc": "ZAR", "ZAC": "ZAR"}.get(ccy, ccy)

    if exchange == "KRX":
        mc = _fetch_market_cap_naver(stock)
        if mc:
            return mc, "KRW"
    if exchange == "UZSE":
        mc = _fetch_market_cap_stockscope(stock)
        if mc:
            return mc, "UZS"

    # KLSE: harvested from the klsescreener screener we already pull for
    # prices. stockanalysis can't serve these — its quote pages need the
    # alpha ticker (SKBSHUT), we key Malaysian stocks by numeric Bursa
    # code (7115), and its search deliberately refuses numeric codes
    # because they collide with unrelated TYO/TPE/HKG listings.
    if exchange == "KLSE":
        _fetch_klse_bulk()
        code = (stock.get("code") or ticker_raw).strip().upper()
        mc = _KLSE_MCAP_CACHE.get(code)
        if mc:
            return mc, (ccy or "MYR")

    meta = _sa_list_meta(exchange)
    if meta:
        slug, list_ccy = meta
        _fetch_sa_list_bulk(slug, list_ccy)          # populates both caches
        tk = _sa_ticker(exchange, ticker_lookup)
        mc = (_SA_MCAP_CACHE.get(slug) or {}).get(tk)
        if mc:
            return mc, (ccy or list_ccy)

    slug = _sa_slug_for(exchange)
    if slug is None and exchange not in ("NASDAQ", "NYSE", "AMEX"):
        return None
    tk = _sa_ticker(exchange, ticker_lookup)
    if not tk:
        return None
    base = (f"https://stockanalysis.com/stocks/{tk}/"
            if slug in (None, "stocks", "s")
            else f"https://stockanalysis.com/quote/{slug}/{tk}/")
    url = base + "__data.json"
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 Chrome/120 Safari/537",
            "Accept": "*/*", "Accept-Encoding": "identity", "Referer": base,
        })
        with urllib.request.urlopen(req, timeout=12) as r:
            sk = json.loads(r.read())
    except Exception as e:
        logger.debug("market cap fetch failed for %s: %s", ticker_raw, e)
        sk = None
    for node in (sk or {}).get("nodes") or []:
        if not (isinstance(node, dict) and isinstance(node.get("data"), list)):
            continue
        arr = node["data"]
        for item in arr:
            if isinstance(item, dict) and "marketCap" in item:
                v = item["marketCap"]
                v = arr[v] if isinstance(v, int) else v
                mc = _parse_market_cap(v)
                if mc:
                    return mc, ccy
    # Some quote pages (notably KRX, and occasionally newly renamed ASX
    # securities) omit marketCap from their compact Svelte data payload
    # even though the public overview renders it.  Read that visible
    # source as a narrow fallback rather than treating the absence as a
    # zero cap.
    try:
        req = urllib.request.Request(base, headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 Chrome/120 Safari/537",
            "Accept": "text/html,*/*", "Accept-Encoding": "identity",
        })
        with urllib.request.urlopen(req, timeout=12) as r:
            html = r.read().decode("utf-8", errors="replace")
        m = re.search(r"Market Cap.{0,300}?([0-9][0-9,.]*\s*[TBMK])",
                      html, re.I | re.S)
        if m:
            mc = _parse_market_cap(m.group(1).replace(" ", ""))
            if mc:
                return mc, ccy
    except Exception as e:
        logger.debug("market cap HTML fallback failed for %s: %s", ticker_raw, e)
    yahoo = _fetch_market_cap_yahoo(stock)
    if yahoo:
        return yahoo
    tv = _fetch_market_cap_tradingview(stock)
    if tv:
        return tv
    return None

def _sa_slug_for(exchange: str):
    """stockanalysis quote slug for an exchange.

    Falls back to the slug recorded in catalog_updaters._SA_LIST_CONFIG.
    The two tables drifted: 11 exchanges (Borsa Istanbul, Ho Chi Minh,
    TSX Venture, Taiwan, Tadawul, Shanghai, Shenzhen, Bucharest,
    Budapest, Prague, CSE Canada) had a bulk list configured but no
    _SA_SLUG entry, and _fetch_price_stockanalysis bails on a missing
    slug BEFORE it reaches the bulk path — so stockanalysis was never
    consulted for them at all, by either route. Reading the list config
    as a fallback keeps the two from diverging again.
    """
    ex = (exchange or "").upper()
    slug = _SA_SLUG.get(ex)
    if slug:
        return slug
    try:
        from catalog_updaters import _SA_LIST_CONFIG
        cfg = _SA_LIST_CONFIG.get(ex)
        if cfg and len(cfg) > 1 and cfg[1]:
            return cfg[1]
    except Exception:
        pass
    return None

def _fetch_price_stockanalysis(stock: dict) -> Optional[tuple]:
    """Fetch the latest price from stockanalysis.com.

    Tries the bulk exchange-list cache first (opt #2), then the
    per-stock quote page. Stockanalysis covers most of our catalog
    exchanges (the same ones in _SA_SLUG) and isn't TLS-fingerprint
    hostile — works from both Python urllib and curl.

    Returns (price, change_pct, currency) or None.
    """
    exchange = (stock.get("exchange") or "").upper()
    ticker_raw = (stock.get("ticker") or "").upper()
    slug = _sa_slug_for(exchange)
    if slug is None and exchange not in ("NASDAQ", "NYSE", "AMEX"):
        return None
    ticker = _sa_ticker(exchange, ticker_raw)
    # Bulk fast-path: if this exchange has a list page, one cached fetch
    # serves every holding on it.
    meta = _sa_list_meta(exchange)
    if meta:
        hit = _fetch_sa_list_bulk(meta[0], meta[1]).get(ticker.upper())
        if hit:
            return hit
    url = (f"https://stockanalysis.com/stocks/{ticker}/"
           if slug is None else
           f"https://stockanalysis.com/quote/{slug}/{ticker}/")

    import ssl as _ssl
    ctx = _ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = _ssl.CERT_NONE
    sa_headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 Chrome/120 Safari/537",
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Encoding": "identity",
    }

    def _fetch_html(u):
        req = urllib.request.Request(u, headers=sa_headers)
        with urllib.request.urlopen(req, timeout=10, context=ctx) as resp:
            return resp.read().decode("utf-8", errors="replace")

    html = None
    try:
        html = _fetch_html(url)
    except urllib.error.HTTPError as e:
        if e.code == 404 and slug:
            # Stock might be on a sub-board (Spotlight, First North,
            # Catalist, etc.). Use SA's search API to resolve the
            # real slug+ticker pair, then retry.
            name = (stock.get("name") or "").strip()
            for q in (name, ticker_raw):
                r = _sa_search_resolve(q, prefer_exchange=slug)
                if not r:
                    continue
                r_slug, r_tk = r
                retry_url = (f"https://stockanalysis.com/quote/{r_slug}/"
                             f"{urllib.parse.quote(r_tk)}/")
                try:
                    html = _fetch_html(retry_url)
                    if html is not None:
                        logger.info(
                            "stockanalysis resolved via search: %s/%s → %s/%s",
                            ticker, exchange, r_slug, r_tk)
                        break
                except Exception:
                    continue
        if html is None:
            logger.info("stockanalysis price fetch failed for %s: HTTP %d",
                        ticker, e.code)
            return None
    except Exception as e:
        logger.info("stockanalysis price fetch failed for %s: %s", ticker, e)
        return None
    if html is None:
        return None

    # Stockanalysis renders the current price in a known DOM block:
    #   <div class="text-4xl font-bold ...">3,544.00</div>
    #   <!----><div class="... text-(green|red)-vivid">+32.00 (0.91%)</div>
    # Illiquid stocks (no trade today) get class "text-light" with
    # change "0.00 (0.00%)" — no sign prefix. We accept either form.
    m_price = re.search(
        r'<div class="text-4xl[^"]*">\s*([\d,]+\.?\d*)\s*</div>',
        html,
    )
    if not m_price:
        return None
    price_str = m_price.group(1)
    try:
        price = float(price_str.replace(",", ""))
    except ValueError:
        return None
    # Look for the change block in the next ~500 chars after the price div.
    # Three observed shapes:
    #   ...text-(green|red)-vivid">+32.00 (0.91%)
    #   ...text-(green|red)-vivid">-2.50 (1.20%)
    #   ...text-light">0.00 (0.00%)            ← flat / illiquid
    tail = html[m_price.end(): m_price.end() + 800]
    m_chg = re.search(
        r'text-(green-vivid|red-vivid|light)[^"]*">\s*'
        r'([+\-]?[\d,.]+)\s*\(([+\-]?[\d.]+)%\)',
        tail,
    )
    change_pct = 0.0
    if m_chg:
        color, _change_abs, change_pct_str = m_chg.group(1, 2, 3)
        try:
            change_pct = float(change_pct_str)
        except ValueError:
            change_pct = 0.0
        if color == "red-vivid" and change_pct > 0:
            change_pct = -change_pct  # unsigned % captured; flip for red

    # Currency lives in "Currency is XXX · Price in YYY". The displayed
    # price is in the trading currency (ZAc for JSE, IDR for IDX, etc.)
    cur_match = re.search(
        r'Currency is\s+([A-Za-z]{3})\s*<!--\[-->.*?Price in\s+([A-Za-z]{3,4})',
        html, re.DOTALL,
    )
    currency = ""
    if cur_match:
        currency = cur_match.group(2)
    else:
        # simpler form when only one is present
        m2 = re.search(r'Currency is\s+([A-Za-z]{3,4})', html)
        if m2:
            currency = m2.group(1)
    return (price, round(change_pct, 2), currency)


def _fetch_price_naver(stock: dict) -> Optional[tuple]:
    """Extract today's price for a KRX stock from finance.naver.com.

    KRX is poorly served by Yahoo (frequent 429s) and stockanalysis.com
    doesn't list Korean small/mid-caps. Naver's per-stock page renders
    the price server-side and isn't TLS-fingerprint hostile, so it's a
    reliable third source for any 6-digit Korean ticker.

    Returns (price, change_pct, 'KRW') or None.
    """
    code = _naver_code_for(stock)
    if not code:
        return None
    url = f"https://finance.naver.com/item/main.naver?code={code}"
    try:
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml",
            },
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            html = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        logger.info("Naver price fetch failed for %s: %s", code, e)
        return None

    # Naver's quote header has three accessibility-text spans in order
    # within the .no_today / .no_exday block, all of class "blind":
    #   1. current price       — e.g.  226,000
    #   2. change amount       — e.g.    4,000
    #   3. change percent      — e.g.     1.80   (the % sign is in a
    #                                             separate <span class="per">)
    # Direction is encoded as <span class="ico plus">/minus/zero> next
    # to the percent span. We grab all three values and the direction
    # from the contiguous block.
    block_match = re.search(
        r'class="no_today"[^>]*>(.*?)class="no_info"',
        html, re.DOTALL,
    )
    if not block_match:
        return None
    block = block_match.group(1)
    blinds = re.findall(r'<span class="blind">\s*([\d,.]+)\s*</span>', block)
    if not blinds:
        return None
    try:
        price = float(blinds[0].replace(",", ""))
    except ValueError:
        return None
    change_pct = 0.0
    if len(blinds) >= 3:
        try:
            change_pct = float(blinds[2].replace(",", ""))
        except ValueError:
            change_pct = 0.0
    # Direction: look at <span class="ico plus|minus|zero"> markers
    # within the block. There may be multiple "plus"/"minus" tags (one
    # for change-amount, one for change-%) — they always agree.
    if 'class="ico minus"' in block or 'class="ico down"' in block:
        change_pct = -abs(change_pct)
    elif 'class="ico plus"' in block or 'class="ico up"' in block:
        change_pct = abs(change_pct)
    else:
        # zero / 보합 / no marker → flat
        change_pct = 0.0

    return (price, round(change_pct, 2), "KRW")


# ── SGX bulk fetcher ──
# SGX exposes an unauthenticated JSON endpoint that returns ALL ~1,200
# Singapore-listed securities in a single response with current
# prices. One call covers every SGX stock in the user's monitor at
# zero rate-limit risk. We cache the response process-wide for 5 min
# so multiple stocks in the same refresh share one fetch.
_SGX_URL = ("https://api.sgx.com/securities/v1.1?excludetypes=bonds&"
            "params=nc%2Cb%2Cbv%2Cp%2Cc%2Clt%2Cl%2Co%2Cpv%2Cs%2Cv")
_SGX_CACHE: tuple[float, dict] | None = None
_SGX_CACHE_TTL = 5 * 60
import threading as _threading
_SGX_LOCK = _threading.Lock()


def _fetch_sgx_bulk() -> dict:
    """Return {ticker → (price, change_pct, 'SGD')} from SGX's API.

    Thread-safe: when many SGX stocks refresh in parallel the first
    caller does the network fetch and subsequent callers wait on the
    lock and read the populated cache, instead of each kicking off
    their own redundant SGX hit.
    """
    import time as _t
    global _SGX_CACHE
    with _SGX_LOCK:
        now = _t.time()
        if _SGX_CACHE and now - _SGX_CACHE[0] < (_SGX_CACHE_TTL
                                                if _SGX_CACHE[1] else _BULK_EMPTY_TTL):
            return _SGX_CACHE[1]
        try:
            req = urllib.request.Request(
                _SGX_URL,
                headers={
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                                  "AppleWebKit/537.36 (KHTML, like Gecko)",
                    "Accept": "application/json",
                },
            )
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except Exception as e:
            logger.warning("SGX bulk fetch failed: %s", e)
            _SGX_CACHE = (now, {})
            return {}
        out: dict[str, tuple] = {}
        prices = (data.get("data") or {}).get("prices") or []
        for row in prices:
            nc = (row.get("nc") or "").strip().upper()
            last = row.get("lt")
            pct = row.get("p")
            if not nc or last is None:
                continue
            try:
                out[nc] = (float(last), round(float(pct or 0), 2), "SGD")
            except (ValueError, TypeError):
                continue
        logger.info("SGX bulk: cached %d quotes", len(out))
        _SGX_CACHE = (now, out)
        return out


def _fetch_price_sgx(stock: dict) -> Optional[tuple]:
    if (stock.get("exchange") or "").upper() != "SGX":
        return None
    bulk = _fetch_sgx_bulk()
    tk = (stock.get("ticker") or "").upper()
    return bulk.get(tk)


# ── KLSE Screener price fetcher (Bursa Malaysia / KLSE) ──
# klsescreener.com renders Bursa Malaysia prices server-side. URL
# pattern is /v2/stocks/view/{ticker}. Page has:
#   <span id="price" data-value="0.510">0.510</span>
#   <span id="priceDiff">0.000 (0.0%)</span>
# We extract both. No rate-limiting issues — small site, retail traffic.
# ── Google Finance unified fetcher ──
# Google Finance covers basically every exchange we care about and is
# scrapeable (no Cloudflare gate, no JS-only rendering for the price
# block). The URL is /finance/quote/{TICKER}:{GOOGLE_EXCHANGE_CODE} —
# we map our internal exchange code to Google's. Their HTML embeds
# data-last-price and data-currency-code as attributes, plus a
# previous-close field we use to compute today's change %.
#
# This is the universal "covers Italy / Tokyo / UK / Hong Kong / KLSE
# / SGX / etc." fallback that lets us delete most of the per-exchange
# scrapers. We keep the dedicated ones (SGX bulk, Naver, KLSE Screener)
# as they're typically faster (one call covers many stocks) or more
# reliable (Korean small-caps Google sometimes mis-indexes).
_GOOGLE_FINANCE_EXCHANGE = {
    "NASDAQ": "NASDAQ", "NYSE": "NYSE", "AMEX": "NYSEAMERICAN",
    "OTC":    "OTCMKTS", "PNK": "OTCMKTS",
    "TSX":    "TSE",                      # Toronto
    "LSE":    "LON",     "IOB": "LON",
    "FRA":    "FRA",     "BIT": "BIT",
    "BME":    "BME",     "WBAG":"VIE",
    "SWX":    "SWX",     "OMX": "STO",
    "HSE":    "HEL",     "OSE": "OSL",
    "CSE":    "CPH",
    "JPX":    "TYO",                      # Tokyo
    "TSE":    "TYO",                      # alias
    "HKSE":   "HKG",     "ASX": "ASX",
    "NZX":    "NZE",     "SGX": "SGX",
    "KLSE":   "KLSE",    "SET": "BKK",
    "IDX":    "IDX",     "PSE": "PSE",
    "HOSE":   "HOSE",
    "KRX":    "KRX",     "KOSPI": "KRX",
    "KOSDAQ": "KOSDAQ",
    "TWSE":   "TPE",
    "JSE":    "JSE",     "EGX": "CAI",
    "ATHEX":  "ATH",
    "WSE":    "WSE",     "BIST": "IST",
    "BMV":    "BMV",     "BCBA": "BCBA",
    "TASE":   "TLV",
}


def _fetch_price_googlefinance(stock: dict) -> Optional[tuple]:
    exchange = (stock.get("exchange") or "").upper()
    gf_ex = _GOOGLE_FINANCE_EXCHANGE.get(exchange)
    if not gf_ex:
        return None
    ticker = (stock.get("ticker") or "").strip()
    if not ticker:
        return None
    # Google strips dots in tickers — '00088K' stays as-is, but
    # '4441.T' would need stripping. We never put a Yahoo suffix into
    # `ticker` (it lives in yahoo_ticker), so direct use is correct.
    url = (f"https://www.google.com/finance/quote/"
           f"{urllib.parse.quote(ticker)}:{gf_ex}")
    try:
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "en-US,en;q=0.9",
            },
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            html = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        logger.info("Google Finance fetch failed for %s:%s: %s",
                    ticker, gf_ex, e)
        return None

    price_m = re.search(r'data-last-price="([\d.]+)"', html)
    cur_m = re.search(r'data-currency-code="([A-Z]+)"', html)
    if not price_m:
        return None
    try:
        price = float(price_m.group(1))
    except ValueError:
        return None
    currency = cur_m.group(1) if cur_m else ""
    # Previous close lives next to the "last closing price" tooltip.
    pc_m = re.search(
        r'last closing price[^<]*</div></span><div class="[^"]+">'
        r'[^\d.,-]*([-+]?[\d,.]+)',
        html,
    )
    change_pct = 0.0
    if pc_m:
        try:
            prev = float(pc_m.group(1).replace(",", ""))
            if prev > 0:
                change_pct = ((price - prev) / prev) * 100
        except ValueError:
            pass
    return (price, round(change_pct, 2), currency)


# Optimization #3 — klsescreener bulk quote table. One request to
# /v2/screener/quote_results returns every Bursa stock (code, price,
# change%) in a single HTML table, so N Malaysian holdings cost 1 fetch
# instead of N. Same thread-safe TTL-cache pattern as SGX.
_KLSE_CACHE: tuple[float, dict] | None = None
_KLSE_CACHE_TTL = 5 * 60
# Market caps harvested from the same screener HTML as the prices.
# klsescreener prints them in MILLIONS of MYR; stored here in MYR.
_KLSE_MCAP_CACHE: dict[str, float] = {}
_KLSE_LOCK = _threading.Lock()


def _fetch_klse_bulk() -> dict:
    """Return {stock_code → (price, change_pct, 'MYR')} for all of Bursa."""
    import time as _t
    global _KLSE_CACHE
    with _KLSE_LOCK:
        now = _t.time()
        if _KLSE_CACHE and now - _KLSE_CACHE[0] < (_KLSE_CACHE_TTL
                                                  if _KLSE_CACHE[1] else _BULK_EMPTY_TTL):
            return _KLSE_CACHE[1]
        try:
            req = urllib.request.Request(
                "https://www.klsescreener.com/v2/screener/quote_results",
                headers={
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                                  "Chrome/120.0.0.0 Safari/537.36",
                    "Accept": "text/html,application/xhtml+xml",
                },
            )
            with urllib.request.urlopen(req, timeout=20) as resp:
                html = resp.read().decode("utf-8", errors="replace")
        except Exception as e:
            logger.warning("klsescreener bulk fetch failed: %s", e)
            _KLSE_CACHE = (now, {})
            return {}
        out: dict[str, tuple] = {}
        # Row columns: [0]=short name, [1]=code, [2]=category, [3]=price,
        # [4]=abs change, [5]=change% ...
        for row in re.findall(r'<tr class="list">(.*?)</tr>', html, re.S):
            cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.S)
            if len(cells) < 6:
                continue
            def _clean(c):
                return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", c)).strip()
            code = _clean(cells[1])
            try:
                price = float(_clean(cells[3]).replace(",", ""))
            except ValueError:
                continue
            pct = 0.0
            mpc = re.search(r"-?\d+\.?\d*", _clean(cells[5]))
            if mpc:
                try:
                    pct = float(mpc.group(0))
                except ValueError:
                    pct = 0.0
            if code and price > 0:
                out[code.upper()] = (price, round(pct, 2), "MYR")
                # "Market Capital" column, printed in millions of MYR.
                mc_cell = None
                _m = re.search(r'title="Market Capital"[^>]*>([^<]+)<', row)
                if _m:
                    try:
                        mc_cell = float(_m.group(1).replace(",", "").strip())
                    except ValueError:
                        mc_cell = None
                if mc_cell and mc_cell > 0:
                    _KLSE_MCAP_CACHE[code.upper()] = mc_cell * 1e6
        logger.info("klsescreener bulk: cached %d quotes", len(out))
        _KLSE_CACHE = (now, out)
        return out


# ── Market-hours skip ────────────────────────────────────────────────
# UTC trading sessions per exchange, deliberately WIDE (open a bit early,
# close a bit late, DST union) so we only ever skip a fetch when the
# market is unambiguously shut. A price captured after the last close
# cannot have changed until the next open — re-fetching it is pure
# waste, and at any hour roughly half the portfolio's exchanges are
# closed. Exchanges not listed here (ambiguous codes, midnight-crossing
# sessions like ASX) are always fetched — the safe default.
_MARKET_SESSIONS_UTC: dict[str, tuple[float, float]] = {
    # Asia-Pacific
    "KRX":   (0.0, 7.0),   "JPX":  (0.0, 7.0),   "HKSE": (1.0, 8.75),
    "SGX":   (0.75, 9.5),  "KLSE": (0.75, 9.25), "IDX":  (1.75, 9.75),
    "PSE":   (1.0, 7.25),  "SET":  (2.75, 10.0), "UZSE": (4.0, 11.0),
    "HOSE":  (1.75, 8.25),  # Ho Chi Minh — 09:00-15:00 ICT (UTC+7)
    "MSE":   (1.75, 5.25),  # Mongolia — 10:00-13:00 Ulaanbaatar (UTC+8)
    # Middle East / Africa (Sun-Thu markets flagged below)
    "DFM":   (5.75, 11.25), "ADX": (5.75, 11.25), "EGX": (6.75, 12.5),
    "TASE":  (6.25, 15.75), "NSEK": (6.25, 13.25), "SEM": (4.75, 10.25),
    "NGX":   (8.25, 14.25), "JSE": (6.75, 15.75), "BRVM": (8.25, 16.25),
    "AIX":   (4.75, 12.25), "KASE": (4.75, 12.25),
    # Europe
    "LSE":   (7.75, 16.75), "OMX": (6.75, 16.25), "OSE":  (6.75, 15.75),
    "CSE":   (6.75, 16.25), "ICE": (8.75, 16.25), "WSE":  (6.75, 15.75),
    "ATHEX": (6.75, 15.75), "BIT": (6.75, 16.25), "BME":  (6.75, 16.25),
    "EUR_FR": (6.75, 16.25), "FRA": (5.75, 21.25),  # Xetra+floor late session
    "CSEC":  (6.75, 15.75), "LIT": (6.75, 14.25),
    "BIST":  (6.75, 15.25),  # Borsa Istanbul — 10:00-18:00 TRT (UTC+3)
    "RIS":   (6.75, 14.25), "TAL": (6.75, 14.25),  # Nasdaq Baltic, same session as Vilnius
    # Americas
    "NYSE":  (13.25, 21.25), "NASDAQ": (13.25, 21.25), "AMEX": (13.25, 21.25),
    "OTC":   (13.25, 21.25), "PNK": (13.25, 21.25),
    "TSX":   (13.25, 21.25), "TSXV": (13.25, 21.25),
    "B3":    (11.75, 21.75), "BMV": (13.75, 21.25), "BVL": (13.25, 21.25),
    "BVC":   (13.25, 20.75), "BCBA": (13.75, 20.75),
}
# Exchanges where stockanalysis.com lags a full day intraday: during the
# session it still serves the PREVIOUS close, so every holding shows the
# prior price and (after our own change_pct recompute) a flat 0.00%,
# which reads as if the market were dead. FT's chartapi carries the
# current session for these, so it goes first and stockanalysis stays as
# the fallback. Observed 2026-08-19 with Milan, Athens and Frankfurt all
# a day behind on SA while FT had that day's prices; Madrid is here too
# because stockanalysis doesn't cover it at all.
_FT_FIRST_EXCHANGES = {"BIT", "ATHEX", "FRA", "BME", "IDX"}


# Markets whose trading week is Sunday-Thursday.
_SUN_THU = {"DFM", "ADX", "EGX", "TASE"}


def _is_trading_day(ex: str, d, week_days: set) -> bool:
    """Weekday within the market's trading week AND not a holiday."""
    if d.weekday() not in week_days:
        return False
    try:
        import market_calendar as _mc
        if _mc.is_holiday(ex, d.date() if hasattr(d, "date") else d):
            return False
    except Exception:
        pass   # no calendar data → treat as a normal trading day
    return True


def market_is_open(exchange: str, now=None) -> bool:
    """Return whether an exchange is in its configured UTC trading session.

    This deliberately uses the same deliberately-wide session table as the
    price-refresh skip.  It is for scheduling, not a user-facing guarantee:
    when a market's hours are unknown, False is the conservative answer so it
    does not jump the queue ahead of exchanges whose live session is known.
    """
    ex = (exchange or "").upper()
    sess = _MARKET_SESSIONS_UTC.get(ex)
    if not sess:
        return False
    from datetime import datetime as _dt_open
    now = now or _dt_open.utcnow()
    days = ({6, 0, 1, 2, 3} if ex in _SUN_THU else {0, 1, 2, 3, 4})
    if not _is_trading_day(ex, now, days):
        return False
    hour_frac = now.hour + now.minute / 60.0
    return sess[0] <= hour_frac < sess[1]


def market_closed_and_current(exchange: str, fetched_at_utc) -> bool:
    """True iff `exchange` is closed right now AND `fetched_at_utc`
    (datetime) falls after the most recent close — i.e. the stored price
    already reflects the last completed session and cannot have moved.

    Unknown exchanges and missing timestamps return False (always fetch).
    Trading holidays come from market_calendar (the same table that
    drives the dashboard's open/closed badge), including days-in-lieu:
    without that, a market shut for a public holiday looked like a
    normal session, so we re-fetched it all day and stored flat 0.00%
    rows that read as a bug."""
    ex = (exchange or "").upper()
    # ASX trades across UTC midnight, which the simple same-day UTC session
    # table below intentionally cannot represent.  Its local weekend is
    # unambiguous, though, and is the important case here: never replace the
    # final Friday move with a synthetic Saturday/Sunday 0.00% quote.
    if ex == "ASX":
        try:
            from datetime import datetime as _dt_asx
            from zoneinfo import ZoneInfo as _ZoneInfo
            if _dt_asx.now(_ZoneInfo("Australia/Sydney")).weekday() >= 5:
                return True
        except Exception:
            pass
    sess = _MARKET_SESSIONS_UTC.get(ex)
    if not sess or fetched_at_utc is None:
        return False
    open_h, close_h = sess
    days = ({6, 0, 1, 2, 3} if ex in _SUN_THU else {0, 1, 2, 3, 4})
    from datetime import datetime as _dt_mh, timedelta as _td_mh
    now = _dt_mh.utcnow()
    # Weekend/holiday quotes are a vendor's replay of the last close, not
    # a new market observation.  Even if our final trading-day refresh was
    # intraday, leave that row intact rather than manufacture a calendar-day
    # 0.00% row while the exchange is closed.
    if not _is_trading_day(ex, now, days):
        return True
    if market_is_open(ex, now):
        return False   # market genuinely open — must fetch
    # Walk back to the most recent completed close, skipping weekends
    # AND holidays (a holiday has no close of its own).
    d = now
    for _ in range(12):
        close_dt = d.replace(hour=int(close_h),
                             minute=int(round((close_h % 1) * 60)),
                             second=0, microsecond=0)
        if _is_trading_day(ex, d, days) and close_dt <= now:
            return fetched_at_utc >= close_dt
        d -= _td_mh(days=1)
    return False


def refresh_market_caps(db, stocks, max_age_days: int = 7) -> int:
    """Top up stored market caps. Returns how many were written.

    Only refetches entries older than `max_age_days` — market cap moves
    with the price but not enough to be worth a request on every refresh,
    and for the exchanges with a bulk list it rides along on a payload we
    already pull, so the incremental cost is close to zero.
    """
    from datetime import datetime as _dt, timedelta as _td
    now = _dt.utcnow()
    cutoff = (now - _td(days=max_age_days)).isoformat() + "Z"
    have = {}
    try:
        for r in db.conn.execute(
                "SELECT ticker, exchange, market_cap_at FROM stock_fundamentals "
                "WHERE market_cap IS NOT NULL"):
            have[(r["ticker"], r["exchange"])] = r["market_cap_at"] or ""
    except Exception:
        return 0
    # Currency actually reported by the price feed, per stock — the most
    # reliable signal for denominating the cap.
    try:
        _MCAP_PRICE_CCY.clear()
        for r in db.conn.execute(
                """SELECT p.ticker, p.exchange, p.currency FROM price_snapshots p
                   INNER JOIN (SELECT ticker, exchange, MAX(snapshot_at) md
                               FROM price_snapshots GROUP BY ticker, exchange) l
                   ON p.ticker=l.ticker AND p.exchange=l.exchange
                   AND p.snapshot_at=l.md
                   WHERE p.currency IS NOT NULL AND p.currency <> ''"""):
            _MCAP_PRICE_CCY[(r["ticker"], r["exchange"])] = r["currency"]
    except Exception:
        pass
    todo = [s for s in stocks
            if have.get((s.get("ticker"), s.get("exchange")), "") < cutoff]
    if not todo:
        return 0
    stamp = now.isoformat() + "Z"
    written = 0
    from concurrent.futures import ThreadPoolExecutor
    def _one(s):
        try:
            return s, fetch_market_cap(s)
        except Exception:
            return s, None
    with ThreadPoolExecutor(max_workers=6) as ex:
        for s, res in ex.map(_one, todo):
            if not res:
                continue
            mc, ccy = res
            try:
                db.conn.execute(
                    """INSERT INTO stock_fundamentals
                       (ticker, exchange, market_cap, market_cap_ccy,
                        market_cap_at, updated_at)
                       VALUES (?,?,?,?,?,?)
                       ON CONFLICT(ticker, exchange) DO UPDATE SET
                         market_cap = excluded.market_cap,
                         market_cap_ccy = excluded.market_cap_ccy,
                         market_cap_at = excluded.market_cap_at""",
                    (s["ticker"], s["exchange"], mc, ccy, stamp, stamp))
                written += 1
            except Exception:
                pass
    try:
        db.conn.commit()
    except Exception:
        pass
    if written:
        logger.info("Market caps refreshed for %d stocks", written)
    return written

def prewarm_bulk_caches(stocks: list) -> None:
    """Warm the slow bulk-source caches (SGX, KLSE, per-exchange SA lists)
    CONCURRENTLY before a parallel refresh.

    Each of these is a single HTTP call that answers a whole exchange at
    once, but they're 15-22s cold (klsescreener served 1148 quotes in
    ~22s on a slow day). Left to warm lazily inside the refresh, the
    first worker that touches each exchange stalls for that full fetch
    mid-run, and different exchanges' fetches serialize behind whichever
    worker happens to trigger them. Warming them all up front, in
    parallel, overlaps every cold fetch with each other and with the
    rest of the run. Safe to call repeatedly and safe if it fails —
    each underlying fetch is cache+lock guarded, so an already-warm
    cache returns instantly and a failure just means the normal lazy
    path runs during the refresh as before.
    """
    exch = {(s.get("exchange") or "").upper() for s in (stocks or [])}
    exch.discard("")
    tasks = []
    if "SGX" in exch:
        tasks.append(_fetch_sgx_bulk)
    if "KLSE" in exch:
        tasks.append(_fetch_klse_bulk)
    seen_slugs = set()
    for e in exch:
        meta = _sa_list_meta(e)
        if meta and meta[0] not in seen_slugs:
            seen_slugs.add(meta[0])
            slug, ccy = meta
            tasks.append(lambda slug=slug, ccy=ccy: _fetch_sa_list_bulk(slug, ccy))
    if not tasks:
        return
    # HARD overall budget. urlopen(timeout=) only bounds socket
    # INACTIVITY, not total transfer — a server that trickles bytes (or a
    # connection wedged by a middlebox) can hang a read indefinitely, and
    # a hung prewarm used to freeze the entire refresh at 0/N before a
    # single stock was fetched. Never block the refresh on this: prewarm
    # is an optimisation, so wait at most `budget` seconds, then proceed
    # regardless. Stragglers keep running as daemon threads and populate
    # their cache whenever they land (or never) — the per-stock path
    # falls back to its own lazy fetch either way.
    from concurrent.futures import ThreadPoolExecutor, wait
    budget = 25.0
    ex = ThreadPoolExecutor(max_workers=min(len(tasks), 8),
                            thread_name_prefix="prewarm")
    try:
        futures = [ex.submit(f) for f in tasks]
        done, pending = wait(futures, timeout=budget)
        if pending:
            logger.warning(
                "prewarm_bulk_caches: %d/%d bulk source(s) still running "
                "after %.0fs — continuing without them",
                len(pending), len(futures), budget)
    except Exception as e:
        logger.info("prewarm_bulk_caches: %s", e)
    finally:
        # Do NOT block on shutdown — that would reintroduce the stall.
        ex.shutdown(wait=False)


def _fetch_price_klsescreener(stock: dict) -> Optional[tuple]:
    if (stock.get("exchange") or "").upper() != "KLSE":
        return None
    # Match by the numeric Bursa code (our KLSE tickers ARE the code).
    code = (stock.get("code") or stock.get("ticker") or "").strip().upper()
    if not code:
        return None
    hit = _fetch_klse_bulk().get(code)
    if hit:
        return hit
    # Fallback: per-stock page (sub-board / newly listed not yet in bulk).
    url = f"https://www.klsescreener.com/v2/stocks/view/{urllib.parse.quote(code)}"
    try:
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml",
            },
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            html = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        logger.info("klsescreener fetch failed for %s: %s", code, e)
        return None

    m_price = re.search(
        r'<span\s+id="price"\s+data-value="([\d.]+)"', html)
    if not m_price:
        return None
    try:
        price = float(m_price.group(1))
    except ValueError:
        return None
    change_pct = 0.0
    m_diff = re.search(
        r'id="priceDiff"[^>]*>\s*([-+]?\d+\.?\d*)\s*\(([-+]?\d+\.?\d*)%\)',
        html)
    if m_diff:
        try:
            change_pct = float(m_diff.group(2))
        except ValueError:
            change_pct = 0.0
    return (price, round(change_pct, 2), "MYR")


# ── TMX Money price fetcher (TSX / TSXV / NEO / Cboe Canada) ──
# TMX Money runs a public GraphQL endpoint at app-money.tmx.com/graphql
# that powers money.tmx.com. It returns delayed quotes for any
# Canadian listing — TSX, TSX Venture, and the NEO / Cboe Canada book —
# from a single symbol query, no API key, no auth, no throttling at
# our volume. We use it as Tier-1 for Canadian listings because Yahoo
# rate-limits hard on .V and .NE suffixes and Google Finance has
# spotty coverage for the TSXV / NEO universe.
_TMX_EXCHANGES = {"TSX", "TSXV", "TSE", "NEO", "CSE", "CNSX", "VAN", "VSE"}


def _fetch_price_tmx(stock: dict) -> Optional[tuple]:
    if (stock.get("exchange") or "").upper() not in _TMX_EXCHANGES:
        return None
    ticker = (stock.get("ticker") or "").strip()
    if not ticker:
        return None
    query = (
        "query getQuoteBySymbol($symbol: String, $locale: String) { "
        "getQuoteBySymbol(symbol: $symbol, locale: $locale) { "
        "symbol price priceChange percentChange currency prevClose "
        "exchangeName } }"
    )
    body = json.dumps({
        "operationName": "getQuoteBySymbol",
        "variables": {"symbol": ticker, "locale": "en"},
        "query": query,
    }).encode("utf-8")
    try:
        req = urllib.request.Request(
            "https://app-money.tmx.com/graphql",
            data=body,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Origin": "https://money.tmx.com",
                "Referer": "https://money.tmx.com/",
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/124.0.0.0 Safari/537.36",
            },
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
    except Exception as e:
        logger.info("TMX fetch failed for %s: %s", ticker, e)
        return None
    q = (data or {}).get("data", {}).get("getQuoteBySymbol") or {}
    try:
        price = float(q.get("price"))
    except (TypeError, ValueError):
        return None
    if price <= 0:
        return None
    try:
        change_pct = float(q.get("percentChange") or 0.0)
    except (TypeError, ValueError):
        change_pct = 0.0
    currency = (q.get("currency") or "CAD").upper()
    return (price, round(change_pct, 2), currency)


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

    # Look up successor symbols without changing the stored/displayed stock.
    # Bioxyne's ASX ticker changed from BXN to BLS; retaining BXN in the
    # database keeps portfolio history and the visible Bioxyne label intact.
    _quote_alias = _PRICE_TICKER_ALIASES.get(
        ((exchange or "").upper(), (ticker or "").upper()))
    if _quote_alias:
        stock = dict(stock)
        stock["ticker"] = _quote_alias
        if (exchange or "").upper() == "ASX":
            stock["yahoo_ticker"] = f"{_quote_alias}.AX"
        logger.info("PRICE symbol alias: %s/%s → %s", ticker, exchange, _quote_alias)

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

    # AFX Kwayisi family (Kenya NSEK, Ghana GSE, Botswana BWSE,
    # Zambia LUSE, Uganda USE) — one HTTP call per exchange gives the
    # whole table. The /slug/ is exchange-specific.
    _AFX_SLUG = {"NSEK": "nse", "GSE": "gse", "BWSE": "bse",
                 "LUSE": "luse", "USE": "use", "ZWZSE": "zse"}
    if exchange in _AFX_SLUG:
        slug = _AFX_SLUG[exchange]
        logger.info("PRICE scrape fallback: %s → afx.kwayisi.org/%s (table)",
                    ticker, slug)
        table = _afx_shares_table(slug)
        if table and ticker in table:
            price, chg = table[ticker]
            currency = stock.get("currency", "")
            logger.info("  → %s %s: %s %s (%+.2f%%)",
                         exchange, ticker, currency, f"{price:,.2f}", chg)
            return (price, chg, currency)
        return None

    # Ecuador BVG — whole equity board on one page, keyed by issuer
    # name (no tickers exist). Match the catalog-stored name.
    if exchange == "BVG":
        logger.info("PRICE scrape fallback: %s → bolsadevaloresguayaquil.com (table)",
                    ticker)
        table = _bvg_shares_table()
        key = _bvg_norm_name(stock.get("name", ""))
        if table and key in table:
            price = table[key]
            currency = stock.get("currency", "USD")
            logger.info("  → BVG %s: %s %.2f", ticker, currency, price)
            # Page carries no intraday change; report 0.0%.
            return (price, 0.0, currency)
        return None

    # Kazakhstan AIX — market-watch JSON API, keyed by secCode. The
    # per-line currency comes from the feed (KZT / USD / CNY) and
    # overrides the catalog default for dual-listed lines.
    if exchange == "AIX":
        logger.info("PRICE scrape fallback: %s → market-backend.aixkz.com (table)",
                    ticker)
        table = _aix_shares_table()
        rec = table.get(ticker.upper()) if table else None
        if rec:
            price, pct, currency = rec
            logger.info("  → AIX %s: %s %.4f (%+.2f%%)",
                        ticker, currency, price, pct)
            return (price, pct, currency)
        return None

    # Tanzania DSE — clean JSON API at dse.co.tz
    if exchange == "DSET":
        logger.info("PRICE scrape fallback: %s → dse.co.tz API (table)", ticker)
        table = _dset_shares_table()
        if table and ticker in table:
            price, chg = table[ticker]
            currency = stock.get("currency", "TZS")
            logger.info("  → DSET %s: %s %s (%+.2f%%)",
                         ticker, currency, f"{price:,.2f}", chg)
            return (price, chg, currency)
        return None

    # Bangladesh DSEB — dsebd.org scroll page, one call for all tickers
    if exchange == "DSEB":
        logger.info("PRICE scrape fallback: %s → dsebd.org (table)", ticker)
        table = _dseb_shares_table()
        if table and ticker in table:
            price, chg = table[ticker]
            currency = stock.get("currency", "BDT")
            logger.info("  → DSEB %s: %s %s (%+.2f%%)",
                         ticker, currency, f"{price:,.2f}", chg)
            return (price, chg, currency)
        return None

    # Pakistan PSX — dps.psx.com.pk/market-watch, one call for all
    if exchange == "PSX":
        logger.info("PRICE scrape fallback: %s → dps.psx.com.pk (table)", ticker)
        table = _psx_shares_table()
        if table and ticker in table:
            price, chg = table[ticker]
            currency = stock.get("currency", "PKR")
            logger.info("  → PSX %s: %s %s (%+.2f%%)",
                         ticker, currency, f"{price:,.2f}", chg)
            return (price, chg, currency)
        return None

    # Croatia ZSE — the main listings page at zse.hr/default.aspx?id=26474
    # contains ticker + ISIN + name + sector + shares + last price in
    # a single table. One HTTP call per 5 minutes.
    if exchange == "ZSE":
        logger.info("PRICE scrape fallback: %s → zse.hr (table)", ticker)
        table = _zse_shares_table()
        if table and ticker in table:
            price, chg = table[ticker]
            currency = stock.get("currency", "EUR")
            logger.info("  → ZSE %s: %s %s (%+.2f%%)",
                         ticker, currency, f"{price:,.2f}", chg)
            return (price, chg, currency)
        return None

    # Rwanda RSE — home-page ticker ribbon carries the whole catalog
    # (~10 names) in `<b>TICKER</b> PRICE RWF` format.
    if exchange == "RSE":
        logger.info("PRICE scrape fallback: %s → rse.rw (ribbon)", ticker)
        table = _rse_shares_table()
        if table and ticker in table:
            price, chg = table[ticker]
            currency = stock.get("currency", "RWF")
            logger.info("  → RSE %s: %s %s (%+.2f%%)",
                         ticker, currency, f"{price:,.2f}", chg)
            return (price, chg, currency)
        return None

    # Mauritius SEM — the Official Market trading-quotes page has every
    # listed equity's live price keyed by company name. We combine it
    # with the interactive-charting <select> (ticker → name) to look up
    # a ticker's price in one pair of HTTP calls per 5 minutes.
    if exchange == "SEM":
        logger.info("PRICE scrape fallback: %s → stockexchangeofmauritius.com (table)", ticker)
        table = _sem_shares_table()
        if table and ticker in table:
            price, chg = table[ticker]
            currency = stock.get("currency", "MUR")
            logger.info("  → SEM %s: %s %s (%+.2f%%)",
                         ticker, currency, f"{price:,.2f}", chg)
            return (price, chg, currency)
        return None

    # Iraq ISX — marketPerformance.html?currLanguage=en carries a table
    # of every ticker that traded today (Close, Change, Change%, ...).
    if exchange == "ISX":
        logger.info("PRICE scrape fallback: %s → isx-iq.net (table)", ticker)
        table = _isx_shares_table()
        if table and ticker in table:
            price, chg = table[ticker]
            currency = stock.get("currency", "IQD")
            logger.info("  → ISX %s: %s %s (%+.2f%%)",
                         ticker, currency, f"{price:,.2f}", chg)
            return (price, chg, currency)
        return None

    # Sri Lanka CSE — POST /api/tradeSummary returns the full market
    # snapshot as JSON. One call per 5 minutes serves every watchlist
    # ticker on the Colombo exchange.
    if exchange == "CSEL":
        logger.info("PRICE scrape fallback: %s → cse.lk API (table)", ticker)
        table = _csel_shares_table()
        if table and ticker in table:
            price, chg = table[ticker]
            currency = stock.get("currency", "LKR")
            logger.info("  → CSEL %s: %s %s (%+.2f%%)",
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

    elif exchange == "MSE":
        # open.mse.mn detail page. The header renders the live quote as:
        #   "... EN  <Company Mongolian>  TICKER  920.96₮  -5.31002 (-0.57%)"
        # The ₮ (tögrög) sign anchors the price; the bracketed value is
        # the day change %. Tested across +/- / zero / large-cap pages.
        m = re.search(
            r"([A-Z][A-Z0-9-]{0,11})\s+"
            r"([0-9][\d,]*\.?\d*)\s*₮\s*"
            r"-?[\d.,]+\s*\(\s*(-?\d+\.?\d*)\s*%\s*\)",
            text,
        )
        if not m:
            logger.warning("  → MSE: no price match for %s", ticker)
            return None
        try:
            price = float(m.group(2).replace(",", ""))
            change = float(m.group(3))
        except ValueError:
            return None
        if price <= 0:
            return None
        logger.info("  → MSE %s: MNT %.2f (%+.2f%%)", ticker, price, change)
        return (price, change, currency or "MNT")

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


# AFX Kwayisi shares table cache — one dict per exchange slug, same
# TTL as KASE. Used by Kenya (nse), Ghana (gse), Botswana (bse),
# Zambia (luse). One HTTP call per slug per 5 minutes serves every
# watchlist stock on that exchange.
_AFX_TABLE_CACHE: dict[str, dict] = {}


def _afx_shares_table(slug: str) -> dict[str, tuple[float, float]]:
    """Return {ticker: (price, change_pct)} parsed from
    afx.kwayisi.org/<slug>/. Works for nse, gse, bse, luse. AFX's
    `change` column is an absolute currency delta — we convert to
    a percent of the current price."""
    import time as _t
    now = _t.time()
    entry = _AFX_TABLE_CACHE.get(slug)
    if entry and now - entry["ts"] < 300 and entry["data"]:
        return entry["data"]

    try:
        import ssl as _ssl
        ctx = _ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = _ssl.CERT_NONE
        req = urllib.request.Request(
            f"https://afx.kwayisi.org/{slug}/",
            headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X)"})
        with urllib.request.urlopen(req, timeout=10, context=ctx) as resp:
            html = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        logger.warning("AFX %s shares fetch failed: %s", slug, e)
        return (entry or {}).get("data", {})

    tables = re.findall(r"<table[\s\S]*?</table>", html)
    if not tables:
        return (entry or {}).get("data", {})
    big = max(tables, key=len)

    out: dict[str, tuple[float, float]] = {}
    for tr in re.split(r"<tr[^>]*>", big)[1:]:
        cells = re.split(r"<td(?:\s+[^>]*)?>", tr)
        if len(cells) < 5:
            continue
        tm = re.search(r">([A-Z][A-Z0-9]{1,10})</a>", cells[1])
        if not tm:
            continue
        ticker = tm.group(1)
        if ticker in out:
            continue
        pm = re.search(r"([\d,]+\.\d+)", cells[4])
        if not pm:
            continue
        try:
            price = float(pm.group(1).replace(",", ""))
        except ValueError:
            continue
        chg = 0.0
        if len(cells) >= 6:
            cm = re.search(r"([+\-]?[\d,]+\.\d+)", cells[5])
            if cm:
                try:
                    chg_abs = float(cm.group(1).replace(",", ""))
                    if price:
                        chg = round(chg_abs / price * 100, 2)
                except ValueError:
                    pass
        out[ticker] = (price, chg)

    _AFX_TABLE_CACHE[slug] = {"ts": now, "data": out}
    logger.info("AFX %s shares table: parsed %d tickers", slug, len(out))
    return out


# Tanzania DSE — clean JSON API returning full market snapshot
_DSET_TABLE_CACHE: dict = {"ts": 0.0, "data": {}}


def _dset_shares_table() -> dict[str, tuple[float, float]]:
    import time as _t
    from datetime import datetime as _dt
    now = _t.time()
    if now - _DSET_TABLE_CACHE["ts"] < 300 and _DSET_TABLE_CACHE["data"]:
        return _DSET_TABLE_CACHE["data"]

    today = _dt.now().strftime("%Y-%m-%d")
    url = ("https://www.dse.co.tz/api/get/market/prices/for/range"
           f"?to_date={today}&isLastTradeTrend=1"
           "&security_code=ALL&class=EQUITY")
    try:
        import ssl as _ssl
        ctx = _ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = _ssl.CERT_NONE
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=10, context=ctx) as resp:
            data = json.loads(resp.read().decode("utf-8", errors="replace"))
    except Exception as e:
        logger.warning("Tanzania DSE fetch failed: %s", e)
        return _DSET_TABLE_CACHE["data"]

    if not isinstance(data, dict) or not data.get("success"):
        return _DSET_TABLE_CACHE["data"]

    out: dict[str, tuple[float, float]] = {}
    for row in data.get("data") or []:
        ticker = (row.get("company") or "").strip().upper()
        try:
            price = float(row.get("closing_price") or 0)
            chg = float(row.get("change") or 0)
        except (TypeError, ValueError):
            continue
        if not ticker or price <= 0:
            continue
        out[ticker] = (price, round(chg, 2))

    _DSET_TABLE_CACHE["ts"] = now
    _DSET_TABLE_CACHE["data"] = out
    logger.info("DSET shares table: parsed %d tickers", len(out))
    return out


# Ecuador BVG — Guayaquil exchange closing-price board. One page holds
# the whole equity market (issuer name | USD close). No tickers, so we
# key the cache by normalised company name; the catalog stores the same
# name, so an exact normalised match resolves the price.
_BVG_TABLE_CACHE: dict = {"ts": 0.0, "data": {}}


def _bvg_norm_name(name: str) -> str:
    """Normalise an Ecuadorean issuer name for matching: uppercase,
    strip accents, collapse non-alphanumerics."""
    import unicodedata as _ud
    s = _ud.normalize("NFKD", (name or "").upper())
    s = "".join(c for c in s if not _ud.combining(c))
    return re.sub(r"[^A-Z0-9]+", " ", s).strip()


def _bvg_shares_table() -> dict[str, float]:
    import time as _t
    now = _t.time()
    if now - _BVG_TABLE_CACHE["ts"] < 300 and _BVG_TABLE_CACHE["data"]:
        return _BVG_TABLE_CACHE["data"]

    url = ("https://www.bolsadevaloresguayaquil.com"
           "/mercados/precios-de-acciones.asp")
    try:
        import ssl as _ssl
        ctx = _ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = _ssl.CERT_NONE
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/126.0 Safari/537.36"})
        with urllib.request.urlopen(req, timeout=15, context=ctx) as resp:
            html = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        logger.warning("Ecuador BVG fetch failed: %s", e)
        return _BVG_TABLE_CACHE["data"]

    out: dict[str, float] = {}
    for tr in re.findall(r"<tr[^>]*>([\s\S]*?)</tr>", html):
        cells = re.findall(r"<t[dh][^>]*>([\s\S]*?)</t[dh]>", tr)
        if len(cells) != 2:
            continue
        name = re.sub(r"<[^>]+>", "", cells[0]).strip()
        px_raw = re.sub(r"<[^>]+>", "", cells[1]).strip().replace(",", "")
        if not name or name.upper() == "EMISOR":
            continue
        try:
            price = float(px_raw)
        except ValueError:
            continue
        if price <= 0:
            continue  # 0.00 = no trades; treat as no quote
        out[_bvg_norm_name(name)] = price

    if out:
        _BVG_TABLE_CACHE["ts"] = now
        _BVG_TABLE_CACHE["data"] = out
        logger.info("BVG shares table: parsed %d priced equities", len(out))
    return out or _BVG_TABLE_CACHE["data"]


# Kazakhstan AIX — Astana International Exchange market-watch JSON API.
# One call returns the whole equity board keyed by secCode, with a
# per-line currency (KZT / USD / CNY for dual-listings).
_AIX_TABLE_CACHE: dict = {"ts": 0.0, "data": {}}


def _aix_shares_table() -> dict[str, tuple[float, float, str]]:
    """Return {secCode: (price, pct_change, currency)} for AIX equities."""
    import time as _t
    now = _t.time()
    if now - _AIX_TABLE_CACHE["ts"] < 300 and _AIX_TABLE_CACHE["data"]:
        return _AIX_TABLE_CACHE["data"]

    url = ("https://market-backend.aixkz.com/api"
           "/table/mw-main-records?instrument=EQTY")
    try:
        import ssl as _ssl
        ctx = _ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = _ssl.CERT_NONE
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/126.0 Safari/537.36",
            "Accept": "application/json",
            "Origin": "https://market.aixkz.com",
            "Referer": "https://market.aixkz.com/"})
        with urllib.request.urlopen(req, timeout=15, context=ctx) as resp:
            data = json.loads(resp.read().decode("utf-8", errors="replace"))
    except Exception as e:
        logger.warning("Kazakhstan AIX fetch failed: %s", e)
        return _AIX_TABLE_CACHE["data"]

    if not isinstance(data, list):
        return _AIX_TABLE_CACHE["data"]

    out: dict[str, tuple[float, float, str]] = {}
    for r in data:
        code = (r.get("secCode") or "").strip().upper()
        if not code:
            continue
        # Prefer a real trade, then the official reference, then the
        # prior close — AIX is thin so most lines only have prevClose.
        raw = (r.get("lastTrade") if r.get("lastTrade") is not None
               else r.get("referencePrice") if r.get("referencePrice") is not None
               else r.get("previousClose"))
        if raw is None:
            continue
        try:
            price = float(raw)
        except (TypeError, ValueError):
            continue
        if price <= 0:
            continue
        try:
            pct = float(r.get("percentChange")) if r.get("percentChange") is not None else 0.0
        except (TypeError, ValueError):
            pct = 0.0
        currency = (r.get("currency") or "KZT").strip().upper()
        out[code] = (price, round(pct, 2), currency)

    if out:
        _AIX_TABLE_CACHE["ts"] = now
        _AIX_TABLE_CACHE["data"] = out
        logger.info("AIX shares table: parsed %d priced equities", len(out))
    return out or _AIX_TABLE_CACHE["data"]


# Bangladesh DSE — dsebd.org scroll page
_DSEB_TABLE_CACHE: dict = {"ts": 0.0, "data": {}}


def _dseb_shares_table() -> dict[str, tuple[float, float]]:
    import time as _t
    now = _t.time()
    if now - _DSEB_TABLE_CACHE["ts"] < 300 and _DSEB_TABLE_CACHE["data"]:
        return _DSEB_TABLE_CACHE["data"]

    try:
        import ssl as _ssl
        ctx = _ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = _ssl.CERT_NONE
        req = urllib.request.Request(
            "https://www.dsebd.org/latest_share_price_scroll_l.php",
            headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15, context=ctx) as resp:
            html = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        logger.warning("Bangladesh DSE fetch failed: %s", e)
        return _DSEB_TABLE_CACHE["data"]

    out: dict[str, tuple[float, float]] = {}
    for tr in re.findall(r"<tr[^>]*>([\s\S]*?)</tr>", html):
        cells = re.findall(r"<td[^>]*>([\s\S]*?)</td>", tr)
        cleaned = [re.sub(r"\s+", " ",
                           re.sub(r"<[^>]+>", " ", c)).strip()
                   for c in cells]
        if len(cleaned) < 7:
            continue
        # Columns: index, ticker, ldcp, open, high, low, ltp, close
        ticker = cleaned[1]
        if not re.match(r"^[A-Z][A-Z0-9]{1,12}$", ticker):
            continue
        try:
            ldcp = float(cleaned[2]) if cleaned[2] else 0
            ltp = float(cleaned[6]) if cleaned[6] else 0
        except ValueError:
            continue
        if ltp <= 0:
            continue
        chg = round((ltp - ldcp) / ldcp * 100, 2) if ldcp > 0 else 0.0
        out[ticker] = (ltp, chg)

    _DSEB_TABLE_CACHE["ts"] = now
    _DSEB_TABLE_CACHE["data"] = out
    logger.info("DSEB shares table: parsed %d tickers", len(out))
    return out


# Pakistan PSX — dps.psx.com.pk/market-watch
_PSX_TABLE_CACHE: dict = {"ts": 0.0, "data": {}}


def _psx_shares_table() -> dict[str, tuple[float, float]]:
    import time as _t
    now = _t.time()
    if now - _PSX_TABLE_CACHE["ts"] < 300 and _PSX_TABLE_CACHE["data"]:
        return _PSX_TABLE_CACHE["data"]

    try:
        import ssl as _ssl
        ctx = _ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = _ssl.CERT_NONE
        req = urllib.request.Request(
            "https://dps.psx.com.pk/market-watch",
            headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15, context=ctx) as resp:
            html = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        logger.warning("PSX Pakistan fetch failed: %s", e)
        return _PSX_TABLE_CACHE["data"]

    out: dict[str, tuple[float, float]] = {}
    for tr in re.findall(r"<tr[^>]*>([\s\S]*?)</tr>", html):
        cells = re.findall(r"<td[^>]*>([\s\S]*?)</td>", tr)
        cleaned = [re.sub(r"\s+", " ",
                           re.sub(r"<[^>]+>", " ", c)).strip()
                   for c in cells]
        if len(cleaned) < 6:
            continue
        ticker = cleaned[0]
        if not re.match(r"^[A-Z][A-Z0-9]{1,10}$", ticker):
            continue
        # Columns: ticker, sector, indices, LDCP, OPEN, HIGH, LOW, CURRENT, ...
        # Not all rows have the same ordering — try several indexes
        price = None
        for idx in (7, 6, 4):
            if idx < len(cleaned) and cleaned[idx]:
                try:
                    candidate = float(cleaned[idx].replace(",", ""))
                    if 0 < candidate < 1_000_000:
                        price = candidate
                        break
                except ValueError:
                    continue
        if price is None:
            continue
        try:
            ldcp = float(cleaned[3].replace(",", "")) if cleaned[3] else 0
        except ValueError:
            ldcp = 0
        chg = round((price - ldcp) / ldcp * 100, 2) if ldcp > 0 else 0.0
        out[ticker] = (price, chg)

    _PSX_TABLE_CACHE["ts"] = now
    _PSX_TABLE_CACHE["data"] = out
    logger.info("PSX shares table: parsed %d tickers", len(out))
    return out


# Croatia ZSE — zse.hr listings page
_ZSE_TABLE_CACHE: dict = {"ts": 0.0, "data": {}}


def _zse_shares_table() -> dict[str, tuple[float, float]]:
    """Parse the zse.hr primary-listings table. Column order:
    ticker | ISIN | name | sector | shares | last_price.
    Primary common shares only (ISIN[6:8] == 'RA')."""
    import time as _t
    now = _t.time()
    if now - _ZSE_TABLE_CACHE["ts"] < 300 and _ZSE_TABLE_CACHE["data"]:
        return _ZSE_TABLE_CACHE["data"]

    try:
        import ssl as _ssl
        ctx = _ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = _ssl.CERT_NONE
        req = urllib.request.Request(
            "https://zse.hr/default.aspx?id=26474",
            headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15, context=ctx) as resp:
            html = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        logger.warning("Zagreb ZSE fetch failed: %s", e)
        return _ZSE_TABLE_CACHE["data"]

    out: dict[str, tuple[float, float]] = {}
    for tr in re.findall(r"<tr[^>]*>([\s\S]*?)</tr>", html):
        cells = re.findall(r"<t[dh][^>]*>([\s\S]*?)</t[dh]>", tr)
        cleaned = [re.sub(r"\s+", " ",
                           re.sub(r"<[^>]+>", " ", c))
                   .replace("\xa0", " ").strip()
                   for c in cells]
        if len(cleaned) < 6:
            continue
        ticker = cleaned[0]
        isin = cleaned[1]
        if not re.match(r"^[A-Z][A-Z0-9-]{0,9}$", ticker):
            continue
        if not (len(isin) == 12 and isin[:2] == "HR" and isin[6:8] == "RA"):
            continue  # primary shares only
        price_cell = cleaned[5]  # "13,00 EUR" or "-"
        pm = re.search(r"([\d.]+,\d+)", price_cell)
        if not pm:
            continue
        # Croatian format: thousand = ".", decimal = ","
        try:
            price = float(pm.group(1).replace(".", "").replace(",", "."))
        except ValueError:
            continue
        if price <= 0:
            continue
        # The ZSE table doesn't include daily change — report 0%
        # until we find a second column. Users get last price, which
        # is what matters for tracking.
        out[ticker] = (price, 0.0)

    _ZSE_TABLE_CACHE["ts"] = now
    _ZSE_TABLE_CACHE["data"] = out
    logger.info("ZSE Zagreb shares table: parsed %d tickers", len(out))
    return out


# Sri Lanka CSE — POST /api/tradeSummary
_CSEL_TABLE_CACHE: dict = {"ts": 0.0, "data": {}}


def _csel_shares_table() -> dict[str, tuple[float, float]]:
    import time as _t
    now = _t.time()
    if now - _CSEL_TABLE_CACHE["ts"] < 300 and _CSEL_TABLE_CACHE["data"]:
        return _CSEL_TABLE_CACHE["data"]

    try:
        import ssl as _ssl
        ctx = _ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = _ssl.CERT_NONE
        req = urllib.request.Request(
            "https://www.cse.lk/api/tradeSummary",
            data=b"", headers={"User-Agent": "Mozilla/5.0"}, method="POST")
        with urllib.request.urlopen(req, timeout=15, context=ctx) as resp:
            data = json.loads(resp.read().decode("utf-8", errors="replace"))
    except Exception as e:
        logger.warning("Sri Lanka CSE fetch failed: %s", e)
        return _CSEL_TABLE_CACHE["data"]

    out: dict[str, tuple[float, float]] = {}
    for row in data.get("reqTradeSummery") or []:
        sym = (row.get("symbol") or "").strip()
        try:
            price = float(row.get("price") or 0)
            chg = float(row.get("percentageChange") or 0)
        except (TypeError, ValueError):
            continue
        if not sym or price <= 0:
            continue
        out[sym] = (price, round(chg, 2))

    _CSEL_TABLE_CACHE["ts"] = now
    _CSEL_TABLE_CACHE["data"] = out
    logger.info("CSEL shares table: parsed %d tickers", len(out))
    return out


# Rwanda RSE shares table — one call to rse.rw home page returns a
# ticker ribbon in the shape `<b>TICKER</b> N RWF`. The exchange lists
# ~10 equities so the overhead is negligible.
_RSE_TABLE_CACHE: dict = {"ts": 0.0, "data": {}}
_RSE_KNOWN = {"BOK","BLR","CMR","IMR","RHB","MTNR","EQTY","KCB","NMG","USL"}


def _rse_shares_table() -> dict[str, tuple[float, float]]:
    import time as _t
    now = _t.time()
    if now - _RSE_TABLE_CACHE["ts"] < 300 and _RSE_TABLE_CACHE["data"]:
        return _RSE_TABLE_CACHE["data"]

    try:
        import ssl as _ssl
        ctx = _ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = _ssl.CERT_NONE
        req = urllib.request.Request(
            "https://rse.rw/",
            headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15, context=ctx) as resp:
            html = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        logger.warning("Rwanda RSE fetch failed: %s", e)
        return _RSE_TABLE_CACHE["data"]

    out: dict[str, tuple[float, float]] = {}
    # Find each <b>TICKER</b> PRICE RWF anchor, then look in the next
    # ~800 chars for a signed percentage to capture the daily change.
    anchors = list(re.finditer(
        r"<b>([A-Z]{2,6})</b>\s*(\d+(?:[.,]\d+)?)\s*RWF", html))
    for i, m in enumerate(anchors):
        ticker = m.group(1)
        if ticker not in _RSE_KNOWN:
            continue  # skip FX labels
        try:
            price = float(m.group(2).replace(",", ""))
        except ValueError:
            continue
        tail_end = anchors[i + 1].start() if i + 1 < len(anchors) else len(html)
        tail = html[m.end(): min(tail_end, m.end() + 800)]
        # rse.rw renders change as `(0.00)` (absolute RWF delta) plus
        # a CSS class on the sibling span: text-warning = unchanged,
        # text-success = up, text-danger = down. We derive the signed
        # percentage from the absolute delta over the current price.
        chg_pct = 0.0
        cm = re.search(r"\(([-+]?\d+(?:\.\d+)?)\)", tail)
        if cm:
            try:
                delta = float(cm.group(1))
            except ValueError:
                delta = 0.0
            if "text-danger" in tail:
                delta = -abs(delta)
            elif "text-warning" in tail:
                delta = 0.0
            if price > 0:
                chg_pct = round(delta / price * 100, 2)
        if price > 0:
            out[ticker] = (price, chg_pct)

    _RSE_TABLE_CACHE["ts"] = now
    _RSE_TABLE_CACHE["data"] = out
    logger.info("RSE shares table: parsed %d tickers", len(out))
    return out


# Mauritius SEM shares table. Two HTTP calls per 5 minutes:
#   1) interactive-charting → {ticker_prefix: company_name}
#   2) trading-quotes/official → {company_name: (price, chg)}
# Combined to {ticker_prefix: (price, chg)}.
_SEM_TABLE_CACHE: dict = {"ts": 0.0, "data": {}}
_SEM_BASE = "https://www.stockexchangeofmauritius.com"


def _sem_name_to_ticker() -> dict[str, str]:
    try:
        import ssl as _ssl
        ctx = _ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = _ssl.CERT_NONE
        req = urllib.request.Request(
            _SEM_BASE + "/products-market-data/equities-board/interactive-charting",
            headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15, context=ctx) as resp:
            html = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        logger.warning("SEM interactive-charting fetch failed: %s", e)
        return {}

    opts = re.findall(
        r'<option[^>]*value="([^"]+)"[^>]*>([^<]+)</option>', html)
    mapping: dict[str, str] = {}
    for code, name in opts:
        m = re.match(r"^([A-Z]{2,5})\.[A-Z]\d{4}$", code.strip())
        if not m:
            continue
        n = re.sub(r"\s+", " ", name).strip().upper()
        # Normalise common suffixes so trading-quotes text matches
        mapping[n] = m.group(1)
    return mapping


def _sem_shares_table() -> dict[str, tuple[float, float]]:
    import time as _t
    now = _t.time()
    if now - _SEM_TABLE_CACHE["ts"] < 300 and _SEM_TABLE_CACHE["data"]:
        return _SEM_TABLE_CACHE["data"]

    name_to_ticker = _sem_name_to_ticker()
    if not name_to_ticker:
        return _SEM_TABLE_CACHE["data"]

    try:
        import ssl as _ssl
        ctx = _ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = _ssl.CERT_NONE
        req = urllib.request.Request(
            _SEM_BASE + "/products-market-data/equities-board/trading-quotes/official",
            headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15, context=ctx) as resp:
            html = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        logger.warning("SEM trading-quotes fetch failed: %s", e)
        return _SEM_TABLE_CACHE["data"]

    # Flatten table rows; the trading-quotes page has one row per
    # equity with the company name in the second cell and a run of
    # prices afterwards. The first numeric value is the previous
    # close and the second is today's close — we use the latter.
    out: dict[str, tuple[float, float]] = {}
    tables = re.findall(r"<table[\s\S]*?</table>", html)
    if not tables:
        _SEM_TABLE_CACHE["ts"] = now
        return out
    big = max(tables, key=len)
    rows = re.findall(r"<tr[^>]*>([\s\S]*?)</tr>", big)
    for r in rows:
        cells = re.findall(r"<t[dh][^>]*>([\s\S]*?)</t[dh]>", r)
        if len(cells) < 5:
            continue
        cleaned = [re.sub(r"\s+", " ",
                          re.sub(r"<[^>]+>", "", c)).strip()
                   for c in cells]
        name = ""
        for c in cleaned:
            if re.match(r"^[A-Z][A-Z0-9 \(\)\&\-'\./]*[A-Z0-9\)]$", c):
                name = c
                break
        if not name:
            continue
        ticker = name_to_ticker.get(name.upper())
        if not ticker:
            # Loose contains match — some rows drop "LIMITED"/"LTD"
            for n, t in name_to_ticker.items():
                if n.startswith(name.upper()) or name.upper().startswith(n):
                    ticker = t
                    break
        if not ticker:
            continue
        # Extract first two decimal numbers as (prev_close, close)
        prices = re.findall(r"\b\d+(?:\.\d+)\b", " ".join(cleaned))
        if len(prices) < 2:
            continue
        try:
            prev = float(prices[0])
            last = float(prices[1])
        except ValueError:
            continue
        if last <= 0:
            continue
        chg = round((last - prev) / prev * 100, 2) if prev > 0 else 0.0
        out[ticker] = (last, chg)

    _SEM_TABLE_CACHE["ts"] = now
    _SEM_TABLE_CACHE["data"] = out
    logger.info("SEM shares table: parsed %d tickers", len(out))
    return out


# Iraq ISX shares table — marketPerformance.html?currLanguage=en
# exposes a table with [Symbol, Company, Close, Open, High, Low,
# Change, Change%, Volume, Traded Shares, No. Trades]. Only tickers
# that traded on the current session appear.
_ISX_TABLE_CACHE: dict = {"ts": 0.0, "data": {}}


def _isx_shares_table() -> dict[str, tuple[float, float]]:
    import time as _t
    now = _t.time()
    if now - _ISX_TABLE_CACHE["ts"] < 300 and _ISX_TABLE_CACHE["data"]:
        return _ISX_TABLE_CACHE["data"]

    try:
        import ssl as _ssl
        ctx = _ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = _ssl.CERT_NONE
        req = urllib.request.Request(
            "http://www.isx-iq.net/isxportal/portal/"
            "marketPerformance.html?currLanguage=en",
            headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15, context=ctx) as resp:
            html = resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        logger.warning("Iraq ISX fetch failed: %s", e)
        return _ISX_TABLE_CACHE["data"]

    out: dict[str, tuple[float, float]] = {}
    # The ISX table is RTL: the HTML cell order runs
    # [0]=No.Trades, [1]=TradedShares, [2]=Volume, [3]=Change%,
    # [4]=Change, [5]=Low, [6]=High, [7]=Open, [8]=Close,
    # [9]=Company, [10]=Symbol — i.e. the visual "Symbol Company
    # Close ..." header is flipped across the 11 cells.
    rows = re.findall(r"<tr[^>]*>([\s\S]*?)</tr>", html)
    for r in rows:
        cells = re.findall(r"<t[dh][^>]*>([\s\S]*?)</t[dh]>", r)
        if len(cells) != 11:
            continue
        cleaned = [re.sub(r"\s+", " ",
                          re.sub(r"<[^>]+>", "", c)).strip()
                   for c in cells]
        sym = cleaned[10]
        if not re.match(r"^[A-Z]{3,6}$", sym):
            continue
        close_raw = cleaned[8].replace(",", "")
        chg_raw = cleaned[3].rstrip("%").replace(",", "")
        try:
            close = float(close_raw)
            chg = float(chg_raw) if chg_raw not in ("", "-") else 0.0
        except ValueError:
            continue
        if close > 0:
            out[sym] = (close, round(chg, 2))

    _ISX_TABLE_CACHE["ts"] = now
    _ISX_TABLE_CACHE["data"] = out
    logger.info("ISX shares table: parsed %d tickers", len(out))
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


def fetch_prices(stock: dict, db: Database, config: dict,
                 bulk: bool = False) -> bool:
    """
    Fetch current price for a stock.
    Strategy: try Yahoo Finance first (if yahoo_ticker is set),
    then fall back to exchange-specific scraping.
    Returns True if a price was stored.

    `bulk=True` is set by the parallel "refresh all" path: it trims
    Yahoo's retry/backoff budget (opt #5) so one throttled stock can't
    stall a worker for 15 s when other sources already cover it.
    """
    ticker = stock["ticker"]
    exchange = stock["exchange"]

    # Look up successor symbols without changing the stored/displayed stock.
    # Bioxyne's ASX ticker changed from BXN to BLS; retaining BXN in the
    # database keeps portfolio history and the visible Bioxyne label intact.
    _quote_alias = _PRICE_TICKER_ALIASES.get(
        ((exchange or "").upper(), (ticker or "").upper()))
    if _quote_alias:
        stock = dict(stock)
        stock["ticker"] = _quote_alias
        if (exchange or "").upper() == "ASX":
            stock["yahoo_ticker"] = f"{_quote_alias}.AX"
        logger.info("PRICE symbol alias: %s/%s → %s", ticker, exchange, _quote_alias)

    # Do not create a new calendar-day row while the exchange is shut.
    # A quote vendor quite reasonably returns Friday's last close over a
    # weekend, but storing that as (Sunday, Friday's price, 0.00%) replaces
    # the useful Friday move in the dashboard with a synthetic flat move.
    # This guard must live here (rather than only in the dashboard's
    # background refresher) because scheduled refreshes call fetch_prices()
    # directly too.
    try:
        _latest = db.get_latest_price(ticker, exchange)
        _fetched = (_latest or {}).get("fetched_at")
        if _fetched:
            from datetime import datetime as _dt_closed
            _fetched_dt = _dt_closed.fromisoformat(
                str(_fetched).replace("Z", "+00:00")
            ).replace(tzinfo=None)
            if market_closed_and_current(exchange, _fetched_dt):
                logger.info("PRICE skip: %s/%s is closed; retaining last trading move",
                            ticker, exchange)
                return True
    except Exception:
        pass  # a price refresh should never fail because this optimisation did
    yahoo_ticker = stock.get("yahoo_ticker", "")
    # Auto-derive a Yahoo ticker from the exchange code when the catalog
    # / autocomplete didn't supply one (common for stockanalysis.com
    # list-page entries like ART/JSE that have no Yahoo metadata).
    if not yahoo_ticker:
        try:
            from stock_search import derive_yahoo_ticker
            yahoo_ticker = derive_yahoo_ticker(ticker, exchange)
            if yahoo_ticker:
                logger.info("PRICE Yahoo auto-derived: %s/%s → %s",
                            ticker, exchange, yahoo_ticker)
        except Exception:
            pass

    result = None
    source_url = ""

    # Source priority is ordered by RELIABILITY × SPEED, not historical
    # accident. Yahoo is moved last because every 429 burns ~15 sec on
    # the retry-with-backoff path; reliable free sources go first so
    # most stocks succeed in 1-2 sec without ever touching Yahoo.
    ex_upper = exchange.upper()

    # Per-stock deadline (bulk refreshes only). On a bad-source day one
    # stock can waterfall through 4-5 throttled tiers and pin a worker
    # for 20-40s; a handful of those stretches the whole refresh. Once
    # ~12s is spent, stop cascading — whatever tier is mid-flight may
    # still finish, but no NEW tier is attempted. A missed stock stays
    # on its last price and the background self-heal retries it later
    # (solo, without the deadline). Interactive single-stock fetches
    # (bulk=False) are never cut short.
    import time as _t_dl
    _dl_start = _t_dl.time()
    _deadline = (_dl_start + 12.0) if bulk else None

    def _time_left() -> bool:
        if _deadline is not None and _t_dl.time() >= _deadline:
            logger.info("  → %s: per-stock deadline hit after %.1fs, "
                        "skipping remaining tiers", ticker,
                        _t_dl.time() - _dl_start)
            return False
        return True

    # Tier 1 — Per-exchange dedicated free sources. Picked because
    # they're fast (server-rendered HTML or one-shot JSON) and don't
    # throttle.
    if result is None and ex_upper == "SGX":
        logger.info("PRICE SGX bulk: %s", ticker)
        result = _fetch_price_sgx(stock)
        if result:
            source_url = "https://api.sgx.com/securities/v1.1"
    if result is None and ex_upper == "KLSE":
        logger.info("PRICE klsescreener: %s", ticker)
        result = _fetch_price_klsescreener(stock)
        if result:
            source_url = f"https://www.klsescreener.com/v2/stocks/view/{ticker}"
    if result is None and ex_upper in ("KRX", "KOSPI", "KOSDAQ"):
        logger.info("PRICE Naver Finance: %s", ticker)
        result = _fetch_price_naver(stock)
        if result:
            code = _naver_code_for(stock)
            source_url = f"https://finance.naver.com/item/main.naver?code={code}"
    if result is None and ex_upper in _TMX_EXCHANGES:
        logger.info("PRICE TMX Money: %s/%s", ticker, ex_upper)
        result = _fetch_price_tmx(stock)
        if result:
            source_url = f"https://money.tmx.com/en/quote/{ticker}"

    # Tier 1b — FT chartapi, for exchanges where stockanalysis is a day
    # behind during the session (see _FT_FIRST_EXCHANGES).
    if result is None and ex_upper in _FT_FIRST_EXCHANGES and ex_upper in _FT_EXCHANGE:
        logger.info("PRICE FT (preferred for %s): %s", ex_upper, ticker)
        result = _fetch_price_ft(stock)
        if result:
            source_url = "https://markets.ft.com/data"

    # Tier 2 — stockanalysis.com. Covers ~40 exchanges via /quote/...
    # URL pattern, plus a per-exchange bulk list cache (one HTTP call
    # serves every holding on that exchange). Tried BEFORE Google
    # Finance because under a parallel "refresh all" it's both faster
    # (~2.3s vs ~3.7s avg) and cheaper — most stocks resolve here, so
    # we skip Google's slow per-stock hit entirely. Google under 10-way
    # concurrency self-throttles; SA's bulk cache does not.
    if result is None and _time_left():
        logger.info("PRICE stockanalysis: %s", ticker)
        result = _fetch_price_stockanalysis(stock)
        if result:
            slug = _sa_slug_for(exchange) or "stocks"
            _source_ticker = stock.get("ticker") or ticker
            source_url = (f"https://stockanalysis.com/stocks/{ticker}/"
                           if slug == "stocks" else
                          f"https://stockanalysis.com/quote/{slug}/{_source_ticker}/")

    # Tier 3 — Google Finance. Universal scraper covering 30+ exchanges
    # (Tokyo / LSE / Borsa Italiana / Hong Kong / Bursa Malaysia / etc.)
    # via /finance/quote/{TICKER}:{EX_CODE}. Fallback for exchanges SA
    # doesn't cover; never 429s but is slow under load.
    if result is None and _time_left():
        gf_ex = _GOOGLE_FINANCE_EXCHANGE.get(ex_upper)
        if gf_ex:
            logger.info("PRICE Google Finance: %s:%s", ticker, gf_ex)
            result = _fetch_price_googlefinance(stock)
            if result:
                source_url = (f"https://www.google.com/finance/quote/"
                              f"{ticker}:{gf_ex}")

    # Tier 4 — Yahoo Finance. Last reliable backstop, demoted from its
    # historical first-place because Yahoo fingerprints Python's TLS
    # and 429s per-IP — every miss costs 11+ seconds on the retry path.
    # We only land here when nothing above resolved; in practice that
    # means exotic listings (LSE IOB GDRs, Israeli, etc.).
    if result is None and yahoo_ticker and _time_left():
        logger.info("PRICE Yahoo: %s → %s", ticker, yahoo_ticker)
        result = _fetch_price_yahoo(yahoo_ticker, bulk=bulk)
        if result:
            source_url = f"https://finance.yahoo.com/quote/{yahoo_ticker}"

    # Tier 5 — exchange-specific scrapes (afx.kwayisi for ZSE/Kenya/
    # Ghana, brvm.org for BRVM, uzse for UZSE, …). Fallback for the
    # truly exotic frontier listings.
    if result is None and _time_left():
        logger.info("PRICE exchange-scrape: %s", ticker)
        result = _fetch_price_scrape(stock, config)
        if result:
            source_url = stock.get("price_url", "")

    # Tier 6 — FT Markets chartapi (last daily close). Covers exchanges
    # that Yahoo rate-limits and stockanalysis doesn't serve — notably
    # BME / Madrid (e.g. Labiana LAB:MCE). Reliable, not throttled.
    if result is None and ex_upper in _FT_EXCHANGE and _time_left():
        logger.info("PRICE FT chartapi: %s/%s", ticker, ex_upper)
        result = _fetch_price_ft(stock)
        if result:
            source_url = "https://markets.ft.com/data"

    # Last resort: Serper search for "TICKER stock price" (skipped in free mode)
    if result is None and _serper_is_enabled() and _time_left():
        logger.info("PRICE Serper search fallback for %s", ticker)
        result = _fetch_price_serper(stock, config)
        if result:
            source_url = "serper search"

    # Sanity check before storing. Serper-search prices come from
    # parsing Google snippet text and have a non-trivial false-positive
    # rate (e.g. picking up the TICKER NUMBER itself as if it were a
    # price, or grabbing a year/volume figure). When the new price
    # disagrees with the last known good price by more than ±50%, drop
    # it — better to keep yesterday's number with a ⚠ stale tag than
    # to corrupt the historical chart with a 30× outlier.
    if result and source_url == "serper search":
        try:
            prev_row = db.conn.execute(
                "SELECT price FROM price_snapshots "
                "WHERE ticker = ? AND exchange = ? "
                "ORDER BY snapshot_at DESC LIMIT 1",
                (ticker, exchange)).fetchone()
            if prev_row:
                prev = float(prev_row["price"])
                new_price = float(result[0])
                if prev > 0 and (
                    new_price > prev * 1.5 or new_price < prev * 0.5
                ):
                    logger.warning(
                        "  → Rejected Serper-fallback price for %s: "
                        "%.4f differs from last known %.4f by >50%%, "
                        "treating as bad parse",
                        ticker, new_price, prev)
                    result = None
        except Exception:
            pass  # if anything goes wrong, just allow the price

    # Recompute change_pct from OUR OWN prior close rather than trusting
    # the source's self-reported figure. Illiquid names can go days
    # without a new trade — the source then keeps re-serving the exact
    # same (price, change%) tuple from the last real print indefinitely,
    # because it never resets "change" when there's nothing new to
    # compare against (e.g. BXN post-reverse-split: price flat at 1.00
    # but the source kept echoing the split day's +3.09% for a week).
    # Our own day-over-day delta is always correct relative to what we
    # actually display, so prefer it whenever we have a prior close.
    if result and ex_upper != "IDX":
        try:
            from datetime import datetime as _dt_cp
            today_iso = _dt_cp.utcnow().strftime("%Y-%m-%d")
            prior_row = db.conn.execute(
                "SELECT price FROM price_snapshots "
                "WHERE ticker = ? AND exchange = ? AND snapshot_at < ? "
                "ORDER BY snapshot_at DESC LIMIT 1",
                (ticker, exchange, today_iso)).fetchone()
            if prior_row:
                prior_price = float(prior_row["price"])
                if prior_price > 0:
                    price_now = float(result[0])
                    own_change = (price_now - prior_price) / prior_price * 100.0
                    result = (result[0], round(own_change, 2), result[2])
        except Exception:
            pass  # fall back to the source's change_pct on any error

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
# E2) HISTORICAL PRICE BACKFILL
# ---------------------------------------------------------------------------
# fetch_prices() above stores ONE row per refresh. The dashboard's
# Graph mode needs a year of daily history to draw meaningful charts
# — when a watchlist is fresh, we have at most a few weeks. The
# helpers below pull a long time-series in a single shot and bulk-
# insert it via db.insert_price(snapshot_date=...). Sources in
# preference order:
#
#   1. Yahoo v8 chart  — JSON, range=1y interval=1d, ~250 trading days.
#      Best universal coverage. Same curl-fallback as live fetcher.
#   2. Stooq CSV       — daily history, no rate limiting. Great for
#      LSE/Polish/Czech/Hungarian listings and US.
#   3. TMX GraphQL     — Canadian listings (TSX/TSXV/NEO/CSE).
#   4. Naver chart     — Korean (KRX/KOSPI/KOSDAQ).
#
# Each helper returns a list of (date_iso, close_price) tuples (no
# currency — we read it from the latest live snapshot or default).


def _backfill_yahoo(yahoo_ticker: str, days: int = 365) -> Optional[list]:
    """Pull up to `days` of daily closes from Yahoo's chart API.

    Returns list of (yyyy-mm-dd, close, currency) or None on failure.
    Uses query2 + HTTP/2 via curl — query1 + HTTP/1.1 gets 429d on
    most residential IPs for the chart endpoint.
    """
    if not yahoo_ticker:
        return None
    if _yahoo_circuit_open():
        return None
    import time as _t
    rng = "1y" if days >= 300 else ("6mo" if days >= 150 else "3mo")
    qt = urllib.parse.quote(yahoo_ticker)
    now = int(_t.time())
    p1 = now - int(days) * 86400
    # Yahoo's chart endpoint 429s aggressively and *inconsistently* per
    # IP — the same request can 429 then 200 seconds later, and the
    # throttle often differs by host (query1 vs query2) and by URL form
    # (range= vs period1/period2). So we try several variants with a
    # short backoff instead of a single shot; whichever combo isn't
    # throttled in this window wins.
    _variants = []
    for _host in ("query2", "query1"):
        base = f"https://{_host}.finance.yahoo.com/v8/finance/chart/{qt}"
        _variants.append(f"{base}?range={rng}&interval=1d")
        _variants.append(f"{base}?period1={p1}&period2={now}&interval=1d")
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/124.0.0.0 Safari/537.36",
        "Accept": "application/json,*/*",
        "Accept-Encoding": "identity",
        "Referer": "https://finance.yahoo.com/",
    }

    def _has_series(d):
        try:
            r0 = ((d.get("chart") or {}).get("result") or [None])[0]
            return bool(r0 and (r0.get("timestamp") or []))
        except Exception:
            return False

    def _try_urllib(u):
        try:
            req = urllib.request.Request(u, headers=headers)
            with urllib.request.urlopen(req, timeout=12) as r:
                return json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code != 429:
                logger.info("Yahoo backfill HTTP %d for %s", e.code, yahoo_ticker)
        except Exception as e:
            logger.debug("Yahoo backfill urllib failed for %s: %s", yahoo_ticker, e)
        return None

    def _try_curl(u):
        # Real-browser TLS + HTTP/2 bypasses Yahoo's urllib fingerprint
        # and gets 200 where urllib gets 429 on residential IPs.
        if not _CURL_BIN:
            return None
        import subprocess as _sp, tempfile as _tf
        tmp = _tf.NamedTemporaryFile(delete=False, suffix=".json")
        tmp.close()
        try:
            cmd = [_CURL_BIN, "-sL", "--http2", "--max-time", "15",
                   "--compressed", "-o", tmp.name, "-w", "%{http_code}",
                   "-A", headers["User-Agent"],
                   "-H", "Accept: " + headers["Accept"],
                   "-H", "Referer: " + headers["Referer"],
                   "-H", "Origin: https://finance.yahoo.com", u]
            code = _sp.check_output(cmd, timeout=18).decode().strip()
            if code == "200":
                with open(tmp.name, "rb") as f:
                    return json.loads(f.read())
        except Exception as e:
            logger.debug("Yahoo backfill curl failed for %s: %s", yahoo_ticker, e)
        finally:
            try: os.unlink(tmp.name)
            except Exception: pass
        return None

    data = None
    for _i, _u in enumerate(_variants):
        # Prefer curl (better 429 rate); fall back to urllib for the
        # same URL before moving to the next variant.
        d = _try_curl(_u) or _try_urllib(_u)
        if _has_series(d):
            data = d
            break
        # brief backoff so retries land in a fresh rate-limit window
        if _i < len(_variants) - 1:
            _t.sleep(0.6)
    if not data:
        logger.info("Yahoo backfill: all variants throttled/empty for %s",
                    yahoo_ticker)
        return None
    try:
        result = (data.get("chart") or {}).get("result") or []
        if not result:
            return None
        r0 = result[0]
        ts = r0.get("timestamp") or []
        closes = (((r0.get("indicators") or {}).get("quote") or [{}])[0]
                  .get("close") or [])
        currency = ((r0.get("meta") or {}).get("currency") or "").upper()
        out: list = []
        for t, c in zip(ts, closes):
            if c is None:
                continue
            d = datetime.utcfromtimestamp(int(t)).strftime("%Y-%m-%d")
            out.append((d, float(c), currency))
        return out or None
    except Exception as e:
        logger.info("Yahoo backfill parse failed for %s: %s",
                    yahoo_ticker, e)
        return None


# Some "exchanges" in our internal map represent the main board of a
# country, but a stock can be listed on a parallel sub-board that SA
# uses a different slug for. Sweden is the canonical case: BPC
# Instruments lives on Spotlight Stock Market (slug "xsat") while
# most other OMX stocks live on Nasdaq Stockholm Main (slug "sto").
# We accept any slug in the family for resolved searches.
_SA_SLUG_FAMILY = {
    "sto":  ["sto",  "xsat", "ngm",  "fns"],   # Sweden: Main + Spotlight + NGM + First North
    "cph":  ["cph",  "fnd"],                   # Denmark + First North Denmark
    "hel":  ["hel",  "fnf"],                   # Finland + First North Finland
    "osl":  ["osl",  "axx",  "mer"],           # Oslo + Euronext Growth Oslo
    "ice":  ["ice",  "fni"],                   # Iceland + First North Iceland
    "kosdaq": ["kosdaq", "krx", "kospi"],      # Korea: KOSDAQ + KOSPI + the
                                               # combined "krx" slug SA uses
                                               # for some tickers (e.g. 00088K).
}


def _sa_search_resolve(query: str, prefer_exchange: str = "") -> Optional[tuple]:
    """Use stockanalysis.com's search API to resolve a free-text query
    (ticker or company name) to an (slug, ticker) tuple.

    Slug-family-aware: when the primary slug for an exchange has
    known parallel sub-boards (Sweden's Main vs Spotlight, Norway's
    Main vs Euronext Growth, etc.), accept any slug in that family
    so a stock listed on the sub-board still resolves. Outside known
    families we still require an exact slug match — KLSE numeric
    tickers (7167) collide with unrelated TYO / TPE / HKG / BOM
    tickers and a permissive match would silently fetch the wrong
    company.
    """
    if not query:
        return None
    pref = (prefer_exchange or "").lower()
    if not pref:
        return None
    accepted = set(_SA_SLUG_FAMILY.get(pref, [pref]))
    try:
        req = urllib.request.Request(
            f"https://api.stockanalysis.com/api/search?q={urllib.parse.quote(query)}",
            headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=8) as r:
            data = json.loads(r.read())
    except Exception:
        return None
    rows = (data or {}).get("data") or []

    # Matching the exchange is NOT enough. The search also matches on
    # company NAME, so an alphabetic ticker could resolve to an unrelated
    # listing that merely shares a word: "FINAMEXO" (Casa de Bolsa
    # Finamex) resolved to GENIUS21, an Actinver "Casa De Bolsa" ETF, and
    # we stored that ETF's 97.20 as Finamex's price — under a source URL
    # built from OUR ticker, which 404s, so it looked legitimate.
    #
    # When the query is alphabetic it IS the ticker, so require the
    # resolved ticker to be prefix-related to it. A numeric query is a
    # listing code (KLSE 7115 -> SKBSHUT) which by design cannot look
    # like its ticker, so those still rely on the search — that is the
    # case this resolver exists for.
    _q = "".join(ch for ch in query.lower() if ch.isalnum())
    _q_is_code = _q.isdigit()
    # Corporate boilerplate carries no identifying information — every
    # Mexican issuer is "S.A.B. de C.V.", so matching on those words is
    # how "Casa de Bolsa Finamex" reached an Actinver "Casa De Bolsa" ETF.
    _STOP = {"sa", "sab", "cv", "de", "del", "la", "el", "plc", "ltd",
             "limited", "inc", "corp", "corporation", "company", "co",
             "berhad", "bhd", "tbk", "pt", "ab", "as", "asa", "nv", "spa",
             "ag", "jsc", "the", "and", "holdings", "holding", "group"}
    _q_tokens = {t for t in "".join(
        c if c.isalnum() else " " for c in query.lower()).split()
        if len(t) > 2 and t not in _STOP}

    def _plausible(tk: str, nm: str = "") -> bool:
        # A numeric query is a listing code (KLSE 7115 -> SKBSHUT); it
        # cannot resemble its ticker, which is the case this resolver
        # exists for.
        if _q_is_code:
            return True
        t = "".join(ch for ch in (tk or "").lower() if ch.isalnum())
        if t and _q and (t == _q or t.startswith(_q) or _q.startswith(t)):
            return True
        # Name query (used to find sub-board listings): every
        # distinguishing word must appear in the matched company's name.
        # "finamex" is absent from the ETF's name, so that match is
        # rejected, while "SKB Shutters Corporation Berhad" still
        # resolves to SKBSHUT.
        if _q_tokens and nm:
            nl = nm.lower()
            return all(tok in nl for tok in _q_tokens)
        return False

    # Prefer the primary slug; fall back to the family.
    for row in rows:
        s = (row.get("s") or "").lower()
        if "/" not in s:
            continue
        slug, tk = s.split("/", 1)
        if slug == pref and _plausible(tk, row.get("n") or ""):
            return (slug, tk)
    for row in rows:
        s = (row.get("s") or "").lower()
        if "/" not in s:
            continue
        slug, tk = s.split("/", 1)
        if slug in accepted and _plausible(tk, row.get("n") or ""):
            return (slug, tk)
    return None


def _backfill_stockanalysis(stock: dict, days: int = 365) -> Optional[list]:
    """Pull daily close history from stockanalysis.com.

    Two transport layers:
      • JSON API at /api/symbol/{slug}/{ticker}/history — works for
        US stocks (slug="s") and returns up to 1Y. Returns 1101 for
        most non-US slugs.
      • SvelteKit __data.json at /quote/{slug}/{ticker}/history/ —
        works for ~40 non-US exchanges. Capped at ~125 trading days
        (~6 months), but that's a year-class improvement over the
        13-day live history we'd otherwise have.
    """
    exchange = (stock.get("exchange") or "").upper()
    # Same slug fallback as the live fetch — without it the exchanges that
    # only appear in _SA_LIST_CONFIG get no history from stockanalysis
    # either, even though their quote pages exist.
    _slug = _sa_slug_for(exchange)
    if _slug is None and exchange not in ("NASDAQ", "NYSE", "AMEX"):
        return None
    slug = _slug or "s"   # "s" = US stocks
    ticker = _sa_ticker(exchange, stock.get("ticker") or "")
    if not ticker:
        return None
    # Browser-like headers — Cloudflare in front of stockanalysis.com
    # 429s requests that look API-flavored (Accept: application/json,
    # no Accept-Encoding). Mirror the live-price fetcher which is the
    # proven-working combination: Chrome/120, Accept: */*, identity
    # encoding, and a Referer pointing at the same stock's quote page.
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 Chrome/120 Safari/537",
        "Accept": "*/*",
        "Accept-Encoding": "identity",
        "Referer": f"https://stockanalysis.com/quote/{slug}/{urllib.parse.quote(ticker)}/",
    }

    # Path A — US-only JSON API. Returns up to 252 daily rows (1Y).
    if slug == "s":
        rng = "1Y" if days >= 300 else ("6M" if days >= 150 else "3M")
        url = (f"https://stockanalysis.com/api/symbol/s/"
               f"{urllib.parse.quote(ticker)}/history"
               f"?range={rng}&type=daily")
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=12) as r:
                data = json.loads(r.read())
        except Exception as e:
            logger.info("stockanalysis API failed for %s: %s", ticker, e)
            data = None
        if data and isinstance(data.get("data"), list):
            out: list = []
            for row in data["data"]:
                try:
                    d = (row.get("t") or "")[:10]
                    c = float(row.get("c"))
                    if d and c > 0:
                        out.append((d, c, ""))
                except (TypeError, ValueError):
                    continue
            if out:
                out.sort(key=lambda x: x[0])
                return out

    # Path B — SvelteKit __data.json for non-US listings. The data
    # is in column-stored format (refs by integer index into a flat
    # array), so we walk the array to find dict rows with both `t`
    # and `c` fields and resolve each.
    sk_url = (f"https://stockanalysis.com/quote/{slug}/"
              f"{urllib.parse.quote(ticker)}/history/__data.json?p=1Y")
    try:
        req = urllib.request.Request(sk_url, headers=headers)
        with urllib.request.urlopen(req, timeout=12) as r:
            sk = json.loads(r.read())
    except Exception as e:
        logger.info("stockanalysis SK failed for %s/%s: %s",
                    ticker, exchange, e)
        return None
    out2: list = []
    for node in (sk or {}).get("nodes") or []:
        if not isinstance(node, dict):
            continue
        arr = node.get("data")
        if not isinstance(arr, list):
            continue
        for item in arr:
            if not (isinstance(item, dict) and "t" in item and "c" in item):
                continue
            try:
                t_ref = item["t"]
                c_ref = item["c"]
                t_val = arr[t_ref] if isinstance(t_ref, int) else t_ref
                c_val = arr[c_ref] if isinstance(c_ref, int) else c_ref
                if (isinstance(t_val, str) and len(t_val) >= 10
                        and isinstance(c_val, (int, float))
                        and float(c_val) > 0):
                    out2.append((t_val[:10], float(c_val), ""))
            except (IndexError, TypeError, ValueError):
                continue
    # Dedup (the SK array often references the same row from multiple
    # places) and sort chronologically.
    if out2:
        unique = {}
        for d, c, ccy in out2:
            unique[d] = (d, c, ccy)
        out2 = sorted(unique.values(), key=lambda x: x[0])
        return out2

    # Path C — SA search to resolve numeric→alpha ticker (KLSE,
    # SGX-Catalist). Re-fetch with the resolved (slug, ticker) pair.
    # Name first, ticker second: numeric tickers collide across
    # exchanges (KLSE 7167 vs TYO 7167 vs HKG 7167 are all different
    # companies), so a company-name search is far less likely to
    # surface the wrong listing.
    name = (stock.get("name") or "").strip()
    queries = [name, stock.get("ticker") or ""]
    seen = {(slug, ticker)}
    for q in queries:
        resolved = _sa_search_resolve(q, prefer_exchange=slug)
        if not resolved or resolved in seen:
            continue
        seen.add(resolved)
        r_slug, r_ticker = resolved
        sk_url2 = (f"https://stockanalysis.com/quote/{r_slug}/"
                   f"{urllib.parse.quote(r_ticker)}/history/__data.json?p=1Y")
        try:
            req = urllib.request.Request(sk_url2, headers=headers)
            with urllib.request.urlopen(req, timeout=12) as r:
                sk2 = json.loads(r.read())
        except Exception:
            continue
        out3: list = []
        for node in (sk2 or {}).get("nodes") or []:
            if not isinstance(node, dict):
                continue
            arr2 = node.get("data")
            if not isinstance(arr2, list):
                continue
            for item in arr2:
                if not (isinstance(item, dict) and "t" in item and "c" in item):
                    continue
                try:
                    t_val = arr2[item["t"]] if isinstance(item["t"], int) else item["t"]
                    c_val = arr2[item["c"]] if isinstance(item["c"], int) else item["c"]
                    if (isinstance(t_val, str) and len(t_val) >= 10
                            and isinstance(c_val, (int, float))
                            and float(c_val) > 0):
                        out3.append((t_val[:10], float(c_val), ""))
                except (IndexError, TypeError, ValueError):
                    continue
        if out3:
            unique = {}
            for d, c, ccy in out3:
                unique[d] = (d, c, ccy)
            logger.info("  → resolved via SA search: %s/%s → %s/%s",
                        stock.get("ticker"), exchange, r_slug, r_ticker)
            return sorted(unique.values(), key=lambda x: x[0])
    return None


def _backfill_tmx(stock: dict, days: int = 365) -> Optional[list]:
    """TMX Money historical chart for Canadian listings."""
    if (stock.get("exchange") or "").upper() not in _TMX_EXCHANGES:
        return None
    ticker = (stock.get("ticker") or "").strip()
    if not ticker:
        return None
    end = datetime.utcnow().strftime("%Y-%m-%d")
    start = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
    body = json.dumps({
        "operationName": "getCompanyPriceHistory",
        "variables": {"symbol": ticker, "start": start, "end": end},
        "query": ("query getCompanyPriceHistory("
                  "$symbol: String!, $start: String, $end: String) { "
                  "getCompanyPriceHistory("
                  "symbol: $symbol, start: $start, end: $end) { "
                  "datetime closePrice } }"),
    }).encode("utf-8")
    try:
        req = urllib.request.Request(
            "https://app-money.tmx.com/graphql", data=body,
            headers={"Content-Type": "application/json",
                     "Accept": "application/json",
                     "Origin": "https://money.tmx.com",
                     "Referer": "https://money.tmx.com/",
                     "User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=12) as r:
            data = json.loads(r.read())
    except Exception as e:
        logger.info("TMX backfill failed for %s: %s", ticker, e)
        return None
    rows = (((data or {}).get("data") or {})
            .get("getCompanyPriceHistory") or [])
    out: list = []
    for r in rows:
        try:
            d = (r.get("datetime") or "")[:10]
            c = float(r.get("closePrice"))
            if d and c > 0:
                out.append((d, c, "CAD"))
        except (TypeError, ValueError):
            continue
    # API returns newest-first; sort ascending so insert is chronological
    out.sort(key=lambda x: x[0])
    return out or None


# ── FT Markets historical chart API ────────────────────────────────
# markets.ft.com publishes a JSON chart endpoint that covers most
# major international exchanges (LSE, Tokyo, Hong Kong, Bursa, B3,
# Warsaw, Athens, Lagos, JSE, TASE, Manila, Vilnius, …) and returns
# a full year of daily closes — a clean upgrade over the
# stockanalysis SvelteKit fallback which caps at ~6 months. Two-step
# protocol: search → resolve to FT's internal `xid` → POST chartapi.
_FT_EXCHANGE = {
    # Our internal exchange code → FT's exchange suffix.
    "LSE":    "LSE",
    "FRA":    "FRA",
    "BIT":    "MIL",          # Borsa Italiana
    "BME":    "MCE",          # Madrid — FT uses MCE (Mercado Continuo Español), not MAD
    "OMX":    "STO",          # Stockholm
    "HSE":    "HEX",          # Helsinki — FT uses HEX (Nokia = NOK1V:HEX), not HEL
    "OSE":    "OSL",          # Oslo
    "CSE":    "CPH",          # Copenhagen — FT uses CPH (Novo = NOVO B:CPH), not COP
    "ICEX":   "ICE",          # Reykjavik
    "SWX":    "VTX",          # SIX Swiss
    "WBAG":   "VIE",          # Vienna
    "EUR_FR": "PAR",
    "EUR_NL": "AEX",
    "EUR_BE": "BRU",
    "EUR_PT": "LIS",
    "EUR_IE": "DUB",
    "JPX":    "TYO",          # Tokyo
    "HKSE":   "HKG",          # Hong Kong
    "ASX":    "ASX",          # Sydney
    "NZX":    "NZE",
    "SGX":    "SES",          # SGX Catalist + main both via SES
    "KLSE":   "KLS",          # Bursa Malaysia
    "IDX":    "JKT",          # Jakarta
    "PSE":    "PHS",          # Manila
    "SET":    "BKK",          # Thailand
    "HOSE":   "HSX",          # HCMC
    "KRX":    "SEO",          # Korea
    "KOSPI":  "SEO",
    "KOSDAQ": "SEO",
    "TWSE":   "TAI",          # Taipei
    "JSE":    "JNB",          # Johannesburg
    "NGX":    "LAG",          # Lagos
    "EGX":    "CAI",          # Cairo
    "ATHEX":  "ATH",          # Athens
    "WSE":    "WSE",          # Warsaw
    "PSE_CZ": "PRA",          # Prague
    "BET":    "BUD",          # Budapest
    "BVB":    "BSE",          # Bucharest
    "BIST":   "IST",          # Istanbul
    "TASE":   "TLV",          # Tel Aviv
    "B3":     "SAO",          # Brazil
    "BMV":    "MEX",          # Mexico
    "BCBA":   "BUE",          # Buenos Aires
    "BVS":    "SGO",          # Santiago
    "TSX":    "TOR",
    "TSXV":   "VEN",
    "VAN":    "VEN",          # Vancouver — listed under TSXV ("VEN" on FT) since 1999
    "VSE":    "VEN",
    "LIT":    "VLX",          # Vilnius
    "RIS":    "RIX",          # Riga
    "TAL":    "TAL",          # Tallinn
    "NASDAQ": "NSQ",
    "NYSE":   "NYQ",
    "AMEX":   "ASE",
}


def _ft_name_variants(name: str) -> list:
    """Build a list of progressively-simpler search queries from a
    company name. FT search returns 0 results when any of the common
    legal suffixes is present in the wrong form (e.g. 'Berhad' vs
    FT's 'Bhd', 'Limited' vs 'Ltd'). Try the original first, then
    variants with suffixes stripped, then a short prefix.
    """
    if not name:
        return []
    out = [name]
    SUFFIXES = (
        " Berhad", " Bhd.", " Bhd",
        " Limited", " Ltd.", " Ltd",
        " Incorporated", " Inc.", " Inc",
        " Corporation", " Corp.", " Corp",
        " Public Company", " Plc.", " Plc",
        " Holdings", " Group",
        " S.A.", " SA",
        " AG", " AB", " NV", " N.V.", " GmbH",
        ", Inc.",
    )
    base = name
    for suf in SUFFIXES:
        if base.endswith(suf):
            base = base[: -len(suf)].rstrip(",. ").strip()
            if base and base not in out:
                out.append(base)
    # First two words is a robust last-ditch query.
    words = base.split()
    if len(words) >= 2:
        head2 = " ".join(words[:2])
        if head2 not in out:
            out.append(head2)
    if len(words) >= 3:
        head3 = " ".join(words[:3])
        if head3 not in out:
            out.append(head3)
    return out


def _ft_resolve_xid(stock: dict) -> Optional[tuple]:
    """Resolve an (xid, symbol) tuple via FT's search API.

    Tries `{ticker}:{ft_exchange}` first, then falls back to multiple
    name variants (full name, suffix-stripped, first-N-words).
    Filters by FT exchange so we don't match the same ticker on the
    wrong venue (KLSE 7167 vs Tokyo 7167 are entirely different
    companies — silent backfilling of the wrong stock looks like real
    data, so we refuse cross-exchange matches outright).
    """
    exchange = (stock.get("exchange") or "").upper()
    ft_ex = _FT_EXCHANGE.get(exchange)
    if not ft_ex:
        return None
    ticker = (stock.get("ticker") or "").strip()
    name   = (stock.get("name")   or "").strip()
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/124.0.0.0 Safari/537.36",
        "Accept": "application/json",
        "Referer": "https://markets.ft.com/",
    }

    def _search(q):
        try:
            url = ("https://markets.ft.com/data/searchapi/searchsecurities"
                   f"?query={urllib.parse.quote(q)}")
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=8) as r:
                return json.loads(r.read())
        except Exception:
            return None

    # Separator-agnostic ticker key. Nordic dual-class shares are written
    # "IDUN-B" by us / Yahoo, "IDUN B" (space) on FT, and "IDUN.B" (dot)
    # on stockanalysis — so an exact "IDUN-B:STO" never matches FT's
    # "IDUN B:STO". Compare with separators stripped.
    def _norm_sym(s: str) -> str:
        return (s or "").upper().replace("-", "").replace(".", "").replace(" ", "")
    target_sym = f"{ticker}:{ft_ex}".upper() if ticker else ""
    target_tk_norm = _norm_sym(ticker) if ticker else ""
    queries: list = []
    if target_sym:
        queries.append(target_sym)
    if ticker:
        queries.append(ticker)
    queries.extend(_ft_name_variants(name))
    # Collect all matches across queries, then pick the best.
    # Priority:
    #   1. Exact symbol match (TICKER:EX)
    #   2. Symbol-before-colon starts with TICKER AND on same exchange
    #      (e.g. BYMAF:BUE — related instrument, but indexed as a stock)
    # Anything else (SPBYMAIG15:BUE, an S&P/BYMA index) is rejected —
    # FT's name search will happily return indices, derivatives, and
    # unrelated funds whose tickers contain the query string.
    fallback: Optional[tuple] = None
    fallback_class: Optional[str] = None  # filter out indices / ETPs
    for query in queries:
        data = _search(query)
        if not data:
            continue
        for sec in (data.get("data", {}).get("security") or []):
            sym = (sec.get("symbol") or "").upper()
            xid = sec.get("xid")
            if not xid or ":" not in sym:
                continue
            sym_tk, sym_ex = sym.split(":", 1)
            asset_class = (sec.get("assetClass") or "").lower()
            sec_name    = (sec.get("name") or "").lower()
            # Skip indices, currencies, and bond/index-named instruments.
            if asset_class and asset_class not in ("equities", "equity", ""):
                continue
            if any(kw in sec_name for kw in (" index", "etf ", " etf",
                                             "fund ", "currency", "bond ",
                                             " bond")):
                continue
            if sym == target_sym:
                return (str(xid), sym)
            # Separator-agnostic exact match on the same exchange
            # (IDUN-B ↔ "IDUN B" ↔ IDUN.B).
            if (sym_ex == ft_ex and target_tk_norm
                    and _norm_sym(sym_tk) == target_tk_norm):
                return (str(xid), sym)
            if (sym_ex == ft_ex and target_tk_norm
                    and _norm_sym(sym_tk).startswith(target_tk_norm)
                    and fallback is None):
                fallback = (str(xid), sym)
    return fallback


def _fetch_price_ft(stock: dict) -> Optional[tuple]:
    """Live price for FT-covered exchanges via the chartapi series.

    Reuses the same chartapi/series endpoint as the history backfill,
    but only needs the last couple of closes: the most recent is the
    "current" price, and the prior one gives day-over-day change %.

    This is the only working free source for some exchanges that Yahoo
    rate-limits and stockanalysis.com doesn't cover (notably BME /
    Madrid — e.g. Labiana, ticker LAB:MCE). Returns
    (price, change_pct, currency) or None.
    """
    series = _backfill_ft(stock, days=10)
    if not series:
        return None
    # series is sorted ascending by date; take the last close as the
    # current price and the prior close for the day-over-day delta.
    last_date, last_close, currency = series[-1]
    change_pct = 0.0
    if len(series) >= 2:
        prev_close = series[-2][1]
        if prev_close and prev_close > 0:
            change_pct = (last_close - prev_close) / prev_close * 100.0
    return (last_close, round(change_pct, 2), currency or "")


def _backfill_sem(stock: dict, days: int = 365) -> Optional[list]:
    """Daily close history for a Stock Exchange of Mauritius (SEM) stock.

    SEM's live-price scrape has no history, but the Highcharts widget on
    the exchange's interactive-charting page pulls its series from
    /interactive-graph?market=official&filename=<CODE>.N0000 — a JSON
    payload of [epoch_ms, price] pairs going back years. The ticker's
    SEM security code is `<TICKER>.N0000` (ordinary shares; the .N0000
    suffix is the ISIN board segment, confirmed from the page's own
    <option value="MCBG.N0000"> entries).
    """
    if (stock.get("exchange") or "").upper() != "SEM":
        return None
    tk = (stock.get("code") or stock.get("ticker") or "").strip().upper()
    if not tk:
        return None
    # Strip any suffix the ticker might already carry, then add .N0000.
    tk = tk.split(".")[0]
    filename = f"{tk}.N0000"
    url = ("https://www.stockexchangeofmauritius.com/interactive-graph?"
           + urllib.parse.urlencode({"market": "official", "filename": filename}))
    try:
        import ssl as _ssl
        ctx = _ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = _ssl.CERT_NONE
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 Chrome/124 Safari/537.36",
            "Accept": "application/json, text/javascript, */*",
            "X-Requested-With": "XMLHttpRequest"})
        with urllib.request.urlopen(req, timeout=20, context=ctx) as r:
            data = json.loads(r.read())
    except Exception as e:
        logger.info("SEM interactive-graph failed for %s: %s", filename, e)
        return None
    pts = data.get("data") if isinstance(data, dict) else data
    if not isinstance(pts, list):
        return None
    currency = (stock.get("currency") or "MUR").strip()
    from datetime import datetime
    cutoff = (datetime.utcnow() - timedelta(days=int(days)))
    by_day: dict[str, float] = {}
    for pt in pts:
        try:
            ts_ms, px = pt[0], float(pt[1])
            d = datetime.utcfromtimestamp(ts_ms / 1000.0)
            if px > 0 and d >= cutoff:
                by_day[d.strftime("%Y-%m-%d")] = px  # last point of a day = close
        except (TypeError, ValueError, IndexError):
            continue
    out = [(d, by_day[d], currency) for d in sorted(by_day)]
    return out or None


def _backfill_aix(stock: dict, days: int = 365) -> Optional[list]:
    """Daily close history for an AIX (Astana / Kazakhstan) stock.

    AIX's market-watch API only gives the live price, but the charting
    backend has /api/symbol/chart-data which returns the full intraday
    tick stream ({x: ISO-timestamp, price}) for a date range. We
    downsample to the last trade of each day = daily close. secCode is
    the AIX ticker (CORE = USD line, CORE.K = KZT line); currency comes
    from the catalog entry.
    """
    if (stock.get("exchange") or "").upper() != "AIX":
        return None
    sec = (stock.get("code") or stock.get("ticker") or "").strip()
    if not sec:
        return None
    currency = (stock.get("currency") or "").strip()
    from datetime import datetime, timedelta
    date_to = datetime.utcnow().strftime("%Y-%m-%d")
    date_from = (datetime.utcnow() - timedelta(days=int(days))).strftime("%Y-%m-%d")
    flt = json.dumps({"secCode": sec, "dateFrom": date_from, "dateTo": date_to},
                     separators=(",", ":"))
    url = ("https://market-backend.aixkz.com/api/symbol/chart-data?"
           + urllib.parse.urlencode({"chartDataFilter": flt}))
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 Chrome/124 Safari/537.36",
            "Accept": "application/json",
            "Origin": "https://market.aixkz.com",
            "Referer": "https://market.aixkz.com/"})
        with urllib.request.urlopen(req, timeout=20) as r:
            data = json.loads(r.read())
    except Exception as e:
        logger.info("AIX chart-data failed for %s: %s", sec, e)
        return None
    if not isinstance(data, list):
        return None
    # Downsample ticks → last price per calendar day.
    by_day: dict[str, float] = {}
    for pt in data:
        try:
            d = str(pt.get("x"))[:10]
            px = float(pt.get("price"))
            if d and px > 0:
                by_day[d] = px   # later ticks overwrite → last = close
        except (TypeError, ValueError):
            continue
    out = [(d, by_day[d], currency) for d in sorted(by_day)]
    return out or None


def _backfill_ft(stock: dict, days: int = 365) -> Optional[list]:
    """Pull daily Close history for a stock from FT's chartapi/series.

    Returns a list of (yyyy-mm-dd, close, currency) or None.
    """
    resolved = _ft_resolve_xid(stock)
    if not resolved:
        return None
    xid, sym = resolved
    body = json.dumps({
        "days": max(7, min(days, 1825)),
        "dataPeriod": "Day",
        "dataInterval": 1,
        "realtime": False,
        "returnDateType": "ISO8601",
        "elements": [{"Type": "price", "Symbol": xid}],
    }).encode("utf-8")
    try:
        req = urllib.request.Request(
            "https://markets.ft.com/data/chartapi/series",
            data=body,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Origin": "https://markets.ft.com",
                "Referer": "https://markets.ft.com/",
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/124.0.0.0 Safari/537.36",
            },
        )
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read())
    except Exception as e:
        logger.info("FT backfill failed for %s: %s", sym, e)
        return None
    # FT response shape: top-level "Dates" array shared across all
    # Elements; each Element has its own ComponentSeries (Open/High/
    # Low/Close).
    dates = data.get("Dates") or []
    elements = data.get("Elements") or []
    if not elements or not dates:
        return None
    el = elements[0]
    closes = []
    for c in el.get("ComponentSeries") or []:
        if c.get("Type") == "Close":
            closes = c.get("Values") or []
            break
    if not closes or len(dates) != len(closes):
        return None
    currency = (el.get("Currency") or "").upper()
    out: list = []
    for d, c in zip(dates, closes):
        try:
            d10 = (d or "")[:10]
            cv  = float(c)
            if d10 and cv > 0:
                out.append((d10, cv, currency))
        except (TypeError, ValueError):
            continue
    return out or None


def _backfill_naver(stock: dict, days: int = 365) -> Optional[list]:
    """Pull daily close history from Naver Finance for KRX listings.

    Page format: /item/sise_day.naver?code=XXXXXX&page=N
    Each page has ~10 trading rows. We walk pages until we either hit
    the requested lookback window or stop seeing new data.
    """
    if (stock.get("exchange") or "").upper() not in ("KRX", "KOSPI", "KOSDAQ"):
        return None
    code = _naver_code_for(stock)
    if not code:
        return None
    cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
    out: list = []
    seen_dates: set = set()
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/124.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
        "Referer": f"https://finance.naver.com/item/main.naver?code={code}",
    }
    # ~10 rows per page, so a 1Y window is ~25 pages. Walking them one at
    # a time cost ~31s per stock (~1.2s x 25 round-trips) and ~9 minutes
    # for the 17 Korean holdings — long enough that the backfill looks
    # hung. The pages are independent URLs, so fetch them CONCURRENTLY
    # and stitch the results together afterwards. Every other backfill
    # source returns a year in a single request; this brings Naver into
    # the same ballpark.
    pages_needed = min(30, max(4, int(days / 365 * 25) + 3))

    def _fetch_page(page: int):
        url = (f"https://finance.naver.com/item/sise_day.naver"
               f"?code={code}&page={page}")
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=10) as r:
                return page, r.read().decode("euc-kr", errors="replace")
        except Exception as e:
            logger.info("Naver backfill page %d failed for %s: %s",
                        page, code, e)
            return page, None

    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=8) as _ex:
        fetched = dict(_ex.map(_fetch_page, range(1, pages_needed + 1)))

    for page in range(1, pages_needed + 1):
        html = fetched.get(page)
        if html is None:
            break
        # Rows: <tr onmouseover=...>  <td><span class=tah p10 gray03>YYYY.MM.DD</span></td>
        #       <td class=num><span class=...>CLOSE</span></td> ...
        # Easier: regex find all (date, close) pairs.
        pairs = re.findall(
            r'<span[^>]*class="[^"]*tah[^"]*"[^>]*>'
            r'(\d{4}\.\d{2}\.\d{2})</span>.*?'
            r'<span[^>]*class="[^"]*tah[^"]*p11[^"]*"[^>]*>([\d,]+)</span>',
            html, re.S)
        if not pairs:
            break
        added_in_page = 0
        oldest_seen = None
        for date_dot, close_str in pairs:
            d = date_dot.replace(".", "-")
            if d in seen_dates:
                continue
            seen_dates.add(d)
            try:
                c = float(close_str.replace(",", ""))
            except ValueError:
                continue
            if c <= 0:
                continue
            out.append((d, c, "KRW"))
            added_in_page += 1
            oldest_seen = d
        if added_in_page == 0:
            break
        if oldest_seen and oldest_seen < cutoff:
            break
    out.sort(key=lambda x: x[0])
    return out or None


def backfill_price_history(stock: dict, db: "Database",
                           days: int = 365) -> dict:
    """Pull ~`days` of daily prices and store them in price_snapshots.

    Returns a dict the caller can use to render per-stock results:
        {
          "inserted": int,         # new rows added (0 on skip / fail)
          "status":   str,         # one of:
                                   #   "ok"               - inserted ≥ 1
                                   #   "already-covered"  - early-skip
                                   #   "no-source"        - exchange not
                                   #                        in any source
                                   #                        map and no
                                   #                        yahoo_ticker
                                   #   "source-failed"    - sources tried
                                   #                        but all empty
                                   #                        / rate-limited
          "tried":    list[str],   # sources attempted (for diagnostics)
          "reason":   str,         # short human-readable explanation
        }
    """
    ticker  = stock.get("ticker") or ""
    exch    = (stock.get("exchange") or "").upper()
    yh      = stock.get("yahoo_ticker") or ""
    if not ticker or not exch:
        return {"inserted": 0, "status": "no-source", "tried": [],
                "reason": "missing ticker or exchange"}

    # Pre-load the dates we already have so we don't even attempt to
    # insert duplicates (the UNIQUE constraint would reject them, but
    # this saves a round trip per duplicate row).
    existing = {r["snapshot_at"][:10] for r in db.conn.execute(
        "SELECT snapshot_at FROM price_snapshots WHERE ticker=? AND exchange=?",
        (ticker, exch)).fetchall()}

    # Early-skip when ≥80% of expected trading days are covered.
    cutoff_iso = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
    rows_in_window = sum(1 for d in existing if d >= cutoff_iso)
    expected_trading = days * 252 / 365.0
    if rows_in_window >= expected_trading * 0.8:
        logger.info("BACKFILL skip %s/%s: %d rows already cover %dd window",
                    ticker, exch, rows_in_window, days)
        return {"inserted": 0, "status": "already-covered", "tried": [],
                "reason": f"{rows_in_window} rows cover {days}d already"}

    tried: list = []
    series = None
    if exch == "AIX":
        tried.append("AIX")
        logger.info("BACKFILL AIX chart-data: %s/%s", ticker, exch)
        series = _backfill_aix(stock, days=days)
    if not series and exch == "SEM":
        tried.append("SEM")
        logger.info("BACKFILL SEM interactive-graph: %s/%s", ticker, exch)
        series = _backfill_sem(stock, days=days)
    if not series and exch in ("KRX", "KOSPI", "KOSDAQ"):
        tried.append("Naver")
        logger.info("BACKFILL Naver: %s/%s", ticker, exch)
        series = _backfill_naver(stock, days=days)
    if not series and exch in _FT_EXCHANGE:
        tried.append("FT")
        logger.info("BACKFILL FT: %s/%s", ticker, exch)
        series = _backfill_ft(stock, days=days)
    if not series and _sa_slug_for(exch):
        tried.append("stockanalysis")
        logger.info("BACKFILL stockanalysis: %s/%s", ticker, exch)
        series = _backfill_stockanalysis(stock, days=days)
    if not series and yh:
        tried.append("Yahoo")
        logger.info("BACKFILL Yahoo: %s → %s", ticker, yh)
        series = _backfill_yahoo(yh, days=days)
    if not series and exch in _TMX_EXCHANGES:
        tried.append("TMX")
        logger.info("BACKFILL TMX: %s/%s", ticker, exch)
        series = _backfill_tmx(stock, days=days)

    # Top-up: if the source that succeeded covers well under the
    # requested window (stockanalysis only serves ~6 months for many
    # non-US exchanges) and a Yahoo ticker exists that we haven't tried
    # yet, fetch Yahoo too and merge by date. Yahoo usually carries a
    # full year, so this extends e.g. Thai (SET) coverage from ~6mo to
    # ~12mo when Yahoo isn't rate-limited — and silently keeps the
    # shorter series when it is.
    if series and yh and "Yahoo" not in tried:
        expected_trading = days * 252 / 365.0
        if len(series) < expected_trading * 0.75:
            tried.append("Yahoo")
            logger.info("BACKFILL Yahoo top-up: %s → %s (%d rows so far)",
                        ticker, yh, len(series))
            extra = _backfill_yahoo(yh, days=days)
            if extra:
                by_date = {d: (d, c, cc) for (d, c, cc) in series}
                for (d, c, cc) in extra:
                    by_date.setdefault(d, (d, c, cc))
                series = sorted(by_date.values(), key=lambda x: x[0])

    if not series:
        if not tried:
            reason = (f"exchange {exch!r} not in any historical-price "
                      f"source map" + (" (no yahoo_ticker either)"
                                        if not yh else ""))
            status = "no-source"
        else:
            reason = (f"all {len(tried)} source(s) returned no data — "
                      f"tried: {', '.join(tried)}")
            status = "source-failed"
        logger.info("BACKFILL %s for %s/%s: %s", status, ticker, exch, reason)
        return {"inserted": 0, "status": status, "tried": tried,
                "reason": reason}

    # Currency: prefer the source's reported currency, else the live
    # snapshot's currency, else "" (unknown).
    src_ccy = next((c for _, _, c in series if c), "")
    if not src_ccy:
        last = db.get_latest_price(ticker, exch) or {}
        src_ccy = last.get("currency", "")

    inserted = 0
    today = datetime.utcnow().strftime("%Y-%m-%d")
    for d, close, ccy in series:
        if not d or d > today:
            continue
        if d in existing:
            continue
        ok = db.insert_price(
            ticker=ticker, exchange=exch,
            price=float(close), change_pct=0.0,
            currency=(ccy or src_ccy or ""),
            source_url="backfill",
            snapshot_date=d)
        if ok:
            inserted += 1
    if inserted:
        logger.info("  → %s backfilled %d new days", ticker, inserted)
        return {"inserted": inserted, "status": "ok", "tried": tried,
                "reason": f"+{inserted} new days from {tried[-1]}"}
    # Source returned data but every date was already present — treat
    # as already-covered rather than failure.
    return {"inserted": 0, "status": "already-covered", "tried": tried,
            "reason": "no new dates (already covered by prior rows)"}


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
    if not _serper_is_enabled():
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

    # Fund-newsletter scan runs once per refresh, not per stock — each
    # report is shared across the whole watchlist. Errors are logged but
    # never block the rest of the pipeline.
    try:
        from funds import run_funds
        fund_summary = run_funds(db, config)
        summary["__funds__"] = fund_summary
    except Exception as e:
        logger.warning("fund-newsletter scan failed: %s", e)

    return summary
