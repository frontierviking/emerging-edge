"""
catalog_updaters.py — refresh the frontier_stocks.json catalog for
specific exchanges by scraping official listing pages.

Each updater returns a tuple:
    (success: bool, count: int, message: str, entries: list[dict])

`entries` is the FULL replacement list for that exchange in the same
shape as frontier_stocks.json. On success the caller merges these into
frontier_stocks.json and writes the file back.

Scrapers preserve name/currency/notes from existing catalog entries
when a ticker is still present, so hand-curated names don't get lost.
"""

from __future__ import annotations

import json
import os
import re
import ssl
import urllib.parse
import urllib.request

from stock_search import get_exchange_defaults


_REPO_DIR = os.path.dirname(os.path.abspath(__file__))
_CATALOG_PATH = os.path.join(_REPO_DIR, "frontier_stocks.json")


# ---------------------------------------------------------------------------
# HTTP helper — tolerant of self-signed certs on frontier exchange sites.
# ---------------------------------------------------------------------------

_SSL_CTX = ssl.create_default_context()
_SSL_CTX.check_hostname = False
_SSL_CTX.verify_mode = ssl.CERT_NONE


def _http_get(url: str, timeout: int = 15) -> str:
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (emerging-edge catalog updater)",
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en",
    })
    with urllib.request.urlopen(req, timeout=timeout, context=_SSL_CTX) as resp:
        raw = resp.read()
    # Try utf-8 first, fall back to latin-1 for older pages
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.decode("latin-1", errors="replace")


# ---------------------------------------------------------------------------
# Catalog I/O
# ---------------------------------------------------------------------------

def load_catalog() -> list[dict]:
    if not os.path.exists(_CATALOG_PATH):
        return []
    with open(_CATALOG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def save_catalog(entries: list[dict]) -> None:
    with open(_CATALOG_PATH, "w", encoding="utf-8") as f:
        json.dump(entries, f, ensure_ascii=False, indent=2)


def _existing_for_exchange(exchange: str) -> dict[str, dict]:
    """Index existing catalog entries for an exchange by ticker."""
    ex = exchange.upper()
    return {s["ticker"].upper(): s for s in load_catalog()
            if s.get("exchange", "").upper() == ex}


def _make_entry(exchange: str, ticker: str, name: str,
                country: str, currency: str,
                existing: dict | None = None) -> dict:
    """
    Build a catalog entry. When `existing` is provided, preserve the
    curated name, notes, lang, and other hand-edited fields.
    """
    defaults = get_exchange_defaults(exchange, ticker)
    if existing:
        base = dict(existing)
        base["ticker"] = ticker
        base["exchange"] = exchange
        # Keep curated name unless blank
        if not base.get("name"):
            base["name"] = name or ticker
        # Ensure defaults exist even on old entries
        base.setdefault("currency", currency)
        base.setdefault("country", country)
        base.setdefault("lang", "en")
        base.setdefault("forum_sources", defaults.get("forum_sources", []))
        base.setdefault("earnings_source", defaults.get("earnings_source", ""))
        base.setdefault("price_url", defaults.get("price_url", ""))
        base.setdefault("code", ticker)
        return base
    return {
        "ticker": ticker,
        "exchange": exchange,
        "name": name or ticker,
        "currency": currency,
        "lang": "en",
        "forum_sources": defaults.get("forum_sources", []),
        "earnings_source": defaults.get("earnings_source", ""),
        "code": ticker,
        "country": country,
        "notes": "",
        "price_url": defaults.get("price_url", ""),
    }


# ---------------------------------------------------------------------------
# UZSE — Republican Stock Exchange Toshkent, uzse.uz
# ---------------------------------------------------------------------------

def update_uzse() -> tuple[bool, int, str, list[dict]]:
    """Scrape all equities from uzse.uz isu_infos pagination."""
    base_url = "https://uzse.uz/isu_infos"
    existing = _existing_for_exchange("UZSE")
    out_map: dict[str, dict] = {}
    pages_visited = 0
    try:
        for page in range(1, 15):  # safety cap
            url = f"{base_url}?locale=en&page={page}"
            html = _http_get(url)
            pages_visited += 1
            rows = re.findall(r"<tr[^>]*>(.*?)</tr>", html, re.S)
            page_rows = 0
            for row in rows:
                cells = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row, re.S)
                if len(cells) < 6:
                    continue
                cleaned = [re.sub(r"<[^>]+>", "", c).strip() for c in cells]
                kind = cleaned[2]
                ticker = cleaned[3]
                if not re.match(r"^[A-Z0-9]{2,8}$", ticker):
                    continue
                # Keep only equities (Premium / Standard / Common). Skip bonds.
                if kind.lower() in ("bond", "bonds", "obligation"):
                    continue
                raw_name = cleaned[5]
                name = (raw_name.replace("&lt;", "").replace("&gt;", "")
                        .replace("&amp;", "&").replace("&#39;", "'")
                        .replace("\n", " ").strip())
                name = re.sub(r"\s+", " ", name)[:120]
                entry = _make_entry("UZSE", ticker, name, "Uzbekistan",
                                    "UZS", existing.get(ticker))
                out_map[ticker] = entry
                page_rows += 1
            if page_rows == 0 and page > 1:
                break  # ran out of pages
    except Exception as e:
        return False, 0, f"fetch failed: {e}", []

    entries = list(out_map.values())
    entries.sort(key=lambda x: x["ticker"])
    msg = f"uzse.uz ({pages_visited} pages) → {len(entries)} equities"
    return True, len(entries), msg, entries


# ---------------------------------------------------------------------------
# NGX — Nigerian Exchange, ngxgroup.com
# ---------------------------------------------------------------------------

def update_ngx() -> tuple[bool, int, str, list[dict]]:
    """Scrape the public equity ticker index on ngxgroup.com."""
    url = "https://ngxgroup.com/exchange/data/equities-price-list/"
    existing = _existing_for_exchange("NGX")
    try:
        html = _http_get(url, timeout=20)
    except Exception as e:
        return False, 0, f"fetch failed: {e}", []

    # Symbols appear in links: symbol=XXXX&directory=companydirectory
    raw = re.findall(
        r"symbol=([A-Z][A-Z0-9]+)&directory=companydirectory", html)
    # Keep alphabetical-only tickers (skip bonds / ETFs like ABBEYBDS / VSPBONDETF).
    seen = set()
    tickers = []
    for s in raw:
        if not s.isalpha():
            continue
        if s.endswith(("BDS", "ETF", "BOND")):
            continue
        if len(s) < 2 or len(s) > 12:
            continue
        if s not in seen:
            seen.add(s)
            tickers.append(s)

    if not tickers:
        return False, 0, "no tickers parsed from ngxgroup.com", []

    entries = []
    for t in sorted(tickers):
        entries.append(_make_entry("NGX", t, "", "Nigeria", "NGN",
                                    existing.get(t)))
    return True, len(entries), f"ngxgroup.com → {len(entries)} equities", entries


# ---------------------------------------------------------------------------
# BRVM — Bourse Régionale des Valeurs Mobilières, brvm.org
# ---------------------------------------------------------------------------

def update_brvm() -> tuple[bool, int, str, list[dict]]:
    url = "https://www.brvm.org/en/cours-actions/0"
    existing = _existing_for_exchange("BRVM")
    try:
        html = _http_get(url, timeout=15)
    except Exception as e:
        return False, 0, f"fetch failed: {e}", []

    rows = re.findall(r"<tr[^>]*>(.*?)</tr>", html, re.S)
    tickers = []
    seen = set()
    for row in rows:
        cells = re.findall(r"<td[^>]*>(.*?)</td>", row, re.S)
        if not cells:
            continue
        first = re.sub(r"<[^>]+>", "", cells[0]).strip()
        if re.match(r"^[A-Z]{3,6}$", first) and first not in seen:
            seen.add(first)
            tickers.append(first)

    if not tickers:
        return False, 0, "no tickers parsed from brvm.org", []

    entries = []
    for t in sorted(tickers):
        entries.append(_make_entry("BRVM", t, "", "BRVM/Ivory Coast",
                                    "XOF", existing.get(t)))
    return True, len(entries), f"brvm.org → {len(entries)} equities", entries


# ---------------------------------------------------------------------------
# KSE — Kyrgyz Stock Exchange, kse.kg
# ---------------------------------------------------------------------------

def update_kse() -> tuple[bool, int, str, list[dict]]:
    """
    kse.kg lists companies by full name in its category tables, without
    machine-readable ticker columns. Automated refresh isn't feasible
    from the public listing page alone, so this updater reports the
    status and leaves the existing catalog entries untouched.
    """
    existing = list(_existing_for_exchange("KSE").values())
    msg = ("kse.kg does not expose tickers in its public listing page. "
           "Catalog kept as-is — add new KSE stocks manually via "
           "the Add-Stock modal.")
    return False, len(existing), msg, existing


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------

UPDATERS = {
    "UZSE": update_uzse,
    "NGX":  update_ngx,
    "BRVM": update_brvm,
    "KSE":  update_kse,
}


def supported_exchanges() -> list[str]:
    return list(UPDATERS.keys())


def refresh_exchange(exchange: str) -> tuple[bool, int, str]:
    """
    Refresh the catalog for one exchange and persist the new entries to
    frontier_stocks.json. Returns (ok, count, message).
    """
    ex = exchange.upper()
    fn = UPDATERS.get(ex)
    if not fn:
        return False, 0, f"no updater registered for {ex}"

    ok, count, msg, new_entries = fn()
    if not ok or not new_entries:
        return ok, count, msg

    # Merge: replace all rows for this exchange with the new list.
    all_entries = load_catalog()
    kept = [s for s in all_entries
            if s.get("exchange", "").upper() != ex]
    merged = kept + new_entries
    # Keep a stable ordering — by exchange, then ticker
    merged.sort(key=lambda s: (s.get("exchange", ""), s.get("ticker", "")))
    save_catalog(merged)
    return True, count, msg
