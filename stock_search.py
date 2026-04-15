"""
stock_search.py — resolve a free-text company name or ticker query into
structured stock metadata for the watchlist.

Two sources, merged and deduped:
  1. Yahoo Finance symbol search (public, free, no API key). Covers
     most major exchanges including NASDAQ, NYSE, KLSE, SGX, JSE, LSE,
     HKSE, ASX, Frankfurt, Borse, etc.
  2. Internal frontier_stocks.json catalog. Covers frontier exchanges
     Yahoo doesn't index well (NGX, BRVM, UZSE, KSE).

Both sources return candidates in the same shape so the UI can treat
them uniformly.
"""

from __future__ import annotations

import json
import logging
import os
import urllib.parse
import urllib.request

logger = logging.getLogger("emerging-edge.stock_search")


_REPO_DIR = os.path.dirname(os.path.abspath(__file__))
_CATALOG_PATH = os.path.join(_REPO_DIR, "frontier_stocks.json")


# ---------------------------------------------------------------------------
# Exchange code mapping: Yahoo → our internal code
# ---------------------------------------------------------------------------
_YAHOO_TO_INTERNAL = {
    "NMS": "NASDAQ", "NAS": "NASDAQ", "NGM": "NASDAQ", "NCM": "NASDAQ",
    "NYQ": "NYSE",   "NYS": "NYSE",
    "KLS": "KLSE",
    "SES": "SGX",    "SG":  "SGX",
    "JNB": "JSE",
    "LSE": "LSE",    "LON": "LSE",
    "HKG": "HKSE",
    "ASX": "ASX",
    "FRA": "FRA",    "GER": "FRA",
    "TOR": "TSX",    "TSX": "TSX",
    "MEX": "BMV",
    "LAG": "NGX",
    "BRV": "BRVM",
    "TSE": "UZSE",
    "KAS": "KASE",
    "BOM": "BSE",    "NSI": "NSE",
    "PAR": "EURONEXT", "AMS": "EURONEXT", "BRU": "EURONEXT",
    "MIL": "BIT",
    "STO": "OMX",
    "HEL": "OMX",
    "OSL": "OSE",
    "CPH": "CSE",
    "ZRH": "SWX",
    "SAO": "B3",
    "BUE": "BCBA",
}

# Exchange → default currency (used when Yahoo doesn't supply one)
_EXCHANGE_CURRENCY = {
    "NASDAQ": "USD", "NYSE": "USD",
    "KLSE": "MYR",
    "SGX": "SGD",
    "JSE": "ZAc",
    "LSE": "GBP",
    "HKSE": "HKD",
    "ASX": "AUD",
    "FRA": "EUR",
    "TSX": "CAD",
    "BMV": "MXN",
    "NGX": "NGN",
    "BRVM": "XOF",
    "UZSE": "UZS",
    "KASE": "KZT",
    "KSE": "KGS",
    "BSE": "INR", "NSE": "INR",
    "EURONEXT": "EUR",
    "BIT": "EUR",
    "OMX": "SEK",
    "OSE": "NOK",
    "CSE": "DKK",
    "SWX": "CHF",
    "B3": "BRL",
    "BCBA": "ARS",
}

# Sensible defaults per exchange for forum/earnings plumbing
_EXCHANGE_DEFAULTS = {
    "KLSE":   {"forum_sources": ["i3investor"], "earnings_source": "klsescreener"},
    "NGX":    {"forum_sources": [],              "earnings_source": "ngx"},
    "BRVM":   {"forum_sources": ["richbourse"], "earnings_source": "brvm"},
    "UZSE":   {"forum_sources": [],              "earnings_source": "uzse"},
    "SGX":    {"forum_sources": [],              "earnings_source": "sgx"},
    "KSE":    {"forum_sources": [],              "earnings_source": "kse"},
    "NASDAQ": {"forum_sources": ["twitter"],    "earnings_source": ""},
    "NYSE":   {"forum_sources": ["twitter"],    "earnings_source": ""},
    "JSE":    {"forum_sources": [],              "earnings_source": ""},
}


def _load_catalog() -> list[dict]:
    if not os.path.exists(_CATALOG_PATH):
        return []
    try:
        with open(_CATALOG_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.warning("Failed to load frontier_stocks.json: %s", e)
        return []


# ---------------------------------------------------------------------------
# Yahoo Finance symbol search
# ---------------------------------------------------------------------------

def search_yahoo(query: str, limit: int = 10) -> list[dict]:
    """Call Yahoo Finance symbol search. Returns [] on failure."""
    q = (query or "").strip()
    if not q:
        return []

    url = (
        "https://query2.finance.yahoo.com/v1/finance/search?"
        + urllib.parse.urlencode({
            "q": q,
            "quotesCount": limit,
            "newsCount": 0,
            "enableFuzzyQuery": "false",
            "quotesQueryId": "tss_match_phrase_query",
        })
    )
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (emerging-edge stock search)",
        "Accept": "application/json",
    })
    try:
        with urllib.request.urlopen(req, timeout=8) as resp:
            data = json.loads(resp.read().decode("utf-8", errors="replace"))
    except Exception as e:
        logger.warning("Yahoo search failed for %r: %s", query, e)
        return []

    out = []
    for q_obj in data.get("quotes", []):
        if q_obj.get("quoteType") != "EQUITY":
            continue
        yahoo_sym = q_obj.get("symbol", "")
        y_exch = q_obj.get("exchange", "")
        internal_exch = _YAHOO_TO_INTERNAL.get(y_exch, y_exch)
        # Prefer longname, fall back to shortname
        name = q_obj.get("longname") or q_obj.get("shortname") or yahoo_sym
        # Ticker = strip exchange suffix (e.g. 5236.KL → 5236; TIGO → TIGO)
        base_ticker = yahoo_sym.split(".")[0] if "." in yahoo_sym else yahoo_sym
        currency = _EXCHANGE_CURRENCY.get(internal_exch, "USD")
        defaults = _EXCHANGE_DEFAULTS.get(internal_exch, {})
        out.append({
            "ticker": base_ticker.upper(),
            "exchange": internal_exch,
            "name": name,
            "currency": currency,
            "yahoo_ticker": yahoo_sym,
            "lang": "en",
            "forum_sources": defaults.get("forum_sources", []),
            "earnings_source": defaults.get("earnings_source", ""),
            "code": base_ticker,
            "country": q_obj.get("exchDisp", ""),
            "notes": q_obj.get("industry", "") or q_obj.get("sector", ""),
            "source": "yahoo",
            "exchDisp": q_obj.get("exchDisp", internal_exch),
        })
    return out


# ---------------------------------------------------------------------------
# Local catalog search
# ---------------------------------------------------------------------------

def search_catalog(query: str, limit: int = 10) -> list[dict]:
    """Substring match against the shipped frontier_stocks.json catalog."""
    q = (query or "").strip().lower()
    if not q:
        return []
    catalog = _load_catalog()
    results = []
    for s in catalog:
        name = (s.get("name") or "").lower()
        ticker = (s.get("ticker") or "").lower()
        if q in name or q in ticker:
            result = dict(s)
            result["source"] = "catalog"
            result["exchDisp"] = s.get("country") or s.get("exchange", "")
            results.append(result)
    return results[:limit]


# ---------------------------------------------------------------------------
# Merged search
# ---------------------------------------------------------------------------

def search_stocks(query: str, limit: int = 10) -> list[dict]:
    """
    Search Yahoo Finance and the internal catalog, deduping by
    (ticker, exchange). Catalog hits rank after Yahoo hits because
    Yahoo has richer metadata.
    """
    q = (query or "").strip()
    if not q:
        return []

    seen = set()
    merged = []

    for src in (search_yahoo(q, limit), search_catalog(q, limit)):
        for r in src:
            key = (r.get("ticker", "").upper(), r.get("exchange", "").upper())
            if key in seen:
                continue
            seen.add(key)
            merged.append(r)
            if len(merged) >= limit:
                return merged
    return merged
