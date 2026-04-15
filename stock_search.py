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
    "NSEK": "KES",   # NSE Kenya — disambiguated from NSE India below
    "GSE": "GHS",    # Ghana Stock Exchange
    "BWSE": "BWP",   # Botswana — disambiguated from Mumbai BSE below
    "LUSE": "ZMW",   # Lusaka Securities Exchange
    "DSET": "TZS",   # Dar es Salaam SE Tanzania — disambiguated from DSEB
    "DSEB": "BDT",   # Dhaka SE Bangladesh — disambiguated from DSET
    "PSX":  "PKR",   # Pakistan Stock Exchange
    "CSEM": "MAD",   # Casablanca SE Morocco — disambiguated from Colombo/Copenhagen CSE
    "ZSE":  "EUR",   # Zagreb Stock Exchange — Croatia switched to EUR in 2023
    "BELEX": "RSD",  # Belgrade Stock Exchange
    "BSSE": "EUR",   # Bratislava Stock Exchange
    "PNGX": "PGK",   # Port Moresby / PNGX Markets — Papua New Guinea kina
    "BVMT": "TND",   # Bourse de Tunis — Tunisian dinar
    "CSEL": "LKR",   # Colombo Stock Exchange Sri Lanka — Sri Lankan rupee
    "UX":   "UAH",   # Ukrainian Exchange — hryvnia
    "USE":  "UGX",   # Uganda Securities Exchange — Ugandan shilling
    "RSE":  "RWF",   # Rwanda Stock Exchange — Rwandan franc
    "SEM":  "MUR",   # Stock Exchange of Mauritius — Mauritian rupee
    "ISX":  "IQD",   # Iraq Stock Exchange — Iraqi dinar
    "ESX":  "ETB",   # Ethiopian Securities Exchange — Ethiopian birr
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

# Sensible defaults per exchange for forum/earnings plumbing.
# `price_url_template` is a string with {TICKER} placeholder — the price
# scraper needs this for exchanges Yahoo Finance doesn't index.
_EXCHANGE_DEFAULTS = {
    "KLSE":   {"forum_sources": ["i3investor"], "earnings_source": "klsescreener",
               "price_url_template": ""},  # KLSE uses Yahoo (.KL suffix)
    "NGX":    {"forum_sources": [],              "earnings_source": "ngx",
               "price_url_template": "https://www.tradingview.com/symbols/NSENG-{TICKER}/"},
    "BRVM":   {"forum_sources": ["richbourse"], "earnings_source": "brvm",
               "price_url_template": "https://www.brvm.org/en/cours-actions/0/{TICKER}"},
    "UZSE":   {"forum_sources": [],              "earnings_source": "uzse",
               "price_url_template": "https://stockscope.uz/en/listings/{TICKER}/general"},
    "SGX":    {"forum_sources": [],              "earnings_source": "sgx",
               "price_url_template": ""},  # SGX uses Yahoo (.SI suffix)
    "KSE":    {"forum_sources": [],              "earnings_source": "kse",
               "price_url_template": "https://kse.kg/en/instrument/{TICKER}"},
    "KASE":   {"forum_sources": [],              "earnings_source": "",
               "price_url_template": "https://kase.kz/en/investors/shares/{TICKER}"},
    "NSEK":   {"forum_sources": [],              "earnings_source": "",
               "price_url_template": "https://afx.kwayisi.org/nse/{TICKER_LOWER}.html"},
    "GSE":    {"forum_sources": [],              "earnings_source": "",
               "price_url_template": "https://afx.kwayisi.org/gse/{TICKER_LOWER}.html"},
    "BWSE":   {"forum_sources": [],              "earnings_source": "",
               "price_url_template": "https://afx.kwayisi.org/bse/{TICKER_LOWER}.html"},
    "LUSE":   {"forum_sources": [],              "earnings_source": "",
               "price_url_template": "https://afx.kwayisi.org/luse/{TICKER_LOWER}.html"},
    "DSET":   {"forum_sources": [],              "earnings_source": "",
               "price_url_template": "https://www.dse.co.tz/"},
    "DSEB":   {"forum_sources": [],              "earnings_source": "",
               "price_url_template": "https://www.dsebd.org/displayCompany.php?name={TICKER}"},
    "PSX":    {"forum_sources": [],              "earnings_source": "",
               "price_url_template": "https://dps.psx.com.pk/company/{TICKER}"},
    "CSEM":   {"forum_sources": [],              "earnings_source": "",
               "price_url_template": ""},  # No free price source for Morocco
    "ZSE":    {"forum_sources": [],              "earnings_source": "",
               "price_url_template": "https://zse.hr/default.aspx?id=26474"},
    "BELEX":  {"forum_sources": [],              "earnings_source": "",
               "price_url_template": ""},  # No free price source for Serbia
    "BSSE":   {"forum_sources": [],              "earnings_source": "",
               "price_url_template": ""},  # No free price source for Slovakia
    "PNGX":   {"forum_sources": [],              "earnings_source": "",
               "price_url_template": ""},  # Prices via Yahoo cross-listings only
    "BVMT":   {"forum_sources": [],              "earnings_source": "",
               "price_url_template": ""},  # No free price source for Tunisia
    "CSEL":   {"forum_sources": [],              "earnings_source": "",
               "price_url_template": "https://www.cse.lk/"},
    "UX":     {"forum_sources": [],              "earnings_source": "",
               "price_url_template": ""},  # No free price source for Ukraine
    "USE":    {"forum_sources": [],              "earnings_source": "",
               "price_url_template": "https://afx.kwayisi.org/use/{TICKER_LOWER}.html"},
    "RSE":    {"forum_sources": [],              "earnings_source": "",
               "price_url_template": "https://rse.rw/"},
    "SEM":    {"forum_sources": [],              "earnings_source": "",
               "price_url_template": "https://www.stockexchangeofmauritius.com/products-market-data/equities-board/trading-quotes/official"},
    "ISX":    {"forum_sources": [],              "earnings_source": "",
               "price_url_template": "http://www.isx-iq.net/isxportal/portal/marketPerformance.html?currLanguage=en"},
    "ESX":    {"forum_sources": [],              "earnings_source": "",
               "price_url_template": ""},  # ESX too new — no price source
    "NASDAQ": {"forum_sources": ["twitter"],    "earnings_source": "",
               "price_url_template": ""},  # NASDAQ uses Yahoo
    "NYSE":   {"forum_sources": ["twitter"],    "earnings_source": "",
               "price_url_template": ""},
    "JSE":    {"forum_sources": [],              "earnings_source": "",
               "price_url_template": ""},  # JSE uses Yahoo (.JO suffix)
}


# Exchanges with a custom price scraper in fetchers._fetch_price_scrape.
# Stocks on these exchanges have a live price source even without a
# yahoo_ticker. Kept in sync with fetchers.py.
_CUSTOM_PRICE_EXCHANGES = {
    "UZSE", "NGX", "BRVM", "KASE", "KSE",
    "NSEK", "GSE", "BWSE", "LUSE", "USE",
    "DSET", "DSEB", "PSX", "ZSE", "CSEL",
    "RSE", "SEM", "ISX",
}


def has_price_source(stock: dict) -> bool:
    """
    Return True if there is any free live-price source wired up for
    this stock. Used by the dashboard to distinguish "awaiting refresh"
    (source exists, no snapshot yet) from "no price source"
    (catalog-only exchanges like ESX, UX, BVMT, CSEM, BELEX, BSSE).
    """
    if stock.get("yahoo_ticker"):
        return True
    return (stock.get("exchange") or "").upper() in _CUSTOM_PRICE_EXCHANGES


def get_exchange_defaults(exchange: str, ticker: str) -> dict:
    """Return per-exchange defaults with the {TICKER} template filled in."""
    base = _EXCHANGE_DEFAULTS.get(exchange.upper(), {}) or {}
    template = base.get("price_url_template", "")
    if template:
        price_url = (template
                     .replace("{TICKER_LOWER}", ticker.lower())
                     .replace("{TICKER}", ticker.upper()))
    else:
        price_url = ""
    return {
        "forum_sources": base.get("forum_sources", []),
        "earnings_source": base.get("earnings_source", ""),
        "price_url": price_url,
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
        defaults = get_exchange_defaults(internal_exch, base_ticker.upper())
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
            "price_url": defaults.get("price_url", ""),
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
            # Fill price_url from the per-exchange template if not set
            if not result.get("price_url"):
                defaults = get_exchange_defaults(result.get("exchange", ""),
                                                  result.get("ticker", ""))
                result["price_url"] = defaults.get("price_url", "")
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
