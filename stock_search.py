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
import re
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
    "LSE": "LSE",    "LON": "LSE",   "IOB": "LSE",   # IOB = LSE International Orderbook → UK
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
    # Euronext split by country (each listing has a single country of record)
    "PAR": "EUR_FR",  # Paris — France
    "AMS": "EUR_NL",  # Amsterdam — Netherlands
    "BRU": "EUR_BE",  # Brussels — Belgium
    "LIS": "EUR_PT",  # Lisbon — Portugal
    "ISE": "EUR_IE",  # Dublin — Ireland
    "MIL": "BIT",
    # Nordics split by country
    "STO": "OMX",     # Stockholm — Sweden
    "HEL": "HSE",     # Helsinki — Finland
    "OSL": "OSE",     # Oslo — Norway
    "CPH": "CSE",     # Copenhagen — Denmark
    "ICE": "ICEX",    # Reykjavik — Iceland (rare)
    "CYS": "CSEC",    # Cyprus Stock Exchange (Nicosia)
    "NIC": "CSEC",    # Nicosia / Cyprus — alternate Yahoo code
    "ZRH": "SWX",
    "SAO": "B3",
    "BUE": "BCBA",
    # ── New exchanges (2026 batch) ──
    "KSC": "KRX",    "KOE": "KRX",     # Korea (KOSPI + KOSDAQ)
    "TAI": "TWSE",                       # Taiwan
    "JKT": "IDX",                        # Indonesia (Jakarta)
    "SET": "SET",    "BKK": "SET",       # Thailand
    "PHS": "PSE",                        # Philippines
    "VSE": "HOSE",   "HNX": "HOSE",     # Vietnam (Ho Chi Minh + Hanoi)
    "TLV": "TASE",                       # Israel (Tel Aviv)
    "SAU": "TADAWUL",                    # Saudi Arabia
    "DFM": "DFM",                        # UAE — Dubai Financial Market
    "ADX": "ADX",                        # UAE — Abu Dhabi
    "DOH": "QSE",                        # Qatar (Doha)
    "IST": "BIST",                       # Turkey (Borsa Istanbul)
    "WSE": "WSE",    "WAR": "WSE",       # Poland (Warsaw)
    "PRA": "PSE_CZ",                     # Czech Republic (Prague)
    "BUD": "BET",                        # Hungary (Budapest)
    "ATH": "ATHEX",                      # Greece (Athens)
    "BVB": "BVB",                        # Romania (Bucharest)
    "NZE": "NZX",                        # New Zealand
    "SHH": "SSE",                        # China — Shanghai
    "SHZ": "SZSE",                       # China — Shenzhen
    # ── Extra country mappings (2026 — discovered via Yahoo probe) ──
    "JPX": "JPX",                        # Japan (Tokyo Stock Exchange)
    "TYO": "JPX",                        # Older Yahoo code
    "MCE": "BME",                        # Spain (Madrid — Bolsa de Madrid / BME)
    "MAD": "BME",
    "VIE": "WBAG",                       # Austria (Vienna / Wiener Börse)
    "SGO": "BVS",                        # Chile (Bolsa de Santiago)
    "BVS": "BVS",
    "EBS": "SWX",                        # Swiss EBS → SIX Swiss Exchange
    "VTX": "SWX",                        # Swiss Virt-X / SIX
    "HAN": "FRA",                        # Hanover regional → Germany
    "MUN": "FRA",                        # Munich regional
    "BER": "FRA",                        # Berlin regional
    "DUS": "FRA",                        # Düsseldorf regional
    "STU": "FRA",                        # Stuttgart regional
    "HAM": "FRA",                        # Hamburg regional
    "ETR": "FRA",                        # XETRA
    "NEO": "TSX",                        # NEO Exchange Canada → TSX
    "TLO": "BIT",                        # TLX (Italian electronic) → Italy
    "ASE": "AMEX",                       # NYSE MKT (formerly AMEX) → US
    "AMX": "AMEX",                       # Legacy AMEX code
    "OID": "OTC",                        # Yahoo OTC pink/other → US
    "OQX": "OTC",                        # OTCQX
    "OBB": "OTC",                        # OTCBB
    "PCX": "NYSE",                       # NYSE Arca → US
    "NGM": "NASDAQ",                     # NASDAQ Global Market (redundant but explicit)
    "NSM": "NASDAQ",                     # NASDAQ Small Market
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
    "MSE": "MNT",    # Mongolian Stock Exchange — Mongolian tögrög
    "BVG": "USD",    # Bolsa de Valores de Guayaquil, Ecuador (USD economy)
    "AIX": "USD",    # Astana Intl Exchange — multi-currency; per-stock
                     # currency (KZT/USD/CNY) is stored on each catalog
                     # entry and returned live by the fetcher.
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
    "CSEC": "EUR",   # Cyprus Stock Exchange — euro (CSEC to disambiguate from Copenhagen/Colombo/Casablanca)
    "UX":   "UAH",   # Ukrainian Exchange — hryvnia
    "USE":  "UGX",   # Uganda Securities Exchange — Ugandan shilling
    "RSE":  "RWF",   # Rwanda Stock Exchange — Rwandan franc
    "SEM":  "MUR",   # Stock Exchange of Mauritius — Mauritian rupee
    "ISX":  "IQD",   # Iraq Stock Exchange — Iraqi dinar
    "ESX":  "ETB",   # Ethiopian Securities Exchange — Ethiopian birr
    "BSE": "INR", "NSE": "INR",
    "KRX":  "KRW",   # Korea Exchange — Korean won
    "TWSE": "TWD",   # Taiwan Stock Exchange — Taiwan dollar
    "IDX":  "IDR",   # Indonesia Stock Exchange — Indonesian rupiah
    "SET":  "THB",   # Stock Exchange of Thailand — Thai baht
    "PSE":  "PHP",   # Philippine Stock Exchange — Philippine peso
    "HOSE": "VND",   # Ho Chi Minh Stock Exchange — Vietnamese dong
    "TASE": "ILS",   # Tel Aviv Stock Exchange — Israeli shekel
    "TADAWUL": "SAR", # Saudi Stock Exchange — Saudi riyal
    "DFM":  "AED",   # Dubai Financial Market — UAE dirham
    "ADX":  "AED",   # Abu Dhabi Securities Exchange — UAE dirham
    "QSE":  "QAR",   # Qatar Stock Exchange — Qatari riyal
    "BIST": "TRY",   # Borsa Istanbul — Turkish lira
    "WSE":  "PLN",   # Warsaw Stock Exchange — Polish zloty
    "PSE_CZ": "CZK", # Prague Stock Exchange — Czech koruna
    "BET":  "HUF",   # Budapest Stock Exchange — Hungarian forint
    "ATHEX": "EUR",  # Athens Stock Exchange — euro
    "BVB":  "RON",   # Bucharest Stock Exchange — Romanian leu
    "NZX":  "NZD",   # New Zealand Exchange — NZ dollar
    "SSE":  "CNY",   # Shanghai Stock Exchange — Chinese yuan
    "SZSE": "CNY",   # Shenzhen Stock Exchange — Chinese yuan
    "JPX":  "JPY",   # Japan (Tokyo Stock Exchange)
    "BME":  "EUR",   # Spain (Bolsa de Madrid)
    "WBAG": "EUR",   # Austria (Wiener Börse)
    "BVS":  "CLP",   # Chile (Bolsa de Santiago)
    "BVC":  "COP",   # Colombia (Bolsa de Valores de Colombia)
    "AMEX": "USD",   # NYSE American
    "OTC":  "USD",   # OTC markets
    "AMS":  "EUR", "PCX": "USD", "PNK": "USD",  # legacy bases
    "EURONEXT": "EUR",
    "EUR_FR": "EUR",   # Euronext Paris — France
    "EUR_NL": "EUR",   # Euronext Amsterdam — Netherlands
    "EUR_BE": "EUR",   # Euronext Brussels — Belgium
    "EUR_PT": "EUR",   # Euronext Lisbon — Portugal
    "EUR_IE": "EUR",   # Euronext Dublin — Ireland
    "BIT": "EUR",
    "OMX": "SEK",      # Stockholm — Swedish krona
    "HSE": "EUR",      # Helsinki — Finnish euro
    "ICEX": "ISK",     # Reykjavik — Icelandic krona
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
    "MSE":    {"forum_sources": [],              "earnings_source": "",
               # Per-stock price_url is set by update_mse() because the
               # open.mse.mn detail page is keyed by numeric id, not the
               # ticker — no {TICKER} template can express it.
               "price_url_template": ""},
    "BVG":    {"forum_sources": [],              "earnings_source": "",
               # update_bvg() pins the shared Guayaquil price page as
               # every entry's price_url; the fetcher matches by name.
               "price_url_template": ""},
    "AIX":    {"forum_sources": [],              "earnings_source": "",
               # update_aix() pins the shared market-watch JSON API as
               # every entry's price_url; the fetcher keys by secCode.
               "price_url_template": ""},
    "SGX":    {"forum_sources": ["valuebuddies", "hardwarezone"],
               "earnings_source": "sgx",
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
    "PSX":    {"forum_sources": ["pakinvestorsguide"],
               "earnings_source": "",
               "price_url_template": "https://dps.psx.com.pk/company/{TICKER}"},
    "CSEM":   {"forum_sources": ["bourse_maroc"],
               "earnings_source": "",
               "price_url_template": ""},  # No free price source for Morocco
    "ZSE":    {"forum_sources": ["bug_hr"],
               "earnings_source": "",
               "price_url_template": "https://zse.hr/default.aspx?id=26474"},
    "BELEX":  {"forum_sources": [],              "earnings_source": "",
               "price_url_template": ""},  # No free price source for Serbia
    "BSSE":   {"forum_sources": ["ako_investovat"],
               "earnings_source": "",
               "price_url_template": ""},  # No free price source for Slovakia
    "PNGX":   {"forum_sources": [],              "earnings_source": "",
               "price_url_template": ""},  # Prices via Yahoo cross-listings only
    "BVMT":   {"forum_sources": ["ilboursa"],
               "earnings_source": "",
               "price_url_template": ""},  # No free price source for Tunisia
    "CSEL":   {"forum_sources": ["lankaninvestor"],
               "earnings_source": "",
               "price_url_template": "https://www.cse.lk/"},
    "UX":     {"forum_sources": [],              "earnings_source": "",
               "price_url_template": ""},  # No free price source for Ukraine
    "USE":    {"forum_sources": [],              "earnings_source": "",
               "price_url_template": "https://afx.kwayisi.org/use/{TICKER_LOWER}.html"},
    "RSE":    {"forum_sources": [],              "earnings_source": "",
               "price_url_template": "https://rse.rw/"},
    "SEM":    {"forum_sources": [],              "earnings_source": "",
               "price_url_template": "https://www.stockexchangeofmauritius.com/products-market-data/equities-board/trading-quotes/official"},
    "ISX":    {"forum_sources": ["investorsiraq"],
               "earnings_source": "",
               "price_url_template": "http://www.isx-iq.net/isxportal/portal/marketPerformance.html?currLanguage=en"},
    "ESX":    {"forum_sources": [],              "earnings_source": "",
               "price_url_template": ""},  # ESX too new — no price source
    "NASDAQ": {"forum_sources": ["twitter"],    "earnings_source": "",
               "price_url_template": ""},  # NASDAQ uses Yahoo
    "NYSE":   {"forum_sources": ["twitter"],    "earnings_source": "",
               "price_url_template": ""},
    "JSE":    {"forum_sources": ["shareforum"],
               "earnings_source": "",
               "price_url_template": ""},  # JSE uses Yahoo (.JO suffix)
    "HKSE":   {"forum_sources": [],              "earnings_source": "",
               "price_url_template": ""},  # HKSE uses Yahoo
    "NSE":    {"forum_sources": ["valuepickr"],
               "earnings_source": "",
               "price_url_template": ""},  # NSE India uses Yahoo
    "BSE":    {"forum_sources": ["valuepickr"],
               "earnings_source": "",
               "price_url_template": ""},  # BSE India uses Yahoo
    "FRA":    {"forum_sources": ["wallstreet_online", "ariva"],
               "earnings_source": "",
               "price_url_template": ""},  # Frankfurt uses Yahoo
    "BIT":    {"forum_sources": ["finanzaonline", "investireoggi"],
               "earnings_source": "",
               "price_url_template": ""},  # Milan uses Yahoo
    "OMX":    {"forum_sources": ["aktiespararna"],
               "earnings_source": "",
               "price_url_template": ""},  # Stockholm uses Yahoo
    "OSE":    {"forum_sources": ["hegnar"],
               "earnings_source": "",
               "price_url_template": ""},  # Oslo uses Yahoo
    "BCBA":   {"forum_sources": ["argentinabursatil"],
               "earnings_source": "",
               "price_url_template": ""},  # Buenos Aires
    "BMV":    {"forum_sources": ["rankia_mx"],
               "earnings_source": "",
               "price_url_template": ""},  # Mexico
}


# Exchanges with a custom price scraper in fetchers._fetch_price_scrape.
# Stocks on these exchanges have a live price source even without a
# yahoo_ticker. Kept in sync with fetchers.py.
_CUSTOM_PRICE_EXCHANGES = {
    "UZSE", "MSE", "BVG", "AIX", "NGX", "BRVM", "KASE", "KSE",
    "NSEK", "GSE", "BWSE", "LUSE", "USE",
    "DSET", "DSEB", "PSX", "ZSE", "CSEL",
    "RSE", "SEM", "ISX",
}


# Internal exchange code → Yahoo Finance ticker suffix. Used to
# auto-derive a yahoo_ticker when the catalog or Yahoo symbol search
# returns a stock without one — common for niche listings on
# stockanalysis.com lists, which never include Yahoo metadata.
_YAHOO_SUFFIX_BY_EXCHANGE: dict[str, str] = {
    "NASDAQ": "", "NYSE": "", "AMEX": "", "OTC": "",
    "TSX": ".TO", "TSXV": ".V",
    "LSE": ".L",
    "ASX": ".AX",
    "JSE": ".JO",
    "KLSE": ".KL",
    "SGX": ".SI",
    "FRA": ".DE",
    "OMX": ".ST",
    "OSE": ".OL",
    "CSE": ".CO",
    "HEL": ".HE",
    "BIT": ".MI",
    "EURONEXT": ".PA",
    "BME": ".MC",
    "WBAG": ".VI",
    "WSE": ".WA",
    "ATHEX": ".AT",
    "BIST": ".IS",
    "BVB": ".RO",
    "BET": ".BD",
    "PSE_CZ": ".PR",
    "TYO": ".T",
    "JPX": ".T",
    "HKSE": ".HK",
    "TWSE": ".TW",
    "KRX": ".KS",
    "NSE": ".NS",
    "BSE_IN": ".BO",
    "TADAWUL": ".SR",
    "TASE": ".TA",
    "SET": ".BK",
    "IDX": ".JK",
    "PSE": ".PS",
    "HOSE": ".VN",
    "ICE": ".IC",
    "BVL": ".LM",
    "EGX": ".CA",
    "BHB": ".BH",
    "QSE": ".QA",
    "ADX": ".AD",
    "DFM": ".AE",
    "KWSE": ".KW",
    "BVS": ".SN",
    "CSEC": ".CY",  # Cyprus Stock Exchange — Yahoo Finance .CY suffix
    "BMV": ".MX",   # Bolsa Mexicana de Valores
}


def derive_yahoo_ticker(ticker: str, exchange: str) -> str:
    """Produce a Yahoo Finance ticker for (ticker, exchange) when one
    isn't already known. Returns "" if Yahoo doesn't cover the exchange.
    """
    if not ticker:
        return ""
    sfx = _YAHOO_SUFFIX_BY_EXCHANGE.get((exchange or "").upper())
    if sfx is None:
        return ""
    return ticker.upper() + sfx


def has_price_source(stock: dict) -> bool:
    """
    Return True if there is any free live-price source wired up for
    this stock. Used by the dashboard to distinguish "awaiting refresh"
    (source exists, no snapshot yet) from "no price source"
    (catalog-only exchanges like ESX, UX, BVMT, CSEM, BELEX, BSSE).
    """
    if stock.get("yahoo_ticker"):
        return True
    if (stock.get("exchange") or "").upper() in _CUSTOM_PRICE_EXCHANGES:
        return True
    # Auto-Yahoo: even if no yahoo_ticker is stored, we can derive one
    # from a covered exchange code at fetch time.
    if (stock.get("exchange") or "").upper() in _YAHOO_SUFFIX_BY_EXCHANGE:
        return True
    return False


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


# The catalog is a ~7 MB JSON file and was re-read AND re-parsed on every
# search — that parse, not the matching, was essentially the entire search
# cost (~170 ms per keystroke-driven lookup). Keep it in memory instead,
# keyed on the file's mtime so a catalog refresh is still picked up
# without a restart. Worst case two threads parse concurrently and one
# result is discarded; there is nothing to corrupt.
_CATALOG_CACHE: list[dict] | None = None
_CATALOG_MTIME: float | None = None


def _load_catalog() -> list[dict]:
    global _CATALOG_CACHE, _CATALOG_MTIME
    if not os.path.exists(_CATALOG_PATH):
        return []
    try:
        mtime = os.path.getmtime(_CATALOG_PATH)
        if _CATALOG_CACHE is not None and _CATALOG_MTIME == mtime:
            return _CATALOG_CACHE
        with open(_CATALOG_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        _CATALOG_CACHE, _CATALOG_MTIME = data, mtime
        return data
    except Exception as e:
        logger.warning("Failed to load frontier_stocks.json: %s", e)
        return []


# ---------------------------------------------------------------------------
# Yahoo Finance symbol search
# ---------------------------------------------------------------------------

def _yahoo_quote_to_result(q_obj: dict) -> dict | None:
    """Convert one Yahoo quote to our internal result shape. Returns None for non-equity."""
    if q_obj.get("quoteType") != "EQUITY":
        return None
    yahoo_sym = q_obj.get("symbol", "")
    y_exch = q_obj.get("exchange", "")
    internal_exch = _YAHOO_TO_INTERNAL.get(y_exch, y_exch)
    name = q_obj.get("longname") or q_obj.get("shortname") or yahoo_sym
    base_ticker = yahoo_sym.split(".")[0] if "." in yahoo_sym else yahoo_sym
    currency = _EXCHANGE_CURRENCY.get(internal_exch, "USD")
    defaults = get_exchange_defaults(internal_exch, base_ticker.upper())
    return {
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
    }


# Tiny TTL cache for Yahoo symbol-search. As the user types, the
# debounced client fires the same final query repeatedly (type → pause
# → backspace → re-type), and the suffix fan-out probes overlapping
# tickers across keystrokes. A 90 s memo turns those into instant hits
# and keeps us under Yahoo's rate limit. Bounded so it can't grow
# without limit on a long-running server.
_YAHOO_CACHE: dict[tuple[str, int], tuple[float, list[dict]]] = {}
_YAHOO_CACHE_TTL = 90.0
_YAHOO_CACHE_MAX = 512


def _yahoo_raw(q: str, limit: int, timeout: int = 6) -> list[dict]:
    """Single Yahoo symbol-search call. Returns raw quotes list (or [])."""
    import time as _t
    key = (q.lower(), limit)
    now = _t.monotonic()
    hit = _YAHOO_CACHE.get(key)
    if hit and (now - hit[0]) < _YAHOO_CACHE_TTL:
        return hit[1]
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
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode("utf-8", errors="replace"))
    except Exception as e:
        logger.warning("Yahoo search failed for %r: %s", q, e)
        return []
    quotes = data.get("quotes", []) or []
    if len(_YAHOO_CACHE) >= _YAHOO_CACHE_MAX:
        # Cheap eviction: drop the oldest ~quarter by timestamp.
        for k in sorted(_YAHOO_CACHE, key=lambda k: _YAHOO_CACHE[k][0]
                        )[: _YAHOO_CACHE_MAX // 4]:
            _YAHOO_CACHE.pop(k, None)
    _YAHOO_CACHE[key] = (now, quotes)
    return quotes


# Common exchange suffixes Yahoo's name search often omits. When the
# query looks like a plain ticker, fire these as parallel lookups so
# e.g. "LGO" surfaces LGO.TO (TSX) alongside NASDAQ LGO.
_TICKER_SUFFIXES = [
    ".TO",  # Toronto (TSX)
    ".V",   # TSX Venture
    ".L",   # LSE alt
    ".AX",  # ASX
    ".HK",  # Hong Kong
    ".JO",  # Johannesburg
    ".KL",  # Kuala Lumpur
    ".SI",  # Singapore
    ".DE",  # Frankfurt
    ".ST",  # Stockholm
    ".OL",  # Oslo
    ".CO",  # Copenhagen
    ".MI",  # Milan
    ".PA",  # Paris
    ".MX",  # Mexico
    ".SA",  # São Paulo
    ".TA",  # Tel Aviv
    ".BK",  # Bangkok
    ".KS",  # Korea (KOSPI)
]


def search_yahoo(query: str, limit: int = 10) -> list[dict]:
    """Yahoo Finance symbol search. After the initial name/ticker lookup,
    probe common exchange suffixes on every ticker that came back so
    cross-listings (e.g. LGO.TO) surface alongside the primary hit."""
    q = (query or "").strip()
    if not q:
        return []

    # Always run the name/fuzzy search first. Tight timeout: Yahoo
    # 429s us intermittently and a 6 s stall here freezes the whole
    # dropdown. The local catalog still returns instantly if this fails.
    quotes = _yahoo_raw(q, limit, timeout=3)

    # Build a set of candidate tickers to probe with exchange suffixes:
    #   1. Every base-ticker from the initial name-search results
    #      (catches cross-listings of the same company — e.g. name
    #      search finds NASDAQ LGO, we probe LGO.TO / LGO.V / ...)
    #   2. The query itself if it looks like a plain ticker (catches
    #      cases where Yahoo's name-search ranks NASDAQ over TSX).
    candidate_tickers: set[str] = set()
    for qt in quotes:
        if qt.get("quoteType") != "EQUITY":
            continue
        sym = (qt.get("symbol") or "")
        base = sym.split(".")[0].upper()
        if base and re.match(r"^[A-Z0-9]{1,8}$", base):
            candidate_tickers.add(base)
    if re.match(r"^[A-Za-z0-9]{2,6}$", q):
        candidate_tickers.add(q.upper())

    if candidate_tickers:
        import concurrent.futures as _cf
        import time as _time
        suffix_queries = [tk + s for tk in candidate_tickers
                          for s in _TICKER_SUFFIXES]
        # Cap total fan-out: Yahoo rate-limits aggressively, so 50+
        # parallel probes mostly just time out and stall the dropdown.
        # The cross-listing suffixes are ordered by how often our users
        # actually hit them, so truncating keeps the useful ones.
        MAX_FANOUT = 24
        suffix_queries = suffix_queries[:MAX_FANOUT]
        # Don't use `with ThreadPoolExecutor(...) as pool:` — its
        # __exit__ calls shutdown(wait=True) and blocks on every
        # in-flight request regardless of our deadline. We want the
        # dropdown to return as soon as the budget elapses.
        pool = _cf.ThreadPoolExecutor(max_workers=10)
        try:
            futures = [pool.submit(_yahoo_raw, sq, 2, 2)
                       for sq in suffix_queries]
            deadline = _time.monotonic() + 1.6
            try:
                for fut in _cf.as_completed(futures, timeout=1.6):
                    if _time.monotonic() > deadline:
                        break
                    try:
                        quotes.extend(fut.result(timeout=0.05) or [])
                    except Exception:
                        pass
            except Exception:
                pass  # as_completed timeout — expected, just stop waiting
        finally:
            # Non-blocking: lingering probes finish (or get cancelled)
            # without holding up the response.
            pool.shutdown(wait=False, cancel_futures=True)

    # Dedupe by Yahoo symbol while preserving order.
    seen: set[str] = set()
    out: list[dict] = []
    for qt in quotes:
        sym = (qt.get("symbol") or "").upper()
        if not sym or sym in seen:
            continue
        seen.add(sym)
        r = _yahoo_quote_to_result(qt)
        if r:
            out.append(r)
        if len(out) >= limit * 2:  # allow more room for cross-listings
            break
    return out


# ---------------------------------------------------------------------------
# Local catalog search
# ---------------------------------------------------------------------------

# Yahoo-style ticker suffixes ("THX.V", "8699.T", "SAF1R.RG"). People
# paste these straight from Yahoo or a broker screen, but the catalog
# stores the bare ticker — so the suffixed form matched nothing and only
# the Yahoo fallback answered, returning the listing under its own venue
# code and currency (THX.V came back as VAN/USD instead of TSXV/CAD).
# Only recognised suffixes are stripped, so tickers that genuinely
# contain a dot are left alone.
_YF_SUFFIXES = {
    "v", "to", "cn", "ne",                       # Canada
    "t", "l", "hk", "si", "kl", "bk", "jk",      # Asia / London
    "ks", "kq", "tw", "ax", "nz",                # Korea, Taiwan, AU/NZ
    "de", "f", "sg", "mu", "be", "hm", "du",     # Germany
    "pa", "as", "br", "ls", "mi", "mc", "vi",    # Euronext / IT / ES / AT
    "st", "ol", "co", "he", "ic", "rg", "ri", "tl",  # Nordics + Baltics
    "sa", "mx", "sn", "bo", "ns", "ta", "jo",    # Americas / India / IL / ZA
    "is", "wa", "pr", "bd", "sw", "cr", "at",    # TR / PL / CZ / HU / CH / GR
}


def _strip_yf_suffix(q: str) -> str:
    """Drop a recognised Yahoo exchange suffix from a ticker query."""
    if "." not in q:
        return q
    base, _, suf = q.rpartition(".")
    return base if (base and suf in _YF_SUFFIXES) else q


def _fold(s: str) -> str:
    """Lowercase and strip diacritics, so an unaccented query matches an
    accented name. Without this "pena verde" cannot find "Peña Verde",
    "assai" cannot find "Assaí", and "balcao" cannot find "Balcão" —
    which is most of Latin America, Iberia and francophone Africa. Users
    type ASCII; the catalog carries the proper spelling.
    """
    import unicodedata
    return "".join(
        c for c in unicodedata.normalize("NFD", (s or "").lower())
        if unicodedata.category(c) != "Mn")

def search_catalog(query: str, limit: int = 10) -> list[dict]:
    """Substring match against the shipped frontier_stocks.json catalog,
    ranked exact-ticker → exact-name → ticker-prefix → name-prefix →
    substring. Without internal ranking, exact ticker matches on
    later-sorted exchanges (e.g. NGX/UBA) get truncated below the limit
    when many earlier exchanges have substring hits — so e.g. searching
    "uba" was missing United Bank For Africa because Italian Ubaldi
    Costruzioni came alphabetically first."""
    q = _fold((query or "").strip())
    if not q:
        return []
    q = _strip_yf_suffix(q)
    # Multi-word queries match when EVERY word appears somewhere in the
    # name/ticker/aliases — the words needn't be contiguous. Without this
    # "grupo sura" fails to find "Grupo de Inversiones Suramericana"
    # (GRUPOSURA) because that exact string never appears, even though
    # "gruposura" and "suramericana" each match on their own.
    q_tokens = q.split()
    multi = len(q_tokens) > 1
    catalog = _load_catalog()
    bucket_exact: list[dict] = []
    bucket_exchange: list[dict] = []
    bucket_prefix: list[dict] = []
    bucket_other: list[dict] = []
    for s in catalog:
        name = _fold(s.get("name") or "")
        ticker = _fold(s.get("ticker") or "")
        exchange = _fold(s.get("exchange") or "")
        # Aliases let the catalog map common rebrands / colloquial names
        # (e.g. "etisalat" → EAND/ADX after the 2022 rebrand to e&,
        # "facebook" → META) without polluting the canonical `name`.
        aliases = [_fold(str(a)) for a in (s.get("aliases") or [])]
        alias_hit = any(q == a or q in a for a in aliases)
        # Exchange-code match — "b3" should surface all Brazilian
        # listings, "ngx" all Nigerian, "brvm" all West-African.
        exchange_hit = (exchange == q)
        # All-words-present match for multi-word queries.
        tokens_hit = False
        if multi:
            hay = name + " " + ticker + " " + " ".join(aliases)
            tokens_hit = all(tok in hay for tok in q_tokens)
        if not (q in name or q in ticker or alias_hit or exchange_hit
                or tokens_hit):
            continue
        result = dict(s)
        result["source"] = "catalog"
        result["exchDisp"] = s.get("country") or s.get("exchange", "")
        if not result.get("price_url"):
            defaults = get_exchange_defaults(result.get("exchange", ""),
                                              result.get("ticker", ""))
            result["price_url"] = defaults.get("price_url", "")
        # Exact alias matches deserve top-bucket placement so a query
        # like "etisalat" lands the right stock above any name-substring
        # noise from other catalog entries.
        if ticker == q or name == q or any(q == a for a in aliases):
            bucket_exact.append(result)
        elif exchange_hit:
            # All stocks on a typed exchange code go in their own
            # bucket between exact and prefix. Without a dedicated
            # bucket, a query like "b3" buries home-market Brazilian
            # listings under noise from foreign tickers that happen
            # to start with "B3" (B3H/FRA, B3K/FRA, …).
            bucket_exchange.append(result)
        elif (ticker.startswith(q) or name.startswith(q)
              or any(a.startswith(q) for a in aliases)):
            bucket_prefix.append(result)
        else:
            bucket_other.append(result)
    return (bucket_exact + bucket_exchange + bucket_prefix + bucket_other)[:limit]


# ---------------------------------------------------------------------------
# Merged search
# ---------------------------------------------------------------------------

def search_stocks(query: str, limit: int = 10) -> list[dict]:
    """
    Search Yahoo Finance and the internal catalog, deduping by
    (ticker, exchange).

    Ranking is relevance-first, source-second within each relevance
    bucket: exact ticker matches before prefix before substring, AND
    within each bucket catalog hits come before Yahoo hits. The
    catalog is curated for frontier/emerging markets — those are the
    stocks this tool exists to surface. So when the same ticker exists
    in multiple places (e.g. "UBA" = Italian Ubaldi via Yahoo + Nigerian
    United Bank For Africa via catalog), the frontier hit ranks first.
    """
    q = (query or "").strip()
    if not q:
        return []
    # Rank against the suffix-stripped form as well, so a pasted
    # "THX.V" scores the catalog's exact THX match instead of
    # dropping to the bottom bucket beneath Yahoo's VAN/USD row.
    q_low = _strip_yf_suffix(q.lower())

    seen = set()
    # Each bucket keeps catalog hits in a separate sub-list so we can
    # interleave them ahead of Yahoo without losing relevance ordering.
    bucket_exact_cat: list[dict] = []
    bucket_exact_yh:  list[dict] = []
    bucket_exchange_cat: list[dict] = []   # exchange-code matches
    bucket_exchange_yh:  list[dict] = []
    bucket_prefix_cat: list[dict] = []
    bucket_prefix_yh:  list[dict] = []
    bucket_other_cat:  list[dict] = []
    bucket_other_yh:   list[dict] = []

    # Pull from both sources at a wider limit so we have room to re-rank.
    fetch_limit = max(limit * 2, 20)
    # Run the two sources concurrently. The catalog is a local JSON
    # scan (~100 ms); Yahoo is a network call that intermittently
    # 429s and can take a couple of seconds. Running them in parallel
    # with a hard overall cap means a slow Yahoo no longer serializes
    # behind / in front of the instant catalog result — worst case the
    # dropdown still returns the catalog hits within the budget.
    import concurrent.futures as _cf2
    _yahoo_results: list[dict] = []
    _catalog_results: list[dict] = []
    _pool = _cf2.ThreadPoolExecutor(max_workers=2)
    try:
        _f_yahoo = _pool.submit(search_yahoo, q, fetch_limit)
        _f_cat = _pool.submit(search_catalog, q, fetch_limit)
        try:
            _catalog_results = _f_cat.result(timeout=2.5) or []
        except Exception:
            _catalog_results = []
        try:
            # Yahoo gets whatever is left of a ~3 s overall budget.
            _yahoo_results = _f_yahoo.result(timeout=3.0) or []
        except Exception:
            _yahoo_results = []  # catalog still carries the response
    finally:
        _pool.shutdown(wait=False, cancel_futures=True)

    for src_name, src in (("yahoo", _yahoo_results),
                            ("catalog", _catalog_results)):
        for r in src:
            ticker = (r.get("ticker") or "").upper()
            exchange = (r.get("exchange") or "").upper()
            key = (ticker, exchange)
            if key in seen or not ticker:
                continue
            seen.add(key)
            t_low = ticker.lower()
            n_low = (r.get("name") or "").lower()
            ex_low = exchange.lower()
            first_word = n_low.split(" ", 1)[0] if n_low else ""
            is_cat = (r.get("source") == "catalog") or (src_name == "catalog")
            # Catalog aliases (e.g. "bank of georgia" → BGEO/LSE after the
            # Lion Finance rebrand) must rank like a name match — otherwise
            # a Yahoo secondary listing whose stale name still prefix-matches
            # outranks the curated primary listing.
            aliases = [str(a).lower() for a in (r.get("aliases") or [])]
            alias_exact = any(a == q_low for a in aliases)
            alias_prefix = any(a.startswith(q_low) for a in aliases)
            if t_low == q_low or n_low == q_low or alias_exact:
                (bucket_exact_cat if is_cat else bucket_exact_yh).append(r)
            elif ex_low == q_low:
                # Dedicated bucket for "you typed an exchange code"
                # so e.g. "b3" surfaces home-market Brazilian listings
                # above incidental B3*-prefixed tickers from FRA.
                (bucket_exchange_cat if is_cat else bucket_exchange_yh).append(r)
            elif (t_low.startswith(q_low) or n_low.startswith(q_low)
                  or first_word == q_low or alias_prefix):
                (bucket_prefix_cat if is_cat else bucket_prefix_yh).append(r)
            else:
                (bucket_other_cat if is_cat else bucket_other_yh).append(r)

    # Bucket order rationale:
    # • exact (t==q or n==q): catalog first. Solves ticker collisions
    #   like "UBA" — Nigerian UBA must beat Italian Ubaldi.
    # • exchange (exchange code == q): catalog first. "b3" surfaces
    #   home-market Brazilian listings, "ngx" all Nigerian, etc.
    # • prefix (ticker/name starts with q): catalog first. Curated
    #   frontier names beat tangential matches.
    # • other (substring): YAHOO first. For a company-name search
    #   like "petrobras" the catalog might only have German DRs
    #   (PJXA:FRA / PJXC:FRA) but Yahoo has the home-market listing
    #   (PETR4:B3, PETR3:B3) plus the NYSE ADR (PBR). Yahoo's name-
    #   relevance ranking is good here; pushing FRA DRs to the top
    #   buried the actual home-market stock.
    merged = (bucket_exact_cat    + bucket_exact_yh
              + bucket_exchange_cat + bucket_exchange_yh
              + bucket_prefix_cat   + bucket_prefix_yh
              + bucket_other_yh     + bucket_other_cat)
    return merged[:limit]
