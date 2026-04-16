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


def _yahoo_raw(q: str, limit: int, timeout: int = 6) -> list[dict]:
    """Single Yahoo symbol-search call. Returns raw quotes list (or [])."""
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
    return data.get("quotes", []) or []


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

    # Always run the name/fuzzy search first.
    quotes = _yahoo_raw(q, limit)

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
        suffix_queries = [tk + s for tk in candidate_tickers for s in _TICKER_SUFFIXES]
        try:
            with _cf.ThreadPoolExecutor(max_workers=10) as pool:
                futures = [pool.submit(_yahoo_raw, sq, 2, 4) for sq in suffix_queries]
                for fut in _cf.as_completed(futures, timeout=8):
                    try:
                        quotes.extend(fut.result() or [])
                    except Exception:
                        pass
        except Exception:
            pass  # main result still works even if fan-out stalls

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
