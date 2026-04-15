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
# KASE — Kazakhstan Stock Exchange, kase.kz
# ---------------------------------------------------------------------------

def update_kase() -> tuple[bool, int, str, list[dict]]:
    """
    Scrape the public shares list at kase.kz/en/shares/ for tickers,
    then resolve each ticker to a company name via stockanalysis.com
    (which has full Kazakhstan coverage at /quote/kase/<ticker>/).

    kase.kz itself is an Angular SPA that renders issuer names client-
    side, so we need a secondary source for names. stockanalysis.com
    exposes the name in the <title> tag of its structured quote pages.
    """
    existing = _existing_for_exchange("KASE")
    try:
        html = _http_get("https://kase.kz/en/shares/", timeout=20)
    except Exception as e:
        return False, 0, f"fetch failed: {e}", []

    # KASE ticker format: 3-5 upper alpha, optional underscore suffix
    # (e.g. HSBK, KSPI, FRHC_KZ). The shares page is pre-rendered by
    # Angular — tickers appear inside <span> elements inside <a> tags
    # pointing at /en/investors/shares/<ticker>.
    raw = re.findall(
        r"/en/investors/shares/([A-Z][A-Z0-9_]{1,12})", html)
    seen = set()
    tickers: list[str] = []
    for t in raw:
        if t not in seen:
            seen.add(t)
            tickers.append(t)
    if not tickers:
        return False, 0, "no tickers parsed from kase.kz", []

    # Resolve each ticker → company name via stockanalysis.com. This is
    # one HTTP call per ticker which is fine for ~50 tickers.
    entries = []
    for t in sorted(tickers):
        name = ""
        try:
            detail = _http_get(
                f"https://stockanalysis.com/quote/kase/{t}/", timeout=10)
            # Title format: "Halyk Bank of Kazakhstan Joint Stock Company
            # (KASE:HSBK) Stock Price & ..."
            m = re.search(r"<title>([^<|]+?)\s*\(KASE:", detail)
            if m:
                name = m.group(1).strip()
                # Decode common HTML entities
                name = (name.replace("&amp;", "&")
                            .replace("&#39;", "'")
                            .replace("&quot;", '"'))[:120]
        except Exception:
            pass  # Leave name blank — _make_entry falls back to ticker
        entries.append(_make_entry("KASE", t, name, "Kazakhstan",
                                    "KZT", existing.get(t)))
    return True, len(entries), (
        f"kase.kz → {len(entries)} equities (names via stockanalysis.com)"
    ), entries


# ---------------------------------------------------------------------------
# Generic afx.kwayisi.org catalog fetcher
# ---------------------------------------------------------------------------
# afx.kwayisi.org hosts compact HTML tables for several African stock
# exchanges, all in the same format: ticker link, company name, volume,
# last price, daily change (absolute). We use it for Kenya, Ghana,
# Botswana, Zambia. Each wrapper just supplies the site-slug plus the
# catalog metadata.

def _afx_kwayisi_update(
    slug: str, exchange_code: str, country: str, currency: str,
) -> tuple[bool, int, str, list[dict]]:
    existing = _existing_for_exchange(exchange_code)
    try:
        html = _http_get(f"https://afx.kwayisi.org/{slug}/", timeout=15)
    except Exception as e:
        return False, 0, f"fetch failed: {e}", []

    tables = re.findall(r"<table[\s\S]*?</table>", html)
    if not tables:
        return False, 0, f"no table found on afx.kwayisi.org/{slug}/", []
    big = max(tables, key=len)

    seen: dict[str, str] = {}
    for tr in re.split(r"<tr[^>]*>", big)[1:]:
        cells = re.split(r"<td(?:\s+[^>]*)?>", tr)
        if len(cells) < 3:
            continue
        tm = re.search(r">([A-Z][A-Z0-9]{1,10})</a>", cells[1])
        nm = re.search(r'title="([^"]+)"', cells[1])
        if not tm:
            continue
        ticker = tm.group(1)
        if ticker in seen:
            continue
        name = nm.group(1).strip() if nm else ticker
        name = (name.replace("&amp;", "&")
                    .replace("&#39;", "'")
                    .replace("&quot;", '"'))[:120]
        seen[ticker] = name

    if not seen:
        return False, 0, f"no tickers parsed from afx.kwayisi.org/{slug}/", []

    entries = []
    for t in sorted(seen.keys()):
        entries.append(_make_entry(
            exchange_code, t, seen[t], country, currency, existing.get(t)))
    return True, len(entries), (
        f"afx.kwayisi.org/{slug} → {len(entries)} equities"
    ), entries


def update_nsek() -> tuple[bool, int, str, list[dict]]:
    """Nairobi Securities Exchange (Kenya). Internal code NSEK
    disambiguates from NSE India."""
    return _afx_kwayisi_update("nse", "NSEK", "Kenya", "KES")


def update_gse() -> tuple[bool, int, str, list[dict]]:
    """Ghana Stock Exchange — Accra."""
    return _afx_kwayisi_update("gse", "GSE", "Ghana", "GHS")


def update_bwse() -> tuple[bool, int, str, list[dict]]:
    """Botswana Stock Exchange — Gaborone. Internal code BWSE to
    avoid collision with Mumbai BSE."""
    return _afx_kwayisi_update("bse", "BWSE", "Botswana", "BWP")


def update_luse() -> tuple[bool, int, str, list[dict]]:
    """Lusaka Securities Exchange — Zambia."""
    return _afx_kwayisi_update("luse", "LUSE", "Zambia", "ZMW")


# ---------------------------------------------------------------------------
# DSET — Dar es Salaam Stock Exchange (Tanzania), public JSON API
# ---------------------------------------------------------------------------
# dse.co.tz has a clean public JSON API that returns every equity's
# OHLC + volume + market cap for a given date. Much better than any
# scraping approach. Internal code DSET disambiguates from DSEB
# (Dhaka SE Bangladesh).

def update_dset() -> tuple[bool, int, str, list[dict]]:
    from datetime import datetime as _dt
    existing = _existing_for_exchange("DSET")
    today = _dt.now().strftime("%Y-%m-%d")
    url = (
        "https://www.dse.co.tz/api/get/market/prices/for/range"
        f"?to_date={today}&isLastTradeTrend=1&security_code=ALL&class=EQUITY"
    )
    try:
        html = _http_get(url, timeout=15)
        data = json.loads(html)
    except Exception as e:
        return False, 0, f"dse.co.tz API failed: {e}", []
    if not isinstance(data, dict) or not data.get("success"):
        return False, 0, "dse.co.tz API returned no data", []

    rows = data.get("data") or []
    entries = []
    for r in rows:
        ticker = (r.get("company") or "").strip().upper()
        if not ticker or not re.match(r"^[A-Z][A-Z0-9]{1,10}$", ticker):
            continue
        # Tanzania DSE API returns the company symbol in `company` —
        # it doesn't ship the full company name. Leave name blank so
        # `_make_entry` falls back to the ticker.
        entries.append(_make_entry("DSET", ticker, "", "Tanzania",
                                    "TZS", existing.get(ticker)))
    if not entries:
        return False, 0, "no equities in DSE Tanzania API response", []
    return True, len(entries), (
        f"dse.co.tz API → {len(entries)} equities"
    ), entries


# ---------------------------------------------------------------------------
# DSEB — Dhaka Stock Exchange (Bangladesh), dsebd.org scrape
# ---------------------------------------------------------------------------
# dsebd.org/latest_share_price_scroll_l.php returns a large HTML table
# with rows in the shape:
#     <td>index</td><td>TICKER</td><td>ldcp</td><td>open</td>
#     <td>high</td><td>low</td><td>ltp</td><td>close</td>...
# We filter out mutual funds (tickers starting with a digit or ending
# in MF/BOND) so the catalog only contains equities.

def update_dseb() -> tuple[bool, int, str, list[dict]]:
    existing = _existing_for_exchange("DSEB")
    try:
        html = _http_get(
            "https://www.dsebd.org/latest_share_price_scroll_l.php", timeout=20)
    except Exception as e:
        return False, 0, f"dsebd.org fetch failed: {e}", []

    rows = re.findall(r"<tr[^>]*>([\s\S]*?)</tr>", html)
    seen: set[str] = set()
    equities: list[str] = []
    for r in rows:
        cells = re.findall(r"<td[^>]*>([\s\S]*?)</td>", r)
        cleaned = [re.sub(r"\s+", " ",
                           re.sub(r"<[^>]+>", " ", c)).strip()
                   for c in cells]
        if len(cleaned) < 4:
            continue
        # Column 1 is the ticker; column 0 is the row index
        ticker = cleaned[1] if cleaned[1] else cleaned[0]
        if not re.match(r"^[A-Z][A-Z0-9]{1,12}$", ticker):
            continue
        # Filter out mutual funds / bonds
        if (ticker.startswith(tuple("0123456789"))
                or ticker.endswith(("MF", "BOND", "BD"))):
            continue
        if ticker in seen:
            continue
        seen.add(ticker)
        equities.append(ticker)

    if not equities:
        return False, 0, "no equities parsed from dsebd.org", []

    entries = []
    for t in sorted(equities):
        entries.append(_make_entry("DSEB", t, "", "Bangladesh",
                                    "BDT", existing.get(t)))
    return True, len(entries), (
        f"dsebd.org → {len(entries)} equities (excl. mutual funds)"
    ), entries


# ---------------------------------------------------------------------------
# PSX — Pakistan Stock Exchange, dps.psx.com.pk scrape
# ---------------------------------------------------------------------------

def update_psx() -> tuple[bool, int, str, list[dict]]:
    existing = _existing_for_exchange("PSX")
    try:
        html = _http_get(
            "https://dps.psx.com.pk/market-watch", timeout=20)
    except Exception as e:
        return False, 0, f"dps.psx.com.pk fetch failed: {e}", []

    rows = re.findall(r"<tr[^>]*>([\s\S]*?)</tr>", html)
    seen: set[str] = set()
    equities: list[str] = []
    for r in rows:
        cells = re.findall(r"<td[^>]*>([\s\S]*?)</td>", r)
        cleaned = [re.sub(r"\s+", " ",
                           re.sub(r"<[^>]+>", " ", c)).strip()
                   for c in cells]
        if not cleaned:
            continue
        ticker = cleaned[0]
        if not re.match(r"^[A-Z][A-Z0-9]{1,10}$", ticker):
            continue
        if ticker in seen:
            continue
        seen.add(ticker)
        equities.append(ticker)

    if not equities:
        return False, 0, "no tickers parsed from dps.psx.com.pk", []

    entries = []
    for t in sorted(equities):
        entries.append(_make_entry("PSX", t, "", "Pakistan",
                                    "PKR", existing.get(t)))
    return True, len(entries), (
        f"dps.psx.com.pk → {len(entries)} equities"
    ), entries


# ---------------------------------------------------------------------------
# CSEM — Casablanca Stock Exchange (Morocco), hardcoded top caps
# ---------------------------------------------------------------------------
# casablanca-bourse.com is an SPA with no public JSON API and no easy
# scrape. For now we ship a hand-curated list of the largest 15
# Moroccan stocks so the autocomplete finds them. Users can always
# add more via the Add Stock modal. Prices and earnings for CSEM are
# not available from free sources — it requires Serper fallback.

_CSEM_TOP = [
    ("ATW",  "Attijariwafa Bank"),
    ("BCP",  "Banque Centrale Populaire"),
    ("BOA",  "Bank of Africa (BMCE Bank)"),
    ("CIH",  "Crédit Immobilier et Hôtelier"),
    ("IAM",  "Itissalat Al-Maghrib (Maroc Telecom)"),
    ("OCP",  "OCP Group (phosphates)"),
    ("CMT",  "Ciments du Maroc"),
    ("LHM",  "LafargeHolcim Maroc"),
    ("MNG",  "Managem"),
    ("ADH",  "Addoha Immobilier"),
    ("SAH",  "Saham Assurance"),
    ("COL",  "Cosumar"),
    ("LES",  "Lesieur Cristal"),
    ("MAB",  "Maroc Automobile"),
    ("WAA",  "Wafa Assurance"),
]


def update_csem() -> tuple[bool, int, str, list[dict]]:
    existing = _existing_for_exchange("CSEM")
    entries = []
    for t, n in _CSEM_TOP:
        entries.append(_make_entry("CSEM", t, n, "Morocco",
                                    "MAD", existing.get(t)))
    return True, len(entries), (
        f"hardcoded top caps → {len(entries)} equities "
        "(Casablanca has no scrapable free source)"
    ), entries


# ---------------------------------------------------------------------------
# ZSE — Zagreb Stock Exchange (Croatia)
# ---------------------------------------------------------------------------
# zse.hr/default.aspx?id=26474 serves a single HTML page with the full
# listing table: ticker, ISIN, name, sector, shares, last price. We
# filter to primary common shares (ISIN[6:8] == "RA") — this excludes
# bonds (OB), preference shares (RB, RC), and commercial paper.

def update_zse() -> tuple[bool, int, str, list[dict]]:
    existing = _existing_for_exchange("ZSE")
    try:
        html = _http_get("https://zse.hr/default.aspx?id=26474", timeout=15)
    except Exception as e:
        return False, 0, f"zse.hr fetch failed: {e}", []

    rows = re.findall(r"<tr[^>]*>([\s\S]*?)</tr>", html)
    equities: dict[str, str] = {}
    for r in rows:
        cells = re.findall(r"<t[dh][^>]*>([\s\S]*?)</t[dh]>", r)
        cleaned = [re.sub(r"\s+", " ",
                           re.sub(r"<[^>]+>", " ", c)).replace("\xa0", " ").strip()
                   for c in cells]
        if len(cleaned) < 3:
            continue
        ticker = cleaned[0]
        isin = cleaned[1] if len(cleaned) > 1 else ""
        name = cleaned[2] if len(cleaned) > 2 else ""
        if not re.match(r"^[A-Z][A-Z0-9-]{0,9}$", ticker):
            continue
        # Primary common-share filter: Croatian ISIN pattern is
        # HR + 4-char-code + RA + 4-digits + check. "RA" marks primary
        # common stock; "RB" is a secondary class, "OB" a bond.
        if len(isin) == 12 and isin[:2] == "HR" and isin[6:8] == "RA":
            if ticker not in equities:
                equities[ticker] = name

    if not equities:
        return False, 0, "no primary shares parsed from zse.hr", []

    entries = []
    for t in sorted(equities.keys()):
        entries.append(_make_entry("ZSE", t, equities[t], "Croatia",
                                    "EUR", existing.get(t)))
    return True, len(entries), (
        f"zse.hr → {len(entries)} primary equities"
    ), entries


# ---------------------------------------------------------------------------
# BELEX — Belgrade Stock Exchange (Serbia)
# ---------------------------------------------------------------------------
# belex.rs doesn't expose a scrapable listings page — trading info is
# loaded client-side via JS. We ship a hand-curated BELEX15 + prominent
# BELEXline seed list. All tickers are verified to exist on
# stockanalysis.com/quote/belex/ so earnings work automatically.

_BELEX_TOP = [
    ("NIIS",  "Naftna Industrija Srbije"),
    ("ENHL",  "Energoprojekt Holding"),
    ("MTLC",  "Metalac"),
    ("AERO",  "Aerodrom Nikola Tesla Belgrade"),
    ("KMBN",  "Komercijalna banka"),
    ("JESV",  "MPP Jedinstvo"),
    ("LSTA",  "Lasta"),
    ("TIGR",  "Tigar"),
    ("IMLK",  "Imlek"),
    ("DNOS",  "Dunav osiguranje"),
    ("JMBN",  "Jubmes banka"),
    ("APTK",  "Apatinska pivara"),
    ("DJMN",  "Dijamant"),
    ("SJPT",  "Sojaprotein"),
    ("FITO",  "Galenika Fitofarmacija"),
    ("VZAS",  "Veterinarski zavod Subotica"),
]


def update_belex() -> tuple[bool, int, str, list[dict]]:
    existing = _existing_for_exchange("BELEX")
    entries = []
    for t, n in _BELEX_TOP:
        entries.append(_make_entry("BELEX", t, n, "Serbia",
                                    "RSD", existing.get(t)))
    return True, len(entries), (
        f"hardcoded BELEX15 + top names → {len(entries)} equities "
        "(belex.rs has no scrapable listings page)"
    ), entries


# ---------------------------------------------------------------------------
# BSSE — Bratislava Stock Exchange (Slovakia)
# ---------------------------------------------------------------------------
# BSSE is extremely small — only a handful of actively traded stocks,
# none indexed by Yahoo Finance. bsse.sk is an SPA without a public
# listings API. We ship the SAX index members + a couple of other
# frequently-traded names as a hand-curated seed list.

_BSSE_TOP = [
    ("TMR",   "Tatry mountain resorts"),
    ("VUB",   "Všeobecná úverová banka"),
    ("BHP",   "Biotika"),
    ("SES",   "SES Tlmace"),
    ("OTP",   "OTP Banka Slovensko"),
    ("SIE",   "Slovnaft"),
    ("TTP",   "Tipos, národná lotériová spoločnosť"),
    ("BSL",   "Bratislavská teplárenská"),
    ("EGS",   "Elektrogas"),
    ("ZSNP",  "Železiarne Podbrezová"),
]


def update_bsse() -> tuple[bool, int, str, list[dict]]:
    existing = _existing_for_exchange("BSSE")
    entries = []
    for t, n in _BSSE_TOP:
        entries.append(_make_entry("BSSE", t, n, "Slovakia",
                                    "EUR", existing.get(t)))
    return True, len(entries), (
        f"hardcoded SAX top caps → {len(entries)} equities "
        "(bsse.sk has no scrapable listings page)"
    ), entries


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------

UPDATERS = {
    "UZSE": update_uzse,
    "NGX":  update_ngx,
    "BRVM": update_brvm,
    "KSE":  update_kse,
    "KASE": update_kase,
    "NSEK": update_nsek,
    "GSE":  update_gse,
    "BWSE": update_bwse,
    "LUSE": update_luse,
    "DSET": update_dset,
    "DSEB": update_dseb,
    "PSX":  update_psx,
    "CSEM": update_csem,
    "ZSE":  update_zse,
    "BELEX": update_belex,
    "BSSE": update_bsse,
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
