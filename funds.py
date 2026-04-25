"""
funds.py — Fund-newsletter scanner for emerging-edge.

When a frontier/emerging-market fund (Asia Frontier Capital, Tundra,
African Lions Fund, Pangolin Asia, Undervalued Shares, …) publishes a
monthly report or letter, we want to know if any of our watchlist
stocks are mentioned. This module pulls each fund's public output,
scans the text for watchlist company names, and stores the
surrounding snippet plus a back-link.

Sources are configured by `_FUND_SOURCES` below. Each entry has:

    id           → stable internal slug (used in DB row keys)
    name         → human label shown in the dashboard
    home_url     → root link to the fund site
    discover     → callable(fetch_fn) → [(report_date, report_url, kind)]
                   where ``kind`` is 'html' or 'pdf' (defaults to 'html')
    weight       → ordering weight in the dashboard (higher = first)

To track another fund, add an entry. PDF text extraction needs ``pypdf``
(stdlib-only fallback returns empty if pypdf isn't installed).

Public API
----------
``run_funds(db, config) -> dict``
    Refresh every configured fund. Returns a per-fund summary.
"""

from __future__ import annotations

import datetime
import io
import json as _json
import logging
import re
import urllib.parse

logger = logging.getLogger("emerging-edge.funds")


# ---------------------------------------------------------------------------
# PDF helper (graceful no-op when pypdf isn't installed)
# ---------------------------------------------------------------------------

def _extract_pdf_text(raw_bytes: bytes, max_pages: int = 20) -> str:
    """Extract text from a PDF byte string. Returns '' on failure."""
    if not raw_bytes:
        return ""
    try:
        import pypdf  # type: ignore
    except Exception:
        logger.info("pypdf not available — PDF sources will be skipped. "
                    "Run `pip install pypdf` to enable.")
        return ""
    try:
        reader = pypdf.PdfReader(io.BytesIO(raw_bytes))
        chunks = []
        for i, page in enumerate(reader.pages):
            if i >= max_pages:
                break
            try:
                chunks.append(page.extract_text() or "")
            except Exception:
                continue
        return "\n".join(chunks)
    except Exception as e:
        logger.info("PDF parse failed: %s", e)
        return ""


def _http_get_bytes(url: str, timeout: int = 15) -> bytes | None:
    """Fetch raw bytes from a URL (used for PDF downloads). Returns None on failure."""
    import ssl as _ssl
    import urllib.request, urllib.error
    ctx = _ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = _ssl.CERT_NONE
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/120.0 Safari/537.36",
        "Accept": "*/*",
    })
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
            return resp.read()
    except Exception as e:
        logger.info("PDF fetch failed for %s: %s", url, e)
        return None


# ---------------------------------------------------------------------------
# AFC monthly newsletter discovery
# ---------------------------------------------------------------------------
# AFC publishes one monthly newsletter that covers all of their funds
# (Asia Frontier, Iraq, Vietnam, Uzbekistan). Index pattern:
#   /newsletter/monthly-newsletter.html
#       └─ /<year>.html
#               └─ /newsletter-<year>/<month>-<year>.html

_AFC_BASE = "https://www.asiafrontiercapital.com"


def _afc_discover_reports(fetch_fn) -> list[tuple[str, str]]:
    """Return [(report_date, absolute_url)] for AFC monthly newsletters
    across the most recent three years. ``report_date`` is YYYY-MM."""
    out: list[tuple[str, str]] = []
    today = datetime.date.today()
    years = [today.year, today.year - 1, today.year - 2]
    for year in years:
        index_url = f"{_AFC_BASE}/{year}.html"
        html = fetch_fn(index_url)
        if not html:
            continue
        # href="/newsletter-2026/march-2026.html"
        for m in re.finditer(
            rf'href="(/newsletter-{year}/([a-z]+)-{year}\.html)"',
            html, re.IGNORECASE
        ):
            href, month_name = m.group(1), m.group(2).lower()
            month_idx = _MONTH_INDEX.get(month_name)
            if not month_idx:
                continue
            date_str = f"{year}-{month_idx:02d}"
            out.append((date_str, _AFC_BASE + href))
    # Newest first
    out.sort(reverse=True)
    return out


_MONTH_INDEX = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5,
    "june": 6, "july": 7, "august": 8, "september": 9, "october": 10,
    "november": 11, "december": 12,
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "jun": 6, "jul": 7,
    "aug": 8, "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12,
}


# ---------------------------------------------------------------------------
# African Lions Fund — Tim Staermose, monthly investor letters as PDFs
# ---------------------------------------------------------------------------
_ALF_INDEX = "https://africanlionsfund.com/letters-to-investors/"


def _alf_discover_reports(fetch_fn) -> list:
    """Return [(date, url, 'pdf')] for African Lions Fund letters."""
    html = fetch_fn(_ALF_INDEX)
    if not html:
        return []
    out: list = []
    seen = set()
    # Letter URLs include date hints, but to be safe we'll parse dates
    # both from the path and the link text. Year & month directly visible
    # in the WP upload path: /uploads/<year>/<mm>/...-<MonthName>-YYYY...pdf
    pat = re.compile(
        r'href="(https://africanlionsfund\.com/wp-content/uploads/[^"]+\.pdf)"',
        re.IGNORECASE)
    for url in pat.findall(html):
        if url in seen:
            continue
        seen.add(url)
        # Extract month/year from the filename portion
        m = re.search(r"-([A-Za-z]+)-?(20\d{2})", url)
        if not m:
            continue
        month_name = m.group(1).lower()
        year = m.group(2)
        idx = _MONTH_INDEX.get(month_name)
        if not idx:
            continue
        date_str = f"{year}-{idx:02d}"
        out.append((date_str, url, "pdf"))
    out.sort(reverse=True)
    return out


# ---------------------------------------------------------------------------
# Tundra Funder — Swedish frontier-markets shop, Wix-hosted blog
# ---------------------------------------------------------------------------
_TUNDRA_INDEX = "https://www.tundrafonder.se/news/categories/monthly-comment"


def _tundra_discover_reports(fetch_fn) -> list:
    """Tundra publishes a monthly Sustainable Frontier comment under
    /post/monthly-comment-sustainable-frontier-<month>-<year>(-N). The
    archive page contains the full URL list.
    """
    html = fetch_fn(_TUNDRA_INDEX)
    if not html:
        return []
    out: list = []
    seen = set()
    pat = re.compile(
        r'monthly-comment-sustainable-frontier-([a-z]+)-(20\d{2})(?:-\d+)?',
        re.IGNORECASE)
    for m in pat.finditer(html):
        month_name = m.group(1).lower()
        year = m.group(2)
        idx = _MONTH_INDEX.get(month_name)
        if not idx:
            continue
        slug = m.group(0).rstrip("\\")
        url = f"https://www.tundrafonder.se/post/{slug}"
        if url in seen:
            continue
        seen.add(url)
        out.append((f"{year}-{idx:02d}", url, "html"))
    out.sort(reverse=True)
    return out


# ---------------------------------------------------------------------------
# Pangolin Asia — monthly letter PDFs. Site blocks bots with Cloudflare; we
# use the Wayback Machine as a free, polite mirror.
# ---------------------------------------------------------------------------
_PANGOLIN_WAYBACK = "https://web.archive.org/web/2024*/pangolinfund.com/comm.html"
_PANGOLIN_AVAILABLE_API = ("https://archive.org/wayback/available"
                           "?url=pangolinfund.com/comm.html&timestamp={ts}")


def _pangolin_discover_reports(fetch_fn) -> list:
    """Use the Wayback Machine to read the Pangolin newsletter archive.

    Pangolin's own server returns 403 to non-browser clients. The Internet
    Archive has snapshots of their public ``comm.html`` index page.
    """
    # Try a recent Wayback snapshot of the index. This is best-effort —
    # if it 503s we just return empty (we'll try again next refresh).
    today = datetime.date.today()
    for offset_days in (7, 30, 90, 180, 365):
        d = today - datetime.timedelta(days=offset_days)
        ts = d.strftime("%Y%m%d")
        snap_url = f"https://web.archive.org/web/{ts}000000*/pangolinfund.com"
        # Easier: just hit a known good snapshot of the comm page
        index_url = (f"https://web.archive.org/web/{ts}120000/"
                     "https://www.pangolinfund.com/comm.html")
        html = fetch_fn(index_url)
        if html and len(html) > 5000:
            break
    else:
        return []

    out: list = []
    seen = set()
    # Pangolin letters live at /assets/PA-Comm-...pdf or /letters/...
    for m in re.finditer(
        r'href="([^"]+(?:Comm|Pangolin|Letter)[^"]*\.pdf)"',
        html, re.IGNORECASE
    ):
        href = m.group(1)
        if not href.startswith("http"):
            # Wayback prefixes paths
            href = "https://web.archive.org" + href
        if href in seen:
            continue
        seen.add(href)
        # Try to pull a date from the filename
        d = re.search(r"(20\d{2})[-_]?(0[1-9]|1[0-2])", href)
        if d:
            date_str = f"{d.group(1)}-{d.group(2)}"
        else:
            mname = re.search(
                r"(January|February|March|April|May|June|July|August|"
                r"September|October|November|December)[\s_-]+(20\d{2})",
                href, re.IGNORECASE)
            if not mname:
                continue
            month_idx = _MONTH_INDEX.get(mname.group(1).lower())
            if not month_idx:
                continue
            date_str = f"{mname.group(2)}-{month_idx:02d}"
        out.append((date_str, href, "pdf"))
    out.sort(reverse=True)
    return out


# ---------------------------------------------------------------------------
# Undervalued Shares (Swen Lorenz) — weekly dispatches via WordPress RSS
# ---------------------------------------------------------------------------
_UV_FEED = "https://www.undervalued-shares.com/feed/"


def _uv_discover_reports(fetch_fn) -> list:
    """Each weekly dispatch is fetched as its own URL — the body is on the
    article page itself, not the RSS (the RSS only has titles)."""
    rss = fetch_fn(_UV_FEED)
    if not rss:
        return []
    items = re.findall(r"<item>(.*?)</item>", rss, re.DOTALL)
    out: list = []
    for it in items[:20]:  # last 20 weekly dispatches
        link_m = re.search(r"<link>([^<]+)</link>", it)
        date_m = re.search(r"<pubDate>([^<]+)</pubDate>", it)
        if not link_m or not date_m:
            continue
        link = link_m.group(1).strip()
        # pubDate format: "Fri, 24 Apr 2026 05:45:26 +0000"
        try:
            dt = datetime.datetime.strptime(
                date_m.group(1).strip()[:25], "%a, %d %b %Y %H:%M:%S")
            date_str = dt.strftime("%Y-%m")
        except Exception:
            continue
        out.append((date_str, link, "html"))
    return out


# ---------------------------------------------------------------------------
# Fund source registry
# ---------------------------------------------------------------------------

_FUND_SOURCES: list[dict] = [
    {
        "id": "afc_monthly",
        "name": "AFC Monthly Newsletter",
        "home_url": _AFC_BASE,
        "discover": _afc_discover_reports,
        "weight": 100,
    },
    {
        "id": "tundra_sustainable",
        "name": "Tundra Sustainable Frontier",
        "home_url": "https://www.tundrafonder.se/",
        "discover": _tundra_discover_reports,
        "weight": 90,
    },
    {
        "id": "alf_letters",
        "name": "African Lions Fund (Tim Staermose)",
        "home_url": "https://africanlionsfund.com/",
        "discover": _alf_discover_reports,
        "weight": 85,
    },
    {
        "id": "pangolin_asia",
        "name": "Pangolin Asia",
        "home_url": "https://www.pangolinfund.com/",
        "discover": _pangolin_discover_reports,
        "weight": 80,
    },
    {
        "id": "undervalued_shares",
        "name": "Undervalued Shares (Swen Lorenz)",
        "home_url": "https://www.undervalued-shares.com/",
        "discover": _uv_discover_reports,
        "weight": 70,
    },
]


# Funds we'd like to add but currently can't reach without manual help —
# either JS-rendered SPA, gated factsheets, or no public commentary. Listed
# here so they're visible in the Engine Room and easy to revisit.
KNOWN_BLOCKED_FUNDS: list[dict] = [
    {
        "id": "robur_smallcap_em",
        "name": "Swedbank Robur Småbolag EM",
        "home_url": "https://www.swedbankrobur.se/",
        "reason": "Fund pages are JS-rendered SPA. Requires headless browser.",
    },
    {
        "id": "fourton",
        "name": "Fourton (Indonesia)",
        "home_url": "https://fourton.fi/",
        "reason": "No public monthly commentary indexed on the site — only "
                  "regulatory PDFs. Their Indonesia letters are behind login.",
    },
    {
        "id": "fidelity_asian_values",
        "name": "Fidelity Asian Values PLC",
        "home_url": "https://www.fidelity.co.uk/factsheet-data/factsheet/"
                    "GB0030328260-fidelity-asian-values-plc/key-statistics",
        "reason": "Factsheets are gated PDFs reached via JS factsheet picker; "
                  "interviews are video-only.",
    },
    {
        "id": "grandeur_peak",
        "name": "Grandeur Peak Global Advisors",
        "home_url": "https://grandeurpeakglobal.com/",
        "reason": "Quarterly commentary is JS-rendered; needs investor login "
                  "for full PDFs.",
    },
]


# ---------------------------------------------------------------------------
# Name matching
# ---------------------------------------------------------------------------

# Single-word company names that overlap with common English / financial
# vocabulary. If the company name cleans down to one of these, we won't
# use it as a needle on its own — we'll require the FULL name (including
# the legal suffix) to appear instead, because "Critical Holdings Bhd"
# is unique while "critical" matches everything.
_GENERIC_NAMES = {
    # generic corporate words
    "global", "industries", "industry", "international", "group", "holdings",
    "holding", "technologies", "tech", "capital", "corp", "company",
    "limited", "ltd", "asia", "africa", "europe", "frontier", "energy",
    "power", "bank", "finance", "investment", "growth", "value", "fund",
    "solutions", "systems", "services", "resources", "products", "trading",
    # adjectives commonly used in company names
    "critical", "premium", "prime", "royal", "alpha", "beta", "delta",
    "omega", "premier", "first", "finest", "solid", "strong", "golden",
    "silver", "smart", "swift", "quick", "super", "mega", "ultra", "nova",
    "vista", "vital", "central", "eastern", "western", "northern",
    "southern", "modern", "classic", "advanced", "standard", "dynamic",
    "general", "national", "regional", "metro", "urban", "elite", "core",
    "magna", "magnum", "platinum", "diamond", "imperial",
    # common nouns
    "media", "online", "express", "direct", "world", "land", "network",
    "channel", "studio", "lab", "labs", "works",
}

# Legal suffixes we strip from company names before matching.
_NAME_SUFFIX_PAT = re.compile(
    r"\s+("
    r"S\.A\.|S\.A|SA|SpA|S\.p\.A\.|N\.V\.|NV|"
    r"PLC|Plc|"
    r"Inc\.?|Corp\.?|Corporation|Co\.|"
    r"Ltd\.?|Limited|Pte\.?\s*Ltd\.?|Pty\.?\s*Ltd\.?|"
    r"AB|ABp|Oyj|ASA|A/S|"
    r"Bhd\.?|Berhad|"
    r"AG|GmbH|"
    r"PSC|JSC|OJSC|"
    r"Holdings?|Group"
    r")\s*$",
    re.IGNORECASE,
)


def _clean_company_name(name: str) -> str:
    """Strip legal suffixes from a company name for substring matching."""
    if not name:
        return ""
    s = name.strip()
    # Some names are double-suffixed ("X Holdings Ltd"). Strip up to twice.
    for _ in range(2):
        new = _NAME_SUFFIX_PAT.sub("", s).strip()
        if new == s:
            break
        s = new
    return s


SETTING_ALIASES = "fund_match_aliases"


# Default stock-name aliases shipped with the repo so a fresh install
# matches popular non-Latin or local-language references out of the box.
# Used by funds-newsletter scanner AND forum matchers (Reddit, capital.gr,
# bankier.pl, …) — see fetchers._build_forum_needles().
#
# Keyed by TICKER:EXCHANGE (uppercase). Add an entry here when you find
# a stock that's regularly referred to by a name that doesn't match the
# watchlist's primary name (Greek script, local-language, abbreviation).
DEFAULT_FUND_ALIASES: dict[str, list[str]] = {
    # Uzbekistan — frequently called by English long-name in fund letters
    "URTS:UZSE":   ["Uzbekistan Commodity Exchange", "Uzbek Commodity Exchange",
                    "UzCEX", "UzRTSB"],
    "HMKB:UZSE":   ["Hamkorbank", "Hamkor Bank"],
    "CBSK:UZSE":   ["Chilonzor", "Chilonzor Universal"],
    # Greece — capital.gr and Greek press use Greek script for tickers
    "OPAP:ATHEX":  ["ΟΠΑΠ"],
    "PPC:ATHEX":   ["ΔΕΗ"],
    "PEIR:ATHEX":  ["Πειραιώς", "Πειραι"],
    "ASCO:ATHEX":  ["AS Company", "AS COMPANY", "ΑΣ ΚΟΜΠΑΝΥ", "AS Commercial"],
    # Indonesia — short names that show up on r/finansial / X / news
    "ARNA:IDX":    ["Arwana", "Arwana Citra", "Arwana Citramulia"],
    "EKAD:IDX":    ["Ekadharma", "Ekadharma International"],
    "IGAR:IDX":    ["Champion Pacific"],
    "MSTI:IDX":    ["Mastersystem", "Mastersystem Infotama"],
    "ULTJ:IDX":    ["Ultrajaya", "Ultra Jaya", "Ultra Milk"],
    # Philippines
    "RFM:PSE":     ["RFM Corporation", "RFM Corp"],
}


def get_aliases(db) -> dict[str, list[str]]:
    """Merged alias map: shipped defaults + user-managed overrides.

    Keyed by ``TICKER:EXCHANGE`` (uppercased), value is a list of
    additional needles to search for. The user's saved entries (from
    the Engine Room "Stock name aliases" card) are layered on top of
    ``DEFAULT_FUND_ALIASES`` — for any ticker the user has saved
    aliases for, their list REPLACES the defaults (so they can drop
    a default they don't like by saving a different list).
    """
    import json as _json
    out: dict[str, list[str]] = {
        k: list(v) for k, v in DEFAULT_FUND_ALIASES.items()
    }
    try:
        raw = db.get_setting(SETTING_ALIASES, "")
    except Exception:
        return out
    if not raw:
        return out
    try:
        m = _json.loads(raw)
    except Exception:
        return out
    for k, v in (m or {}).items():
        key = str(k).strip().upper()
        if isinstance(v, list):
            vals = [str(x).strip() for x in v if str(x).strip()]
            if vals:
                # User entry replaces default entry for this ticker
                out[key] = vals
    return out


def set_aliases(db, mapping: dict[str, list[str]]) -> None:
    """Persist the alias map."""
    import json as _json
    cleaned: dict[str, list[str]] = {}
    for k, v in (mapping or {}).items():
        key = str(k).strip().upper()
        if not key or not isinstance(v, list):
            continue
        vals = [str(x).strip() for x in v if str(x).strip()]
        if vals:
            cleaned[key] = vals
    db.set_setting(SETTING_ALIASES, _json.dumps(cleaned, ensure_ascii=False))


def _build_match_terms(stock: dict, aliases: dict[str, list[str]] | None = None) -> list[str]:
    """Return lowercase needles for this stock that, if found in a fund
    report, count as a match.

    Strategy:
      * If the company name has 2+ words after stripping legal suffixes,
        use that as the primary needle ("Bridge Solutions Hub").
      * If it cleans to a single word, only use it standalone if the word
        is distinctive (not in ``_GENERIC_NAMES``); otherwise require
        the full name (with suffix) as the needle to avoid false matches
        on common English words like "critical" or "global".
      * Tickers are not used as standalone needles by default — fund
        newsletters write narratives, not tickers, and short tickers
        cause too many false positives.
    """
    name = (stock.get("name") or "").strip()
    cleaned = _clean_company_name(name).strip()
    ticker = (stock.get("ticker") or "").strip().upper()
    exchange = (stock.get("exchange") or "").strip().upper()

    needles: list[str] = []
    # User-supplied aliases come first — these are the most authoritative.
    if aliases:
        key = f"{ticker}:{exchange}"
        for alias in aliases.get(key, []):
            a = alias.strip().lower()
            if a and a not in needles:
                needles.append(a)

    word_count = len(cleaned.split()) if cleaned else 0
    cleaned_low = cleaned.lower()
    name_low = name.lower()

    if word_count >= 2:
        needles.append(cleaned_low)
    elif word_count == 1:
        # Only use a single-word needle if it's not generic English.
        # ≥4 chars, not in our generic blocklist, and contains a letter.
        has_letter = any(c.isalpha() for c in cleaned_low)
        if (len(cleaned_low) >= 4
                and has_letter
                and cleaned_low not in _GENERIC_NAMES):
            needles.append(cleaned_low)
        elif name_low and name_low not in needles:
            # Fall back to the full multi-word name (e.g. "Tech Bhd")
            needles.append(name_low)

    # Tickers as needles: only include when distinctive — pure alpha,
    # ≥4 chars, not a common English/financial word. Funds often
    # parenthesise the ticker after the name (e.g. "(TSE: URTS)").
    if (ticker
            and len(ticker) >= 4
            and ticker.isalpha()
            and ticker.lower() not in _GENERIC_NAMES
            and ticker.lower() not in needles):
        needles.append(ticker.lower())
    return needles


def _find_mentions(text: str, stock: dict, max_snippet: int = 320,
                   aliases: dict[str, list[str]] | None = None) -> list[str]:
    """Return list of unique snippets where ``stock`` is mentioned in
    ``text``. Each snippet is ~max_snippet chars centred on the match."""
    if not text or not stock:
        return []
    needles = _build_match_terms(stock, aliases=aliases)
    if not needles:
        return []
    text_low = text.lower()
    seen_starts: set[int] = set()
    snippets: list[str] = []
    for needle in needles:
        # Word-boundary search (or near-word boundary) — avoid matching
        # 'inpro' inside 'input', 'plenitude' inside 'plenitudefoo' etc.
        pat = re.compile(r"(?:^|[\s\W])" + re.escape(needle) + r"(?:$|[\s\W])",
                         re.IGNORECASE)
        for m in pat.finditer(text_low):
            start = max(0, m.start() - max_snippet // 2)
            end = min(len(text), start + max_snippet)
            # De-dupe overlapping hits
            if any(abs(start - s) < 60 for s in seen_starts):
                continue
            seen_starts.add(start)
            raw = text[start:end].strip()
            # Trim partial words at edges
            if start > 0:
                raw = "…" + raw[raw.find(" ") + 1:] if " " in raw[:60] else "…" + raw
            if end < len(text):
                last_space = raw.rfind(" ")
                if last_space > max_snippet * 0.6:
                    raw = raw[:last_space] + "…"
                else:
                    raw = raw + "…"
            raw = re.sub(r"\s+", " ", raw)
            snippets.append(raw)
            if len(snippets) >= 3:  # cap per stock per report
                break
        if len(snippets) >= 3:
            break
    return snippets


# ---------------------------------------------------------------------------
# HTML → text extraction
# ---------------------------------------------------------------------------

def _strip_html_to_text(html: str) -> str:
    """Reduce a fund newsletter HTML page to readable text."""
    if not html:
        return ""
    # Drop scripts/styles/navs entirely
    s = re.sub(r"<script[^>]*>.*?</script>", " ", html, flags=re.DOTALL | re.IGNORECASE)
    s = re.sub(r"<style[^>]*>.*?</style>",   " ", s,    flags=re.DOTALL | re.IGNORECASE)
    s = re.sub(r"<nav[^>]*>.*?</nav>",       " ", s,    flags=re.DOTALL | re.IGNORECASE)
    # Convert paragraph/break tags to newlines so snippets read naturally
    s = re.sub(r"</(p|div|li|h[1-6]|tr|br)\s*>", "\n", s, flags=re.IGNORECASE)
    s = re.sub(r"<br\s*/?>",                    "\n", s, flags=re.IGNORECASE)
    # Strip the rest
    s = re.sub(r"<[^>]+>", " ", s)
    # HTML entities — minimal subset
    s = (s.replace("&nbsp;", " ").replace("&amp;", "&")
           .replace("&quot;", '"').replace("&#39;", "'")
           .replace("&lt;", "<").replace("&gt;", ">"))
    # Collapse whitespace
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

def run_funds(db, config: dict, fetch_fn=None) -> dict:
    """Iterate every configured fund, discover its recent reports, and
    scan each one for watchlist stock mentions. Cached by report URL —
    we never re-scan the same report.

    ``fetch_fn`` is a callable that takes a URL and returns raw HTML
    text, defaults to fetchers._fetch_page_text(url, raw=True).
    """
    if fetch_fn is None:
        from fetchers import _fetch_page_text  # type: ignore
        def fetch_fn(url: str) -> str:
            return _fetch_page_text(url, timeout=15, raw=True) or ""

    from fetchers import get_active_stocks  # type: ignore
    watchlist = get_active_stocks(db, config)
    if not watchlist:
        logger.info("funds: empty watchlist, nothing to scan")
        return {}
    aliases = get_aliases(db)

    summary: dict[str, dict] = {}
    for fund in _FUND_SOURCES:
        fund_id = fund["id"]
        fund_name = fund["name"]
        already_scanned = db.get_fund_mention_report_urls(fund_id)
        try:
            reports = fund["discover"](fetch_fn) or []
        except Exception:
            logger.exception("funds: discover failed for %s", fund_id)
            reports = []
        # Per refresh, scan the most recent N reports per fund. Already
        # scanned URLs are skipped, so cost on subsequent runs is just
        # the discovery fetch + a couple of fresh ones.
        reports = reports[:24]
        new_reports = 0
        new_mentions = 0
        for entry in reports:
            # Backward compat: older 2-tuple discoverers still work
            if len(entry) == 2:
                report_date, report_url = entry
                kind = "html"
            else:
                report_date, report_url, kind = entry
            if report_url in already_scanned:
                continue
            new_reports += 1
            text = ""
            if kind == "pdf":
                raw = _http_get_bytes(report_url)
                if raw:
                    text = _extract_pdf_text(raw)
            else:
                html = fetch_fn(report_url)
                if html:
                    text = _strip_html_to_text(html)
            if not text:
                # Mark as scanned via a sentinel so we don't retry forever.
                # Insert a synthetic row keyed by url with empty ticker is
                # not allowed (ticker NOT NULL), so just skip; we'll retry
                # next refresh.
                continue
            for stock in watchlist:
                snippets = _find_mentions(text, stock, aliases=aliases)
                if not snippets:
                    continue
                joined = "  •  ".join(snippets)[:600]
                stored = db.insert_fund_mention(
                    fund_id=fund_id,
                    fund_name=fund_name,
                    report_date=report_date,
                    report_url=report_url,
                    ticker=stock.get("ticker", ""),
                    exchange=stock.get("exchange", ""),
                    snippet=joined,
                )
                if stored:
                    new_mentions += 1
                    logger.info("FUND %s/%s: %s mentioned in %s",
                                fund_id, report_date,
                                stock.get("ticker"), report_url)
        summary[fund_id] = {
            "name": fund_name,
            "new_reports_scanned": new_reports,
            "new_mentions": new_mentions,
        }
    return summary


def list_funds() -> list[dict]:
    """Return a copy of the configured fund sources (for the Engine Room)."""
    return [{k: v for k, v in f.items() if k != "discover"}
            for f in _FUND_SOURCES]
